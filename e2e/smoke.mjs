#!/usr/bin/env node
/**
 * M0 冒烟 e2e（设计文档 §9 M0 验收口径：profile + webserver + 单 turn 对话 +
 * SSE 三帧 + MySQL 落事件；扩展钉：durable SSE 续传 / 五态 status / 冷恢复 / 多用户隔离）。
 *
 * 编排：起 mock LLM（复用 agent-framework e2e 录制回放引擎，零密钥）→ 清库 →
 * 起 dsh oaf-web → 断言 → 冷恢复 → 断言 → 收尾。
 *
 * 环境前置：本机 MySQL（MYSQL_URL 指定库，默认 docker e2e-mysql 13306 的 agent_framework_dsh，
 * 库不存在会自动创建）；1521 之外端口均可经 env 覆盖。
 *
 * 运行：node e2e/smoke.mjs   （或 make smoke）
 */
import { spawn, execSync } from 'node:child_process';
import { setTimeout as sleep } from 'node:timers/promises';
import { existsSync, mkdirSync, readFileSync, symlinkSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import path from 'node:path';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const AF_E2E = process.env.AF_E2E_DIR ?? path.join(ROOT, '..', 'agent-framework', 'e2e');

const PORT = Number(process.env.SERVER_PORT ?? 18100);
const MOCK_PORT = Number(process.env.MOCK_LLM_PORT ?? 18091);
const MOCK_BASE = `http://127.0.0.1:${MOCK_PORT}`;
const BASE = `http://127.0.0.1:${PORT}`;
// 运行时驱动：spawn = 本地进程（开发回归）；docker = 部署容器（部署验证，冷恢复 = 容器强杀重建）；
// k8s = kind 集群实例（C19 平台集成验证，冷恢复 = Pod 强杀由 Deployment 重建）
const DRIVER = process.env.OAF_RUNTIME_DRIVER ?? 'spawn';
const IMAGE = process.env.OAF_IMAGE ?? 'agent-framework-dsh:m0';
const CONTAINER = process.env.OAF_CONTAINER ?? 'oaf-dsh-e2e';
const K8S_MANIFEST = process.env.OAF_K8S_MANIFEST ?? path.join(ROOT, 'manifests', 'k8s-m0.yaml');
const K8S_MYSQL = {
  localPort: Number(process.env.OAF_K8S_MYSQL_PORT ?? 13307),
  user: process.env.OAF_K8S_MYSQL_USER ?? 'oaf_dsh',
  pass: process.env.OAF_K8S_MYSQL_PASSWORD ?? 'OafDsh2026',
};
const MYSQL = {
  url: process.env.CHECKPOINT_JDBC_URL ?? 'jdbc:mysql://127.0.0.1:13306/agent_framework_dsh',
  user: process.env.CHECKPOINT_USERNAME ?? 'e2e',
  pass: process.env.CHECKPOINT_PASSWORD ?? 'e2e-pass',
};
/** 运行时环境（两种驱动共用同一份 env 契约）。 */
const RUNTIME_ENV = {
  LLM_API_KEY: 'mock-key',
  LLM_MODEL_ID: 'gpt-mock',
  LLM_BASE_URL: `${MOCK_BASE}/v1`,
  SERVER_HOST: '127.0.0.1',
  SERVER_PORT: String(PORT),
  CHECKPOINT_JDBC_URL: MYSQL.url,
  CHECKPOINT_USERNAME: MYSQL.user,
  CHECKPOINT_PASSWORD: MYSQL.pass,
};

let passCount = 0;
const failures = [];
/** 断言登记：全部跑完再汇总（不短路，一次看到所有缺口）。 */
function check(name, ok, detail = '') {
  if (ok) {
    passCount += 1;
    console.log(`  ✔ ${name}`);
  } else {
    failures.push(name);
    console.error(`  ✘ ${name}${detail ? ` — ${detail}` : ''}`);
  }
}

/** MySQL 断言通道：mysql2 直连（mysql 解析 CHECKPOINT_JDBC_URL，本地/CI/转发环境同路径）。 */
async function dbQuery(sql) {
  const mysql = await import('mysql2/promise');
  const m = /jdbc:mysql:\/\/([^:/]+):(\d+)\/([^?]+)/.exec(MYSQL.url);
  if (!m) throw new Error(`CHECKPOINT_JDBC_URL 解析失败: ${MYSQL.url}`);
  const conn = await mysql.createConnection({
    host: m[1], port: Number(m[2]), database: m[3],
    user: MYSQL.user, password: MYSQL.pass, multipleStatements: true,
  });
  try {
    const [rows] = await conn.query(sql);
    return JSON.stringify(rows);
  } finally {
    await conn.end();
  }
}

async function dockerMySql(sql) {
  if (DRIVER === 'k8s') {
    // K8s 模式：经本地 mysql 客户端连 oaf-mysql 的 port-forward（startRuntime 已建立）
    return execSync(
      `mysql -h127.0.0.1 -P${K8S_MYSQL.localPort} -u${K8S_MYSQL.user} -p${K8S_MYSQL.pass} agent_framework_dsh 2>/dev/null`,
      { encoding: 'utf8', input: sql },
    );
  }
  return dbQuery(sql);
}

async function getJson(pathname) {
  const res = await fetch(`${BASE}${pathname}`);
  return { status: res.status, body: await res.json().catch(() => null) };
}

/** 读 SSE 流到终止条件；返回 {frames(含 seq), controlFrames, raw}。 */
async function readSse(pathname, { body, headers = {}, maxMs = 90_000 } = {}) {
  const res = await fetch(`${BASE}${pathname}`, {
    method: body ? 'POST' : 'GET',
    headers: { 'Content-Type': 'application/json', ...headers },
    body: body ? JSON.stringify(body) : undefined,
  });
  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let raw = '';
  const deadline = Date.now() + maxMs;
  while (Date.now() < deadline) {
    const { done, value } = await reader.read();
    if (done) break;
    raw += decoder.decode(value, { stream: true });
    if (raw.includes('\n\ndata:') || /AGENT_END|error|done/.test(raw.split('data:').pop() ?? '')) {
      // 终止判定：Agent End、error 或 done 之后短暂等待以吸收尾部帧
      const tail = raw.slice(-200);
      if (/"type":"(AGENT_END|error|done)"/.test(tail)) {
        await sleep(300);
        const { value: extra } = await reader.read().catch(() => ({ value: undefined }));
        if (extra) raw += decoder.decode(extra, { stream: true });
        break;
      }
    }
  }
  reader.cancel().catch(() => {});
  const frames = [];
  const controls = [];
  let seq = null;
  for (const line of raw.split('\n')) {
    if (line.startsWith('id: ')) seq = Number(line.slice(4));
    else if (line.startsWith('data: ')) {
      const payload = JSON.parse(line.slice(6));
      if (seq !== null) frames.push({ seq, payload });
      else controls.push(payload);
      seq = null;
    }
  }
  return { status: res.status, frames, controls, raw };
}

function waitFor(port, pathName, timeoutMs = 60_000) {
  const deadline = Date.now() + timeoutMs;
  return (async function poll() {
    while (Date.now() < deadline) {
      try {
        const res = await fetch(`http://127.0.0.1:${port}${pathName}`, { signal: AbortSignal.timeout(2000) });
        if (res.ok) return true;
      } catch { /* 未就绪 */ }
      await sleep(500);
    }
    return false;
  })();
}

/** k8s 驱动的 port-forward 进程（startRuntime 重建、stopRuntime 释放）。 */
let k8sPortForward = null;
/** k8s 模式下 oaf-mysql 的本地转发（e2e 断言查询用，全程存活）。 */
let k8sMySqlForward = null;

function stopPortForward() {
  if (k8sPortForward) {
    k8sPortForward.kill('SIGKILL');
    k8sPortForward = null;
  }
}

/** k8s 模式：把 oaf-mysql 转发到本地并等待可连（断言经本地 mysql 客户端查询集群库）。 */
async function ensureK8sMySqlForward() {
  if (k8sMySqlForward) return;
  k8sMySqlForward = spawn('kubectl', [
    '-n', 'agent-platform', 'port-forward', 'svc/oaf-mysql', `${K8S_MYSQL.localPort}:3306`,
  ], { stdio: ['ignore', 'ignore', 'pipe'] });
  // port-forward 就绪是异步的：轮询 TCP 直到可连（否则 truncate 会静默失败留下旧数据）
  const { createConnection } = await import('node:net');
  const deadline = Date.now() + 20_000;
  for (;;) {
    const ok = await new Promise((resolve) => {
      const sock = createConnection(K8S_MYSQL.localPort, '127.0.0.1');
      sock.once('connect', () => { sock.destroy(); resolve(true); });
      sock.once('error', () => { sock.destroy(); resolve(false); });
    });
    if (ok) return;
    if (Date.now() > deadline) throw new Error('oaf-mysql port-forward 20s 未就绪');
    await sleep(300);
  }
}

/** k8s 模式的集群运行时环境（manifests 已内嵌同值；此处仅供文档对照）。 */
const K8S_RUNTIME_ENV_NOTE = '容器 env 以 manifests/k8s-m0.yaml 为准（mock LLM 经宿主 172.20.0.1:18099 转发、oaf-mysql 独立库 agent_framework_dsh）';

async function startRuntime(label) {
  if (DRIVER === 'k8s') {
    // C19 平台集成：manifests 即部署形态（Deployment + Service + 探针），镜像已 kind load
    execSync(`kubectl apply -f ${K8S_MANIFEST}`, { encoding: 'utf8' });
    execSync('kubectl -n agent-platform wait --for=condition=available --timeout=120s deploy/oaf-dsh-agent', {
      encoding: 'utf8', stdio: ['ignore', 'ignore', 'pipe'],
    });
    stopPortForward();
    k8sPortForward = spawn('kubectl', ['-n', 'agent-platform', 'port-forward', 'svc/oaf-dsh-svc', `${PORT}:8100`], {
      stdio: ['ignore', 'ignore', 'pipe'],
    });
  } else if (DRIVER === 'docker') {
    // 自举 DSH_HOME：profiles 必须是软链本身（先 mkdir 成目录会让链接嵌套失效）
    const home = path.join(ROOT, '.dsh-home');
    mkdirSync(home, { recursive: true });
    if (!existsSync(path.join(home, 'profiles'))) {
      symlinkSync(path.join(ROOT, 'profiles'), path.join(home, 'profiles'), 'dir');
    }
    execSync(`docker rm -f ${CONTAINER} 2>/dev/null || true`, { encoding: 'utf8' });
    const envFlags = Object.entries(RUNTIME_ENV).map(([k, v]) => `-e ${k}='${v}'`).join(' ');
    execSync(`docker run -d --name ${CONTAINER} --network host ${envFlags} ${IMAGE}`, { encoding: 'utf8' });
  } else {
    const home = path.join(ROOT, '.dsh-home');
    mkdirSync(home, { recursive: true });
    if (!existsSync(path.join(home, 'profiles'))) {
      symlinkSync(path.join(ROOT, 'profiles'), path.join(home, 'profiles'), 'dir');
    }
    const child = spawn('./node_modules/.bin/dsh', ['--profile', 'oaf-web'], {
      cwd: ROOT,
      env: { ...process.env, DSH_HOME: home, ...RUNTIME_ENV },
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    child.stderr.on('data', (d) => console.error(`[dsh:${label}] ${d}`.trimEnd()));
    spawnHandle = { kind: 'spawn', child };
  }
  const up = await waitFor(PORT, '/health');
  if (!up) throw new Error(`dsh 运行时（${label}，driver=${DRIVER}）60s 未就绪`);
}

let spawnHandle = null;

function stopRuntime(_handle) {
  if (DRIVER === 'k8s') {
    // Pod 强杀（SIGKILL 等价）；Deployment 自动重建新副本——冷恢复即真实 K8s 故障接管路径
    try {
      execSync('kubectl -n agent-platform delete pod -l app=oaf-dsh-agent --force --grace-period=0 --ignore-not-found', {
        encoding: 'utf8', stdio: ['ignore', 'ignore', 'pipe'],
      });
    } finally {
      stopPortForward(); // 旧 Pod 的转发随删除断开；startRuntime 重建
    }
    return Promise.resolve();
  }
  if (DRIVER === 'docker') {
    // rm -f = SIGKILL 等价（冷恢复保真：进程即刻消失，无优雅退出）
    execSync(`docker rm -f ${CONTAINER}`, { encoding: 'utf8' });
    return Promise.resolve();
  }
  return new Promise((resolve) => {
    if (!spawnHandle) return resolve();
    const child = spawnHandle.child;
    child.removeAllListeners('exit');
    child.on('exit', resolve);
    child.kill('SIGKILL');
  });
}

async function mysqlCount(table, where = '') {
  const out = await dockerMySql(`SELECT COUNT(*) AS c FROM ${table} ${where}`);
  return Number(out.match(/(\d+)/)?.[1] ?? -1);
}

// ─── 主流程 ───────────────────────────────────────────────────────────────

let mock = null;
let code = 0;

try {
  // 1. mock LLM（AF 录制回放引擎，零密钥）
  if (!existsSync(path.join(AF_E2E, 'mock', 'llm-server.mjs'))) {
    throw new Error(`找不到 AF mock LLM：${AF_E2E}/mock/llm-server.mjs（设 AF_E2E_DIR 指向 agent-framework/e2e）`);
  }
  mock = spawn('node', [path.join(AF_E2E, 'mock', 'llm-server.mjs')], {
    env: { ...process.env, MOCK_LLM_PORT: String(MOCK_PORT) },
    stdio: ['ignore', 'ignore', 'pipe'],
  });
  mock.stderr.on('data', (d) => console.error(`[mock-llm] ${d}`.trimEnd()));
  if (!(await waitFor(MOCK_PORT, '/health', 15_000))) throw new Error('mock LLM 15s 未就绪');
  await fetch(`${MOCK_BASE}/reset`);

  // 2. 清库（全新会话状态）；k8s 模式先建 oaf-mysql 本地转发（断言查询通道）
  if (DRIVER === 'k8s') await ensureK8sMySqlForward();
  for (const t of ['session_event', 'session_user', 'dsh_session_log', 'dsh_session_header']) {
    try { await dockerMySql(`TRUNCATE ${t}`); } catch { /* 表未建（首跑）时由迁移自举 */ }
  }

  // 3. 起 dsh 运行时
  console.log(`▶ 启动 dsh oaf-web 运行时（driver=${DRIVER}）…`);
  await startRuntime('boot');
  console.log('▶ M0 冒烟断言');

  // 3.1 health/root 契约（AF §6.1/6.2 形状）
  const health = await getJson('/health');
  check('GET /health 契约', health.status === 200 && health.body.status === 'healthy'
    && typeof health.body.llm_configured === 'boolean' && health.body.tenant_prefix === 'oaf-dsh-agent',
  JSON.stringify(health.body));
  const root = await getJson('/');
  check('GET / 服务信息契约', root.status === 200 && root.body.protocols?.oaf === 'v0.8.0' && root.body.engine?.includes('dsh'));

  // 3.2 新会话单 turn：帧序列 + delta 拼接 == 录制件原文（R3 一致性锚）
  const fixture = JSON.parse(readFileSync(path.join(AF_E2E, 'mock', 'fixtures', 'llm', 'plain.json'), 'utf8'));
  const chat = await readSse('/threads/chat', {
    body: { message: '[E2E:plain] 你好', userId: 'alice' },
  });
  const types = chat.frames.map((f) => f.payload.type);
  const sid = chat.controls.find((c) => c.type === 'session_created')?.session_id;
  check('首帧为 session_created（控制帧无 seq）', Boolean(sid));
  // 期望帧序列从录制件推导：DELTA 帧数随录制 chunk 切分变化，其余帧型固定
  const fixtureChunks = fixture.calls[0].chunks.filter((c) => c !== '[DONE]')
    .filter((c) => (JSON.parse(c)?.choices?.[0]?.delta?.content ?? '') !== '');
  const expectedTypes = [
    'AGENT_START', 'MODEL_CALL_START', 'TEXT_BLOCK_START',
    ...fixtureChunks.map(() => 'TEXT_BLOCK_DELTA'),
    'TEXT_BLOCK_END', 'MODEL_CALL_END', 'AGENT_END',
  ];
  check('帧类型序列与 AF A.1 时序一致（DELTA 数 = 录制件 chunk 数）', JSON.stringify(types) === JSON.stringify(expectedTypes),
    types.join(' → '));
  const seqs = chat.frames.map((f) => f.seq);
  check('seq 从 1 连续单调', seqs.every((s, i) => s === i + 1), seqs.join(','));
  const deltas = chat.frames
    .filter((f) => f.payload.type === 'TEXT_BLOCK_DELTA')
    .map((f) => f.payload.delta).join('');
  const expectedText = fixture.calls[0].chunks
    .map((c) => (c === '[DONE]' ? '' : JSON.parse(c)?.choices?.[0]?.delta?.content ?? ''))
    .join('');
  check('TEXT_BLOCK_DELTA 拼接 == 录制件回放原文（R3 锚）', deltas === expectedText.trim(),
    `got=${JSON.stringify(deltas)} want=${JSON.stringify(expectedText.trim())}`);
  check('每帧带 replyId 与 id（e{seq}）', chat.frames.every((f) => f.payload.replyId === 'r1' && f.payload.id === `e${f.seq}`));
  const modelEnd = chat.frames.find((f) => f.payload.type === 'MODEL_CALL_END')?.payload;
  check('MODEL_CALL_END 携带 token 三字段（AF 契约）', modelEnd
    && Number.isFinite(modelEnd.inputTokens) && Number.isFinite(modelEnd.outputTokens) && Number.isFinite(modelEnd.totalTokens),
  JSON.stringify(modelEnd));

  // 3.3 MySQL 落事件（M0 验收硬指标）
  check('dsh 会话日志落 MySQL（session_log ≥ 1 行）', (await mysqlCount('dsh_session_log', `WHERE session_id='${sid}'`)) >= 1);
  check('dsh 会话头落 MySQL（session_header = 1 行）', (await mysqlCount('dsh_session_header', `WHERE session_id='${sid}'`)) === 1);
  const mirrorRows = await mysqlCount('session_event', `WHERE session_id='${sid}'`);
  check('AF 事件镜像落 MySQL（seq 连续无洞）', mirrorRows === chat.frames.length, `mirror=${mirrorRows} frames=${chat.frames.length}`);

  // 3.4 durable SSE：afterSeq 续传 + done 收尾（AF C3）
  const sub = await readSse(`/threads/${sid}/subscribe?afterSeq=5`);
  check('subscribe 从 afterSeq 严格续传', sub.frames.length === chat.frames.length - 5
    && sub.frames.every((f, i) => f.seq === 6 + i), JSON.stringify(sub.frames.map((f) => f.seq)));
  check('已完成 turn 追加 done 帧并关流', sub.controls.some((c) => c.type === 'done'));
  const subAll = await readSse(`/threads/${sid}/subscribe`);
  check('subscribe 从头回放 = chat 全量帧', subAll.frames.length === chat.frames.length);

  // 3.5 status 五态（AF C3）
  const st = await getJson(`/threads/${sid}/status`);
  check('status=completed + latest_event_seq', st.status === 200 && st.body.state === 'completed'
    && st.body.latest_event_seq === chat.frames.length && st.body.pending_confirm === '',
  JSON.stringify(st.body));
  const stIdle = await getJson(`/threads/${crypto.randomUUID()}/status`);
  check('未知会话 status=idle', stIdle.body.state === 'idle' && stIdle.body.latest_event_seq === 0);

  // 3.6 多用户隔离：不同 userId 会话互不可见（AF C7 最小面）
  await readSse('/threads/chat', { body: { message: '[E2E:plain] bob 的会话', userId: 'bob' } });
  const aliceThreads = await getJson('/threads?userId=alice');
  check('/threads 按 userId 过滤（C7）', aliceThreads.body.length === 1
    && aliceThreads.body[0].user_id === 'alice' && aliceThreads.body[0].session_id === sid,
  JSON.stringify(aliceThreads.body));

  // 3.7 M0 声明边界（诚实契约钉）
  const noMsg = await fetch(`${BASE}/threads/chat`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ userId: 'alice' }),
  });
  check('空请求体 → 400', noMsg.status === 400);
  const files = await fetch(`${BASE}/threads/chat`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ fileIds: ['不存在的id'] }),
  });
  // M1 已落地：fileIds 物化失败不阻断（该 id 无 file_asset 行 → 降级为纯文本 turn 正常完成）
  check('fileIds 未知 id 容错（M1 物化降级）', files.status === 200);

  // 3.8 冷恢复：杀运行时 → 重启 → 同 sessionId 续聊（C10+C11 持久化语义；
  //     k8s 驱动 = Pod 强杀由 Deployment 重建，即真实故障接管路径）
  console.log('▶ 冷恢复：强杀运行时并重启');
  await stopRuntime();
  await startRuntime('resume');
  const resumed = await readSse('/threads/chat', {
    body: { message: '[E2E:plain] 第二轮', userId: 'alice', sessionId: sid },
  });
  const resumedTypes = resumed.frames.map((f) => f.payload.type);
  check('重启后续聊无 session_created', !resumed.controls.some((c) => c.type === 'session_created'));
  check('冷恢复 turn 计数续接（replyId=r2）', resumedTypes[0] === 'AGENT_START' && resumed.frames[0].payload.replyId === 'r2',
  resumed.frames[0]?.payload?.replyId);
  check('冷恢复镜像 seq 续接（首帧 = 前段末帧 + 1）', resumed.frames[0]?.seq === chat.frames.length + 1,
  `first=${resumed.frames[0]?.seq} prevMax=${chat.frames.length}`);
  check('冷恢复 dsh 日志续写', (await mysqlCount('dsh_session_log', `WHERE session_id='${sid}'`)) > 16);
  check('冷恢复后 status=completed', (await getJson(`/threads/${sid}/status`)).body.state === 'completed');
} catch (err) {
  failures.push(`编排失败: ${err.message}`);
  console.error(err);
} finally {
  // k8s 部署实例保留运行（部署验证的交付状态）；仅释放 port-forward
  if (DRIVER === 'k8s') {
    stopPortForward();
    k8sMySqlForward?.kill('SIGKILL');
    k8sMySqlForward = null;
  } else {
    await stopRuntime().catch(() => {});
  }
  mock?.kill('SIGKILL');
}

console.log(`\n结果：${passCount} 通过 / ${failures.length} 失败`);
if (failures.length) {
  for (const f of failures) console.error(`  ✘ ${f}`);
  code = 1;
}
process.exit(code);
