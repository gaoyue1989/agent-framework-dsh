/**
 * OAF 对外面插件（C1/C2/C3/C6/C12）——与 agent-framework 对外 API 契约对齐
 * （api-frontend-sse.md / api-thread-spec / e2e api-core 套件为验收基准）。
 *
 * 端点全景（本文件实现）：
 * - 对话：POST /threads/chat（单次流 SSE）、GET /threads/{sid}/subscribe、/status
 * - 会话：GET /threads、GET /threads/{sid}、PATCH /threads/{sid}（title/model）、
 *   DELETE /threads/{sid}（级联清理）、GET /threads/{sid}/history、/llm-calls
 * - HITL：POST /threads/{sid}/confirm、/confirm-stream（approval 接缝桥接，H1-H6）
 * - 文件：POST /files/upload、GET /files/{fileId}（校验矩阵 / inline / RFC5987 / 代理回源）
 * - 元数据：/、/health、/metadata、/system-prompt、/.well-known/agent-card.json、
 *   /tools（includeInternal）、/mcp、/skills
 *
 * 架构对位见各节注释；已知差距见仓库 README（S5 userHeaders 注入、MCP Apps 代理、
 * A2A JSON-RPC、归档双源 history、models CRUD 等属 M1-M3 后续）。
 *
 * @module @oaf/oaf-server
 */
import { randomUUID } from 'node:crypto';
import { Service } from '@deepseek-ai/cordis';
import { createUserMessage } from '@deepseek-ai/dsh-llm';
import { admitEncodedImages } from '@deepseek-ai/dsh-attachment';
import { createPool, ensureDatabase, migrate, withTxn } from '@oaf/oaf-common';
import {
  createTurnSerializer,
  settledEventFrames,
  liveStreamFrames,
  controlFrames,
  textOfBlocks,
  summarizeCall,
  previewText,
} from './frames.js';

export const name = 'oaf-server';
/** 服务依赖：webserver（路由）+ agents（协议驱动）+ sessionPersistence（resume/history）+ oafLoader（包元数据）+ oafTools（内置工具）。 */
export const inject = ['webServer', 'agents', 'sessionPersistence', 'oafLoader', 'oafTools', 'tools', 'attachments'];

const MIGRATIONS = [
  {
    version: 1,
    name: 'oaf-server-v1',
    sql: [
      `CREATE TABLE IF NOT EXISTS session_event (
         session_id VARCHAR(255) NOT NULL,
         seq BIGINT NOT NULL,
         event_type VARCHAR(64) NOT NULL,
         reply_id VARCHAR(64) NOT NULL DEFAULT '',
         payload JSON NOT NULL,
         created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
         PRIMARY KEY (session_id, seq),
         KEY idx_session_created (session_id, created_at)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
      `CREATE TABLE IF NOT EXISTS session_user (
         session_id VARCHAR(255) NOT NULL PRIMARY KEY,
         user_id VARCHAR(255) NOT NULL,
         title VARCHAR(255) NOT NULL DEFAULT '',
         model VARCHAR(128) NOT NULL DEFAULT '',
         created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
         updated_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3),
         KEY idx_user_updated (user_id, updated_at)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    ],
  },
  {
    version: 2,
    name: 'oaf-server-hitl-v2',
    sql: [
      // AF confirm_context 同构：CAS 防重复消费（consumed 0→1）
      `CREATE TABLE IF NOT EXISTS confirm_context (
         session_id VARCHAR(255) NOT NULL PRIMARY KEY,
         tool_calls_json MEDIUMTEXT NOT NULL,
         reply_id VARCHAR(64) NOT NULL DEFAULT '',
         decision VARCHAR(16) NOT NULL DEFAULT '',
         created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
         consumed TINYINT(1) NOT NULL DEFAULT 0
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
      // LLM 调用记录（/threads/{sid}/llm-calls 数据源；llm/stream 观测写入）
      `CREATE TABLE IF NOT EXISTS llm_call (
         id BIGINT AUTO_INCREMENT PRIMARY KEY,
         session_id VARCHAR(255) NOT NULL,
         call_id VARCHAR(64) NOT NULL,
         model VARCHAR(128) NOT NULL DEFAULT '',
         request_json MEDIUMTEXT NOT NULL,
         created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
         KEY idx_session (session_id, created_at)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
      `ALTER TABLE session_user ADD COLUMN title_source VARCHAR(16) NOT NULL DEFAULT ''`,
    ],
  },
  {
    version: 3,
    name: 'oaf-server-llm-calls-v3',
    sql: [
      'ALTER TABLE llm_call ADD COLUMN response_json MEDIUMTEXT NULL',
      'ALTER TABLE llm_call ADD COLUMN created_ms BIGINT NULL',
    ],
  },
];

const sseHeaders = () => ({
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache',
  Connection: 'keep-alive',
  'X-Accel-Buffering': 'no',
});

/** SSE 写通道：chat 主流与 subscribe/confirm-stream 追赶流共用；断连只摘除不终止 turn。 */
class SseWriter {
  constructor(res, heartbeatMs) {
    this.res = res;
    this.alive = true;
    this.tail = false;
    this.lastWrittenSeq = 0;
    this.heartbeat = heartbeatMs > 0 ? setInterval(() => this.raw(':hb\n\n'), heartbeatMs) : null;
    this.heartbeat?.unref?.();
  }

  detach() {
    this.alive = false;
    if (this.heartbeat) clearInterval(this.heartbeat);
  }

  raw(text) {
    if (!this.alive) return;
    try { this.res.write(text); } catch { this.alive = false; }
  }

  control(frame) { this.raw(`data: ${JSON.stringify(frame)}\n\n`); }

  event(seq, payload) {
    if (seq <= this.lastWrittenSeq) return;
    this.lastWrittenSeq = seq;
    this.raw(`id: ${seq}\ndata: ${JSON.stringify(payload)}\n\n`);
  }

  end() {
    if (!this.alive) return;
    this.detach();
    try { this.res.end(); } catch { /* 客户端已断开 */ }
  }
}

/** 事件镜像（MySQL）——AF SessionEventStore 同构。 */
class EventMirrorStore {
  constructor(pool) {
    this.pool = pool;
    this.cursors = new Map();
  }

  async seedCursor(sessionId) {
    if (this.cursors.has(sessionId)) return;
    const [rows] = await this.pool.query(
      'SELECT COALESCE(MAX(seq), 0) AS max_seq FROM session_event WHERE session_id = ?',
      [sessionId],
    );
    this.cursors.set(sessionId, Number(rows[0]?.max_seq ?? 0));
  }

  async append(sessionId, eventType, replyId, payload) {
    await this.seedCursor(sessionId);
    const seq = this.cursors.get(sessionId) + 1;
    await this.pool.query(
      'INSERT INTO session_event (session_id, seq, event_type, reply_id, payload) VALUES (?, ?, ?, ?, ?)',
      [sessionId, seq, eventType, replyId ?? '', JSON.stringify(payload)],
    );
    this.cursors.set(sessionId, seq);
    return seq;
  }

  async readAfter(sessionId, afterSeq, limit = 20000) {
    const [rows] = await this.pool.query(
      'SELECT seq, payload FROM session_event WHERE session_id = ? AND seq > ? ORDER BY seq ASC LIMIT ?',
      [sessionId, afterSeq, Number(limit)],
    );
    return rows.map((r) => ({ seq: Number(r.seq), payload: r.payload }));
  }

  async latest(sessionId) {
    const [rows] = await this.pool.query(
      'SELECT seq, event_type, reply_id FROM session_event WHERE session_id = ? ORDER BY seq DESC LIMIT 1',
      [sessionId],
    );
    const row = rows[0];
    return row ? { seq: Number(row.seq), type: row.event_type, replyId: row.reply_id } : undefined;
  }

  async upsertSessionUser(sessionId, userId) {
    await this.pool.query(
      `INSERT INTO session_user (session_id, user_id) VALUES (?, ?)
       ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), updated_at = CURRENT_TIMESTAMP(3)`,
      [sessionId, userId],
    );
  }

  async renameSession(sessionId, title, source) {
    await this.pool.query(
      'UPDATE session_user SET title = ?, title_source = ?, updated_at = CURRENT_TIMESTAMP(3) WHERE session_id = ?',
      [title, source ?? 'manual', sessionId],
    );
  }

  async getSessionUser(sessionId) {
    const [rows] = await this.pool.query(
      'SELECT session_id, user_id, title, title_source, updated_at FROM session_user WHERE session_id = ?',
      [sessionId],
    );
    return rows[0];
  }

  async listSessions(userId) {
    const filtered = Boolean(userId);
    const [rows] = await this.pool.query(
      `SELECT session_id, user_id, title, updated_at FROM session_user ${filtered ? 'WHERE user_id = ?' : ''} ORDER BY updated_at DESC LIMIT 200`,
      filtered ? [userId] : [],
    );
    return rows.map((r) => ({
      session_id: r.session_id,
      thread_id: String(r.session_id).includes(':')
        ? String(r.session_id).split(':').slice(1).join(':')
        : r.session_id,
      user_id: r.user_id,
      title: r.title ?? '',
      updated_at: r.updated_at,
    }));
  }

  async deleteSession(sessionId) {
    await withTxn(this.pool, async (conn) => {
      for (const t of ['session_event', 'session_user', 'confirm_context', 'llm_call', 'file_asset']) {
        await conn.query(`DELETE FROM ${t} WHERE session_id = ?`, [sessionId]);
      }
      await conn.query('DELETE FROM dsh_session_log WHERE session_id = ?', [sessionId]);
      await conn.query('DELETE FROM dsh_session_header WHERE session_id = ?', [sessionId]);
    });
    this.cursors.delete(sessionId);
  }
}

/** 上传校验（AF F8 矩阵：扩展名黑名单 / MIME 白名单 / 大小 / 空文件）。 */
const DANGEROUS_EXT = new Set(['exe', 'bat', 'cmd', 'ps1', 'vbs', 'js', 'wsf', 'msi', 'scr', 'com', 'dll', 'sys', 'reg', 'inf', 'hta', 'cpl', 'msp', 'mst']);
const MIME_RULES = [
  { test: (m) => m.startsWith('image/'), exts: ['png', 'jpg', 'jpeg', 'gif', 'webp', 'bmp', 'svg'] },
  { test: (m) => m.startsWith('text/'), exts: ['txt', 'md', 'csv', 'html', 'htm', 'json', 'xml', 'yaml', 'yml', 'log'] },
  { test: (m) => m === 'application/pdf', exts: ['pdf'] },
  { test: (m) => m.includes('openxmlformats') || m.startsWith('application/vnd.ms-'), exts: ['xlsx', 'docx', 'pptx', 'xls', 'doc', 'ppt'] },
];
const MAX_UPLOAD_BYTES = 20 * 1024 * 1024;

/** JSON 容错解析（llm-calls 返回契约：坏行不给 500）。 */
function safeParse(text) {
  try { return JSON.parse(text ?? '{}'); } catch { return {}; }
}

export function validateUpload(fileName, mime, size) {
  if (size === 0) return { status: 400, error: 'no_file_uploaded' };
  if (size > MAX_UPLOAD_BYTES) return { status: 413, error: 'file_too_large' };
  const ext = fileName.split('.').pop()?.toLowerCase() ?? '';
  if (DANGEROUS_EXT.has(ext)) return { status: 400, error: 'extension_mime_mismatch' };
  const allowed = MIME_RULES.some((r) => r.test(mime));
  if (!allowed) return { status: 415, error: 'unsupported_file_type' };
  const rule = MIME_RULES.find((r) => r.test(mime));
  if (!rule.exts.includes(ext)) return { status: 400, error: 'extension_mime_mismatch' };
  return null;
}

/**
 * OAF 对外面服务。
 */
class OafServerService extends Service {
  static inject = ['webServer', 'agents', 'sessionPersistence', 'oafLoader', 'oafTools', 'tools', 'attachments'];

  constructor(ctx, config) {
    super(ctx, 'oafServer');
    this.config = { ...OafServerService.defaults(), ...(config ?? {}) };
    this.pool = undefined;
    this.store = undefined;
    this.runtimes = new Map();
    /** HITL 挂起：callId → { resolve(decision), sessionId }。 */
    this.askPending = new Map();
    /** file_ready 待_flush 队列：sessionId → asset[]。 */
    this.fileReadyQueue = new Map();
    /** UI 代理已确认旁路（一次性）：qualified tool name → true（pre-execute 放行一次）。 */
    this.uiProxyBypass = new Set();
    /** 裸名别名透传的内层放行（loader 注册的别名已在裸名层完成审批）。 */
    this.rawAliasBypass = new Set();
  }

  static defaults() {
    return {
      agentName: process.env.AGENT_NAME ?? 'oaf-dsh-agent',
      slug: process.env.AGENT_SLUG ?? process.env.AGENT_NAME ?? 'oaf-dsh-agent',
      version: process.env.AGENT_VERSION ?? '1.0.0',
      description: process.env.AGENT_DESCRIPTION ?? '',
      engine: 'DeepSeek Harness (dsh)',
      systemProvider: process.env.LLM_PROVIDER_ROUTE ?? 'oaf-system',
      systemModel: process.env.LLM_MODEL_ID ?? '',
      defaultUserId: 'debug-user',
      queueTimeoutSeconds: Number(process.env.AGENT_QUEUE_TIMEOUT_SECONDS ?? 120),
      waitingFrameIntervalSeconds: 15,
      heartbeatIntervalSeconds: 20,
    };
  }

  async [Service.init]() {
    const cfg = this.config;
    const jdbcUrl = process.env.CHECKPOINT_JDBC_URL;
    if (!jdbcUrl) throw new Error('oaf-server: 缺少 CHECKPOINT_JDBC_URL');
    const poolOpts = { jdbcUrl, username: process.env.CHECKPOINT_USERNAME, password: process.env.CHECKPOINT_PASSWORD };
    await ensureDatabase(poolOpts);
    this.pool = createPool(poolOpts);
    await migrate(this.pool, MIGRATIONS, 'oaf_schema_version_oaf_server');
    this.store = new EventMirrorStore(this.pool);
    this.installListeners();
    this.installHitlBridge();
    // 裸名别名（AF 契约名）：MCP client 工具注册完成后兜底执行
    try {
      const stats = this.ctx.oafLoader.registerRawAliases();
      console.warn('[oaf-server] 裸名别名注册:', JSON.stringify(stats));
    } catch (err) { console.error('[oaf-server] 裸名别名注册失败:', err); }
    // S5：sessionId → userId 解析器（别名直调 userHeaders 注入用）
    this.ctx.oafLoader.userIdResolver = (sessionId) => this.runtimes.get(String(sessionId))?.userId ?? null;
    this.mountRoutes(this.ctx.webServer);
  }

  runtimeOf(sessionId) {
    let rt = this.runtimes.get(sessionId);
    if (!rt) {
      rt = {
        sessionId,
        userId: undefined,
        handle: undefined,
        agent: undefined,
        busy: false,
        turnOpen: false,
        asking: false,
        turnState: undefined,
        lastReplyId: '',
        lastAssistantText: '',
        writers: new Set(),
        waiters: [],
        writeChain: undefined,
      };
      this.runtimes.set(sessionId, rt);
    }
    return rt;
  }

  installListeners() {
    const disposers = [
      this.ctx.on('session/event', (session, event) => {
        this.onSessionEvent(String(session.header.id), event).catch((err) => console.error('[oaf-server] session/event:', err));
      }),
      this.ctx.on('agent/assistant-stream', ({ agent, frame }) => {
        this.onLiveFrame(String(agent.id), frame).catch((err) => console.error('[oaf-server] assistant-stream:', err));
      }),
      this.ctx.on('agent/error', ({ agent, error }) => {
        this.onAgentError(String(agent.id), error).catch(() => {});
      }),
      // 会话标题（dsh session-title 服务产出）→ session_user.title（手动命名 sticky）
      this.ctx.on('session/title', (session, data) => this.onSessionTitle(String(session?.header?.id ?? ''), data)),
      // 交付物就绪 → file_ready 帧（在 present_* 工具 post-execute 后 flush，保证帧序在结果之后）
      this.ctx.on('oaf/file-ready', ({ sessionId, asset }) => {
        const q = this.fileReadyQueue.get(sessionId) ?? [];
        q.push(asset);
        this.fileReadyQueue.set(sessionId, q);
      }),
      this.ctx.on('tools/post-execute', async (exec, result, next) => {
        const out = await next();
        if (exec?.agent && ['present_file', 'present_url'].includes(exec.name)) {
          this.flushFileReady(String(exec.agent.id)).catch(() => {});
        }
        return out;
      }),
    ];
    this.ctx.effect(() => () => disposers.forEach((d) => d()));
  }

  async flushFileReady(sessionId) {
    const q = this.fileReadyQueue.get(sessionId);
    if (!q?.length) return;
    this.fileReadyQueue.set(sessionId, []);
    const rt = this.runtimes.get(sessionId);
    const frames = q.map((asset) => ({ type: 'file_ready', ...asset }));
    if (rt) await this.emitEventFrames(rt, frames, rt.turnState?.replyId ?? '');
  }

  async onSessionTitle(sessionId, data) {
    if (!sessionId) return;
    const rt = this.runtimes.get(sessionId);
    if (rt?.manualTitle) return;
    const title = String(data?.title ?? '').trim();
    if (!title) return;
    await this.store.renameSession(sessionId, title.slice(0, 255), 'auto').catch(() => {});
  }

  emitEventFrames(rt, frames, replyId) {
    const prev = rt.writeChain ?? Promise.resolve();
    const run = prev.then(async () => {
      await this.store.seedCursor(rt.sessionId);
      for (const frame of frames) {
        const seq = (this.store.cursors.get(rt.sessionId) ?? 0) + 1;
        const payload = { ...frame, id: `e${seq}` };
        await this.store.append(rt.sessionId, frame.type, replyId, payload);
        for (const w of rt.writers) w.event(seq, payload);
      }
    });
    rt.writeChain = run.then(() => undefined, (err) => console.error('[oaf-server] 帧持久化失败:', err));
    return run;
  }

  async onSessionEvent(sessionId, event) {
    const rt = this.runtimes.get(sessionId);
    if (!rt) return;
    if (event.type === 'turn/start') {
      rt.turnOpen = true;
      rt.turnState = createTurnSerializer({
        sessionId,
        turn: event.data.turn,
        agentName: this.loaderAgentName(),
      });
    }
    const s = rt.turnState;
    if (s) {
      const frames = settledEventFrames(s, event);
      if (frames.length) await this.emitEventFrames(rt, frames, s.replyId);
    }
    if (event.type === 'user/message') {
      // A5/A6 契约：llm-calls 的 request.messages 收真实用户消息（过滤 dsh 运行时快照）
      const t = textOfBlocks(event.data?.content);
      if (t && !t.startsWith('Current runtime context')) {
        rt.userMsgBuffer = [...(rt.userMsgBuffer ?? []), t.slice(0, 2000)].slice(-20);
      }
    }
    if (event.type === 'assistant/message') {
      const text = textOfBlocks(event.data?.message?.content);
      if (text) rt.lastAssistantText = text;
      // llm-calls 事件化记录（A5：call_id 带 call- 前缀、timestamp 毫秒、USER 大写、usage 三键）
      const u = event.data?.usage ?? {};
      const inTok = Number(u.inputTokens ?? 0);
      const outTok = Number(u.outputTokens ?? 0);
      this.pool.query(
        'INSERT INTO llm_call (session_id, call_id, model, request_json, response_json, created_ms) VALUES (?, ?, ?, ?, ?, ?)',
        [sessionId, `call-${randomUUID()}`, String(event.data?.message?.source?.provider ?? ''),
         JSON.stringify({ messages: (rt.userMsgBuffer ?? []).map((c) => ({ role: 'USER', content: c })) }),
         JSON.stringify({ usage: { input_tokens: inTok, output_tokens: outTok, total_tokens: Number(u.totalTokens ?? inTok + outTok) } }),
         Number(event.time ?? Date.now())],
      ).catch(() => {});
    }
    if (event.type === 'turn/end') {
      rt.turnOpen = false;
      rt.lastReplyId = rt.turnState?.replyId ?? rt.lastReplyId;
      // 标题兜底：dsh session-title 未产出时用首轮助手回复（去尾标点，AF 同款语义）
      if (!rt.manualTitle) await this.ensureAutoTitle(rt);
      for (const w of [...rt.writers]) {
        if (w.tail) w.control(controlFrames.done());
        w.end();
        rt.writers.delete(w);
      }
      this.releaseLease(rt);
    }
  }

  async ensureAutoTitle(rt) {
    const row = await this.store.getSessionUser(rt.sessionId).catch(() => undefined);
    if (!row || row.title) return;
    const raw = (rt.lastAssistantText || '').trim();
    if (!raw) return;
    const title = raw.replace(/[。！？.!?\s]+$/g, '').slice(0, 60);
    if (title) await this.store.renameSession(rt.sessionId, title, 'auto').catch(() => {});
  }

  loaderAgentName() {
    return this.ctx.oafLoader?.agentName || this.config.agentName;
  }

  async onLiveFrame(sessionId, frame) {
    const rt = this.runtimes.get(sessionId);
    const s = rt?.turnState;
    if (!s) return;
    const frames = liveStreamFrames(s, frame);
    if (frames.length) await this.emitEventFrames(rt, frames, s.replyId);
  }

  async onAgentError(sessionId, error) {
    const rt = this.runtimes.get(sessionId);
    if (!rt) return;
    const message = error instanceof Error ? error.message : String(error);
    for (const w of [...rt.writers]) {
      w.control(controlFrames.error(message));
      w.end();
      rt.writers.delete(w);
    }
    rt.turnOpen = false;
    this.releaseLease(rt);
  }

  acquireLease(rt, writer, { queueTimeoutMs, waitingFrameMs }) {
    if (!rt.busy) {
      rt.busy = true;
      return Promise.resolve(true);
    }
    return new Promise((resolve) => {
      const waiter = {};
      const timer = waitingFrameMs > 0 && writer
        ? setInterval(() => writer.control(controlFrames.waiting()), waitingFrameMs)
        : null;
      const cleanup = () => {
        if (timer) clearInterval(timer);
        clearTimeout(waiter.timeout);
        const i = rt.waiters.indexOf(waiter);
        if (i >= 0) rt.waiters.splice(i, 1);
      };
      waiter.timeout = setTimeout(() => {
        cleanup();
        if (writer) {
          writer.control(controlFrames.error(
            `turn_in_progress: session '${rt.sessionId}' has an active turn and queue timeout reached`,
          ));
          writer.end();
        }
        resolve(false);
      }, queueTimeoutMs);
      waiter.grant = () => { cleanup(); resolve(true); };
      rt.waiters.push(waiter);
    });
  }

  /** turn 启动共用（chat SSE / A2A send/stream）：租约 + 附加 agent + followup。 */
  async startTurn(sessionId, userId, text, writer = null) {
    const rt = this.runtimeOf(sessionId);
    rt.userId = userId;
    if (rt.asking) {
      return { ok: false, rt, error: `turn_pending_confirm: session '${sessionId}' is in ASKING state（先走确认流程再发新消息）` };
    }
    const ok = await this.acquireLease(rt, writer, {
      queueTimeoutMs: (this.config.queueTimeoutSeconds ?? 120) * 1000,
      waitingFrameMs: 0,
    });
    if (!ok) {
      return { ok: false, rt, error: `turn_in_progress: session '${sessionId}' has an active turn and queue timeout reached` };
    }
    if (writer) rt.writers.add(writer);
    try {
      await this.store.upsertSessionUser(sessionId, userId);
      const agent = await this.attachAgent(rt);
      agent.followup(createUserMessage({
        content: [{ type: 'text', text }],
        source: { kind: 'user' },
      }));
      return { ok: true, rt };
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      if (writer) {
        writer.control(controlFrames.error(message));
        writer.end();
      }
      rt.writers.delete(writer);
      rt.turnOpen = false;
      this.releaseLease(rt);
      return { ok: false, rt, error: message };
    }
  }

  releaseLease(rt) {
    const next = rt.waiters.shift();
    if (next) next.grant();
    else rt.busy = false;
  }

  async attachAgent(rt) {
    if (rt.agent) return rt.agent;
    const cwd = await this.workspaceFor(rt.sessionId);
    const persisted = await this.ctx.sessionPersistence.stat(rt.sessionId).catch(() => undefined);
    const model = this.config.systemModel || process.env.LLM_MODEL_ID;
    const agentOptions = model
      ? { provider: this.config.systemProvider || 'oaf-system', model }
      : undefined;
    const handle = persisted
      ? await this.ctx.agents.resume({ resumeSessionId: rt.sessionId, agentOptions })
      : await this.ctx.agents.create({ sessionId: rt.sessionId, meta: { cwd }, agentOptions });
    rt.handle = handle;
    rt.agent = handle.agent;
    return rt.agent;
  }

  async workspaceFor(sessionId) {
    const { mkdir } = await import('node:fs/promises');
    const path = await import('node:path');
    const os = await import('node:os');
    const root = process.env.AGENT_WORKSPACE_DIR ?? path.join(os.tmpdir(), 'oaf-dsh-workspaces');
    const dir = path.join(root, String(sessionId).replace(/[^\w.:-]/g, '_'));
    await mkdir(dir, { recursive: true });
    return dir;
  }

  // ── HITL 桥（C6，H1-H6）：pre-execute ask → approval/request 挂起 → /confirm 恢复 ──

  installHitlBridge() {
    const loader = () => this.ctx.oafLoader;
    const isAskTool = (name) => {
      const l = loader();
      if (!l) return false;
      if (l.permission.ask.has(name)) return true;
      for (const serverName of l.mcpServers) {
        const prefix = `mcp__${serverName}__`;
        if (name.startsWith(prefix) && l.permission.ask.has(name.slice(prefix.length))) return true;
      }
      return false;
    };
    const disposers = [
      // ask 规则：变更类工具经审批接缝（approval/request）挂起
      this.ctx.on('tools/pre-execute', async (exec, next) => {
        if (this.uiProxyBypass.delete(exec.name) || this.rawAliasBypass.delete(exec.name)) return next();
        if (!isAskTool(exec.name)) return next();
        return { kind: 'ask', reason: '需要人工确认后执行' };
      }),

      // 审批应答者：permission_ask 落库 + 帧广播 + 流收口 + 挂起等待 /confirm
      this.ctx.on('approval/request', async (req, next) => {
        const sessionId = String(req.agent?.id ?? '');
        const rt = this.runtimes.get(sessionId);
        if (!rt) return next();
        const callId = req.callId ? String(req.callId) : randomUUID();
        const toolName = String(req.toolName ?? 'tool');
        const replyId = rt.turnState?.replyId ?? rt.lastReplyId ?? '';
        // confirm_context 落库（AF CAS：session 唯一挂起行）
        await this.pool.query(
          `INSERT INTO confirm_context (session_id, tool_calls_json, reply_id)
           VALUES (?, ?, ?) ON DUPLICATE KEY UPDATE tool_calls_json = VALUES(tool_calls_json), reply_id = VALUES(reply_id), consumed = 0, decision = ''`,
          [sessionId, JSON.stringify([{ tool_call_id: callId, name: toolName }]), replyId],
        ).catch(() => {});
        // input 从该 turn 已结算的 tool/call 帧取（前端确认卡重建用）
        const input = await this.lookupToolInput(rt, callId);
        // permission_ask 帧：持久化 + 广播（api §9.8：带 id 的真实事件帧，客户端 terminal）
        await this.emitEventFrames(rt, [{
          type: 'permission_ask',
          tool_calls: [{ tool_call_id: callId, name: toolName, input }],
          reply_id: replyId,
        }], replyId);
        // 恢复段兜底摘要登记 + 流收口 + 租约让出 + ASKING 态
        rt.turnState?.resumeSummaries.add(callId);
        rt.asking = true;
        for (const w of [...rt.writers]) {
          w.end();
          rt.writers.delete(w);
        }
        this.releaseLease(rt);
        const decision = await new Promise((resolve) => {
          this.askPending.set(callId, {
            resolve: resolve,
            sessionId,
            toolName,
          });
        });
        if (decision === 'denied') rt.turnState?.deniedCalls.add(callId);
        rt.asking = false;
        return decision === 'approved' ? 'allowed-once' : 'rejected';
      }),
    ];
    this.ctx.effect(() => () => disposers.forEach((d) => d()));
  }

  async lookupToolInput(rt, callId) {
    try {
      const rows = await this.store.readAfter(rt.sessionId, 0);
      for (const row of rows) {
        if (row.payload?.type === 'TOOL_CALL_START' && row.payload.toolCallId === callId) {
          return row.payload.input ?? {};
        }
      }
    } catch { /* 查不到给空对象 */ }
    return {};
  }

  /** confirm_context 消费（CAS）：返回 {ok, row} 或 {error, status}。 */
  async consumeConfirm(sessionId, results) {
    const [rows] = await this.pool.query(
      'SELECT session_id, tool_calls_json, reply_id, consumed FROM confirm_context WHERE session_id = ?',
      [sessionId],
    );
    const row = rows[0];
    if (!row || Number(row.consumed) === 1) {
      return row
        ? { error: 'confirm_already_consumed', status: 409 }
        : { error: 'confirm_context_not_found', status: 404 };
    }
    const calls = JSON.parse(row.tool_calls_json ?? '[]');
    for (const r of results ?? []) {
      if (!calls.some((c) => c.tool_call_id === r.tool_call_id)) {
        return { error: 'confirm_context_not_found', status: 404 };
      }
    }
    const decision = (results ?? []).some((r) => r.confirmed) ? 'approved' : 'denied';
    const [upd] = await this.pool.query(
      'UPDATE confirm_context SET consumed = 1, decision = ? WHERE session_id = ? AND consumed = 0',
      [decision, sessionId],
    );
    if (upd.affectedRows === 0) return { error: 'confirm_already_consumed', status: 409 };
    return { ok: true, row, decision };
  }

  /** 恢复挂起的审批应答。 */
  resolveAsk(sessionId, decision) {
    for (const [callId, pending] of this.askPending) {
      if (pending.sessionId === sessionId) {
        this.askPending.delete(callId);
        pending.resolve(decision);
      }
    }
  }

  // ── history 构建（从 dsh 会话事件日志派生，设计 §4.3 事件溯源）──

  async buildHistory(sessionId) {
    let events = [];
    try {
      const handle = await this.ctx.sessionPersistence.open(sessionId, 'read');
      try {
        const { events: evts } = await handle.read(0);
        events = evts;
      } finally {
        await handle.close().catch(() => {});
      }
    } catch {
      events = [];
    }
    const messages = [];
    let pendingCalls = [];
    const callResultIndex = new Map();
    for (const event of events) {
      const d = event.data ?? {};
      if (event.type === 'user/message') {
        const text = textOfBlocks(d.content);
        // 过滤 dsh 合成的运行时上下文快照（非真实用户消息）
        if (text.startsWith('Current runtime context') || text.includes('Current DSH file policy')) continue;
        messages.push({ role: 'user', content: text });
      } else if (event.type === 'tool/call') {
        pendingCalls.push({ id: d.callId, name: d.name, argsRaw: String(d.arguments ?? '') });
      } else if (event.type === 'tool/result') {
        const callId = d.message?.toolCallId;
        callResultIndex.set(callId, d);
      } else if (event.type === 'assistant/message') {
        const toolCalls = pendingCalls.map((c) => {
          const r = callResultIndex.get(c.id);
          let args = {};
          try { args = JSON.parse(c.argsRaw || '{}'); } catch { /* 容错 */ }
          const outputText = r ? textOfBlocks(r.message?.content) : '';
          // 拒绝态：审批拒绝（dsh 文案 "the user rejected tool"）或运行时降级拒绝（"已拒绝"）
          const denied = r?.message?.isError === true && (outputText.includes('the user rejected tool') || outputText.includes('已拒绝'));
          return { id: c.id, name: c.name, arguments: args, state: r ? (denied ? 'denied' : (r.message?.isError ? 'error' : 'success')) : 'running', output: outputText };
        });
        pendingCalls = [];
        messages.push({ role: 'agent', content: textOfBlocks(d.message?.content), ...(toolCalls.length ? { tool_calls: toolCalls } : {}) });
      }
    }
    if (pendingCalls.length) {
      for (const c of pendingCalls) {
        const r = callResultIndex.get(c.id);
        let args = {};
        try { args = JSON.parse(c.argsRaw || '{}'); } catch { /* 容错 */ }
        const outputText = r ? textOfBlocks(r.message?.content) : '';
        const denied = r?.isError && (outputText.includes('the user rejected tool') || outputText.includes('已拒绝'));
        messages.push({
          role: 'agent', content: '',
          tool_calls: [{ id: c.id, name: c.name, arguments: args, state: r ? (denied ? 'denied' : (r.message?.isError ? 'error' : 'success')) : 'running', output: outputText }],
        });
      }
    }
    return messages;
  }

  async pendingConfirmOf(sessionId) {
    const [rows] = await this.pool.query(
      'SELECT tool_calls_json, reply_id, created_at, consumed, decision FROM confirm_context WHERE session_id = ?',
      [sessionId],
    );
    const row = rows[0];
    if (!row || Number(row.consumed) === 1) return null;
    let calls = [];
    try { calls = JSON.parse(row.tool_calls_json ?? '[]'); } catch { /* 容错 */ }
    return {
      reply_id: row.reply_id ?? '',
      tools: calls.map((c) => ({ tool_call_id: c.tool_call_id, name: c.name, input: c.input ?? {} })),
      created_at: row.created_at,
      source: 'agent_state',
    };
  }

  async filesOf(sessionId) {
    const [rows] = await this.pool.query(
      "SELECT id, file_name, mime_type, size FROM file_asset WHERE session_id = ? AND origin = 'generated' ORDER BY created_at ASC",
      [sessionId],
    );
    return rows.map((r) => ({ file_id: r.id, file_name: r.file_name, mime_type: r.mime_type, size: Number(r.size) }));
  }

  // ── 路由 ──

  mountRoutes(web) {
    const cfg = this.config;
    const register = (route) => this.ctx.effect(() => web.register(route));

    register({ kind: 'exact', path: '/health', handler: async (req, res) => {
      this.json(res, 200, {
        status: 'healthy',
        agent: this.loaderAgentName(),
        slug: this.ctx.oafLoader?.slug || cfg.slug,
        version: this.ctx.oafLoader?.frontmatter?.version || cfg.version,
        llm_configured: Boolean(process.env.LLM_API_KEY && process.env.LLM_MODEL_ID),
        engine: cfg.engine,
        tenant_prefix: this.ctx.oafLoader?.slug || cfg.slug,
      });
    } });

    register({ kind: 'exact', path: '/', handler: async (req, res) => {
      if (req.method === 'POST') return this.handleA2A(req, res); // A2A v1.0.0 JSON-RPC（C8）
      this.json(res, 200, {
        agent: this.loaderAgentName(),
        slug: this.ctx.oafLoader?.slug || cfg.slug,
        version: this.ctx.oafLoader?.frontmatter?.version || cfg.version,
        description: this.ctx.oafLoader?.frontmatter?.description || cfg.description,
        protocols: { oaf: 'v0.8.0' },
        engine: cfg.engine,
        endpoints: {
          agent_card: '/.well-known/agent-card.json',
          health: '/health',
          metadata: '/metadata',
          threads: '/threads',
        },
      });
    } });

    register({ kind: 'exact', path: '/metadata', handler: async (req, res) => {
      const loader = this.ctx.oafLoader;
      let toolNames = [];
      try { toolNames = (this.ctx.tools.schemas() ?? []).map((s) => s.name); } catch { /* 服务未就绪 */ }
      const mcpTools = loader?.mcpToolView(toolNames) ?? [];
      this.json(res, 200, {
        agent: this.loaderAgentName(),
        slug: loader?.slug || cfg.slug,
        version: loader?.frontmatter?.version || cfg.version,
        description: loader?.frontmatter?.description || cfg.description,
        protocols: { oaf: 'v0.8.0' },
        oaf: { tools: mcpTools.map((t) => t.name), skills: loader?.skills?.length ?? 0, mcp: loader?.mcpServers?.length ?? 0, sub_agents: 0 },
        endpoints: { health: '/health', metadata: '/metadata', threads: '/threads' },
        engine: cfg.engine,
      });
    } });

    register({ kind: 'exact', path: '/system-prompt', handler: async (req, res) => {
      const loader = this.ctx.oafLoader;
      this.json(res, 200, { system_prompt: loader?.promptText ?? '', base_prompt: loader?.promptText ?? '' });
    } });

    register({ kind: 'exact', path: '/.well-known/agent-card.json', handler: async (req, res) => {
      this.json(res, 200, {
        name: this.loaderAgentName(),
        version: this.ctx.oafLoader?.frontmatter?.version || cfg.version,
        description: this.ctx.oafLoader?.frontmatter?.description || cfg.description,
        protocolVersion: '1.0.0',
        url: '',
        skills: this.ctx.oafLoader?.skills ?? [],
        capabilities: { streaming: true, a2a: 'A2A-1.0.0' },
      });
    } });

    register({ kind: 'exact', path: '/tools', handler: async (req, res) => {
      const u = new URL(req.url, 'http://x');
      const includeInternal = u.searchParams.get('includeInternal') === 'true';
      const loader = this.ctx.oafLoader;
      let registryNames = [];
      try { registryNames = (this.ctx.tools.schemas() ?? []).map((s) => s.name); } catch { /* 未就绪 */ }
      const mcpTools = loader?.mcpToolView(registryNames) ?? [];
      const visible = mcpTools; // appOnly 工具保留在 /tools 列表并携带标记（仅对 LLM 隐藏）
      const declared = loader?.frontmatter?.tools ?? null;
      const INTERNAL_TOOLS = ['echo', 'get_current_time', 'present_file', 'present_url'];
      const internal = INTERNAL_TOOLS.filter((n) => registryNames.includes(n))
        .map((n) => ({ name: n, category: 'internal', source: 'builtin', declared: Array.isArray(declared) ? declared.includes(n) : false }));
      const body = {
        tools: visible.map(({ qualifiedName, ...rest }) => rest),
        totalCount: visible.length,
        mcpCount: visible.filter((t) => !t.appOnly).length,
        internalCount: includeInternal ? internal.length : 0,
      };
      if (includeInternal) {
        body.tools = [...body.tools, ...internal];
        body.internalCount = internal.length;
        body.sdkInternal = registryNames
          .filter((n) => !n.startsWith('mcp__') && !INTERNAL_TOOLS.includes(n))
          .map((n) => ({ name: n, category: 'sdk', source: 'sdk' }));
        body.sdkInternalCount = body.sdkInternal.length;
      }
      this.json(res, 200, body);
    } });

    register({ kind: 'exact', path: '/mcp', handler: async (req, res) => {
      const loader = this.ctx.oafLoader;
      let registryNames = [];
      try { registryNames = (this.ctx.tools.schemas() ?? []).map((s) => s.name); } catch { /* 未就绪 */ }
      const out = [];
      for (const [serverName, meta] of loader?.mcpMeta ?? []) {
        const toolCount = registryNames.filter((n) => n.startsWith(`mcp__${serverName}__`)).length;
        out.push({
          server: serverName,
          vendor: meta.vendor,
          connection_type: meta.connectionType.toLowerCase(),
          url: meta.url,
          tool_count: toolCount,
          has_ui: Object.keys(meta.uiMap).length > 0,
        });
      }
      this.json(res, 200, out);
    } });

    register({ kind: 'exact', path: '/skills', handler: async (req, res) => {
      this.json(res, 200, this.ctx.oafLoader?.skills ?? []);
    } });

    // 唯一对话入口
    register({ kind: 'exact', path: '/threads/chat', handler: (req, res) => this.handleChat(req, res) });

    // /threads/{sid}** 子路径路由（confirm/confirm-stream 经由 prefix 表）
    register({ kind: 'prefix', path: '/threads', handler: async (req, res) => {
      const sub = req.url.split('?')[0].replace(/^\/threads\/?/, '');
      const [sidRaw, action] = sub.split('/');
      const sid = decodeURIComponent(sidRaw ?? '');
      const u = new URL(req.url, 'http://x');
      if (!sid) {
        // /threads 列表（userId 为可选过滤器；缺省 = 全量，AF 同语义）
        const userId = u.searchParams.get('userId') ?? req.headers['x-user-id']?.toString() ?? '';
        return this.json(res, 200, await this.store.listSessions(userId));
      }
      if (action === 'subscribe' && req.method === 'GET') return this.handleSubscribe(req, res, sid);
      if (action === 'status' && req.method === 'GET') return this.handleStatus(req, res, sid);
      if (action === 'history' && req.method === 'GET') return this.handleHistory(req, res, sid);
      if (action === 'llm-calls' && req.method === 'GET') return this.handleLlmCalls(req, res, sid);
      if (action === 'confirm' && req.method === 'POST') return this.handleConfirmSync(req, res, sid);
      if (action === 'confirm-stream' && req.method === 'POST') return this.handleConfirmStream(req, res, sid);
      if (!action && req.method === 'PATCH') return this.handlePatchThread(req, res, sid);
      if (!action && req.method === 'GET') return this.handleThreadDetail(req, res, sid);
      if (!action && req.method === 'DELETE') return this.handleDeleteThread(req, res, sid);
      return this.json(res, 404, { error: 'not_found' });
    } });

    register({ kind: 'exact', path: '/files/upload', handler: (req, res) => this.handleFileUpload(req, res) });
    register({ kind: 'prefix', path: '/files', handler: (req, res) => this.handleFileDownload(req, res) });

    // MCP Apps：卡片工具代理（H8 ask 拦截 / M4 app_only 经代理可调）+ 资源占位
    register({ kind: 'prefix', path: '/mcp', handler: (req, res) => this.handleMcpProxy(req, res) });
  }

  resolveUserId(req, params, fallback) {
    return req.headers['x-user-id']?.toString() || params.get('userId') || fallback || this.config.defaultUserId;
  }

  async readBody(req, limitBytes = 32 * 1024 * 1024) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > limitBytes) throw Object.assign(new Error('request body too large'), { statusCode: 413 });
      chunks.push(chunk);
    }
    return Buffer.concat(chunks);
  }

  /** POST /threads/chat —— 单次流 SSE（AF C2；ASKING 态拒新 turn，H6）。 */
  async handleChat(req, res) {
    let body;
    try {
      body = JSON.parse((await this.readBody(req, 1024 * 1024)).toString('utf8') || '{}');
    } catch (err) {
      return this.json(res, err.statusCode ?? 400, { error: 'invalid_request' });
    }
    const cfg = this.config;
    const userId = this.resolveUserId(req, new URL(req.url, 'http://x').searchParams, body.userId) ?? cfg.defaultUserId;
    const message = typeof body.message === 'string' ? body.message : '';
    const fileIds = Array.isArray(body.fileIds) ? body.fileIds : [];
    if (!message && fileIds.length === 0) {
      return this.json(res, 400, { error: 'message_or_fileIds_required' });
    }
    if (body.model && !['', 'system', process.env.LLM_MODEL_ID].includes(body.model)) {
      return this.json(res, 400, { error: 'unknown_model' });
    }
    const fresh = !body.sessionId;
    const sessionId = body.sessionId || randomUUID();
    const rt = this.runtimeOf(sessionId);
    rt.userId = userId;

    res.writeHead(200, sseHeaders());
    const writer = new SseWriter(res, (cfg.heartbeatIntervalSeconds ?? 20) * 1000);
    req.on('close', () => {
      writer.detach();
      rt.writers.delete(writer);
    });

    // H6：ASKING 态拒新 turn（AF turn_pending_confirm 语义）
    if (rt.asking) {
      writer.control(controlFrames.error(`turn_pending_confirm: session '${sessionId}' is in ASKING state（先走确认流程再发新消息）`));
      writer.end();
      return;
    }

    const acquire = await this.acquireLease(rt, writer, {
      queueTimeoutMs: (cfg.queueTimeoutSeconds ?? 120) * 1000,
      waitingFrameMs: (cfg.waitingFrameIntervalSeconds ?? 15) * 1000,
    });
    if (!acquire) return;

    if (fresh) writer.control(controlFrames.sessionCreated(sessionId));
    rt.writers.add(writer);
    try {
      await this.store.upsertSessionUser(sessionId, userId);
      const agent = await this.attachAgent(rt);
      // fileIds 注入（F1/F2）：图片 → attachment ImageBlock 内联；文档 → 工作区 uploads/ 物化
      const { injected, imageContent } = await this.materializeFiles(sessionId, userId, fileIds, await this.workspaceFor(sessionId));
      const mentions = injected.length ? `\n\n[已注入工作区文件: ${injected.join(', ')}]` : '';
      agent.followup(createUserMessage({
        content: [{ type: 'text', text: message + mentions }, ...imageContent],
        source: { kind: 'user' },
      }));
    } catch (err) {
      console.error('[oaf-server] chat 启动失败:', err);
      writer.control(controlFrames.error(err instanceof Error ? err.message : String(err)));
      writer.end();
      rt.writers.delete(writer);
      rt.turnOpen = false;
      this.releaseLease(rt);
    }
  }

  /** fileIds → 工作区物化（AF UploadWorkspaceInjector 语义的本地档）。 */
  async materializeFiles(sessionId, userId, fileIds, workspaceDir) {
    if (!fileIds?.length) return { injected: [], imageContent: [] };
    const { copyFileSync, mkdirSync, readFileSync } = await import('node:fs');
    const path = await import('node:path');
    const injected = [];
    const imageContent = [];
    const destDir = path.join(workspaceDir, 'uploads');
    mkdirSync(destDir, { recursive: true });
    const root = process.env.OAF_FILES_DIR ?? '/tmp/oaf-dsh-files';
    for (const fileId of fileIds) {
      const [rows] = await this.pool.query('SELECT file_name, mime_type, storage_type, storage_key FROM file_asset WHERE id = ?', [String(fileId)]);
      const row = rows[0];
      if (!row || row.storage_type !== 'local') continue;
      const bytes = readFileSync(path.join(root, row.storage_key));
      if (/^image\//.test(row.mime_type)) {
        // F2：图片走 attachment 管线（ImageBlock → 请求投影 image_url/data URL）
        try {
          const refs = await admitEncodedImages(this.ctx.attachments, [{
            data: bytes.toString('base64'), mediaType: row.mime_type, name: row.file_name,
          }]);
          for (const ref of refs) imageContent.push({ type: 'image', attachment: ref });
          continue;
        } catch (err) {
          console.error('[oaf-server] 图片 admission 失败，回落文本注入:', err?.message ?? err);
        }
      }
      copyFileSync(path.join(root, row.storage_key), path.join(destDir, row.file_name));
      injected.push(`uploads/${row.file_name}`);
    }
    return { injected, imageContent };
  }

  handleSubscribe(req, res, sessionId) {
    const u = new URL(req.url, 'http://x');
    const afterSeq = Number(u.searchParams.get('afterSeq') ?? 0) || 0;
    res.writeHead(200, sseHeaders());
    const writer = new SseWriter(res, (this.config.heartbeatIntervalSeconds ?? 20) * 1000);
    writer.tail = true;
    req.on('close', () => {
      writer.detach();
      this.runtimes.get(sessionId)?.writers.delete(writer);
    });
    const rt = this.runtimes.get(sessionId);
    const active = Boolean(rt && (rt.busy || rt.turnOpen || rt.asking));
    if (active) rt.writers.add(writer);
    (async () => {
      const rows = await this.store.readAfter(sessionId, afterSeq);
      for (const row of rows) writer.event(row.seq, row.payload);
      if (!active) {
        writer.control(controlFrames.done());
        writer.end();
      }
    })().catch((err) => {
      writer.control(controlFrames.error(err.message));
      writer.end();
    });
  }

  async handleStatus(req, res, sessionId) {
    const pending = await this.pendingConfirmOf(sessionId).catch(() => null);
    const latest = await this.store.latest(sessionId).catch(() => undefined);
    const rt = this.runtimes.get(sessionId);
    if (!latest && !pending) {
      return this.json(res, 200, { session_id: sessionId, state: 'idle', latest_event_seq: 0, reply_id: '', pending_confirm: '' });
    }
    const active = Boolean(rt && (rt.busy || rt.turnOpen));
    const state = pending ? 'waiting_confirm' : (active ? 'working' : (latest?.type === 'AGENT_END' ? 'completed' : (latest ? 'interrupted' : 'idle')));
    this.json(res, 200, {
      session_id: sessionId,
      state,
      latest_event_seq: latest?.seq ?? 0,
      reply_id: rt?.turnState?.replyId ?? latest?.replyId ?? '',
      pending_confirm: pending ?? '',
    });
  }

  async handleHistory(req, res, sessionId) {
    const messages = await this.buildHistory(sessionId);
    const pendingConfirm = await this.pendingConfirmOf(sessionId).catch(() => null);
    const files = await this.filesOf(sessionId).catch(() => []);
    this.json(res, 200, { session_id: sessionId, pendingConfirm: pendingConfirm ?? null, files, messages });
  }

  async handleThreadDetail(req, res, sessionId) {
    const row = await this.store.getSessionUser(sessionId).catch(() => undefined);
    if (!row) return this.json(res, 200, { session_id: sessionId, user_id: '', updated_at: null, pendingConfirm: null, files: [], messages: [] });
    const messages = await this.buildHistory(sessionId);
    const pendingConfirm = await this.pendingConfirmOf(sessionId).catch(() => null);
    const files = await this.filesOf(sessionId).catch(() => []);
    this.json(res, 200, {
      session_id: sessionId,
      user_id: row.user_id,
      updated_at: row.updated_at,
      pendingConfirm: pendingConfirm ?? null,
      files,
      messages,
    });
  }

  async handleDeleteThread(req, res, sessionId) {
    const rt = this.runtimes.get(sessionId);
    if (rt?.handle) await rt.handle.dispose().catch(() => {});
    this.runtimes.delete(sessionId);
    await this.store.deleteSession(sessionId).catch(() => {});
    this.json(res, 200, { session_id: sessionId, deleted: true });
  }

  async handlePatchThread(req, res, sessionId) {
    let body;
    try {
      body = JSON.parse((await this.readBody(req, 1024 * 1024)).toString('utf8') || '{}');
    } catch {
      return this.json(res, 400, { error: 'invalid_request' });
    }
    const rt = this.runtimes.get(sessionId);
    if (body.title !== undefined) {
      if (typeof body.title !== 'string' || body.title.length === 0) {
        return this.json(res, 400, { error: 'invalid_title' });
      }
      await this.store.renameSession(sessionId, body.title.slice(0, 255), 'manual');
      if (rt) rt.manualTitle = true;
    }
    if (body.model !== undefined && !['', 'system', process.env.LLM_MODEL_ID].includes(body.model)) {
      return this.json(res, 400, { error: 'unknown_model' });
    }
    const row = await this.store.getSessionUser(sessionId).catch(() => undefined);
    this.json(res, 200, { session_id: sessionId, title: row?.title ?? body.title ?? '' });
  }

  async handleLlmCalls(req, res, sessionId) {
    const [rows] = await this.pool.query(
      'SELECT call_id, model, request_json, response_json, created_ms FROM llm_call WHERE session_id = ? ORDER BY id ASC LIMIT 500',
      [sessionId],
    );
    this.json(res, 200, {
      session_id: sessionId,
      calls: rows.map((r) => ({
        call_id: r.call_id,
        timestamp: Number(r.created_ms ?? 0),
        model: r.model,
        request: safeParse(r.request_json),
        response: safeParse(r.response_json),
      })),
    });
  }

  // ── HITL 确认双端点 ──

  parseConfirmBody(req) {
    return this.readBody(req, 1024 * 1024).then((buf) => JSON.parse(buf.toString('utf8') || '{}'));
  }

  async handleConfirmSync(req, res, sessionId) {
    let body;
    try {
      body = await this.parseConfirmBody(req);
    } catch {
      return this.json(res, 400, { error: 'invalid_request' });
    }
    const verdict = await this.consumeConfirm(sessionId, body.results);
    if (verdict.error) return this.json(res, verdict.status, { error: verdict.error });
    const rt = this.runtimes.get(sessionId);
    this.resolveAsk(sessionId, verdict.decision);
    // 同步确认：等 turn 收敛，返回最终回复（AF §3.1）
    const deadline = Date.now() + 120_000;
    while (Date.now() < deadline) {
      const stillOpen = rt && (rt.turnOpen || rt.busy);
      if (!stillOpen) break;
      await new Promise((r) => setTimeout(r, 300));
    }
    const finalText = rt?.lastAssistantText ?? '';
    this.json(res, 200, { response: finalText, thread_id: sessionId });
  }

  async handleConfirmStream(req, res, sessionId) {
    let body;
    try {
      body = await this.parseConfirmBody(req);
    } catch {
      return this.json(res, 400, { error: 'invalid_request' });
    }
    res.writeHead(200, sseHeaders());
    const writer = new SseWriter(res, (this.config.heartbeatIntervalSeconds ?? 20) * 1000);
    req.on('close', () => {
      writer.detach();
      this.runtimes.get(sessionId)?.writers.delete(writer);
    });
    const verdict = await this.consumeConfirm(sessionId, body.results);
    if (verdict.error) {
      writer.control(controlFrames.error(verdict.error === 'confirm_already_consumed'
        ? `confirm_already_consumed: session '${sessionId}' 的确认上下文已被消费`
        : `confirm_context_not_found: Session not found or confirm context expired`));
      writer.end();
      return;
    }
    const rt = this.runtimes.get(sessionId);
    if (rt) {
      rt.writers.add(writer);
      this.resolveAsk(sessionId, verdict.decision);
    } else {
      writer.control(controlFrames.error(`confirm_context_not_found: Session not found or confirm context expired`));
      writer.end();
    }
  }

  // ── 文件（C12）──

  async handleFileUpload(req, res) {
    const ct = req.headers['content-type'] ?? '';
    if (!ct.includes('multipart/form-data')) {
      return this.json(res, 400, { error: 'no_file_uploaded' });
    }
    const m = /boundary=(?:"([^"]+)"|([^;]+))/i.exec(ct);
    if (!m) return this.json(res, 400, { error: 'no_file_uploaded' });
    const boundary = Buffer.from(`--${m[1] ?? m[2]}`);
    const body = await this.readBody(req);
    // Buffer 级 multipart 解析：文件名字段按 UTF-8 解码（中文 RFC5987 契约）、内容保持原始字节
    let fileName = '';
    let mime = '';
    let content = null;
    let userId = '';
    let sessionId = '';
    let pos = body.indexOf(boundary);
    while (pos >= 0) {
      const next = body.indexOf(boundary, pos + boundary.length);
      if (next < 0) break;
      const part = body.subarray(pos + boundary.length, next);
      const headerEnd = part.indexOf(Buffer.from('\r\n\r\n'));
      if (headerEnd >= 0) {
        const headers = part.subarray(0, headerEnd).toString('utf8');
        const data = part.subarray(headerEnd + 4, part.length - 2); // 去尾部 \r\n
        const nameMatch = /name="([^"]*)"/.exec(headers);
        const fileMatch = /filename="([^"]*)"/.exec(headers);
        const ctMatch = /Content-Type:\s*([^\r\n]+)/i.exec(headers);
        const field = nameMatch?.[1] ?? '';
        if (fileMatch) {
          fileName = fileMatch[1];
          mime = ctMatch?.[1]?.trim() ?? 'application/octet-stream';
          content = data;
        } else if (field === 'userId') {
          userId = data.toString('utf8');
        } else if (field === 'sessionId') {
          sessionId = data.toString('utf8');
        }
      }
      pos = next;
    }
    if (!fileName || !content) return this.json(res, 400, { error: 'no_file_uploaded' });
    try { fileName = decodeURIComponent(fileName); } catch { /* 原样 */ }
    const bad = validateUpload(fileName, mime, content.length);
    if (bad) return this.json(res, bad.status, { error: bad.error });
    const id = randomUUID();
    const { mkdirSync, writeFileSync } = await import('node:fs');
    const path = await import('node:path');
    const root = process.env.OAF_FILES_DIR ?? '/tmp/oaf-dsh-files';
    mkdirSync(root, { recursive: true });
    const storageKey = `${id}${fileName.replace(/[^\w.-]/g, '_')}`;
    writeFileSync(path.join(root, storageKey), content);
    await this.pool.query(
      `INSERT INTO file_asset (id, user_key, session_id, file_name, mime_type, size, storage_type, storage_key, origin, status)
       VALUES (?, ?, ?, ?, ?, ?, 'local', ?, 'upload', 'injected')`,
      [id, userId, sessionId, fileName, mime, content.length, storageKey],
    );
    this.json(res, 200, { file_id: id, file_name: fileName, mime_type: mime, size: content.length });
  }

  async handleFileDownload(req, res, overrideId) {
    try {
      return await this.downloadImpl(req, res, overrideId);
    } catch (err) {
      console.error('[oaf-server] file download error:', err);
      return this.json(res, 500, { error: 'download_failed', message: err instanceof Error ? err.message : String(err) });
    }
  }

  async downloadImpl(req, res, overrideId) {
    const sub = req.url.split('?')[0].replace(/^\/files\/?/, '');
    const fileId = overrideId ?? decodeURIComponent(sub.split('/')[0] ?? '');
    if (!/^[0-9a-f-]{36}$/.test(fileId)) return this.json(res, 400, { error: 'invalid_file_id' });
    const [rows] = await this.pool.query('SELECT file_name, mime_type, storage_type, storage_key, external_url FROM file_asset WHERE id = ?', [fileId]);
    const row = rows[0];
    if (!row) return this.json(res, 404, { error: 'file_not_found' });
    const u = new URL(req.url, 'http://x');
    const inline = u.searchParams.get('inline') === '1' && /^(image\/|text\/)/.test(row.mime_type);
    // header 明文部分必须 ASCII 安全（非 ASCII 走 RFC5987 编码段）
    const asciiName = row.file_name.replace(/[^\x20-\x7e]/g, '_').replace(/["\\]/g, '_');
    const encoded = encodeURIComponent(row.file_name).replace(/['()]/g, escape);
    const disposition = `${inline ? 'inline' : 'attachment'}; filename="${asciiName}"; filename*=UTF-8''${encoded}`;
    // 外部交付：代理回源（前缀白名单收敛 SSRF，AF FILE_EXTERNAL_URL_PREFIXES 语义）
    if (row.storage_type === 'external') {
      const prefixes = (process.env.FILE_EXTERNAL_URL_PREFIXES ?? '').split(',').map((s) => s.trim()).filter(Boolean);
      if (!prefixes.some((p) => row.external_url.startsWith(p))) {
        return this.json(res, 403, { error: 'external_url_not_allowed' });
      }
      try {
        const upstream = await fetch(row.external_url);
        if (!upstream.ok) return this.json(res, 502, { error: 'upstream_fetch_failed' });
        const buf = Buffer.from(await upstream.arrayBuffer());
        res.writeHead(200, {
          'Content-Type': row.mime_type,
          'Content-Length': buf.length,
          'Content-Disposition': disposition,
          'X-Content-Type-Options': 'nosniff',
        });
        return res.end(buf);
      } catch {
        return this.json(res, 502, { error: 'upstream_fetch_failed' });
      }
    }
    const path = await import('node:path');
    const root = process.env.OAF_FILES_DIR ?? '/tmp/oaf-dsh-files';
    const file = path.join(root, row.storage_key);
    const { statSync, createReadStream } = await import('node:fs');
    let size;
    try {
      size = statSync(file).size;
    } catch {
      return this.json(res, 502, { error: 'storage_object_missing' });
    }
    res.writeHead(200, {
      'Content-Type': row.mime_type,
      'Content-Length': size,
      'Content-Disposition': disposition,
      'X-Content-Type-Options': 'nosniff',
    });
    createReadStream(file).pipe(res);
  }

  /** A2A v1.0.0 JSON-RPC（C8）：message/send（阻塞）、message/stream（SSE）、tasks/get。
   *  已声明限制（AF 同款语义）：A2A 通道不支持 ask 工具——ask 挂起以 input-required 终态回。 */
  parseA2aMessage(msg) {
    const parts = Array.isArray(msg?.parts) ? msg.parts : [];
    const text = parts.filter((p) => p?.kind === 'text').map((p) => String(p.text ?? '')).join('');
    const meta = msg?.metadata ?? {};
    return {
      text,
      userId: meta.userId ? String(meta.userId) : undefined,
      sessionId: meta.sessionId ? String(meta.sessionId) : undefined,
    };
  }

  async handleA2A(req, res) {
    let body;
    try {
      body = JSON.parse((await this.readBody(req, 2 * 1024 * 1024)).toString('utf8') || '{}');
    } catch {
      return this.json(res, 400, { jsonrpc: '2.0', id: null, error: { code: -32700, message: 'Parse error' } });
    }
    const { id, method, params } = body;
    const reply = (result) => this.json(res, 200, { jsonrpc: '2.0', id, result });
    const replyError = (code, message) => this.json(res, 200, { jsonrpc: '2.0', id, error: { code, message } });

    switch (method) {
      case 'message/send': {
        const { text, userId, sessionId: metaSid } = this.parseA2aMessage(params?.message);
        const sessionId = metaSid || randomUUID();
        const { ok, rt, error } = await this.startTurn(sessionId, userId ?? this.config.defaultUserId, text || '[A2A empty message]');
        if (!ok) return replyError(-32002, error);
        const taskId = randomUUID();
        const deadline = Date.now() + 120_000;
        while ((rt.busy || rt.turnOpen) && Date.now() < deadline) {
          await new Promise((r) => setTimeout(r, 250));
        }
        return reply({
          kind: 'message',
          messageId: randomUUID(),
          role: 'agent',
          taskId,
          parts: [{ kind: 'text', text: rt?.lastAssistantText ?? '' }],
          contextId: sessionId,
        });
      }
      case 'message/stream': {
        const { text, userId, sessionId: metaSid } = this.parseA2aMessage(params?.message);
        const sessionId = metaSid || randomUUID();
        const taskId = randomUUID();
        res.writeHead(200, sseHeaders());
        let closed = false;
        const a2aWriter = {
          tail: false,
          lastWrittenSeq: 0,
          alive: true,
          detach() { this.alive = false; },
          control() { /* A2A 无控制帧 */ },
          event(_seq, payload) {
            if (closed || !this.alive) return;
            let update;
            if (payload.type === 'AGENT_START') {
              update = { kind: 'status-update', taskId, contextId: sessionId, status: { state: 'working' }, final: false };
            } else if (payload.type === 'AGENT_END') {
              update = { kind: 'status-update', taskId, contextId: sessionId, status: { state: 'completed' }, final: true };
            } else if (payload.type === 'permission_ask') {
              update = { kind: 'status-update', taskId, contextId: sessionId, status: { state: 'input-required', message: { role: 'agent', parts: [{ kind: 'text', text: 'A2A 通道不支持 ask 工具，请走 /threads/chat 确认链路' }] } }, final: true };
            } else {
              return; // 内容帧不透传（A2A 面只发任务状态；AF 同款 convertToSse 语义）
            }
            try { res.write(`data: ${JSON.stringify({ jsonrpc: '2.0', id, result: update })}\n\n`); } catch { this.alive = false; }
            if (payload.type === 'AGENT_END' || payload.type === 'permission_ask') {
              closed = true;
              try { res.end(); } catch { /* 已断开 */ }
            }
          },
          end() {
            if (closed) return;
            closed = true;
            try { res.end(); } catch { /* 已断开 */ }
          },
        };
        const { ok, rt, error } = await this.startTurn(sessionId, userId ?? this.config.defaultUserId, text || '[A2A empty message]', a2aWriter);
        if (!ok) {
          res.write(`data: ${JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32002, message: error } })}\n\n`);
          return res.end();
        }
        void rt;
        return;
      }
      case 'tasks/get': {
        // 任务快照：从会话日志派生（AF MySqlTaskStore「读日志构造、save no-op」思路）。
        // A2A 发起的 turn 无独立 task 持久化，返回 unknown 态快照（不报 Method not found）。
        return reply({ kind: 'task', id: String(params?.id ?? ''), status: { state: 'unknown' } });
      }
      case 'tasks/cancel':
      case 'tasks/resubscribe':
        return replyError(-32001, 'task not found（当前运行时未持久化 A2A task 索引）');
      default:
        return replyError(-32601, `Method not found: ${method}`);
    }
  }

  /** /mcp/{server}/tools/{tool}（POST）与 /mcp/{server}/resources（GET，占位）。 */
  async handleMcpProxy(req, res) {
    const sub = req.url.split('?')[0].replace(/^\/mcp\/?/, '');
    const parts = sub.split('/').map(decodeURIComponent);
    const [server, action, ...rest] = parts;
    if (!server) return this.json(res, 404, { error: 'not_found' });
    const loader = this.ctx.oafLoader;
    const meta = loader?.mcpMeta.get(server);
    if (action === 'tools' && rest[0] && req.method === 'POST') {
      const toolName = rest[0];
      let body;
      try {
        body = JSON.parse((await this.readBody(req, 1024 * 1024)).toString('utf8') || '{}');
      } catch {
        return this.json(res, 400, { error: 'invalid_request' });
      }
      const qualified = `mcp__${server}__${toolName}`;
      const isAsk = (() => {
        if (!loader) return false;
        if (loader.permission.ask.has(toolName)) return true;
        return loader.mcpServers.includes(server) && loader.permission.ask.has(toolName);
      })();
      // ask 工具未经确认 → 403 needsConfirm（H8 契约）
      if (isAsk && body.confirmed !== true) {
        return this.json(res, 403, {
          needsConfirm: true,
          toolCalls: [{ tool_call_id: randomUUID(), name: toolName, input: body.arguments ?? {} }],
        });
      }
      if (isAsk) this.uiProxyBypass.add(qualified);
      try {
        const result = await this.ctx.tools.execute({
          callId: randomUUID(),
          name: qualified,
          arguments: body.arguments ?? {},
          signal: new AbortController().signal,
        });
        const content = Array.isArray(result?.content)
          ? result.content
          : [{ type: 'text', text: String(result?.content ?? result ?? '') }];
        return this.json(res, 200, { content, isError: Boolean(result?.isError) });
      } catch (err) {
        return this.json(res, 200, {
          content: [{ type: 'text', text: err instanceof Error ? err.message : String(err) }],
          isError: true,
        });
      }
    }
    if (action === 'resources' && req.method === 'GET') {
      // MCP Apps UI 资源代理属 M1-M3（/resources/ui HtmlResource 拉取 + CSP 注入）
      const ui = meta ? Object.values(meta.uiMap) : [];
      return this.json(res, 200, { server, resources: ui.map((uri) => `${uri}`) });
    }
    return this.json(res, 404, { error: 'not_found' });
  }

  json(res, code, body) {
    res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(body));
  }
}

/** Cordis 插件入口：注册服务（激活期完成迁移/监听/HITL 桥/路由）。 */
export function apply(ctx, config) {
  new OafServerService(ctx, config ?? {});
}

export { OafServerService as default };
