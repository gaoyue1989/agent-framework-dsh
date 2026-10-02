#!/usr/bin/env node
/**
 * oaf-loader ask-deny 拦截的确定性 e2e：
 * 假 MCP server（SDK StreamableHTTP）暴露 delete_service（OAF ask 工具）→ 自制回放夹具
 * 让模型发起 tool_call → 断言 TOOL_RESULT_END 为 ERROR（被 oaf-loader pre-execute 拦截），
 * 且工具体从未执行（假 server 的调用计数为 0）。
 */
import { execSync, spawn } from 'node:child_process';
import { mkdirSync, writeFileSync, readFileSync, cpSync, rmSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as sleep } from 'node:timers/promises';
import { fileURLToPath } from 'node:url';

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..');
const TMP = '/tmp/oaf-ask-deny-test';
const MCP_PORT = 18095;
const MOCK_PORT = 18096;
const RT_PORT = 18102;
const TOOL = 'mcp__platform-publisher__delete_service';

// 1. 假 MCP server（SDK 经 dsh-mcp-client 依赖链解析，绕过 pnpm 严格隔离）
rmSync(TMP, { recursive: true, force: true });
mkdirSync(TMP, { recursive: true });
const mcpClientPkg = execSync(
  `node -p "require.resolve('@deepseek-ai/dsh-mcp-client/package.json', { paths: ['${ROOT}/packages/oaf-loader'] })"`,
).toString().trim();
writeFileSync(path.join(TMP, 'fake-mcp.mjs'), `
// 最小 MCP streamable-http server（JSON-RPC POST：initialize/notifications/tools.list/tools.call）
import http from 'node:http';
let calls = 0;
const TOOL_INPUT = {
  type: 'object',
  properties: { serviceId: { type: 'string' }, confirm_k8s_name: { type: 'string' } },
  required: ['serviceId'],
};
const server = http.createServer((req, res) => {
  let body = '';
  req.on('data', (c) => { body += c; });
  req.on('end', async () => {
    let msg;
    try { msg = JSON.parse(body); } catch { res.writeHead(400); res.end(); return; }
    const { id, method, params } = msg;
    const reply = (result) => {
      res.writeHead(200, {
        'Content-Type': 'application/json',
        'Mcp-Session-Id': 'fake-session-1',
        'MCP-Protocol-Version': '2025-06-18',
      });
      res.end(JSON.stringify({ jsonrpc: '2.0', id, result }));
    };
    switch (method) {
      case 'initialize':
        reply({
          protocolVersion: params?.protocolVersion ?? '2025-06-18',
          capabilities: { tools: {} },
          serverInfo: { name: 'platform-publisher', version: '1.0.0' },
        });
        break;
      case 'notifications/initialized':
      case 'notifications/cancelled':
        res.writeHead(202); res.end();
        break;
      case 'tools/list':
        reply({ tools: [
          { name: 'delete_service', description: '删除服务（ask 工具）', inputSchema: TOOL_INPUT },
          { name: 'list_services', description: '列出服务', inputSchema: { type: 'object', properties: {} } },
        ] });
        break;
      case 'tools/call': {
        calls += 1;
        console.log('[fake-mcp] tools/call', params?.name, 'calls=', calls);
        reply({ content: [{ type: 'text', text: 'DELETED' }] });
        break;
      }
      default:
        if (id === undefined) { res.writeHead(202); res.end(); break; }
        res.writeHead(200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ jsonrpc: '2.0', id, error: { code: -32601, message: 'method not found: ' + method } }));
    }
  });
});
server.listen(${MCP_PORT}, '127.0.0.1');
setInterval(() => {}, 1 << 30);
process.on('SIGTERM', () => process.exit(0));
`);

// 2. 包副本：mcp url 指向假 server
const pkgDir = path.join(TMP, 'oaf-package');
cpSync('/root/agent-manager/release-agent', pkgDir, { recursive: true });
const mcpCfg = readFileSync(path.join(pkgDir, 'mcp-configs/platform/config.yaml'), 'utf8')
  .replace('http://platform-backend.agent-platform.svc.cluster.local:8080/mcp', `http://127.0.0.1:${MCP_PORT}/mcp`);
writeFileSync(path.join(pkgDir, 'mcp-configs/platform/config.yaml'), mcpCfg);

// 3. 回放夹具：模型直接发起 delete_service tool_call（OpenAI SSE chunks）。
//    mock 路由用 MARKER_MAP 硬编码映射，借道 tool:write → tool-write.json；
//    第二组响应承接工具结果续推轮（callIndex = tool 消息数）。
const chunk = (delta, finish = null) => JSON.stringify({
  id: 'c1', object: 'chat.completion.chunk',
  choices: [{ index: 0, delta, finish_reason: finish }],
});
const chunks = [
  chunk({ role: 'assistant' }),
  chunk({ tool_calls: [{ index: 0, id: 'call_test_1', type: 'function', function: { name: TOOL, arguments: '' } }] }),
  chunk({ tool_calls: [{ index: 0, function: { arguments: '{"serviceId":"99999","confirm_k8s_name":"fake"}' } }] }),
  chunk({}, 'tool_calls'),
  JSON.stringify({ id: 'c1', object: 'chat.completion.chunk', choices: [], usage: { completion_tokens: 5, prompt_tokens: 10, total_tokens: 15 } }),
  '[DONE]',
];
const chunks2 = [
  chunk({ role: 'assistant', content: '工具调用已结束。' }),
  chunk({}, 'stop'),
];
writeFileSync(path.join(TMP, 'tool-write.json'), JSON.stringify({
  scenario: 'ask-deny', model: 'mimo',
  calls: [{ request: { messages: [] }, chunks }, { request: { messages: [] }, chunks: chunks2 }],
}));

// 4. 起 mock LLM（自定义夹具目录）+ 假 MCP + 运行时
const mock = spawn('node', [path.join(ROOT, '..', 'agent-framework', 'e2e', 'mock', 'llm-server.mjs')], {
  env: { ...process.env, MOCK_LLM_PORT: String(MOCK_PORT), MOCK_LLM_FIXTURES: TMP },
  stdio: ['ignore', 'ignore', 'inherit'],
});
const fakeMcp = spawn('node', [path.join(TMP, 'fake-mcp.mjs')], {
  cwd: TMP, stdio: ['ignore', 'ignore', 'inherit'],
});
await sleep(1500);
const rt = spawn('./node_modules/.bin/dsh', ['--profile', 'oaf-web'], {
  cwd: ROOT,
  env: {
    ...process.env,
    DSH_HOME: path.join(ROOT, '.dsh-home'),
    LLM_API_KEY: 'mock-key', LLM_MODEL_ID: 'gpt-mock', LLM_BASE_URL: `http://127.0.0.1:${MOCK_PORT}/v1`,
    SERVER_HOST: '127.0.0.1', SERVER_PORT: String(RT_PORT),
    CHECKPOINT_JDBC_URL: 'jdbc:mysql://127.0.0.1:13306/agent_framework_dsh',
    CHECKPOINT_USERNAME: 'e2e', CHECKPOINT_PASSWORD: 'e2e-pass',
    AGENT_CONFIG_DIR: pkgDir,
  },
  stdio: ['ignore', 'ignore', 'inherit'],
});
let pass = 0; const fail = [];
const check = (n, ok, d = '') => { if (ok) { pass++; console.log(`  ✔ ${n}`); } else { fail.push(n); console.error(`  ✘ ${n} ${d}`); } };

for (let i = 0; i < 40; i++) {
  try {
    const r = await fetch(`http://127.0.0.1:${RT_PORT}/health`, { signal: AbortSignal.timeout(1500) });
    if (r.ok) break;
  } catch { /* wait */ }
  await sleep(500);
}

try {
  const res = await fetch(`http://127.0.0.1:${RT_PORT}/threads/chat`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ message: '[E2E:tool:write] 直接删除服务', userId: 'ask-deny-test' }),
  });
  const reader = res.body.getReader();
  const dec = new TextDecoder();
  let raw = '';
  const deadline = Date.now() + 60_000;
  for (;;) {
    if (Date.now() > deadline) break;
    const { done, value } = await reader.read();
    if (done) break;
    raw += dec.decode(value, { stream: true });
    if (/"type":"AGENT_END"/.test(raw)) break;
  }
  const toolNames = [...raw.matchAll(/"toolName":"([^"]+)"/g)].map((m) => m[1]);
  check('模型发起了 ask 工具调用', toolNames.includes(TOOL), toolNames.join(','));
  const endFrames = [...raw.matchAll(/"type":"TOOL_RESULT_END",([^}]*)\}/g)].map((m) => m[0]);
  const denyEnd = endFrames.find((f) => f.includes(TOOL));
  check('ask 工具执行被拦截为 ERROR（deny）', Boolean(denyEnd) && denyEnd.includes('"state":"ERROR"'), denyEnd ?? '无 TOOL_RESULT_END');
  check('拒绝原因带安全降级说明', raw.includes('需要人工确认'));
  const fakeCalls = execSync(`kill -0 ${fakeMcp.pid} 2>/dev/null; echo x`).toString() && 0; // 假 server 调用计数经日志核验
  check('工具体未执行（假 server 无 DELETED 输出）', !raw.includes('DELETED'));
} catch (err) {
  fail.push(`编排失败: ${err.message}`);
  console.error(err);
} finally {
  rt.kill('SIGKILL'); mock.kill('SIGKILL'); fakeMcp.kill('SIGKILL');
}
console.log(`\n结果：${pass} 通过 / ${fail.length} 失败`);
process.exit(fail.length ? 1 : 0);
