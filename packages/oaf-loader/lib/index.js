/**
 * OAF 包加载插件（设计 §5.4）：AGENTS.md frontmatter → 系统提示词段；mcp-configs →
 * dsh-mcp-client 桥（fail-soft）；权限三态 / ui.resources / skills 目录 → 元数据服务
 * （oafLoader）供 oaf-server 的 API 面与 HITL 桥消费。
 *
 * @module @oaf/oaf-loader
 */
import { readFileSync, existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { randomUUID } from 'node:crypto';
import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
import yaml from 'js-yaml';
import { defineTool } from '@deepseek-ai/dsh-tools';
import * as McpClient from '@deepseek-ai/dsh-mcp-client';

/** Cordis 插件名（Loader 诊断用）。 */
export const name = 'oaf-loader';

/** 解析 AGENTS.md：`---` frontmatter + body 提示词。无 frontmatter 时整体视为提示词。 */
export function parseAgentsMarkdown(raw) {
  const text = String(raw ?? '');
  const m = /^---\r?\n([\s\S]*?)\r?\n---\r?\n?([\s\S]*)$/.exec(text);
  if (!m) return { frontmatter: {}, body: text.trim() };
  return { frontmatter: yaml.load(m[1]) ?? {}, body: m[2].trim() };
}

/** 解析 SKILL.md frontmatter（宽松：仅取 name/description/version）。 */
export function parseSkillMarkdown(raw, fallbackName) {
  const { frontmatter } = parseAgentsMarkdown(raw);
  return {
    name: String(frontmatter.name ?? fallbackName),
    description: String(frontmatter.description ?? ''),
    version: String(frontmatter.version ?? ''),
  };
}

/** OAF mcp-configs/connection.type → dsh-mcp-client transport 映射。 */
export function toMcpClientConfig(serverName, oafConfig, { failOnStartupError = false } = {}) {
  const connection = oafConfig?.connection ?? {};
  const type = String(connection.type ?? '').toLowerCase();
  const server = oafConfig?.server ?? serverName;
  if (type === 'streamablehttp' || type === 'streamable_http' || type === 'sse') {
    return {
      transport: 'streamable-http',
      serverName: server,
      url: String(connection.url ?? ''),
      headers: connection.headers ?? {},
      failOnStartupError,
    };
  }
  throw new Error(`oaf-loader: 暂不支持的 MCP connection.type: ${connection.type}（server=${server}）`);
}

/**
 * OAF permissions.tools 三态收集（AF 语义：MCP 显式规则 > frontmatter 声明 > 自动放行）。
 */
export function parsePermissionTools(frontmatter, oafMcpConfigs = []) {
  const allow = new Set();
  const ask = new Set();
  const collect = (tools) => {
    for (const [tool, state] of Object.entries(tools ?? {})) {
      if (state === 'ask') ask.add(tool);
      else if (state === 'allow') allow.add(tool);
    }
  };
  collect(frontmatter?.config?.permission?.tools);
  for (const cfg of oafMcpConfigs) collect(cfg?.permissions?.tools);
  return { allow, ask };
}

/** 每个 MCP server 的展示元数据（/mcp、/tools、ui 元数据来源）。
 *  ui.app_only 三形态：true（整 server）/ {tool: uiRef}（逐工具）/ 缺省。 */
export function summarizeMcpServer(oafConfig) {
  const uiTools = oafConfig?.ui?.tools ?? {};
  const uiMap = {};
  for (const [tool, ref] of Object.entries(uiTools)) {
    uiMap[tool] = typeof ref === 'string' ? ref : String(ref?.resource_uri ?? ref?.resourceUri ?? '');
  }
  const appOnlyRaw = oafConfig?.ui?.app_only;
  const appOnlySet = new Set();
  let appOnlyAll = false;
  if (appOnlyRaw === true) appOnlyAll = true;
  else if (appOnlyRaw && typeof appOnlyRaw === 'object') {
    for (const [tool, ref] of Object.entries(appOnlyRaw)) {
      appOnlySet.add(tool);
      if (!uiMap[tool]) uiMap[tool] = typeof ref === 'string' ? ref : String(ref?.resource_uri ?? ref?.resourceUri ?? '');
    }
  }
  const uh = oafConfig?.userHeaders;
  const userHeaders = uh?.headers && typeof uh.headers === 'object'
    ? { headers: { ...uh.headers }, onMissing: String(uh['on-missing'] ?? uh.onMissing ?? 'deny') }
    : null;
  return {
    server: String(oafConfig?.server ?? ''),
    vendor: String(oafConfig?.vendor ?? ''),
    version: String(oafConfig?.version ?? ''),
    connectionType: String(oafConfig?.connection?.type ?? ''),
    url: String(oafConfig?.connection?.url ?? ''),
    uiMap,
    appOnlyAll,
    appOnlySet,
    userHeaders,
  };
}

/**
 * 直连 MCP streamable-http 工具调用（S5 userHeaders per-call 注入的执行路径）：
 * initialize → initialized → tools/call（params._meta 协议双通道 + 动态 HTTP header）。
 * 每次调用独立会话；响应兼容 application/json 与 text/event-stream 两种回包。
 */
export async function directMcpToolCall(url, rawName, args, { dynamicHeaders = {}, staticHeaders = {}, meta, timeoutMs = 60_000 } = {}) {
  const baseHeaders = {
    'Content-Type': 'application/json',
    Accept: 'application/json, text/event-stream',
    ...staticHeaders,
    ...dynamicHeaders,
  };
  const call = async (payload) => {
    const res = await fetch(url, {
      method: 'POST',
      headers: baseHeaders,
      body: JSON.stringify(payload),
      signal: AbortSignal.timeout(timeoutMs),
    });
    const sid = res.headers.get('mcp-session-id');
    const ct = res.headers.get('content-type') ?? '';
    const text = await res.text();
    let json;
    if (ct.includes('text/event-stream')) {
      for (const line of text.split('\n')) {
        if (line.startsWith('data:')) {
          try {
            const evt = JSON.parse(line.slice(5).trim());
            if (evt.id !== undefined || evt.result || evt.error) { json = evt; break; }
          } catch { /* 跳过坏行 */ }
        }
      }
    } else {
      try { json = JSON.parse(text); } catch { /* 下面统一报错 */ }
    }
    if (!json) throw new Error(`MCP 响应不可解析（http ${res.status}）`);
    return { json, sid };
  };
  const init = await call({
    jsonrpc: '2.0', id: 1, method: 'initialize',
    params: { protocolVersion: '2025-06-18', capabilities: {}, clientInfo: { name: 'oaf-runtime', version: '1.0.0' } },
  });
  if (init.json.error) throw new Error(`MCP initialize 失败: ${init.json.error.message ?? ''}`);
  const sessionHeaders = init.sid ? { 'Mcp-Session-Id': init.sid } : {};
  await fetch(url, {
    method: 'POST',
    headers: { ...baseHeaders, ...sessionHeaders },
    body: JSON.stringify({ jsonrpc: '2.0', method: 'notifications/initialized' }),
    signal: AbortSignal.timeout(timeoutMs),
  }).catch(() => {});
  const result = await call({
    jsonrpc: '2.0', id: 2, method: 'tools/call',
    params: { name: rawName, arguments: args ?? {}, ...(meta ? { _meta: meta } : {}) },
  });
  if (result.json.error) throw new Error(`MCP tools/call 失败: ${result.json.error.message ?? ''}`);
  return result.json.result;
}

/** 扫描 configDir/skills 目录下的 SKILL.md（声明 ∪ 目录事实，目录缺失返回空）。 */
export function scanSkills(dir) {
  const skillsDir = join(dir, 'skills');
  if (!existsSync(skillsDir)) return [];
  const out = [];
  for (const entry of readdirSync(skillsDir, { withFileTypes: true })) {
    if (!entry.isDirectory()) continue;
    const skillPath = join(skillsDir, entry.name, 'SKILL.md');
    if (!existsSync(skillPath)) {
      out.push({ name: entry.name, description: '', version: '', source: 'local-dynamic', dynamic: true, declaredButMissing: false });
      continue;
    }
    const meta = parseSkillMarkdown(readFileSync(skillPath, 'utf8'), entry.name);
    out.push({ ...meta, source: 'local-dynamic', dynamic: false, declaredButMissing: false });
  }
  return out;
}

class OafLoaderService extends Service {
  static inject = ['systemPrompt', 'tools'];
  static Config = z.object({
    configDir: z.string().default('/config'),
    /** dsh-mcp-client failOnStartupError 取反（AF fail-soft 语义）。 */
    mcpRequired: z.boolean().default(false),
  });

  constructor(ctx, config) {
    super(ctx, 'oafLoader');
    this.config = config;
    this.frontmatter = {};
    this.promptText = '';
    this.agentName = '';
    this.slug = '';
    this.permission = { allow: new Set(), ask: new Set() };
    /** server 名 → 展示元数据（summarizeMcpServer）。 */
    this.mcpMeta = new Map();
    /** 已成功挂载的 dsh-mcp-client server 名。 */
    this.mcpServers = [];
    this.skills = [];
  }

  async [Service.init]() {
    const dir = this.config.configDir;
    if (!existsSync(dir)) {
      this.ctx.logger?.warn?.(`oaf-loader: AGENT_CONFIG_DIR 不存在: ${dir}（跳过加载）`);
      return;
    }
    // 1. AGENTS.md → 系统提示词段 + frontmatter 元数据
    const agentsPath = join(dir, 'AGENTS.md');
    if (existsSync(agentsPath)) {
      const { frontmatter, body } = parseAgentsMarkdown(readFileSync(agentsPath, 'utf8'));
      this.frontmatter = frontmatter;
      this.promptText = body;
      this.agentName = frontmatter.name ?? '';
      this.slug = frontmatter.slug ?? '';
      if (body) {
        const disposer = this.ctx.systemPrompt.section({
          name: 'oaf-agent-prompt',
          order: 0, // 与 DEPLOYMENT_PERSONA_PREFIX 同级：包人设先于工具/行为说明段
          text: body,
        });
        this.ctx.effect(() => disposer);
      }
    }
    // 2. mcp-configs/{server}/config.yaml → MCP 桥 + 展示元数据
    const mcpDir = join(dir, 'mcp-configs');
    const oafMcpConfigs = [];
    if (existsSync(mcpDir)) {
      for (const entry of readdirSync(mcpDir, { withFileTypes: true })) {
        if (!entry.isDirectory()) continue;
        const configPath = join(mcpDir, entry.name, 'config.yaml');
        if (!existsSync(configPath)) continue;
        const oafConfig = yaml.load(readFileSync(configPath, 'utf8')) ?? {};
        oafMcpConfigs.push(oafConfig);
        const clientConfig = toMcpClientConfig(entry.name, oafConfig, {
          failOnStartupError: this.config.mcpRequired,
        });
        this.mcpMeta.set(clientConfig.serverName, summarizeMcpServer(oafConfig));
        // AF fail-soft：server 不可达仅告警跳过
        try {
          await this.ctx.plugin(McpClient, clientConfig);
          this.mcpServers.push(clientConfig.serverName);
        } catch (err) {
          this.ctx.logger?.warn?.(`oaf-loader: MCP server "${clientConfig.serverName}" 连接失败（fail-soft 跳过）: ${err?.message ?? err}`);
        }
      }
    }
    this.permission = parsePermissionTools(this.frontmatter, oafMcpConfigs);
    // 3. skills 目录事实
    this.skills = scanSkills(dir);
    // 4. 裸名别名由 oaf-server init 兜底触发（MCP client 工具注册是异步 fiber，
    //    loader init 时 schemas() 可能尚未包含 client 工具）
  }

  /** 裸名别名注册（MCP 工具 → AF 裸名透传）。返回 {registered, failed} 供调用方诊断。 */
  registerRawAliases() {
    const failed = [];
    let names = [];
    try { names = (this.ctx.tools.schemas() ?? []).map((s) => s.name); } catch (e) { return { registered: 0, failed: [`schemas(): ${e?.message ?? e}`] }; }
    const qualified = names.filter((n) => n.startsWith('mcp__'));
    const seen = new Set();
    const disposers = [];
    for (const q of qualified) {
      const rest = q.slice('mcp__'.length);
      const idx = rest.indexOf('__');
      if (idx <= 0) continue;
      const raw = rest.slice(idx + 2);
      if (!raw || seen.has(raw)) continue; // 首服务器优先（同名单工具跨 server 时）
      seen.add(raw);
      const serverName = rest.slice(0, idx);
      const meta = this.mcpMeta.get(serverName);
      try {
        const disposer = this.ctx.tools.register(defineTool({
          name: raw,
          description: `AF 裸名别名：等价于 ${q}`,
          parameters: {},
          output: {
            schema: {
              type: 'object',
              additionalProperties: false,
              properties: { text: { type: 'string', required: true } },
            },
            render: (_args, value) => [{ type: 'text', text: String(value?.text ?? '').slice(0, 8000) }],
          },
          execute: async (args, exec) => {
            // S5：userHeaders server → 直连 MCP 调用，按调用方 userId 注入 header + _meta 双通道
            if (meta?.userHeaders) {
              const sessionId = String(exec?.agent?.id ?? '');
              const userId = this.userIdResolver ? this.userIdResolver(sessionId) : null;
              if (!userId && meta.userHeaders.onMissing !== 'passthrough') {
                return { text: `Error: 用户身份缺失（userHeaders on-missing: ${meta.userHeaders.onMissing}），已拒绝调用 ${raw}` };
              }
              const result = await directMcpToolCall(meta.url, raw, args, {
                dynamicHeaders: { 'X-User-Id': userId ?? '' },
                meta: { userId: userId ?? '' },
                timeoutMs: 60_000,
              });
              return { text: JSON.stringify(result ?? {}) };
            }
            this.rawAliasBypass?.add(q);
            const result = await this.ctx.tools.execute({
              callId: randomUUID(),
              name: q,
              arguments: args,
              signal: new AbortController().signal,
            });
            return { text: JSON.stringify(result ?? {}) };
          },
        }));
        disposers.push(disposer);
        this.rawAliases = [...(this.rawAliases ?? []), { raw, qualified: q }];
      } catch (err) {
        failed.push(`${raw}: ${err?.message ?? err}`);
      }
    }
    if (disposers.length) this.ctx.effect(() => () => disposers.forEach((d) => d()));
    this.aliasStats = { registered: disposers.length, failed };
    return this.aliasStats;
  }

  /** /tools 视图：MCP 工具（裸名 + server 归属 + ui 元数据）。 */
  mcpToolView(registryNames) {
    const tools = [];
    for (const name of registryNames) {
      for (const [serverName, meta] of this.mcpMeta) {
        const prefix = `mcp__${serverName}__`;
        if (!name.startsWith(prefix)) continue;
        const raw = name.slice(prefix.length);
        const appOnly = meta.appOnlyAll || meta.appOnlySet.has(raw);
        tools.push({
          name: raw,
          qualifiedName: name,
          server: serverName,
          category: 'mcp',
          description: '',
          uiResourceUri: meta.uiMap[raw] || undefined,
          appOnly: appOnly || undefined,
        });
      }
    }
    return tools;
  }
}

/** Cordis 插件入口：注册服务（加载在服务激活期完成）。 */
export function apply(ctx, config) {
  new OafLoaderService(ctx, config ?? {});
}

export { OafLoaderService as default };
