/**
 * OAF 包加载插件（设计 §5.4 oaf-loader 的 M0 最小版）——release-agent 切换验证所需：
 *
 * 1. AGENTS.md 解析：frontmatter（YAML）+ body → ctx.systemPrompt.section 注入系统提示词
 * 2. mcp-configs/{server}/config.yaml → ctx.plugin(McpClient) 挂载（streamableHttp →
 *    streamable-http；AF fail-soft 语义：failOnStartupError=false，不可达跳过不阻断启动）
 * 3. 权限三态的 M0 安全降级：`permissions.tools.{tool}: ask` 的变更类工具在 dsh 无 HITL
 *    确认桥（hitl-bridge 属 M2）前经 tools/pre-execute 拦截为 deny——宁可拒绝也不无确认执行
 *    变更操作；allow 与未声明的查询类工具按默认放行（与 AF 未声明默认 ALLOW 一致）
 *
 * M0 未覆盖（报告为差距）：skills/ 目录注册、deniedTools → restrict、subagents 声明、
 * 动态 reload、userHeaders per-call 注入。
 *
 * @module @oaf/oaf-loader
 */
import { readFileSync, existsSync, readdirSync } from 'node:fs';
import { join } from 'node:path';
import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
import yaml from 'js-yaml';
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

/** OAF mcp-configs/connection.type → dsh-mcp-client transport 映射。 */
export function toMcpClientConfig(serverName, oafConfig, { failOnStartupError = false } = {}) {
  const connection = oafConfig?.connection ?? {};
  const type = String(connection.type ?? '').toLowerCase();
  const server = oafConfig?.server ?? serverName;
  if (type === 'streamablehttp' || type === 'streamable_http') {
    return {
      transport: 'streamable-http',
      serverName: server,
      url: String(connection.url ?? ''),
      headers: connection.headers ?? {},
      failOnStartupError,
    };
  }
  if (type === 'sse') {
    // M0：sse 传输未接（release-agent 用 streamableHttp）；按 streamable-http 端点尝试
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
 * ask 清单来自两处并集：AGENTS.md frontmatter 的 config.permission.tools 与
 * 各 mcp-configs/{server}/config.yaml 的 permissions.tools——后者才是 release-agent
 * 这类包的 ask 主来源（变更类 MCP 工具）。
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

class OafLoaderService extends Service {
  static inject = ['systemPrompt', 'tools', 'oafServer'];
  static Config = z.object({
    configDir: z.string().default('/config'),
    /** M0 安全降级：ask 工具拦截为 deny（HITL 桥上线后关闭）。 */
    askDeny: z.boolean().default(true),
    /** dsh-mcp-client failOnStartupError 取反（AF fail-soft 语义）。 */
    mcpRequired: z.boolean().default(false),
  });

  constructor(ctx, config) {
    super(ctx, 'oafLoader');
    this.config = config;
  }

  async [Service.init]() {
    const dir = this.config.configDir;
    if (!existsSync(dir)) {
      this.ctx.logger?.warn?.(`oaf-loader: AGENT_CONFIG_DIR 不存在: ${dir}（跳过加载）`);
      return;
    }
    // 1. AGENTS.md → 系统提示词段
    const agentsPath = join(dir, 'AGENTS.md');
    if (existsSync(agentsPath)) {
      const { frontmatter, body } = parseAgentsMarkdown(readFileSync(agentsPath, 'utf8'));
      if (body) {
        const disposer = this.ctx.systemPrompt.section({
          name: 'oaf-agent-prompt',
          order: 0, // 与 DEPLOYMENT_PERSONA_PREFIX 同级：包人设先于工具/行为说明段
          text: body,
        });
        this.ctx.effect(() => disposer);
        this.frontmatter = frontmatter;
        this.agentName = frontmatter.name ?? '';
        this.slug = frontmatter.slug ?? '';
        // 展示面与 Java 运行时对齐：/health、/ 的 agent/slug 跟随包 frontmatter
        const oafServer = this.ctx.oafServer;
        if (oafServer) {
          if (this.agentName) oafServer.config.agentName = this.agentName;
          if (this.slug) oafServer.config.slug = this.slug;
          if (frontmatter.description) oafServer.config.description = frontmatter.description;
          if (frontmatter.version) oafServer.config.version = frontmatter.version;
        }
      }
    }
    // 2. mcp-configs/{server}/config.yaml → MCP 桥 + 权限三态收集
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
        // AF fail-soft：server 不可达仅告警跳过（McpClient 侧 failOnStartupError=false 时静默降级）
        try {
          await this.ctx.plugin(McpClient, clientConfig);
          this.mcpServers = [...(this.mcpServers ?? []), clientConfig.serverName];
        } catch (err) {
          this.ctx.logger?.warn?.(`oaf-loader: MCP server "${clientConfig.serverName}" 连接失败（fail-soft 跳过）: ${err?.message ?? err}`);
        }
      }
    }
    this.permission = parsePermissionTools(this.frontmatter, oafMcpConfigs);
    // 3. M0 安全降级：ask 工具（变更类，两处来源并集）→ deny（无确认桥不执行变更）。
    //    匹配须同时覆盖 dsh 注册名（mcp__{server}__{tool}）与 OAF 裸名——
    //    dsh-mcp-client 以带前缀的 public name 注册，OAF permissions.tools 写的是裸名。
    if (this.config.askDeny && this.permission.ask.size) {
      const askTools = this.permission.ask;
      const serverNames = this.mcpServers ?? [];
      const isAskTool = (name) => askTools.has(name)
        || serverNames.some((s) => {
          const prefix = `mcp__${s}__`;
          return name.startsWith(prefix) && askTools.has(name.slice(prefix.length));
        });
      const disposer = this.ctx.on('tools/pre-execute', async (exec, next) => {
        if (isAskTool(exec.name)) {
          return {
            kind: 'deny',
            reason: `工具 "${exec.name}" 为变更类操作，需要人工确认后执行；当前 dsh 运行时（M0）未接入确认链路，已安全拒绝。请改用平台 frontend 的发布助手（Java 运行时）完成该操作。`,
          };
        }
        return next();
      });
      this.ctx.effect(() => disposer);
    }
  }
}

/** Cordis 插件入口：注册服务（加载在服务激活期完成）。 */
export function apply(ctx, config) {
  new OafLoaderService(ctx, config ?? {});
}

export { OafLoaderService as default };
