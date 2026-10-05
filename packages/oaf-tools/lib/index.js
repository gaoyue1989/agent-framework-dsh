/**
 * AF 自定义工具对位（agent-framework @Tool 注解等价，AGENTS.md「自定义工具」表）：
 *
 * - echo(text)：回显输入（S4 契约：结果 `echo: {text}`）
 * - get_current_time(timezone)：指定时区当前时间
 * - present_file(file_path, file_content_base64?)：工作区产物登记为可下载交付
 *   （file_asset 落库 + `oaf/file-ready` 事件 → oaf-server 合成 file_ready 帧）
 * - present_url(file_name, url, mime_type?, size?)：外部交付物登记（登记侧执行
 *   FILE_EXTERNAL_URL_PREFIXES 前缀白名单，下载侧代理回源二次校验）
 *
 * @module @oaf/oaf-tools
 */
import { randomUUID } from 'node:crypto';
import { readFileSync, existsSync, mkdirSync, copyFileSync, statSync } from 'node:fs';
import { join, basename, dirname } from 'node:path';
import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
import { defineTool } from '@deepseek-ai/dsh-tools';
import { createPool, ensureDatabase, migrate } from '@oaf/oaf-common';

/** Cordis 插件名（Loader 诊断用）。 */
export const name = 'oaf-tools';

const MIGRATIONS = [
  {
    version: 1,
    name: 'oaf-tools-file-asset-v1',
    sql: `CREATE TABLE IF NOT EXISTS file_asset (
            id VARCHAR(36) NOT NULL PRIMARY KEY,
            user_key VARCHAR(255) NOT NULL DEFAULT '',
            session_id VARCHAR(255) NOT NULL DEFAULT '',
            file_name VARCHAR(255) NOT NULL,
            workspace_path VARCHAR(512) NOT NULL DEFAULT '',
            mime_type VARCHAR(128) NOT NULL DEFAULT 'application/octet-stream',
            size BIGINT NOT NULL DEFAULT 0,
            storage_type VARCHAR(16) NOT NULL DEFAULT 'local',
            storage_key VARCHAR(512) NOT NULL DEFAULT '',
            external_url VARCHAR(1024) NOT NULL DEFAULT '',
            origin VARCHAR(16) NOT NULL DEFAULT 'generated',
            status VARCHAR(16) NOT NULL DEFAULT 'injected',
            created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
            KEY idx_session (session_id, created_at)
          ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
  },
];

/** 本地文件存储根（local 后端；s3 列 M3 file-asset 完整版）。 */
export function fileStorageRoot() {
  const root = process.env.OAF_FILES_DIR ?? '/tmp/oaf-dsh-files';
  mkdirSync(root, { recursive: true });
  return root;
}

/** 工具名 → 摘要文案（AF ToolSummaryGenerator 对位，oaf-server 合成帧用）。 */
export function summarizeToolCall(name, args) {
  switch (name) {
    case 'echo':
      return `echo ${String(args?.text ?? '').slice(0, 60)}`;
    case 'present_file':
      return `交付 ${String(args?.file_path ?? '')}`;
    case 'present_url':
      return `登记外部交付 ${String(args?.file_name ?? '')}`;
    default:
      return `执行 ${name}`;
  }
}

class OafToolsService extends Service {
  static inject = ['tools'];
  static Config = z.object({
    jdbcUrl: z.string().default(''),
    username: z.string().default(''),
    password: z.string().default(''),
  });

  constructor(ctx, config) {
    super(ctx, 'oafTools');
    this.config = config;
    this.pool = undefined;
  }

  async [Service.init]() {
    const jdbcUrl = this.config.jdbcUrl || process.env.CHECKPOINT_JDBC_URL;
    if (!jdbcUrl) throw new Error('oaf-tools: 缺少 CHECKPOINT_JDBC_URL');
    const poolOpts = { jdbcUrl, username: this.config.username || process.env.CHECKPOINT_USERNAME, password: this.config.password || process.env.CHECKPOINT_PASSWORD };
    await ensureDatabase(poolOpts);
    this.pool = createPool(poolOpts);
    await migrate(this.pool, MIGRATIONS, 'oaf_schema_version_oaf_tools');
    this.registerTools();
  }

  /** 登记交付物（file_asset 落库 + file-ready 事件广播给 oaf-server）。 */
  async presentAsset({ sessionId, userId, fileName, mimeType, size, sourcePath, externalUrl }) {
    const id = randomUUID();
    const storageKey = sourcePath ? `${id}${fileName.replace(/[^\w.-]/g, '_')}` : '';
    if (sourcePath) {
      const dest = join(fileStorageRoot(), storageKey);
      mkdirSync(dirname(dest), { recursive: true });
      copyFileSync(sourcePath, dest);
    }
    await this.pool.query(
      `INSERT INTO file_asset (id, user_key, session_id, file_name, mime_type, size, storage_type, storage_key, external_url, origin, status)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'generated', 'injected')`,
      [id, userId ?? '', sessionId ?? '', fileName, mimeType, size, externalUrl ? 'external' : 'local', storageKey, externalUrl ?? ''],
    );
    this.ctx.emit('oaf/file-ready', {
      sessionId,
      asset: { file_id: id, file_name: fileName, mime_type: mimeType, size, download_url: `/files/${id}` },
    });
    return { file_id: id, file_name: fileName, mime_type: mimeType, size };
  }

  registerTools() {
    const disposers = [
      // echo：S4 契约（结果 `echo: {text}`）
      this.ctx.tools.register(defineTool({
        name: 'echo',
        description: '回显输入文本，用于端到端链路验证。',
        parameters: { text: { type: 'string', required: true, description: '要回显的文本' } },
        output: {
          schema: { type: 'object', additionalProperties: false, properties: { text: { type: 'string', required: true } } },
          render: (args) => [{ type: 'text', text: `echo: ${args.text}` }],
        },
        execute: async (args) => ({ text: `echo: ${args.text}` }),
      })),
      this.ctx.tools.register(defineTool({
        name: 'get_current_time',
        description: '获取指定时区（默认 Asia/Shanghai）的当前时间。',
        parameters: { timezone: { type: 'string', description: 'IANA 时区名，如 Asia/Shanghai' } },
        output: {
          schema: { type: 'object', additionalProperties: false, properties: { time: { type: 'string', required: true } } },
          render: (args, value) => [{ type: 'text', text: value.time }],
        },
        execute: async (args) => {
          const tz = args?.timezone || 'Asia/Shanghai';
          let time;
          try {
            time = new Intl.DateTimeFormat('zh-CN', { timeZone: tz, dateStyle: 'full', timeStyle: 'long' }).format(new Date());
          } catch {
            time = new Date().toISOString();
          }
          return { time: `${time}（${tz}）` };
        },
      })),
      this.ctx.tools.register(defineTool({
        name: 'present_file',
        description: '把工作区产物登记为用户可下载的交付文件（可传 file_content_base64 直接登记内容）。',
        parameters: {
          file_path: { type: 'string', required: true, description: '工作区相对或绝对路径' },
          file_content_base64: { type: 'string', description: '可选：直接登记的 base64 内容' },
        },
        output: {
          schema: { type: 'object', additionalProperties: false, properties: { file_id: { type: 'string', required: true }, file_name: { type: 'string', required: true }, mime_type: { type: 'string' }, size: { type: 'number' } } },
          render: (args, value) => [{ type: 'text', text: `已登记交付文件 ${value.file_name}（${value.file_id}）` }],
        },
        execute: async (args, exec) => {
          const cwd = exec.agent?.session?.header?.cwd ?? process.cwd();
          const rawPath = String(args.file_path ?? '');
          const abs = rawPath.startsWith('/') ? rawPath : join(cwd, rawPath);
          if (!existsSync(abs)) throw new Error(`文件不存在: ${rawPath}`);
          const buf = args.file_content_base64
            ? Buffer.from(String(args.file_content_base64), 'base64')
            : readFileSync(abs);
          const asset = await this.presentAsset({
            sessionId: this.currentSessionId(exec),
            userId: exec.agent?.id ?? '',
            fileName: basename(abs),
            mimeType: guessMime(basename(abs)),
            size: buf.length,
            sourcePath: abs,
          });
          return asset;
        },
      })),
      this.ctx.tools.register(defineTool({
        name: 'present_url',
        description: '把外部系统的 http(s) 产物登记为用户可下载的交付（运行时经白名单代理回源）。',
        parameters: {
          file_name: { type: 'string', required: true, description: '展示文件名' },
          url: { type: 'string', required: true, description: 'http(s) 下载地址' },
          mime_type: { type: 'string', description: 'MIME 类型' },
          size: { type: 'integer', description: '字节数' },
        },
        output: {
          schema: { type: 'object', additionalProperties: false, properties: { file_id: { type: 'string', required: true }, file_name: { type: 'string', required: true }, mime_type: { type: 'string' }, size: { type: 'number' } } },
          render: (args, value) => [{ type: 'text', text: `已登记外部交付 ${value.file_name}（${value.file_id}）` }],
        },
        execute: async (args, exec) => {
          const url = String(args.url ?? '').trim();
          // 登记侧白名单（AF FileTools.presentUrl 同款语义，issue dsh#2）：
          // 失败/拒绝路径只回错误结果，不落 file_asset、不发 file-ready 事件
          const prefixes = (process.env.FILE_EXTERNAL_URL_PREFIXES ?? '').split(',').map((s) => s.trim()).filter(Boolean);
          if (!prefixes.length) throw new Error('present_url unavailable: no external URL prefixes configured (FILE_EXTERNAL_URL_PREFIXES)');
          if (!url) throw new Error('url is required');
          if (!/^https?:\/\//.test(url)) throw new Error(`url must be http(s): ${url}`);
          const allowed = prefixes.some((p) => url === p || url.startsWith(p + '/'));
          if (!allowed) throw new Error(`url not in configured external prefixes: ${url}`);
          const asset = await this.presentAsset({
            sessionId: this.currentSessionId(exec),
            userId: exec.agent?.id ?? '',
            fileName: String(args.file_name ?? 'download'),
            mimeType: String(args.mime_type ?? 'application/octet-stream'),
            size: Number(args.size ?? 0),
            externalUrl: url,
          });
          return asset;
        },
      })),
    ];
    this.ctx.effect(() => () => disposers.forEach((d) => d()));
  }

  /** 会话键：AF file_asset.session_id 绑定业务会话（不得是网关 hash）。 */
  currentSessionId(exec) {
    const sid = exec.agent?.id ?? '';
    // dsh SessionId 即我们创建 agent 时传入的 AF 会话键
    return String(sid);
  }
}

/** 扩展名 → MIME（AF 上传白名单子集）。 */
export function guessMime(fileName) {
  const ext = fileName.split('.').pop()?.toLowerCase() ?? '';
  const map = {
    txt: 'text/plain', md: 'text/markdown', csv: 'text/csv',
    png: 'image/png', jpg: 'image/jpeg', jpeg: 'image/jpeg', gif: 'image/gif', webp: 'image/webp',
    pdf: 'application/pdf',
    xlsx: 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    docx: 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
    pptx: 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
    zip: 'application/zip',
  };
  return map[ext] ?? 'application/octet-stream';
}

/** Cordis 插件入口：注册服务（表迁移/工具注册在激活期完成）。 */
export function apply(ctx, config) {
  new OafToolsService(ctx, config ?? {});
}

export { OafToolsService as default };
