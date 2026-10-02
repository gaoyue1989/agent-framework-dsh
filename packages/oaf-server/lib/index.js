/**
 * OAF 对外面插件（C1/C2/C3，设计 §5.1/§5.2）——全部 AF HTTP 端点注册在 dsh-host-webserver。
 *
 * M0 范围：POST /threads/chat（单次流 SSE，AF 帧词表）、GET /threads/{sid}/subscribe
 * （断连续传/回放 + done）、GET /threads/{sid}/status（五态，M0 覆盖 idle/working/completed/
 * interrupted）、GET /threads、PATCH /threads/{sid}（title）、GET /health、GET /。
 *
 * 架构对位（与 agent-framework 同构）：
 * - 事件镜像（MySQL session_event 表）↔ AF SessionEventStore：帧持久化 + seq 游标，
 *   /subscribe 任意副本可回放（M2 按设计迁 Redis Streams）
 * - 进程内 turn 租约 + waiting 帧 ↔ AF TurnLeaseStore（跨副本 Redis 租约列入 M2）
 * - ctx.agents.create/resume + followup ↔ AF AgentRuntimeService（协议驱动范式，§4.3）
 * - session/event + agent/assistant-stream 监听 ↔ AF AgentEventSseSerializer（帧映射在 frames.js）
 *
 * 已知 M0 边界（诚实清单）：error/interrupted 控制帧不落镜像（AF 同为控制帧无 seq）；
 * fileIds/HITL/A2A/模型托管面未实现（M1-M3，见 README 路线表）。
 *
 * @module @oaf/oaf-server
 */
import { randomUUID } from 'node:crypto';
import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
import { createUserMessage } from '@deepseek-ai/dsh-llm';
import { createPool, ensureDatabase, migrate } from '@oaf/oaf-common';
import {
  createTurnSerializer,
  settledEventFrames,
  liveStreamFrames,
  controlFrames,
} from './frames.js';

const MIGRATIONS = [
  {
    version: 1,
    name: 'oaf-server-v1',
    sql: [
      // AF session_event 契约（api-frontend-sse.md §14）：seq 为前端游标，PK 保证唯一单调
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
      // AF session_user 同构：会话列表按 userId 过滤 + title/model 会话元数据
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
];

const sseHeaders = () => ({
  'Content-Type': 'text/event-stream',
  'Cache-Control': 'no-cache',
  Connection: 'keep-alive',
  'X-Accel-Buffering': 'no',
});

/** SSE 写通道：chat 主流与 subscribe 追赶流共用；断连只摘除不终止 turn。 */
class SseWriter {
  constructor(res, heartbeatMs) {
    this.res = res;
    this.alive = true;
    this.tail = false; // subscribe 追赶流（收尾补 done 帧）
    this.lastWrittenSeq = 0; // 回放/广播去重游标
    this.heartbeat = heartbeatMs > 0 ? setInterval(() => this.raw(':hb\n\n'), heartbeatMs) : null;
    this.heartbeat?.unref?.();
  }

  detach() {
    this.alive = false;
    if (this.heartbeat) clearInterval(this.heartbeat);
  }

  raw(text) {
    if (!this.alive) return;
    try {
      this.res.write(text);
    } catch {
      this.alive = false;
    }
  }

  /** 控制帧：无 id: 行（api-frontend-sse.md §9.9）。 */
  control(frame) {
    this.raw(`data: ${JSON.stringify(frame)}\n\n`);
  }

  /** 事件帧：id: 行携带 seq 游标（断连续传依据），data.id 为事件 ID（e{seq}）。 */
  event(seq, payload) {
    if (seq <= this.lastWrittenSeq) return; // 回放后追加减除重复
    this.lastWrittenSeq = seq;
    this.raw(`id: ${seq}\ndata: ${JSON.stringify(payload)}\n\n`);
  }

  end() {
    if (!this.alive) return;
    this.detach();
    try {
      this.res.end();
    } catch { /* 客户端已断开 */ }
  }
}

/** 事件镜像（MySQL）——AF SessionEventStore 同构：seq 编号 + 回放 + 最新态查询。 */
class EventMirrorStore {
  constructor(pool) {
    this.pool = pool;
    /** 进程内 seq 计数（按 session），惰性从流顶端播种——与 AF 语义一致（流是唯一事实来源）。 */
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

  /** 追加一帧并返回 seq；payload 为含 data.id 的完整帧 JSON。 */
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

  async readAfter(sessionId, afterSeq, limit = 5000) {
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
    return row
      ? { seq: Number(row.seq), type: row.event_type, replyId: row.reply_id }
      : undefined;
  }

  async upsertSessionUser(sessionId, userId, title = '') {
    await this.pool.query(
      `INSERT INTO session_user (session_id, user_id, title) VALUES (?, ?, ?)
       ON DUPLICATE KEY UPDATE user_id = VALUES(user_id), updated_at = CURRENT_TIMESTAMP(3)`,
      [sessionId, userId, title],
    );
  }

  async renameSession(sessionId, title) {
    await this.pool.query(
      'UPDATE session_user SET title = ?, updated_at = CURRENT_TIMESTAMP(3) WHERE session_id = ?',
      [title, sessionId],
    );
  }

  async listSessions(userId) {
    const [rows] = await this.pool.query(
      'SELECT session_id, user_id, title, updated_at FROM session_user WHERE user_id = ? ORDER BY updated_at DESC LIMIT 200',
      [userId],
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
}

/**
 * OAF 对外面服务：路由表 + 会话运行时（进程内）+ 全局事件监听。
 * 表迁移在 [Service.init]（激活期 await，失败即启动失败）；路由在初始化完成后挂载。
 */
class OafServerService extends Service {
  /** 类插件服务依赖：webserver（路由落点）+ agents（协议驱动）+ sessionPersistence（resume 判定）。 */
  static inject = ['webServer', 'agents', 'sessionPersistence'];
  static Config = z.object({
    agentName: z.string().default('oaf-dsh-agent'),
    slug: z.string().default('oaf-dsh-agent'),
    version: z.string().default('1.0.0'),
    description: z.string().default(''),
    engine: z.string().default('DeepSeek Harness (dsh)'),
    systemProvider: z.string().default('oaf-system'),
    systemModel: z.string().default(''),
    defaultUserId: z.string().default('debug-user'),
    queueTimeoutSeconds: z.number().default(120),
    waitingFrameIntervalSeconds: z.number().default(15),
    heartbeatIntervalSeconds: z.number().default(20),
    jdbcUrl: z.string().default(''),
    username: z.string().default(''),
    password: z.string().default(''),
  });

  constructor(ctx, config) {
    super(ctx, 'oafServer');
    this.config = config;
    this.pool = undefined;
    this.store = undefined;
    /** sessionId → 会话运行时（活 agent 句柄、租约、开着的 SSE 写通道）。 */
    this.runtimes = new Map();
  }

  async [Service.init]() {
    const cfg = this.config;
    // 环境变量兜底（CHECKPOINT_* 契约）：patch 未显式给出时回落 env
    const jdbcUrl = cfg.jdbcUrl || process.env.CHECKPOINT_JDBC_URL;
    const username = cfg.username || process.env.CHECKPOINT_USERNAME;
    const password = cfg.password || process.env.CHECKPOINT_PASSWORD;
    if (!jdbcUrl) throw new Error('oaf-server: 缺少 CHECKPOINT_JDBC_URL（config 或环境变量）');
    await ensureDatabase({ jdbcUrl, username, password });
    this.pool = createPool({ jdbcUrl, username, password });
    await migrate(this.pool, MIGRATIONS, 'oaf_schema_version_oaf_server');
    this.store = new EventMirrorStore(this.pool);
    this.installListeners();
    // 路由随服务初始化完成再挂载（请求不会打到未就绪的 store）；注销走 Cordis effect
    this.mountRoutes(this.ctx.webServer);
  }

  /** 全局事件监听（dsh 协议驱动的观测面，dsh-acp 同模式）。 */
  installListeners() {
    const disposers = [
      this.ctx.on('session/event', (session, event) => {
        this.onSessionEvent(String(session.header.id), event).catch((err) => {
          console.error('[oaf-server] session/event 处理失败:', err);
        });
      }),
      this.ctx.on('agent/assistant-stream', ({ agent, frame }) => {
        this.onLiveFrame(String(agent.id), frame).catch((err) => {
          console.error('[oaf-server] assistant-stream 处理失败:', err);
        });
      }),
      this.ctx.on('agent/error', ({ agent, error }) => {
        this.onAgentError(String(agent.id), error).catch((err) => {
          console.error('[oaf-server] agent/error 处理失败:', err);
        });
      }),
    ];
    this.ctx.effect(() => () => disposers.forEach((d) => d()));
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
        turnState: undefined,
        lastReplyId: '',
        writers: new Set(),
        waiters: [],
        writeChain: undefined,
      };
      this.runtimes.set(sessionId, rt);
    }
    return rt;
  }

  /** 帧发射：镜像持久化（seq 先算后写，payload 带 data.id）→ 全通道广播。
   *  按 session 串行（writeChain）：seq 分配与写入原子化，帧顺序与事件到达顺序一致。 */
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
    rt.writeChain = run.then(
      () => undefined,
      (err) => {
        console.error('[oaf-server] 帧持久化失败:', err);
      },
    );
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
        agentName: this.config.agentName,
      });
    }
    const s = rt.turnState;
    if (s) {
      const frames = settledEventFrames(s, event);
      if (frames.length) await this.emitEventFrames(rt, frames, s.replyId);
    }
    if (event.type === 'turn/end') {
      // turn 终点：AGENT_END 已广播；chat 主流直接关，subscribe 追赶流补 done 后关。
      // M0：aborted/blocked 等 reason 一律按终态关流（HITL 挂起分流列入 M2 hitl-bridge）。
      rt.turnOpen = false;
      rt.lastReplyId = rt.turnState?.replyId ?? rt.lastReplyId;
      for (const w of [...rt.writers]) {
        if (w.tail) w.control(controlFrames.done());
        w.end();
        rt.writers.delete(w);
      }
      this.releaseLease(rt);
    }
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

  /** 进程内 turn 租约：busy 互斥 + FIFO 排队；排队期间向该请求发 waiting 帧（AF C11 单副本面）。 */
  acquireLease(rt, writer, { queueTimeoutMs, waitingFrameMs }) {
    if (!rt.busy) {
      rt.busy = true;
      return Promise.resolve(true);
    }
    return new Promise((resolve) => {
      const waiter = {};
      const timer = waitingFrameMs > 0
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
        writer.control(controlFrames.error(
          `turn_in_progress: session '${rt.sessionId}' has an active turn and queue timeout reached`,
        ));
        writer.end();
        resolve(false);
      }, queueTimeoutMs);
      waiter.grant = () => {
        cleanup();
        resolve(true);
      };
      rt.waiters.push(waiter);
    });
  }

  releaseLease(rt) {
    const next = rt.waiters.shift();
    if (next) next.grant(); // 租约直接移交队头（busy 保持 true）
    else rt.busy = false;
  }

  /** 活 agent 附加：持久会话 resume（冷恢复），否则 create（协议驱动，设计 §4.3）。 */
  async attachAgent(rt) {
    if (rt.agent) return rt.agent;
    const cwd = await this.workspaceFor(rt.sessionId);
    const persisted = await this.ctx.sessionPersistence.stat(rt.sessionId).catch(() => undefined);
    // 程序化 create/resume 必须显式选模型（provider 路由 + 模型 id）
    const agentOptions = this.config.systemModel
      ? { provider: this.config.systemProvider, model: this.config.systemModel }
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

  mountRoutes(web) {
    const cfg = this.config;
    const register = (route) => this.ctx.effect(() => web.register(route));

    register({
      kind: 'exact',
      path: '/health',
      handler: async (req, res) => {
        this.json(res, 200, {
          status: 'healthy',
          agent: cfg.agentName,
          slug: cfg.slug,
          version: cfg.version,
          llm_configured: Boolean(process.env.LLM_API_KEY && process.env.LLM_MODEL_ID),
          engine: cfg.engine,
          tenant_prefix: cfg.slug,
        });
      },
    });

    register({
      kind: 'exact',
      path: '/',
      handler: async (req, res) => {
        this.json(res, 200, {
          agent: cfg.agentName,
          slug: cfg.slug,
          version: cfg.version,
          description: cfg.description,
          protocols: { oaf: 'v0.8.0' },
          engine: cfg.engine,
          endpoints: {
            health: '/health',
            threads: '/threads',
            threads_chat: '/threads/chat',
          },
        });
      },
    });

    // 唯一对话入口：POST /threads/chat（sessionId 入 body，AF 契约）
    register({
      kind: 'exact',
      path: '/threads/chat',
      handler: (req, res) => this.handleChat(req, res),
    });

    register({
      kind: 'exact',
      path: '/threads',
      handler: async (req, res) => {
        const userId = this.resolveUserId(req, new URL(req.url, 'http://x').searchParams);
        this.json(res, 200, await this.store.listSessions(userId));
      },
    });

    // /threads/{sid} 及子路径（subscribe/status/PATCH）
    register({
      kind: 'prefix',
      path: '/threads',
      handler: async (req, res) => {
        const sub = req.url.split('?')[0].replace(/^\/threads\/?/, '');
        const [sidRaw, action] = sub.split('/');
        const sid = decodeURIComponent(sidRaw ?? '');
        if (!sid) return this.json(res, 404, { error: 'not_found' });
        if (action === 'subscribe' && req.method === 'GET') {
          return this.handleSubscribe(req, res, sid);
        }
        if (action === 'status' && req.method === 'GET') return this.handleStatus(req, res, sid);
        if (!action && req.method === 'PATCH') return this.handlePatchThread(req, res, sid);
        return this.json(res, 404, { error: 'not_found' });
      },
    });
  }

  resolveUserId(req, params, fallback) {
    // AF 三来源优先级：X-User-Id header（网关注入）> 请求参数 > body.userId > 默认
    return req.headers['x-user-id']?.toString()
      || params.get('userId')
      || fallback
      || this.config.defaultUserId;
  }

  async readBody(req, limitBytes = 1024 * 1024) {
    const chunks = [];
    let size = 0;
    for await (const chunk of req) {
      size += chunk.length;
      if (size > limitBytes) throw Object.assign(new Error('request body too large'), { statusCode: 413 });
      chunks.push(chunk);
    }
    const text = Buffer.concat(chunks).toString('utf8');
    return text ? JSON.parse(text) : {};
  }

  /** POST /threads/chat —— 单次流 SSE（AF C2）。 */
  async handleChat(req, res) {
    const cfg = this.config;
    let body;
    try {
      body = await this.readBody(req);
    } catch (err) {
      return this.json(res, err.statusCode ?? 400, { error: 'invalid_request' });
    }
    const userId = this.resolveUserId(req, new URL(req.url, 'http://x').searchParams, body.userId)
      ?? cfg.defaultUserId;
    const message = typeof body.message === 'string' ? body.message : '';
    const fileIds = Array.isArray(body.fileIds) ? body.fileIds : [];
    if (!message && fileIds.length === 0) {
      return this.json(res, 400, { error: 'message_or_fileIds_required' });
    }
    if (fileIds.length > 0) {
      // M0 边界：fileIds 物化（UploadWorkspaceInjector 语义）列入 M1
      return this.json(res, 400, { error: 'fileIds_not_supported_in_m0' });
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
      // 断连只摘除写通道，turn 继续执行（Durable SSE 语义）
      writer.detach();
      rt.writers.delete(writer);
    });

    const ok = await this.acquireLease(rt, writer, {
      queueTimeoutMs: (cfg.queueTimeoutSeconds ?? 120) * 1000,
      waitingFrameMs: (cfg.waitingFrameIntervalSeconds ?? 15) * 1000,
    });
    if (!ok) return;

    if (fresh) writer.control(controlFrames.sessionCreated(sessionId));
    rt.writers.add(writer);
    try {
      await this.store.upsertSessionUser(sessionId, userId);
      const agent = await this.attachAgent(rt);
      agent.followup(createUserMessage({
        content: [{ type: 'text', text: message }],
        source: { kind: 'user' },
      }));
      // settlement 由 turn/end 监听关流（AGENT_END 广播 → writers 清理 → 租约释放）
    } catch (err) {
      writer.control(controlFrames.error(err instanceof Error ? err.message : String(err)));
      writer.end();
      rt.writers.delete(writer);
      rt.turnOpen = false;
      this.releaseLease(rt);
    }
  }

  /** GET /threads/{sid}/subscribe —— 回放 + 追加/收尾（AF C3 durable SSE）。 */
  async handleSubscribe(req, res, sessionId) {
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
    const active = Boolean(rt && (rt.busy || rt.turnOpen));
    // 先挂通道再回放：回放与广播的竞态由 writer.lastWrittenSeq 去重
    if (active) rt.writers.add(writer);
    try {
      const rows = await this.store.readAfter(sessionId, afterSeq);
      for (const row of rows) writer.event(row.seq, row.payload);
      if (!active) {
        writer.control(controlFrames.done());
        writer.end();
      }
    } catch (err) {
      writer.control(controlFrames.error(err instanceof Error ? err.message : String(err)));
      writer.end();
    }
  }

  /** GET /threads/{sid}/status —— 五态裁决（AF C3；interrupted = 有事件但无租约无确认）。 */
  async handleStatus(req, res, sessionId) {
    const latest = await this.store.latest(sessionId);
    if (!latest) {
      return this.json(res, 200, {
        session_id: sessionId,
        state: 'idle',
        latest_event_seq: 0,
        reply_id: '',
        pending_confirm: '',
      });
    }
    const rt = this.runtimes.get(sessionId);
    const active = Boolean(rt && (rt.busy || rt.turnOpen));
    const state = active ? 'working' : (latest.type === 'AGENT_END' ? 'completed' : 'interrupted');
    this.json(res, 200, {
      session_id: sessionId,
      state,
      latest_event_seq: latest.seq,
      reply_id: rt?.turnState?.replyId ?? latest.replyId ?? '',
      pending_confirm: '',
    });
  }

  /** PATCH /threads/{sid} —— title 重命名（model 会话绑定列入 M3 模型面）。 */
  async handlePatchThread(req, res, sessionId) {
    let body;
    try {
      body = await this.readBody(req);
    } catch {
      return this.json(res, 400, { error: 'invalid_request' });
    }
    if (body.title !== undefined) {
      if (typeof body.title !== 'string' || body.title.length === 0) {
        return this.json(res, 400, { error: 'invalid_title' });
      }
      await this.store.renameSession(sessionId, body.title.slice(0, 255));
    }
    if (body.model !== undefined && !['', 'system', process.env.LLM_MODEL_ID].includes(body.model)) {
      return this.json(res, 400, { error: 'unknown_model' });
    }
    this.json(res, 200, { session_id: sessionId, title: body.title ?? '' });
  }

  json(res, code, body) {
    res.writeHead(code, { 'Content-Type': 'application/json; charset=utf-8' });
    res.end(JSON.stringify(body));
  }
}

export { OafServerService as default };
