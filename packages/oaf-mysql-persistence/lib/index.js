/**
 * ctx.sessionPersistence 的 MySQL provider —— dsh 会话事件日志落库（设计 §6.1）。
 *
 * 契约要点（@deepseek-ai/dsh-session-persistence）：
 * - 事件从 seq 0 连续、永不改写；torn-tail 由事务原子性天然防御（优于 JSONL 截断修复）
 * - append 尽力而为，flush 是持久化屏障；本实现 append 即事务提交，
 *   flush 退化为 materialize-if-needed（create 已落 header，天然可列出）
 * - 进程内单写者：重复 write-open 抛 SessionAlreadyOwnedError（对齐 dsh 契约）；
 *   跨副本互斥（Redis 租约双层）按设计列入 M2
 * - 校验统一走官方 storage-contract 助手，与 JSONL 参照实现同规则
 *
 * 表结构见 MIGRATIONS（V1）；迁移走 @oaf/oaf-common 的轻量版本表迁移器
 * （设计 §6.3：Node 侧不引 Flyway，保留「只新增版本 / 已合并不可改 / 失败即启动失败」纪律）。
 *
 * @module @oaf/oaf-mysql-persistence
 */
import { Service } from '@deepseek-ai/cordis';
import z from '@deepseek-ai/schemastery';
import {
  SessionPersistence,
  SessionPersistenceRevision,
  SessionAlreadyExistsError,
  SessionAlreadyOwnedError,
  SessionHandleClosedError,
  SessionPersistenceNotFoundError,
  SessionReadOnlyError,
  assertContiguous,
  assertStoredId,
  assertVersion,
  materializeAppendBatch,
  materializeCreateHeader,
  validateStoredEvents,
} from '@deepseek-ai/dsh-session-persistence';
import { createPool, ensureDatabase, migrate, withTxn } from '@oaf/oaf-common';

/** 迁移声明（版本表迁移器见 @oaf/oaf-common）。 */
const MIGRATIONS = [
  {
    version: 1,
    name: 'dsh-session-log-v1',
    sql: [
      `CREATE TABLE IF NOT EXISTS dsh_session_header (
         session_id VARCHAR(255) NOT NULL,
         format_version INT NOT NULL,
         meta JSON NOT NULL,
         inherited_event_count BIGINT NOT NULL DEFAULT 0,
         created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
         PRIMARY KEY (session_id)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
      `CREATE TABLE IF NOT EXISTS dsh_session_log (
         session_id VARCHAR(255) NOT NULL,
         seq BIGINT NOT NULL,
         event JSON NOT NULL,
         created_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
         PRIMARY KEY (session_id, seq)
       ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`,
    ],
  },
];

/** header 行 → SessionPersistenceSnapshot（revision 随 max_seq 前进而变化）。 */
function snapshotOf(row) {
  return {
    header: row.meta,
    revision: SessionPersistenceRevision(`${row.session_id}:${Number(row.max_seq ?? 0)}`),
    eventCount: row.event_count === undefined ? undefined : Number(row.event_count),
  };
}

/**
 * 单会话读写通道。变更按 handle 内 promise 链串行（对齐 JSONL 参照实现）；
 * 读经 DB——write handle 读自己的成功 append，满足「append 解析后同实例读可见」的新鲜度契约。
 */
class MySqlSessionHandle {
  constructor(storage, prepared, access) {
    this.storage = storage;
    this.id = prepared.id;
    this.header = prepared.meta;
    this.inheritedEventCount = prepared.inheritedEventCount;
    this.access = access;
    this.closed = false;
    this.cursor = prepared.nextSeq;
    this.chain = Promise.resolve();
  }

  assertOpen(operation) {
    if (this.closed) throw new SessionHandleClosedError(this.id, operation);
  }

  assertWrite(operation) {
    this.assertOpen(operation);
    if (this.access !== 'write') throw new SessionReadOnlyError(this.id, operation);
  }

  /** 串行化通道：变更排队执行；前一变更失败不阻断后续排队者（各自传播错误）。 */
  enqueue(op) {
    const run = this.chain.then(op, op);
    this.chain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  }

  async read(offset = 0, length, options = {}) {
    this.assertOpen('read');
    const rows = await this.storage.readEvents(this.id, offset, length, options.signal);
    // 读路径统一过官方校验（未知事件类型 fail-closed + 冻结）
    const events = validateStoredEvents(this.header, rows.map((r) => r.event));
    return { eventState: 'shared-frozen', events };
  }

  append(events, options = {}) {
    return this.enqueue(async () => {
      this.assertWrite('append');
      assertContiguous(this.id, events, this.cursor);
      const batch = materializeAppendBatch(events);
      await this.storage.appendBatch(this.id, batch, options.signal);
      this.cursor += batch.length;
    });
  }

  async flush(options = {}) {
    await this.chain;
    this.assertWrite('flush');
    options.signal?.throwIfAborted?.();
    // append 即事务提交：flush 退化为 materialize-if-needed（create 时 header 已落库、可列出）
  }

  async close() {
    if (this.closed) return;
    await this.chain;
    this.closed = true;
    this.storage.releaseOwnership(this.id, this);
  }

  [Symbol.asyncDispose]() {
    return this.close();
  }
}

/**
 * SessionPersistence 的 MySQL 实现（ctx.sessionPersistence）。
 * 池与迁移在 [Service.init] 完成（激活期 await，失败即启动失败——与 webserver 同模式）。
 */
export class MySqlSessionPersistence extends SessionPersistence {
  /** 类插件：无服务依赖（服务注册由 SessionPersistence 构造器完成）。 */
  static inject = [];
  static Config = z.object({
    jdbcUrl: z.string().required(),
    username: z.string().default(''),
    password: z.string().default(''),
  });

  constructor(ctx, config) {
    super(ctx);
    this.config = config;
    this.pool = undefined;
    /** 进程内写权登记：sessionId → handle（单写者契约的进程内半边）。 */
    this.owners = new Map();
  }

  async [Service.init]() {
    const { jdbcUrl, username, password } = this.config;
    if (!jdbcUrl) throw new Error('oaf-mysql-persistence: 缺少 config.jdbcUrl（CHECKPOINT_JDBC_URL）');
    const poolOpts = { jdbcUrl, username, password };
    await ensureDatabase(poolOpts);
    this.pool = createPool(poolOpts);
    await migrate(this.pool, MIGRATIONS, 'oaf_schema_version_mysql_persistence');
    this.install();
  }

  /**
   * 活事件路由与收尾（对齐 jsonl 参照实现的 install 契约）：
   * 发布态 session 的每个事件路由进写 handle；session/flush 已由「append 即事务提交」
   * 天然满足（无需 drain）；session/disposed 关闭 handle 释放写权。
   */
  install() {
    const disposers = [
      this.ctx.on('session/event', (session, event) => {
        const handle = this.owners.get(session.id);
        if (!handle) return;
        handle.append([event]).catch((err) => {
          this.ctx.logger?.warn?.(`oaf-mysql-persistence: session "${session.id}" 后台写入失败: ${err?.message ?? err}`);
        });
      }),
      this.ctx.on('session/disposed', (session) => {
        const handle = this.owners.get(session.id);
        if (!handle) return;
        handle.close().catch((err) => {
          this.ctx.logger?.warn?.(`oaf-mysql-persistence: session "${session.id}" 收尾关闭失败: ${err?.message ?? err}`);
        });
      }),
    ];
    this.ctx.effect(() => () => disposers.forEach((d) => d()));
  }

  releaseOwnership(id, handle) {
    if (this.owners.get(id) === handle) this.owners.delete(id);
  }

  async readHeaderRow(id) {
    const [rows] = await this.pool.query(
      'SELECT session_id, format_version, meta, inherited_event_count FROM dsh_session_header WHERE session_id = ?',
      [id],
    );
    return rows[0];
  }

  async readEvents(id, offset, length, signal) {
    signal?.throwIfAborted?.();
    if (length === undefined) {
      const [rows] = await this.pool.query(
        'SELECT seq, event FROM dsh_session_log WHERE session_id = ? AND seq >= ? ORDER BY seq ASC',
        [id, offset],
      );
      return rows;
    }
    const [rows] = await this.pool.query(
      'SELECT seq, event FROM dsh_session_log WHERE session_id = ? AND seq >= ? ORDER BY seq ASC LIMIT ?',
      [id, offset, Number(length)],
    );
    return rows;
  }

  async appendBatch(id, batch, signal) {
    signal?.throwIfAborted?.();
    await withTxn(this.pool, async (conn) => {
      for (const event of batch) {
        await conn.query(
          'INSERT INTO dsh_session_log (session_id, seq, event) VALUES (?, ?, ?)',
          [id, event.seq, JSON.stringify(event)],
        );
      }
    });
  }

  async create(header, options = {}) {
    options.signal?.throwIfAborted?.();
    const meta = materializeCreateHeader(header);
    const inherited = options.inheritedEventCount ?? 0;
    if (meta.isSeeded && inherited <= 0) {
      throw new TypeError(`oaf-mysql-persistence: isSeeded 会话必须提供正数 inheritedEventCount: ${meta.id}`);
    }
    if (!meta.isSeeded && inherited !== 0) {
      throw new TypeError(`oaf-mysql-persistence: 非 seeded 会话不接受 inheritedEventCount: ${meta.id}`);
    }
    try {
      await this.pool.query(
        'INSERT INTO dsh_session_header (session_id, format_version, meta, inherited_event_count) VALUES (?, ?, ?, ?)',
        [meta.id, meta.version, JSON.stringify(meta), inherited],
      );
    } catch (err) {
      if (err?.code === 'ER_DUP_ENTRY') throw new SessionAlreadyExistsError(meta.id);
      throw err;
    }
    return this.claimWriteHandle({
      id: meta.id,
      meta,
      inheritedEventCount: inherited,
      nextSeq: 0,
    });
  }

  async open(id, access, options = {}) {
    options.signal?.throwIfAborted?.();
    const row = await this.readHeaderRow(id);
    if (!row) throw new SessionPersistenceNotFoundError(id);
    const meta = row.meta;
    assertStoredId(id, meta);
    // assertVersion 对非当前 format_version fail-closed（SessionFormatUnsupportedError）
    assertVersion({ id, version: Number(row.format_version) });
    const [[{ next_seq }]] = await this.pool.query(
      'SELECT COALESCE(MAX(seq), -1) + 1 AS next_seq FROM dsh_session_log WHERE session_id = ?',
      [id],
    );
    const prepared = {
      id,
      meta,
      inheritedEventCount: Number(row.inherited_event_count ?? 0),
      nextSeq: Number(next_seq),
    };
    if (access !== 'write') return new MySqlSessionHandle(this, prepared, 'read');
    return this.claimWriteHandle(prepared);
  }

  /** 进程内单写者：登记成功才发放 write handle（dsh 契约的 SessionAlreadyOwnedError）。 */
  claimWriteHandle(prepared) {
    if (this.owners.has(prepared.id)) throw new SessionAlreadyOwnedError(prepared.id);
    const handle = new MySqlSessionHandle(this, prepared, 'write');
    this.owners.set(prepared.id, handle);
    return handle;
  }

  async flush() {
    // 服务级屏障：本实现 append 已逐批事务提交，全部写 handle 均已 durable
  }
  async stat(id, options = {}) {
    options.signal?.throwIfAborted?.();
    const [rows] = await this.pool.query(
      `SELECT h.session_id, h.meta, COALESCE(MAX(l.seq), -1) + 1 AS max_seq, COUNT(l.seq) AS event_count
         FROM dsh_session_header h LEFT JOIN dsh_session_log l ON l.session_id = h.session_id
        WHERE h.session_id = ? GROUP BY h.session_id, h.meta`,
      [id],
    );
    return rows[0] ? snapshotOf(rows[0]) : undefined;
  }

  async list(options = {}) {
    options.signal?.throwIfAborted?.();
    const [rows] = await this.pool.query(
      `SELECT h.session_id, h.meta, COALESCE(MAX(l.seq), -1) + 1 AS max_seq, COUNT(l.seq) AS event_count
         FROM dsh_session_header h LEFT JOIN dsh_session_log l ON l.session_id = h.session_id
        GROUP BY h.session_id, h.meta`,
    );
    return rows.map(snapshotOf);
  }
}

export { MySqlSessionPersistence as default };
