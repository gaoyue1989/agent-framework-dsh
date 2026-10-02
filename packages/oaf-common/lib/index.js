/**
 * OAF dsh 运行时公共件。
 *
 * - parseJdbcUrl：CHECKPOINT_JDBC_URL（jdbc:mysql://host:port/db?params）→ 连接参数
 * - createPool / withTxn：mysql2 连接池与事务包装
 * - migrate：20 行级轻量版本表迁移器（设计 §6.3：Node 侧不引 Flyway，
 *   保留纪律——只新增版本、禁止改已合并版本、失败即启动失败）
 *
 * @module @oaf/oaf-common
 */
import mysql from 'mysql2/promise';

/** 解析 AF 契约的 CHECKPOINT_JDBC_URL；非法输入抛错（fail-fast，不吞）。 */
export function parseJdbcUrl(jdbcUrl) {
  const m = /^jdbc:mysql:\/\/([^/:?]+)(?::(\d+))?\/([^?]+)(?:\?(.*))?$/.exec(String(jdbcUrl ?? ''));
  if (!m) throw new Error(`oaf-common: 非法 CHECKPOINT_JDBC_URL: ${jdbcUrl}`);
  const [, host, port, database, query] = m;
  const params = new URLSearchParams(query ?? '');
  return {
    host,
    port: Number(port ?? 3306),
    database,
    sslMode: params.get('sslMode') ?? params.get('useSSL') ?? undefined,
  };
}

/** 创建 MySQL 连接池（database 不存在时由调用方先 ensureDatabase）。 */
export function createPool({ jdbcUrl, username, password }, extra = {}) {
  const { host, port, database } = parseJdbcUrl(jdbcUrl);
  return mysql.createPool({
    host,
    port,
    user: username,
    password,
    database,
    connectionLimit: Number(process.env.OAF_MYSQL_POOL_SIZE ?? 8),
    // JSON 列可读性：不启用 string 化，mysql2 默认对 JSON 列返回已解析对象
    charset: 'utf8mb4',
    ...extra,
  });
}

/** 确保目标库存在（首次启动/新环境自举；库存在时为 no-op）。 */
export async function ensureDatabase({ jdbcUrl, username, password }) {
  const { host, port, database } = parseJdbcUrl(jdbcUrl);
  const conn = await mysql.createConnection({ host, port, user: username, password });
  try {
    await conn.query(
      `CREATE DATABASE IF NOT EXISTS \`${database}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_0900_ai_ci`,
    );
  } finally {
    await conn.end();
  }
}

/** 事务包装：fn 拿到连接，抛错即回滚。 */
export async function withTxn(pool, fn) {
  const conn = await pool.getConnection();
  try {
    await conn.beginTransaction();
    const out = await fn(conn);
    await conn.commit();
    return out;
  } catch (err) {
    try { await conn.rollback(); } catch { /* 回滚失败以原错误为准 */ }
    throw err;
  } finally {
    conn.release();
  }
}

/**
 * 轻量版本表迁移器。migrations: [{ version, name, sql: string | string[] }]，
 * 版本必须从 1 连续递增；已应用版本跳过；应用按序执行且与版本号写入同事务。
 * 对齐 AF Flyway 纪律：启动时执行、失败即启动失败（fail-fast）、已合并版本内容不可再改
 * （内容变更靠名称一致性校验兜底）。
 *
 * @param tableName 版本表名——同一数据库上多个插件各自迁移时必须各用独立表
 *   （如 `oaf_schema_version_oaf_server`），避免版本号空间互撞。
 */
export async function migrate(pool, migrations, tableName = 'oaf_schema_version') {
  const table = `\`${tableName}\``;
  await pool.query(
    `CREATE TABLE IF NOT EXISTS ${table} (
       version INT NOT NULL PRIMARY KEY,
       name VARCHAR(255) NOT NULL,
       applied_at DATETIME(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3)
     ) ENGINE=InnoDB`,
  );
  const [rows] = await pool.query(`SELECT version, name FROM ${table} ORDER BY version`);
  const applied = new Map(rows.map((r) => [r.version, r.name]));
  let expect = 1;
  for (const mig of migrations) {
    if (mig.version !== expect) {
      throw new Error(`oaf-common: 迁移版本必须连续递增，期望 ${expect}，得到 ${mig.version}`);
    }
    expect += 1;
    if (applied.has(mig.version)) {
      if (applied.get(mig.version) !== mig.name) {
        throw new Error(
          `oaf-common: 已合并迁移 ${tableName} V${mig.version} 内容不可改（记录名 ${applied.get(mig.version)} ≠ ${mig.name}）`,
        );
      }
      continue;
    }
    const stmts = Array.isArray(mig.sql) ? mig.sql : [mig.sql];
    await withTxn(pool, async (conn) => {
      for (const sql of stmts) await conn.query(sql);
      await conn.query(`INSERT INTO ${table} (version, name) VALUES (?, ?)`, [
        mig.version,
        mig.name,
      ]);
    });
  }
}
