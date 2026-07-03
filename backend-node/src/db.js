const { Pool } = require("pg");
const { loadConfig } = require("./config");
const { getDatabaseCredential } = require("./lakebase-auth");

const config = loadConfig();
const connectionUrl = new URL(config.databaseUrl);
const sslMode = connectionUrl.searchParams.get("sslmode");

const pool = new Pool({
  host: connectionUrl.hostname,
  port: Number(connectionUrl.port || 5432),
  user: decodeURIComponent(connectionUrl.username),
  database: decodeURIComponent(connectionUrl.pathname.slice(1)),
  password: () => getDatabaseCredential(config),
  ssl: sslMode === "disable" ? false : { rejectUnauthorized: false },
  connectionTimeoutMillis: 10000,
  idleTimeoutMillis: 30000,
  max: 5,
});

async function verifyDatabase() {
  const result = await pool.query(
    "SELECT current_database() AS database_name, current_user AS user_name"
  );
  return result.rows[0];
}

module.exports = { pool, verifyDatabase };
