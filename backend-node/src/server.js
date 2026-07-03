const { createApp } = require("./app");
const { pool, verifyDatabase } = require("./db");
const { loadConfig } = require("./config");

async function startServer() {
  const config = loadConfig();
  const database = await verifyDatabase();
  console.log(`Connected to Lakebase database: ${database.database_name}`);

  const server = createApp().listen(config.port, () => {
    console.log(`Weather API listening on http://localhost:${config.port}`);
  });

  async function shutdown(signal) {
    console.log(`${signal} received, shutting down`);
    server.close(async () => {
      await pool.end();
      process.exit(0);
    });
  }

  process.on("SIGINT", () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

startServer().catch((error) => {
  console.error(`Failed to start Weather API: ${error.message}`);
  process.exit(1);
});
