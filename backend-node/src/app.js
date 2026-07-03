const express = require("express");
const cors = require("cors");
const fs = require("node:fs");
const path = require("node:path");
const swaggerUi = require("swagger-ui-express");
const YAML = require("yaml");
const { createWeatherRouter } = require("./routes/weather");

const openapi = YAML.parse(
  fs.readFileSync(path.resolve(__dirname, "../openapi.yaml"), "utf8")
);

function createApp(options = {}) {
  let query = options.query;
  if (!query) {
    const { pool } = require("./db");
    query = pool.query.bind(pool);
  }

  const app = express();

  app.disable("x-powered-by");
  app.use(cors());
  app.use(express.json());

  app.get("/openapi.json", (req, res) => res.json(openapi));
  app.use("/api-docs", swaggerUi.serve, swaggerUi.setup(openapi));

  app.get("/health", async (req, res, next) => {
    try {
      await query("SELECT 1");
      res.json({ status: "ok", database: "connected" });
    } catch (error) {
      next(error);
    }
  });

  app.use("/api/weather", createWeatherRouter(query));

  app.use((req, res) => {
    res.status(404).json({ error: "Not found" });
  });

  app.use((error, req, res, next) => {
    console.error(error.message);
    res.status(500).json({ error: "Internal server error" });
  });

  return app;
}

module.exports = { createApp };
