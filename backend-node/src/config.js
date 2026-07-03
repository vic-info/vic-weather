const fs = require("node:fs");
const path = require("node:path");

function parseEnvFile(filePath) {
  if (!fs.existsSync(filePath)) {
    return {};
  }

  const config = {};
  const rawValues = [];

  for (const rawLine of fs.readFileSync(filePath, "utf8").split(/\r?\n/)) {
    const line = rawLine.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const assignment = line.match(/^([A-Za-z_][A-Za-z0-9_]*)=(.*)$/);
    if (assignment) {
      const value = assignment[2].trim().replace(/^['"]|['"]$/g, "");
      config[assignment[1]] = value;
    } else {
      rawValues.push(line);
    }
  }

  config.DATABASE_URL ??= rawValues.find((value) => /^postgres(ql)?:\/\//.test(value));
  config.PGPASSWORD ??= rawValues.find(
    (value) => !/^postgres(ql)?:\/\//.test(value)
  );
  return config;
}

function loadConfig() {
  const backendEnvPath = path.resolve(__dirname, "../.env");
  const rootEnvPath = path.resolve(__dirname, "../../.env");
  const backendConfig = parseEnvFile(process.env.ENV_FILE || backendEnvPath);
  const connectionConfig = parseEnvFile(rootEnvPath);
  const databaseUrl = (
    process.env.DATABASE_URL
    || backendConfig.DATABASE_URL
    || connectionConfig.DATABASE_URL
  );
  const databasePassword = (
    process.env.PGPASSWORD
    || backendConfig.PGPASSWORD
    || connectionConfig.PGPASSWORD
  );
  const databricksToken = (
    process.env.DATABRICKS_TOKEN
    || backendConfig.DATABRICKS_TOKEN
    || connectionConfig.DATABRICKS_TOKEN
  );
  const lakebaseEndpoint = (
    process.env.LAKEBASE_ENDPOINT
    || backendConfig.LAKEBASE_ENDPOINT
    || connectionConfig.LAKEBASE_ENDPOINT
  );

  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required");
  }

  return {
    databaseUrl,
    databasePassword,
    databricksHost: process.env.DATABRICKS_HOST || backendConfig.DATABRICKS_HOST,
    databricksProfile: process.env.DATABRICKS_PROFILE || backendConfig.DATABRICKS_PROFILE,
    databricksToken,
    lakebaseEndpoint,
    port: Number(process.env.PORT || backendConfig.PORT || 3000),
  };
}

module.exports = { loadConfig, parseEnvFile };
