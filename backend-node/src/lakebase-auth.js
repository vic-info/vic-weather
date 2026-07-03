const { execFile } = require("node:child_process");
const { promisify } = require("node:util");

const execFileAsync = promisify(execFile);
let cachedCredential;

async function getDatabaseCredential(config) {
  const now = Date.now();
  if (cachedCredential && cachedCredential.expiresAt - now > 5 * 60 * 1000) {
    return cachedCredential.token;
  }

  if (config.databricksProfile) {
    if (!config.lakebaseEndpoint) {
      throw new Error("LAKEBASE_ENDPOINT is required with DATABRICKS_PROFILE");
    }
    const { stdout } = await execFileAsync(
      process.env.DATABRICKS_CLI || "databricks",
      [
        "postgres",
        "generate-database-credential",
        config.lakebaseEndpoint,
        "--profile",
        config.databricksProfile,
        "--output",
        "json",
      ]
    );
    const credential = JSON.parse(stdout);
    cachedCredential = {
      token: credential.token,
      expiresAt: Date.parse(credential.expire_time),
    };
    return cachedCredential.token;
  }

  if (config.databasePassword) {
    return config.databasePassword;
  }

  throw new Error("DATABRICKS_PROFILE or PGPASSWORD is required");
}

module.exports = { getDatabaseCredential };
