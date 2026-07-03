const assert = require("node:assert/strict");
const fs = require("node:fs");
const os = require("node:os");
const path = require("node:path");
const test = require("node:test");

const { parseEnvFile } = require("../src/config");

test("parses standard dotenv values", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "weather-env-"));
  const file = path.join(directory, ".env");
  fs.writeFileSync(file, "DATABASE_URL=postgresql://user@host/db\nPGPASSWORD=secret\n");

  const config = parseEnvFile(file);
  assert.equal(config.DATABASE_URL, "postgresql://user@host/db");
  assert.equal(config.PGPASSWORD, "secret");
});

test("parses the course raw token and URL format", () => {
  const directory = fs.mkdtempSync(path.join(os.tmpdir(), "weather-env-"));
  const file = path.join(directory, ".env");
  fs.writeFileSync(file, "short-lived-token\n\npostgresql://user@host/db?sslmode=require\n");

  const config = parseEnvFile(file);
  assert.equal(config.DATABASE_URL, "postgresql://user@host/db?sslmode=require");
  assert.equal(config.PGPASSWORD, "short-lived-token");
});
