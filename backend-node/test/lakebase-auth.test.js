const assert = require("node:assert/strict");
const test = require("node:test");

const { getDatabaseCredential } = require("../src/lakebase-auth");

test("uses a copied Lakebase OAuth token as the database password", async () => {
  const token = await getDatabaseCredential({ databasePassword: "short-lived-token" });
  assert.equal(token, "short-lived-token");
});
