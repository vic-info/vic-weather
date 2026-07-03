const assert = require("node:assert/strict");
const test = require("node:test");

const { createApp } = require("../src/app");

async function withServer(query, run) {
  const server = createApp({ query }).listen(0);
  await new Promise((resolve) => server.once("listening", resolve));
  const { port } = server.address();

  try {
    await run(`http://127.0.0.1:${port}`);
  } finally {
    await new Promise((resolve, reject) => {
      server.close((error) => (error ? reject(error) : resolve()));
    });
  }
}

test("health reports a connected database", async () => {
  await withServer(async () => ({ rows: [{ one: 1 }] }), async (baseUrl) => {
    const response = await fetch(`${baseUrl}/health`);
    assert.equal(response.status, 200);
    assert.deepEqual(await response.json(), { status: "ok", database: "connected" });
  });
});

test("serves the OpenAPI contract and Swagger UI", async () => {
  await withServer(async () => ({ rows: [] }), async (baseUrl) => {
    const specification = await fetch(`${baseUrl}/openapi.json`);
    const openapi = await specification.json();
    const swagger = await fetch(`${baseUrl}/api-docs/`);

    assert.equal(specification.status, 200);
    assert.equal(openapi.openapi, "3.0.3");
    assert.ok(openapi.paths["/api/weather/daily"]);
    assert.equal(swagger.status, 200);
    assert.match(await swagger.text(), /Swagger UI/);
  });
});

test("daily returns stable JSON and parameterizes the SQL query", async () => {
  let capturedParameters;
  const query = async (sql, parameters) => {
    assert.match(sql, /\$1/);
    assert.match(sql, /"default"\.weather_daily_metrics/);
    capturedParameters = parameters;
    return {
      rows: [{ city: "Austin", weatherDate: "2024-01-01", maxTempC: 13.1 }],
      rowCount: 1,
    };
  };

  await withServer(query, async (baseUrl) => {
    const response = await fetch(
      `${baseUrl}/api/weather/daily?city=Austin&from=2024-01-01&to=2024-01-02`
    );
    const body = await response.json();

    assert.equal(response.status, 200);
    assert.deepEqual(capturedParameters, ["Austin", "2024-01-01", "2024-01-02"]);
    assert.equal(body.meta.count, 1);
    assert.equal(body.data[0].weatherDate, "2024-01-01");
  });
});

test("daily rejects missing or invalid date parameters before querying", async () => {
  let queryCount = 0;
  const query = async () => {
    queryCount += 1;
    return { rows: [], rowCount: 0 };
  };

  await withServer(query, async (baseUrl) => {
    const missing = await fetch(`${baseUrl}/api/weather/daily?city=Austin`);
    const invalid = await fetch(
      `${baseUrl}/api/weather/daily?city=Austin&from=2024-02-01&to=2024-01-01`
    );

    assert.equal(missing.status, 400);
    assert.equal(invalid.status, 400);
    assert.equal(queryCount, 0);
  });
});
