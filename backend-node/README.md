# Weather Serving API

Node.js REST API for `"default".weather_daily_metrics` in Lakebase.

## Setup

```bash
npm install
cp .env.example .env
```

Set `DATABASE_URL` to the Lakebase **Copy snippet** value. Authentication supports:

1. `DATABRICKS_PROFILE` plus `LAKEBASE_ENDPOINT` for automatically refreshed credentials.
2. `PGPASSWORD` containing the one-hour **Copy OAuth token** value.

## Run

```bash
npm test
npm start
```

The API listens on `http://localhost:3000` by default.

```text
GET /health
GET /api/weather/cities
GET /api/weather/daily?city=San%20Francisco&from=2024-01-01&to=2024-01-31
GET /api-docs
GET /openapi.json
```

Open `http://localhost:3000/api-docs` to inspect and execute requests with Swagger UI.
