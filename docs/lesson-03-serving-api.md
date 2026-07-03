# Lesson 3：Gold → Serving Layer → Node.js REST API

## 课程目标

本节课用 2 小时完成最后一段 serving 链路：

```text
Gold Delta Tables
        ↓
Lakebase/Postgres Serving Tables
        ↓
Node.js + Express REST API
        ↓
Swagger / Postman / curl
```

学生完成后应该能解释：

1. 为什么 API 不应该直接查询 Spark / Delta。
2. Serving layer 在数据产品里的职责。
3. Lakebase/Postgres 表如何承接 Gold 表。
4. Express API 如何用参数化 SQL 查询 serving table 并返回 JSON。

## 本节课边界

### 核心必讲

- Serving layer 的作用。
- Gold table 到 Postgres-compatible table 的映射。
- Node.js、Express 和 `pg` 的最小查询链路。
- REST endpoint、query parameters 和稳定的 JSON response。

### 简讲

- Lakebase synced table：只讲从 Gold Delta 到 serving table。
- Swagger：只作为 API 文档和测试入口。

### 不在本节讲

- React frontend。
- OAuth / service principal。
- Continuous sync。
- ORM 和复杂分层架构。
- 生产级 API auth、rate limiting 和 deployment。
- Spring Boot 替代实现。

## 时间安排

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 0:00-0:20 | 解释 serving layer | 学生理解为什么需要 Postgres/Lakebase |
| 0:20-0:45 | 同步 Gold daily 表 | daily serving table |
| 0:45-1:05 | 用 SQL 验证 serving table | daily 查询成功 |
| 1:05-1:40 | 创建最小 Express API | `/cities`、`/daily` |
| 1:40-1:55 | Swagger / Postman / curl 测试 | JSON response |
| 1:55-2:00 | 项目总结 | end-to-end demo |

## 课前准备

学生需要已经完成 Lesson 2，并且 Databricks 中存在：

```text
workspace.default.gold_city_daily_weather_metrics
workspace.default.gold_city_monthly_weather_summary
workspace.default.gold_weather_risk_days
```

教师提前准备：

1. Lakebase project 或普通 Postgres 替代环境。
2. 已同步的 daily serving table。
3. 数据库连接字符串。
4. Node.js 20+ 和 npm。
5. Postman 或浏览器。

Lakebase synced tables 可以在 Catalog UI 中逐张创建，也可以运行：

```text
databricks/05_sync_gold_to_lakebase.py
```

该 notebook 使用 Snapshot 模式创建 daily synced table。当前课程环境的 `DATABASE_TABLE_SYNC` active pipeline 配额为 1，因此 `SYNC_TABLES` 列表只启用 `weather_daily_metrics`。Monthly 和 risk synced tables 保留为升级配额后的扩展。运行前应先确认 Lesson 2 DQ 已通过。

## Step 1：为什么需要 Serving Layer

```text
Databricks Delta:
负责数据处理、历史分析、事实来源。

Lakebase/Postgres:
负责低延迟查询和 API serving。

Node.js + Express:
负责 HTTP API、参数校验和 JSON response。
```

课堂重点：

- Spark / Delta 适合 batch analytics，不适合每个 API request 直接查询。
- API 需要低延迟、稳定连接和简单查询模型。
- Serving table 是 Gold table 面向在线查询的副本。

## Step 2：Gold 表到 Serving 表映射

```text
gold_city_daily_weather_metrics    → weather_daily_metrics
```

当前环境只允许 1 个 active database-table-sync pipeline。Monthly 和 risk 仍保留在 Gold 层，升级配额后可按相同方式增加到 `SYNC_TABLES` 列表。第一版使用 Snapshot，不在课堂实现 continuous sync。

## Step 3：先验证 Serving Tables

在 Lakebase SQL Editor 或 Postgres client 中运行：

```sql
SELECT * FROM weather_daily_metrics LIMIT 10;
```

业务查询示例：

```sql
SELECT *
FROM weather_daily_metrics
WHERE city = 'San Francisco'
  AND weather_date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY weather_date;

```

先验证 SQL，再写 API。SQL 本身查不到数据时，不要先修改 Express route。

## Step 4：API Endpoint 设计

课堂实现：

```text
GET /health
GET /api/weather/cities
GET /api/weather/daily?city=San Francisco&from=2024-01-01&to=2024-01-31
```

课后扩展：

```text
GET /api/weather/monthly?city=San Francisco&year=2024
GET /api/weather/risks?city=San Francisco
GET /api/weather/risks?city=San Francisco&riskType=HEAVY_RAIN
```

课堂重点：

- Endpoint 围绕用户问题设计，不直接暴露数据库结构。
- Query parameters 用于 city、date range、year 等过滤条件。
- SQL 必须参数化，不能拼接用户输入。
- API response 使用稳定的 camelCase 字段。

## Step 5：Node.js 项目结构

```text
backend-node/
├── package.json
├── .env.example
└── src/
    ├── server.js
    ├── db.js
    └── routes/
        └── weather.js
```

初始化依赖：

```bash
mkdir backend-node
cd backend-node
npm init -y
npm install express pg dotenv cors
npm install --save-dev nodemon
```

课堂不引入 ORM。直接使用 `pg` 可以让学生看清 endpoint 到 SQL 的完整路径。

## Step 6：数据库连接

`.env.example`：

```dotenv
DATABASE_URL=postgresql://username:password@host:5432/weather_serving
PGSSL=true
PORT=3000
```

`src/db.js`：

```javascript
const { Pool } = require("pg");

const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: process.env.PGSSL === "true"
    ? { rejectUnauthorized: false }
    : false,
});

module.exports = pool;
```

真实密码只放在本地 `.env` 或 secret manager 中，不提交到 Git。

## Step 7：最小 Express Server

`src/server.js`：

```javascript
require("dotenv").config();

const express = require("express");
const cors = require("cors");
const weatherRouter = require("./routes/weather");

const app = express();
const port = Number(process.env.PORT || 3000);

app.use(cors());
app.use(express.json());

app.get("/health", (req, res) => {
  res.json({ status: "ok" });
});

app.use("/api/weather", weatherRouter);

app.use((error, req, res, next) => {
  console.error(error);
  res.status(500).json({ error: "Internal server error" });
});

app.listen(port, () => {
  console.log(`Weather API listening on port ${port}`);
});
```

## Step 8：参数化查询 Route

`src/routes/weather.js`：

```javascript
const express = require("express");
const pool = require("../db");

const router = express.Router();

router.get("/cities", async (req, res, next) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT city FROM weather_daily_metrics ORDER BY city"
    );
    res.json(result.rows.map((row) => row.city));
  } catch (error) {
    next(error);
  }
});

router.get("/daily", async (req, res, next) => {
  const { city, from, to } = req.query;
  if (!city || !from || !to) {
    return res.status(400).json({ error: "city, from and to are required" });
  }

  try {
    const result = await pool.query(
      `SELECT city,
              weather_date AS "weatherDate",
              max_temp_c AS "maxTempC",
              min_temp_c AS "minTempC",
              mean_temp_c AS "meanTempC",
              precipitation_mm AS "precipitationMm",
              max_wind_speed_kmh AS "maxWindSpeedKmh",
              weather_severity_score AS "weatherSeverityScore"
       FROM weather_daily_metrics
       WHERE city = $1 AND weather_date BETWEEN $2 AND $3
       ORDER BY weather_date`,
      [city, from, to]
    );
    res.json(result.rows);
  } catch (error) {
    next(error);
  }
});

module.exports = router;
```

## Step 9：运行和测试

在 `package.json` 中添加：

```json
{
  "scripts": {
    "dev": "nodemon src/server.js",
    "start": "node src/server.js"
  }
}
```

启动：

```bash
npm run dev
```

测试：

```bash
curl "http://localhost:3000/health"
curl "http://localhost:3000/api/weather/cities"
curl "http://localhost:3000/api/weather/daily?city=San%20Francisco&from=2024-01-01&to=2024-01-31"
```

Swagger 只做简短演示，不在课堂现场编写完整 OpenAPI 文档。

## Demo 顺序

1. 展示 Raw、Bronze、Silver、Gold 链路。
2. 展示 Data Quality `10/10 passed`。
3. 查询 Lakebase/Postgres daily serving table。
4. 启动 Node.js API。
5. 调用 `/health`、`/cities`、`/daily`。
6. 展示 Swagger、Postman 或 curl 返回的 JSON。

## 验收标准

- Serving database 中存在 daily 表。
- SQL 可以查询 daily 数据。
- Node.js API 可以通过 `npm start` 启动。
- `/health` 返回 `{"status":"ok"}`。
- `/api/weather/cities` 返回城市列表。
- `/api/weather/daily` 返回指定城市和日期范围的数据。
- 所有包含用户输入的 SQL 都使用参数化查询。

## 课堂提问

1. 为什么 API 不直接查询 Spark / Delta？
2. Serving table 和 Gold table 的关系是什么？
3. 为什么不能把 city 直接拼接进 SQL？
4. 为什么先验证 SQL，再实现 Express route？
5. Gold table 更新后，serving table 如何保持同步？

## 课后作业

1. 配额允许后同步 monthly/risk 表，并添加 `/monthly` 和 `/risks` endpoints。
2. 给 `/daily` 添加日期格式和日期范围校验。
3. 添加 OpenAPI 文档和 Swagger UI。
4. 为 routes 添加集成测试。

进阶作业：

1. 用 React 创建天气 dashboard。
2. 用 OAuth / service principal 替代 password auth。
3. 把 snapshot sync 改成 continuous sync。
4. 使用 TypeScript 重写 Node.js backend。
