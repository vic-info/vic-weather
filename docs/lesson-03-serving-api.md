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

### 在 Workflow 中添加 DQ Condition

不要让 sync task 只依赖 Gold task。使用 Lesson 2 DQ notebook 输出的 task value 建立质量门：

```text
data_quality_checks
        ↓  Run if: All done
check_dq_passed (If/else condition)
        ↓  true
sync_gold_to_lakebase
```

在 Jobs UI 中配置：

1. 将 DQ notebook task key 设为 `data_quality_checks`。
2. 新建 **If/else condition** task，task key 使用 `check_dq_passed`。
3. 依赖 `data_quality_checks`，`Run if dependencies` 选择 **All done**。
4. 左值填写 `{{tasks.data_quality_checks.values.dq_passed}}`。
5. Operator 选择 **Equals**，右值填写 `true`。
6. `sync_gold_to_lakebase` 依赖 condition task 的 **true** outcome。

DQ 失败时 notebook 会先保存 `dq_passed=false`，随后抛出异常，因此整个 Job 保持失败状态；
condition task 仍可评估结果，但 Lakebase sync 不会运行。

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
├── test/
└── src/
    ├── app.js
    ├── config.js
    ├── lakebase-auth.js
    ├── server.js
    ├── db.js
    └── routes/
        └── weather.js
```

初始化依赖：

```bash
cd backend-node
npm install
```

课堂不引入 ORM。直接使用 `pg` 可以让学生看清 endpoint 到 SQL 的完整路径。

## Step 6：数据库连接

从 Lakebase 的 **Connect to your database** 对话框获取：

- **Copy snippet**：Postgres `DATABASE_URL`，其中没有 password。
- **Copy OAuth token**：一小时有效的数据库 password，不是 workspace API token。

推荐课堂环境使用 Databricks CLI profile 自动刷新数据库凭证。创建 profile 时包含
`postgres` scope，然后在 `backend-node/.env` 配置：

```dotenv
DATABASE_URL=postgresql://username@host:5432/databricks_postgres?sslmode=require
DATABRICKS_PROFILE=weather-course
LAKEBASE_ENDPOINT=projects/vic-weather-db/branches/production/endpoints/primary
PORT=3000
```

如果不用 CLI，则删除 `DATABRICKS_PROFILE` 和 `LAKEBASE_ENDPOINT`，改为：

```dotenv
PGPASSWORD=粘贴 Copy OAuth token 的结果
```

该 token 一小时后失效，需要重新复制。真实 URL 和 token 只放在本地 `.env`，不提交到 Git。

连接池必须把 URL 拆成独立字段。Lakebase snippet 没有 password；若同时使用
`connectionString` 和单独的 password，部分 `pg` 版本会让 URL 中的空 password 覆盖 OAuth token。

`src/db.js` 的核心配置：

```javascript
const { Pool } = require("pg");
const { getDatabaseCredential } = require("./lakebase-auth");

const connectionUrl = new URL(config.databaseUrl);

const pool = new Pool({
  host: connectionUrl.hostname,
  port: Number(connectionUrl.port || 5432),
  user: decodeURIComponent(connectionUrl.username),
  database: decodeURIComponent(connectionUrl.pathname.slice(1)),
  password: () => getDatabaseCredential(config),
  ssl: { rejectUnauthorized: false },
});
```

## Step 7：最小 Express Server

项目已将 app 创建和进程启动分开：

```javascript
const database = await verifyDatabase();
const server = createApp().listen(config.port);
```

`/health` 会执行 `SELECT 1`，因此返回 200 代表 HTTP server 和数据库都可用。

## Step 8：参数化查询 Route

`src/routes/weather.js`：

```javascript
const express = require("express");
const { pool } = require("../db");

const router = express.Router();

router.get("/cities", async (req, res, next) => {
  try {
    const result = await pool.query(
      "SELECT DISTINCT city FROM \"default\".weather_daily_metrics ORDER BY city"
    );
    res.json({ data: result.rows.map((row) => row.city) });
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
              weather_date::date::text AS "weatherDate",
              max_temp_c AS "maxTempC",
              min_temp_c AS "minTempC",
              mean_temp_c AS "meanTempC",
              precipitation_mm AS "precipitationMm",
              max_wind_speed_kmh AS "maxWindSpeedKmh",
              weather_severity_score AS "weatherSeverityScore"
       FROM "default".weather_daily_metrics
       WHERE city = $1
         AND weather_date BETWEEN $2::date AND $3::date
       ORDER BY weather_date`,
      [city, from, to]
    );
    res.json({
      data: result.rows,
      meta: { city, from, to, count: result.rowCount },
    });
  } catch (error) {
    next(error);
  }
});

module.exports = router;
```

## Step 9：运行和测试

启动：

```bash
cd backend-node
npm test
npm start
```

测试：

```bash
curl "http://localhost:3000/health"
curl "http://localhost:3000/api/weather/cities"
curl "http://localhost:3000/api/weather/daily?city=San%20Francisco&from=2024-01-01&to=2024-01-31"
```

浏览器打开 `http://localhost:3000/api-docs`，可以查看参数、response schema 并直接执行请求。
`http://localhost:3000/openapi.json` 返回原始 OpenAPI contract。Swagger 只做简短演示，
不在课堂现场编写完整 OpenAPI 文档。

## Demo 顺序

1. 展示 Raw、Bronze、Silver、Gold 链路。
2. 展示 Data Quality `10/10 passed`。
3. 展示 `dq_passed=true` 和 condition task 的 true outcome。
4. 查询 Lakebase/Postgres daily serving table。
5. 启动 Node.js API。
6. 调用 `/health`、`/cities`、`/daily`。
7. 展示 Swagger、Postman 或 curl 返回的 JSON。

## 验收标准

- Serving database 中存在 daily 表。
- Condition task 只在 `dq_passed=true` 时运行 Lakebase sync。
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
