# Lesson 3：Gold → Serving Layer → REST API

## 课程目标

本节课用 2 小时完成最后一段 serving 链路：

```text
Gold Delta Tables
        ↓
Lakebase/Postgres Serving Tables
        ↓
Spring Boot REST API
        ↓
Swagger / Postman
```

学生完成后应该能解释：

1. 为什么 API 不应该直接查询 Spark / Delta。
2. Serving layer 在数据产品里的职责。
3. Lakebase/Postgres 表如何承接 Gold 表。
4. Spring Boot API 如何查询 serving table 并返回 JSON。

## 本节课边界

### 核心必讲

- Serving layer 的作用。
- Gold table 到 Postgres-compatible table 的映射。
- REST API endpoint 设计。
- Spring Boot 中 JDBC 查询和 JSON response 的基本链路。

### 简讲

- Lakebase synced table：只讲从 Gold Delta 到 serving table。
- Swagger：只作为 API 测试界面。

### 不在本节讲

- Node.js backend。
- React frontend。
- OAuth / service principal。
- Continuous sync。
- 复杂权限模型。
- 生产级 API auth。

## 时间安排

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 0:00-0:20 | 解释 serving layer | 学生理解为什么需要 Postgres/Lakebase |
| 0:20-0:45 | 同步或导出 Gold 表 | serving tables |
| 0:45-1:05 | 用 SQL 验证 serving tables | daily/monthly/risk 查询成功 |
| 1:05-1:40 | 创建最小 Spring Boot API | `/cities`、`/daily`、`/monthly` |
| 1:40-1:55 | Swagger / Postman 测试 | JSON response |
| 1:55-2:00 | 项目包装和简历表达 | demo script / resume bullets |

## 课前准备

学生需要已经完成 Lesson 2，并且 Databricks 中存在：

```text
workspace.default.gold_city_daily_weather_metrics
workspace.default.gold_city_monthly_weather_summary
workspace.default.gold_weather_risk_days
```

教师提前准备：

1. Lakebase project 或普通 Postgres 替代环境。
2. 数据库连接信息。
3. Spring Boot starter project。
4. Postman 或 Swagger 测试页面。

## Step 1：为什么需要 Serving Layer

先讲清楚分工：

```text
Databricks Delta:
负责数据处理、历史分析、事实来源。

Lakebase/Postgres:
负责低延迟查询和 API serving。

Spring Boot:
负责业务 API 暴露和 response shape。
```

课堂重点：

- Spark / Delta 适合 batch analytics，不适合每次 API request 都直接查。
- API 需要低延迟、稳定连接、简单查询模型。
- Serving table 通常是 Gold table 的查询友好副本。

## Step 2：Gold 表到 Serving 表映射

Gold 表：

```text
workspace.default.gold_city_daily_weather_metrics
workspace.default.gold_city_monthly_weather_summary
workspace.default.gold_weather_risk_days
```

Serving 表：

```text
weather_daily_metrics
weather_monthly_summary
weather_risk_days
```

映射关系：

```text
gold_city_daily_weather_metrics   → weather_daily_metrics
gold_city_monthly_weather_summary → weather_monthly_summary
gold_weather_risk_days            → weather_risk_days
```

课堂重点：

- 表名在 serving layer 里可以更短、更面向 API。
- Serving table 不一定要暴露所有 Gold 字段。
- 第一版用 snapshot sync 或手动导出即可，不追求实时。

## Step 3：验证 Serving Tables

在 Lakebase SQL Editor 或 Postgres client 中运行：

```sql
SELECT *
FROM weather_daily_metrics
LIMIT 10;

SELECT *
FROM weather_monthly_summary
LIMIT 10;

SELECT *
FROM weather_risk_days
LIMIT 10;
```

业务查询示例：

```sql
SELECT *
FROM weather_daily_metrics
WHERE city = 'San Francisco'
  AND weather_date BETWEEN '2024-01-01' AND '2024-01-31'
ORDER BY weather_date;

SELECT *
FROM weather_monthly_summary
WHERE city = 'San Francisco'
  AND year = 2024
ORDER BY month;

SELECT *
FROM weather_risk_days
WHERE city = 'San Francisco'
ORDER BY weather_date;
```

课堂重点：

- 先用 SQL 验证 serving tables，再写 API。
- 如果 SQL 本身查不到数据，API 层不应该先背锅。
- API endpoint 基本就是把常用 SQL 查询参数化。

## Step 4：API Endpoint 设计

课堂主线只实现 3 个 endpoint：

```text
GET /api/weather/cities
GET /api/weather/daily?city=San Francisco&from=2024-01-01&to=2024-01-31
GET /api/weather/monthly?city=San Francisco&year=2024
```

可选 endpoint：

```text
GET /api/weather/risks?city=San Francisco
GET /api/weather/risks?city=San Francisco&riskType=HEAVY_RAIN
```

课堂重点：

- Endpoint 应该围绕用户问题设计，不围绕数据库表名设计。
- Query params 适合过滤条件，例如 city、from、to、year。
- Response shape 应该稳定，字段名用 API 友好的 camelCase。

## Step 5：Spring Boot 最小结构

推荐课堂只讲最小必要结构：

```text
backend-springboot/
├── pom.xml
└── src/main/java/com/example/weather/
    ├── WeatherApiApplication.java
    ├── controller/
    │   └── WeatherController.java
    ├── service/
    │   └── WeatherService.java
    ├── repository/
    │   └── WeatherRepository.java
    └── dto/
        ├── DailyWeatherResponse.java
        └── MonthlyWeatherResponse.java
```

课堂重点：

- Controller 负责 HTTP request / response。
- Service 负责业务入口，第一版可以很薄。
- Repository 负责 SQL 查询。
- DTO 负责 API response shape。

不要在本节展开复杂 Spring 架构。

## Step 6：application.yml

本地开发使用环境变量：

```yaml
spring:
  datasource:
    url: ${LAKEBASE_JDBC_URL}
    username: ${LAKEBASE_USERNAME}
    password: ${LAKEBASE_PASSWORD}
    driver-class-name: org.postgresql.Driver

server:
  port: 8080

springdoc:
  swagger-ui:
    path: /swagger-ui.html
```

运行前设置：

```bash
export LAKEBASE_JDBC_URL="jdbc:postgresql://<host>:5432/weather_serving"
export LAKEBASE_USERNAME="<username>"
export LAKEBASE_PASSWORD="<password>"
```

课堂重点：

- 不要把真实 password commit 到 GitHub。
- 环境变量是第一版最简单可教的做法。
- OAuth / service principal 放到课后扩展。

## Step 7：API 测试

启动 Spring Boot：

```bash
mvn spring-boot:run
```

测试：

```bash
curl "http://localhost:8080/api/weather/cities"
curl "http://localhost:8080/api/weather/daily?city=San%20Francisco&from=2024-01-01&to=2024-01-31"
curl "http://localhost:8080/api/weather/monthly?city=San%20Francisco&year=2024"
```

Swagger：

```text
http://localhost:8080/swagger-ui.html
```

课堂重点：

- 先看 `/cities`，因为它最容易验证数据库连接。
- 再看 `/daily` 和 `/monthly`。
- 如果 API 报错，按顺序排查：env vars → DB connection → SQL → controller params。

## Step 8：项目 Demo Script

最终 demo 顺序：

1. 展示架构图。
2. 展示 raw CSV。
3. 展示 Bronze / Silver / Gold 表。
4. 展示 Data Quality checks 通过。
5. 展示 serving table SQL 查询。
6. 启动 Spring Boot。
7. 用 Swagger / Postman 调 API。
8. 展示 README 和简历 bullet points。

## 验收标准

学生完成本节后应该能做到：

- Serving database 中存在 daily/monthly/risk 表。
- SQL 可以查询 daily 和 monthly 数据。
- Spring Boot 可以启动。
- `/api/weather/cities` 返回城市列表。
- `/api/weather/daily` 返回某城市日期范围的 daily metrics。
- `/api/weather/monthly` 返回某城市年度 monthly summary。

## 课堂提问

1. 为什么 API 不直接查询 Silver 表？
2. 为什么 API 不直接查询 Spark / Delta？
3. Serving table 和 Gold table 的关系是什么？
4. 如果 Gold table 更新了，serving table 怎么同步？
5. 为什么 password 不应该写在 `application.yml` 里？

## 课后作业

1. 添加 `/api/weather/risks` endpoint。
2. 给 `/daily` endpoint 添加默认日期范围。
3. 给 API 添加简单错误处理，例如 city 为空时报 400。
4. 在 README 中补充 API examples。

进阶作业：

1. 用 Node.js + Express 实现同样的 API。
2. 用 React 做一个简单 dashboard。
3. 用 OAuth / service principal 替代 password auth。
4. 把 snapshot sync 改成 continuous sync。
