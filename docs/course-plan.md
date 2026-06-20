# Weather Lakehouse Course Plan

## 课程定位

这门课是一个 3 节课、每节 2 小时的端到端数据工程项目。

主线只保留一条：

```text
Raw Weather Data
        ↓
Bronze Delta
        ↓
Silver Delta
        ↓
Gold Delta
        ↓
Data Quality
        ↓
Lakebase/Postgres Serving
        ↓
Spring Boot REST API
```

课程目标不是覆盖所有 Databricks 和后端知识，而是让学生理解一个数据产品从 raw data 到 API serving 的完整路径。

## 内容取舍

### 核心必讲

这些内容是课堂主线，每个学生都应该能说清楚并跑通：

- Bronze / Silver / Gold 分层
- PySpark transform
- Gold 表设计
- Data quality
- Serving layer
- REST API

### 简讲

这些内容只解释它们在项目中的位置，不展开成完整专题：

- Unity Catalog
- Volume
- Databricks Workflow
- Swagger
- Lakebase synced table

### 课后扩展

这些内容不进入 6 小时主课，可以作为作业、bonus 或第二版项目：

- Node.js backend
- React frontend
- OAuth auth
- Continuous sync
- 更多天气风险规则
- 自动化 Job 调度

## 三节课规划

| 课程 | 主线 | 核心产出 |
| --- | --- | --- |
| [Lesson 1](lesson-01-raw-bronze-silver.md) | Raw → Bronze → Silver | raw CSV、Bronze table、Silver table |
| [Lesson 2](lesson-02-gold-data-quality.md) | Silver → Gold → Data Quality | Daily metrics、Monthly summary、Risk days、DQ checks |
| [Lesson 3](lesson-03-serving-api.md) | Gold → Serving → REST API | Lakebase/Postgres tables、Spring Boot API、Swagger/Postman demo |

## Lesson 1：Raw → Bronze → Silver

### 必讲

- 什么是 raw weather data。
- Bronze 为什么保留原始字段。
- Silver 为什么做字段标准化。
- PySpark DataFrame 的基本 transform 链路。

### 简讲

- Unity Catalog：告诉学生表和 volume 的命名空间在哪里。
- Volume：告诉学生 CSV 上传到哪里。
- Delta table：告诉学生 `saveAsTable` 后数据变成可查询表。

### 不讲

- Gold 聚合表。
- Data quality framework。
- Workflow 自动调度。
- Lakebase / API。

### 代码范围

```text
scripts/fetch_open_meteo_weather.py
databricks/00_setup.py
databricks/01_ingest_bronze_weather.py
databricks/02_clean_silver_weather.py
```

### 验收标准

- 本地生成 `data/raw_weather_daily.csv`。
- CSV 有 5 个城市、每个城市 366 行。
- Databricks 中创建 `bronze_weather_daily_raw`。
- Databricks 中创建 `silver_weather_daily_clean`。
- Silver 表中有 `weather_date`、`is_rainy_day`、`is_hot_day`、`is_freezing_day`。

## Lesson 2：Gold → Data Quality

### 必讲

- Gold 表不是简单“更干净”，而是面向业务消费。
- Daily metrics 的粒度是 `city + weather_date`。
- Monthly summary 的粒度是 `city + year + month`。
- Risk days 是事件表，不是普通宽表。
- Data quality 是发布前的质量门槛。

### 简讲

- Databricks Workflow：只展示如何把 notebook 串起来。

### 代码范围

计划新增：

```text
databricks/03_build_gold_weather_metrics.py
databricks/04_data_quality_checks.py
docs/lesson-02-gold-data-quality.md
```

### 验收标准

- 创建 `gold_city_daily_weather_metrics`。
- 创建 `gold_city_monthly_weather_summary`。
- 创建 `gold_weather_risk_days`。
- DQ 检查通过：非空、日期非空、daily 表无重复 city/date。

## Lesson 3：Serving → REST API

### 必讲

- 为什么 API 不直接查 Delta / Spark。
- Serving layer 的职责是低延迟查询。
- Spring Boot API 如何从 Postgres/Lakebase 查数据并返回 JSON。

### 简讲

- Lakebase synced table：只讲从 Gold Delta 到 Postgres-compatible serving table。
- Swagger：只作为 API 测试入口。

### 代码范围

计划新增：

```text
backend-springboot/
docs/lesson-03-serving-api.md
```

### 验收标准

- Serving database 能查询 daily/monthly/risk 表。
- Spring Boot 能启动。
- `/api/weather/cities` 返回城市列表。
- `/api/weather/daily` 返回某城市某日期范围的天气数据。
- `/api/weather/monthly` 返回某城市月度汇总。

## 当前第一部分是否正确

当前第一部分是正确的，因为它满足三个条件：

1. 范围清楚：只覆盖 Raw → Bronze → Silver。
2. 代码可运行：本地抓数脚本已经成功生成 `data/raw_weather_daily.csv`。
3. 教学重点集中：Silver transform 已经包含字段标准化、日期特征、业务 flags。

当前第一部分暂时不需要加入 Gold、DQ、Lakebase 或 API。那些内容应该从 Lesson 2 开始逐步加入。
