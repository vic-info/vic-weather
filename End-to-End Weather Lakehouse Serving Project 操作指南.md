# End-to-End Weather Lakehouse Serving Project 操作指南

## 课程版使用说明

这份文件是完整项目参考手册，内容覆盖端到端项目的完整形态。

当前 starter repo 的 3 节课版本以 [README.md](/Users/sunhlf/Desktop/vicinfo/vic-wearther-big-data-course/README.md) 和 [docs/course-plan.md](/Users/sunhlf/Desktop/vicinfo/vic-wearther-big-data-course/docs/course-plan.md) 为准。

课程重点按下面方式取舍：

核心必讲：

- Bronze / Silver / Gold 分层
- PySpark transform
- Gold 表设计
- Data quality
- Serving layer
- REST API

简讲：

- Unity Catalog
- Volume
- Databricks Workflow
- Swagger
- Lakebase synced table

课后扩展：

- Node.js backend
- React frontend
- OAuth auth
- Continuous sync
- 更多天气风险规则
- 自动化 Job 调度

三节课安排：

```text
Lesson 1: Raw → Bronze → Silver
Lesson 2: Silver → Gold → Data Quality
Lesson 3: Gold → Serving → REST API
```

也就是说，本文件中的 Node.js、React、OAuth、Continuous sync、复杂 Workflow 等内容可以保留作参考，但不进入 6 小时主课。

## 目录

0. [课程版使用说明](#课程版使用说明)
1. [项目目标](#1-项目目标)
2. [技术栈](#2-技术栈)
3. [数据源选择](#3-数据源选择)
4. [最终数据模型](#4-最终数据模型)
5. [GitHub 项目目录](#5-github-项目目录)
6. [Step 1：本地获取 Raw Weather Data](#6-step-1本地获取-raw-weather-data)
7. [Step 2：Databricks 创建项目目录](#7-step-2databricks-创建项目目录)
8. [Step 3：上传 Raw CSV 到 Databricks](#8-step-3上传-raw-csv-到-databricks)
9. [Notebook 00：Setup](#9-notebook-00setup)
10. [Notebook 01：Ingest Bronze Weather](#10-notebook-01ingest-bronze-weather)
11. [Notebook 02：Clean Silver Weather](#11-notebook-02clean-silver-weather)
12. [Notebook 03：Build Gold Weather Metrics](#12-notebook-03build-gold-weather-metrics)
13. [Notebook 04：Data Quality Checks](#13-notebook-04data-quality-checks)
14. [Databricks Workflow / Job](#14-databricks-workflow--job)
15. [Lakebase Serving Layer](#15-lakebase-serving-layer)
16. [Spring Boot Backend](#16-spring-boot-backend)
17. [Optional：Node.js Backend](#17-optionalnodejs-backend)
18. [API 设计文档](#18-api-设计文档)
19. [End-to-End Demo Script](#19-end-to-end-demo-script)
20. [README 模板](#20-readme-模板)
21. [简历 Bullet Points](#21-简历-bullet-points)
22. [教学安排](#22-教学安排)
23. [关键设计理念](#23-关键设计理念)
24. [替代方案](#24-替代方案)
25. [最终验收 Checklist](#25-最终验收-checklist)
26. [一句话项目介绍](#26-一句话项目介绍)

---

## 1. 项目目标

本项目带同学完整走一遍天气数据从原始数据到 API 服务的端到端链路。

```text
Raw Weather Data
        ↓
Databricks Bronze Layer
        ↓
Databricks Silver Layer
        ↓
Databricks Gold Layer
        ↓
Lakebase Postgres Serving Tables
        ↓
Spring Boot / Node Backend API
        ↓
Client / Postman / Frontend
```

项目名称：

```text
Weather Lakehouse Serving System
```

英文简历版项目名：

```text
End-to-End Weather Lakehouse Serving Pipeline with Databricks, Lakebase, and Spring Boot
```

项目核心能力：

1. 从公开天气数据源获取 raw data。
2. 在 Databricks 中建立 Bronze / Silver / Gold 三层。
3. 使用 PySpark 做数据清洗、标准化、聚合。
4. Gold Layer 保存在 Delta / Unity Catalog 中，作为 analytical source of truth。
5. 将 Gold 表同步到 Lakebase Postgres。
6. 使用 Spring Boot 暴露 REST API。
7. 通过 Postman / Swagger 查询天气指标。

Node.js backend 和 React frontend 保留为课后扩展，不作为 3 节课主线。

---

## 2. 技术栈

Data Source：

- Open-Meteo Historical Weather API

Data Engineering：

- Databricks Free Edition / Databricks Workspace
- PySpark
- Delta Lake
- Unity Catalog
- Databricks Workflows / Jobs

Serving Layer：

- Databricks Lakebase Postgres

Backend：

- Spring Boot 3
- Java 17
- JdbcTemplate
- PostgreSQL JDBC Driver
- Swagger / OpenAPI

Optional：

- Node.js + Express
- React frontend

---

## 3. 数据源选择

本项目使用天气数据。

推荐数据源：

```text
Open-Meteo Historical Weather API
```

选择原因：

1. 免费。
2. 不需要 API key。
3. 字段直观。
4. 适合教学。
5. 可以按城市、日期、天气指标拉取历史数据。

建议城市：

- San Francisco
- New York
- Seattle
- Austin
- Chicago

建议日期范围：

```text
2024-01-01 到 2024-12-31
```

建议数据粒度：

```text
Daily weather data
```

第一版不要直接做 hourly，因为 hourly 数据字段更多、数据量更大、处理复杂度更高。Daily 数据已经足够展示完整 pipeline。

---

## 4. 最终数据模型

### 4.1 Raw Data 字段

从 Open-Meteo 拉下来的 raw CSV 字段大概是：

```text
time
temperature_2m_max
temperature_2m_min
temperature_2m_mean
precipitation_sum
rain_sum
snowfall_sum
wind_speed_10m_max
city
latitude
longitude
```

### 4.2 Bronze Layer

表名：

```text
bronze_weather_daily_raw
```

职责：

1. 原始数据落地。
2. 不做复杂清洗。
3. 保留 source。
4. 保留 ingestion timestamp。
5. 方便 replay / debug。

字段：

```text
time
temperature_2m_max
temperature_2m_min
temperature_2m_mean
precipitation_sum
rain_sum
snowfall_sum
wind_speed_10m_max
city
latitude
longitude
source
ingestion_timestamp
```

### 4.3 Silver Layer

表名：

```text
silver_weather_daily_clean
```

职责：

1. 清洗空值。
2. 标准化字段名。
3. 添加业务字段。
4. 添加时间维度。
5. 添加风险标记字段。

字段：

```text
city
weather_date
year
month
day_of_week
latitude
longitude
max_temp_c
min_temp_c
mean_temp_c
temperature_range_c
precipitation_mm
rain_mm
snowfall_cm
max_wind_speed_kmh
is_rainy_day
is_hot_day
is_freezing_day
```

规则：

```text
is_rainy_day = precipitation_mm > 1
is_hot_day = max_temp_c >= 30
is_freezing_day = min_temp_c <= 0
temperature_range_c = max_temp_c - min_temp_c
```

### 4.4 Gold Layer

Gold 层做三张表。

#### Gold Table 1：每日城市天气指标

表名：

```text
gold_city_daily_weather_metrics
```

粒度：

```text
city + weather_date
```

字段：

```text
city
weather_date
max_temp_c
min_temp_c
mean_temp_c
precipitation_mm
max_wind_speed_kmh
is_rainy_day
is_hot_day
is_freezing_day
weather_severity_score
```

`weather_severity_score` 规则：

```text
rainy day: +1
hot day: +1
freezing day: +1
wind speed > 40 km/h: +1
```

#### Gold Table 2：月度城市天气汇总

表名：

```text
gold_city_monthly_weather_summary
```

粒度：

```text
city + year + month
```

字段：

```text
city
year
month
avg_mean_temp_c
max_temp_c
min_temp_c
total_precipitation_mm
rainy_days
hot_days
freezing_days
avg_wind_speed_kmh
```

#### Gold Table 3：天气风险日

表名：

```text
gold_weather_risk_days
```

粒度：

```text
city + weather_date + risk_type
```

字段：

```text
city
weather_date
risk_type
risk_level
metric_value
description
```

风险类型：

```text
HEAVY_RAIN
HOT_DAY
FREEZING
STRONG_WIND
```

---

## 5. GitHub 项目目录

建议 repo 结构：

```text
weather-lakehouse-serving-system
├── README.md
├── data/
│   └── raw_weather_daily.csv
├── scripts/
│   └── fetch_open_meteo_weather.py
├── databricks/
│   ├── 00_setup.py
│   ├── 01_ingest_bronze_weather.py
│   ├── 02_clean_silver_weather.py
│   ├── 03_build_gold_weather_metrics.py
│   ├── 04_data_quality_checks.py
│   └── 05_publish_to_lakebase.md
├── backend-springboot/
│   ├── pom.xml
│   └── src/main/java/com/example/weather/
│       ├── WeatherApiApplication.java
│       ├── controller/
│       │   └── WeatherController.java
│       ├── service/
│       │   └── WeatherService.java
│       ├── repository/
│       │   └── WeatherRepository.java
│       └── dto/
│           ├── DailyWeatherResponse.java
│           ├── MonthlyWeatherResponse.java
│           └── RiskDayResponse.java
├── backend-node-optional/
│   ├── package.json
│   └── src/
│       ├── server.ts
│       ├── db.ts
│       └── routes/
│           └── weather.ts
└── docs/
    ├── architecture.md
    ├── api-spec.md
    └── runbook.md
```

---

## 6. Step 1：本地获取 Raw Weather Data

因为 Databricks Free Edition 可能限制 outbound internet，所以建议先在本地拉数据，再上传 CSV。

创建文件：

```text
scripts/fetch_open_meteo_weather.py
```

代码：

```python
import requests
import pandas as pd

cities = [
    {"city": "San Francisco", "latitude": 37.7749, "longitude": -122.4194},
    {"city": "New York", "latitude": 40.7128, "longitude": -74.0060},
    {"city": "Seattle", "latitude": 47.6062, "longitude": -122.3321},
    {"city": "Austin", "latitude": 30.2672, "longitude": -97.7431},
    {"city": "Chicago", "latitude": 41.8781, "longitude": -87.6298},
]

all_rows = []

for city in cities:
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": city["latitude"],
        "longitude": city["longitude"],
        "start_date": "2024-01-01",
        "end_date": "2024-12-31",
        "daily": [
            "temperature_2m_max",
            "temperature_2m_min",
            "temperature_2m_mean",
            "precipitation_sum",
            "rain_sum",
            "snowfall_sum",
            "wind_speed_10m_max",
        ],
        "timezone": "auto",
    }

    response = requests.get(url, params=params)
    response.raise_for_status()

    data = response.json()
    daily = data["daily"]

    df = pd.DataFrame(daily)
    df["city"] = city["city"]
    df["latitude"] = city["latitude"]
    df["longitude"] = city["longitude"]

    all_rows.append(df)

final_df = pd.concat(all_rows, ignore_index=True)
final_df.to_csv("data/raw_weather_daily.csv", index=False)

print(final_df.head())
print(f"Total rows: {len(final_df)}")
```

运行：

```bash
python scripts/fetch_open_meteo_weather.py
```

预期生成：

```text
data/raw_weather_daily.csv
```

大约数据量：

```text
5 cities × 366 days = 1830 rows
```

---

## 7. Step 2：Databricks 创建项目目录

在 Databricks Workspace 里创建 folder：

```text
/Workspace/Users/<your-email>/weather-lakehouse-serving-system
```

创建 notebooks：

```text
00_setup
01_ingest_bronze_weather
02_clean_silver_weather
03_build_gold_weather_metrics
04_data_quality_checks
```

---

## 8. Step 3：上传 Raw CSV 到 Databricks

推荐路径：

```text
/Volumes/workspace/default/weather/raw_weather_daily.csv
```

如果没有 volume，可以先创建：

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.weather;
```

然后把本地 `raw_weather_daily.csv` 上传到 volume 中。

最终 raw path：

```text
/Volumes/workspace/default/weather/raw_weather_daily.csv
```

如果你的 catalog 不是 `workspace`，先运行：

```sql
SHOW CATALOGS;
```

然后把后续代码里的 `workspace.default` 替换成你的 catalog / schema。

---

## 9. Notebook 00：Setup

Notebook 名称：

```text
00_setup
```

代码：

```python
catalog_name = "workspace"
schema_name = "default"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.weather")

print(f"Using catalog: {catalog_name}")
print(f"Using schema: {schema_name}")
```

检查 catalog 和 schema：

```python
spark.sql("SHOW CATALOGS").show(truncate=False)
spark.sql(f"SHOW SCHEMAS IN {catalog_name}").show(truncate=False)
```

---

## 10. Notebook 01：Ingest Bronze Weather

Notebook 名称：

```text
01_ingest_bronze_weather
```

目标：读取 raw CSV，写入 Bronze Delta table。

代码：

```python
from pyspark.sql.functions import current_timestamp, lit

catalog_name = "workspace"
schema_name = "default"

raw_path = f"/Volumes/{catalog_name}/{schema_name}/weather/raw_weather_daily.csv"
bronze_table = f"{catalog_name}.{schema_name}.bronze_weather_daily_raw"

raw_df = (
    spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(raw_path)
)

bronze_df = (
    raw_df
    .withColumn("source", lit("open_meteo"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Bronze table created: {bronze_table}")
display(bronze_df.limit(10))
```

检查：

```python
spark.sql(f"""
SELECT COUNT(*) AS row_count
FROM {bronze_table}
""").show()

spark.table(bronze_table).printSchema()
```

---

## 11. Notebook 02：Clean Silver Weather

Notebook 名称：

```text
02_clean_silver_weather
```

目标：清洗 Bronze 数据，标准化字段，添加业务字段。

代码：

```python
from pyspark.sql.functions import (
    col,
    to_date,
    year,
    month,
    dayofweek,
    round,
)

catalog_name = "workspace"
schema_name = "default"

bronze_table = f"{catalog_name}.{schema_name}.bronze_weather_daily_raw"
silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"

bronze_df = spark.read.table(bronze_table)

silver_df = (
    bronze_df
    .filter(col("city").isNotNull())
    .filter(col("time").isNotNull())
    .withColumn("weather_date", to_date(col("time")))
    .withColumn("year", year(col("weather_date")))
    .withColumn("month", month(col("weather_date")))
    .withColumn("day_of_week", dayofweek(col("weather_date")))
    .withColumnRenamed("temperature_2m_max", "max_temp_c")
    .withColumnRenamed("temperature_2m_min", "min_temp_c")
    .withColumnRenamed("temperature_2m_mean", "mean_temp_c")
    .withColumnRenamed("precipitation_sum", "precipitation_mm")
    .withColumnRenamed("rain_sum", "rain_mm")
    .withColumnRenamed("snowfall_sum", "snowfall_cm")
    .withColumnRenamed("wind_speed_10m_max", "max_wind_speed_kmh")
    .withColumn("temperature_range_c", round(col("max_temp_c") - col("min_temp_c"), 2))
    .withColumn("is_rainy_day", col("precipitation_mm") > 1.0)
    .withColumn("is_hot_day", col("max_temp_c") >= 30.0)
    .withColumn("is_freezing_day", col("min_temp_c") <= 0.0)
    .select(
        "city",
        "weather_date",
        "year",
        "month",
        "day_of_week",
        "latitude",
        "longitude",
        "max_temp_c",
        "min_temp_c",
        "mean_temp_c",
        "temperature_range_c",
        "precipitation_mm",
        "rain_mm",
        "snowfall_cm",
        "max_wind_speed_kmh",
        "is_rainy_day",
        "is_hot_day",
        "is_freezing_day",
        "source",
        "ingestion_timestamp",
    )
)

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver table created: {silver_table}")
display(silver_df.limit(10))
```

检查：

```python
spark.sql(f"""
SELECT
  city,
  COUNT(*) AS row_count,
  MIN(weather_date) AS min_date,
  MAX(weather_date) AS max_date
FROM {silver_table}
GROUP BY city
ORDER BY city
""").show()
```

---

## 12. Notebook 03：Build Gold Weather Metrics

Notebook 名称：

```text
03_build_gold_weather_metrics
```

目标：基于 Silver 层生成面向 API 和 dashboard 的 Gold 表。

基础代码：

```python
from pyspark.sql.functions import (
    col,
    avg,
    max,
    min,
    sum,
    when,
    lit,
    concat,
)

catalog_name = "workspace"
schema_name = "default"

silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"
gold_daily_table = f"{catalog_name}.{schema_name}.gold_city_daily_weather_metrics"
gold_monthly_table = f"{catalog_name}.{schema_name}.gold_city_monthly_weather_summary"
gold_risk_table = f"{catalog_name}.{schema_name}.gold_weather_risk_days"

silver_df = spark.read.table(silver_table)
```

### 12.1 Gold Daily Metrics

```python
daily_df = (
    silver_df
    .withColumn(
        "weather_severity_score",
        when(col("is_rainy_day"), 1).otherwise(0)
        + when(col("is_hot_day"), 1).otherwise(0)
        + when(col("is_freezing_day"), 1).otherwise(0)
        + when(col("max_wind_speed_kmh") > 40, 1).otherwise(0),
    )
    .select(
        "city",
        "weather_date",
        "max_temp_c",
        "min_temp_c",
        "mean_temp_c",
        "precipitation_mm",
        "max_wind_speed_kmh",
        "is_rainy_day",
        "is_hot_day",
        "is_freezing_day",
        "weather_severity_score",
    )
)

(
    daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_daily_table)
)

print(f"Gold daily table created: {gold_daily_table}")
```

### 12.2 Gold Monthly Summary

```python
monthly_df = (
    silver_df
    .groupBy("city", "year", "month")
    .agg(
        avg("mean_temp_c").alias("avg_mean_temp_c"),
        max("max_temp_c").alias("max_temp_c"),
        min("min_temp_c").alias("min_temp_c"),
        sum("precipitation_mm").alias("total_precipitation_mm"),
        sum(when(col("is_rainy_day"), 1).otherwise(0)).alias("rainy_days"),
        sum(when(col("is_hot_day"), 1).otherwise(0)).alias("hot_days"),
        sum(when(col("is_freezing_day"), 1).otherwise(0)).alias("freezing_days"),
        avg("max_wind_speed_kmh").alias("avg_wind_speed_kmh"),
    )
)

(
    monthly_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_monthly_table)
)

print(f"Gold monthly table created: {gold_monthly_table}")
```

### 12.3 Gold Risk Days

```python
heavy_rain_df = (
    silver_df
    .filter(col("precipitation_mm") >= 20)
    .select(
        "city",
        "weather_date",
        lit("HEAVY_RAIN").alias("risk_type"),
        when(col("precipitation_mm") >= 50, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("precipitation_mm").alias("metric_value"),
        concat(lit("Heavy rainfall detected: "), col("precipitation_mm"), lit(" mm")).alias("description"),
    )
)

hot_day_df = (
    silver_df
    .filter(col("max_temp_c") >= 35)
    .select(
        "city",
        "weather_date",
        lit("HOT_DAY").alias("risk_type"),
        when(col("max_temp_c") >= 40, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("max_temp_c").alias("metric_value"),
        concat(lit("High temperature detected: "), col("max_temp_c"), lit(" C")).alias("description"),
    )
)

freezing_df = (
    silver_df
    .filter(col("min_temp_c") <= 0)
    .select(
        "city",
        "weather_date",
        lit("FREEZING").alias("risk_type"),
        when(col("min_temp_c") <= -10, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("min_temp_c").alias("metric_value"),
        concat(lit("Freezing temperature detected: "), col("min_temp_c"), lit(" C")).alias("description"),
    )
)

strong_wind_df = (
    silver_df
    .filter(col("max_wind_speed_kmh") >= 40)
    .select(
        "city",
        "weather_date",
        lit("STRONG_WIND").alias("risk_type"),
        when(col("max_wind_speed_kmh") >= 60, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("max_wind_speed_kmh").alias("metric_value"),
        concat(lit("Strong wind detected: "), col("max_wind_speed_kmh"), lit(" km/h")).alias("description"),
    )
)

risk_df = (
    heavy_rain_df
    .unionByName(hot_day_df)
    .unionByName(freezing_df)
    .unionByName(strong_wind_df)
)

(
    risk_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_risk_table)
)

print(f"Gold risk table created: {gold_risk_table}")
```

检查：

```python
display(spark.table(gold_daily_table).limit(10))
display(spark.table(gold_monthly_table).limit(10))
display(spark.table(gold_risk_table).limit(10))
```

---

## 13. Notebook 04：Data Quality Checks

Notebook 名称：

```text
04_data_quality_checks
```

目标：验证 Silver 和 Gold 数据是否可用。

代码：

```python
catalog_name = "workspace"
schema_name = "default"

silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"
gold_daily_table = f"{catalog_name}.{schema_name}.gold_city_daily_weather_metrics"
gold_monthly_table = f"{catalog_name}.{schema_name}.gold_city_monthly_weather_summary"
gold_risk_table = f"{catalog_name}.{schema_name}.gold_weather_risk_days"

silver_count = spark.table(silver_table).count()
assert silver_count > 0, "Silver table is empty"

null_date_count = spark.sql(f"""
SELECT COUNT(*) AS cnt
FROM {silver_table}
WHERE weather_date IS NULL
""").collect()[0]["cnt"]

assert null_date_count == 0, "Silver table has null weather_date"
print("Silver data quality checks passed.")

daily_count = spark.table(gold_daily_table).count()
assert daily_count > 0, "Gold daily table is empty"

duplicate_count = spark.sql(f"""
SELECT city, weather_date, COUNT(*) AS cnt
FROM {gold_daily_table}
GROUP BY city, weather_date
HAVING COUNT(*) > 1
""").count()

assert duplicate_count == 0, "Gold daily table has duplicate city/date rows"
print("Gold daily checks passed.")

monthly_count = spark.table(gold_monthly_table).count()
assert monthly_count > 0, "Gold monthly table is empty"
print("Gold monthly checks passed.")

risk_count = spark.table(gold_risk_table).count()
print(f"Risk day rows: {risk_count}")
print("Risk table check completed.")
```

---

## 14. Databricks Workflow / Job

在 Databricks 左侧进入：

```text
Jobs & Pipelines
```

创建 Job：

```text
weather_lakehouse_pipeline
```

添加 tasks：

| Task | Name | Type | Depends on |
| --- | --- | --- | --- |
| 1 | `00_setup` | Notebook | 无 |
| 2 | `01_ingest_bronze_weather` | Notebook | `00_setup` |
| 3 | `02_clean_silver_weather` | Notebook | `01_ingest_bronze_weather` |
| 4 | `03_build_gold_weather_metrics` | Notebook | `02_clean_silver_weather` |
| 5 | `04_data_quality_checks` | Notebook | `03_build_gold_weather_metrics` |

Dependency graph：

```text
00_setup
   ↓
01_ingest_bronze_weather
   ↓
02_clean_silver_weather
   ↓
03_build_gold_weather_metrics
   ↓
04_data_quality_checks
```

点击 `Run now`，确认所有 tasks 成功。

---

## 15. Lakebase Serving Layer

### 15.1 创建 Lakebase Project

在 Databricks 中打开：

```text
Apps / Lakebase Postgres
```

创建 project：

```text
weather-serving-project
```

创建 database：

```text
weather_serving
```

创建 branch：

```text
production
```

记录 connection information：

```text
host
port
database
role / username
auth method
```

如果使用 password authentication，记录 password。如果使用 OAuth，需要后端使用 Databricks SDK 定期生成 database credential。

教学项目中，优先使用最简单的 password authentication。如果 workspace 不允许 password auth，再使用 OAuth / service principal。

### 15.2 将 Gold Delta 表同步到 Lakebase

目标链路：

```text
Unity Catalog Gold Tables
        ↓
Lakebase Synced Tables
        ↓
Backend API Query
```

需要同步的 Gold 表：

```text
workspace.default.gold_city_daily_weather_metrics
workspace.default.gold_city_monthly_weather_summary
workspace.default.gold_weather_risk_days
```

Lakebase 中对应表名：

```text
weather_daily_metrics
weather_monthly_summary
weather_risk_days
```

逻辑映射：

```text
gold_city_daily_weather_metrics   → weather_daily_metrics
gold_city_monthly_weather_summary → weather_monthly_summary
gold_weather_risk_days            → weather_risk_days
```

建议同步模式：

- 第一版项目：Snapshot sync。
- 进阶版本：Continuous sync。

### 15.3 Lakebase 表结构预期

Lakebase 中最终应该能查询：

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
FROM weather_risk_days
WHERE city = 'San Francisco'
ORDER BY weather_date;

SELECT *
FROM weather_monthly_summary
WHERE city = 'San Francisco'
  AND year = 2024
ORDER BY month;
```

---

## 16. Spring Boot Backend

### 16.1 Backend 目标

```text
Spring Boot API
        ↓
Lakebase Postgres
        ↓
Return weather metrics as JSON
```

API endpoints：

```text
GET /api/weather/daily?city=San Francisco&from=2024-01-01&to=2024-01-31
GET /api/weather/monthly?city=San Francisco&year=2024
GET /api/weather/risks?city=San Francisco
GET /api/weather/summary?city=San Francisco
GET /api/weather/cities
```

### 16.2 Spring Boot 项目依赖

使用 Spring Initializr 创建项目：

```text
Project: Maven
Language: Java
Spring Boot: 3.x
Java: 17
Dependencies:
- Spring Web
- JDBC API
- PostgreSQL Driver
- Spring Boot Actuator
- Springdoc OpenAPI / Swagger
```

`pom.xml` 关键依赖：

```xml
<dependencies>
    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-web</artifactId>
    </dependency>

    <dependency>
        <groupId>org.springframework.boot</groupId>
        <artifactId>spring-boot-starter-jdbc</artifactId>
    </dependency>

    <dependency>
        <groupId>org.postgresql</groupId>
        <artifactId>postgresql</artifactId>
        <scope>runtime</scope>
    </dependency>

    <dependency>
        <groupId>org.springdoc</groupId>
        <artifactId>springdoc-openapi-starter-webmvc-ui</artifactId>
        <version>2.6.0</version>
    </dependency>
</dependencies>
```

### 16.3 Spring Boot application.yml

路径：

```text
backend-springboot/src/main/resources/application.yml
```

直接配置版本：

```yaml
spring:
  application:
    name: weather-serving-api
  datasource:
    url: jdbc:postgresql://<lakebase-host>:5432/weather_serving
    username: <lakebase-role-or-user>
    password: <lakebase-password-or-token>
    driver-class-name: org.postgresql.Driver

server:
  port: 8080

springdoc:
  swagger-ui:
    path: /swagger-ui.html
```

不要把真实 password commit 到 GitHub。本地开发建议用环境变量：

```yaml
spring:
  datasource:
    url: ${LAKEBASE_JDBC_URL}
    username: ${LAKEBASE_USERNAME}
    password: ${LAKEBASE_PASSWORD}
    driver-class-name: org.postgresql.Driver
```

### 16.4 DTO：DailyWeatherResponse

文件：

```text
dto/DailyWeatherResponse.java
```

代码：

```java
package com.example.weather.dto;

import java.time.LocalDate;

public record DailyWeatherResponse(
        String city,
        LocalDate weatherDate,
        Double maxTempC,
        Double minTempC,
        Double meanTempC,
        Double precipitationMm,
        Double maxWindSpeedKmh,
        Boolean rainyDay,
        Boolean hotDay,
        Boolean freezingDay,
        Integer weatherSeverityScore
) {
}
```

### 16.5 DTO：MonthlyWeatherResponse

文件：

```text
dto/MonthlyWeatherResponse.java
```

代码：

```java
package com.example.weather.dto;

public record MonthlyWeatherResponse(
        String city,
        Integer year,
        Integer month,
        Double avgMeanTempC,
        Double maxTempC,
        Double minTempC,
        Double totalPrecipitationMm,
        Integer rainyDays,
        Integer hotDays,
        Integer freezingDays,
        Double avgWindSpeedKmh
) {
}
```

### 16.6 DTO：RiskDayResponse

文件：

```text
dto/RiskDayResponse.java
```

代码：

```java
package com.example.weather.dto;

import java.time.LocalDate;

public record RiskDayResponse(
        String city,
        LocalDate weatherDate,
        String riskType,
        String riskLevel,
        Double metricValue,
        String description
) {
}
```

### 16.7 Repository：WeatherRepository

文件：

```text
repository/WeatherRepository.java
```

代码：

```java
package com.example.weather.repository;

import com.example.weather.dto.DailyWeatherResponse;
import com.example.weather.dto.MonthlyWeatherResponse;
import com.example.weather.dto.RiskDayResponse;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Repository;

import java.time.LocalDate;
import java.util.List;

@Repository
public class WeatherRepository {

    private final JdbcTemplate jdbcTemplate;

    public WeatherRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public List<DailyWeatherResponse> findDailyWeather(
            String city,
            LocalDate from,
            LocalDate to
    ) {
        String sql = """
            SELECT
                city,
                weather_date,
                max_temp_c,
                min_temp_c,
                mean_temp_c,
                precipitation_mm,
                max_wind_speed_kmh,
                is_rainy_day,
                is_hot_day,
                is_freezing_day,
                weather_severity_score
            FROM weather_daily_metrics
            WHERE city = ?
              AND weather_date BETWEEN ? AND ?
            ORDER BY weather_date
            """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> new DailyWeatherResponse(
                rs.getString("city"),
                rs.getDate("weather_date").toLocalDate(),
                rs.getDouble("max_temp_c"),
                rs.getDouble("min_temp_c"),
                rs.getDouble("mean_temp_c"),
                rs.getDouble("precipitation_mm"),
                rs.getDouble("max_wind_speed_kmh"),
                rs.getBoolean("is_rainy_day"),
                rs.getBoolean("is_hot_day"),
                rs.getBoolean("is_freezing_day"),
                rs.getInt("weather_severity_score")
        ), city, from, to);
    }

    public List<MonthlyWeatherResponse> findMonthlyWeather(String city, Integer year) {
        String sql = """
            SELECT
                city,
                year,
                month,
                avg_mean_temp_c,
                max_temp_c,
                min_temp_c,
                total_precipitation_mm,
                rainy_days,
                hot_days,
                freezing_days,
                avg_wind_speed_kmh
            FROM weather_monthly_summary
            WHERE city = ?
              AND year = ?
            ORDER BY month
            """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> new MonthlyWeatherResponse(
                rs.getString("city"),
                rs.getInt("year"),
                rs.getInt("month"),
                rs.getDouble("avg_mean_temp_c"),
                rs.getDouble("max_temp_c"),
                rs.getDouble("min_temp_c"),
                rs.getDouble("total_precipitation_mm"),
                rs.getInt("rainy_days"),
                rs.getInt("hot_days"),
                rs.getInt("freezing_days"),
                rs.getDouble("avg_wind_speed_kmh")
        ), city, year);
    }

    public List<RiskDayResponse> findRiskDays(String city, String riskType) {
        String baseSql = """
            SELECT
                city,
                weather_date,
                risk_type,
                risk_level,
                metric_value,
                description
            FROM weather_risk_days
            WHERE city = ?
            """;

        if (riskType == null || riskType.isBlank()) {
            String sql = baseSql + " ORDER BY weather_date";
            return jdbcTemplate.query(sql, (rs, rowNum) -> new RiskDayResponse(
                    rs.getString("city"),
                    rs.getDate("weather_date").toLocalDate(),
                    rs.getString("risk_type"),
                    rs.getString("risk_level"),
                    rs.getDouble("metric_value"),
                    rs.getString("description")
            ), city);
        }

        String sql = baseSql + " AND risk_type = ? ORDER BY weather_date";
        return jdbcTemplate.query(sql, (rs, rowNum) -> new RiskDayResponse(
                rs.getString("city"),
                rs.getDate("weather_date").toLocalDate(),
                rs.getString("risk_type"),
                rs.getString("risk_level"),
                rs.getDouble("metric_value"),
                rs.getString("description")
        ), city, riskType);
    }

    public List<String> findCities() {
        String sql = """
            SELECT DISTINCT city
            FROM weather_daily_metrics
            ORDER BY city
            """;

        return jdbcTemplate.query(sql, (rs, rowNum) -> rs.getString("city"));
    }
}
```

### 16.8 Service：WeatherService

文件：

```text
service/WeatherService.java
```

代码：

```java
package com.example.weather.service;

import com.example.weather.dto.DailyWeatherResponse;
import com.example.weather.dto.MonthlyWeatherResponse;
import com.example.weather.dto.RiskDayResponse;
import com.example.weather.repository.WeatherRepository;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.util.List;

@Service
public class WeatherService {

    private final WeatherRepository weatherRepository;

    public WeatherService(WeatherRepository weatherRepository) {
        this.weatherRepository = weatherRepository;
    }

    public List<DailyWeatherResponse> getDailyWeather(String city, LocalDate from, LocalDate to) {
        return weatherRepository.findDailyWeather(city, from, to);
    }

    public List<MonthlyWeatherResponse> getMonthlyWeather(String city, Integer year) {
        return weatherRepository.findMonthlyWeather(city, year);
    }

    public List<RiskDayResponse> getRiskDays(String city, String riskType) {
        return weatherRepository.findRiskDays(city, riskType);
    }

    public List<String> getCities() {
        return weatherRepository.findCities();
    }
}
```

### 16.9 Controller：WeatherController

文件：

```text
controller/WeatherController.java
```

代码：

```java
package com.example.weather.controller;

import com.example.weather.dto.DailyWeatherResponse;
import com.example.weather.dto.MonthlyWeatherResponse;
import com.example.weather.dto.RiskDayResponse;
import com.example.weather.service.WeatherService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDate;
import java.util.List;

@RestController
@RequestMapping("/api/weather")
public class WeatherController {

    private final WeatherService weatherService;

    public WeatherController(WeatherService weatherService) {
        this.weatherService = weatherService;
    }

    @GetMapping("/daily")
    public List<DailyWeatherResponse> getDailyWeather(
            @RequestParam String city,
            @RequestParam LocalDate from,
            @RequestParam LocalDate to
    ) {
        return weatherService.getDailyWeather(city, from, to);
    }

    @GetMapping("/monthly")
    public List<MonthlyWeatherResponse> getMonthlyWeather(
            @RequestParam String city,
            @RequestParam Integer year
    ) {
        return weatherService.getMonthlyWeather(city, year);
    }

    @GetMapping("/risks")
    public List<RiskDayResponse> getRiskDays(
            @RequestParam String city,
            @RequestParam(required = false) String riskType
    ) {
        return weatherService.getRiskDays(city, riskType);
    }

    @GetMapping("/cities")
    public List<String> getCities() {
        return weatherService.getCities();
    }
}
```

### 16.10 Spring Boot 启动类

文件：

```text
WeatherApiApplication.java
```

代码：

```java
package com.example.weather;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class WeatherApiApplication {

    public static void main(String[] args) {
        SpringApplication.run(WeatherApiApplication.class, args);
    }
}
```

### 16.11 本地运行 Spring Boot

设置环境变量：

```bash
export LAKEBASE_JDBC_URL="jdbc:postgresql://<lakebase-host>:5432/weather_serving"
export LAKEBASE_USERNAME="<lakebase-user>"
export LAKEBASE_PASSWORD="<lakebase-password>"
```

运行：

```bash
mvn spring-boot:run
```

测试：

```bash
curl "http://localhost:8080/api/weather/cities"
curl "http://localhost:8080/api/weather/daily?city=San%20Francisco&from=2024-01-01&to=2024-01-31"
curl "http://localhost:8080/api/weather/monthly?city=San%20Francisco&year=2024"
curl "http://localhost:8080/api/weather/risks?city=San%20Francisco"
```

Swagger：

```text
http://localhost:8080/swagger-ui.html
```

---

## 17. Optional：Node.js Backend

如果同学更熟 TypeScript，可以做 Node.js 版本。

依赖：

```bash
npm init -y
npm install express pg dotenv
npm install -D typescript ts-node @types/node @types/express
```

`src/db.ts`：

```ts
import { Pool } from "pg";
import dotenv from "dotenv";

dotenv.config();

export const pool = new Pool({
  connectionString: process.env.LAKEBASE_DATABASE_URL,
  ssl: {
    rejectUnauthorized: false,
  },
});
```

`src/server.ts`：

```ts
import express from "express";
import { pool } from "./db";

const app = express();

app.use(express.json());

app.get("/api/weather/cities", async (_req, res) => {
  const result = await pool.query(`
    SELECT DISTINCT city
    FROM weather_daily_metrics
    ORDER BY city
  `);

  res.json(result.rows);
});

app.get("/api/weather/daily", async (req, res) => {
  const { city, from, to } = req.query;

  const result = await pool.query(
    `
    SELECT *
    FROM weather_daily_metrics
    WHERE city = $1
      AND weather_date BETWEEN $2 AND $3
    ORDER BY weather_date
    `,
    [city, from, to]
  );

  res.json(result.rows);
});

app.get("/api/weather/monthly", async (req, res) => {
  const { city, year } = req.query;

  const result = await pool.query(
    `
    SELECT *
    FROM weather_monthly_summary
    WHERE city = $1
      AND year = $2
    ORDER BY month
    `,
    [city, year]
  );

  res.json(result.rows);
});

app.get("/api/weather/risks", async (req, res) => {
  const { city, riskType } = req.query;

  if (riskType) {
    const result = await pool.query(
      `
      SELECT *
      FROM weather_risk_days
      WHERE city = $1
        AND risk_type = $2
      ORDER BY weather_date
      `,
      [city, riskType]
    );

    res.json(result.rows);
    return;
  }

  const result = await pool.query(
    `
    SELECT *
    FROM weather_risk_days
    WHERE city = $1
    ORDER BY weather_date
    `,
    [city]
  );

  res.json(result.rows);
});

app.listen(3000, () => {
  console.log("Weather API is running on port 3000");
});
```

运行：

```bash
npx ts-node src/server.ts
```

---

## 18. API 设计文档

### GET /api/weather/cities

说明：返回所有可查询城市。

Response：

```json
[
  "Austin",
  "Chicago",
  "New York",
  "San Francisco",
  "Seattle"
]
```

### GET /api/weather/daily

Request：

```http
GET /api/weather/daily?city=San Francisco&from=2024-01-01&to=2024-01-31
```

Response：

```json
[
  {
    "city": "San Francisco",
    "weatherDate": "2024-01-01",
    "maxTempC": 14.3,
    "minTempC": 8.2,
    "meanTempC": 11.1,
    "precipitationMm": 2.4,
    "maxWindSpeedKmh": 18.3,
    "rainyDay": true,
    "hotDay": false,
    "freezingDay": false,
    "weatherSeverityScore": 1
  }
]
```

### GET /api/weather/monthly

Request：

```http
GET /api/weather/monthly?city=San Francisco&year=2024
```

Response：

```json
[
  {
    "city": "San Francisco",
    "year": 2024,
    "month": 1,
    "avgMeanTempC": 11.8,
    "maxTempC": 18.2,
    "minTempC": 5.4,
    "totalPrecipitationMm": 92.1,
    "rainyDays": 12,
    "hotDays": 0,
    "freezingDays": 0,
    "avgWindSpeedKmh": 20.5
  }
]
```

### GET /api/weather/risks

Request：

```http
GET /api/weather/risks?city=San Francisco
```

Optional：

```http
GET /api/weather/risks?city=San Francisco&riskType=HEAVY_RAIN
```

Response：

```json
[
  {
    "city": "San Francisco",
    "weatherDate": "2024-02-05",
    "riskType": "HEAVY_RAIN",
    "riskLevel": "HIGH",
    "metricValue": 52.3,
    "description": "Heavy rainfall detected: 52.3 mm"
  }
]
```

---

## 19. End-to-End Demo Script

### Demo 1：展示架构图

```text
Raw Weather Data
        ↓
Bronze Delta
        ↓
Silver Delta
        ↓
Gold Delta
        ↓
Lakebase Postgres
        ↓
Spring Boot API
```

重点解释：

- Databricks 负责 batch processing 和 lakehouse analytics。
- Lakebase 负责 operational serving。
- Spring Boot 负责 API exposure。

### Demo 2：展示 Raw CSV

打开：

```text
data/raw_weather_daily.csv
```

讲解：这是从 Open-Meteo 下载的一年五个城市的日级天气数据。

### Demo 3：运行 Bronze notebook

运行：

```text
01_ingest_bronze_weather
```

讲解：Bronze 层只负责原始数据落地，不做业务清洗。

### Demo 4：运行 Silver notebook

运行：

```text
02_clean_silver_weather
```

讲解：Silver 层负责清洗和标准化，例如字段改名、日期解析、添加 `is_rainy_day` / `is_hot_day` / `is_freezing_day`。

### Demo 5：运行 Gold notebook

运行：

```text
03_build_gold_weather_metrics
```

讲解：Gold 层面向业务消费，生成 daily metrics、monthly summary、risk days 三张表。

### Demo 6：运行 Data Quality notebook

运行：

```text
04_data_quality_checks
```

讲解：Data quality 是生产 pipeline 很重要的一环，这里检查空表、空日期、重复 city/date。

### Demo 7：展示 Lakebase synced table

在 Lakebase SQL Editor 中运行：

```sql
SELECT *
FROM weather_daily_metrics
LIMIT 10;
```

讲解：Gold Delta table 是 analytical source of truth，Lakebase table 是 serving copy，给 API 做低延迟查询。

### Demo 8：启动 Spring Boot

运行：

```bash
mvn spring-boot:run
```

打开 Swagger：

```text
http://localhost:8080/swagger-ui.html
```

调用：

```text
/api/weather/daily
/api/weather/monthly
/api/weather/risks
```

---

## 20. README 模板

```markdown
# Weather Lakehouse Serving System

## Overview

This project builds an end-to-end weather analytics platform using Databricks, PySpark, Delta Lake, Lakebase Postgres, and Spring Boot.

The pipeline processes raw historical weather data through bronze, silver, and gold layers in Databricks, syncs curated Gold-layer tables into Lakebase Postgres, and exposes the metrics through REST APIs.

## Architecture

Raw Weather Data
→ Bronze Delta Table
→ Silver Cleaned Delta Table
→ Gold Analytical Tables
→ Lakebase Postgres Synced Tables
→ Spring Boot REST API

## Tech Stack

- Databricks
- PySpark
- Delta Lake
- Unity Catalog
- Lakebase Postgres
- Spring Boot
- PostgreSQL JDBC
- Swagger

## Data Source

The project uses historical daily weather data from Open-Meteo for multiple cities.

## Data Layers

### Bronze

Stores raw weather records with ingestion metadata.

### Silver

Cleans and standardizes weather records, adds date features and weather flags.

### Gold

Creates curated metrics tables:

- Daily city weather metrics
- Monthly city weather summary
- Weather risk days

## Serving Layer

Gold tables are synced to Lakebase Postgres and exposed through REST APIs.

## API Endpoints

- GET /api/weather/cities
- GET /api/weather/daily
- GET /api/weather/monthly
- GET /api/weather/risks

## How to Run

1. Fetch raw weather data.
2. Upload CSV to Databricks volume.
3. Run Databricks notebooks.
4. Sync Gold tables to Lakebase.
5. Start Spring Boot API.
6. Query endpoints with Swagger or Postman.
```

---

## 21. 简历 Bullet Points

- Built an end-to-end weather analytics lakehouse using Databricks, PySpark, Delta Lake, Lakebase Postgres, and Spring Boot, processing raw weather data across bronze, silver, and gold layers.
- Designed curated Gold-layer Delta tables for daily metrics, monthly summaries, and weather risk detection, then synced them into Lakebase Postgres for low-latency API serving.
- Implemented a Spring Boot REST API backed by Lakebase Postgres to expose city-level weather metrics, monthly trends, and risk-day alerts to downstream clients.
- Added data quality checks to validate table freshness, null dates, duplicate keys, and pipeline output consistency before publishing serving tables.

---

## 22. 教学安排

建议分 5 次课。

### Session 1：Architecture + Raw Data

内容：

1. 讲解项目架构。
2. 解释 Bronze / Silver / Gold。
3. 解释 Lakebase serving layer。
4. 本地拉 Open-Meteo 数据。
5. 生成 `raw_weather_daily.csv`。

产出：

- `raw_weather_daily.csv`
- architecture diagram

### Session 2：Databricks Bronze / Silver

内容：

1. 上传 CSV 到 Databricks。
2. 创建 volume。
3. 创建 Bronze table。
4. 创建 Silver table。
5. 做字段清洗和标准化。

产出：

- `bronze_weather_daily_raw`
- `silver_weather_daily_clean`

### Session 3：Gold Layer + Data Quality

内容：

1. 创建 daily metrics。
2. 创建 monthly summary。
3. 创建 risk days。
4. 添加 data quality checks。
5. 创建 Databricks job。

产出：

- `gold_city_daily_weather_metrics`
- `gold_city_monthly_weather_summary`
- `gold_weather_risk_days`
- `weather_lakehouse_pipeline` job

### Session 4：Lakebase Serving

内容：

1. 创建 Lakebase project。
2. 创建 database。
3. 创建 synced tables。
4. 验证 Lakebase SQL query。
5. 解释 serving layer 和 analytical layer 的区别。

产出：

- `weather_daily_metrics`
- `weather_monthly_summary`
- `weather_risk_days`

### Session 5：Backend API

内容：

1. 创建 Spring Boot project。
2. 配置 Lakebase JDBC。
3. 实现 Repository。
4. 实现 Service。
5. 实现 Controller。
6. 用 Swagger / Postman 测试 API。

产出：

- Spring Boot Weather API

---

## 23. 关键设计理念

### 23.1 为什么 Gold 还是 Delta？

因为 Gold Delta table 是 analytical source of truth。

它适合：

1. 大规模分析。
2. BI dashboard。
3. Spark processing。
4. 历史数据回放。
5. 数据 lineage。

### 23.2 为什么还需要 Lakebase？

因为 API 不适合每次直接查 Spark / Delta。

Lakebase 适合：

1. 低延迟 lookup。
2. REST API serving。
3. App 查询。
4. Agent 查询。
5. Postgres-compatible tools。

### 23.3 最终分工

Databricks Delta：

```text
负责数据处理和分析事实来源。
```

Lakebase：

```text
负责 operational serving。
```

Spring Boot：

```text
负责业务 API exposure。
```

一句话总结：

```text
Databricks processes the data, Lakebase serves the data, and Spring Boot exposes the data.
```

---

## 24. 替代方案

如果你的 workspace 没有 Lakebase，就用普通 Postgres 模拟。

替代架构：

```text
Gold Delta
    ↓
Export CSV / JDBC
    ↓
Local Postgres / Supabase / Neon
    ↓
Spring Boot API
```

README 里可以写：

```text
This project uses PostgreSQL to simulate the operational serving layer.
In a production Databricks environment, this can be replaced by Lakebase synced tables.
```

---

## 25. 最终验收 Checklist

项目完成时应该具备：

1. GitHub repo。
2. raw data fetch script。
3. Databricks notebooks。
4. Bronze table。
5. Silver table。
6. Gold tables。
7. Databricks workflow job。
8. Lakebase synced tables。
9. Spring Boot backend API。
10. Swagger / Postman examples。
11. README。
12. Architecture diagram。
13. Demo screenshots。

Databricks：

- [ ] `raw_weather_daily.csv` uploaded
- [ ] `bronze_weather_daily_raw` created
- [ ] `silver_weather_daily_clean` created
- [ ] `gold_city_daily_weather_metrics` created
- [ ] `gold_city_monthly_weather_summary` created
- [ ] `gold_weather_risk_days` created
- [ ] data quality notebook passed
- [ ] workflow job runs successfully

Lakebase：

- [ ] Lakebase project created
- [ ] database created
- [ ] synced table for daily metrics created
- [ ] synced table for monthly summary created
- [ ] synced table for risk days created
- [ ] SQL query returns data

Backend：

- [ ] Spring Boot starts successfully
- [ ] `/api/weather/cities` works
- [ ] `/api/weather/daily` works
- [ ] `/api/weather/monthly` works
- [ ] `/api/weather/risks` works
- [ ] Swagger works

Documentation：

- [ ] README complete
- [ ] architecture diagram included
- [ ] setup instructions included
- [ ] API examples included
- [ ] resume bullets included

---

## 26. 一句话项目介绍

中文：

```text
这是一个端到端天气数据处理与服务系统：使用 Databricks 将原始天气数据处理成 Bronze、Silver、Gold 三层 Delta 表，再将 Gold 层同步到 Lakebase Postgres，最后通过 Spring Boot REST API 对外提供低延迟查询。
```

英文：

```text
This project builds an end-to-end weather analytics serving system using Databricks, PySpark, Delta Lake, Lakebase Postgres, and Spring Boot, transforming raw weather data through bronze, silver, and gold layers and exposing curated metrics through REST APIs.
```

这份可以直接放进 Notes。后面可以再拆成两份：

1. 给同学的 step-by-step lab。
2. 自己的 GitHub README。
