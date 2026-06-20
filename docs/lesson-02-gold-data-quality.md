# Lesson 2：Silver → Gold → Data Quality

## 课程目标

本节课用 2 小时完成第二段数据工程链路：

```text
Silver Delta Table
        ↓
Gold Daily Metrics
        ↓
Gold Monthly Summary
        ↓
Gold Risk Days
        ↓
Data Quality Checks
```

学生完成后应该能解释：

1. Gold 表为什么不是“更干净的 Silver”，而是面向业务消费的数据模型。
2. Daily metrics、Monthly summary、Risk days 三类表的不同粒度。
3. PySpark aggregation 和 business rule transform 的基本写法。
4. Data quality checks 为什么是数据发布前的质量门槛。

## 本节课边界

### 核心必讲

- Gold 表设计。
- 表粒度：`city + weather_date`、`city + year + month`、`city + weather_date + risk_type`。
- PySpark `groupBy` / `agg` / `when` / `unionByName`。
- Data quality 的基本检查：非空、日期非空、主键重复。

### 简讲

- Databricks Workflow：只讲它可以把 notebooks 串成 pipeline。
- Delta table：只强调 Gold 仍然是 analytical source of truth。

### 不在本节讲

- Lakebase synced table。
- Spring Boot REST API。
- Swagger / Postman。
- OAuth / service principal。
- Continuous sync。

## 时间安排

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 0:00-0:15 | 复习 Bronze / Silver / Gold 分层 | 学生理解 Gold 的消费导向 |
| 0:15-0:45 | 创建 Gold Daily Metrics | `gold_city_daily_weather_metrics` |
| 0:45-1:10 | 创建 Gold Monthly Summary | `gold_city_monthly_weather_summary` |
| 1:10-1:30 | 创建 Gold Risk Days | `gold_weather_risk_days` |
| 1:30-1:55 | 添加 Data Quality Checks | `04_data_quality_checks` |
| 1:55-2:00 | 总结和作业 | 学生知道第三节 serving 目标 |

## 课前准备

学生需要已经完成 Lesson 1，并且 Databricks 中存在：

```text
workspace.default.silver_weather_daily_clean
```

教师提前准备：

1. Lesson 1 的 Silver 表截图或查询结果。
2. Gold 表设计草图。
3. Data quality fail case 示例，例如重复 city/date 或空 date。

## Step 1：复习 Silver 表

先查询 Silver 表：

```sql
SELECT *
FROM workspace.default.silver_weather_daily_clean
LIMIT 10;
```

确认字段：

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
source
ingestion_timestamp
```

课堂重点：

- Silver 是清洗后的明细数据。
- Gold 是面向业务问题重新建模后的数据。

## Step 2：Gold Daily Metrics

输出表：

```text
workspace.default.gold_city_daily_weather_metrics
```

粒度：

```text
city + weather_date
```

用途：

- 支撑 API 查询某城市某日期范围的每日天气指标。
- 支撑 dashboard 的 daily trend。

关键字段：

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

课堂重点：

- 这是最接近 API serving 的主表。
- `weather_severity_score` 是业务规则，不是原始数据字段。
- Gold 表可以包含适合下游使用的派生指标。

## Step 3：Gold Monthly Summary

输出表：

```text
workspace.default.gold_city_monthly_weather_summary
```

粒度：

```text
city + year + month
```

用途：

- 支撑月度趋势分析。
- 支撑 dashboard 汇总卡片。
- 支撑 API 查询某城市某年的月度统计。

关键字段：

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

课堂重点：

- Monthly summary 是 aggregation 表。
- 它牺牲明细，换取更简单、更快的查询。
- `sum(when(...))` 是统计 boolean flag 天数的常见写法。

## Step 4：Gold Risk Days

输出表：

```text
workspace.default.gold_weather_risk_days
```

粒度：

```text
city + weather_date + risk_type
```

用途：

- 支撑天气风险告警。
- 支撑 API 查询风险日列表。
- 支撑事件型数据分析。

风险类型：

```text
HEAVY_RAIN
HOT_DAY
FREEZING
STRONG_WIND
```

课堂重点：

- Risk days 是事件表。
- 同一天同一个城市可能有多个风险事件。
- 事件表通常比宽表更适合 alert / notification / filtering。

## Step 5：Data Quality Checks

Data quality 检查目标：

1. Silver 表不为空。
2. Silver 表 `weather_date` 不为空。
3. Gold daily 表不为空。
4. Gold daily 表没有重复 `city + weather_date`。
5. Gold monthly 表不为空。
6. Risk table 可以为空，但要能成功查询。

示例检查：

```sql
SELECT city, weather_date, COUNT(*) AS cnt
FROM workspace.default.gold_city_daily_weather_metrics
GROUP BY city, weather_date
HAVING COUNT(*) > 1;
```

课堂重点：

- Data quality 不是为了“看起来专业”，而是为了阻止坏数据进入 serving layer。
- 第一版只做简单 assert，后续可以扩展 Great Expectations / DLT expectations / alerting。
- Risk table 为空不一定是错误，因为某些数据集可能没有风险日。

## Step 6：Workflow 简讲

本节只展示概念，不建议占用太多时间现场配置。

推荐依赖顺序：

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

课堂重点：

- Workflow 的职责是 orchestration。
- Notebook 的职责是 transform。
- Data quality checks 应该放在 publish / serving 之前。

## 验收标准

学生完成本节后，Databricks 中应该有：

```text
workspace.default.gold_city_daily_weather_metrics
workspace.default.gold_city_monthly_weather_summary
workspace.default.gold_weather_risk_days
```

并且：

- Gold daily 表有数据。
- Gold monthly 表有数据。
- Gold daily 表没有重复 `city + weather_date`。
- Data quality notebook 能完整跑完。

## 课堂提问

1. Gold 表和 Silver 表的区别是什么？
2. 为什么 daily metrics 的粒度应该是 `city + weather_date`？
3. 为什么 risk days 适合做成事件表？
4. Data quality checks 应该在 Gold 前做，还是 Gold 后做？
5. 如果 `weather_severity_score` 规则变了，应该重跑哪几层？

## 课后作业

1. 给 daily metrics 添加 `is_windy_day` 字段。
2. 把 `weather_severity_score` 规则改成 windy day 也加 1。
3. 在 monthly summary 中统计 `windy_days`。
4. 在 data quality checks 中检查 daily metrics 的 `weather_severity_score` 不小于 0。

进阶作业：

1. 新增风险类型 `DRY_DAY`，规则为 `precipitation_mm = 0` 且 `max_temp_c >= 30`。
2. 给 risk days 添加 `created_at` 字段。
3. 设计一个 `gold_city_yearly_weather_summary` 表。
