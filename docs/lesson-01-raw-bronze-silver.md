# Lesson 1：Raw Data → Bronze → Silver

## 课程目标

本节课用 2 小时完成第一段数据工程链路，只覆盖 Raw → Bronze → Silver。

```text
Open-Meteo API
        ↓
Local CSV
        ↓
Databricks Volume
        ↓
Bronze Delta Table
        ↓
Silver Delta Table
```

学生完成后应该能解释：

1. Raw data 是什么。
2. Bronze 层为什么尽量保留原始字段。
3. Silver 层为什么要做字段标准化和业务标记。
4. Databricks Volume、Delta table、Spark DataFrame 在这个项目中的作用。

## 本节课边界

### 核心必讲

- Raw data 和 structured data 的区别。
- Bronze 层为什么保留原始字段。
- Silver 层为什么做字段标准化。
- PySpark transform 的基本写法。

### 简讲

- Unity Catalog：只讲 catalog / schema / table 的三段命名。
- Volume：只讲它是上传 CSV 的位置。
- Delta table：只讲它是后续 notebook 和 SQL 能查询的表。

### 不在本节讲

- Gold 表设计。
- Data quality checks。
- Databricks Workflow。
- Lakebase synced table。
- Spring Boot REST API。

## 时间安排

| 时间 | 内容 | 产出 |
| --- | --- | --- |
| 0:00-0:15 | 项目架构和课程目标 | 学生理解主线 |
| 0:15-0:35 | 本地抓取 Open-Meteo 数据 | `data/raw_weather_daily.csv` |
| 0:35-0:50 | 上传 CSV 到 Databricks Volume | raw CSV 位于 Volume |
| 0:50-1:20 | 创建 Bronze table | `bronze_weather_daily_raw` |
| 1:20-1:55 | 创建 Silver table | `silver_weather_daily_clean` |
| 1:55-2:00 | 总结和作业 | 学生知道下一步 |

## 课前准备

教师提前准备：

1. Databricks workspace。
2. 可运行的 cluster / SQL warehouse。
3. 本 repo。
4. Python 3.10+。

学生需要：

1. 能运行 Python。
2. 能登录 Databricks。
3. 能上传文件到 Databricks Volume。

## Step 1：本地抓数据

安装依赖：

```bash
pip3 install -r requirements.txt
```

运行脚本：

```bash
python3 data/fetch_open_meteo_weather.py
```

生成文件：

```text
data/raw_weather_daily.csv
```

检查 CSV：

```bash
head data/raw_weather_daily.csv
```

预期字段：

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

本 repo 已经验证过默认配置可以生成：

```text
1830 rows = 5 cities × 366 days
```

脚本会在写出 CSV 前做一次轻量 sanity check：

- 是否包含预期字段。
- 关键字段是否有空值。
- 每个城市的数据行数和日期范围。

这一步只是为了确认 raw CSV 可以上传，不替代第二节课的数据质量检查。

如果学生网络不稳定，可以直接使用教师提前生成的 `data/raw_weather_daily.csv`。

## Step 2：Databricks Setup

在 Databricks 中运行：

```text
databricks/00_setup.py
databricks/00_table_schemas.py
```

它们会创建：

```text
workspace.default.weather
```

课堂只需要说明：

- `workspace` 是 catalog。
- `default` 是 schema。
- `weather` 是 volume。
- CSV 文件会上传到 volume。
- `00_table_schemas` 集中展示 Raw、Bronze、Silver 和 Gold 的字段契约，并幂等创建各层空 Delta 表。

不要在第一节课展开 Unity Catalog 权限模型。

### Notebook 粘贴规则

如果是导入 repo 中的 Databricks source file，`# MAGIC %run` 会按 notebook cell 执行。

如果是手动粘贴代码，必须在 Bronze 和 Silver 顶部新建一个独立 cell，加载实际存放 schema 定义的 notebook。例如 schema 代码已合并到名为 `Setup` 的 notebook：

```python
%run ./Setup
```

`%run` 必须单独占一个 cell，并且路径使用 Databricks UI 中的实际 notebook 名称。

上传本地文件：

```text
data/raw_weather_daily.csv
```

到 Databricks 路径：

```text
/Volumes/workspace/default/weather/raw_weather_daily.csv
```

## Step 3：Bronze Table

运行：

```text
databricks/01_ingest_bronze_weather.py
```

输出表：

```text
workspace.default.bronze_weather_daily_raw
```

默认数据的预期结果：

```text
Schema validation passed: raw_weather_csv
Schema validation passed: bronze_weather_daily_raw
row_count = 1830
```

课堂重点：

- Bronze table 尽量保留原始字段。
- Bronze table 加上 `source` 和 `ingestion_timestamp`。
- 这层方便 debug、replay 和审计。

这一节不要在 Bronze 里做业务清洗。否则学生会混淆 Bronze 和 Silver 的职责。

## Step 4：Silver Table

运行：

```text
databricks/02_clean_silver_weather.py
```

输出表：

```text
workspace.default.silver_weather_daily_clean
```

默认数据的预期行数：

```text
1830
```

Silver 主要变化：

- `time` 转成 `weather_date`。
- 添加 `year`、`month`、`day_of_week`。
- 原始字段改成业务友好的字段名。
- 添加 `is_rainy_day`、`is_hot_day`、`is_freezing_day`。
- 添加 `temperature_range_c`。

这一步是本节课最重要的 PySpark transform。讲解时建议按顺序拆成四类：

1. 过滤坏数据。
2. 解析日期和添加时间维度。
3. 重命名原始字段。
4. 添加业务 flags。

## 第一节代码是否正确

当前第一节代码是正确的，理由如下：

1. `data/fetch_open_meteo_weather.py` 已经实际跑通，生成了 `data/raw_weather_daily.csv`。
2. CSV 字段和 Bronze notebook 读取逻辑一致。
3. Bronze notebook 只新增 `source` 和 `ingestion_timestamp`，没有提前做业务清洗。
4. Silver notebook 才做字段重命名、日期解析和业务 flags，职责边界清楚。
5. Silver 输出字段能直接支撑第二节课的 Gold 表设计。

当前第一节不需要加入 Gold、Data Quality、Lakebase 或 REST API。

## 课堂提问

1. 为什么 Bronze 不直接改字段名？
2. 如果 Open-Meteo 的字段明天变了，哪一层最容易发现问题？
3. 为什么 API 不应该直接查询 Bronze table？
4. `is_hot_day` 的阈值应该写死在代码里吗？生产环境可以怎么做？

## 课后作业

1. 新增一个城市，例如 Los Angeles。
2. 重新运行本地抓数脚本。
3. 重新上传 CSV。
4. 重新运行 Bronze 和 Silver notebooks。
5. 检查 Silver 表中新增城市是否出现。

进阶作业：

1. 添加 `is_windy_day` 字段，规则为 `max_wind_speed_kmh >= 30`。
2. 在 Silver 检查 SQL 中统计每个城市的 windy days。
