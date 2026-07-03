# Databricks notebook source
# Define schema contracts and create empty Bronze, Silver, and Gold Delta tables

# COMMAND ----------

# 步骤 1：导入 PySpark 相关的类型定义，用于构建静态 Schema 契约 (Data Contract)
from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# COMMAND ----------

# 步骤 2：定义原始 CSV 数据结构 (Raw CSV Schema)
# 采用读取时指定 Schema (Schema-on-read)，避免 Spark 自动推断类型出错
RAW_WEATHER_SCHEMA = StructType([
    StructField("time", StringType(), True),
    StructField("temperature_2m_max", DoubleType(), True),
    StructField("temperature_2m_min", DoubleType(), True),
    StructField("temperature_2m_mean", DoubleType(), True),
    StructField("precipitation_sum", DoubleType(), True),
    StructField("rain_sum", DoubleType(), True),
    StructField("snowfall_sum", DoubleType(), True),
    StructField("wind_speed_10m_max", DoubleType(), True),
    StructField("city", StringType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

# COMMAND ----------

# 步骤 3：定义 Bronze (青铜/原始) 表结构
# Bronze 层保留原始副本，另外追加了数据源(source)和时间戳(ingestion_timestamp)审计字段
BRONZE_WEATHER_SCHEMA = StructType([
    *RAW_WEATHER_SCHEMA.fields,
    StructField("source", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------

# 步骤 4：定义 Silver (白银/清洗) 表结构
# Silver 层对字段重命名规范化，做类型转换（time -> weather_date），并提取日期特征及业务布尔判定字段
SILVER_WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("weather_date", DateType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("day_of_week", IntegerType(), True),
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
    StructField("max_temp_c", DoubleType(), True),
    StructField("min_temp_c", DoubleType(), True),
    StructField("mean_temp_c", DoubleType(), True),
    StructField("temperature_range_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("rain_mm", DoubleType(), True),
    StructField("snowfall_cm", DoubleType(), True),
    StructField("max_wind_speed_kmh", DoubleType(), True),
    StructField("is_rainy_day", BooleanType(), True),
    StructField("is_hot_day", BooleanType(), True),
    StructField("is_freezing_day", BooleanType(), True),
    StructField("source", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------

# 步骤 5：定义 Gold (黄金/应用) 层的三个业务表结构，服务于不同的下游分析需求

# 1. 每日天气指标表 (粒度：每个城市每天一行)，包含自定义的天气严重性得分
GOLD_DAILY_WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("weather_date", DateType(), True),
    StructField("max_temp_c", DoubleType(), True),
    StructField("min_temp_c", DoubleType(), True),
    StructField("mean_temp_c", DoubleType(), True),
    StructField("precipitation_mm", DoubleType(), True),
    StructField("rain_mm", DoubleType(), True),
    StructField("snowfall_cm", DoubleType(), True),
    StructField("max_wind_speed_kmh", DoubleType(), True),
    StructField("is_rainy_day", BooleanType(), True),
    StructField("is_hot_day", BooleanType(), True),
    StructField("is_freezing_day", BooleanType(), True),
    StructField("weather_severity_score", IntegerType(), False),
])

# COMMAND ----------

# 2. 月度天气统计表 (粒度：每个城市每个月一行)，包含月气温极值、均值和降水量累计
GOLD_MONTHLY_WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("year", IntegerType(), True),
    StructField("month", IntegerType(), True),
    StructField("avg_mean_temp_c", DoubleType(), True),
    StructField("max_temp_c", DoubleType(), True),
    StructField("min_temp_c", DoubleType(), True),
    StructField("total_precipitation_mm", DoubleType(), True),
    StructField("rainy_days", LongType(), True),
    StructField("hot_days", LongType(), True),
    StructField("freezing_days", LongType(), True),
    StructField("avg_wind_speed_kmh", DoubleType(), True),
])

# COMMAND ----------

# 3. 气象风险天数事件表 (粒度：每个风险事件一行)，记录高温、冰冻、暴雨和大风等极端气象
GOLD_RISK_WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("weather_date", DateType(), True),
    StructField("risk_type", StringType(), False),
    StructField("risk_level", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("description", StringType(), False),
])

# COMMAND ----------

# 步骤 6：配置全局物理层和 Delta 表的 Schema 映射字典
LAYER_SCHEMAS = {
    "raw_weather_csv": RAW_WEATHER_SCHEMA,
    "bronze_weather_daily_raw": BRONZE_WEATHER_SCHEMA,
    "silver_weather_daily_clean": SILVER_WEATHER_SCHEMA,
    "gold_city_daily_weather_metrics": GOLD_DAILY_WEATHER_SCHEMA,
    "gold_city_monthly_weather_summary": GOLD_MONTHLY_WEATHER_SCHEMA,
    "gold_weather_risk_days": GOLD_RISK_WEATHER_SCHEMA,
}

catalog_name = "workspace"
schema_name = "default"

TABLE_SCHEMAS = {
    f"{catalog_name}.{schema_name}.bronze_weather_daily_raw": BRONZE_WEATHER_SCHEMA,
    f"{catalog_name}.{schema_name}.silver_weather_daily_clean": SILVER_WEATHER_SCHEMA,
    f"{catalog_name}.{schema_name}.gold_city_daily_weather_metrics": GOLD_DAILY_WEATHER_SCHEMA,
    f"{catalog_name}.{schema_name}.gold_city_monthly_weather_summary": GOLD_MONTHLY_WEATHER_SCHEMA,
    f"{catalog_name}.{schema_name}.gold_weather_risk_days": GOLD_RISK_WEATHER_SCHEMA,
}


# COMMAND ----------

# 步骤 7：定义数据质量控制校验函数，验证实际 DataFrame 是否符合 Schema 规范
def validate_dataframe_schema(df, expected_schema, dataset_name):
    actual_fields = {field.name: field.dataType for field in df.schema.fields}
    expected_fields = {field.name: field.dataType for field in expected_schema.fields}

    missing = sorted(set(expected_fields) - set(actual_fields))
    unexpected = sorted(set(actual_fields) - set(expected_fields))
    wrong_types = sorted(
        f"{name}: expected {expected_fields[name].simpleString()}, "
        f"got {actual_fields[name].simpleString()}"
        for name in set(actual_fields) & set(expected_fields)
        if actual_fields[name] != expected_fields[name]
    )

    problems = []
    if missing:
        problems.append(f"missing columns={missing}")
    if unexpected:
        problems.append(f"unexpected columns={unexpected}")
    if wrong_types:
        problems.append(f"wrong types={wrong_types}")

    if problems:
        raise ValueError(f"Schema validation failed for {dataset_name}: {'; '.join(problems)}")

    print(f"Schema validation passed: {dataset_name}")


# 步骤 8：定义空 Delta 表初始化创建函数 (表存在时则忽略以保护数据)
def create_table_if_not_exists(table_name, table_schema):
    (
        spark.createDataFrame([], table_schema)
        .write
        .format("delta")
        .mode("ignore")
        .saveAsTable(table_name)
    )
    validate_dataframe_schema(spark.table(table_name), table_schema, table_name)
    print(f"Table ready: {table_name}")


# COMMAND ----------

# 步骤 9：提取各层 Schema 的结构属性，生成并以表格形式展示“数据字典”
schema_rows = [
    (layer, position + 1, field.name, field.dataType.simpleString(), field.nullable)
    for layer, schema in LAYER_SCHEMAS.items()
    for position, field in enumerate(schema.fields)
]

schema_catalog_df = spark.createDataFrame(
    schema_rows,
    ["layer", "position", "column_name", "data_type", "nullable"],
)

display(schema_catalog_df.orderBy("layer", "position"))

# COMMAND ----------

# 步骤 10：执行数据库 Schema 创建，并批量初始化所有的空 Delta 表
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

for table_name, table_schema in TABLE_SCHEMAS.items():
    create_table_if_not_exists(table_name, table_schema)

print(f"{len(TABLE_SCHEMAS)} Delta tables are ready.")
