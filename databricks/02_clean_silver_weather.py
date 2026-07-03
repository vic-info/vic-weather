# Databricks notebook source
# Lesson 1 Notebook 02: Clean Silver Weather

# COMMAND ----------

# 步骤 1：导入 Schema 定义与校验函数
# MAGIC %run ./00_table_schemas

# COMMAND ----------

# 步骤 2：引入 PySpark 清洗相关的时间处理与数学函数，并配置表路径
from pyspark.sql.functions import col, dayofweek, month, round, to_date, year

catalog_name = "workspace"
schema_name = "default"

bronze_table = f"{catalog_name}.{schema_name}.bronze_weather_daily_raw"
silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"

print(f"Reading Bronze table from: {bronze_table}")
print(f"Writing Silver table to: {silver_table}")

# COMMAND ----------

# 步骤 3：从 Delta 湖中读取 Bronze 层的原始数据并展示
bronze_df = spark.read.table(bronze_table)

display(bronze_df.limit(10))
bronze_df.printSchema()

# COMMAND ----------

# 步骤 4：执行核心清洗与转换逻辑
# 1. 过滤核心主键的空值
# 2. 转换日期格式，提取年份、月份、星期几等常用分析维度
# 3. 规范化列名以契合业务语义，计算派生字段（温差、是否雨天/炎热天/结冰天等布尔标记）
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

validate_dataframe_schema(silver_df, SILVER_WEATHER_SCHEMA, "silver_weather_daily_clean")
display(silver_df.limit(10))
silver_df.printSchema()

# COMMAND ----------

# 步骤 5：将清洗并校验通过的结构化数据 Overwrite 写入 Silver Delta 表中
(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver table created: {silver_table}")

# COMMAND ----------

# 步骤 6：通过 SQL 进行城市粒度的数据探索，验证清洗衍生字段的统计分布
spark.sql(f"""
SELECT
  city,
  COUNT(*) AS row_count,
  MIN(weather_date) AS min_date,
  MAX(weather_date) AS max_date,
  SUM(CASE WHEN is_rainy_day THEN 1 ELSE 0 END) AS rainy_days,
  SUM(CASE WHEN is_hot_day THEN 1 ELSE 0 END) AS hot_days,
  SUM(CASE WHEN is_freezing_day THEN 1 ELSE 0 END) AS freezing_days
FROM {silver_table}
GROUP BY city
ORDER BY city
""").show(truncate=False)
