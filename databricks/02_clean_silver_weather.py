# Databricks notebook source
# Lesson 1 Notebook 02: Clean Silver Weather

# COMMAND ----------

# MAGIC %run ./00_table_schemas

# COMMAND ----------

from pyspark.sql.functions import col, dayofweek, month, round, to_date, year

catalog_name = "workspace"
schema_name = "default"

bronze_table = f"{catalog_name}.{schema_name}.bronze_weather_daily_raw"
silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"

print(f"Reading Bronze table from: {bronze_table}")
print(f"Writing Silver table to: {silver_table}")

# COMMAND ----------

bronze_df = spark.read.table(bronze_table)

display(bronze_df.limit(10))
bronze_df.printSchema()

# COMMAND ----------

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

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(silver_table)
)

print(f"Silver table created: {silver_table}")

# COMMAND ----------

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
