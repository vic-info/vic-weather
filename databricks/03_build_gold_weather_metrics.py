# Databricks notebook source
# Lesson 2 Notebook 03: Build Gold Weather Metrics

# COMMAND ----------

# MAGIC %run ./00_table_schemas

# COMMAND ----------

from pyspark.sql.functions import avg, col, lit, max, min, round, sum, when

catalog_name = "workspace"
schema_name = "default"

silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"
gold_daily_table = f"{catalog_name}.{schema_name}.gold_city_daily_weather_metrics"
gold_monthly_table = f"{catalog_name}.{schema_name}.gold_city_monthly_weather_summary"
gold_risk_table = f"{catalog_name}.{schema_name}.gold_weather_risk_days"

print(f"Reading Silver from: {silver_table}")
print(f"Writing Gold daily to: {gold_daily_table}")
print(f"Writing Gold monthly to: {gold_monthly_table}")
print(f"Writing Gold risk to: {gold_risk_table}")

# COMMAND ----------

silver_df = spark.read.table(silver_table)

display(silver_df.limit(5))
print(f"Silver row count: {silver_df.count()}")

# COMMAND ----------
# Gold Daily Metrics — grain: city + weather_date

daily_df = silver_df.select(
    col("city"),
    col("weather_date"),
    col("max_temp_c"),
    col("min_temp_c"),
    col("mean_temp_c"),
    col("precipitation_mm"),
    col("rain_mm"),
    col("snowfall_cm"),
    col("max_wind_speed_kmh"),
    col("is_rainy_day"),
    col("is_hot_day"),
    col("is_freezing_day"),
    (
        when(col("is_rainy_day"), 1).otherwise(0)
        + when(col("is_hot_day"), 1).otherwise(0)
        + when(col("is_freezing_day"), 1).otherwise(0)
        + when(col("max_wind_speed_kmh") > 40, 1).otherwise(0)
    ).alias("weather_severity_score"),
)

validate_dataframe_schema(daily_df, GOLD_DAILY_WEATHER_SCHEMA, "gold_city_daily_weather_metrics")
display(daily_df.limit(10))
print(f"Gold daily schema:")
daily_df.printSchema()

# COMMAND ----------

(
    daily_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_daily_table)
)

print(f"Gold daily table created: {gold_daily_table}")

# COMMAND ----------

spark.sql(f"""
SELECT city, COUNT(*) AS row_count,
       MIN(weather_date) AS min_date,
       MAX(weather_date) AS max_date,
       ROUND(AVG(weather_severity_score), 2) AS avg_severity
FROM {gold_daily_table}
GROUP BY city
ORDER BY city
""").show(truncate=False)

# COMMAND ----------
# Gold Monthly Summary — grain: city + year + month

monthly_df = (
    silver_df
    .groupBy(col("city"), col("year"), col("month"))
    .agg(
        round(avg(col("mean_temp_c")), 2).alias("avg_mean_temp_c"),
        max(col("max_temp_c")).alias("max_temp_c"),
        min(col("min_temp_c")).alias("min_temp_c"),
        round(sum(col("precipitation_mm")), 2).alias("total_precipitation_mm"),
        sum(when(col("is_rainy_day"), 1).otherwise(0)).alias("rainy_days"),
        sum(when(col("is_hot_day"), 1).otherwise(0)).alias("hot_days"),
        sum(when(col("is_freezing_day"), 1).otherwise(0)).alias("freezing_days"),
        round(avg(col("max_wind_speed_kmh")), 2).alias("avg_wind_speed_kmh"),
    )
    .orderBy(col("city"), col("year"), col("month"))
)

validate_dataframe_schema(monthly_df, GOLD_MONTHLY_WEATHER_SCHEMA, "gold_city_monthly_weather_summary")
display(monthly_df.limit(10))
print(f"Gold monthly schema:")
monthly_df.printSchema()

# COMMAND ----------

(
    monthly_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_monthly_table)
)

print(f"Gold monthly table created: {gold_monthly_table}")

# COMMAND ----------

spark.sql(f"""
SELECT city, year, COUNT(*) AS num_months,
       ROUND(AVG(avg_mean_temp_c), 2) AS year_avg_temp,
       ROUND(SUM(total_precipitation_mm), 2) AS year_total_precip
FROM {gold_monthly_table}
GROUP BY city, year
ORDER BY city, year
""").show(truncate=False)

# COMMAND ----------
# Gold Risk Days — event table, grain: city + weather_date + risk_type

hot_days_df = (
    silver_df
    .filter(col("max_temp_c") >= 35)
    .select(
        col("city"),
        col("weather_date"),
        lit("HOT_DAY").alias("risk_type"),
        when(col("max_temp_c") >= 40, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("max_temp_c").alias("metric_value"),
        lit("Max temp >= 35 C").alias("description"),
    )
)

freezing_days_df = (
    silver_df
    .filter(col("is_freezing_day"))
    .select(
        col("city"),
        col("weather_date"),
        lit("FREEZING").alias("risk_type"),
        when(col("min_temp_c") <= -10, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("min_temp_c").alias("metric_value"),
        lit("Min temp <= 0 C").alias("description"),
    )
)

heavy_rain_df = (
    silver_df
    .filter(col("precipitation_mm") >= 20)
    .select(
        col("city"),
        col("weather_date"),
        lit("HEAVY_RAIN").alias("risk_type"),
        when(col("precipitation_mm") >= 50, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("precipitation_mm").alias("metric_value"),
        lit("Precipitation >= 20 mm").alias("description"),
    )
)

strong_wind_df = (
    silver_df
    .filter(col("max_wind_speed_kmh") >= 40)
    .select(
        col("city"),
        col("weather_date"),
        lit("STRONG_WIND").alias("risk_type"),
        when(col("max_wind_speed_kmh") >= 60, "HIGH").otherwise("MEDIUM").alias("risk_level"),
        col("max_wind_speed_kmh").alias("metric_value"),
        lit("Wind speed >= 40 km/h").alias("description"),
    )
)

risk_df = (
    hot_days_df.unionByName(freezing_days_df)
    .unionByName(heavy_rain_df)
    .unionByName(strong_wind_df)
)

validate_dataframe_schema(risk_df, GOLD_RISK_WEATHER_SCHEMA, "gold_weather_risk_days")

# COMMAND ----------

print(f"Risk events found: {risk_df.count()}")
display(risk_df.limit(10))

# COMMAND ----------

(
    risk_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(gold_risk_table)
)

print(f"Gold risk table created: {gold_risk_table}")

# COMMAND ----------

spark.sql(f"""
SELECT risk_type, COUNT(*) AS event_count
FROM {gold_risk_table}
GROUP BY risk_type
ORDER BY risk_type
""").show(truncate=False)

spark.sql(f"""
SELECT city, COUNT(*) AS total_risks
FROM {gold_risk_table}
GROUP BY city
ORDER BY total_risks DESC
""").show(truncate=False)
