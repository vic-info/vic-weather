# Databricks notebook source
# Lesson 1 Notebook 01: Ingest Bronze Weather

# COMMAND ----------

# MAGIC %run ./00_table_schemas

# COMMAND ----------

from pyspark.sql.functions import current_timestamp, lit

catalog_name = "workspace"
schema_name = "default"
volume_name = "weather"

raw_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/raw_weather_daily.csv"
bronze_table = f"{catalog_name}.{schema_name}.bronze_weather_daily_raw"

print(f"Reading raw file from: {raw_path}")
print(f"Writing Bronze table to: {bronze_table}")

# COMMAND ----------

raw_df = (
    spark.read
    .schema(RAW_WEATHER_SCHEMA)
    .option("header", "true")
    .csv(raw_path)
)

validate_dataframe_schema(raw_df, RAW_WEATHER_SCHEMA, "raw_weather_csv")
display(raw_df.limit(10))
raw_df.printSchema()

# COMMAND ----------

bronze_df = (
    raw_df
    .withColumn("source", lit("open_meteo"))
    .withColumn("ingestion_timestamp", current_timestamp())
)

validate_dataframe_schema(bronze_df, BRONZE_WEATHER_SCHEMA, "bronze_weather_daily_raw")

(
    bronze_df.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(bronze_table)
)

print(f"Bronze table created: {bronze_table}")

# COMMAND ----------

spark.sql(f"""
SELECT COUNT(*) AS row_count
FROM {bronze_table}
""").show()

spark.sql(f"""
SELECT *
FROM {bronze_table}
LIMIT 10
""").show(truncate=False)
