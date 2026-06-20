# Databricks notebook source
# Lesson 1 Notebook 00: Setup

# COMMAND ----------

catalog_name = "workspace"
schema_name = "default"
volume_name = "weather"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Volume: {volume_name}")
print(f"Upload raw CSV to: /Volumes/{catalog_name}/{schema_name}/{volume_name}/raw_weather_daily.csv")

# COMMAND ----------

spark.sql("SHOW CATALOGS").show(truncate=False)
spark.sql(f"SHOW SCHEMAS IN {catalog_name}").show(truncate=False)
spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema_name}").show(truncate=False)
