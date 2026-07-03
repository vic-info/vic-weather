# Databricks notebook source
# Lesson 1 Notebook 00: Setup

# COMMAND ----------

# 步骤 1：定义 Unity Catalog (UC) 的三层命名空间（Catalog -> Schema -> Volume）
# Volume 是 UC 中管理非结构化或半结构化原始数据文件（如原始 CSV）的存储桶封装
catalog_name = "workspace"
schema_name = "default"
volume_name = "weather"

# 步骤 2：在 Unity Catalog 中创建 Schema (数据库) 和 Volume (存储卷)
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog_name}.{schema_name}.{volume_name}")

print(f"Catalog: {catalog_name}")
print(f"Schema: {schema_name}")
print(f"Volume: {volume_name}")
print(f"Upload raw CSV to: /Volumes/{catalog_name}/{schema_name}/{volume_name}/raw_weather_daily.csv")

# COMMAND ----------

# 步骤 3：验证创建结果，展示当前环境下的 Catalog、Schema 和 Volume 元数据
spark.sql("SHOW CATALOGS").show(truncate=False)
spark.sql(f"SHOW SCHEMAS IN {catalog_name}").show(truncate=False)
spark.sql(f"SHOW VOLUMES IN {catalog_name}.{schema_name}").show(truncate=False)
