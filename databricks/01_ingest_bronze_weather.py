# Databricks notebook source
# Lesson 1 Notebook 01: Ingest Bronze Weather

# COMMAND ----------

# 步骤 1：使用 %run 魔术命令引入 Schema 定义和工具校验函数
# MAGIC %run ./00_table_schemas

# COMMAND ----------

# 步骤 2：配置原始 CSV 输入路径与 Bronze 目标表全路径
from pyspark.sql.functions import current_timestamp, lit

catalog_name = "workspace"
schema_name = "default"
volume_name = "weather"

raw_path = f"/Volumes/{catalog_name}/{schema_name}/{volume_name}/raw_weather_daily.csv"
bronze_table = f"{catalog_name}.{schema_name}.bronze_weather_daily_raw"

print(f"Reading raw file from: {raw_path}")
print(f"Writing Bronze table to: {bronze_table}")

# COMMAND ----------

# 步骤 3：读取原始天气 CSV 数据，应用 Raw Schema，并对结构进行强校验
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

# 步骤 4：追加审计特征列（数据源、写入系统时间戳），验证后将数据 Overwrite 写入 Bronze Delta 表中
# Bronze 层保存完整未清洗的数据备份，以便在清洗逻辑变更时随时重新加工
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

# 步骤 5：使用 Spark SQL 查询并展示 Bronze 表的记录行数与前 10 行样本，完成落地验证
spark.sql(f"""
SELECT COUNT(*) AS row_count
FROM {bronze_table}
""").show()

spark.sql(f"""
SELECT *
FROM {bronze_table}
LIMIT 10
""").show(truncate=False)
