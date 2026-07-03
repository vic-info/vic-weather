# Databricks notebook source
# Define schema contracts and create empty Bronze, Silver, and Gold Delta tables

# COMMAND ----------

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
# Raw CSV schema

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
# Bronze table schema

BRONZE_WEATHER_SCHEMA = StructType([
    *RAW_WEATHER_SCHEMA.fields,
    StructField("source", StringType(), False),
    StructField("ingestion_timestamp", TimestampType(), False),
])

# COMMAND ----------
# Silver table schema

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
# Gold daily metrics schema: city + weather_date

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
# Gold monthly summary schema: city + year + month

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
# Gold risk event schema: city + weather_date + risk_type

GOLD_RISK_WEATHER_SCHEMA = StructType([
    StructField("city", StringType(), True),
    StructField("weather_date", DateType(), True),
    StructField("risk_type", StringType(), False),
    StructField("risk_level", StringType(), False),
    StructField("metric_value", DoubleType(), True),
    StructField("description", StringType(), False),
])

# COMMAND ----------

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
# Create the database schema and empty Delta tables without overwriting existing data.

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog_name}.{schema_name}")

for table_name, table_schema in TABLE_SCHEMAS.items():
    create_table_if_not_exists(table_name, table_schema)

print(f"{len(TABLE_SCHEMAS)} Delta tables are ready.")
