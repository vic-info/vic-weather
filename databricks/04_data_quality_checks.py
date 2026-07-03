# Databricks notebook source
# Lesson 2 Notebook 04: Data Quality Checks

# COMMAND ----------

# MAGIC %run ./00_table_schemas

# COMMAND ----------

catalog_name = "workspace"
schema_name = "default"

silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"
gold_daily_table = f"{catalog_name}.{schema_name}.gold_city_daily_weather_metrics"
gold_monthly_table = f"{catalog_name}.{schema_name}.gold_city_monthly_weather_summary"
gold_risk_table = f"{catalog_name}.{schema_name}.gold_weather_risk_days"

failed_checks = []

def check_schema(table_name, expected_schema):
    try:
        validate_dataframe_schema(spark.table(table_name), expected_schema, table_name)
    except Exception as error:
        failed_checks.append(f"FAIL: {table_name} schema mismatch: {error}")
        print(f"  FAIL — {table_name} schema mismatch: {error}")
    else:
        print(f"  PASS — {table_name} has the expected schema")

# COMMAND ----------
# DQ 1: Silver table is not empty

silver_count = spark.table(silver_table).count()
if silver_count == 0:
    failed_checks.append(f"FAIL: {silver_table} is empty")
    print(f"  FAIL — {silver_table} is empty")
else:
    print(f"  PASS — {silver_table} has {silver_count} rows")

# COMMAND ----------
# DQ 2: Silver table has no null weather_date

silver_null_dates = (
    spark.table(silver_table)
    .filter("weather_date IS NULL")
    .count()
)
if silver_null_dates > 0:
    failed_checks.append(f"FAIL: {silver_table} has {silver_null_dates} null weather_date rows")
    print(f"  FAIL — {silver_table} has {silver_null_dates} null weather_date rows")
else:
    print(f"  PASS — {silver_table} has no null weather_date")

# COMMAND ----------
# DQ 3: Gold daily table has expected schema

check_schema(gold_daily_table, GOLD_DAILY_WEATHER_SCHEMA)

# COMMAND ----------
# DQ 4: Gold daily table is not empty

daily_count = spark.table(gold_daily_table).count()
if daily_count == 0:
    failed_checks.append(f"FAIL: {gold_daily_table} is empty")
    print(f"  FAIL — {gold_daily_table} is empty")
else:
    print(f"  PASS — {gold_daily_table} has {daily_count} rows")

# COMMAND ----------
# DQ 5: Gold daily table has no duplicate city + weather_date

daily_dupes = (
    spark.table(gold_daily_table)
    .groupBy("city", "weather_date")
    .count()
    .filter("count > 1")
    .count()
)
if daily_dupes > 0:
    failed_checks.append(f"FAIL: {gold_daily_table} has {daily_dupes} duplicate city+weather_date")
    print(f"  FAIL — {gold_daily_table} has {daily_dupes} duplicate city+weather_date pairs")
else:
    print(f"  PASS — {gold_daily_table} has no duplicate city+weather_date")

# COMMAND ----------
# DQ 6: Gold monthly table has expected schema

check_schema(gold_monthly_table, GOLD_MONTHLY_WEATHER_SCHEMA)

# COMMAND ----------
# DQ 7: Gold monthly table is not empty

monthly_count = spark.table(gold_monthly_table).count()
if monthly_count == 0:
    failed_checks.append(f"FAIL: {gold_monthly_table} is empty")
    print(f"  FAIL — {gold_monthly_table} is empty")
else:
    print(f"  PASS — {gold_monthly_table} has {monthly_count} rows")

# COMMAND ----------
# DQ 8: Gold risk table has expected schema

check_schema(gold_risk_table, GOLD_RISK_WEATHER_SCHEMA)

# COMMAND ----------
# DQ 9: Gold risk table is queryable (can be empty)

try:
    risk_count = spark.table(gold_risk_table).count()
    print(f"  PASS — {gold_risk_table} is queryable with {risk_count} rows")
except Exception as e:
    failed_checks.append(f"FAIL: {gold_risk_table} query failed: {e}")
    print(f"  FAIL — {gold_risk_table} query failed: {e}")

# COMMAND ----------
# DQ 10: Gold risk table has no duplicate city + weather_date + risk_type

risk_dupes = (
    spark.table(gold_risk_table)
    .groupBy("city", "weather_date", "risk_type")
    .count()
    .filter("count > 1")
    .count()
)
if risk_dupes > 0:
    failed_checks.append(f"FAIL: {gold_risk_table} has {risk_dupes} duplicate city+date+risk_type")
    print(f"  FAIL — {gold_risk_table} has {risk_dupes} duplicate city+date+risk_type rows")
else:
    print(f"  PASS — {gold_risk_table} has no duplicate city+date+risk_type")

# COMMAND ----------
# Summary

total_checks = 10
passed_checks = total_checks - len(failed_checks)
dq_passed = len(failed_checks) == 0

# Lakeflow Jobs task values are available to downstream If/else tasks.
dbutils.jobs.taskValues.set(key="dq_passed", value=dq_passed)

print(f"\n{'='*50}")
print(f"Data Quality Summary: {passed_checks}/{total_checks} passed")
print(f"Workflow task value: dq_passed={str(dq_passed).lower()}")
print(f"{'='*50}")

if failed_checks:
    print("\nFailed checks:")
    for check in failed_checks:
        print(f"  {check}")
    raise RuntimeError(f"Data quality checks failed: {len(failed_checks)}/{total_checks}")
else:
    print("All data quality checks passed!")
