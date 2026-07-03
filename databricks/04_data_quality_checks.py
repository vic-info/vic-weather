# Databricks notebook source
# Lesson 2 Notebook 04: Data Quality Checks

# COMMAND ----------

# 步骤 1：导入 Schema 元数据和校验函数
# MAGIC %run ./00_table_schemas

# COMMAND ----------

# 步骤 2：配置待检测的所有表路径，并初始化错误收集列表
catalog_name = "workspace"
schema_name = "default"

silver_table = f"{catalog_name}.{schema_name}.silver_weather_daily_clean"
gold_daily_table = f"{catalog_name}.{schema_name}.gold_city_daily_weather_metrics"
gold_monthly_table = f"{catalog_name}.{schema_name}.gold_city_monthly_weather_summary"
gold_risk_table = f"{catalog_name}.{schema_name}.gold_weather_risk_days"

failed_checks = []

# 定义异常捕获版的 Schema 校验器，避免单项质检失败立刻阻断全部流程
def check_schema(table_name, expected_schema):
    try:
        validate_dataframe_schema(spark.table(table_name), expected_schema, table_name)
    except Exception as error:
        failed_checks.append(f"FAIL: {table_name} schema mismatch: {error}")
        print(f"  FAIL — {table_name} schema mismatch: {error}")
    else:
        print(f"  PASS — {table_name} has the expected schema")

# COMMAND ----------

# 步骤 3：数据质量检查（DQ 1 - DQ 2）- Silver 清洗层校验
# 1. 确认 Silver 表不为空
silver_count = spark.table(silver_table).count()
if silver_count == 0:
    failed_checks.append(f"FAIL: {silver_table} is empty")
    print(f"  FAIL — {silver_table} is empty")
else:
    print(f"  PASS — {silver_table} has {silver_count} rows")

# 2. 确认 Silver 表中没有空的日期（weather_date）
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

# 步骤 4：数据质量检查（DQ 3 - DQ 5）- Gold Daily 表校验
# 1. 检验每日指标表 Schema 结构契约是否正确
check_schema(gold_daily_table, GOLD_DAILY_WEATHER_SCHEMA)

# 2. 确保每日指标表不为空
daily_count = spark.table(gold_daily_table).count()
if daily_count == 0:
    failed_checks.append(f"FAIL: {gold_daily_table} is empty")
    print(f"  FAIL — {gold_daily_table} is empty")
else:
    print(f"  PASS — {gold_daily_table} has {daily_count} rows")

# 3. 校验主键唯一性：确保同一个城市同一天不存在重复数据行
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

# 步骤 5：数据质量检查（DQ 6 - DQ 7）- Gold Monthly 表校验
# 1. 校验月度统计表的 Schema
check_schema(gold_monthly_table, GOLD_MONTHLY_WEATHER_SCHEMA)

# 2. 确保月度统计表不为空
monthly_count = spark.table(gold_monthly_table).count()
if monthly_count == 0:
    failed_checks.append(f"FAIL: {gold_monthly_table} is empty")
    print(f"  FAIL — {gold_monthly_table} is empty")
else:
    print(f"  PASS — {gold_monthly_table} has {monthly_count} rows")

# COMMAND ----------

# 步骤 6：数据质量检查（DQ 8 - DQ 10）- Gold Risk 表校验
# 1. 校验风险天数表的 Schema
check_schema(gold_risk_table, GOLD_RISK_WEATHER_SCHEMA)

# 2. 确保风险天数表可以正常执行查询（在没有灾害天气时，允许表为空，但不能无法访问）
try:
    risk_count = spark.table(gold_risk_table).count()
    print(f"  PASS — {gold_risk_table} is queryable with {risk_count} rows")
except Exception as e:
    failed_checks.append(f"FAIL: {gold_risk_table} query failed: {e}")
    print(f"  FAIL — {gold_risk_table} query failed: {e}")

# 3. 校验联合主键唯一性：确保同一城市同一天针对同一类型风险，没有重复写入数据
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

# 步骤 7：汇总检查结果，并通过 Databricks taskValues 向下游传递运行决策 (dq_passed)
# 这一机制能在工作流（Workflow Job）中将本次质检结果通知给后继任务。如果失败，则抛出异常以熔断流水线。
total_checks = 10
passed_checks = total_checks - len(failed_checks)
dq_passed = len(failed_checks) == 0

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
