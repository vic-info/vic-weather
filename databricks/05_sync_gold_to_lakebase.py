# Databricks notebook source
# Paste this entire file into one Databricks Python cell and run it.

# 步骤 1：引入依赖并定义 Serving 层同步的元数据配置 (Lakebase/Postgres)
# Lakebase 要求在目标端定义主键以支持 upsert (合并覆盖) 的快照增量同步
import time
from typing import TYPE_CHECKING, Any

from databricks.sdk import WorkspaceClient

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

    dbutils: Any
    spark: SparkSession


PROJECT_ID = "vic-weather-db"
BRANCH = f"projects/{PROJECT_ID}/branches/production"
POSTGRES_DATABASE = "databricks_postgres"

SYNC_TABLES = [
    (
        "workspace.default.gold_city_daily_weather_metrics",
        "workspace.default.weather_daily_metrics",
        ["city", "weather_date"],
    ),
    # 如有需要，可以取消注释以同步月度摘要表和风险事件表
    # (
    #     "workspace.default.gold_city_monthly_weather_summary",
    #     "workspace.default.weather_monthly_summary",
    #     ["city", "year", "month"],
    # ),
    # (
    #     "workspace.default.gold_weather_risk_days",
    #     "workspace.default.weather_risk_days",
    #     ["city", "weather_date", "risk_type"],
    # )
]

# 步骤 2：初始化 Databricks SDK 的 Workspace 客户端
client = WorkspaceClient()
api = client.api_client
headers = {"Accept": "application/json", "Content-Type": "application/json"}


# 步骤 3：定义向 Lakebase API 注册同步表任务的函数（内含异步长任务 Polling 进度查询）
def create_synced_table(source_table, target_table, primary_keys):
    operation = api.do(
        "POST",
        "/api/2.0/postgres/synced_tables",
        query={"synced_table_id": target_table},
        headers=headers,
        body={
            "spec": {
                "source_table_full_name": source_table,
                "branch": BRANCH,
                "postgres_database": POSTGRES_DATABASE,
                "primary_key_columns": primary_keys,
                "scheduling_policy": "SNAPSHOT",
                "create_database_objects_if_missing": True,
            }
        },
    )

    # 循环检查异步操作进度
    while not operation.get("done", False):
        time.sleep(5)
        operation = api.do(
            "GET",
            f"/api/2.0/postgres/{operation['name']}",
            headers=headers,
        )

    if operation.get("error"):
        raise RuntimeError(f"Lakebase sync failed: {operation['error']}")

    return operation["response"]["name"]


# 步骤 4：定义阻塞等待目标表就绪 (detailed_state 达到 ONLINE) 的轮询函数
def wait_until_online(target_table, timeout_seconds=900):
    deadline = time.time() + timeout_seconds
    previous_state = None

    while time.time() < deadline:
        synced_table = api.do(
            "GET",
            f"/api/2.0/postgres/synced_tables/{target_table}",
            headers=headers,
        )
        status = synced_table.get("status", {})
        state = status.get("detailed_state", "UNKNOWN")

        if state != previous_state:
            print(f"Sync state: {target_table} -> {state}")
            previous_state = state

        if "FAILED" in state:
            raise RuntimeError(
                f"Lakebase sync failed for {target_table}: {status.get('message')}"
            )

        if state.startswith("SYNCED_TABLE_ONLINE"):
            return

        time.sleep(10)

    raise TimeoutError(f"Timed out waiting for Lakebase table: {target_table}")


# 步骤 5：遍历同步任务，检查源表就绪状态，触发同步并等待上线
for source_table, target_table, primary_keys in SYNC_TABLES:
    if not spark.catalog.tableExists(source_table):
        raise RuntimeError(f"Source table does not exist: {source_table}")

    print(f"Source ready: {source_table} ({spark.table(source_table).count()} rows)")

    created_name = create_synced_table(source_table, target_table, primary_keys)
    print(f"Created synced-table resource: {created_name}")
    wait_until_online(target_table)
    print(f"Postgres table online: {target_table}")

print("Lakebase sync setup completed.")
print("Postgres table: default.weather_daily_metrics")
