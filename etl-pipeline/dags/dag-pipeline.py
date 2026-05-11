from datetime import datetime

from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

from self_healing_agent.callbacks import self_healing_failure_callback  # type: ignore[reportMissingImports]

default_args = {
    "owner": "data_engineering",
    "start_date": datetime(2026, 5, 1),
    "on_failure_callback": self_healing_failure_callback,
}

with DAG(
    "snowflake_medallion_etl_modular",
    default_args=default_args,
    schedule="@daily",  # Airflow 3 style
    catchup=False,
) as dag:
    load_bronze = SQLExecuteQueryOperator(
        task_id="load_to_bronze",
        conn_id="snowflake_default",
        sql="EXECUTE IMMEDIATE FROM @sql_scripts_stage/01_load_bronze.sql; ",
    )

    transform_silver = SQLExecuteQueryOperator(
        task_id="transform_to_silver",
        conn_id="snowflake_default",
        sql="EXECUTE IMMEDIATE FROM @sql_scripts_stage/02_transform_silver.sql; ",
    )

    calculate_gold = SQLExecuteQueryOperator(
        task_id="calculate_gold_kpis",
        conn_id="snowflake_default",
        sql="EXECUTE IMMEDIATE FROM @sql_scripts_stage/03_calculate_gold.sql; ",
    )

    load_bronze >> transform_silver >> calculate_gold