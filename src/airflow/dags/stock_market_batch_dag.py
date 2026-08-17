import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "prasun",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define DAG
dag = DAG(
    "stock_market_batch_pipeline",
    default_args=default_args,
    description="Stock Market Data Pipeline",
    schedule_interval=timedelta(days=1),
    start_date=datetime(2026, 4, 1),
    catchup=False,
)


# ── Task 1: Fetch historical data from AlphaVantage and publish to Kafka ──────
fetch_historical_data = BashOperator(
    task_id="fetch_historical_data",
    bash_command="python /opt/airflow/dags/scripts/batch_data_producer.py {{ ds }}",
    dag=dag,
)


# ── Task 2: Consume from Kafka and write raw CSVs to MinIO ────────────────────
# BUG FIX: task_id was incorrectly set to "fetch_historical_data" (duplicate).
# Fixed to "consume_historical_data".
consume_historical_data = BashOperator(
    task_id="consume_historical_data",
    bash_command="python /opt/airflow/dags/scripts/batch_data_consumer.py {{ ds }}",
    dag=dag,
)


# ── Task 3: Spark batch processing — reads raw CSVs, writes Parquet to MinIO ──
process_data = BashOperator(
    task_id="process_data",
    bash_command="""
    docker exec stockmarket_datapipeline-spark-master-1 \
        spark-submit \
            --master spark://spark-master:7077 \
            --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
            /opt/spark/jobs/spark_batch_processor.py {{ ds }}
    """,
    dag=dag,
)


# ── Task 4: Incremental load from MinIO Parquet into Snowflake ────────────────
load_to_snowflake = BashOperator(
    task_id="load_historical_to_snowflake",
    bash_command="python /opt/airflow/dags/scripts/load_to_snowflake.py {{ ds }}",
    dag=dag,
)


# ── Task 5: Completion marker ─────────────────────────────────────────────────
# BUG FIX: dag= parameter was missing, so Airflow could not register this task.
process_complete = BashOperator(
    task_id="process_complete",
    bash_command='echo "Batch pipeline for {{ ds }} completed successfully."',
    dag=dag,
)


# ── DAG dependency chain ──────────────────────────────────────────────────────
fetch_historical_data >> consume_historical_data >> process_data >> load_to_snowflake >> process_complete
