from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import os

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
}

dag = DAG(
    dag_id="postgres_to_parquet_export",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["export", "parquet", "postgres"],
)

OUTPUT_DIR = "/opt/airflow/output"

# -----------------------------
# TASK 1: CHECK TABLE EXISTS
# -----------------------------
def check_table_exists():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    records = hook.get_records(
        "SELECT COUNT(*) FROM transformed_employee_data"
    )

    row_count = records[0][0]

    if row_count == 0:
        raise ValueError("transformed_employee_data is empty")

    return row_count

# -----------------------------
# TASK 2: EXPORT TO PARQUET
# -----------------------------
def export_to_parquet(**context):
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    df = pd.read_sql("SELECT * FROM transformed_employee_data", engine)

    execution_date = context["ds"]
    file_name = f"employee_data_{execution_date}.parquet"
    file_path = os.path.join(OUTPUT_DIR, file_name)

    table = pa.Table.from_pandas(df)
    pq.write_table(table, file_path, compression="snappy")

    file_size = os.path.getsize(file_path)

    return {
        "file_path": file_path,
        "row_count": len(df),
        "file_size_bytes": file_size
    }

# -----------------------------
# TASK 3: VALIDATE PARQUET FILE
# -----------------------------
def validate_parquet(**context):
    ti = context["ti"]
    metadata = ti.xcom_pull(task_ids="export_to_parquet")

    file_path = metadata["file_path"]

    table = pq.read_table(file_path)
    df = table.to_pandas()

    if df.empty:
        raise ValueError("Parquet file is empty")

    return True

# -----------------------------
# TASK DEFINITIONS
# -----------------------------
check_table_task = PythonOperator(
    task_id="check_source_table",
    python_callable=check_table_exists,
    dag=dag,
)

export_task = PythonOperator(
    task_id="export_to_parquet",
    python_callable=export_to_parquet,
    provide_context=True,
    dag=dag,
)

validate_task = PythonOperator(
    task_id="validate_parquet_file",
    python_callable=validate_parquet,
    provide_context=True,
    dag=dag,
)

# -----------------------------
# TASK DEPENDENCIES
# -----------------------------
check_table_task >> export_task >> validate_task
