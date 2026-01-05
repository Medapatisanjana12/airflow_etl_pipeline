from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv

default_args = {
    "owner": "airflow",
    "retries": 1,
}

dag = DAG(
    dag_id="csv_to_postgres_ingestion",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "csv", "postgres"],
)

def create_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("""
        CREATE TABLE IF NOT EXISTS raw_employee_data (
            id INTEGER PRIMARY KEY,
            name VARCHAR(255),
            age INTEGER,
            city VARCHAR(100),
            salary NUMERIC(10,2),
            join_date DATE
        );
    """)

def truncate_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("TRUNCATE TABLE raw_employee_data;")

def load_csv():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cur = conn.cursor()

    file_path = "/opt/airflow/data/input.csv"
    rows = 0

    with open(file_path, "r") as f:
        reader = csv.DictReader(f)
        for row in reader:
            cur.execute(
                """
                INSERT INTO raw_employee_data
                (id, name, age, city, salary, join_date)
                VALUES (%s, %s, %s, %s, %s, %s)
                """,
                (
                    int(row["id"]),
                    row["name"],
                    int(row["age"]),
                    row["city"],
                    float(row["salary"]),
                    row["join_date"],
                ),
            )
            rows += 1

    conn.commit()
    cur.close()
    conn.close()
    return rows

create_table_task = PythonOperator(
    task_id="create_table",
    python_callable=create_table,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_table,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id="load_csv",
    python_callable=load_csv,
    dag=dag,
)

create_table_task >> truncate_table_task >> load_csv_task
