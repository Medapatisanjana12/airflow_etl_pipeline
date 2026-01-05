from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import csv

# -----------------------------
# DAG CONFIG
# -----------------------------
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

# -----------------------------
# TASK 1: CREATE TABLE
# -----------------------------
def create_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS raw_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary NUMERIC(10,2),
        join_date DATE
    );
    """

    hook.run(create_table_sql)

# -----------------------------
# TASK 2: TRUNCATE TABLE
# -----------------------------
def truncate_employee_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    hook.run("TRUNCATE TABLE raw_employee_data;")

# -----------------------------
# TASK 3: LOAD CSV DATA
# -----------------------------
def load_csv_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    conn = hook.get_conn()
    cursor = conn.cursor()

    file_path = "/opt/airflow/data/input.csv"
    rows_inserted = 0

    with open(file_path, "r") as file:
        reader = csv.DictReader(file)
        for row in reader:
            cursor.execute(
                """
                INSERT INTO raw_employee_data (id, name, age, city, salary, join_date)
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
            rows_inserted += 1

    conn.commit()
    cursor.close()
    conn.close()

    return rows_inserted

# -----------------------------
# AIRFLOW TASKS
# -----------------------------
create_table_task = PythonOperator(
    task_id="create_table_if_not_exists",
    python_callable=create_employee_table,
    dag=dag,
)

truncate_table_task = PythonOperator(
    task_id="truncate_table",
    python_callable=truncate_employee_table,
    dag=dag,
)

load_csv_task = PythonOperator(
    task_id="load_csv_to_postgres",
    python_callable=load_csv_data,
    dag=dag,
)

# -----------------------------
# TASK DEPENDENCIES
# -----------------------------
create_table_task >> truncate_table_task >> load_csv_task
