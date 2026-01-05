from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
import pandas as pd

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
}

dag = DAG(
    dag_id="data_transformation_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "transform", "postgres"],
)

# -----------------------------
# TASK 1: CREATE TRANSFORMED TABLE
# -----------------------------
def create_transformed_table():
    hook = PostgresHook(postgres_conn_id="postgres_default")

    create_table_sql = """
    CREATE TABLE IF NOT EXISTS transformed_employee_data (
        id INTEGER PRIMARY KEY,
        name VARCHAR(255),
        age INTEGER,
        city VARCHAR(100),
        salary NUMERIC(10,2),
        join_date DATE,
        full_info VARCHAR(500),
        age_group VARCHAR(20),
        salary_category VARCHAR(20),
        year_joined INTEGER
    );
    """

    hook.run(create_table_sql)

# -----------------------------
# TASK 2: TRANSFORM & LOAD DATA
# -----------------------------
def transform_data():
    hook = PostgresHook(postgres_conn_id="postgres_default")
    engine = hook.get_sqlalchemy_engine()

    # Read source data
    df = pd.read_sql("SELECT * FROM raw_employee_data", engine)
    rows_processed = len(df)

    if rows_processed == 0:
        return {
            "rows_processed": 0,
            "rows_inserted": 0
        }

    # -----------------------------
    # TRANSFORMATIONS
    # -----------------------------
    df["full_info"] = df["name"] + " - " + df["city"]

    df["age_group"] = df["age"].apply(
        lambda x: "Young" if x < 30 else "Mid" if 30 <= x <= 49 else "Senior"
    )

    df["salary_category"] = df["salary"].apply(
        lambda x: "Low" if x < 50000 else "Medium" if 50000 <= x <= 79999 else "High"
    )

    df["year_joined"] = pd.to_datetime(df["join_date"]).dt.year

    # -----------------------------
    # LOAD DATA (IDEMPOTENT)
    # -----------------------------
    with engine.begin() as conn:
        conn.execute("TRUNCATE TABLE transformed_employee_data")
        df.to_sql(
            "transformed_employee_data",
            con=conn,
            if_exists="append",
            index=False
        )

    rows_inserted = len(df)

    return {
        "rows_processed": rows_processed,
        "rows_inserted": rows_inserted
    }

# -----------------------------
# AIRFLOW TASKS
# -----------------------------
create_transformed_table_task = PythonOperator(
    task_id="create_transformed_table",
    python_callable=create_transformed_table,
    dag=dag,
)

transform_and_load_task = PythonOperator(
    task_id="transform_and_load",
    python_callable=transform_data,
    dag=dag,
)

# -----------------------------
# TASK DEPENDENCIES
# -----------------------------
create_transformed_table_task >> transform_and_load_task
