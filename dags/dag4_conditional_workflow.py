from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "retries": 1,
}

dag = DAG(
    dag_id="conditional_workflow_pipeline",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["conditional", "branching"],
)

# -----------------------------
# BRANCH DECISION LOGIC
# -----------------------------
def determine_branch(**context):
    execution_date = context["execution_date"]
    day_of_week = execution_date.weekday()  # Monday=0, Sunday=6

    if day_of_week <= 2:
        return "weekday_processing"
    elif day_of_week <= 4:
        return "end_of_week_processing"
    else:
        return "weekend_processing"

# -----------------------------
# WEEKDAY PROCESS
# -----------------------------
def weekday_process():
    return {
        "day_type": "weekday",
        "task_type": "weekday_processing",
        "record_count": 120
    }

# -----------------------------
# END-OF-WEEK PROCESS
# -----------------------------
def end_of_week_process():
    return {
        "day_type": "end_of_week",
        "task_type": "end_of_week_processing",
        "weekly_summary": "Weekly aggregation completed"
    }

# -----------------------------
# WEEKEND PROCESS
# -----------------------------
def weekend_process():
    return {
        "day_type": "weekend",
        "task_type": "weekend_processing",
        "cleanup_status": "Cleanup completed"
    }

# -----------------------------
# TASK DEFINITIONS
# -----------------------------
start_task = EmptyOperator(
    task_id="start",
    dag=dag,
)

branch_task = BranchPythonOperator(
    task_id="branch_by_day",
    python_callable=determine_branch,
    dag=dag,
)

# ----- Weekday branch -----
weekday_task = PythonOperator(
    task_id="weekday_processing",
    python_callable=weekday_process,
    dag=dag,
)

weekday_summary = EmptyOperator(
    task_id="weekday_summary",
    dag=dag,
)

# ----- End-of-week branch -----
end_of_week_task = PythonOperator(
    task_id="end_of_week_processing",
    python_callable=end_of_week_process,
    dag=dag,
)

end_of_week_report = EmptyOperator(
    task_id="end_of_week_report",
    dag=dag,
)

# ----- Weekend branch -----
weekend_task = PythonOperator(
    task_id="weekend_processing",
    python_callable=weekend_process,
    dag=dag,
)

weekend_cleanup = EmptyOperator(
    task_id="weekend_cleanup",
    dag=dag,
)

# ----- End task (always runs) -----
end_task = EmptyOperator(
    task_id="end",
    trigger_rule="none_failed_min_one_success",
    dag=dag,
)

# -----------------------------
# TASK DEPENDENCIES
# -----------------------------
start_task >> branch_task

# Weekday path
branch_task >> weekday_task >> weekday_summary >> end_task

# End-of-week path
branch_task >> end_of_week_task >> end_of_week_report >> end_task

# Weekend path
branch_task >> weekend_task >> weekend_cleanup >> end_task
