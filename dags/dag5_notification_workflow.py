from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime
import json

# -----------------------------
# DAG CONFIG
# -----------------------------
default_args = {
    "owner": "airflow",
    "retries": 0,
}

dag = DAG(
    dag_id="notification_workflow",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["notifications", "error-handling"],
)

# -----------------------------
# CALLBACKS
# -----------------------------
def send_success_notification(context):
    payload = {
        "notification_type": "success",
        "status": "sent",
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "execution_date": str(context["execution_date"]),
        "timestamp": datetime.utcnow().isoformat(),
        "message": "Risky operation succeeded",
    }
    print(json.dumps(payload, indent=2))
    return payload


def send_failure_notification(context):
    payload = {
        "notification_type": "failure",
        "status": "sent",
        "dag_id": context["dag"].dag_id,
        "task_id": context["task_instance"].task_id,
        "execution_date": str(context["execution_date"]),
        "timestamp": datetime.utcnow().isoformat(),
        "error": str(context.get("exception")),
        "message": "Risky operation failed",
    }
    print(json.dumps(payload, indent=2))
    return payload

# -----------------------------
# RISKY OPERATION
# -----------------------------
def risky_operation(**context):
    execution_date = context["execution_date"]
    day = execution_date.day

    # Fail if day is divisible by 5
    if day % 5 == 0:
        raise Exception("Simulated failure: day divisible by 5")

    return {
        "status": "success",
        "execution_date": str(execution_date),
        "success": True,
    }

# -----------------------------
# CLEANUP TASK
# -----------------------------
def cleanup_task():
    payload = {
        "cleanup_status": "completed",
        "timestamp": datetime.utcnow().isoformat(),
    }
    print(json.dumps(payload, indent=2))
    return payload

# -----------------------------
# TASK DEFINITIONS
# -----------------------------
start_task = EmptyOperator(
    task_id="start",
    dag=dag,
)

risky_task = PythonOperator(
    task_id="risky_operation",
    python_callable=risky_operation,
    provide_context=True,
    on_success_callback=send_success_notification,
    on_failure_callback=send_failure_notification,
    dag=dag,
)

success_notification_task = EmptyOperator(
    task_id="success_notification",
    trigger_rule="all_success",
    dag=dag,
)

failure_notification_task = EmptyOperator(
    task_id="failure_notification",
    trigger_rule="all_failed",
    dag=dag,
)

always_cleanup_task = PythonOperator(
    task_id="always_execute_cleanup",
    python_callable=cleanup_task,
    trigger_rule="all_done",
    dag=dag,
)

# -----------------------------
# DEPENDENCIES
# -----------------------------
start_task >> risky_task
risky_task >> [success_notification_task, failure_notification_task]
[success_notification_task, failure_notification_task] >> always_cleanup_task
