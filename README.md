# Airflow ETL Pipeline Project

## Project Overview

This project demonstrates a **complete, production-style data engineering workflow** built using **Apache Airflow** and **Docker**. It showcases how to design, orchestrate, monitor, and test ETL pipelines using best practices followed in real-world data engineering teams.

The project includes **five distinct Airflow DAGs**, each highlighting a different orchestration pattern such as ingestion, transformation, export, conditional branching, and error handling with notifications.

---

## Architecture

**Tech Stack Used:**

* Apache Airflow 2.8.0
* Docker & Docker Compose
* PostgreSQL (Airflow metadata + data warehouse)
* Pandas
* PyArrow (Parquet with Snappy compression)
* Pytest (unit testing)

**High-level flow:**

```
CSV File → PostgreSQL → Transformed Table → Parquet File
                  ↘ Conditional & Notification Workflows
```

---

##  Project Structure

```
airflow_etl_pipeline/
├── dags/
│   ├── dag1_csv_to_postgres.py
│   ├── dag2_data_transformation.py
│   ├── dag3_postgres_to_parquet.py
│   ├── dag4_conditional_workflow.py
│   └── dag5_notification_workflow.py
├── tests/
│   ├── test_dag1.py
│   └── test_utils.py
├── data/
│   └── input.csv
├── output/
│   └── (generated parquet files)
├── docker-compose.yml
├── requirements.txt
└── README.md
```

---

## Setup Instructions

### 1️ Prerequisites

* Docker Desktop installed and running
* Git Bash (Windows) or any terminal

### 2️ Clone the Repository

```bash
git clone https://github.com/Medapatisanjana12/airflow_etl_pipeline.git
cd airflow_etl_pipeline
```

### 3️ Start Airflow Environment

```bash
docker compose up -d
```

* Airflow UI will be available at: **[http://localhost:8081](http://localhost:8081)**
* Default login (if required):

  * Username: `airflow`
  * Password: `airflow`

---

## DAG Descriptions

### DAG 1: CSV to PostgreSQL Ingestion

* **DAG ID:** `csv_to_postgres_ingestion`
* **Schedule:** Daily
* **Purpose:** Load employee data from CSV into PostgreSQL
* **Features:**

  * Table creation if not exists
  * Truncation for idempotency
  * Row count returned

---

### DAG 2: Data Transformation Pipeline

* **DAG ID:** `data_transformation_pipeline`
* **Schedule:** Daily
* **Purpose:** Transform raw employee data
* **Transformations:**

  * `full_info` = name + city
  * `age_group` categorization
  * `salary_category` categorization
  * `year_joined` extraction

---

### DAG 3: PostgreSQL to Parquet Export

* **DAG ID:** `postgres_to_parquet_export`
* **Schedule:** Weekly
* **Purpose:** Export transformed data to Parquet
* **Features:**

  * Snappy compression
  * Schema validation
  * Metadata via XCom

---

### DAG 4: Conditional Workflow

* **DAG ID:** `conditional_workflow_pipeline`
* **Schedule:** Daily
* **Purpose:** Demonstrate branching logic
* **Logic:**

  * Mon–Wed → Weekday tasks
  * Thu–Fri → End-of-week tasks
  * Sat–Sun → Weekend tasks
* **End task always runs**

---

### DAG 5: Notification & Error Handling Workflow

* **DAG ID:** `notification_workflow`
* **Schedule:** Daily
* **Purpose:** Demonstrate callbacks and trigger rules
* **Features:**

  * Risky task that may fail
  * Success & failure callbacks
  * Cleanup task always executes

---

## Running Unit Tests

Tests validate DAG structure without requiring Airflow to be running.

```bash
pytest tests/
```

**Test Coverage Includes:**

* DAG loading without errors
* Correct DAG IDs
* Task counts
* Dependency validation
* No import errors

---

## Dependencies

All dependencies are listed in `requirements.txt`:

```
apache-airflow==2.8.0
apache-airflow-providers-postgres==5.10.0
pandas==2.1.4
pyarrow==14.0.2
psycopg2-binary==2.9.9
pytest==7.4.3
```

---


---

## Expected Outcomes Achieved

- Fully functional local Airflow environment-
- Five operational DAGs
- PostgreSQL populated from CSV
- Parquet file generation
- Unit tests validating DAG structure
- Well-documented project

---

## Author

**Sanjana Medapati**

---


