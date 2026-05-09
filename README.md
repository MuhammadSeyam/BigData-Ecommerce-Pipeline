# End-to-End E-Commerce Big Data Pipeline

## 1. Project Overview

This project implements an end-to-end batch-oriented Big Data pipeline for an E-Commerce platform. The system generates synthetic transactional data, stores it inside a distributed storage layer (HDFS), processes and transforms the data using Apache Spark, loads it into a Data Warehouse designed with a Star Schema model, and validates data integrity through automated quality checks.

The entire workflow is orchestrated using Apache Airflow running inside a fully containerized Docker environment.

The project demonstrates the integration of modern data engineering components including:

* Distributed storage
* Distributed processing
* Workflow orchestration
* ETL pipelines
* Data warehouse modeling
* Data quality validation
* Containerized infrastructure

---

# 2. Pipeline Workflow

The pipeline follows a sequential batch-processing architecture:

1. Generate synthetic E-Commerce datasets.
2. Upload raw CSV files into HDFS.
3. Process and clean data using PySpark.
4. Build analytical Star Schema tables.
5. Save transformed datasets as Parquet files.
6. Load processed data into the Data Warehouse.
7. Execute validation and integrity checks.

---

# 3. Architecture Explanation

The architecture follows a decoupled Big Data batch-processing design.

![High-level architecture diagram showing the flow from Airflow orchestration through HDFS storage, Spark processing, and into the PostgreSQL Data Warehouse](docs/images/architecture.png)

## Pipeline Stages

### 1. Data Ingestion

Airflow triggers a Python-based ingestion script that generates synthetic customer, product, and sales datasets.

### 2. Distributed Storage Layer

The generated CSV files are uploaded into HDFS, which acts as the raw distributed data lake.

### 3. Distributed Processing Layer

A PySpark application connects to the Spark Standalone cluster, reads the raw data from HDFS, cleans invalid rows, and transforms the data into analytical structures.

### 4. Data Warehouse Layer

The transformed datasets are loaded into PostgreSQL, which simulates a Snowflake-style analytical warehouse.

### 5. Orchestration Layer

Apache Airflow manages task dependencies, execution order, retries, scheduling, and monitoring.

### 6. Validation Layer

Validation queries ensure:

* Correct row counts
* Successful joins
* Revenue consistency
* Referential integrity
* Absence of orphan records

---

# 4. ASCII Architecture Diagram

```text
+-------------------+      +-----------------------+      +-------------------------+
|                   |      |                       |      |                         |
|  Airflow Scheduler| ---> |  HDFS (NameNode/DN)   | ---> |  Spark Master & Worker  |
|  (Orchestrator)   |      |  (Data Lake - Raw)    |      |  (Distributed Compute)  |
|                   |      |                       |      |                         |
+-------------------+      +-----------------------+      +-------------------------+
        |                                                            |
        |                                                            v
        |                                                 +-------------------------+
        |                                                 | Processed Parquet Files |
        |                                                 | (Local Staging Layer)   |
        |                                                 +-------------------------+
        v                                                            |
+-------------------+                                       +-------------------------+
|                   |                                       |                         |
|  Validation Task  |                                       |  Mock Snowflake (DWH)  |
|  (Data Quality)   |<--------------------------------------|  PostgreSQL Warehouse   |
|                   |                                       |                         |
+-------------------+                                       +-------------------------+
```

---

# 5. Technology Stack

| Component              | Technology                     |
| ---------------------- | ------------------------------ |
| Workflow Orchestration | Apache Airflow 2.8.0           |
| Distributed Storage    | Hadoop HDFS 3.2.1              |
| Distributed Processing | Apache Spark 3.5.0             |
| Data Warehouse         | PostgreSQL 15 (Mock Snowflake) |
| Programming Language   | Python 3.10                    |
| Containerization       | Docker & Docker Compose        |
| Data Format            | CSV / Parquet                  |
| ORM & DB Access        | SQLAlchemy 1.4.49              |

---

# 6. Data Warehouse Schema

The pipeline transforms raw operational datasets into a Star Schema optimized for analytical workloads.

## Fact Table

### `fact_sales`

Contains transactional sales records.

Columns:

* sale_id
* customer_id
* product_id
* sale_date_key
* sale_hour_key
* quantity
* total_amount

## Dimension Tables

### `dim_customers`

Customer information and attributes.

### `dim_products`

Product details, categories, and pricing.

### `dim_time`

Time-based analytical attributes.

### Data Warehouse Tables Overview

![Star Schema tables visible in the PostgreSQL Data Warehouse, including fact_sales and all associated dimension tables](docs/images/dwh-tables.png)

### Fact Sales Preview

![Sample rows from the fact_sales table showing sale_id, customer_id, product_id, date/time keys, quantity, and total_amount columns](docs/images/fact-sales-preview.png)

---

# 7. Infrastructure Components

## Containerized Environment

All services run inside Docker containers orchestrated with Docker Compose. The screenshot below shows all running containers and their health status.

![Docker Desktop view listing all active containers for the pipeline, including Airflow, Hadoop, Spark, and PostgreSQL services](docs/images/docker-containers.png)

---

## Airflow Components

### airflow-webserver

Provides the Airflow user interface.

### airflow-scheduler

Responsible for DAG scheduling and task execution.

### airflow-postgres

Stores Airflow metadata and execution history.

---

## Hadoop Components

### namenode

Responsible for HDFS metadata management.

### datanode

Stores distributed file blocks.

---

## Spark Components

### spark-master

Coordinates distributed Spark jobs.

### spark-worker

Executes Spark tasks.

---

## Data Warehouse

### snowflake-mock

A PostgreSQL container acting as a simulated Snowflake environment.

---

# 8. Prerequisites

Before running the project, ensure the following requirements are satisfied.

## Required Software

* Docker Desktop
* Docker Compose

## Recommended Docker Resources

Assign at least:

* 8 GB RAM
* 4 CPU cores

---

## Required Open Ports

Ensure the following ports are available:

| Port | Service            |
| ---- | ------------------ |
| 8080 | Spark Master UI    |
| 8082 | Airflow UI         |
| 9870 | HDFS NameNode UI   |
| 5432 | Airflow PostgreSQL |
| 5433 | Mock Snowflake     |

---

## WSL2 Note (Windows Users)

If running under WSL2, avoid placing the project inside:

```text
/mnt/c/
/mnt/d/
```

Windows NTFS bind mounts can create synchronization and permission issues with Docker.

Recommended location:

```text
~/bigdata-project
```

---

# 9. Installation and Execution Guide

## Step 1 — Clone or Extract the Project

Place the project inside your working directory.

---

## Step 2 — Start the Infrastructure

Run the following command from the root project directory:

```bash
docker compose up -d
```

Wait approximately 30–60 seconds for all services to initialize.

---

## Step 3 — Initialize the Airflow Database

Run:

```bash
docker compose run --rm airflow-scheduler airflow db migrate
```

This initializes Airflow's metadata database.

---

## Step 4 — Restart Airflow Services

Run:

```bash
docker compose restart airflow-webserver airflow-scheduler
```

This ensures the scheduler and webserver reload correctly after database initialization.

---

## Step 5 — Create an Airflow Admin User

Run:

```bash
docker compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

If the command fails initially, wait 10–15 seconds and retry.

---

## Step 6 — Access Airflow

Open:

```text
http://localhost:8082
```

Credentials:

| Username | Password |
| -------- | -------- |
| admin    | admin    |

---

## Step 7 — Trigger the Pipeline

1. Open the DAGs page.
2. Locate `ecommerce_bigdata_pipeline`.
3. Enable the DAG.
4. Click the Trigger button.
5. Open the Graph view to monitor task execution.

All tasks should complete successfully.

---

# 10. Airflow DAG Structure

The DAG contains five sequential tasks:

```text
ingest_generate_data
        ↓
upload_to_hdfs
        ↓
spark_processing
        ↓
load_to_snowflake
        ↓
validate_data_warehouse
```

### DAGs List View

![Airflow UI showing the ecommerce_bigdata_pipeline DAG listed on the DAGs page, with its schedule and last run status](docs/images/airflow-dags.png)

### DAG Graph View

![Airflow Graph view of the ecommerce_bigdata_pipeline showing all five sequential tasks and their execution state after a successful run](docs/images/airflow-graph.png)

---

# 11. Accessing Infrastructure Interfaces

| Service          | URL                                            | Credentials                     |
| ---------------- | ---------------------------------------------- | ------------------------------- |
| Airflow UI       | [http://localhost:8082](http://localhost:8082) | admin / admin                   |
| Spark Master UI  | [http://localhost:8080](http://localhost:8080) | None                            |
| HDFS NameNode UI | [http://localhost:9870](http://localhost:9870) | None                            |
| Mock Snowflake   | localhost:5433                                 | snowflake_user / snowflake_pass |

You can connect to PostgreSQL using:

* DBeaver
* pgAdmin
* DataGrip

---

## Spark Cluster UI

![Spark Master UI displaying the cluster status, number of active workers, available memory, and submitted application history](docs/images/spark-cluster.png)

---

## HDFS NameNode UI

![HDFS NameNode web interface showing cluster summary, live DataNodes, and overall filesystem capacity](docs/images/hdfs-home.png)

### Raw Files in HDFS

![HDFS file browser displaying the uploaded raw CSV files (customers, products, sales) stored inside the distributed data lake](docs/images/hdfs-raw-files.png)

---

# 12. Validation Outputs

The `validate_data_warehouse` task executes several validation queries.

## Validation Checks

### 1. Row Count Validation

Ensures all tables were populated successfully.

### 2. Revenue Validation

Calculates total revenue using:

```sql
SUM(total_amount)
```

### 3. Star Schema Join Validation

Joins fact and dimension tables to calculate category revenue.

### 4. Referential Integrity Validation

Detects orphan records using LEFT JOIN checks.

### Validation Task Logs

![Airflow task logs from the validate_data_warehouse step, showing the results of all validation checks including row counts, revenue totals, and referential integrity results](docs/images/validation-logs.png)

### Category Revenue Query

![SQL query output displaying total revenue grouped by product category, produced by joining fact_sales with dim_products inside the Data Warehouse](docs/images/category-revenue-query.png)

---

# 13. Technical Design Decisions

The project includes several intentional engineering decisions to simplify local distributed execution.

---

## SQLAlchemy Version Lock

The project uses:

```text
SQLAlchemy 1.4.49
```

Instead of SQLAlchemy 2.x due to compatibility issues with Airflow 2.8.

---

## Local Parquet Staging

Spark converts distributed DataFrames into local Parquet files using:

```python
.toPandas().to_parquet()
```

This avoids HDFS permission conflicts inside Docker.

---

## Full Refresh Strategy

The pipeline drops and reloads warehouse tables during every run.

This simplifies reruns and avoids foreign key conflicts.

---

## Mock Snowflake Environment

PostgreSQL is used as a lightweight local simulation of a Snowflake-style warehouse.

---

# 14. Known Limitations

This project is educational and simplified for local execution.

## Current Limitations

* Uses Full Refresh instead of Incremental Loading
* Uses `.toPandas()` which is not ideal for real Big Data scale
* Uses a single Spark worker
* Does not include Kafka or streaming pipelines
* Does not implement CDC pipelines

---

# 15. Troubleshooting

## DAG Does Not Appear

Airflow may require 30–60 seconds to parse DAG files.

Refresh the browser after waiting.

---

## Spark Connection Refused

Spark Master may still be initializing.

Wait a few seconds and rerun the task.

---

## HDFS Upload Failure

HDFS may temporarily enter SafeMode during startup.

The ingestion script includes retry logic.

---

## Port Already in Use

Modify the port mapping inside:

```text
docker-compose.yml
```

Example:

```yaml
"8081:8080"
```

---

# 16. Shutting Down the Project

To stop all containers:

```bash
docker compose down
```

To completely remove volumes and reset the environment:

```bash
docker compose down -v
```

If volumes are removed, you must repeat:

* Airflow database migration
* Admin user creation

---

# 17. Educational Objectives

This project demonstrates practical understanding of:

* Distributed Data Engineering
* Batch ETL Pipelines
* Workflow Orchestration
* HDFS Storage
* Spark Distributed Processing
* Star Schema Modeling
* Data Validation
* Dockerized Infrastructure
* Data Warehouse Loading

---

# 18. Future Improvements

Potential future enhancements include:

* Kafka integration
* Streaming pipelines
* Incremental loading
* CDC support
* Multi-worker Spark cluster
* Direct HDFS Parquet writes
* Real Snowflake integration
* CI/CD automation
* Monitoring and alerting

---

# 19. Project Status

Current Status:

```text
Completed and Operational
```

The pipeline successfully executes end-to-end inside a local Dockerized Big Data environment.