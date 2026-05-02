# End-to-End E-Commerce Big Data Pipeline

## 1. Project Overview
This project implements a production-grade batch data pipeline for an E-commerce platform. It generates synthetic sales data, stores it in a distributed file system (HDFS), processes and transforms it using distributed computing (Apache Spark), loads it into a Data Warehouse using a Star Schema, and validates data integrity. The entire workflow is orchestrated and monitored by Apache Airflow.

## 2. Architecture Explanation
The architecture follows a modern decoupled batch processing pattern:
1. **Ingestion**: Airflow triggers a Python script to generate synthetic CSV data.
2. **Data Lake Storage**: Raw CSVs are uploaded to HDFS for durable, distributed storage.
3. **Processing**: A PySpark application connects to the Spark Standalone cluster, reads from HDFS, cleans nulls, and structures the data into a Star Schema.
4. **Data Warehouse**: Processed data is loaded into a mocked Snowflake environment (PostgreSQL) acting as the serving layer.
5. **Orchestration**: Airflow manages the DAG dependencies, retries, and scheduling.
6. **Validation**: SQL queries are executed against the DWH to ensure referential integrity and data accuracy.

### ASCII Architecture Diagram
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
        |                                                 | (Staging Area)          |
        |                                                 +-------------------------+
        v                                                            |
+-------------------+                                       +-------------------------+
|                   |                                       |                         |
|  Validation Task  |                                       |  Mock Snowflake (DWH)   |
|  (Data Quality)   |<--------------------------------------|  (Star Schema - Postgres)|
|                   |                                       |                         |
+-------------------+                                       +-------------------------+
```

## 3. Tech Stack
* **Orchestration**: Apache Airflow 2.8.0 (Python 3.10)
* **Data Lake Storage**: HDFS (Hadoop 3.2.1)
* **Data Processing**: Apache Spark 3.5.0 (Standalone Mode)
* **Data Warehouse**: Snowflake (Simulated via PostgreSQL 15)
* **Python Libraries**: PySpark 3.5.0, Pandas 2.1.4, PyArrow 14.0.1, SQLAlchemy 1.4.49, Psycopg2 2.9.9
* **Containers**: Docker & Docker Compose v5

## 4. Data Warehouse (DWH) Schema
The pipeline transforms raw relational data into a **Star Schema** inside the Data Warehouse.
* **Fact Table**: `fact_sales` (Contains `sale_id`, `customer_id`, `product_id`, `sale_date_key`, `sale_hour_key`, `quantity`, `total_amount`).
* **Dimension Tables**:
  * `dim_customers` (Customer attributes)
  * `dim_products` (Product categories and prices)
  * `dim_time` (Date, hour, day of week, month, year)
* **Referential Integrity**: Foreign keys are enforced to ensure no orphan records exist in the fact table.

## 5. Prerequisites
* **Docker Desktop**: Installed and running. Assign at least 8GB RAM to Docker in settings.
* **Ports Available**: Ensure ports `8080`, `8082`, `9870`, `5432`, and `5433` are free on your host machine.
* **Operating System Note**: If running on Windows via WSL2, do not execute this project inside `/mnt/c/` or `/mnt/d/`. Windows NTFS file caching causes severe permission and synchronization issues with Docker bind mounts. Clone or copy the project to the native Linux home directory (e.g., `~/bigdata-project`).

## 6. Step-by-Step Installation and Execution Guide

### Step 1: Extract or Clone the Project
Move the project folder to your designated working directory.

### Step 2: Start the Infrastructure
Open a terminal in the root directory of the project (where `docker-compose.yml` is located) and run:
```bash
docker compose up -d
```
*Wait approximately 30-45 seconds for all containers (Postgres, HDFS, Spark, Airflow) to fully initialize.*

### Step 3: Initialize the Airflow Database
Due to specific environment configurations to ensure pipeline stability, you must initialize the Airflow metadata database manually before the first run:
```bash
docker compose run --rm airflow-scheduler airflow db migrate
```

### Step 4: Create an Airflow Admin User
Create a user to access the Airflow Web UI:
```bash
docker compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```
*(If the webserver is not ready yet, wait 10 seconds and retry the command).*

### Step 5: Access the UI and Run the Pipeline
1. Open your web browser and navigate to: **http://localhost:8082**
2. Log in using the credentials:
   * **Username**: `admin`
   * **Password**: `admin`
3. On the left sidebar, click on **DAGs**.
4. Locate the DAG named `ecommerce_bigdata_pipeline`.
5. Toggle the switch on the left to turn the DAG **On**.
6. Click the **Play button** (Trigger DAG) on the far right.
7. Click the **Graph** tab to visually monitor the pipeline progress. All 5 tasks should turn green upon successful completion.

## 7. Accessing the Infrastructure UIs
| Service        | URL                      | Credentials                        |
|----------------|--------------------------|------------------------------------|
| Airflow UI     | http://localhost:8082    | admin / admin                      |
| Spark Master   | http://localhost:8080    | None                               |
| HDFS NameNode  | http://localhost:9870    | None                               |
| Mock Snowflake | localhost:5433           | snowflake_user / snowflake_pass    |
|                |                          | (Connect via DBeaver or pgAdmin)   |

## 8. Validation Outputs
Once the `validate_data_warehouse` task completes successfully, you can view its logs in the Airflow UI. The logs will display:
1. **Row Counts**: Verification that all dimension and fact tables were populated.
2. **Total Revenue**: The sum of all valid sales.
3. **Sales by Category**: Revenue aggregated by product category using a Star Schema JOIN.
4. **Data Quality Check**: A confirmation message that no orphan records violated the Star Schema constraints.

## 9. Technical Implementation Notes
The following architectural decisions were deliberately made to resolve specific conflicts between distributed systems running in a local Docker environment:

* **SQLAlchemy Version**: This project explicitly uses `SQLAlchemy 1.4.49` instead of 2.0+. This bypasses a well-documented conflict between Apache Airflow 2.8.0 and SQLAlchemy 2.0+ regarding the `executemany_mode` parameter in Psycopg2, which causes immediate fatal crashes on startup.
* **Data Staging Strategy**: Spark writes its processed output to local Parquet files via Pandas (`.toPandas().to_parquet()`) rather than writing directly to HDFS. This bypasses HDFS `Permission denied` errors (`user=airflow, access=WRITE, inode="/":root:supergroup`) that occur when Spark attempts to write to the local Docker filesystem mapped as an HDFS path.
* **Database Initialization**: The `load_to_snowflake` script explicitly drops tables in reverse-dependency order using `DROP TABLE ... CASCADE` before loading new data. This prevents Foreign Key constraint violations during pipeline reruns that occur when using standard Pandas `if_exists="replace"` logic.
* **Log Authentication**: A hardcoded `AIRFLOW__WEBSERVER__SECRET_KEY` is injected into both the webserver and scheduler environments to prevent `403 FORBIDDEN` errors when the Airflow UI attempts to fetch task logs from the scheduler.

## 10. Troubleshooting
* **Airflow DAG not showing up**: Airflow takes about 30-45 seconds to parse Python files on startup. Wait a minute and refresh the browser.
* **Spark task fails with "Connection Refused"**: The Spark Master might still be initializing. Wait 20 seconds and trigger the DAG task again.
* **HDFS Upload fails**: HDFS enters "Safemode" on startup. The Python script includes a built-in retry loop (up to 60 seconds) to wait for HDFS to exit safemode automatically.
* **Port 8080 already in use**: You likely have another service running on your machine. Stop it, or change the port mapping in `docker-compose.yml` (e.g., change `"8080:8080"` to `"8081:8080"`).

## 11. Shutting Down the Project
To stop all containers and free up system resources, run:
```bash
docker compose down
```
*Note: To completely wipe the Airflow database and start fresh, run `docker compose down -v`. You will need to repeat Steps 3 and 4 if you do this.*
