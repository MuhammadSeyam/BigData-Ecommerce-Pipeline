
# 🛒 End-to-End E-Commerce Big Data Pipeline

## 1. Project Overview
This project implements a production-grade Big Data pipeline designed for an E-commerce platform. It ingests raw synthetic sales data, stores it in a distributed file system (HDFS), processes and transforms it using distributed computing (Apache Spark), loads it into a Data Warehouse using a Star Schema, and finally validates the data integrity—all orchestrated by Apache Airflow.

## 2. Architecture Explanation
The architecture follows a modern **Lambda-style batch processing** pattern:
1. **Generation/Ingestion**: Airflow triggers a synthetic data generator.
2. **Storage (Data Lake)**: Raw CSVs are pushed to HDFS for durable, distributed storage.
3. **Processing (Compute)**: A PySpark application connects to the Spark Standalone cluster, reads from HDFS, cleans nulls, and structures the data into a Star Schema.
4. **Serving (Data Warehouse)**: Processed Parquet files are loaded into a Mock Snowflake database (PostgreSQL).
5. **Orchestration**: Airflow manages the DAG dependencies, retries, and scheduling.

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
        |                                                            |
        v                                                            v
+-------------------+                                       +-------------------------+
|                   |                                       |                         |
|  Validation Task  |                                       |  Mock Snowflake (DWH)   |
|  (Data Quality)   |<--------------------------------------|  (Star Schema - Postgres)|
|                   |                                       |                         |
+-------------------+                                       +-------------------------+
```

## 3. Tech Stack
* **Orchestration**: Apache Airflow 2.8.0
* **Data Lake Storage**: HDFS (Hadoop 3.2.1)
* **Data Processing**: Apache Spark 3.5.0 (Standalone Mode)
* **Data Warehouse**: Snowflake (Simulated via PostgreSQL 15)
* **Containers**: Docker & Docker Compose

## 4. Step-by-Step Installation Guide (Beginner Friendly)

### Prerequisites
- Install [Docker Desktop](https://www.docker.com/products/docker-desktop/) (Includes Docker Compose).
- Ensure your system has at least **8GB RAM** allocated to Docker (Docker Desktop -> Settings -> Resources).
- Ensure ports **8080, 8082, 9870, 5432, 5433** are free.

### Execution Steps
1. **Clone/Download** this project into a folder named `bigdata-project`.
2. Open a terminal in the `bigdata-project` directory.
3. Create required local directories:
   ```bash
   mkdir -p dags scripts data/raw data/processed
   ```
4. **Start the Infrastructure** (This will download images and start all containers):
   ```bash
   docker-compose up -d --build
   ```
   *Note: Wait about 60 seconds for all services (especially HDFS and Airflow) to fully initialize.*
5. **Access Airflow UI**: Go to `http://localhost:8082`.
   - Login: `airflow` / `airflow`
6. **Run the Pipeline**:
   - In the Airflow UI, find the DAG named `ecommerce_bigdata_pipeline`.
   - Toggle the switch on the left to **"On"**.
   - Click the **"Trigger DAG"** button (Play icon).
   - Watch the task instances turn green as they execute.

## 5. How to Access UIs
| Service        | URL                      | Credentials        |
|----------------|--------------------------|--------------------|
| Airflow UI     | http://localhost:8082    | airflow / airflow  |
| Spark Master   | http://localhost:8080    | None               |
| HDFS NameNode  | http://localhost:9870    | None               |
| Mock Snowflake | localhost:5433           | snowflake_user / snowflake_pass (Use DBeaver/pgAdmin) |

## 6. Example Outputs
If you click on the `validate_data_warehouse` task in Airflow and view its logs, you will see:
```text
--- VALIDATION QUERY 1: Row Counts ---
       table_name  count
0  dim_customers    100
1   dim_products     50
2      dim_time    874
3    fact_sales    995

--- VALIDATION QUERY 2: Total Revenue ---
   total_revenue
0  1548232.45

--- VALIDATION QUERY 3: Sales by Category (Star Schema Join) ---
     category  num_sales    revenue
0  Electronics       215  452150.80
1      Sports       210  381250.50
2      Home           195  352100.20
3    Clothing       190  210500.95
4      Books       185  152230.00

--- DATA QUALITY CHECK ---
✅ SUCCESS: No orphan records in fact_sales. Referential integrity maintained.
```

## 7. Troubleshooting
* **Airflow DAG not showing up**: Airflow takes ~45 seconds to parse DAGs on startup. Wait a minute and refresh the UI.
* **Spark task fails with "Connection Refused"**: The Spark Master might still be starting. Rerun the DAG task.
* **HDFS Upload fails**: HDFS enters "Safemode" on startup. The Python script has a built-in 30-second retry loop, but if your machine is slow, increase `time.sleep(2)` in `hdfs_client.py`.
* **Port 8080 already in use**: You likely have another service running. Stop it, or change the Airflow port mapping in `docker-compose.yml` to `8083:8080` and access it via `8083`.
```

---

### ⚙️ Important note on `data_generator.py`
To ensure the Airflow DAG can import it as a module (as done in step 10), wrap the actual execution code in `data_generator.py` inside a function like this before running:

```python
# Change the bottom of data_generator.py to:
def generate_data_main():
    OUTPUT_DIR = "/opt/airflow/data/raw"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    random.seed(42)
    # ... (all the csv generation code) ...
    print("Data generation completed successfully.")

if __name__ == "__main__":
    generate_data_main()