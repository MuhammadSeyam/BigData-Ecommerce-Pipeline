# End-to-End E-Commerce Big Data Pipeline

This project implements a batch-oriented Big Data pipeline for an E-Commerce platform. It generates synthetic transactional data, stores it in HDFS, processes and transforms it with Apache Spark, loads it into a Star Schema Data Warehouse, and validates data integrity — all orchestrated by Apache Airflow inside a fully containerized Docker environment.

---

## Architecture

![High-level architecture diagram showing the flow from Airflow orchestration through HDFS storage, Spark processing, and into the PostgreSQL Data Warehouse](docs/images/architecture.png)

The architecture follows a decoupled batch-processing design across five layers:

| Layer | Responsibility |
| ----- | -------------- |
| **Ingestion** | Airflow triggers a Python script that generates synthetic customer, product, and sales datasets |
| **Storage** | Raw CSV files are uploaded to HDFS, acting as the distributed data lake |
| **Processing** | PySpark reads from HDFS, cleans invalid rows, and transforms data into analytical structures |
| **Warehouse** | Transformed data is loaded into PostgreSQL, simulating a Snowflake-style analytical warehouse |
| **Validation** | Automated queries check row counts, revenue consistency, joins, and referential integrity |

```text
+-------------------+      +-----------------------+      +-------------------------+
|  Airflow          | ---> |  HDFS (NameNode/DN)   | ---> |  Spark Master & Worker  |
|  (Orchestrator)   |      |  (Data Lake - Raw)    |      |  (Distributed Compute)  |
+-------------------+      +-----------------------+      +-------------------------+
        |                                                            |
        v                                                            v
+-------------------+                                  +-------------------------+
|  Validation Task  |<---------------------------------|  PostgreSQL (DWH)       |
|  (Data Quality)   |                                  |  (Mock Snowflake)       |
+-------------------+                                  +-------------------------+
```

---

## Technology Stack

| Component | Technology |
| --------- | ---------- |
| Workflow Orchestration | Apache Airflow 2.8.0 |
| Distributed Storage | Hadoop HDFS 3.2.1 |
| Distributed Processing | Apache Spark 3.5.0 |
| Data Warehouse | PostgreSQL 15 (Mock Snowflake) |
| Programming Language | Python 3.10 |
| Containerization | Docker & Docker Compose |
| Data Formats | CSV / Parquet |
| ORM & DB Access | SQLAlchemy 1.4.49 |

---

## Infrastructure & Containers

All services run as Docker containers managed by Docker Compose.

![Docker Desktop showing all active pipeline containers — Airflow, Hadoop, Spark, and PostgreSQL — along with their health status](docs/images/docker-containers.png)

### Prerequisites

- Docker Desktop with at least **8 GB RAM** and **4 CPU cores** allocated
- The following ports must be free:

| Port | Service |
| ---- | ------- |
| 8080 | Spark Master UI |
| 8082 | Airflow UI |
| 9870 | HDFS NameNode UI |
| 5432 | Airflow PostgreSQL |
| 5433 | Mock Snowflake |

> **WSL2 (Windows):** Place the project under `~/bigdata-project` rather than `/mnt/c/` or `/mnt/d/` to avoid NTFS bind-mount permission issues.

---

## Installation & Setup

**Step 1 — Start the infrastructure:**
```bash
docker compose up -d
```
Wait 30–60 seconds for all services to initialize.

**Step 2 — Initialize the Airflow metadata database:**
```bash
docker compose run --rm airflow-scheduler airflow db migrate
```

**Step 3 — Restart Airflow services:**
```bash
docker compose restart airflow-webserver airflow-scheduler
```

**Step 4 — Create an admin user:**
```bash
docker compose exec airflow-webserver airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```
If this fails, wait 10–15 seconds and retry.

**Step 5 — Trigger the pipeline:**

Open **http://localhost:8082** (admin / admin), locate `ecommerce_bigdata_pipeline`, enable it, and click Trigger.

---

## Airflow DAG

The DAG defines five sequential tasks with no branching:

```
ingest_generate_data → upload_to_hdfs → spark_processing → load_to_snowflake → validate_data_warehouse
```

![Airflow UI DAGs page showing the ecommerce_bigdata_pipeline with its schedule interval and last run status](docs/images/airflow-dags.png)

![Airflow Graph view of the pipeline showing all five tasks and their successful execution state](docs/images/airflow-graph.png)

---

## HDFS — Distributed Storage

The raw CSV files generated during ingestion are uploaded directly into HDFS, which serves as the project's data lake layer.

![HDFS NameNode web UI showing cluster summary, live DataNodes, and overall filesystem capacity](docs/images/hdfs-home.png)

![HDFS file browser displaying the uploaded raw CSV files — customers, products, and sales — stored in the distributed data lake](docs/images/hdfs-raw-files.png)

---

## Spark — Distributed Processing

A PySpark job connects to the Spark Standalone cluster, reads raw data from HDFS, applies cleaning and transformation logic, and writes results as Parquet files to a local staging layer before warehouse loading.

![Spark Master UI displaying cluster status, active worker nodes, available memory, and submitted application history](docs/images/spark-cluster.png)

---

## Data Warehouse Schema

The warehouse uses a **Star Schema** optimized for analytical queries.

### Fact Table — `fact_sales`
| Column | Description |
| ------ | ----------- |
| sale_id | Unique sale identifier |
| customer_id | FK to dim_customers |
| product_id | FK to dim_products |
| sale_date_key | FK to dim_time (date) |
| sale_hour_key | FK to dim_time (hour) |
| quantity | Units sold |
| total_amount | Revenue for the transaction |

### Dimension Tables
- **`dim_customers`** — customer information and attributes
- **`dim_products`** — product details, categories, and pricing
- **`dim_time`** — time-based analytical attributes

![Star Schema tables visible in the PostgreSQL Data Warehouse, including fact_sales and all associated dimension tables](docs/images/dwh-tables.png)

![Sample rows from the fact_sales table showing sale_id, foreign keys, quantity, and total_amount values](docs/images/fact-sales-preview.png)

---

## Validation

The `validate_data_warehouse` task runs four automated checks after every pipeline execution:

1. **Row Count Validation** — confirms all tables were populated
2. **Revenue Validation** — cross-checks `SUM(total_amount)` against source data
3. **Star Schema Join Validation** — joins fact and dimension tables to calculate category-level revenue
4. **Referential Integrity Validation** — uses LEFT JOIN checks to detect orphan records

![Airflow task logs from validate_data_warehouse showing the results of all checks including row counts, revenue totals, and referential integrity results](docs/images/validation-logs.png)

![SQL query output displaying total revenue grouped by product category, produced by joining fact_sales with dim_products](docs/images/category-revenue-query.png)

---

## Service URLs

| Service | URL | Credentials |
| ------- | --- | ----------- |
| Airflow UI | http://localhost:8082 | admin / admin |
| Spark Master UI | http://localhost:8080 | — |
| HDFS NameNode UI | http://localhost:9870 | — |
| Mock Snowflake | localhost:5433 | snowflake_user / snowflake_pass |

PostgreSQL can be accessed via DBeaver, pgAdmin, or DataGrip.

---

## Technical Design Decisions

**SQLAlchemy 1.4.49** is pinned instead of 2.x due to incompatibilities with Airflow 2.8.

**Local Parquet staging** — Spark uses `.toPandas().to_parquet()` to write files locally, avoiding HDFS permission conflicts inside Docker containers.

**Full Refresh strategy** — warehouse tables are dropped and reloaded on every run, simplifying reruns and eliminating foreign key conflicts at the cost of incremental efficiency.

**Mock Snowflake** — PostgreSQL acts as a lightweight local substitute for a real Snowflake environment.

---

## Troubleshooting

| Symptom | Cause | Fix |
| ------- | ----- | --- |
| DAG doesn't appear | Airflow still parsing files | Wait 30–60 s and refresh |
| Spark connection refused | Spark Master still initializing | Wait a few seconds and rerun the task |
| HDFS upload failure | HDFS in SafeMode during startup | Retry — the ingestion script includes retry logic |
| Port already in use | Local port conflict | Edit the port mapping in `docker-compose.yml` (e.g. `"8081:8080"`) |

---

## Shutdown

```bash
docker compose down          # stop all containers
docker compose down -v       # stop and remove volumes (requires full re-initialization)
```

---

## Known Limitations & Future Improvements

This project is educational and scoped for local execution. Current limitations include full refresh only (no incremental loading), a single Spark worker, use of `.toPandas()` for Parquet staging, and no streaming or CDC support.

Planned improvements: Kafka streaming · incremental loading · CDC pipelines · multi-worker Spark · direct HDFS Parquet writes · real Snowflake integration · CI/CD automation · monitoring & alerting