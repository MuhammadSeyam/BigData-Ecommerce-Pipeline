from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.insert(0, "/opt/airflow/scripts")
from data_generator import generate_data_main
from hdfs_client import upload_to_hdfs
from spark_processing import process_data
from load_to_snowflake import load_to_dwh
from validation import validate_dwh

default_args = {"owner": "data_engineer", "depends_on_past": False, "start_date": datetime(2023, 10, 1), "retries": 1, "retry_delay": 0}

with DAG("ecommerce_bigdata_pipeline", default_args=default_args, schedule_interval=None, catchup=False, tags=["final_project"]) as dag:
    t1 = PythonOperator(task_id="ingest_generate_data", python_callable=generate_data_main)
    t2 = PythonOperator(task_id="upload_to_hdfs", python_callable=upload_to_hdfs)
    t3 = PythonOperator(task_id="spark_processing", python_callable=process_data)
    t4 = PythonOperator(task_id="load_to_snowflake", python_callable=load_to_dwh)
    t5 = PythonOperator(task_id="validate_data_warehouse", python_callable=validate_dwh)
    t1 >> t2 >> t3 >> t4 >> t5
