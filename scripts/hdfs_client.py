import time
from hdfs import InsecureClient
def upload_to_hdfs():
    client = InsecureClient("http://namenode:9870", user="root")
    print("Waiting for HDFS to be ready...")
    for _ in range(30):
        try:
            if client.status("/"): print("HDFS is ready."); break
        except Exception: time.sleep(2)
    client.makedirs("/user/airflow/raw", permission="755")
    for file in ["customers.csv", "products.csv", "sales.csv"]:
        client.upload(f"/user/airflow/raw/{file}", f"/opt/airflow/data/raw/{file}", overwrite=True)
        print(f"Uploaded {file} to HDFS")
