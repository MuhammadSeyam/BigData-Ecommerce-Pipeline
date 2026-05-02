import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, hour, dayofweek, month, year

def process_data():
    spark = SparkSession.builder.appName("EcommerceTransformation").master("spark://spark-master:7077").config("spark.hadoop.fs.defaultFS", "hdfs://namenode:8020").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    
    OUTPUT_DIR = "/opt/airflow/data/processed"
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    # 1. Read from HDFS
    print("Reading data from HDFS...")
    sales_df = spark.read.csv("hdfs://namenode:8020/user/airflow/raw/sales.csv", header=True, inferSchema=True)
    customers_df = spark.read.csv("hdfs://namenode:8020/user/airflow/raw/customers.csv", header=True, inferSchema=True)
    products_df = spark.read.csv("hdfs://namenode:8020/user/airflow/raw/products.csv", header=True, inferSchema=True)

    # 2. Clean
    print("Cleaning data...")
    sales_df = sales_df.dropna(subset=["sale_time"])

    # 3. Transform
    print("Building Star Schema...")
    dim_customers = customers_df.select(col("customer_id"), col("name"), col("email"), col("city"))
    dim_products = products_df.select(col("product_id"), col("name"), col("category"), col("price"))
    dim_time = sales_df.select(col("sale_date").alias("date"), hour("sale_time").alias("hour"), dayofweek("sale_date").alias("day_of_week"), month("sale_date").alias("month"), year("sale_date").alias("year")).dropDuplicates(["date", "hour"])
    fact_sales = sales_df.select(col("sale_id"), col("customer_id"), col("product_id"), col("sale_date").alias("sale_date_key"), hour("sale_time").alias("sale_hour_key"), col("quantity"), col("total_amount"))

    # 4. Save locally using Pandas (avoids HDFS permission issues)
    print("Saving processed data to local staging...")
    dim_customers.toPandas().to_parquet(f"{OUTPUT_DIR}/dim_customers.parquet")
    dim_products.toPandas().to_parquet(f"{OUTPUT_DIR}/dim_products.parquet")
    dim_time.toPandas().to_parquet(f"{OUTPUT_DIR}/dim_time.parquet")
    fact_sales.toPandas().to_parquet(f"{OUTPUT_DIR}/fact_sales.parquet")

    print("Spark processing completed successfully.")
    spark.stop()
