import pandas as pd
from sqlalchemy import create_engine

def load_to_dwh():
    engine = create_engine("postgresql://snowflake_user:snowflake_pass@snowflake-mock:5432/ECOMMERCE_DWH")
    
    # Drop tables in reverse dependency order to avoid Foreign Key errors
    with engine.begin() as conn:
        conn.execute("DROP TABLE IF EXISTS fact_sales CASCADE;")
        conn.execute("DROP TABLE IF EXISTS dim_time CASCADE;")
        conn.execute("DROP TABLE IF EXISTS dim_products CASCADE;")
        conn.execute("DROP TABLE IF EXISTS dim_customers CASCADE;")
    
    tables = ["dim_customers", "dim_products", "dim_time", "fact_sales"]
    
    for table in tables:
        print(f"Loading {table} into Mock Snowflake...")
        df = pd.read_parquet(f"/opt/airflow/data/processed/{table}.parquet")
        
        # Use append because we already dropped and recreated cleanly
        df.to_sql(table, engine, if_exists="append", index=False)
        print(f"Successfully loaded {len(df)} rows into {table}.")
