import pandas as pd
from sqlalchemy import create_engine

def validate_dwh():
    engine = create_engine("postgresql://snowflake_user:snowflake_pass@snowflake-mock:5432/ECOMMERCE_DWH")
    
    print("\n--- VALIDATION QUERY 1: Row Counts ---")
    q1 = """
        SELECT 'dim_customers' as table_name, COUNT(*) as count FROM dim_customers
        UNION ALL
        SELECT 'dim_products', COUNT(*) FROM dim_products
        UNION ALL
        SELECT 'dim_time', COUNT(*) FROM dim_time
        UNION ALL
        SELECT 'fact_sales', COUNT(*) FROM fact_sales;
    """
    print(pd.read_sql(q1, engine))

    print("\n--- VALIDATION QUERY 2: Total Revenue ---")
    print(pd.read_sql("SELECT SUM(total_amount) as total_revenue FROM fact_sales;", engine))

    print("\n--- VALIDATION QUERY 3: Sales by Category (Star Schema Join) ---")
    q3 = """
        SELECT 
            p.category,
            COUNT(f.sale_id) as num_sales,
            SUM(f.total_amount) as revenue
        FROM fact_sales f
        JOIN dim_products p ON f.product_id = p.product_id
        GROUP BY p.category
        ORDER BY revenue DESC;
    """
    print(pd.read_sql(q3, engine))
    
    # Data Quality Check
    print("\n--- DATA QUALITY CHECK ---")
    orphan_sales = pd.read_sql("""
        SELECT COUNT(*) as orphan_count FROM fact_sales f
        LEFT JOIN dim_customers c ON f.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
    """, engine)
    
    if orphan_sales["orphan_count"][0] == 0:
        print("✅ SUCCESS: No orphan records in fact_sales. Referential integrity maintained.")
    else:
        print("❌ FAILED: Orphan records detected!")
