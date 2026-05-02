import csv, random, os
from datetime import datetime, timedelta
def generate_data_main():
    OUTPUT_DIR = "/opt/airflow/data/raw"
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    random.seed(42)
    customers = [[i, f"customer_{i}", f"email_{i}@test.com", random.choice(["NY", "LA", "TX", "FL", "WA"])] for i in range(1, 101)]
    with open(f"{OUTPUT_DIR}/customers.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["customer_id", "name", "email", "city"]); w.writerows(customers)
    categories = ["Electronics", "Clothing", "Home", "Sports", "Books"]
    products = [[i, f"product_{i}", random.choice(categories), round(random.uniform(10.0, 500.0), 2)] for i in range(1, 51)]
    with open(f"{OUTPUT_DIR}/products.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["product_id", "name", "category", "price"]); w.writerows(products)
    sales = []; base_date = datetime(2023, 1, 1)
    for i in range(1, 1001):
        sale_date = base_date + timedelta(days=random.randint(0, 365))
        cust_id, prod_id, qty = random.randint(1, 100), random.randint(1, 50), random.randint(1, 5)
        price = next(p[3] for p in products if p[0] == prod_id)
        total = round(qty * price, 2)
        time_val = None if i in [50, 150, 350, 550, 750] else sale_date.strftime("%H:%M:%S")
        sales.append([i, cust_id, prod_id, sale_date.strftime("%Y-%m-%d"), time_val, qty, total])
    with open(f"{OUTPUT_DIR}/sales.csv", "w", newline="") as f:
        w = csv.writer(f); w.writerow(["sale_id", "customer_id", "product_id", "sale_date", "sale_time", "quantity", "total_amount"]); w.writerows(sales)
    print("Data generation completed successfully.")
