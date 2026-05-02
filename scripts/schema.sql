-- Create Schema
CREATE SCHEMA IF NOT EXISTS raw;

-- Dim Customers
CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    name VARCHAR(100),
    email VARCHAR(100),
    city VARCHAR(50)
);

-- Dim Products
CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    name VARCHAR(100),
    category VARCHAR(50),
    price DECIMAL(10, 2)
);

-- Dim Time
CREATE TABLE IF NOT EXISTS dim_time (
    date DATE PRIMARY KEY,
    hour INT,
    day_of_week INT,
    month INT,
    year INT
);

-- Fact Sales
CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id INT PRIMARY KEY,
    customer_id INT,
    product_id INT,
    sale_date_key DATE,
    sale_hour_key INT,
    quantity INT,
    total_amount DECIMAL(10, 2),
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id)
);