import pandas as pd
import os

# Define file paths correctly
instacart_orders_path = "/root/instacart_project/instacart_pipeline/pipelines/instacart_orders.csv"
order_products_path = "/root/instacart_project/instacart_pipeline/pipelines/order_products.csv"
departments_path = "/root/instacart_project/instacart_pipeline/pipelines/departments.csv"
aisles_path = "/root/instacart_project/instacart_pipeline/pipelines/aisles.csv"
products_path = "/root/instacart_project/instacart_pipeline/pipelines/products.csv"

# Ensure files exist before reading
for path in [instacart_orders_path, order_products_path, departments_path, aisles_path, products_path]:
    if not os.path.exists(path):
        raise FileNotFoundError(f"File not found: {path}")

# Load the data from the CSV files
instacart_orders = pd.read_csv(instacart_orders_path)
order_products = pd.read_csv(order_products_path)
departments = pd.read_csv(departments_path)
aisles = pd.read_csv(aisles_path)
products = pd.read_csv(products_path)

# Ensure the necessary columns exist in the instacart_orders table
instacart_orders['order_number'] = instacart_orders['order_number'].fillna(0).astype(int)
instacart_orders['order_dow'] = instacart_orders['order_dow'].fillna(0).astype(int)
instacart_orders['order_hour_of_day'] = instacart_orders['order_hour_of_day'].fillna(0).astype(int)
instacart_orders['days_since_prior_order'] = instacart_orders['days_since_prior_order'].fillna(0).astype(int)

# Merge the data step by step
# Merge order_products with instacart_orders to get the products for each order
fact_orders = instacart_orders.merge(
    order_products[['order_id', 'product_id', 'add_to_cart_order', 'reordered']], 
    on='order_id', 
    how='left'
)

# Merge products table to get aisle_id and department_id
fact_orders = fact_orders.merge(
    products[['product_id', 'aisle_id', 'department_id']], 
    on='product_id', 
    how='left'
)

# Merge aisles table to add aisle information
fact_orders = fact_orders.merge(
    aisles[['aisle_id']], 
    on='aisle_id', 
    how='left'
)

# Merge departments table to add department information
fact_orders = fact_orders.merge(
    departments[['department_id']], 
    on='department_id', 
    how='left'
)

# Handle any missing data by filling missing columns
fact_orders['order_id'] = fact_orders['order_id'].fillna(0).astype(int)
fact_orders['product_id'] = fact_orders['product_id'].fillna(0).astype(int)
fact_orders['user_id'] = fact_orders['user_id'].fillna(0).astype(int)
fact_orders['add_to_cart_order'] = fact_orders['add_to_cart_order'].fillna(0).astype(int)
fact_orders['reordered'] = fact_orders['reordered'].fillna(0).astype(int)
fact_orders['aisle_id'] = fact_orders['aisle_id'].fillna(-1).astype(int)
fact_orders['department_id'] = fact_orders['department_id'].fillna(-1).astype(int)

# Save the transformed data
fact_orders.to_csv('/root/instacart_project/instacart_pipeline/pipelines/fact_orders.csv', index=False, encoding='utf-8')

print("Data transformation complete and saved to fact_orders.csv!")
