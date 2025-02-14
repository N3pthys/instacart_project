import snowflake.connector
import pandas as pd
import yaml

# Load Snowflake credentials from the YAML file
with open('/root/instacart_project/instacart_pipeline/my_connections/snowflake.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Snowflake connection
conn = snowflake.connector.connect(
    user=config['SNOWFLAKE_USER'],
    password=config['SNOWFLAKE_PASSWORD'],
    account=config['SNOWFLAKE_ACCOUNT'],
    warehouse=config['SNOWFLAKE_WAREHOUSE'],
    database=config['SNOWFLAKE_DATABASE'],
    schema=config['SNOWFLAKE_SCHEMA']
)

# List of tables to extract
tables = [
    'instacart_orders',
    'departments',
    'aisles',
    'order_products',
    'products'
]

# Extract data from each table
for table in tables:
    query = f"SELECT * FROM INSTACART_DB.RAW.\"{table}\""
    df = pd.read_sql(query, conn)
    df.to_csv(f'{table}.csv', index=False)
    print(f"Data extracted for {table}")

# Close connection
conn.close()
