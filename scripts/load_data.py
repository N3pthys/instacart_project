import snowflake.connector
import pandas as pd
import yaml

# Load Snowflake credentials
with open('/root/instacart_project/instacart_pipeline/my_connections/snowflake.yaml', 'r') as file:
    config = yaml.safe_load(file)

# Connect to Snowflake
conn = snowflake.connector.connect(
    user=config['SNOWFLAKE_USER'],
    password=config['SNOWFLAKE_PASSWORD'],
    account=config['SNOWFLAKE_ACCOUNT'],
    warehouse=config['SNOWFLAKE_WAREHOUSE'],
    database=config['SNOWFLAKE_DATABASE'],
    schema='CLEAN'  # Load to CLEAN schema
)
cur = conn.cursor()

# Load CSV data into Pandas DataFrames
tables = {
    "DIM_PRODUCTS": pd.read_csv('dim_products.csv'),
    "DIM_DEPARTMENTS": pd.read_csv('dim_departments.csv'),
    "DIM_AISLES": pd.read_csv('dim_aisles.csv'),
    "FACT_ORDERS": pd.read_csv('fact_orders.csv')  # Ensure correct table name
}

BATCH_SIZE = 10000  # Define batch size

# Function to get the last loaded row for incremental loading
def get_last_loaded_row(table_name, column, conn):
    query = f"SELECT MAX({column}) FROM CLEAN.{table_name}"
    cur.execute(query)
    result = cur.fetchone()
    last_value = result[0] if result[0] is not None else 0
    print(f"🔍 Last loaded {column} in {table_name}: {last_value}")  # Debugging step
    return last_value

# Function to insert new data in batches
def load_table(data, table_name, primary_key, conn, cur):
    last_loaded_value = get_last_loaded_row(table_name, primary_key, conn)
    new_data = data[data[primary_key] > last_loaded_value]  # Filter only new data

    if new_data.empty:
        print(f"✅ No new data to insert for {table_name}.")
        return

    # Debugging step: check for NaN values
    if new_data.isna().sum().sum() > 0:
        print(f"⚠️ Warning: NaN values detected in {table_name} before insertion.")
        print(new_data.isna().sum())  # Show columns with NaN values

    # Ensure column names match Snowflake schema (case-sensitive)
    new_data.columns = [col.upper() for col in new_data.columns]

    num_rows = len(new_data)
    print(f"📥 {num_rows} new rows found for {table_name}. Loading in batches of {BATCH_SIZE}...")

    for start in range(0, num_rows, BATCH_SIZE):
        batch_df = new_data.iloc[start:start + BATCH_SIZE]
        columns = ', '.join(batch_df.columns)
        values_list = [tuple(row) for row in batch_df.to_numpy()]
        
        # Use executemany for efficient batch insertion
        placeholders = ', '.join(['%s'] * len(batch_df.columns))
        insert_query = f"INSERT INTO CLEAN.{table_name} ({columns}) VALUES ({placeholders})"
        
        try:
            cur.executemany(insert_query, values_list)
            conn.commit()
            print(f"✅ Batch {start // BATCH_SIZE + 1} inserted successfully into {table_name}.")
        except Exception as e:
            print(f"❌ Error inserting batch {start // BATCH_SIZE + 1} into {table_name}: {e}")
            conn.rollback()  # Rollback on error

# Debugging step: Check if tables have data before insertion
for table_name, df in tables.items():
    print(f"📊 Checking {table_name}: {len(df)} rows")
    print(df.head())  # Print first few rows to verify structure

# Load dimension tables (incremental)
load_table(tables["DIM_PRODUCTS"], "DIM_PRODUCTS", "product_id", conn, cur)
load_table(tables["DIM_DEPARTMENTS"], "DIM_DEPARTMENTS", "department_id", conn, cur)
load_table(tables["DIM_AISLES"], "DIM_AISLES", "aisle_id", conn, cur)

# Load fact table (incremental) - FIXED table name
load_table(tables["FACT_ORDERS"], "FACT_ORDERS", "order_id", conn, cur)

# Close Snowflake connection
cur.close()
conn.close()
