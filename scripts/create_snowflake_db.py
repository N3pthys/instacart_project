import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='NEPHTHYS',
    password='Funcat1314',
    account='ETSSQKV.us-east-1.snowflakecomputing.com',
    warehouse='ACCOUNTADMIN',
    database='instacart_db',
    schema='instacart_schema'
)

# Create a cursor object to interact with Snowflake
cursor = conn.cursor()

# SQL commands to create database and schema
try:
    cursor.execute("CREATE DATABASE instacart_db;")
    cursor.execute("USE DATABASE instacart_db;")
    cursor.execute("CREATE SCHEMA instacart_schema;")
    print("Database and schema created successfully!")
finally:
    cursor.close()
    conn.close()
