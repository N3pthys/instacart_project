from mage_ai.data_preparation.decorators import pipeline
from mage_ai.data_integrations.connections.mysql import MySQLConnection
from mage_ai.data_integrations.connections.snowflake import SnowflakeConnection
from config import MYSQL_CONFIG, SNOWFLAKE_CONFIG

# Define the pipeline
@pipeline
def mysql_to_snowflake_pipeline():
    # MySQL connection
    mysql_conn = MySQLConnection(config=MYSQL_CONFIG)

    # Snowflake connection
    snowflake_conn = SnowflakeConnection(config=SNOWFLAKE_CONFIG)

    # List of tables to transfer
    tables = [
        "instacart_orders",
        "products",
        "order_products",
        "aisles",
        "departments"
    ]

    for table in tables:
        # Extract data from MySQL table
        mysql_query = f"SELECT * FROM {table}"
        mysql_data = mysql_conn.query(mysql_query)

        # Load data into Snowflake table under the 'RAW' schema
        snowflake_conn.insert(mysql_data, table_name=f"RAW.{table}")
