from mage_ai.data_preparation.decorators import pipeline, step
from mage_ai.io.mysql import MySQL
from mage_ai.io.snowflake import Snowflake

@pipeline
def mysql_to_snowflake_pipeline():
    # MySQL Source Configuration
    mysql = MySQL(
        host="localhost",
        user="instacart_user",
        password="hola2025",
        database="instacart_db",
    )

    # Snowflake Target Configuration
    snowflake = Snowflake(
        user="NEPHTHYS",
        password="Funcat1314",
        account="ETSSQKV.us-east-1.snowflakecomputing.com",
        warehouse="ACCOUNTADMIN",
        database="INSTACART_DB",
        schema="RAW"
    )

    # Step 1: Extract data from MySQL
    @step
    def extract_data():
        table_names = ["instacart_orders", "products", "order_products", "aisles", "departments"]
        data = {}
        for table in table_names:
            query = f"SELECT * FROM {table}"
            data[table] = mysql.query(query)
        return data

    # Step 2: Load data to Snowflake (no transformation)
    @step
    def load_data_to_snowflake(data):
        for table, df in data.items():
            snowflake.write(df, table=table, overwrite=True)

    extract_data() >> load_data_to_snowflake()
