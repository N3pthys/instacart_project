from pymysql.cursors import DictCursor
import pymysql
import pandas as pd
import yaml
from mage_ai.io.snowflake import Snowflake
import logging

logging.basicConfig(level=logging.DEBUG)

# Load MySQL credentials manually
with open('/root/instacart_project/instacart_pipeline/my_connections/mysql.yaml', 'r') as f:
    mysql_config = yaml.safe_load(f)

print("✅ MySQL config loaded:", mysql_config)

if not isinstance(mysql_config, dict):
    raise ValueError("❌ mysql.yaml did not load as a dictionary!")

# Load Snowflake credentials manually
with open('/root/instacart_project/instacart_pipeline/my_connections/snowflake.yaml', 'r') as f:
    snowflake_config = yaml.safe_load(f)

print("✅ Snowflake config loaded:", snowflake_config)

if not isinstance(snowflake_config, dict):
    raise ValueError("❌ snowflake.yaml did not load as a dictionary!")


# Initialize Snowflake connection
snowflake = Snowflake(
    account=snowflake_config["SNOWFLAKE_ACCOUNT"],
    user=snowflake_config["SNOWFLAKE_USER"],
    password=snowflake_config["SNOWFLAKE_PASSWORD"],
    warehouse=snowflake_config["SNOWFLAKE_WAREHOUSE"],
    database=snowflake_config["SNOWFLAKE_DATABASE"],
    schema=snowflake_config["SNOWFLAKE_SCHEMA"],
    insecure_mode=True
)

# MySQL Connection using pymysql
try:
    connection = pymysql.connect(
        host=mysql_config["MYSQL_HOST"],
        database=mysql_config["MYSQL_DATABASE"],
        user=mysql_config["MYSQL_USER"],
        password=mysql_config["MYSQL_PASSWORD"],
        port=int(mysql_config["MYSQL_PORT"]),
        cursorclass=DictCursor
    )
    print("✅ MySQL connection established.")
except pymysql.MySQLError as e:
    print(f"❌ MySQL connection error: {e}")
    exit(1)

tables = ["instacart_orders", "products", "order_products", "aisles", "departments"]
BATCH_SIZE = 10000

# Open a Snowflake connection using `with_config`
with Snowflake.with_config(snowflake_config) as snowflake_conn:
    for table in tables:
        print(f"📥 Extracting {table} from MySQL...")

        try:
            cursor = connection.cursor()
            cursor.execute(f"SELECT * FROM {table}")
            rows = cursor.fetchall()
            df = pd.DataFrame(rows)

            print(f"✅ Extracted {table} successfully!")

            # Batch processing
            num_rows = len(df)
            for start in range(0, num_rows, BATCH_SIZE):
                batch_df = df.iloc[start:start+BATCH_SIZE]

                print(f"📤 Inserting {len(batch_df)} rows into {table} (Batch {start//BATCH_SIZE + 1})...")

                # ✅ Use the Snowflake connection within the `with` statement
                snowflake_conn.export(batch_df, table, schema=snowflake_config["SNOWFLAKE_SCHEMA"])

                print(f"✅ Batch {start//BATCH_SIZE + 1} inserted successfully!")

            print(f"✅ Loaded {table} into Snowflake successfully!")

        except Exception as e:
            print(f"❌ Error processing {table}: {e}")

connection.close()
print("🎉✅ Data successfully transferred from MySQL to Snowflake!")
