from mage_ai.io.mysql import MySQL
from mage_ai.io.snowflake import Snowflake
import yaml

# Load MySQL credentials manually
with open('/root/instacart_project/instacart_pipeline/my_connections/mysql.yaml', 'r') as f:
    mysql_config = yaml.safe_load(f)

# Debugging: Print MySQL config
print("✅ MySQL config loaded:", mysql_config)

# Ensure it's a dictionary
if not isinstance(mysql_config, dict):
    raise ValueError("❌ mysql.yaml did not load as a dictionary!")

# Connect to MySQL using manually loaded credentials
mysql = MySQL(
    database=mysql_config["MYSQL_DATABASE"],
    host=mysql_config["MYSQL_HOST"],
    user=mysql_config["MYSQL_USER"],
    password=mysql_config["MYSQL_PASSWORD"],
    port=int(mysql_config["MYSQL_PORT"]),
)

# Load Snowflake credentials manually
with open('/root/instacart_project/instacart_pipeline/my_connections/snowflake.yaml', 'r') as f:
    snowflake_config = yaml.safe_load(f)

# Debugging: Print Snowflake config
print("✅ Snowflake config loaded:", snowflake_config)

# Ensure it's a dictionary
if not isinstance(snowflake_config, dict):
    raise ValueError("❌ snowflake.yaml did not load as a dictionary!")

# Connect to Snowflake using manually loaded credentials
snowflake = Snowflake(
    account=snowflake_config["SNOWFLAKE_ACCOUNT"],
    user=snowflake_config["SNOWFLAKE_USER"],
    password=snowflake_config["SNOWFLAKE_PASSWORD"],
    database=snowflake_config["SNOWFLAKE_DATABASE"],
    warehouse=snowflake_config["SNOWFLAKE_WAREHOUSE"],
    schema=snowflake_config["SNOWFLAKE_SCHEMA"]
)

# Tables to transfer
tables = ["instacart_orders", "products", "order_products", "aisles", "departments"]

for table in tables:
    print(f"Extracting {table} from MySQL...")
    df = mysql.load(f"SELECT * FROM {table}")

    print(f"Loading {table} into Snowflake...")
    snowflake.export(df, table, schema="RAW")

print("✅ Data successfully transferred from MySQL to Snowflake!")
