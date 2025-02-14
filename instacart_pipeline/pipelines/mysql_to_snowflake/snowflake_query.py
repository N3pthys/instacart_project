import snowflake.connector

# Connect to Snowflake
conn = snowflake.connector.connect(
    user='NEPHTHYS',
    password='Funcat1314',
    account='jub37779.us-east-1',
    warehouse='COMPUTE_WH',
    database='INSTACART_DB',
    schema='RAW'
)

# Create a cursor object
cursor = conn.cursor()

# Run the query to check grants
cursor.execute('SHOW GRANTS TO USER NEPHTHYS;')

# Fetch and print the result
grants = cursor.fetchall()
for grant in grants:
    print(grant)

# Close the cursor and connection
cursor.close()
conn.close()
