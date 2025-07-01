# test_odbc_connection.py

import pyodbc
import pandas as pd

# Connection details
dsn = "DenodoODBCTerraProd"
database = "terra"
user = "ndorado1@its.jnj.com"
password = "Comercio2024"

# Example SQL query - replace with your table or view
sql_query = "SELECT TOP 10 * FROM view_ra_action_license_details"

# Create connection string using DSN
conn_str = f"DSN={dsn};DATABASE={database};UID={user};PWD={password};"

try:
    # Connect to ODBC source
    conn = pyodbc.connect(conn_str)

    print("Connection successful!")

    # Run query
    df = pd.read_sql(sql_query, conn)
    print(df.head())

    conn.close()

except Exception as e:
    print("Connection failed.")
    print(e)