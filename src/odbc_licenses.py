# test_odbc_connection.py

import pyodbc
import pandas as pd
import os

# Connection details
dsn = "DenodoODBCTerraProd"
database = "terra"
user = "ndorado1@its.jnj.com"
password = "Trading2024"

# Example SQL query - replace with your table or view
sql_query = """
SELECT * FROM rim_ref:master
WHERE "Region" = 'LATAM'
"""
output_path = os.path.join("data", "raw", "LATAM_licenses_details.csv")

os.makedirs(os.path.join("data", "raw"), exist_ok=True)

# Create connection string using DSN
conn_str = f"DSN={dsn};DATABASE={database};UID={user};PWD={password};"

try:
    # Connect to ODBC source
    conn = pyodbc.connect(conn_str) 

    print("Connection successful!")

    # Run query
    df = pd.read_sql(sql_query, conn)
    print(f"Total Rows: {len(df)}")

    df.to_csv (output_path,index=False)
    print(f"CSV saved to {output_path}")

    conn.close()

except Exception as e:
    print("Connection failed.")
    print(e)