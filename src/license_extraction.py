import pyodbc
import pandas as pd
import os
import time

# ✅ 1. Connection details (mismas credenciales que odbc_match.py)
dsn = "DenodoODBCTerraProd"
database = "terra"
user = "ndorado1@its.jnj.com"
password = "Trading2024"

# ✅ 2. Output path
output_dir = os.path.join("data", "raw")
os.makedirs(output_dir, exist_ok=True)
output_file = os.path.join(output_dir, "rim_license_392.csv")

# ✅ 3. Connection string
conn_str = f"DSN={dsn};DATABASE={database};UID={user};PWD={password};"

try:
    # ✅ 4. Open connection
    print("🚀 Iniciando extracción de 'rim_license' (countryid = 392)...")
    start_time = time.time()

    conn = pyodbc.connect(conn_str)
    print("✅ Conexión ODBC exitosa")

    # ✅ 5. Ejecutar consulta filtrando columnas y país
    sql_query = """
    SELECT licenseid, licensenumber
    FROM rim_license
    WHERE countryid = 392
    """

    print("📥 Ejecutando consulta (sin particionar)...")
    df = pd.read_sql(sql_query, conn)
    print(f"📊 Filas extraídas: {len(df):,}")
    print(f"📊 Columnas: {len(df.columns):,}")

    # ✅ 6. Guardar CSV
    if len(df) > 0:
        df.to_csv(output_file, index=False)
        print(f"💾 Archivo guardado: {output_file}")
    else:
        print("⚠️ La tabla no contiene datos. No se generó archivo.")

    # ✅ 7. Cerrar conexión
    conn.close()

    total_time = time.time() - start_time
    print(f"✅✅ Extracción completa en {total_time/60:.2f} minutos.")

except Exception as e:
    print("❌ Error durante la extracción:")
    print(e)
    if 'conn' in locals():
        try:
            conn.close()
        except Exception:
            pass 