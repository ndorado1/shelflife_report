# odbc_extract_by_year.py

import pyodbc
import pandas as pd
import os
import datetime
import time

# ✅ 1. Connection details
dsn = "DenodoODBCTerraProd"
database = "terra"
user = "ndorado1@its.jnj.com"
password = "Trading2024"

# ✅ 2. Output path
output_dir = os.path.join("data", "processed", "rim_license_product_filtered")
os.makedirs(output_dir, exist_ok=True)

# ✅ 3. Connection string
conn_str = f"DSN={dsn};DATABASE={database};UID={user};PWD={password};"

try:
    # ✅ 4. Open connection
    conn = pyodbc.connect(conn_str)
    print("✅ Connection successful!")

    # ✅ 5. Start timer
    start_time = time.time()

    # ✅ 6. Extracción paginada por claves (25k filas por archivo)
    table_name = "rim_license_product"
    id_column = "licenseproductid"
    chunk_size = 100_000

    total_rows = 0
    part = 1

    # Mensajes de depuración
    print("🔎 Debug: inicio de extracción")
    print(f"   Tabla: {table_name}")
    print(f"   Clave de orden: {id_column}")
    print(f"   Chunk size: {chunk_size:,}")
    print("   Filtros: countryid = '392', licenseproductstatus = 'Active'")
    print(f"   Carpeta destino: {output_dir}")
    print(f"   Hora de inicio: {time.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"📦 Extrayendo tabla completa: {table_name} en chunks de {chunk_size:,} filas...")

    last_id = None
    while True:
        if last_id is None:
            sql_query = f"""
            SELECT *
            FROM {table_name}
            WHERE {id_column} IS NOT NULL
              AND countryid = '392'
              AND licenseproductstatus = 'Active'
            ORDER BY {id_column}
            LIMIT {chunk_size}
            """
            try:
                df = pd.read_sql(sql_query, conn)
            except Exception as page_error:
                print(f"   ❌ Error en la paginación inicial: {page_error}")
                break
        else:
            sql_query = f"""
            SELECT *
            FROM {table_name}
            WHERE {id_column} > ?
              AND countryid = '392'
              AND licenseproductstatus = 'Active'
            ORDER BY {id_column}
            LIMIT {chunk_size}
            """
            try:
                df = pd.read_sql(sql_query, conn, params=[int(last_id)])
            except Exception as page_error:
                print(f"   ❌ Error en la paginación (last_id={last_id}): {page_error}")
                break

        if id_column not in df.columns:
            print(f"   ❌ La columna de clave '{id_column}' no está en el resultado.")
            break

        num_rows = len(df)
        if num_rows == 0:
            print("   ✅ No hay más filas para extraer.")
            break

        total_rows += num_rows
        # Intentar guardar como Parquet; fallback a CSV si no está pyarrow
        try:
            import pyarrow  # noqa: F401
            output_file = os.path.join(output_dir, f"{table_name}_{part:05d}.parquet")
            df.to_parquet(output_file, index=False)
            saved_fmt = "parquet"
        except Exception:
            output_file = os.path.join(output_dir, f"{table_name}_{part:05d}.csv")
            df.to_csv(output_file, index=False)
            saved_fmt = "csv"

        # Log de progreso
        first_id_val = df[id_column].iloc[0]
        last_id_val = df[id_column].iloc[-1]
        print(
            f"   ✅ Chunk {part:05d}: {num_rows:,} filas | {id_column}: {first_id_val} → {last_id_val} | Archivo: {output_file} ({saved_fmt})"
        )

        part += 1
        try:
            last_id = int(last_id_val)
        except Exception:
            last_id = last_id_val

    # Resumen
    print("\n📊 RESUMEN:")
    print(f"   Total de filas extraídas: {total_rows:,}")
    print(f"   Total de archivos generados: {part - 1}")
    print(f"   Carpeta de salida: {output_dir}")

    # ✅ 7. Close connection
    conn.close()

    total_time = time.time() - start_time
    print(f"\n✅✅ Extraction complete in {total_time/60:.2f} minutes.")

except Exception as e:
    print("❌ Error during extraction:")
    print(e)
    if 'conn' in locals():
        conn.close()