import duckdb
from pathlib import Path
import sys

# Detecta la carpeta de salida correcta (preferir data/processed fuera de src)
BASE_DIR = Path(__file__).resolve().parents[1]
DEFAULT_DIR = BASE_DIR / "data" / "processed" / "rim_license_product_filtered"
ALT_DIR = BASE_DIR / "src" / "data" / "processed" / "rim_license_product_filtered"

if DEFAULT_DIR.exists():
    DATA_DIR = DEFAULT_DIR
elif ALT_DIR.exists():
    DATA_DIR = ALT_DIR
else:
    print("❌ No se encontró la carpeta de datos procesados.")
    print(f"   Buscado en: {DEFAULT_DIR}")
    print(f"   Alternativa: {ALT_DIR}")
    sys.exit(1)

# Detecta archivos Parquet o CSV
PARQUET_GLOB = str(DATA_DIR / "rim_license_product_*.parquet")
CSV_GLOB = str(DATA_DIR / "rim_license_product_*.csv")

has_parquet = any(DATA_DIR.glob("rim_license_product_*.parquet"))
has_csv = any(DATA_DIR.glob("rim_license_product_*.csv"))

if not has_parquet and not has_csv:
    print("❌ No se encontraron archivos Parquet ni CSV en la carpeta de entrada.")
    print(f"   Revisar carpeta: {DATA_DIR}")
    sys.exit(1)

# Carpeta para guardar Parquet con columnas seleccionadas
PARQUET_DIR = DATA_DIR / "parquet_selected_cols"
PARQUET_DIR.mkdir(exist_ok=True)

# Conexión a DuckDB (in-memory)
con = duckdb.connect()
con.execute("PRAGMA threads=8;")
con.execute("INSTALL parquet; LOAD parquet;")

# 📝 Columnas necesarias
COLUMNS = [
    "licenseproductid",
    "countryid",
    "licenseid",
    "refproductid",
    "localproductcode",
    "approvedshelflife",
]
cols_str = ",".join(COLUMNS)

# 1️⃣ Crear vista a partir del formato disponible
if has_parquet:
    con.execute(f"""
    CREATE OR REPLACE VIEW rim_filtered AS
    SELECT {cols_str}
    FROM read_parquet('{PARQUET_GLOB}');
    """)
else:
    con.execute(f"""
    CREATE OR REPLACE VIEW rim_filtered AS
    SELECT {cols_str}
    FROM read_csv_auto(
        '{CSV_GLOB}',
        SAMPLE_SIZE=-1,
        ignore_errors=true
    );
    """)

# 2️⃣ Conteo rápido
total_rows = con.execute("SELECT COUNT(*) FROM rim_filtered;").fetchone()[0]
print(f"Total de filas combinadas: {total_rows:,}")

# 3️⃣ Exportar a Parquet optimizado
parquet_path = PARQUET_DIR / "rim_filtered.parquet"
con.execute(f"""
COPY rim_filtered
TO '{parquet_path}'
(FORMAT PARQUET, COMPRESSION ZSTD);
""")

print(f"Archivo Parquet creado en: {parquet_path}")

# 4️⃣ Consulta de ejemplo: top 10 países por cantidad de registros
df_summary = con.execute("""
SELECT countryid, COUNT(*) AS total
FROM rim_filtered
GROUP BY countryid
ORDER BY total DESC
LIMIT 10
""").fetchdf()

print("\nTop 10 countryid por cantidad de registros:")
print(df_summary)

# 5️⃣ Exportar resumen a CSV para Excel
summary_csv = DATA_DIR / "resumen_countryid.csv"
df_summary.to_csv(summary_csv, index=False)
print(f"Resumen exportado a: {summary_csv}")

con.close()