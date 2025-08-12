import os
import duckdb
import pandas as pd
import streamlit as st
from io import BytesIO
from pathlib import Path


# ========================
# Sidebar: Data Source
# ========================
st.set_page_config(page_title="RIM License Shelflife Product – Report", layout="wide")
st.title("RIM License Shelf Life Product – Report")

with st.sidebar:
    st.header("Data Source")
    default_parquet = \
        "/Users/personal/Library/CloudStorage/OneDrive-JNJ/Documentos/ODBC/src/data/processed/rim_license_product_filtered/parquet_selected_cols/rim_filtered.parquet"

    st.info("**Source:** Parquet file")
    st.code(default_parquet, language=None)
    if Path(default_parquet).exists():
        file_size = Path(default_parquet).stat().st_size / (1024*1024)  # MB
        st.caption(f"File size: {file_size:.1f} MB")
    else:
        st.warning("File not found at expected location")
    
    data_info = {"mode": "parquet", "path": default_parquet}

# ========================
# DuckDB connection helper
# ========================
@st.cache_resource(show_spinner=False)
def get_con():
    try:
        con = duckdb.connect()
        con.execute("PRAGMA threads=8;")
        con.execute("INSTALL parquet; LOAD parquet;")
        return con
    except Exception as e:
        st.error(f"Error initializing DuckDB connection: {e}")
        st.stop()

con = get_con()

# ========================
# Register dataset as a DuckDB view
# ========================
base_view_name = "rim_base"
view_name = "rim"

# Buscar CSV de licencias en múltiples ubicaciones posibles
BASE_DIR = Path(__file__).resolve().parents[1]
possible_paths = [
    BASE_DIR / "data" / "raw" / "rim_license_392.csv",
    BASE_DIR / "src" / "data" / "raw" / "rim_license_392.csv",
    Path(__file__).parent / "data" / "raw" / "rim_license_392.csv",
]

LICENSE_CSV_PATH = None
for path in possible_paths:
    if path.exists():
        LICENSE_CSV_PATH = path
        break



try:
    path = data_info["path"]
    if not path or not Path(path).exists():
        st.error("Parquet file not found. Please check the file location.")
        st.stop()
    # Crea vista base desde Parquet
    escaped_path = path.replace("'", "''")
    con.execute(f"CREATE OR REPLACE VIEW {base_view_name} AS SELECT * FROM read_parquet('{escaped_path}')")

    # Enriquecer con licensenumber si el CSV existe
    if LICENSE_CSV_PATH and LICENSE_CSV_PATH.exists():
        lic_path_esc = str(LICENSE_CSV_PATH).replace("'", "''")
        # Normalizar y TRIM para join robusto
        con.execute(
            f"""
            CREATE OR REPLACE VIEW license_map AS
            SELECT TRIM(CAST(licenseid AS VARCHAR)) AS licenseid,
                   TRIM(CAST(licensenumber AS VARCHAR)) AS licensenumber
            FROM read_csv_auto('{lic_path_esc}', SAMPLE_SIZE=-1, ignore_errors=true, header=true, normalize_names=true)
            """
        )
        # Info del CSV cargado
       

        con.execute(
            f"""
            CREATE OR REPLACE VIEW {view_name} AS
            SELECT r.*, lm.licensenumber
            FROM {base_view_name} r
            LEFT JOIN license_map lm
              ON TRIM(CAST(CAST(r.licenseid AS INTEGER) AS VARCHAR)) = lm.licenseid
            """
        )
    else:
        # Si no existe el CSV, usar la vista base tal cual
        con.execute(f"CREATE OR REPLACE VIEW {view_name} AS SELECT * FROM {base_view_name}")

except Exception as e:
    st.exception(e)
    st.stop()

# ========================
# Basic schema (expecting 6+ columns)
# ========================
expected_cols = [
    "licenseid","localproductcode","approvedshelflife"
]
# Solo esperamos licensenumber si existe el CSV
if LICENSE_CSV_PATH and LICENSE_CSV_PATH.exists():
    expected_cols.append("licensenumber")

cols_df = con.execute(f"PRAGMA table_info('{view_name}')").fetchdf()
present_cols = cols_df["name"].str.lower().tolist()
missing = [c for c in expected_cols if c not in present_cols]
if missing:
    st.warning(f"Expected columns missing: {missing}. Proceeding with available columns.")

# ========================
# Filters (sidebar)
# ========================
with st.sidebar:
    st.header("Filters")

    # Distincts for selectors (avoid scanning huge data: limit to reasonable)
    def distinct(col):
        return con.execute(f"SELECT DISTINCT {col} AS v FROM {view_name} WHERE {col} IS NOT NULL LIMIT 20000").fetchdf()["v"].tolist()

    # Filtro por licensenumber si existe
    licensenumber_filter = ""
    if "licensenumber" in present_cols:
        licensenumber_filter = st.text_input("Licencia Contiene… (Sensible a mayúsculas y minúsculas)")

    lpc_like = st.text_input("Producto Contiene… (Sensible a mayúsculas y minúsculas)")
    asl_like = st.text_input("Shelf Life Contiene… (Sensible a mayúsculas y minúsculas)")

    st.divider()
    limit_rows = st.slider("Preview rows", 1000, 200000, 20000, step=1000, help="Only limits the preview table below.")

# ========================
# Build WHERE clause dynamically
# ========================
where = []
params = []

if licensenumber_filter.strip() and "licensenumber" in present_cols:
    where.append("lower(licensenumber) LIKE lower(?)")
    params.append(f"%{licensenumber_filter}%")

if lpc_like.strip() and "localproductcode" in present_cols:
    where.append("lower(localproductcode) LIKE lower(?)")
    params.append(f"%{lpc_like}%")

if asl_like.strip() and "approvedshelflife" in present_cols:
    where.append("CAST(approvedshelflife AS VARCHAR) LIKE ?")
    params.append(f"%{asl_like}%")

where_sql = (" WHERE " + " AND ".join(where)) if where else ""

# ========================
# KPIs
# ========================
col1, col2, col3 = st.columns(3)

try:
    total_rows = con.execute(f"SELECT COUNT(*) FROM {view_name}{where_sql}", params).fetchone()[0]
    with col1:
        st.metric("Rows (filtered)", f"{total_rows:,}")
except Exception as e:
    with col1:
        st.error("Count failed")

try:
    distinct_licenses = con.execute(f"SELECT COUNT(DISTINCT licenseid) FROM {view_name}{where_sql}", params).fetchone()[0]
    with col2:
        st.metric("Distinct licenses", f"{distinct_licenses:,}")
except Exception:
    pass

try:
    distinct_products = con.execute(f"SELECT COUNT(DISTINCT localproductcode) FROM {view_name}{where_sql}", params).fetchone()[0]
    with col3:
        st.metric("Distinct products", f"{distinct_products:,}")
except Exception:
    pass

st.divider()

# ========================
# Preview table (limited)
# ========================
try:
    preview_sql = f"SELECT * FROM {view_name}{where_sql} LIMIT {limit_rows}"
    preview_df = con.execute(preview_sql, params).fetchdf()
except Exception as e:
    st.error(f"Error executing preview query: {e}")
    st.info("Try refreshing the page or adjusting your filters.")
    st.stop()
# Filtrar columnas que queremos mostrar
cols_to_show = ["licensenumber", "licenseid", "localproductcode", "approvedshelflife"]
available_cols = [c for c in cols_to_show if c in preview_df.columns]
preview_df = preview_df[available_cols]

# Convertir columnas a enteros (si existen) y configurar visualización sin separador de miles
int_cols = [c for c in ["licenseid", "approvedshelflife"] if c in preview_df.columns]
for c in int_cols:
    preview_df[c] = pd.to_numeric(preview_df[c], errors="coerce").astype("Int64")

# Si hay licensenumber, ocultar licenseid
if "licensenumber" in preview_df.columns and "licenseid" in preview_df.columns:
    preview_df = preview_df.drop(columns=["licenseid"])
    int_cols = [c for c in int_cols if c != "licenseid"]

col_config = {c: st.column_config.NumberColumn(format="%d", step=1) for c in int_cols}

st.caption(f"Preview: showing up to {limit_rows:,} rows")
st.dataframe(preview_df, use_container_width=True, hide_index=True, column_config=col_config)



# ========================
# Downloads (CSV / Excel)
# ========================
st.divider()
st.subheader("Download filtered data")

max_export = 500_000  # safety cap
try:
    export_sql = f"SELECT * FROM {view_name}{where_sql} LIMIT {max_export}"
    export_df = con.execute(export_sql, params).fetchdf()
except Exception as e:
    st.error(f"Error preparing export data: {e}")
    st.info("Try reducing the number of preview rows or simplifying your filters.")
    st.stop()
# Filtrar columnas para exportación
cols_to_export = ["licensenumber", "licenseid", "localproductcode", "approvedshelflife"]
available_export_cols = [c for c in cols_to_export if c in export_df.columns]
export_df = export_df[available_export_cols]

# Asegurar enteros en exportación
int_cols_export = [c for c in ["licenseid", "approvedshelflife"] if c in export_df.columns]
for c in int_cols_export:
    export_df[c] = pd.to_numeric(export_df[c], errors="coerce").astype("Int64")

# Si hay licensenumber, ocultar licenseid en export
if "licensenumber" in export_df.columns and "licenseid" in export_df.columns:
    export_df = export_df.drop(columns=["licenseid"])

csv_bytes = export_df.to_csv(index=False).encode("utf-8")
st.download_button("Download CSV (filtered)", data=csv_bytes, file_name="rim_filtered_export.csv", mime="text/csv")

# Excel export (autodetección de engine)
excel_engine = None
try:
    import xlsxwriter  # type: ignore
    excel_engine = "xlsxwriter"
except Exception:
    try:
        import openpyxl  # type: ignore
        excel_engine = "openpyxl"
    except Exception:
        excel_engine = None

if excel_engine:
    buf = BytesIO()
    with pd.ExcelWriter(buf, engine=excel_engine) as writer:
        export_df.to_excel(writer, index=False, sheet_name="data")
        # Ajuste de ancho de columnas (solo con xlsxwriter)
        if excel_engine == "xlsxwriter":
            for i, col in enumerate(export_df.columns):
                width = min(max(10, export_df[col].astype(str).str.len().max()), 50)
                writer.sheets["data"].set_column(i, i, width)

    st.download_button(
        "Download Excel (filtered)",
        data=buf.getvalue(),
        file_name="rim_filtered_export.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )
else:
    st.info("Excel export deshabilitado (instala 'xlsxwriter' o 'openpyxl' en tu entorno para habilitarlo).")
    st.code("conda install -c conda-forge xlsxwriter\n# o\nconda install -c conda-forge openpyxl", language="bash")

st.caption("Exports are capped to prevent browser crashes. Adjust 'max_export' in the code if needed.")
