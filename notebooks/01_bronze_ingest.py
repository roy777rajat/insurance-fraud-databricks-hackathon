# Databricks notebook source
# MAGIC %md
# MAGIC #### Databricks notebook source
# MAGIC #### 01_bronze_ingest — Auto-Detect CSV or Promote From Staging
# MAGIC ##This notebook will:
# MAGIC ###### (1) Try an explicit `INPUT_PATH` (if provided)  
# MAGIC ###### (2) Else promote from a staging table (e.g., `default.tmp_claims_upload`) created by the UI  
# MAGIC ###### (3) Else auto-discover the **latest CSV** under `dbfs:/FileStore/tables/`  
# MAGIC ###### (4) Write to **Delta** table `insurance_fraud.bronze_claims`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Import and Widget Setup

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.utils import AnalysisException
import re

dbutils.widgets.text("INPUT_PATH", "")
dbutils.widgets.text("STAGING_TABLE", "default.tmp_claims_upload")
dbutils.widgets.text("FILENAME_HINT", "insurance_claims")
dbutils.widgets.dropdown("WRITE_MODE", "overwrite", ["overwrite","append"])
dbutils.widgets.dropdown("DROP_STAGING_AFTER", "false", ["true","false"])

INPUT_PATH = dbutils.widgets.get("INPUT_PATH").strip()
STAGING_TABLE = dbutils.widgets.get("STAGING_TABLE").strip()
FILENAME_HINT = dbutils.widgets.get("FILENAME_HINT").strip().lower()
WRITE_MODE = dbutils.widgets.get("WRITE_MODE")
DROP_STAGING = dbutils.widgets.get("DROP_STAGING_AFTER") == "true"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Set Catlog and Bronze table

# COMMAND ----------

CATALOG = "insurance_fraud"
BRONZE  = f"{CATALOG}.bronze_claims"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Check values

# COMMAND ----------

print(f"INPUT_PATH={INPUT_PATH or '(auto)'}")
print(f"STAGING_TABLE={STAGING_TABLE}")
print(f"FILENAME_HINT={FILENAME_HINT}")
print(f"WRITE_MODE={WRITE_MODE}, DROP_STAGING_AFTER={DROP_STAGING}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Dynamically determine the source content (path based, delta table)
# MAGIC #### Method : 
# MAGIC ##### (a)_try_read_csv : If path valid and exist, then read from there
# MAGIC ##### (b)_table_exists & _read_staging_table : Feth from Delta table (temp, here the table name we used : default.tmp_claims_upload)
# MAGIC ##### (c)_discover_latest_csv : If a new .csv landed, pick it up

# COMMAND ----------


def _try_read_csv(path: str):
    """Try reading a CSV quickly to validate path."""
    try:
        df = spark.read.csv(path, header=True, inferSchema=True)
        _ = df.limit(1).count()
        print(f"Read OK: {path}")
        return df
    except Exception as e:
        print(f"Skipping {path}: {e}")
        return None

def _table_exists(fullname: str) -> bool:
    try:
        return spark.catalog.tableExists(fullname)
    except:
        return False

def _read_staging_table(name: str):
    try:
        df = spark.table(name)
        _ = df.limit(1).count()
        print(f"Using staging table: {name}")
        return df
    except AnalysisException as e:
        print(f"Staging table not found: {name} ({e})")
        return None
    except Exception as e:
        print(f"Could not read staging table {name}: {e}")
        return None

def _discover_latest_csv(hint: str):
    base = "dbfs:/FileStore/tables/"
    try:
        files = dbutils.fs.ls(base)
    except Exception as e:
        print(f"Could not list {base}: {e}")
        return None

    # pick CSVs, newest first
    csvs = [f for f in files if f.path.lower().endswith(".csv")]
    if hint:
        csvs = [f for f in csvs if hint in f.path.lower()]
    csvs = sorted(csvs, key=lambda x: x.modificationTime, reverse=True)

    for f in csvs[:20]:
        print(f"Checking: {f.path} ({f.size} bytes)")
        df = _try_read_csv(f.path)
        if df is not None:
            return df
    return None

# COMMAND ----------

# MAGIC %md
# MAGIC #### Load into Bronze table

# COMMAND ----------


source_df = None
source_desc = None

# 1) Explicit path
if INPUT_PATH:
    source_df = _try_read_csv(INPUT_PATH)
    source_desc = f"CSV: {INPUT_PATH}"

# 2) Staging table (from UI Create Table) if not found yet
if source_df is None and _table_exists(STAGING_TABLE):
    source_df = _read_staging_table(STAGING_TABLE)
    source_desc = f"STAGING TABLE: {STAGING_TABLE}"

# 3) Auto-discover latest CSV from FileStore
if source_df is None:
    source_df = _discover_latest_csv(FILENAME_HINT)
    source_desc = "AUTO CSV from dbfs:/FileStore/tables/"

assert source_df is not None, (
    " - Could not locate any input. Do one of the following:\n"
    " - Set an explicit INPUT_PATH widget (dbfs:/...)\n"
    " - Use the UI 'Create table' and set STAGING_TABLE correctly (e.g., default.tmp_claims_upload)\n"
    " - Upload to Data → Add data (FileStore), then re-run with FILENAME_HINT adjusted"
)

print(f"Source chosen: {source_desc}")

# Light sanity: required columns check (non-fatal, just warn if missing)
expected_cols = {
    "claim_id"
    ,"incident_date"
    ,"claim_reported"
    ,"total_claim_amount"
    ,"policy_annual_mileage",
    "insured_zip"
    ,"fraud_reported"
}
missing = [c for c in expected_cols if c not in map(str.lower, source_df.columns)]
if missing:
    print(f"Warning: expected column names (case-insensitive) not fully present: {missing}")


# Write to Bronze (Delta)
source_df.write.format("delta").mode(WRITE_MODE).saveAsTable(BRONZE)

print(f"Wrote to {BRONZE} with mode={WRITE_MODE}")

# Optional cleanup of staging table
if source_desc.startswith("STAGING TABLE") and DROP_STAGING:
    try:
        spark.sql(f"DROP TABLE IF EXISTS {STAGING_TABLE}")
        print(f"Dropped staging table: {STAGING_TABLE}")
    except Exception as e:
        print(f"Could not drop staging table {STAGING_TABLE}: {e}")

# Show a peek
display(spark.table(BRONZE).limit(20))

# Also print counts for confidence
#print("Rows in bronze:", spark.table(BRONZE).count())