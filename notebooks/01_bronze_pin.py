# Databricks notebook source
# MAGIC %md
# MAGIC #### Databricks notebook source
# MAGIC #### 01_bronze_pin — Auto-Detect CSV or Promote From Staging
# MAGIC ## What it is?:
# MAGIC ###### (1) This is a master data for various Pin code for different states for teh country India
# MAGIC ###### (2) This .csv uploaded and set in the temporary table default.pin_master  
# MAGIC ###### (3) From default.pin_master delta table to store default.bronze_pin after cleaning the data (cast,pin code length check, dedup)` 

# COMMAND ----------

# MAGIC %md
# MAGIC ### Default Catlog, Schema and Source Table

# COMMAND ----------

catalog = "workspace"                 # default catalog
schema  = "insurance_fraud"
src_tbl = "default.pin_master"

target_fqn = f"`{catalog}`.`{schema}`.`bronze_pin`"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Ensure schema exists

# COMMAND ----------

# 1) Ensure schema exists
spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Temporary View Creation

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_pin_raw AS
SELECT
  CAST(pin AS STRING)      AS pin_raw,
  CAST(district AS STRING) AS district_raw,
  CAST(state AS STRING)    AS state_raw
FROM {src_tbl}
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW v_pin_clean AS
SELECT
  REGEXP_REPLACE(LOWER(pin_raw), '[^0-9]', '') AS pin,
  INITCAP(TRIM(district_raw))                  AS district,
  INITCAP(TRIM(state_raw))                     AS state
FROM v_pin_raw
WHERE pin_raw IS NOT NULL
  AND LENGTH(REGEXP_REPLACE(LOWER(pin_raw), '[^0-9]', '')) = 6
""")

spark.sql("""
CREATE OR REPLACE TEMP VIEW v_pin_dedup AS
SELECT pin, district, state
FROM (
  SELECT
    pin, district, state,
    ROW_NUMBER() OVER (PARTITION BY pin ORDER BY pin) AS rn
  FROM v_pin_clean
)
WHERE rn = 1
""")


# COMMAND ----------

# MAGIC %md
# MAGIC ### Persist using CREATE OR REPLACE TABLE ... AS SELECT

# COMMAND ----------

# 3) Persist using CREATE OR REPLACE TABLE ... AS SELECT
spark.sql(f"""
CREATE OR REPLACE TABLE {target_fqn}
USING DELTA
AS
SELECT
  pin,
  district,
  state,
  current_timestamp() AS snapshot_ts
FROM v_pin_dedup
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ###  Sanity checks

# COMMAND ----------

# 4) Quick sanity checks
display(spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`"))
display(spark.sql(f"SELECT COUNT(*) AS rows, COUNT(DISTINCT pin) AS distinct_pins FROM {target_fqn}"))