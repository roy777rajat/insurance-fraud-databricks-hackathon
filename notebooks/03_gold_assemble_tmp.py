# Databricks notebook source
# MAGIC %md
# MAGIC #### Databricks notebook source
# MAGIC ### 03_gold_assemble_tmp — assemble interim Gold with REST fields
# MAGIC ## What it is?:
# MAGIC ###### (1) Taking data from 'silver_enriched' to 'gold_claim_feature_tmp'

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 : Set the configuration

# COMMAND ----------


catalog = "workspace"
schema  = "insurance_fraud"
src     = f"`{catalog}`.`{schema}`.`silver_enriched`"
goldtmp = f"`{catalog}`.`{schema}`.`gold_claim_features_tmp`"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2 : Build the Gold-temp straight from the enriched Silver

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {goldtmp}
USING DELTA
AS
SELECT
  e.claim_id,
  e.incident_date,
  /* keep provided month; if missing, compute from date */
  COALESCE(e.incident_month, month(e.incident_date)) AS incident_month,
  /* keep provided delay; if missing, compute from dates */
  COALESCE(e.claim_delay_days, DATEDIFF(e.claim_reported, e.incident_date)) AS claim_delay_days,
  e.total_claim_amount,
  e.repair_estimate,
  CAST(e.policy_annual_mileage AS INT) AS policy_annual_mileage,
  e.policy_type,
  e.vehicle_segment,
  e.incident_type,
  e.police_report_available,
  e.injury_severity,
  e.fraud_reported,
  e.pin_valid,
  e.pin_state   AS state,
  e.pin_district AS district,
  CAST(NULL AS STRING) AS region_risk_bucket
FROM {src} e
""")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 : Display and Quick Sanity

# COMMAND ----------

display(spark.table(goldtmp).limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Promote temp → final Gold (Idempotent)
# MAGIC #### This block:
# MAGIC ####1) Ensures the target schema exists  
# MAGIC ####2) Creates (or replaces) the final Gold table with an explicit schema  
# MAGIC ####3) INSERT OVERWRITEs data from `gold_claim_features_tmp` into `gold_claim_features`  
# MAGIC ####4) Shows a quick preview + row counts

# COMMAND ----------


from pyspark.sql.utils import AnalysisException

catalog = "workspace"
schema  = "insurance_fraud"
SRC     = f"`{catalog}`.`{schema}`.`gold_claim_features_tmp`"
TGT     = f"`{catalog}`.`{schema}`.`gold_claim_features`"
SCHEMAQ = f"`{catalog}`.`{schema}`"

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {SCHEMAQ}")


spark.sql("""
CREATE OR REPLACE TABLE workspace.insurance_fraud.gold_claim_features (
  claim_id STRING,
  incident_date DATE,
  incident_month INT,
  claim_delay_days INT,
  total_claim_amount DOUBLE,
  repair_estimate DOUBLE,
  policy_annual_mileage INT,
  policy_type STRING,
  vehicle_segment STRING,
  incident_type STRING,
  police_report_available STRING,
  injury_severity STRING,
  fraud_reported STRING,
  pin_valid BOOLEAN,
  state STRING,
  district STRING,
  region_risk_bucket STRING
) USING DELTA
""")
try:
    _ = spark.table(SRC).limit(1).count()
except AnalysisException as e:
    raise RuntimeError(f"Source temp table {SRC} not found. Make sure earlier steps created it.") from e

src_count = spark.table(SRC).count()
print(f"Source rows in {SRC}: {src_count}")


spark.sql(f"""
INSERT OVERWRITE TABLE {TGT}
SELECT
  claim_id,
  incident_date,
  incident_month,
  claim_delay_days,
  total_claim_amount,
  repair_estimate,
  policy_annual_mileage,
  policy_type,
  vehicle_segment,
  incident_type,
  police_report_available,
  injury_severity,
  fraud_reported,
  pin_valid,
  state,
  district,
  region_risk_bucket
FROM {SRC}
""")

tgt_df = spark.table(TGT)
tgt_count = tgt_df.count()
print(f"Promoted to {TGT}. Rows now: {tgt_count}")
display(tgt_df.limit(20))
