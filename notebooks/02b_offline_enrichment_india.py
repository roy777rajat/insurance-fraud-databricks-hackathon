# Databricks notebook source
# MAGIC %md
# MAGIC #### Databricks notebook source
# MAGIC ### 02_offline_enrichment_india — Pin code parse and join with CLaim Data and store in enriched silver layer (the claim data with PIN)
# MAGIC ## What it is?:
# MAGIC ###### (1) Normalize the claim data and Pin code data
# MAGIC ###### (2) Join with claim data

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 1 : Configuration

# COMMAND ----------

catalog = "workspace"
schema  = "insurance_fraud"
claims_fqn = f"`{catalog}`.`{schema}`.`silver_claims`"
pins_fqn   = f"`{catalog}`.`{schema}`.`bronze_pin`"
target_fqn = f"`{catalog}`.`{schema}`.`silver_enriched`"

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 2: Ensure target schema exists (idempotent)

# COMMAND ----------


spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 3 :
# MAGIC #### - Normalize claims + PIN
# MAGIC #### - Insured_zip is BIGINT → cast to string
# MAGIC #### - Keep only 6 digits; if not 6 after cleaning, set NULL so it won't match

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TEMP VIEW v_claims_norm AS
SELECT
  c.claim_id,
  c.policy_id,
  c.customer_id,
  c.policy_type,
  c.vehicle_segment,
  c.incident_type,
  c.incident_date,
  c.claim_reported,
  c.total_claim_amount,
  c.repair_estimate,
  c.police_report_available,
  c.injury_severity,
  c.policy_annual_mileage,
  /* PIN normalization from BIGINT → string, digits-only, expect 6 */
  CASE
    WHEN c.insured_zip IS NULL THEN NULL
    ELSE
      CASE
        WHEN LENGTH(REGEXP_REPLACE(CAST(c.insured_zip AS STRING), '[^0-9]', '')) = 6
          THEN REGEXP_REPLACE(CAST(c.insured_zip AS STRING), '[^0-9]', '')
        ELSE NULL
      END
  END AS pin_norm,
  INITCAP(c.insured_city)  AS insured_city,
  INITCAP(c.insured_state) AS insured_state,
  c.fraud_reported,
  c.claim_report_date,
  c.incident_month,
  c.claim_delay_days
FROM {claims_fqn} c
""")


# COMMAND ----------

# MAGIC %md
# MAGIC #### Step 4 : Enrichment Join -> silver_enriched Delta table

# COMMAND ----------


spark.sql(f"""
CREATE OR REPLACE TABLE {target_fqn}
USING DELTA
AS
SELECT
  cn.claim_id,
  cn.policy_id,
  cn.customer_id,
  cn.policy_type,
  cn.vehicle_segment,
  cn.incident_type,
  cn.incident_date,
  cn.claim_reported,
  cn.total_claim_amount,
  cn.repair_estimate,
  cn.police_report_available,
  cn.injury_severity,
  cn.policy_annual_mileage,
  cn.pin_norm                                  AS insured_pin,
  cn.insured_city,
  cn.insured_state,
  cn.fraud_reported,
  cn.claim_report_date,
  cn.incident_month,
  cn.claim_delay_days,
  CASE WHEN p.pin IS NOT NULL THEN true ELSE false END AS pin_valid,
  p.district AS pin_district,
  p.state    AS pin_state,
  CASE WHEN p.pin IS NOT NULL THEN 'master' ELSE 'unknown' END AS pin_source,
  current_timestamp() AS enriched_at
FROM v_claims_norm cn
LEFT JOIN {pins_fqn} p
  ON cn.pin_norm = p.pin
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ####Step 5 : Sanity Check

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN `{catalog}`.`{schema}`"))
display(spark.sql(f"SELECT COUNT(*) AS rows, SUM(CASE WHEN pin_valid THEN 1 ELSE 0 END) AS valid_pins FROM {target_fqn}"))