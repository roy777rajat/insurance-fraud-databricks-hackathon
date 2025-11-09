# Databricks notebook source
# MAGIC %md
# MAGIC #### Databricks notebook source
# MAGIC ### 02_silver_clean — normalize dates, add derived fields (Silver)
# MAGIC ## What it is?:
# MAGIC ###### (1) Remove duplicates if any duplicate claim data present (uniques are : claim_id)
# MAGIC ###### (2) Store clean data into 'insurance_fraud.silver_claim'

# COMMAND ----------

from pyspark.sql import functions as F

CATALOG = "insurance_fraud"
BRONZE = f"{CATALOG}.bronze_claims"
SILVER = f"{CATALOG}.silver_claims"

b = spark.table(BRONZE)

s = (b
     .withColumn("incident_date", F.to_date("incident_date","MM/dd/yyyy"))
     .withColumn("claim_report_date", F.to_date("claim_reported","MM/dd/yyyy"))
     .withColumn("incident_month", F.month("incident_date"))
     .withColumn("claim_delay_days", F.datediff(F.col("claim_report_date"), F.col("incident_date")))
     .dropDuplicates(["claim_id"])
    )

s.write.format("delta").mode("overwrite").saveAsTable(SILVER)

display(spark.table(SILVER).limit(10))