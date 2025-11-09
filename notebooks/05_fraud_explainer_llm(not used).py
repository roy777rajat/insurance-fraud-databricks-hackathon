# Databricks notebook source
# MAGIC %md
# MAGIC ### 05_fraud_explainer_llm — open model explains "why this claim"

# COMMAND ----------

from pyspark.sql.functions import col
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from mlflow.deployments import get_deploy_client

CATALOG = "insurance_fraud"
SCORED = f"{CATALOG}.gold_scored_claims"
GOLD   = f"{CATALOG}.gold_claim_features"
OUT    = f"{CATALOG}.gold_scored_claims_explained"

# Change if not available in your region
MODEL_NAME = "databricks-llama-2-7b-chat"
client = get_deploy_client("databricks")

def explain_claim(claim_id, fraud_score, region_risk_bucket, pin_valid, claim_delay_days, total_claim_amount, incident_type, police_report_available):
    prompt = f"""
You are an Indian motor insurance fraud analyst.
Explain briefly (<= 120 words) why this claim may be risky. Use only the provided fields.

Claim ID: {claim_id}
Fraud Score: {fraud_score:.3f}
Region Risk Bucket: {region_risk_bucket}
PIN Valid: {pin_valid}
Claim Delay Days: {claim_delay_days}
Total Claim Amount (INR): {total_claim_amount}
Incident Type: {incident_type}
Police Report Available: {police_report_available}

Output only the explanation sentence(s).
"""
    try:
        resp = client.predict(endpoint=MODEL_NAME, inputs=prompt)
        if isinstance(resp, dict):
            pred = resp.get("predictions") or resp.get("choices") or resp.get("data")
            if isinstance(pred, list):
                return str(pred[0])
            return str(resp)
        return str(resp)
    except Exception as e:
        return f"Explanation unavailable: {e}"

explain_udf = udf(explain_claim, StringType())

s = spark.table(SCORED)
g = spark.table(GOLD).select("claim_id","region_risk_bucket","pin_valid","claim_delay_days","total_claim_amount","incident_type","police_report_available")
j = s.join(g, "claim_id")

j = j.withColumn("fraud_explanation",
                 explain_udf(col("claim_id"),
                             col("fraud_score"),
                             col("region_risk_bucket"),
                             col("pin_valid"),
                             col("claim_delay_days"),
                             col("total_claim_amount"),
                             col("incident_type"),
                             col("police_report_available")))

j.write.format("delta").mode("overwrite").saveAsTable(OUT)

display(spark.table(OUT).limit(10))