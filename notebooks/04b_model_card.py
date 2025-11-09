# Databricks notebook source
# MAGIC %md
# MAGIC # 04b_model_card — Model Evaluation Summary
# MAGIC
# MAGIC This notebook creates a **simple Model Card** to record how well our fraud scoring model is performing.
# MAGIC
# MAGIC ### What this Notebook Does
# MAGIC 1. **Loads Data**
# MAGIC    - Reads the *gold claim features* (which contain the true fraud labels).
# MAGIC    - Reads the *scored claims* table (which contains model fraud scores).
# MAGIC
# MAGIC 2. **Cleans and Aligns Labels**
# MAGIC    - Converts `fraud_reported` to numeric labels:
# MAGIC      - `Y / Yes / True / 1` → **1 (fraud)**
# MAGIC      - `N / No / False / 0` → **0 (not fraud)**
# MAGIC    - Ignores rows where the label is unclear.
# MAGIC
# MAGIC 3. **Calculates Key Metrics**
# MAGIC    - **Positive vs Negative counts** (helps understand class balance).
# MAGIC    - **AUC (Area Under ROC Curve)** — measures how well the model separates fraud vs. non-fraud.
# MAGIC    - **Precision@50** — among the *top 50 highest-risk* claims, what % are truly fraud.
# MAGIC    - **Precision@100** — same idea, but top 100 claims.
# MAGIC
# MAGIC    These metrics are important because in real insurance fraud operations, analysts usually investigate only the **top suspected cases**, not every claim.
# MAGIC
# MAGIC 4. **Writes Metrics to a Model Card Table**
# MAGIC    - Appends one row per model run to:
# MAGIC      ```
# MAGIC      workspace.insurance_fraud.model_card_gbt
# MAGIC      ```
# MAGIC    - This allows **tracking model quality over time**, especially if the model is retrained later.
# MAGIC
# MAGIC 5. **Displays the Most Recent Results**
# MAGIC    - Shows latest runs sorted by timestamp so you can monitor improvements or regressions.
# MAGIC
# MAGIC ### Why This Matters in Hackathon
# MAGIC - Demonstrates **ML evaluation discipline**, not just scoring.
# MAGIC - Enables **transparent comparison** if you tweak features or model parameters later.
# MAGIC - Provides a **simple & professional artifact** your judges can point to.
# MAGIC
# MAGIC ### Output Table Contains:
# MAGIC | Column | Meaning |
# MAGIC |-------|---------|
# MAGIC | run_ts | When the evaluation was recorded |
# MAGIC | rows | Total scored claim rows |
# MAGIC | labeled_rows | Rows used to calculate metrics |
# MAGIC | positives / negatives | Fraud vs. Non-fraud counts |
# MAGIC | auc | Overall model discrimination power |
# MAGIC | precision_at_50 / precision_at_100 | Quality of top-ranked predictions |
# MAGIC | notes | Short description of which model/config produced these metrics |
# MAGIC
# MAGIC ---
# MAGIC

# COMMAND ----------

# 04b_model_card — Free Edition compatible
import numpy as np, pandas as pd
from sklearn.metrics import roc_auc_score, precision_score

catalog, schema = "workspace", "insurance_fraud"
GOLD   = f"`{catalog}`.`{schema}`.`gold_claim_features`"
SCORED = f"`{catalog}`.`{schema}`.`gold_scored_claims`"
CARD   = f"`{catalog}`.`{schema}`.`model_card_gbt`"

# 1) Load current scored data + labels for metrics
gold = spark.table(GOLD).toPandas()
scored = spark.table(SCORED).toPandas()

# robust label mapping
val = gold["fraud_reported"].astype(str).str.strip().str.lower()
y = np.where(val.isin(["y","yes","true","1"]), 1,
    np.where(val.isin(["n","no","false","0"]), 0, np.nan)).astype(float)

# join to align rows (by claim_id)
df = pd.merge(scored, gold[["claim_id","fraud_reported"]], on="claim_id", how="left")
y_card = np.where(val.isin(["y","yes","true","1"]), 1,
          np.where(val.isin(["n","no","false","0"]), 0, np.nan))

# 2) Metrics
pos = int(np.nansum(y == 1))
neg = int(np.nansum(y == 0))
total = int(np.nansum(~np.isnan(y)))
auc  = float(np.round(roc_auc_score(y[~np.isnan(y)], df.loc[~np.isnan(y), "fraud_score"]), 4))

# precision@k
def precision_at_k(scores, labels, k):
    order = np.argsort(scores)[::-1]
    topk = order[:k]
    return float(np.mean(labels[topk]))

labels_clean = y[~np.isnan(y)]
scores_clean = df.loc[~np.isnan(y), "fraud_score"].values
p_at_50  = precision_at_k(scores_clean, labels_clean, min(50, len(scores_clean)))
p_at_100 = precision_at_k(scores_clean, labels_clean, min(100, len(scores_clean)))

# 3) Assemble card row
from datetime import datetime, timezone
row = [{
  "run_ts": datetime.now(timezone.utc),
  "rows": int(len(df)),
  "labeled_rows": total,
  "positives": pos,
  "negatives": neg,
  "auc": auc,
  "precision_at_50": float(np.round(p_at_50,4)),
  "precision_at_100": float(np.round(p_at_100,4)),
  "notes": "GBT (sklearn), features: policy/vehicle/incident + delay + amounts + mileage + pin_valid"
}]
spark.createDataFrame(pd.DataFrame(row)).write.format("delta").mode("append").saveAsTable(CARD)

display(spark.table(CARD).orderBy("run_ts", ascending=False).limit(5))
