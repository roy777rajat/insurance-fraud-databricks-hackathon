# Databricks notebook source
# MAGIC %md
# MAGIC ## 04_train_and_score
# MAGIC #### This notebook creates a fraud score for each claim and assigns a decision flag.  
# MAGIC #### It supports two modes:
# MAGIC - **Supervised model** if labels exist (Y/N)
# MAGIC - **Unsupervised anomaly model** if labels are missing or one-class

# COMMAND ----------

# MAGIC %md
# MAGIC ### Import & Setup

# COMMAND ----------

import numpy as np
import pandas as pd
from pyspark.sql.functions import col
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import OneHotEncoder
from sklearn.ensemble import GradientBoostingClassifier, IsolationForest
from sklearn.metrics import roc_auc_score

catalog = "workspace"
schema  = "insurance_fraud"
GOLD   = f"`{catalog}`.`{schema}`.`gold_claim_features`"
SCORED = f"`{catalog}`.`{schema}`.`gold_scored_claims`"

# COMMAND ----------

# MAGIC %md
# MAGIC ### Load feature table
# MAGIC #### Convert the Delta table into a pandas DataFrame so we can train using scikit-learn.
# MAGIC

# COMMAND ----------

# -------- load to pandas --------
df = spark.table(GOLD).toPandas()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Clean fraud label
# MAGIC #### Normalize variations like Y/Yes/TRUE → 1 and N/No/FALSE → 0.  
# MAGIC #### Drop rows where the label cannot be determined.
# MAGIC

# COMMAND ----------

# -------- robust label mapping --------
# handle common variants: Y/Yes/TRUE/1 vs N/No/FALSE/0
val = (df["fraud_reported"].astype(str).str.strip().str.lower())
df["label"] = np.where(val.isin(["y","yes","true","1"]), 1,
                np.where(val.isin(["n","no","false","0"]), 0, np.nan))

# drop rows with unknown label for supervised path
df_sup = df.dropna(subset=["label"]).copy()
df_sup["label"] = df_sup["label"].astype(int)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Select model features
# MAGIC ### Split features into:
# MAGIC ###- Categorical columns (will be one-hot encoded)
# MAGIC ###- Numeric columns (will be median-filled for missing values)
# MAGIC

# COMMAND ----------

# feature sets
cat_cols = ["policy_type","vehicle_segment","incident_type","police_report_available","injury_severity","state"]
num_cols = ["claim_delay_days","incident_month","total_claim_amount","repair_estimate","policy_annual_mileage","pin_valid"]

# simple NA handling for numerics
for c in num_cols:
    if c in df_sup.columns:
        df_sup[c] = pd.to_numeric(df_sup[c], errors="coerce").fillna(df_sup[c].median())

# cast categoricals to string (OneHotEncoder expects strings)
X_cat = df_sup[cat_cols].astype(str)
X_num = df_sup[num_cols].astype(float)
y = df_sup["label"].values

# COMMAND ----------

# MAGIC %md
# MAGIC ### Check if both fraud classes exist
# MAGIC ### If dataset has at least one fraud (1) and one non-fraud (0), we can train supervised.  
# MAGIC ### Otherwise, fallback to unsupervised anomaly detection.
# MAGIC

# COMMAND ----------

# class distribution
pos = int((y == 1).sum())
neg = int((y == 0).sum())
print(f"Label counts (supervised candidates): positives={pos}, negatives={neg}, total={len(y)}")

def write_scores_back(pdf_scores: pd.DataFrame):
    scored_spark = spark.createDataFrame(pdf_scores[["claim_id","fraud_score","decision_flag"]])
    (scored_spark.write
        .format("delta")
        .mode("overwrite")
        .option("overwriteSchema","true")
        .saveAsTable(SCORED))
    display(scored_spark.orderBy(col("fraud_score").desc()).limit(20))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Supervised Model (Gradient Boosting)
# MAGIC ### Train a predictive model using labeled data.
# MAGIC ### Evaluate and then score all records.
# MAGIC

# COMMAND ----------


# ---------- Path A: Supervised GBDT if we truly have both classes ----------
if pos >= 1 and neg >= 1:
    print("Training supervised GradientBoostingClassifier (stratified split).")
    # encode categoricals
    enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    X_cat_enc = enc.fit_transform(X_cat)

    X = np.hstack([X_cat_enc, X_num.values])

    # stratified split to keep both classes in train/test
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=11, stratify=y
    )

    clf = GradientBoostingClassifier(max_depth=3, n_estimators=140, learning_rate=0.08, random_state=42)
    clf.fit(X_train, y_train)

    # Evaluate AUC (guard if a tiny split still lands single-class)
    try:
        auc = roc_auc_score(y_test, clf.predict_proba(X_test)[:,1])
        print("AUC:", round(auc, 4))
    except ValueError as e:
        print("AUC unavailable (test set single class). Proceeding without AUC. Details:", e)

    # Score all rows (including unlabeled ones)
    # Build full feature matrix for all rows reusing the same encoder and numeric handling
    df_all = df.copy()
    for c in num_cols:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce").fillna(df_sup[c].median() if c in df_sup.columns else 0.0)
    X_all = np.hstack([
        enc.transform(df_all[cat_cols].astype(str)),
        df_all[num_cols].astype(float).values
    ])
    df_all["fraud_score"] = clf.predict_proba(X_all)[:,1]
    df_all["decision_flag"] = np.where(df_all["fraud_score"]>=0.70, "REVIEW",
                                np.where(df_all["fraud_score"]>=0.50, "WATCH", "PASS"))
    write_scores_back(df_all)

# ---------- Path B: Unsupervised fallback (IsolationForest) ----------
else:
    print("Only one class present or no clean labels. Using unsupervised IsolationForest as fallback.")
    # Build features from ALL records (no label needed)
    df_all = df.copy()
    # numeric fixes
    for c in num_cols:
        df_all[c] = pd.to_numeric(df_all[c], errors="coerce")
        df_all[c] = df_all[c].fillna(df_all[c].median())
    # encode categoricals
    enc = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    X_cat_enc = enc.fit_transform(df_all[cat_cols].astype(str))
    X_all = np.hstack([X_cat_enc, df_all[num_cols].astype(float).values])

    # IsolationForest: higher anomaly_score -> more anomalous
    iso = IsolationForest(n_estimators=200, contamination="auto", random_state=42)
    iso.fit(X_all)
    raw = -iso.score_samples(X_all)  # larger means more anomalous
    # min-max to [0,1] as "fraud_score"
    s = (raw - raw.min()) / (raw.max() - raw.min() + 1e-9)
    df_all["fraud_score"] = s
    df_all["decision_flag"] = np.where(df_all["fraud_score"]>=0.70, "REVIEW",
                                np.where(df_all["fraud_score"]>=0.50, "WATCH", "PASS"))
    write_scores_back(df_all)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Add Rule 
# MAGIC This step adds clear human-readable **explanations** for why a claim received its fraud score.
# MAGIC
# MAGIC We join:
# MAGIC - The **scored claims** table (fraud_score + decision_flag)
# MAGIC - The **feature** table (claim attributes)
# MAGIC
# MAGIC Then we check simple rule signals (examples):
# MAGIC - Claim reported late
# MAGIC - High repair estimate
# MAGIC - Low mileage vs high cost
# MAGIC - Invalid address PIN
# MAGIC - Severe injury reported
# MAGIC - Missing police report
# MAGIC
# MAGIC Each triggered signal is collected into a list and combined into a **single explanation** text field.
# MAGIC
# MAGIC The result is written to a new table: `gold_scored_claims_explained`.  
# MAGIC Dashboard and RAG Q&A will use this explanation field to provide insights.
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import col, when, concat_ws, array, lit, expr

catalog = "workspace"
schema  = "insurance_fraud"
GOLD    = f"`{catalog}`.`{schema}`.`gold_claim_features`"
SCORED  = f"`{catalog}`.`{schema}`.`gold_scored_claims`"
EXPLAIN = f"`{catalog}`.`{schema}`.`gold_scored_claims_explained`"

df_features = spark.table(GOLD)
df_scores   = spark.table(SCORED)

# Join and assign explanation signals
df_exp = (
    df_features.join(df_scores, "claim_id")
    .withColumn("r_delay", when(col("claim_delay_days") > 10, lit("Claim reported late")))
    .withColumn("r_high_repair", when(col("repair_estimate") > 20000, lit("High repair estimate")))
    .withColumn("r_low_mileage_high_cost",
                when((col("policy_annual_mileage") < 3000) & (col("repair_estimate") > 15000),
                     lit("Low mileage but high value claim")))
    .withColumn("r_pin_invalid", when(col("pin_valid") == lit(False), lit("Address PIN does not match official registry")))
    .withColumn("r_injury_major",
                when(col("injury_severity").isin("Major", "Extreme"), lit("Severe injury claim reported")))
    .withColumn("r_police_missing",
                when(col("police_report_available") == "NO", lit("No police report filed")))
    .withColumn("reasons_array",
                array("r_delay","r_high_repair","r_low_mileage_high_cost",
                      "r_pin_invalid","r_injury_major","r_police_missing"))
    .withColumn(
        "explanation",
        expr("""
            aggregate(
                reasons_array,
                '',
                (acc, x) -> CASE WHEN x IS NOT NULL THEN
                    CASE WHEN acc = '' THEN x ELSE concat(acc, '; ', x) END
                ELSE acc END
            )
        """)
    )
    .select("claim_id","fraud_score","decision_flag","explanation")
)


df_exp.write.format("delta").mode("overwrite").option("overwriteSchema","true").saveAsTable(EXPLAIN)

display(df_exp.orderBy(col("fraud_score").desc()).limit(20))
