# Databricks notebook source
dbutils.widgets.text("Question", "Why were these claims flagged?")
dbutils.widgets.text("start_date", "2025-01-01")
dbutils.widgets.text("end_date", "2025-12-31")
dbutils.widgets.text("States", "")          # e.g. "Delhi,Uttar Pradesh"
dbutils.widgets.dropdown(
    "Decisions",
    "WATCH",
    ["WATCH", "REVIEW", "PASS"]
)

q            = dbutils.widgets.get("Question")
start_date   = dbutils.widgets.get("start_date")
end_date     = dbutils.widgets.get("end_date")
states_csv   = dbutils.widgets.get("States").strip()
decisions_csv= dbutils.widgets.get("Decisions").strip()


# COMMAND ----------

from pyspark.sql.functions import col, lower, regexp_replace, concat_ws, lit, expr

base = spark.table("workspace.insurance_fraud.vw_claims_enriched_scored") \
    .where((col("report_dt") >= lit(start_date)) & (col("report_dt") <= lit(end_date)))

if states_csv:
    states = [s.strip() for s in states_csv.split(",") if s.strip()]
    base = base.where(col("state").isin(states))

if decisions_csv:
    decisions = [s.strip() for s in decisions_csv.split(",") if s.strip()]
    base = base.where(col("decision_flag").isin(decisions))

# very simple keyword hit score
q_l = q.lower()
scored = base.select(
    "claim_id","report_dt","state","decision_flag","fraud_score",
    "repair_estimate","policy_annual_mileage","pin_valid","explanation"
).withColumn("hit",
    expr(f"""
      (CASE WHEN lower(state) LIKE '%{q_l}%' THEN 1 ELSE 0 END) +
      (CASE WHEN lower(decision_flag) LIKE '%{q_l}%' THEN 1 ELSE 0 END) +
      REGEXP_COUNT(lower(COALESCE(explanation,'')), '{q_l}')
    """)
)

topk = scored.orderBy(col("hit").desc(), col("fraud_score").desc(), col("report_dt").desc()).limit(12)
# join the rows into a context blob, filtering out None values
ctx_rows = [
    row for row in topk.selectExpr(
        """
        concat(
            '• claim_id=', claim_id,
            ' | date=', cast(report_dt as string),
            ' | state=', state,
            ' | decision=', decision_flag,
            ' | score=', cast(round(fraud_score,3) as string),
            ' | est=', cast(round(repair_estimate,2) as string),
            ' | pin_valid=', cast(pin_valid as string),
            CASE WHEN coalesce(explanation,'') <> '' THEN concat(' | note=', explanation) ELSE '' END
        ) as rowtxt
        """
    ).toPandas()["rowtxt"].tolist() if row is not None
]

context_blob = "\n".join(ctx_rows) if ctx_rows else "No matching rows."
#print(context_blob[:800])

# COMMAND ----------

import mlflow
from mlflow.deployments import get_deploy_client

endpoint = "databricks-meta-llama-3-3-70b-instruct"  # pick any served chat model you see as Ready
client = get_deploy_client("databricks")

prompt = f"""You are a fraud analyst assistant. Answer concisely (<=500 words) using only the context rows.
If you are not aware about the context,polyfill the answer with your best knowledge.
If asked for next actions, give 3 short bullets.
Always share Total Cliam Number, Little insight about the state, and any other relevant information.


Question: {q}

Context rows:
{context_blob}
"""

resp = client.predict(
    endpoint=endpoint,
    inputs={
        "messages": [
            {"role": "system", "content": "You are helpful and concise."},
            {"role": "user", "content": prompt}
        ],
        "max_tokens": 256,
        "temperature": 0.1,
    },
)
# responses differ by model; handle common shapes
try:
    ai_text = resp["choices"][0]["message"]["content"]
except Exception:
    ai_text = str(resp)

print(f"User: \n{q}\n")
print(f"AI: \n{ai_text}")

