# 🚗 Insurance Fraud Detection on Databricks

End-to-end **Motor Insurance Fraud Detection** pipeline built on the **Databricks Lakehouse** using **Delta Lake**, **scikit-learn**, and a **Fraud Command Center Dashboard** for claim investigators.

The workflow detects suspicious claims, explains *why* they look risky, and provides an **AI Q&A assistant** to support investigation decisions.

---

## 🎯 Problem Overview

Manual fraud review is slow and expensive. Our solution:

- Ingests and cleans claim data
- Enriches with **PIN / region quality signals**
- Trains a **fraud risk scoring model**
- Generates **human-readable investigation notes**
- Presents risk insights via a **live interactive dashboard**
- Allows **natural language Q&A** using a Databricks-served LLM

---

## 🧱 Data Pipeline (Bronze → Silver → Gold → ML → Dashboard)

| Step | Notebook | Output |
|---|---|---|
| Raw Ingest | `01_bronze_ingest.py` | `bronze_claims` (Delta) |
| Postal PIN Enrichment | `01_bronze_pin.py` | Valid / mismatch region signals |
| Cleaning + Feature Creation | `02_silver_clean.py` & `02b_offline_enrichment_india.py` | `silver_enriched_claims` |
| Feature Assembly | `03_gold_assemble_tmp.py` → `03c_promote_gold` | `gold_claim_features` |
| Model Training + Scoring | `04_train_and_score.py` | `gold_scored_claims` |
| Explainability Rules | `05_add_rule_explanations.py` | `gold_scored_claims_explained` |
| Final Dashboard View | `General_Output_view.sql` | `vw_claims_enriched_scored` |

---

## 📊 Fraud Command Center Dashboard

![Fraud Analytics Dashboard](./dashboard/Full-View.jpg)

The dashboard provides:

| Feature | Purpose |
|---|---|
| **Total & Flagged Claims** | Assess overall portfolio risk |
| **State & Decision Filters** | Regional fraud pattern discovery |
| **Fraud Score Distribution** | Evaluate model threshold alignment |
| **Claim Timeline Trends** | Detect sudden or coordinated spikes |
| **High-Risk Claim Table** | Immediate investigation entry point |

### Decision Thresholds
| Score Range | Action |
|---|---|
| `< 0.50` | PASS |
| `0.50 - 0.70` | WATCH |
| `≥ 0.70` | REVIEW |

---

## 🧾 Explainability Layer

Each scored claim receives a **human-interpretable explanation**, e.g.:

- *Claim reported late*
- *High repair estimate relative to mileage*
- *PIN mismatch with official registry*
- *Severe injury reported*
- *No police report filed*

Stored in: **`gold_scored_claims_explained`**

---

## 🤖 AI Q&A Assistant (Free Edition, No Vector DB)

```python
from mlflow.deployments import get_deploy_client
client = get_deploy_client("databricks")

resp = client.predict(
  endpoint="databricks-meta-llama-3-3-70b-instruct",
  inputs={"messages":[
      {"role":"system","content":"You are helpful and concise."},
      {"role":"user","content": prompt}
  ]}
)

print(resp["choices"][0]["message"]["content"])
```

**Example Response**
```
There are 11 claims. Several show late reporting and unusually high repair costs.
Most occur in Delhi, Telangana, and West Bengal.
```

---

## 📂 Repository Structure

```text
insurance-fraud-databricks-hackathon/
├── notebooks/
├── dashboard/
└── README.md
```

---

## 🚀 How to Run

1. Run notebooks **01 → 05**
2. Build dashboard in Databricks SQL
3. Use AI Q&A notebook for investigation summaries

---

## 🏁 Result

This solution demonstrates **practical, explainable fraud detection** with real investigation value.
