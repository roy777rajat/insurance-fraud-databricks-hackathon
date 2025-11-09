# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE OR REPLACE VIEW workspace.insurance_fraud.vw_claims_enriched_scored AS
# MAGIC SELECT
# MAGIC     f.claim_id,
# MAGIC     f.incident_date AS report_dt,   -- using incident_date as dashboard timeline
# MAGIC     f.state,                        -- already enriched during PIN join
# MAGIC     f.total_claim_amount,
# MAGIC     f.repair_estimate,
# MAGIC     f.policy_annual_mileage,
# MAGIC     CAST(f.pin_valid AS BOOLEAN) AS pin_valid,
# MAGIC     s.fraud_score,
# MAGIC     s.decision_flag,
# MAGIC     x.explanation
# MAGIC FROM workspace.insurance_fraud.gold_claim_features f
# MAGIC JOIN workspace.insurance_fraud.gold_scored_claims s
# MAGIC     ON f.claim_id = s.claim_id
# MAGIC LEFT JOIN workspace.insurance_fraud.gold_scored_claims_explained x
# MAGIC     ON f.claim_id = x.claim_id;
# MAGIC