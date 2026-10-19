-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 11 · Workstream B — Recurring failures and root-cause investigation
-- MAGIC **Question:** which assets and equipment models generate repeat work, and what is the likely operational impact?
-- MAGIC **Manager action:** review maintenance history of repeat-failure assets; raise model-level issues with the OEM service partner.
-- MAGIC
-- MAGIC A **repeat symptom** (same asset, same symptom within 30 days) is evidence of a recurring problem.
-- MAGIC It is *not* a confirmed root cause: `root_cause` is only filled on ~55% of closed tickets and flagged
-- MAGIC confirmed/suspected. The views keep the two apart.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_asset_reliability AS
WITH corr AS (
  SELECT f.*, r.cutoff_date
  FROM fitco_gold.fact_maintenance_ticket f
  CROSS JOIN fitco_gold.report_params r
  WHERE f.intervention_type = 'Corrective' AND f.status_group <> 'Cancelled'
),
agg AS (
  SELECT
    asset_id,
    COUNT(*)                                                                      AS corrective_total,
    SUM(CASE WHEN opened_date >  date_add(cutoff_date, -30)  THEN 1 ELSE 0 END)   AS corrective_30d,
    SUM(CASE WHEN opened_date >  date_add(cutoff_date, -90)  THEN 1 ELSE 0 END)   AS corrective_90d,
    SUM(CASE WHEN opened_date >  date_add(cutoff_date, -365) THEN 1 ELSE 0 END)   AS corrective_365d,
    SUM(CASE WHEN is_repeat_30d THEN 1 ELSE 0 END)                                AS repeat_30d_tickets,
    SUM(CASE WHEN is_repeat_30d_same_symptom THEN 1 ELSE 0 END)                   AS repeat_same_symptom_30d,
    SUM(COALESCE(downtime_hours, 0))                                              AS downtime_hours,
    SUM(CASE WHEN root_cause IS NOT NULL AND root_cause_confirmed = 'Y' THEN 1 ELSE 0 END) AS confirmed_root_causes,
    MAX(opened_date)                                                              AS last_failure_date
  FROM corr
  GROUP BY asset_id
),
sym AS (
  SELECT asset_id, symptom, COUNT(*) AS n,
         ROW_NUMBER() OVER (PARTITION BY asset_id ORDER BY COUNT(*) DESC, symptom) AS rn
  FROM corr
  GROUP BY asset_id, symptom
),
cost AS (
  SELECT asset_id,
         SUM(CASE WHEN cost_class = 'REALISED' THEN line_net_eur ELSE 0 END) AS realised_cost_eur,
         SUM(CASE WHEN cost_class = 'PENDING'  THEN line_net_eur ELSE 0 END) AS pending_cost_eur
  FROM fitco_gold.fact_maintenance_cost
  GROUP BY asset_id
)
SELECT
  a.asset_id, a.club_id, c.club_name, c.country, a.asset_category, a.asset_type, a.manufacturer, a.model_code,
  a.criticality, a.life_stage, a.proactive_service_programme, a.age_years_at_cutoff, a.operating_status,
  COALESCE(g.corrective_total, 0)         AS corrective_total,
  COALESCE(g.corrective_30d, 0)           AS corrective_30d,
  COALESCE(g.corrective_90d, 0)           AS corrective_90d,
  COALESCE(g.corrective_365d, 0)          AS corrective_365d,
  COALESCE(g.repeat_30d_tickets, 0)       AS repeat_30d_tickets,
  COALESCE(g.repeat_same_symptom_30d, 0)  AS repeat_same_symptom_30d,
  COALESCE(g.downtime_hours, 0)           AS downtime_hours,
  COALESCE(g.confirmed_root_causes, 0)    AS confirmed_root_causes,
  g.last_failure_date,
  s.symptom                               AS top_symptom,
  COALESCE(k.realised_cost_eur, 0)        AS realised_cost_eur,
  COALESCE(k.pending_cost_eur, 0)         AS pending_cost_eur,
  COALESCE(g.corrective_total, 0) / NULLIF(a.exposure_years_in_window, 0) AS failures_per_year
FROM fitco_gold.dim_asset a
JOIN fitco_gold.dim_club c ON c.club_id = a.club_id
LEFT JOIN agg g  ON g.asset_id = a.asset_id
LEFT JOIN sym s  ON s.asset_id = a.asset_id AND s.rn = 1
LEFT JOIN cost k ON k.asset_id = a.asset_id;

-- COMMAND ----------

-- Equipment-model failure rates: exposes problem batches (e.g. one treadmill series failing far more than its peers)
CREATE OR REPLACE VIEW fitco_gold.v_model_failure_rates AS
WITH per_model AS (
  SELECT
    a.asset_category, a.manufacturer, a.model_code,
    COUNT(*)                                   AS assets,
    SUM(a.exposure_years_in_window)            AS asset_years,
    SUM(COALESCE(r.corrective_total, 0))       AS corrective_tickets,
    SUM(COALESCE(r.repeat_30d_tickets, 0))     AS repeat_30d_tickets,
    SUM(COALESCE(r.realised_cost_eur, 0))      AS realised_cost_eur
  FROM fitco_gold.dim_asset a
  LEFT JOIN fitco_gold.v_asset_reliability r ON r.asset_id = a.asset_id
  GROUP BY a.asset_category, a.manufacturer, a.model_code
),
per_cat AS (
  SELECT asset_category,
         SUM(corrective_tickets) / NULLIF(SUM(asset_years), 0) AS category_failures_per_asset_year
  FROM per_model
  GROUP BY asset_category
)
SELECT
  m.asset_category, m.manufacturer, m.model_code, m.assets, ROUND(m.asset_years, 1) AS asset_years,
  m.corrective_tickets, m.repeat_30d_tickets, m.realised_cost_eur,
  m.corrective_tickets / NULLIF(m.asset_years, 0)                                   AS failures_per_asset_year,
  c.category_failures_per_asset_year,
  (m.corrective_tickets / NULLIF(m.asset_years, 0)) / NULLIF(c.category_failures_per_asset_year, 0) AS failure_index_vs_category,
  m.repeat_30d_tickets * 1.0 / NULLIF(m.corrective_tickets, 0)                      AS repeat_30d_share,
  (m.assets >= 20)                                                                  AS sample_ok
FROM per_model m
JOIN per_cat c ON c.asset_category = m.asset_category;

-- COMMAND ----------

-- Symptom × root cause: how much of the cause information is confirmed vs merely suspected
CREATE OR REPLACE VIEW fitco_gold.v_symptom_root_cause AS
SELECT
  asset_category, symptom,
  COUNT(*)                                                                              AS corrective_tickets,
  SUM(CASE WHEN root_cause IS NULL THEN 1 ELSE 0 END)                                   AS no_root_cause_recorded,
  SUM(CASE WHEN root_cause IS NOT NULL AND root_cause_confirmed = 'Y' THEN 1 ELSE 0 END) AS confirmed,
  SUM(CASE WHEN root_cause IS NOT NULL AND root_cause_confirmed = 'N' THEN 1 ELSE 0 END) AS suspected,
  root_cause
FROM fitco_gold.fact_maintenance_ticket
WHERE intervention_type = 'Corrective' AND status_group = 'Closed'
GROUP BY asset_category, symptom, root_cause;