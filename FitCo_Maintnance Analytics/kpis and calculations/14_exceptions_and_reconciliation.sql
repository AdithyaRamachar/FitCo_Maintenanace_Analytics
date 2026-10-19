-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 14 · Workstream E — Exception monitoring and invoice reconciliation
-- MAGIC **Question:** what should a facility manager look at first today?
-- MAGIC Rules are deliberately simple and explainable. Each flag carries a reason, severity, supporting value, affected
-- MAGIC club/asset/supplier and the date generated. **The dashboard escalates; the manager decides the action.**
-- MAGIC
-- MAGIC | Flag | Rule |
-- MAGIC |---|---|
-- MAGIC | SLA breach | open corrective ticket older than its policy SLA (High if ≥3× SLA, Medium if ≥1.5×) |
-- MAGIC | Repeat failure | ≥3 corrective tickets on one asset in the trailing 30 days (High if critical asset or ≥5) |
-- MAGIC | Cost outlier | closed in last 90 days, committed cost above the category/type p95 **and** above 2× median (min 30 comparable tickets) |
-- MAGIC | Supplier deterioration | 90-day SLA ≥8pp below own history with z ≥ 2 and ≥30 recent tickets |
-- MAGIC | Preventive backlog | club with ≥25 overdue preventive tasks (High if ≥50) |
-- MAGIC | Financial mismatch | labour billed >10% above indexed contract rate (≥5 lines in 90 days); orphan invoice lines; closed external tickets with no invoice after 60 days |

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_ticket_cost_thresholds AS
WITH tc AS (
  SELECT ticket_id, SUM(CASE WHEN cost_class IN ('REALISED', 'PENDING') THEN line_net_eur ELSE 0 END) AS committed_cost_eur
  FROM fitco_gold.fact_maintenance_cost
  GROUP BY ticket_id
)
SELECT
  f.asset_category, f.intervention_type,
  COUNT(*)                                                       AS comparable_tickets,
  MEDIAN(tc.committed_cost_eur)                                  AS median_cost_eur,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY tc.committed_cost_eur) AS p95_cost_eur
FROM fitco_gold.fact_maintenance_ticket f
JOIN tc ON tc.ticket_id = f.ticket_id
WHERE f.status_group = 'Closed' AND tc.committed_cost_eur > 0
GROUP BY f.asset_category, f.intervention_type;

-- COMMAND ----------

-- Row-level anti-join: closed external corrective tickets with no invoice line after 60 days
CREATE OR REPLACE VIEW fitco_gold.v_uninvoiced_tickets AS
SELECT
  f.ticket_id, f.club_id, f.asset_id, f.asset_category, f.supplier_id, f.priority,
  f.opened_at, f.closed_at, CAST(f.closed_at AS DATE) AS closed_date
FROM fitco_gold.fact_maintenance_ticket f
CROSS JOIN fitco_gold.report_params r
LEFT JOIN (SELECT DISTINCT ticket_id FROM fitco_gold.fact_maintenance_cost) c ON c.ticket_id = f.ticket_id
WHERE f.intervention_type = 'Corrective' AND f.status_group = 'Closed' AND f.supplier_id IS NOT NULL
  AND f.closed_at IS NOT NULL
  AND CAST(f.closed_at AS DATE) < date_add(r.cutoff_date, -60)
  AND c.ticket_id IS NULL;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_reconciliation_summary AS
SELECT '1 Invoice lines received (Silver)' AS check_name, COUNT(*) AS line_count,
       SUM(CAST(line_net_eur AS DOUBLE)) AS net_eur
FROM fitco_silver.invoice_lines
UNION ALL
SELECT '2 Exact duplicate lines excluded', COUNT(*), SUM(CAST(line_net_eur AS DOUBLE))
FROM fitco_silver.invoice_lines WHERE is_duplicate_line
UNION ALL
SELECT '3 Lines referencing unknown tickets (orphans) excluded', COUNT(*), SUM(CAST(line_net_eur AS DOUBLE))
FROM fitco_silver.invoice_lines WHERE is_orphan_ticket AND NOT is_duplicate_line
UNION ALL
SELECT '4 Valid lines in Gold', COUNT(*), SUM(line_net_eur) FROM fitco_gold.fact_maintenance_cost
UNION ALL
SELECT '5 Realised cost (approved + credit notes)', COUNT(*), SUM(line_net_eur)
FROM fitco_gold.fact_maintenance_cost WHERE cost_class = 'REALISED'
UNION ALL
SELECT '6 Pending approval (accrual)', COUNT(*), SUM(line_net_eur)
FROM fitco_gold.fact_maintenance_cost WHERE cost_class = 'PENDING'
UNION ALL
SELECT '7 Disputed', COUNT(*), SUM(line_net_eur)
FROM fitco_gold.fact_maintenance_cost WHERE cost_class = 'DISPUTED'
UNION ALL
SELECT '8 Rejected', COUNT(*), SUM(line_net_eur)
FROM fitco_gold.fact_maintenance_cost WHERE cost_class = 'REJECTED'
UNION ALL
SELECT '9 Closed external tickets with no invoice after 60 days', COUNT(*), CAST(NULL AS DOUBLE)
FROM fitco_gold.v_uninvoiced_tickets;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_exceptions AS
WITH sla_breach AS (
  SELECT
    'SLA breach' AS flag_type,
    CASE WHEN b.sla_overrun_ratio >= 3 THEN 'High' WHEN b.sla_overrun_ratio >= 1.5 THEN 'Medium' ELSE 'Low' END AS severity,
    b.club_id, b.club_name, b.asset_id, b.supplier_id, b.ticket_id,
    CONCAT(b.priority, ' ticket open ', CAST(b.open_age_days AS STRING), ' days vs SLA of ',
           CAST(ROUND(b.sla_target_hours / 24.0, 1) AS STRING), ' days: ', b.symptom) AS reason,
    CAST(b.open_age_hours AS DOUBLE) AS supporting_value, 'hours open' AS supporting_unit,
    CAST(NULL AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.v_backlog_open b
  WHERE b.is_past_sla
),
repeat_failure AS (
  SELECT
    'Repeat failure' AS flag_type,
    CASE WHEN a.criticality = 'High' OR a.corrective_30d >= 5 THEN 'High' ELSE 'Medium' END AS severity,
    a.club_id, a.club_name, a.asset_id, CAST(NULL AS STRING) AS supplier_id, CAST(NULL AS STRING) AS ticket_id,
    CONCAT(CAST(a.corrective_30d AS STRING), ' corrective tickets in 30 days on ', a.asset_type, ' (', a.model_code,
           '); top symptom: ', COALESCE(a.top_symptom, 'n/a')) AS reason,
    CAST(a.corrective_30d AS DOUBLE) AS supporting_value, 'tickets in 30d' AS supporting_unit,
    CAST(NULL AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.v_asset_reliability a
  WHERE a.corrective_30d >= 3
),
cost_outlier AS (
  SELECT
    'Cost outlier' AS flag_type,
    CASE WHEN tc.committed_cost_eur >= 2 * th.p95_cost_eur THEN 'High' ELSE 'Medium' END AS severity,
    f.club_id, c.club_name, f.asset_id, f.supplier_id, f.ticket_id,
    CONCAT(f.asset_category, ' ', LOWER(f.intervention_type), ' ticket cost EUR ', CAST(ROUND(tc.committed_cost_eur, 0) AS STRING),
           ' vs median EUR ', CAST(ROUND(th.median_cost_eur, 0) AS STRING), ' (p95 EUR ', CAST(ROUND(th.p95_cost_eur, 0) AS STRING), ')') AS reason,
    CAST(tc.committed_cost_eur AS DOUBLE) AS supporting_value, 'EUR' AS supporting_unit,
    CAST(tc.committed_cost_eur - th.median_cost_eur AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.fact_maintenance_ticket f
  JOIN (SELECT ticket_id, SUM(CASE WHEN cost_class IN ('REALISED','PENDING') THEN line_net_eur ELSE 0 END) AS committed_cost_eur
        FROM fitco_gold.fact_maintenance_cost GROUP BY ticket_id) tc ON tc.ticket_id = f.ticket_id
  JOIN fitco_gold.v_ticket_cost_thresholds th ON th.asset_category = f.asset_category AND th.intervention_type = f.intervention_type
  JOIN fitco_gold.dim_club c ON c.club_id = f.club_id
  CROSS JOIN fitco_gold.report_params r
  WHERE f.status_group = 'Closed' AND CAST(f.closed_at AS DATE) > date_add(r.cutoff_date, -90)
    AND th.comparable_tickets >= 30
    AND tc.committed_cost_eur > th.p95_cost_eur AND tc.committed_cost_eur > 2 * th.median_cost_eur
),
supplier_det AS (
  SELECT
    'Supplier deterioration' AS flag_type,
    CASE WHEN d.drop_pp >= 0.15 THEN 'High' ELSE 'Medium' END AS severity,
    CAST(NULL AS STRING) AS club_id, CAST(NULL AS STRING) AS club_name, CAST(NULL AS STRING) AS asset_id,
    d.supplier_id, CAST(NULL AS STRING) AS ticket_id,
    CONCAT(d.supplier_name, ': SLA compliance ', CAST(ROUND(d.sla_recent * 100, 1) AS STRING), '% in last 90 days vs ',
           CAST(ROUND(d.sla_baseline * 100, 1) AS STRING), '% before (z=', CAST(ROUND(d.z_score, 1) AS STRING), ')') AS reason,
    CAST(d.drop_pp * 100 AS DOUBLE) AS supporting_value, 'pp SLA drop' AS supporting_unit,
    CAST(NULL AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.v_supplier_deterioration d
  WHERE d.is_deteriorating
),
pm_backlog AS (
  SELECT
    'Preventive backlog' AS flag_type,
    CASE WHEN COUNT(*) >= 50 THEN 'High' ELSE 'Medium' END AS severity,
    p.club_id, c.club_name, CAST(NULL AS STRING) AS asset_id, CAST(NULL AS STRING) AS supplier_id, CAST(NULL AS STRING) AS ticket_id,
    CONCAT(CAST(COUNT(*) AS STRING), ' preventive tasks overdue (oldest ', CAST(MAX(p.days_overdue_at_cutoff) AS STRING), ' days)') AS reason,
    CAST(COUNT(*) AS DOUBLE) AS supporting_value, 'overdue tasks' AS supporting_unit,
    CAST(NULL AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.fact_preventive_plan p
  JOIN fitco_gold.dim_club c ON c.club_id = p.club_id
  WHERE p.status = 'Overdue'
  GROUP BY p.club_id, c.club_name
  HAVING COUNT(*) >= 25
),
overcharge AS (
  SELECT
    'Financial mismatch' AS flag_type,
    CASE WHEN SUM(f.overcharge_eur) >= 1000 THEN 'High' ELSE 'Medium' END AS severity,
    CAST(NULL AS STRING) AS club_id, CAST(NULL AS STRING) AS club_name, CAST(NULL AS STRING) AS asset_id,
    f.supplier_id, CAST(NULL AS STRING) AS ticket_id,
    CONCAT(CAST(COUNT(*) AS STRING), ' labour lines billed >10% above indexed contract rate in last 90 days') AS reason,
    CAST(COUNT(*) AS DOUBLE) AS supporting_value, 'lines over contract' AS supporting_unit,
    CAST(SUM(f.overcharge_eur) AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.fact_maintenance_cost f
  CROSS JOIN fitco_gold.report_params r
  WHERE f.is_overcharge AND f.invoice_date > date_add(r.cutoff_date, -90)
  GROUP BY f.supplier_id
  HAVING COUNT(*) >= 5
),
orphan_lines AS (
  SELECT
    'Financial mismatch' AS flag_type,
    'High' AS severity,
    CAST(NULL AS STRING) AS club_id, CAST(NULL AS STRING) AS club_name, CAST(NULL AS STRING) AS asset_id,
    CAST(NULL AS STRING) AS supplier_id, CAST(NULL AS STRING) AS ticket_id,
    CONCAT(CAST(COUNT(*) AS STRING), ' invoice lines reference a ticket that does not exist - reconcile with procurement/finance') AS reason,
    CAST(COUNT(*) AS DOUBLE) AS supporting_value, 'orphan lines' AS supporting_unit,
    CAST(SUM(CAST(l.line_net_eur AS DOUBLE)) AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_silver.invoice_lines l
  WHERE l.is_orphan_ticket AND NOT l.is_duplicate_line
  HAVING COUNT(*) > 0
),
uninvoiced AS (
  SELECT
    'Financial mismatch' AS flag_type,
    'Medium' AS severity,
    CAST(NULL AS STRING) AS club_id, CAST(NULL AS STRING) AS club_name, CAST(NULL AS STRING) AS asset_id,
    u.supplier_id, CAST(NULL AS STRING) AS ticket_id,
    CONCAT(CAST(COUNT(*) AS STRING), ' closed corrective tickets with no invoice after 60 days - check accrual / supplier billing') AS reason,
    CAST(COUNT(*) AS DOUBLE) AS supporting_value, 'tickets without invoice' AS supporting_unit,
    CAST(NULL AS DOUBLE) AS estimated_cost_impact_eur
  FROM fitco_gold.v_uninvoiced_tickets u
  GROUP BY u.supplier_id
  HAVING COUNT(*) >= 5
),
unioned AS (
  SELECT * FROM sla_breach
  UNION ALL SELECT * FROM repeat_failure
  UNION ALL SELECT * FROM cost_outlier
  UNION ALL SELECT * FROM supplier_det
  UNION ALL SELECT * FROM pm_backlog
  UNION ALL SELECT * FROM overcharge
  UNION ALL SELECT * FROM orphan_lines
  UNION ALL SELECT * FROM uninvoiced
)
SELECT
  ROW_NUMBER() OVER (ORDER BY CASE u.severity WHEN 'High' THEN 1 WHEN 'Medium' THEN 2 ELSE 3 END, u.flag_type, u.supporting_value DESC) AS exception_id,
  u.flag_type, u.severity, u.club_id, u.club_name, u.asset_id, u.supplier_id, u.ticket_id,
  u.reason, u.supporting_value, u.supporting_unit, u.estimated_cost_impact_eur,
  r.cutoff_date AS generated_on,
  'New' AS status
FROM unioned u
CROSS JOIN fitco_gold.report_params r;