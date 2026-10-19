-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 04 · Data-quality and reconciliation checks
-- MAGIC Run **after** Gold and the SQL views. Writes `fitco_silver.dq_results`.
-- MAGIC * `PASS`/`FAIL` checks must all pass: keys unique, row counts reconcile across layers, cost totals match an independent recompute.
-- MAGIC * `INFO` rows count the **injected data defects** (duplicates, orphans, missing dates…). They are expected to be non-zero: that is the point of the exercise.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.dq_results AS
WITH c AS (
  SELECT 'DQ01' AS check_id, 'Bronze to Silver' AS layer, 'Ticket rows preserved' AS check_name,
         CAST((SELECT COUNT(*) FROM fitco_bronze.maintenance_tickets) AS DOUBLE) AS expected_value,
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets) AS DOUBLE) AS actual_value, 'EQUAL' AS rule
  UNION ALL SELECT 'DQ02', 'Bronze to Silver', 'Invoice line rows preserved',
         CAST((SELECT COUNT(*) FROM fitco_bronze.invoice_lines) AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ03', 'Silver keys', 'ticket_id unique', 0.0,
         CAST((SELECT COUNT(*) - COUNT(DISTINCT ticket_id) FROM fitco_silver.maintenance_tickets) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ04', 'Silver keys', 'asset_id unique', 0.0,
         CAST((SELECT COUNT(*) - COUNT(DISTINCT asset_id) FROM fitco_silver.assets) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ05', 'Silver keys', 'plan_id unique', 0.0,
         CAST((SELECT COUNT(*) - COUNT(DISTINCT plan_id) FROM fitco_silver.maintenance_plans) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ06', 'Silver keys', 'invoice line_id unique', 0.0,
         CAST((SELECT COUNT(*) - COUNT(DISTINCT line_id) FROM fitco_silver.invoice_lines) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ07', 'Silver keys', 'supplier name_key unique (name matching is unambiguous)', 0.0,
         CAST((SELECT COUNT(*) - COUNT(DISTINCT name_key) FROM fitco_silver.suppliers) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ08', 'Silver rules', 'Supplier names all resolved to a supplier_id', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE supplier_unresolved AND NOT is_duplicate) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ09', 'Silver rules', 'Invoice dates all parsed', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines WHERE invoice_date_unparsed) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ10', 'Silver rules', 'All invoice currencies are EUR after standardising', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines WHERE currency_not_eur) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ11', 'Silver rules', 'Invoice net + tax = total (within 2 cents)', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines WHERE total_mismatch) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ12', 'Silver to Gold', 'Gold tickets = Silver tickets minus duplicates',
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE NOT is_duplicate) AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_ticket) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ13', 'Silver to Gold', 'Gold cost lines = Silver lines minus duplicate and orphan lines',
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines WHERE NOT is_duplicate_line AND NOT is_orphan_ticket) AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_cost) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ14', 'Gold grain', 'fact_maintenance_cost: one row per line_id (no join fan-out)',
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_cost) AS DOUBLE),
         CAST((SELECT COUNT(DISTINCT line_id) FROM fitco_gold.fact_maintenance_cost) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ15', 'Gold grain', 'fact_maintenance_ticket: one row per ticket_id',
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_ticket) AS DOUBLE),
         CAST((SELECT COUNT(DISTINCT ticket_id) FROM fitco_gold.fact_maintenance_ticket) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ16', 'Cost reconciliation', 'Realised cost in Gold = independent recompute from Silver (EUR net)',
         CAST((SELECT SUM(CAST(line_net_eur AS DOUBLE)) FROM fitco_silver.invoice_lines
               WHERE invoice_status IN ('APPROVED', 'CREDIT_NOTE') AND NOT is_duplicate_line AND NOT is_orphan_ticket) AS DOUBLE),
         CAST((SELECT SUM(line_net_eur) FROM fitco_gold.fact_maintenance_cost WHERE cost_class = 'REALISED') AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ17', 'Cost reconciliation', 'v_cost_monthly_invoice realised total = fact realised total (EUR)',
         CAST((SELECT SUM(line_net_eur) FROM fitco_gold.fact_maintenance_cost WHERE cost_class = 'REALISED') AS DOUBLE),
         CAST((SELECT SUM(realised_cost_eur) FROM fitco_gold.v_cost_monthly_invoice) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ18', 'Gold rules', 'Ticket club_id matches the asset register club_id', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_ticket f JOIN fitco_gold.dim_asset a ON a.asset_id = f.asset_id
               WHERE f.club_id <> a.club_id) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ19', 'Gold rules', 'Every SLA-eligible ticket has an SLA result', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_ticket WHERE is_sla_eligible AND sla_met IS NULL) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ20', 'Gold rules', 'No ticket opened after the reporting cutoff', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_ticket f CROSS JOIN fitco_gold.report_params r
               WHERE f.opened_date > r.cutoff_date) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ21', 'Gold rules', 'No invoice dated after the reporting cutoff', 0.0,
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_maintenance_cost f CROSS JOIN fitco_gold.report_params r
               WHERE f.invoice_date > r.cutoff_date) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ22', 'Gold rules', 'Preventive plan rows preserved',
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_plans) AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_preventive_plan) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'DQ23', 'Gold rules', 'Club-month rows preserved',
         CAST((SELECT COUNT(*) FROM fitco_silver.club_monthly_kpis) AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_gold.fact_club_month) AS DOUBLE), 'EQUAL'
  UNION ALL SELECT 'INF01', 'Defect (expected)', 'Duplicate tickets (same source_ref) flagged', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF02', 'Defect (expected)', 'Tickets referencing an unknown asset (kept in Gold as category Unknown)', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE is_orphan_asset AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF03', 'Defect (expected)', 'Closed tickets with no closed_at', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE missing_closed_at AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF04', 'Defect (expected)', 'Tickets with closed_at before opened_at (swapped)', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE timestamps_swapped AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF05', 'Defect (expected)', 'Corrective tickets with missing SLA target', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE sla_missing AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF06', 'Defect (expected)', 'Corrective tickets with SLA target inconsistent with priority', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE sla_inconsistent AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF07', 'Defect (expected)', 'Closed tickets with missing downtime', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE downtime_missing AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF08', 'Defect (expected)', 'Duplicate invoice lines flagged', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines WHERE is_duplicate_line) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF09', 'Defect (expected)', 'Invoice lines referencing an unknown ticket', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.invoice_lines WHERE is_orphan_ticket AND NOT is_duplicate_line) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF10', 'Defect (expected)', 'Tickets whose supplier name needed fuzzy matching (no supplier_id)', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_silver.maintenance_tickets WHERE supplier_resolved_by_name AND NOT is_duplicate) AS DOUBLE), 'INFO'
  UNION ALL SELECT 'INF11', 'Output', 'Rows in exception register', CAST(NULL AS DOUBLE),
         CAST((SELECT COUNT(*) FROM fitco_gold.v_exceptions) AS DOUBLE), 'INFO'
)
SELECT
  check_id, layer, check_name, expected_value, actual_value, rule,
  CASE WHEN rule = 'INFO' THEN 'INFO'
       WHEN abs(actual_value - expected_value) <= 0.01 THEN 'PASS'
       ELSE 'FAIL' END AS status
FROM c;

-- COMMAND ----------

SELECT * FROM fitco_silver.dq_results ORDER BY check_id;

-- COMMAND ----------

-- Must return zero rows
SELECT * FROM fitco_silver.dq_results WHERE status = 'FAIL';