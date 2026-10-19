-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 12 · Workstream D — Supplier and SLA benchmarking
-- MAGIC **Question:** which suppliers deliver reliable service at a reasonable cost?
-- MAGIC **Manager action:** review recent cases for suppliers flagged as deteriorating; renegotiate where cost and SLA both lag peers.
-- MAGIC
-- MAGIC *Like-for-like rule:* suppliers are only compared **within the same asset category and urgency band**
-- MAGIC (urgent = P1–P2, routine = P3–P4). Ranks are only shown when a segment has at least 30 SLA-eligible tickets.
-- MAGIC The Wilson interval (95%) shows how uncertain each SLA % is at small sample sizes.
-- MAGIC *Cost per ticket* uses only **settled** tickets (closed more than 60 days before the cutoff) so late invoices do not flatter anyone.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_supplier_scorecard AS
WITH t AS (
  SELECT
    f.ticket_id, f.supplier_id, f.asset_category,
    CASE WHEN f.priority IN ('P1', 'P2') THEN 'Urgent (P1-P2)' ELSE 'Routine (P3-P4)' END AS urgency,
    f.is_sla_eligible, f.sla_met, f.resolution_hours, f.is_repeat_30d, f.status_group,
    f.open_age_hours, f.sla_target_hours, f.closed_at
  FROM fitco_gold.fact_maintenance_ticket f
  WHERE f.intervention_type = 'Corrective' AND f.supplier_id IS NOT NULL AND f.status_group <> 'Cancelled'
),
tc AS (
  SELECT ticket_id, SUM(CASE WHEN cost_class = 'REALISED' THEN line_net_eur ELSE 0 END) AS realised_cost_eur
  FROM fitco_gold.fact_maintenance_cost
  GROUP BY ticket_id
),
agg AS (
  SELECT
    t.supplier_id, t.asset_category, t.urgency,
    COUNT(*)                                                                            AS tickets,
    SUM(CASE WHEN t.is_sla_eligible THEN 1 ELSE 0 END)                                  AS n_eligible,
    SUM(CASE WHEN t.is_sla_eligible AND t.sla_met THEN 1 ELSE 0 END)                    AS n_met,
    MEDIAN(CASE WHEN t.is_sla_eligible THEN t.resolution_hours END)                     AS median_resolution_h,
    AVG(CASE WHEN t.is_sla_eligible THEN t.resolution_hours END)                        AS mean_resolution_h,
    SUM(CASE WHEN t.is_repeat_30d THEN 1 ELSE 0 END) * 1.0 / COUNT(*)                   AS repeat_rate_30d,
    SUM(CASE WHEN t.status_group = 'Open' THEN 1 ELSE 0 END)                            AS open_tickets,
    SUM(CASE WHEN t.status_group = 'Open' AND t.open_age_hours > t.sla_target_hours THEN 1 ELSE 0 END) AS open_past_sla,
    AVG(CASE WHEN t.status_group = 'Closed' AND t.closed_at < CAST(date_add(r.cutoff_date, -60) AS TIMESTAMP)
             THEN COALESCE(tc.realised_cost_eur, 0) END)                                AS avg_cost_per_settled_ticket_eur,
    SUM(CASE WHEN t.status_group = 'Closed' AND t.closed_at < CAST(date_add(r.cutoff_date, -60) AS TIMESTAMP)
             THEN 1 ELSE 0 END)                                                         AS settled_tickets
  FROM t
  LEFT JOIN tc ON tc.ticket_id = t.ticket_id
  CROSS JOIN fitco_gold.report_params r
  GROUP BY t.supplier_id, t.asset_category, t.urgency
),
w AS (
  SELECT a.*,
    a.n_met * 1.0 / NULLIF(a.n_eligible, 0) AS sla_pct,
    (a.n_eligible >= 30)                    AS sample_ok
  FROM agg a
)
SELECT
  s.supplier_id, s.supplier_name, s.supplier_type, w.asset_category, w.urgency,
  w.tickets, w.n_eligible AS sla_eligible_tickets, w.n_met AS sla_met_tickets,
  w.sla_pct,
  (w.sla_pct + 1.9208 / w.n_eligible - 1.96 * sqrt(w.sla_pct * (1 - w.sla_pct) / w.n_eligible + 0.9604 / (w.n_eligible * w.n_eligible)))
    / (1 + 3.8416 / w.n_eligible)           AS sla_wilson_low,
  (w.sla_pct + 1.9208 / w.n_eligible + 1.96 * sqrt(w.sla_pct * (1 - w.sla_pct) / w.n_eligible + 0.9604 / (w.n_eligible * w.n_eligible)))
    / (1 + 3.8416 / w.n_eligible)           AS sla_wilson_high,
  w.median_resolution_h, w.mean_resolution_h, w.repeat_rate_30d,
  w.settled_tickets, w.avg_cost_per_settled_ticket_eur,
  w.open_tickets, w.open_past_sla, w.sample_ok,
  CASE WHEN w.sample_ok THEN
       RANK() OVER (PARTITION BY w.asset_category, w.urgency, w.sample_ok ORDER BY w.sla_pct DESC) END AS sla_rank_in_segment
FROM w
JOIN fitco_gold.dim_supplier s ON s.supplier_id = w.supplier_id
WHERE w.n_eligible > 0;

-- COMMAND ----------

-- Invoice variance against contract: labour lines billed above the indexed contract rate (+10% tolerance)
CREATE OR REPLACE VIEW fitco_gold.v_supplier_invoice_variance AS
SELECT
  s.supplier_id, s.supplier_name, f.asset_category, f.country,
  SUM(CASE WHEN f.line_type = 'Labour' AND f.expected_rate_eur_hr IS NOT NULL THEN 1 ELSE 0 END)  AS labour_lines_checked,
  SUM(CASE WHEN f.is_overcharge THEN 1 ELSE 0 END)                                                 AS overcharged_lines,
  SUM(CASE WHEN f.is_overcharge THEN 1 ELSE 0 END) * 1.0
    / NULLIF(SUM(CASE WHEN f.line_type = 'Labour' AND f.expected_rate_eur_hr IS NOT NULL THEN 1 ELSE 0 END), 0) AS overcharge_line_share,
  SUM(COALESCE(f.overcharge_eur, 0))                                                               AS overcharge_eur,
  SUM(CASE WHEN f.invoice_status = 'DISPUTED' THEN f.line_net_eur ELSE 0 END)                      AS disputed_eur
FROM fitco_gold.fact_maintenance_cost f
JOIN fitco_gold.dim_supplier s ON s.supplier_id = f.supplier_id
GROUP BY s.supplier_id, s.supplier_name, f.asset_category, f.country;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_supplier_monthly_sla AS
SELECT
  f.supplier_id, s.supplier_name,
  CAST(date_trunc('month', f.closed_at) AS DATE)             AS closed_month,
  COUNT(*)                                                    AS sla_eligible_tickets,
  SUM(CASE WHEN f.sla_met THEN 1 ELSE 0 END)                  AS sla_met_tickets,
  SUM(CASE WHEN f.sla_met THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS sla_pct,
  MEDIAN(f.resolution_hours)                                  AS median_resolution_h
FROM fitco_gold.fact_maintenance_ticket f
JOIN fitco_gold.dim_supplier s ON s.supplier_id = f.supplier_id
WHERE f.is_sla_eligible AND f.supplier_id IS NOT NULL
GROUP BY f.supplier_id, s.supplier_name, CAST(date_trunc('month', f.closed_at) AS DATE);

-- COMMAND ----------

-- Deterioration: last 90 days vs all earlier history. Flag needs >= 30 recent tickets, >= 8pp drop and z >= 2.
CREATE OR REPLACE VIEW fitco_gold.v_supplier_deterioration AS
WITH base AS (
  SELECT f.supplier_id, CAST(f.closed_at AS DATE) AS closed_date, f.sla_met, r.cutoff_date
  FROM fitco_gold.fact_maintenance_ticket f
  CROSS JOIN fitco_gold.report_params r
  WHERE f.is_sla_eligible AND f.supplier_id IS NOT NULL
),
agg AS (
  SELECT
    supplier_id,
    SUM(CASE WHEN closed_date >  date_add(cutoff_date, -90) THEN 1 ELSE 0 END)                    AS n_recent,
    SUM(CASE WHEN closed_date >  date_add(cutoff_date, -90) AND sla_met THEN 1 ELSE 0 END)        AS k_recent,
    SUM(CASE WHEN closed_date <= date_add(cutoff_date, -90) THEN 1 ELSE 0 END)                    AS n_base,
    SUM(CASE WHEN closed_date <= date_add(cutoff_date, -90) AND sla_met THEN 1 ELSE 0 END)        AS k_base
  FROM base
  GROUP BY supplier_id
),
calc AS (
  SELECT a.*,
    a.k_recent * 1.0 / NULLIF(a.n_recent, 0) AS sla_recent,
    a.k_base * 1.0 / NULLIF(a.n_base, 0)     AS sla_baseline,
    (a.k_recent + a.k_base) * 1.0 / NULLIF(a.n_recent + a.n_base, 0) AS p_pooled
  FROM agg a
)
SELECT
  c.supplier_id, s.supplier_name, s.supplier_type,
  c.n_recent, c.n_base, c.sla_recent, c.sla_baseline,
  c.sla_baseline - c.sla_recent AS drop_pp,
  (c.sla_baseline - c.sla_recent)
    / NULLIF(sqrt(c.p_pooled * (1 - c.p_pooled) * (1.0 / NULLIF(c.n_recent, 0) + 1.0 / NULLIF(c.n_base, 0))), 0) AS z_score,
  (c.n_recent >= 30 AND (c.sla_baseline - c.sla_recent) >= 0.08
     AND (c.sla_baseline - c.sla_recent)
         / NULLIF(sqrt(c.p_pooled * (1 - c.p_pooled) * (1.0 / NULLIF(c.n_recent, 0) + 1.0 / NULLIF(c.n_base, 0))), 0) >= 2.0) AS is_deteriorating
FROM calc c
JOIN fitco_gold.dim_supplier s ON s.supplier_id = c.supplier_id;