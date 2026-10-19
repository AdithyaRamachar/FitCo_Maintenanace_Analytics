-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 13 · Workstream C — Cost drivers and equipment lifecycle economics
-- MAGIC **Question:** where is the maintenance budget going, what is driving the change, and does proactive servicing pay off?
-- MAGIC **Manager action:** investigate the segments with the largest volume or rate effect; decide refurbish vs run-to-failure.
-- MAGIC
-- MAGIC *Two time views:* `invoice_month` = when the supplier billed (finance view, lags operations);
-- MAGIC `ticket_month` = when the work was opened (operational view). Use the ticket view for trends, the invoice view to reconcile to finance.
-- MAGIC *Committed cost* = REALISED + PENDING invoice lines (disputed/rejected excluded).
-- MAGIC Costs are **invoice spend only**. Downtime is a separate, hypothetical estimate (see `v_downtime_impact_estimate`).

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_cost_monthly_invoice AS
SELECT
  invoice_month, country, asset_category, intervention_type, cost_borne_by,
  SUM(CASE WHEN cost_class = 'REALISED' THEN line_net_eur ELSE 0 END) AS realised_cost_eur,
  SUM(CASE WHEN cost_class = 'PENDING'  THEN line_net_eur ELSE 0 END) AS pending_cost_eur,
  SUM(CASE WHEN cost_class = 'DISPUTED' THEN line_net_eur ELSE 0 END) AS disputed_cost_eur,
  COUNT(*)                                                            AS invoice_lines,
  COUNT(DISTINCT ticket_id)                                           AS tickets_billed
FROM fitco_gold.fact_maintenance_cost
GROUP BY invoice_month, country, asset_category, intervention_type, cost_borne_by;

-- COMMAND ----------

-- Operational cost view: cost attached to the month the ticket was opened (closed tickets only).
-- Ticket cost is pre-aggregated first so multi-line invoices cannot multiply ticket counts.
CREATE OR REPLACE VIEW fitco_gold.v_cost_by_ticket_month AS
WITH tc AS (
  SELECT ticket_id,
         SUM(CASE WHEN cost_class IN ('REALISED', 'PENDING') THEN line_net_eur ELSE 0 END) AS committed_cost_eur
  FROM fitco_gold.fact_maintenance_cost
  GROUP BY ticket_id
)
SELECT
  f.ticket_month, c.country, c.ownership_model, f.asset_category, f.intervention_type,
  COUNT(*)                                          AS closed_tickets,
  SUM(CASE WHEN tc.ticket_id IS NOT NULL THEN 1 ELSE 0 END) AS tickets_with_cost,
  SUM(COALESCE(tc.committed_cost_eur, 0))           AS committed_cost_eur,
  SUM(COALESCE(tc.committed_cost_eur, 0)) / COUNT(*) AS cost_per_closed_ticket_eur
FROM fitco_gold.fact_maintenance_ticket f
JOIN fitco_gold.dim_club c ON c.club_id = f.club_id
LEFT JOIN tc ON tc.ticket_id = f.ticket_id
WHERE f.status_group = 'Closed'
GROUP BY f.ticket_month, c.country, c.ownership_model, f.asset_category, f.intervention_type;

-- COMMAND ----------

-- Volume vs rate decomposition: last 6 complete ticket-months vs the 6 before.
-- delta = volume_effect (more/fewer tickets at the old cost) + rate_effect (cost per ticket change on the new volume).
CREATE OR REPLACE VIEW fitco_gold.v_cost_driver_decomposition AS
WITH idx AS (
  SELECT
    b.*,
    year(b.ticket_month) * 12 + month(b.ticket_month) AS m_idx,
    year(r.cutoff_date) * 12 + month(r.cutoff_date)   AS cut_idx
  FROM fitco_gold.v_cost_by_ticket_month b
  CROSS JOIN fitco_gold.report_params r
),
per AS (
  SELECT
    country, asset_category, intervention_type,
    SUM(CASE WHEN m_idx BETWEEN cut_idx - 6  AND cut_idx - 1 THEN closed_tickets ELSE 0 END)       AS n_recent,
    SUM(CASE WHEN m_idx BETWEEN cut_idx - 6  AND cut_idx - 1 THEN committed_cost_eur ELSE 0 END)   AS cost_recent,
    SUM(CASE WHEN m_idx BETWEEN cut_idx - 12 AND cut_idx - 7 THEN closed_tickets ELSE 0 END)       AS n_prior,
    SUM(CASE WHEN m_idx BETWEEN cut_idx - 12 AND cut_idx - 7 THEN committed_cost_eur ELSE 0 END)   AS cost_prior
  FROM idx
  GROUP BY country, asset_category, intervention_type
)
SELECT
  p.*,
  p.cost_recent - p.cost_prior                                                      AS delta_cost_eur,
  (p.n_recent - p.n_prior) * (p.cost_prior / NULLIF(p.n_prior, 0))                  AS volume_effect_eur,
  p.n_recent * (p.cost_recent / NULLIF(p.n_recent, 0) - p.cost_prior / NULLIF(p.n_prior, 0)) AS rate_effect_eur
FROM per p
WHERE p.n_prior > 0 AND p.n_recent > 0;

-- COMMAND ----------

-- Lifecycle economics: corrective + preventive + refurbishment cost per asset-year by life stage and programme enrolment
CREATE OR REPLACE VIEW fitco_gold.v_lifecycle_economics AS
WITH cost AS (
  SELECT asset_id,
    SUM(CASE WHEN intervention_type = 'Corrective'    AND cost_class IN ('REALISED','PENDING') THEN line_net_eur ELSE 0 END) AS corrective_cost_eur,
    SUM(CASE WHEN intervention_type = 'Preventive'    AND cost_class IN ('REALISED','PENDING') THEN line_net_eur ELSE 0 END) AS preventive_cost_eur,
    SUM(CASE WHEN intervention_type = 'Refurbishment' AND cost_class IN ('REALISED','PENDING') THEN line_net_eur ELSE 0 END) AS refurbishment_cost_eur
  FROM fitco_gold.fact_maintenance_cost
  GROUP BY asset_id
),
tix AS (
  SELECT asset_id, COUNT(*) AS corrective_tickets
  FROM fitco_gold.fact_maintenance_ticket
  WHERE intervention_type = 'Corrective' AND status_group <> 'Cancelled'
  GROUP BY asset_id
)
SELECT
  a.asset_category, a.proactive_service_programme, a.life_stage,
  COUNT(*)                                                       AS assets,
  SUM(a.exposure_years_in_window)                                AS asset_years,
  SUM(COALESCE(t.corrective_tickets, 0)) / NULLIF(SUM(a.exposure_years_in_window), 0)        AS failures_per_asset_year,
  SUM(COALESCE(c.corrective_cost_eur, 0)) / NULLIF(SUM(a.exposure_years_in_window), 0)       AS corrective_cost_per_asset_year_eur,
  SUM(COALESCE(c.preventive_cost_eur, 0)) / NULLIF(SUM(a.exposure_years_in_window), 0)       AS preventive_cost_per_asset_year_eur,
  SUM(COALESCE(c.refurbishment_cost_eur, 0)) / NULLIF(SUM(a.exposure_years_in_window), 0)    AS refurbishment_cost_per_asset_year_eur,
  (SUM(COALESCE(c.corrective_cost_eur, 0)) + SUM(COALESCE(c.preventive_cost_eur, 0)) + SUM(COALESCE(c.refurbishment_cost_eur, 0)))
    / NULLIF(SUM(a.exposure_years_in_window), 0)                                             AS total_cost_per_asset_year_eur,
  AVG(CAST(a.replacement_cost_eur AS DOUBLE))                    AS avg_replacement_cost_eur
FROM fitco_gold.dim_asset a
LEFT JOIN cost c ON c.asset_id = a.asset_id
LEFT JOIN tix t  ON t.asset_id = a.asset_id
WHERE a.asset_category IN ('Cardio', 'Strength Machine', 'Cable')
GROUP BY a.asset_category, a.proactive_service_programme, a.life_stage;

-- COMMAND ----------

-- Hypothetical downtime impact. NOT observed lost revenue: assumes (adjust the two constants) that every
-- downtime hour of a machine category affects a share of members' visits.
-- assumption: 1 machine-hour down = EUR 2.50 lost contribution for cardio/strength/cable, EUR 0 otherwise.
CREATE OR REPLACE VIEW fitco_gold.v_downtime_impact_estimate AS
SELECT
  ticket_month, club_id, asset_category,
  SUM(COALESCE(downtime_hours, 0))                                                    AS downtime_hours,
  SUM(COALESCE(downtime_hours, 0)) * 2.50                                             AS hypothetical_lost_contribution_eur,
  'ASSUMPTION: EUR 2.50 per machine downtime hour; not an observed financial loss'    AS note
FROM fitco_gold.fact_maintenance_ticket
WHERE asset_category IN ('Cardio', 'Strength Machine', 'Cable') AND status_group = 'Closed'
GROUP BY ticket_month, club_id, asset_category;