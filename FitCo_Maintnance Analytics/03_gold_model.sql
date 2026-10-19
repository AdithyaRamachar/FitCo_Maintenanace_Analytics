-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 03 · Gold — star schema for analytics
-- MAGIC **Grain (never mix them in one aggregate without pre-aggregating):**
-- MAGIC | Table | One row per |
-- MAGIC |---|---|
-- MAGIC | `fact_maintenance_ticket` | de-duplicated ticket (tickets whose asset is not in the register are kept, category `Unknown`, flagged `is_orphan_asset`, so invoices still reconcile) |
-- MAGIC | `fact_maintenance_cost` | valid invoice line (duplicate and orphan-ticket lines removed) |
-- MAGIC | `fact_preventive_plan` | scheduled preventive task |
-- MAGIC | `fact_club_month` | club × month (revenue, staff, rent, energy, maintenance) |
-- MAGIC
-- MAGIC **Cost policy:** cost = invoice **net** amount (VAT is recoverable). `REALISED` = approved lines + credit notes,
-- MAGIC `PENDING` = not yet approved (accrual), `DISPUTED`/`REJECTED` are excluded from cost but kept visible.
-- MAGIC Internal labour is not costed (in-house tickets carry zero invoice cost by design).

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

-- Reporting parameters. Change cutoff_date only if you regenerate data with a different extract date.
CREATE OR REPLACE TABLE fitco_gold.report_params AS
SELECT DATE '2026-09-30' AS cutoff_date, DATE '2024-10-01' AS window_start_date;

-- COMMAND ----------

-- SPARK_ONLY
CREATE OR REPLACE TABLE fitco_gold.dim_date AS
SELECT
  d AS date_day,
  year(d) AS year,
  quarter(d) AS quarter,
  month(d) AS month_num,
  CAST(date_trunc('month', d) AS DATE) AS month_start,
  CAST(date_trunc('week', d) AS DATE)  AS week_start,
  date_format(d, 'yyyy-MM') AS year_month,
  (dayofweek(d) IN (1, 7)) AS is_weekend
FROM (SELECT explode(sequence(DATE '2024-01-01', DATE '2027-12-31', INTERVAL 1 DAY)) AS d);

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.dim_club AS
SELECT
  x.*,
  CASE WHEN x.months_open_at_cutoff >= 36 THEN 'Mature' ELSE 'Immature' END AS maturity
FROM (
  SELECT
    c.club_id, c.club_name, c.city, c.country, c.ownership_model, c.operating_model, c.opened_date,
    c.opening_hours_per_week, c.staffed_hours_per_week, c.floor_area_m2,
    (year(p.cutoff_date) - year(c.opened_date)) * 12 + month(p.cutoff_date) - month(c.opened_date) AS months_open_at_cutoff
  FROM fitco_silver.clubs c
  CROSS JOIN fitco_gold.report_params p
) x;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.dim_supplier AS
SELECT
  supplier_id, supplier_name, supplier_type, countries_served, categories_served, preferred_supplier, active,
  (supplier_type = 'OEM Service Partner') AS is_oem_service_partner
FROM fitco_silver.suppliers;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.dim_asset AS
SELECT
  y.*,
  CASE WHEN y.age_years_at_cutoff / y.expected_life_years < 0.5 THEN '1 Early life'
       WHEN y.age_years_at_cutoff / y.expected_life_years < 0.9 THEN '2 Mid life'
       WHEN y.age_years_at_cutoff / y.expected_life_years < 1.0 THEN '3 Approaching end of life'
       ELSE '4 Beyond expected life' END AS life_stage,
  CASE WHEN y.proactive_service_programme = 'Y' THEN 12 ELSE y.expected_life_years END AS target_life_years
FROM (
  SELECT
    a.asset_id, a.club_id, a.asset_category, a.asset_type, a.manufacturer, a.model_code, a.serial_number,
    a.install_date, a.warranty_end_date, a.expected_life_years, a.proactive_service_programme, a.criticality,
    a.replacement_cost_eur, a.operating_status, a.decommission_date,
    ROUND(datediff(COALESCE(a.decommission_date, p.cutoff_date), a.install_date) / 365.25, 2) AS age_years_at_cutoff,
    ROUND(GREATEST(datediff(LEAST(COALESCE(a.decommission_date, p.cutoff_date), p.cutoff_date),
                            GREATEST(a.install_date, p.window_start_date)), 0) / 365.25, 3) AS exposure_years_in_window
  FROM fitco_silver.assets a
  CROSS JOIN fitco_gold.report_params p
) y;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.fact_maintenance_ticket AS
WITH base AS (
  SELECT
    t.ticket_id, t.asset_id, t.club_id, t.intervention_type, t.priority, t.status_raw, t.status_group,
    t.raised_channel, t.opened_at, t.closed_at, t.supplier_id, t.is_in_house, t.symptom, t.root_cause,
    t.root_cause_confirmed, t.downtime_hours, t.sla_target_hours, t.resolution_hours, t.plan_id,
    t.missing_closed_at, t.timestamps_swapped, t.sla_missing, t.sla_inconsistent, t.downtime_missing,
    t.is_orphan_asset,
    COALESCE(a.asset_category, 'Unknown') AS asset_category, COALESCE(a.asset_type, 'Unknown') AS asset_type,
    COALESCE(a.model_code, 'Unknown') AS model_code, COALESCE(a.criticality, 'Unknown') AS criticality
  FROM fitco_silver.maintenance_tickets t
  LEFT JOIN fitco_silver.assets a ON a.asset_id = t.asset_id
  WHERE NOT t.is_duplicate
),
chain AS (
  SELECT
    b.ticket_id,
    LAG(b.ticket_id) OVER (PARTITION BY b.asset_id ORDER BY b.opened_at, b.ticket_id) AS prev_ticket_id,
    LAG(COALESCE(b.closed_at, b.opened_at)) OVER (PARTITION BY b.asset_id ORDER BY b.opened_at, b.ticket_id) AS prev_end_at,
    LAG(b.symptom) OVER (PARTITION BY b.asset_id ORDER BY b.opened_at, b.ticket_id) AS prev_symptom
  FROM base b
  WHERE b.intervention_type = 'Corrective' AND b.status_group <> 'Cancelled'
)
SELECT
  b.ticket_id, b.asset_id, b.club_id, b.supplier_id, b.is_in_house,
  b.asset_category, b.asset_type, b.model_code, b.criticality,
  b.intervention_type, b.priority, b.status_raw, b.status_group, b.raised_channel,
  b.opened_at, b.closed_at,
  CAST(b.opened_at AS DATE)                                  AS opened_date,
  CAST(date_trunc('month', b.opened_at) AS DATE)             AS ticket_month,
  b.symptom, b.root_cause, b.root_cause_confirmed, b.downtime_hours,
  b.sla_target_hours, b.resolution_hours, b.plan_id,
  (b.intervention_type = 'Corrective' AND b.status_group = 'Closed' AND b.closed_at IS NOT NULL
     AND b.resolution_hours > 0)                             AS is_sla_eligible,
  CASE WHEN b.intervention_type = 'Corrective' AND b.status_group = 'Closed' AND b.closed_at IS NOT NULL
            AND b.resolution_hours > 0
       THEN (b.resolution_hours <= b.sla_target_hours) END   AS sla_met,
  CASE WHEN b.status_group = 'Open'
       THEN (unix_timestamp(CAST(date_add(p.cutoff_date, 1) AS TIMESTAMP)) - unix_timestamp(b.opened_at)) / 3600.0
  END                                                        AS open_age_hours,
  c.prev_ticket_id,
  (unix_timestamp(b.opened_at) - unix_timestamp(c.prev_end_at)) / 86400.0 AS days_since_prev_ticket,
  COALESCE((unix_timestamp(b.opened_at) - unix_timestamp(c.prev_end_at)) / 86400.0 <= 7, FALSE)  AS is_repeat_7d,
  COALESCE((unix_timestamp(b.opened_at) - unix_timestamp(c.prev_end_at)) / 86400.0 <= 30, FALSE) AS is_repeat_30d,
  COALESCE((unix_timestamp(b.opened_at) - unix_timestamp(c.prev_end_at)) / 86400.0 <= 30
           AND c.prev_symptom = b.symptom, FALSE)                                                 AS is_repeat_30d_same_symptom,
  b.is_orphan_asset, b.missing_closed_at, b.timestamps_swapped, b.sla_missing, b.sla_inconsistent, b.downtime_missing
FROM base b
LEFT JOIN chain c ON c.ticket_id = b.ticket_id
CROSS JOIN fitco_gold.report_params p;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.fact_maintenance_cost AS
SELECT
  l.line_id, l.invoice_id, l.ticket_id, l.supplier_id, l.line_type, l.quantity,
  CAST(l.unit_rate_eur AS DOUBLE)  AS unit_rate_eur,
  CAST(l.line_net_eur AS DOUBLE)   AS line_net_eur,
  CAST(l.tax_eur AS DOUBLE)        AS tax_eur,
  CAST(l.line_total_eur AS DOUBLE) AS line_total_eur,
  l.invoice_date,
  CAST(date_trunc('month', l.invoice_date) AS DATE) AS invoice_month,
  l.invoice_status,
  CASE WHEN l.invoice_status IN ('APPROVED', 'CREDIT_NOTE') THEN 'REALISED'
       WHEN l.invoice_status = 'PENDING'  THEN 'PENDING'
       WHEN l.invoice_status = 'DISPUTED' THEN 'DISPUTED'
       ELSE 'REJECTED' END AS cost_class,
  l.cost_borne_by,
  f.club_id, f.asset_id, f.asset_category, f.model_code, f.intervention_type, f.priority, f.ticket_month,
  CAST(f.closed_at AS DATE) AS ticket_closed_date,
  c.country, c.ownership_model,
  datediff(l.invoice_date, CAST(f.closed_at AS DATE)) AS days_close_to_invoice,
  l.contract_id,
  k.labour_rate_eur_hr AS contract_rate_eur_hr,
  CASE WHEN l.line_type = 'Labour' AND k.labour_rate_eur_hr IS NOT NULL AND f.closed_at IS NOT NULL
       THEN k.labour_rate_eur_hr * power(1.04, datediff(CAST(f.closed_at AS DATE), DATE '2024-10-01') / 365.25)
  END AS expected_rate_eur_hr,
  CASE WHEN l.line_type = 'Labour' AND k.labour_rate_eur_hr IS NOT NULL AND f.closed_at IS NOT NULL
        AND CAST(l.unit_rate_eur AS DOUBLE) > 1.10 * k.labour_rate_eur_hr * power(1.04, datediff(CAST(f.closed_at AS DATE), DATE '2024-10-01') / 365.25)
       THEN TRUE ELSE FALSE END AS is_overcharge,
  CASE WHEN l.line_type = 'Labour' AND k.labour_rate_eur_hr IS NOT NULL AND f.closed_at IS NOT NULL
        AND CAST(l.unit_rate_eur AS DOUBLE) > 1.10 * k.labour_rate_eur_hr * power(1.04, datediff(CAST(f.closed_at AS DATE), DATE '2024-10-01') / 365.25)
       THEN (CAST(l.unit_rate_eur AS DOUBLE) - k.labour_rate_eur_hr * power(1.04, datediff(CAST(f.closed_at AS DATE), DATE '2024-10-01') / 365.25)) * l.quantity
  END AS overcharge_eur
FROM fitco_silver.invoice_lines l
JOIN fitco_gold.fact_maintenance_ticket f ON f.ticket_id = l.ticket_id
JOIN fitco_gold.dim_club c                ON c.club_id = f.club_id
LEFT JOIN fitco_silver.supplier_contracts k ON k.contract_id = l.contract_id
WHERE NOT l.is_duplicate_line;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.fact_preventive_plan AS
SELECT
  p.plan_id, p.asset_id, p.club_id, a.asset_category, a.criticality, p.task_type,
  p.planned_date, p.due_date, p.completed_date, p.linked_ticket_id,
  CASE WHEN p.completed_date IS NOT NULL THEN
         CASE WHEN p.completed_date <= p.due_date THEN 'Completed' ELSE 'Completed Late' END
       WHEN p.due_date <= r.cutoff_date THEN 'Overdue'
       ELSE 'Scheduled' END                                      AS status,
  (p.due_date <= r.cutoff_date)                                  AS is_due,
  (p.completed_date IS NOT NULL AND p.completed_date <= p.due_date) AS completed_on_time,
  CAST(date_trunc('month', p.due_date) AS DATE)                  AS due_month,
  CASE WHEN p.completed_date IS NULL AND p.due_date <= r.cutoff_date
       THEN datediff(r.cutoff_date, p.due_date) END              AS days_overdue_at_cutoff,
  CASE WHEN p.completed_date > p.due_date THEN datediff(p.completed_date, p.due_date) END AS days_late
FROM fitco_silver.maintenance_plans p
JOIN fitco_silver.assets a ON a.asset_id = p.asset_id
CROSS JOIN fitco_gold.report_params r;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_gold.fact_club_month AS
WITH maint AS (
  SELECT
    club_id, invoice_month AS month_start,
    SUM(CASE WHEN cost_class = 'REALISED' AND cost_borne_by = 'FitCo' THEN line_net_eur ELSE 0 END) AS maintenance_cost_eur,
    SUM(CASE WHEN cost_class = 'PENDING'  AND cost_borne_by = 'FitCo' THEN line_net_eur ELSE 0 END) AS maintenance_pending_eur
  FROM fitco_gold.fact_maintenance_cost
  GROUP BY club_id, invoice_month
),
tix AS (
  SELECT
    club_id, ticket_month AS month_start,
    SUM(CASE WHEN intervention_type = 'Corrective' AND status_group <> 'Cancelled' THEN 1 ELSE 0 END) AS corrective_tickets,
    SUM(CASE WHEN downtime_hours IS NOT NULL THEN downtime_hours ELSE 0 END)                           AS downtime_hours
  FROM fitco_gold.fact_maintenance_ticket
  GROUP BY club_id, ticket_month
)
SELECT
  k.club_id, k.month_start, c.country, c.ownership_model, c.operating_model, c.maturity,
  k.members_end, k.avg_members, k.new_members, k.cancellations, k.arpu_eur,
  k.membership_revenue_eur, k.other_revenue_eur,
  k.membership_revenue_eur + k.other_revenue_eur AS total_revenue_eur,
  k.staff_cost_eur, k.rent_eur, k.other_opex_eur, k.member_visits,
  e.kwh AS energy_kwh, e.energy_cost_eur,
  COALESCE(m.maintenance_cost_eur, 0)    AS maintenance_cost_eur,
  COALESCE(m.maintenance_pending_eur, 0) AS maintenance_pending_eur,
  COALESCE(t.corrective_tickets, 0)      AS corrective_tickets,
  COALESCE(t.downtime_hours, 0)          AS downtime_hours,
  COALESCE(m.maintenance_cost_eur, 0) / NULLIF(k.avg_members, 0) AS maintenance_cost_per_member_eur,
  e.energy_cost_eur / NULLIF(k.avg_members, 0)                   AS energy_cost_per_member_eur,
  CASE WHEN c.ownership_model = 'Owned' THEN
       k.membership_revenue_eur + k.other_revenue_eur - k.staff_cost_eur - k.rent_eur - k.other_opex_eur
       - e.energy_cost_eur - COALESCE(m.maintenance_cost_eur, 0) END AS ebitda_less_rent_proxy_eur,
  (k.month_start >= CAST(date_trunc('month', date_add(r.cutoff_date, -45)) AS DATE)) AS cost_month_incomplete
FROM fitco_silver.club_monthly_kpis k
JOIN fitco_gold.dim_club c ON c.club_id = k.club_id
LEFT JOIN fitco_silver.energy_monthly e ON e.club_id = k.club_id AND e.month_start = k.month_start
LEFT JOIN maint m ON m.club_id = k.club_id AND m.month_start = k.month_start
LEFT JOIN tix t   ON t.club_id = k.club_id AND t.month_start = k.month_start
CROSS JOIN fitco_gold.report_params r;