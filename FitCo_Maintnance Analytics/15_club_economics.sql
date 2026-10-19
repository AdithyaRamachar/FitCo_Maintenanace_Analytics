-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 15 · Club economics — maintenance in the context of the Basic-Fit-style P&L
-- MAGIC **Question:** how much do maintenance and energy cost per member, and how does that move club-level EBITDA less rent?
-- MAGIC `ebitda_less_rent_proxy_eur` = revenue − staff − rent − other opex − energy − realised maintenance (FitCo-borne, invoice month).
-- MAGIC It is only calculated for **owned** clubs (franchise club P&L belongs to the franchisee).
-- MAGIC The most recent two months are flagged `cost_month_incomplete` because supplier invoices arrive 1–8 weeks after the work; they are excluded from the scorecard.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_country_unit_economics AS
SELECT
  month_start, country, maturity,
  COUNT(*)                                                  AS clubs,
  SUM(avg_members)                                          AS member_months,
  SUM(total_revenue_eur)                                    AS revenue_eur,
  SUM(maintenance_cost_eur)                                 AS maintenance_cost_eur,
  SUM(energy_cost_eur)                                      AS energy_cost_eur,
  SUM(maintenance_cost_eur) / NULLIF(SUM(avg_members), 0)   AS maintenance_per_member_eur,
  SUM(energy_cost_eur) / NULLIF(SUM(avg_members), 0)        AS energy_per_member_eur,
  SUM(maintenance_cost_eur) / NULLIF(SUM(total_revenue_eur), 0) AS maintenance_pct_of_revenue,
  SUM(ebitda_less_rent_proxy_eur) / NULLIF(SUM(CASE WHEN ownership_model = 'Owned' THEN total_revenue_eur END), 0) AS ebitda_less_rent_margin_owned,
  MAX(CASE WHEN cost_month_incomplete THEN 1 ELSE 0 END)    AS cost_month_incomplete
FROM fitco_gold.fact_club_month
GROUP BY month_start, country, maturity;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_club_scorecard_latest AS
WITH ranked AS (
  SELECT m.*, ROW_NUMBER() OVER (PARTITION BY m.club_id ORDER BY m.month_start DESC) AS rn
  FROM fitco_gold.fact_club_month m
  WHERE NOT m.cost_month_incomplete
),
last3 AS (
  SELECT club_id,
    COUNT(*)                                                                AS months_in_window,
    AVG(avg_members)                                                        AS avg_members,
    SUM(maintenance_cost_eur) / NULLIF(SUM(avg_members), 0)                 AS maintenance_per_member_eur,
    SUM(energy_cost_eur) / NULLIF(SUM(avg_members), 0)                      AS energy_per_member_eur,
    SUM(corrective_tickets) * 1000.0 / NULLIF(SUM(avg_members), 0)          AS corrective_tickets_per_1000_members,
    SUM(downtime_hours)                                                     AS downtime_hours,
    AVG(ebitda_less_rent_proxy_eur)                                         AS avg_monthly_ebitda_less_rent_eur,
    SUM(ebitda_less_rent_proxy_eur) / NULLIF(SUM(total_revenue_eur), 0)     AS ebitda_less_rent_margin
  FROM ranked
  WHERE rn <= 3
  GROUP BY club_id
),
sla AS (
  SELECT f.club_id,
         SUM(CASE WHEN f.sla_met THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS sla_compliance_90d
  FROM fitco_gold.fact_maintenance_ticket f
  CROSS JOIN fitco_gold.report_params r
  WHERE f.is_sla_eligible AND CAST(f.closed_at AS DATE) > date_add(r.cutoff_date, -90)
  GROUP BY f.club_id
),
pm AS (
  SELECT p.club_id,
         SUM(CASE WHEN p.completed_on_time THEN 1 ELSE 0 END) * 1.0 / NULLIF(COUNT(*), 0) AS pm_on_time_rate_6m
  FROM fitco_gold.fact_preventive_plan p
  CROSS JOIN fitco_gold.report_params r
  WHERE p.is_due AND p.due_date > date_add(r.cutoff_date, -180)
  GROUP BY p.club_id
)
SELECT
  c.club_id, c.club_name, c.country, c.ownership_model, c.operating_model, c.maturity,
  l.months_in_window, l.avg_members, l.maintenance_per_member_eur, l.energy_per_member_eur,
  l.corrective_tickets_per_1000_members, l.downtime_hours,
  l.avg_monthly_ebitda_less_rent_eur, l.ebitda_less_rent_margin,
  s.sla_compliance_90d, p.pm_on_time_rate_6m
FROM fitco_gold.dim_club c
LEFT JOIN last3 l ON l.club_id = c.club_id
LEFT JOIN sla s   ON s.club_id = c.club_id
LEFT JOIN pm p    ON p.club_id = c.club_id;