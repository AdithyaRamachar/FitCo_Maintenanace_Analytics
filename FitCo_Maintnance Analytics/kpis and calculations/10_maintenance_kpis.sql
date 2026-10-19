-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 10 · Workstream A — Maintenance performance
-- MAGIC **Question:** are issues resolved efficiently and is preventive work keeping pace?
-- MAGIC **Manager action:** escalate clubs/suppliers with falling SLA, ageing backlog or overdue preventive tasks.
-- MAGIC
-- MAGIC *KPI definitions:* corrective volume = corrective tickets opened in the month, cancelled excluded.
-- MAGIC SLA compliance = eligible closed corrective tickets resolved within the policy SLA ÷ all eligible closed corrective tickets.
-- MAGIC MTTR is shown as mean **and** median (a few parts-delay tickets drag the mean).
-- MAGIC `v_kpi_monthly` is an *opened-month cohort*: tickets still open are not in SLA/MTTR yet, so the latest 1-2 months look better than they will end up
-- MAGIC (see `still_open_tickets`). `v_headline_kpis` measures SLA/MTTR on tickets **resolved** in the period to avoid that bias.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.v_kpi_monthly AS
SELECT
  f.ticket_month                                                   AS month_start,
  c.club_id, c.club_name, c.country, c.operating_model, c.ownership_model,
  SUM(CASE WHEN f.intervention_type = 'Corrective' AND f.status_group <> 'Cancelled' THEN 1 ELSE 0 END) AS corrective_tickets,
  SUM(CASE WHEN f.intervention_type = 'Preventive' THEN 1 ELSE 0 END)                                    AS preventive_tickets,
  SUM(CASE WHEN f.intervention_type = 'Refurbishment' THEN 1 ELSE 0 END)                                 AS refurbishment_tickets,
  SUM(CASE WHEN f.is_sla_eligible THEN 1 ELSE 0 END)                                                     AS sla_eligible_tickets,
  SUM(CASE WHEN f.is_sla_eligible AND f.sla_met THEN 1 ELSE 0 END)                                       AS sla_met_tickets,
  SUM(CASE WHEN f.is_sla_eligible AND f.sla_met THEN 1 ELSE 0 END) * 1.0
    / NULLIF(SUM(CASE WHEN f.is_sla_eligible THEN 1 ELSE 0 END), 0)                                      AS sla_compliance,
  AVG(CASE WHEN f.is_sla_eligible THEN f.resolution_hours END)                                           AS mttr_hours_mean,
  MEDIAN(CASE WHEN f.is_sla_eligible THEN f.resolution_hours END)                                        AS mttr_hours_median,
  SUM(CASE WHEN f.intervention_type = 'Corrective' AND f.status_group = 'Open' THEN 1 ELSE 0 END)       AS still_open_tickets,
  SUM(CASE WHEN f.is_repeat_30d THEN 1 ELSE 0 END)                                                       AS repeat_tickets_30d,
  SUM(COALESCE(f.downtime_hours, 0))                                                                     AS downtime_hours
FROM fitco_gold.fact_maintenance_ticket f
JOIN fitco_gold.dim_club c ON c.club_id = f.club_id
GROUP BY f.ticket_month, c.club_id, c.club_name, c.country, c.operating_model, c.ownership_model;

-- COMMAND ----------

-- Backlog at the reporting cutoff: open corrective tickets, ageing bucket and SLA status
CREATE OR REPLACE VIEW fitco_gold.v_backlog_open AS
SELECT
  f.ticket_id, f.club_id, c.club_name, c.country, f.asset_id, f.asset_category, f.priority,
  f.status_raw, f.supplier_id, f.symptom, f.opened_at, f.sla_target_hours,
  ROUND(f.open_age_hours, 1)                                         AS open_age_hours,
  ROUND(f.open_age_hours / 24.0, 1)                                  AS open_age_days,
  CASE WHEN f.open_age_hours < 24  THEN '1 <1 day'
       WHEN f.open_age_hours < 72  THEN '2 1-3 days'
       WHEN f.open_age_hours < 168 THEN '3 3-7 days'
       WHEN f.open_age_hours < 336 THEN '4 7-14 days'
       ELSE '5 >14 days' END                                         AS age_bucket,
  (f.open_age_hours > f.sla_target_hours)                            AS is_past_sla,
  f.open_age_hours / f.sla_target_hours                              AS sla_overrun_ratio
FROM fitco_gold.fact_maintenance_ticket f
JOIN fitco_gold.dim_club c ON c.club_id = f.club_id
WHERE f.status_group = 'Open' AND f.intervention_type = 'Corrective';

-- COMMAND ----------

-- Preventive completion by due month: tasks due, completed on time, late and still overdue
CREATE OR REPLACE VIEW fitco_gold.v_preventive_completion AS
SELECT
  p.due_month,
  c.club_id, c.club_name, c.country, p.asset_category,
  COUNT(*)                                                         AS tasks_due,
  SUM(CASE WHEN p.completed_on_time THEN 1 ELSE 0 END)             AS completed_on_time,
  SUM(CASE WHEN p.status = 'Completed Late' THEN 1 ELSE 0 END)     AS completed_late,
  SUM(CASE WHEN p.status = 'Overdue' THEN 1 ELSE 0 END)            AS still_overdue,
  SUM(CASE WHEN p.completed_on_time THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS on_time_rate
FROM fitco_gold.fact_preventive_plan p
JOIN fitco_gold.dim_club c ON c.club_id = p.club_id
WHERE p.is_due
GROUP BY p.due_month, c.club_id, c.club_name, c.country, p.asset_category;

-- COMMAND ----------

-- Headline KPI card values at the cutoff: last 30 days vs the 30 days before.
-- Volume = tickets opened in the period; SLA and MTTR = tickets resolved (closed) in the period.
CREATE OR REPLACE VIEW fitco_gold.v_headline_kpis AS
WITH vol AS (
  SELECT
    CASE WHEN f.opened_date > date_add(r.cutoff_date, -30) THEN 'last_30d'
         WHEN f.opened_date > date_add(r.cutoff_date, -60) THEN 'prior_30d' END AS period,
    COUNT(*) AS corrective_tickets
  FROM fitco_gold.fact_maintenance_ticket f
  CROSS JOIN fitco_gold.report_params r
  WHERE f.intervention_type = 'Corrective' AND f.status_group <> 'Cancelled'
  GROUP BY 1
),
res AS (
  SELECT
    CASE WHEN CAST(f.closed_at AS DATE) > date_add(r.cutoff_date, -30) THEN 'last_30d'
         WHEN CAST(f.closed_at AS DATE) > date_add(r.cutoff_date, -60) THEN 'prior_30d' END AS period,
    SUM(CASE WHEN f.sla_met THEN 1 ELSE 0 END) * 1.0 / COUNT(*) AS sla_compliance,
    MEDIAN(f.resolution_hours)                                  AS mttr_hours_median
  FROM fitco_gold.fact_maintenance_ticket f
  CROSS JOIN fitco_gold.report_params r
  WHERE f.is_sla_eligible
  GROUP BY 1
)
SELECT v.period, v.corrective_tickets, s.sla_compliance, s.mttr_hours_median
FROM vol v
JOIN res s ON s.period = v.period
WHERE v.period IS NOT NULL;