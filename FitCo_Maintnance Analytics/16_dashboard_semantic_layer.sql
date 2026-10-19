-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 16 · Dashboard semantic layer
-- MAGIC AI/BI dashboard datasets are **independent queries**: there is no relationship designer like Power BI. Two things connect them:
-- MAGIC 1. **Conformed, pre-joined datasets** (`v_dash_*`): the star schema is joined here, once, so one dataset can be sliced by any club / supplier / asset attribute.
-- MAGIC 2. **Shared field names** (`month_start`, `country`, `club_name`, `asset_category`, `supplier_name`) so one global filter can bind to many datasets.
-- MAGIC
-- MAGIC It also adds informational PK/FK constraints to the Gold tables (documents the model in Catalog Explorer; Unity Catalog does not enforce them).
-- MAGIC Measures are computed in the dashboard as ratios of sums using the 0/1 integer columns, never by averaging precomputed percentages.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

-- Grain: ticket. Use for volumes, SLA, MTTR, repeat failures, downtime, backlog, by any club/supplier/asset attribute.
CREATE OR REPLACE VIEW fitco_gold.v_dash_ticket AS
SELECT
  t.ticket_id, t.ticket_month AS month_start, t.opened_date, t.opened_at, t.closed_at,
  t.club_id, c.club_name, c.country, c.ownership_model, c.operating_model, c.maturity,
  t.asset_id, t.asset_category, t.asset_type, t.model_code, t.criticality,
  a.manufacturer, a.life_stage, a.proactive_service_programme, a.age_years_at_cutoff,
  t.supplier_id, COALESCE(s.supplier_name, CASE WHEN t.is_in_house THEN 'In-house' ELSE 'Unresolved' END) AS supplier_name,
  s.supplier_type,
  t.intervention_type, t.priority, t.status_group, t.status_raw, t.symptom, t.root_cause,
  t.sla_target_hours, t.resolution_hours, t.open_age_hours, t.downtime_hours,
  CASE WHEN t.intervention_type = 'Corrective' AND t.status_group <> 'Cancelled' THEN 1 ELSE 0 END AS corrective_flag,
  CASE WHEN t.intervention_type = 'Corrective' AND t.status_group = 'Open' THEN 1 ELSE 0 END      AS open_corrective_flag,
  CASE WHEN t.is_sla_eligible THEN 1 ELSE 0 END                                                   AS sla_eligible_flag,
  CASE WHEN t.is_sla_eligible AND t.sla_met THEN 1 ELSE 0 END                                     AS sla_met_flag,
  CASE WHEN t.intervention_type = 'Corrective' AND t.status_group = 'Open'
        AND t.open_age_hours > t.sla_target_hours THEN 1 ELSE 0 END                               AS open_past_sla_flag,
  CASE WHEN t.is_repeat_30d THEN 1 ELSE 0 END                                                     AS repeat_30d_flag,
  CASE WHEN t.is_repeat_7d THEN 1 ELSE 0 END                                                      AS repeat_7d_flag,
  CAST(t.closed_at AS DATE)                                                                       AS closed_date,
  date_trunc('month', CAST(t.closed_at AS DATE))                                                  AS closed_month
FROM fitco_gold.fact_maintenance_ticket t
LEFT JOIN fitco_gold.dim_club     c ON c.club_id     = t.club_id
LEFT JOIN fitco_gold.dim_asset    a ON a.asset_id    = t.asset_id
LEFT JOIN fitco_gold.dim_supplier s ON s.supplier_id = t.supplier_id;

-- COMMAND ----------

-- Grain: invoice line. month_start = INVOICE month. Count tickets with COUNT(DISTINCT ticket_id); never SUM ticket counts from here.
CREATE OR REPLACE VIEW fitco_gold.v_dash_cost AS
SELECT
  f.line_id, f.invoice_id, f.ticket_id, f.invoice_month AS month_start, f.ticket_month, f.invoice_date,
  f.club_id, c.club_name, f.country, f.ownership_model, c.operating_model, c.maturity,
  f.asset_id, f.asset_category, f.model_code, a.manufacturer, a.life_stage, a.proactive_service_programme,
  f.supplier_id, s.supplier_name, s.supplier_type,
  f.intervention_type, f.priority, f.line_type, f.cost_class, f.cost_borne_by, f.invoice_status,
  f.line_net_eur,
  CASE WHEN f.cost_class = 'REALISED' THEN f.line_net_eur ELSE 0 END AS realised_eur,
  CASE WHEN f.cost_class = 'PENDING'  THEN f.line_net_eur ELSE 0 END AS pending_eur,
  CASE WHEN f.cost_class = 'DISPUTED' THEN f.line_net_eur ELSE 0 END AS disputed_eur,
  CASE WHEN f.cost_class = 'REJECTED' THEN f.line_net_eur ELSE 0 END AS rejected_eur,
  CASE WHEN f.is_overcharge THEN 1 ELSE 0 END                         AS overcharge_flag,
  COALESCE(f.overcharge_eur, 0)                                       AS overcharge_eur,
  f.days_close_to_invoice
FROM fitco_gold.fact_maintenance_cost f
LEFT JOIN fitco_gold.dim_club     c ON c.club_id     = f.club_id
LEFT JOIN fitco_gold.dim_asset    a ON a.asset_id    = f.asset_id
LEFT JOIN fitco_gold.dim_supplier s ON s.supplier_id = f.supplier_id;

-- COMMAND ----------

-- Grain: preventive plan task
CREATE OR REPLACE VIEW fitco_gold.v_dash_preventive AS
SELECT
  p.plan_id, p.due_month AS month_start, p.due_date, p.completed_date, p.status, p.task_type,
  p.club_id, c.club_name, c.country, c.ownership_model, c.operating_model, c.maturity,
  p.asset_id, p.asset_category, p.criticality, a.manufacturer, a.life_stage, a.proactive_service_programme,
  CASE WHEN p.is_due THEN 1 ELSE 0 END                AS due_flag,
  CASE WHEN p.is_due AND p.completed_on_time THEN 1 ELSE 0 END AS on_time_flag,
  CASE WHEN p.is_due AND p.status = 'Completed Late' THEN 1 ELSE 0 END AS late_flag,
  CASE WHEN p.is_due AND p.status = 'Overdue' THEN 1 ELSE 0 END AS overdue_flag,
  p.days_overdue_at_cutoff, p.days_late
FROM fitco_gold.fact_preventive_plan p
LEFT JOIN fitco_gold.dim_club  c ON c.club_id  = p.club_id
LEFT JOIN fitco_gold.dim_asset a ON a.asset_id = p.asset_id;

-- COMMAND ----------

-- Grain: club x month (members, revenue, energy, maintenance). Ratios = SUM / SUM in the dashboard.
CREATE OR REPLACE VIEW fitco_gold.v_dash_club_month AS
SELECT m.*, c.club_name, c.city
FROM fitco_gold.fact_club_month m
LEFT JOIN fitco_gold.dim_club c ON c.club_id = m.club_id;

-- COMMAND ----------

-- Aggregated views get conformed month/field names so the global filters bind to them too
CREATE OR REPLACE VIEW fitco_gold.v_dash_supplier_monthly AS
SELECT m.supplier_id, m.supplier_name, s.supplier_type, m.closed_month AS month_start,
       m.sla_eligible_tickets, m.sla_met_tickets, m.median_resolution_h
FROM fitco_gold.v_supplier_monthly_sla m
LEFT JOIN fitco_gold.dim_supplier s ON s.supplier_id = m.supplier_id;

-- COMMAND ----------

-- Exceptions enriched with country / supplier name so they filter like everything else
CREATE OR REPLACE VIEW fitco_gold.v_dash_exceptions AS
SELECT e.*, c.country, c.ownership_model, s.supplier_name
FROM fitco_gold.v_exceptions e
LEFT JOIN fitco_gold.dim_club     c ON c.club_id     = e.club_id
LEFT JOIN fitco_gold.dim_supplier s ON s.supplier_id = e.supplier_id;

-- COMMAND ----------

-- SPARK_ONLY
-- Informational model documentation (Catalog Explorer -> table -> Overview shows keys; Catalog "Lineage / ER diagram")
-- Primary keys need NOT NULL columns. Constraints are NOT enforced by Unity Catalog.
ALTER TABLE fitco_gold.dim_club     ALTER COLUMN club_id     SET NOT NULL;
ALTER TABLE fitco_gold.dim_supplier ALTER COLUMN supplier_id SET NOT NULL;
ALTER TABLE fitco_gold.dim_asset    ALTER COLUMN asset_id    SET NOT NULL;
ALTER TABLE fitco_gold.dim_date     ALTER COLUMN date_day    SET NOT NULL;
ALTER TABLE fitco_gold.fact_maintenance_ticket ALTER COLUMN ticket_id SET NOT NULL;
ALTER TABLE fitco_gold.fact_maintenance_cost   ALTER COLUMN line_id   SET NOT NULL;
ALTER TABLE fitco_gold.fact_preventive_plan    ALTER COLUMN plan_id   SET NOT NULL;

-- COMMAND ----------

-- SPARK_ONLY
ALTER TABLE fitco_gold.dim_club     ADD CONSTRAINT pk_dim_club     PRIMARY KEY (club_id);
ALTER TABLE fitco_gold.dim_supplier ADD CONSTRAINT pk_dim_supplier PRIMARY KEY (supplier_id);
ALTER TABLE fitco_gold.dim_asset    ADD CONSTRAINT pk_dim_asset    PRIMARY KEY (asset_id);
ALTER TABLE fitco_gold.dim_date     ADD CONSTRAINT pk_dim_date     PRIMARY KEY (date_day);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT pk_fact_ticket  PRIMARY KEY (ticket_id);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT pk_fact_cost    PRIMARY KEY (line_id);
ALTER TABLE fitco_gold.fact_preventive_plan    ADD CONSTRAINT pk_fact_plan    PRIMARY KEY (plan_id);

-- COMMAND ----------

-- SPARK_ONLY
-- No FK from fact tables to dim_asset: orphan-asset tickets are kept on purpose (category 'Unknown'), so that key is not guaranteed.
ALTER TABLE fitco_gold.dim_asset               ADD CONSTRAINT fk_asset_club   FOREIGN KEY (club_id) REFERENCES fitco_gold.dim_club (club_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_club  FOREIGN KEY (club_id) REFERENCES fitco_gold.dim_club (club_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_supp  FOREIGN KEY (supplier_id) REFERENCES fitco_gold.dim_supplier (supplier_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_open  FOREIGN KEY (opened_date) REFERENCES fitco_gold.dim_date (date_day);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_club    FOREIGN KEY (club_id) REFERENCES fitco_gold.dim_club (club_id);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_supp    FOREIGN KEY (supplier_id) REFERENCES fitco_gold.dim_supplier (supplier_id);
ALTER TABLE fitco_gold.fact_preventive_plan    ADD CONSTRAINT fk_plan_club    FOREIGN KEY (club_id) REFERENCES fitco_gold.dim_club (club_id);
ALTER TABLE fitco_gold.fact_preventive_plan    ADD CONSTRAINT fk_plan_asset   FOREIGN KEY (asset_id) REFERENCES fitco_gold.dim_asset (asset_id);