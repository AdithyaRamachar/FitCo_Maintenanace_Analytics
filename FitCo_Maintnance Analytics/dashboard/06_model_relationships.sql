-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 06 · Data model — keys, relationships, semantic views
-- MAGIC Run **after** `03_gold_model` (and after 10–15). Safe to re-run; **re-run it whenever 03 is re-run** because `CREATE OR REPLACE TABLE` drops constraints.
-- MAGIC
-- MAGIC Unity Catalog PK/FK constraints are *informational* (not enforced). They power the Catalog Explorer relationship diagram, Genie/AI-BI context and
-- MAGIC are what Power BI picks up via a connector. Dashboard datasets use the `v_dash_*` views from `sql/16`; `sem_ticket` and `sem_cost` here are ready-joined views for ad-hoc SQL and Power BI.
-- MAGIC
-- MAGIC | Child | Column | → Parent | Column | Note |
-- MAGIC |---|---|---|---|---|
-- MAGIC | dim_asset | club_id | dim_club | club_id | |
-- MAGIC | fact_maintenance_ticket | club_id / asset_id / supplier_id / opened_date | dim_club / dim_asset / dim_supplier / dim_date | | supplier_id NULL = in-house work |
-- MAGIC | fact_maintenance_cost | ticket_id / club_id / asset_id / supplier_id / invoice_date | fact_maintenance_ticket / dim_club / dim_asset / dim_supplier / dim_date | | |
-- MAGIC | fact_preventive_plan | asset_id / club_id / due_date | dim_asset / dim_club / dim_date | | |
-- MAGIC | fact_club_month | club_id / month_start | dim_club / dim_date | | PK (club_id, month_start) |

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

-- 1. Referential integrity: 164 tickets and 220 cost lines point at asset_ids that are not in the asset register (injected defect).
--    Standard fix: add them as "inferred members" of dim_asset (category 'Unknown'), so the FK holds and the spend still reconciles.
INSERT INTO fitco_gold.dim_asset (asset_id, club_id, asset_category, asset_type, operating_status, life_stage)
SELECT x.asset_id, MIN(x.club_id), 'Unknown', 'Unknown (not in asset register)', 'Unknown', 'Unknown'
FROM (
  SELECT asset_id, club_id FROM fitco_gold.fact_maintenance_ticket WHERE asset_id IS NOT NULL
  UNION ALL
  SELECT asset_id, club_id FROM fitco_gold.fact_maintenance_cost   WHERE asset_id IS NOT NULL
) x
LEFT JOIN fitco_gold.dim_asset a ON a.asset_id = x.asset_id
WHERE a.asset_id IS NULL
GROUP BY x.asset_id;

-- COMMAND ----------

-- 2a. Integrity checks (all should return 0)
SELECT 'ticket->asset'   AS rel, COUNT(*) AS orphans FROM fitco_gold.fact_maintenance_ticket t LEFT JOIN fitco_gold.dim_asset a ON a.asset_id=t.asset_id WHERE t.asset_id IS NOT NULL AND a.asset_id IS NULL
UNION ALL SELECT 'cost->asset',     COUNT(*) FROM fitco_gold.fact_maintenance_cost c LEFT JOIN fitco_gold.dim_asset a ON a.asset_id=c.asset_id WHERE c.asset_id IS NOT NULL AND a.asset_id IS NULL
UNION ALL SELECT 'cost->ticket',    COUNT(*) FROM fitco_gold.fact_maintenance_cost c LEFT JOIN fitco_gold.fact_maintenance_ticket t ON t.ticket_id=c.ticket_id WHERE t.ticket_id IS NULL
UNION ALL SELECT 'cost->date',      COUNT(*) FROM fitco_gold.fact_maintenance_cost c LEFT JOIN fitco_gold.dim_date d ON d.date_day=c.invoice_date WHERE c.invoice_date IS NOT NULL AND d.date_day IS NULL
UNION ALL SELECT 'ticket->date',    COUNT(*) FROM fitco_gold.fact_maintenance_ticket t LEFT JOIN fitco_gold.dim_date d ON d.date_day=t.opened_date WHERE d.date_day IS NULL
UNION ALL SELECT 'plan->date',      COUNT(*) FROM fitco_gold.fact_preventive_plan p LEFT JOIN fitco_gold.dim_date d ON d.date_day=p.due_date WHERE p.due_date IS NOT NULL AND d.date_day IS NULL
UNION ALL SELECT 'clubmonth->date', COUNT(*) FROM fitco_gold.fact_club_month m LEFT JOIN fitco_gold.dim_date d ON d.date_day=m.month_start WHERE d.date_day IS NULL;

-- COMMAND ----------

-- 2b. Primary keys: columns must be NOT NULL first.
-- SPARK_ONLY
ALTER TABLE fitco_gold.dim_date ALTER COLUMN date_day SET NOT NULL;
ALTER TABLE fitco_gold.dim_club ALTER COLUMN club_id SET NOT NULL;
ALTER TABLE fitco_gold.dim_supplier ALTER COLUMN supplier_id SET NOT NULL;
ALTER TABLE fitco_gold.dim_asset ALTER COLUMN asset_id SET NOT NULL;
ALTER TABLE fitco_gold.fact_maintenance_ticket ALTER COLUMN ticket_id SET NOT NULL;
ALTER TABLE fitco_gold.fact_maintenance_cost ALTER COLUMN line_id SET NOT NULL;
ALTER TABLE fitco_gold.fact_preventive_plan ALTER COLUMN plan_id SET NOT NULL;
ALTER TABLE fitco_gold.fact_club_month ALTER COLUMN club_id SET NOT NULL;
ALTER TABLE fitco_gold.fact_club_month ALTER COLUMN month_start SET NOT NULL;

-- COMMAND ----------

-- SPARK_ONLY
ALTER TABLE fitco_gold.dim_date DROP CONSTRAINT IF EXISTS pk_dim_date CASCADE; ALTER TABLE fitco_gold.dim_date ADD CONSTRAINT pk_dim_date PRIMARY KEY (date_day);
ALTER TABLE fitco_gold.dim_club DROP CONSTRAINT IF EXISTS pk_dim_club CASCADE; ALTER TABLE fitco_gold.dim_club ADD CONSTRAINT pk_dim_club PRIMARY KEY (club_id);
ALTER TABLE fitco_gold.dim_supplier DROP CONSTRAINT IF EXISTS pk_dim_supplier CASCADE; ALTER TABLE fitco_gold.dim_supplier ADD CONSTRAINT pk_dim_supplier PRIMARY KEY (supplier_id);
ALTER TABLE fitco_gold.dim_asset DROP CONSTRAINT IF EXISTS pk_dim_asset CASCADE; ALTER TABLE fitco_gold.dim_asset ADD CONSTRAINT pk_dim_asset PRIMARY KEY (asset_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket DROP CONSTRAINT IF EXISTS pk_fact_ticket CASCADE; ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT pk_fact_ticket PRIMARY KEY (ticket_id);
ALTER TABLE fitco_gold.fact_maintenance_cost DROP CONSTRAINT IF EXISTS pk_fact_cost CASCADE; ALTER TABLE fitco_gold.fact_maintenance_cost ADD CONSTRAINT pk_fact_cost PRIMARY KEY (line_id);
ALTER TABLE fitco_gold.fact_preventive_plan DROP CONSTRAINT IF EXISTS pk_fact_plan CASCADE; ALTER TABLE fitco_gold.fact_preventive_plan ADD CONSTRAINT pk_fact_plan PRIMARY KEY (plan_id);
ALTER TABLE fitco_gold.fact_club_month DROP CONSTRAINT IF EXISTS pk_fact_club_month CASCADE; ALTER TABLE fitco_gold.fact_club_month ADD CONSTRAINT pk_fact_club_month PRIMARY KEY (club_id, month_start);

-- COMMAND ----------

-- 2c. Foreign keys (parents first, so PKs above must exist).
-- SPARK_ONLY
ALTER TABLE fitco_gold.dim_asset               ADD CONSTRAINT fk_asset_club       FOREIGN KEY (club_id)      REFERENCES fitco_gold.dim_club(club_id);

ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_club      FOREIGN KEY (club_id)      REFERENCES fitco_gold.dim_club(club_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_asset     FOREIGN KEY (asset_id)     REFERENCES fitco_gold.dim_asset(asset_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_supplier  FOREIGN KEY (supplier_id)  REFERENCES fitco_gold.dim_supplier(supplier_id);
ALTER TABLE fitco_gold.fact_maintenance_ticket ADD CONSTRAINT fk_ticket_date      FOREIGN KEY (opened_date)  REFERENCES fitco_gold.dim_date(date_day);

ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_ticket      FOREIGN KEY (ticket_id)    REFERENCES fitco_gold.fact_maintenance_ticket(ticket_id);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_club        FOREIGN KEY (club_id)      REFERENCES fitco_gold.dim_club(club_id);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_asset       FOREIGN KEY (asset_id)     REFERENCES fitco_gold.dim_asset(asset_id);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_supplier    FOREIGN KEY (supplier_id)  REFERENCES fitco_gold.dim_supplier(supplier_id);
ALTER TABLE fitco_gold.fact_maintenance_cost   ADD CONSTRAINT fk_cost_date        FOREIGN KEY (invoice_date) REFERENCES fitco_gold.dim_date(date_day);

ALTER TABLE fitco_gold.fact_preventive_plan    ADD CONSTRAINT fk_plan_asset       FOREIGN KEY (asset_id)     REFERENCES fitco_gold.dim_asset(asset_id);
ALTER TABLE fitco_gold.fact_preventive_plan    ADD CONSTRAINT fk_plan_club        FOREIGN KEY (club_id)      REFERENCES fitco_gold.dim_club(club_id);
ALTER TABLE fitco_gold.fact_preventive_plan    ADD CONSTRAINT fk_plan_due_date    FOREIGN KEY (due_date)     REFERENCES fitco_gold.dim_date(date_day);

ALTER TABLE fitco_gold.fact_club_month         ADD CONSTRAINT fk_cm_club          FOREIGN KEY (club_id)      REFERENCES fitco_gold.dim_club(club_id);
ALTER TABLE fitco_gold.fact_club_month         ADD CONSTRAINT fk_cm_month         FOREIGN KEY (month_start)  REFERENCES fitco_gold.dim_date(date_day);

-- COMMAND ----------

-- 2d. Verify the constraints were registered
-- SPARK_ONLY
SELECT table_name, constraint_name, constraint_type
FROM workspace.information_schema.table_constraints
WHERE table_schema = 'fitco_gold' AND constraint_type IN ('PRIMARY KEY','FOREIGN KEY')
ORDER BY constraint_type DESC, table_name, constraint_name;

-- COMMAND ----------

-- 3. Semantic (pre-joined) views for dashboard datasets and ad-hoc analysis. Grain stays one row per ticket / per invoice line.
CREATE OR REPLACE VIEW fitco_gold.sem_ticket AS
SELECT
  t.ticket_id, t.opened_at, t.opened_date, t.ticket_month, t.closed_at,
  t.intervention_type, t.priority, t.status_group, t.symptom, t.root_cause,
  t.sla_target_hours, t.resolution_hours, t.is_sla_eligible, t.sla_met, t.open_age_hours,
  t.downtime_hours, t.is_repeat_7d, t.is_repeat_30d, t.is_in_house,
  c.club_id, c.club_name, c.city, c.country, c.ownership_model, c.operating_model, c.maturity,
  a.asset_id, a.asset_category, a.asset_type, a.manufacturer, a.model_code, a.criticality,
  a.age_years_at_cutoff, a.life_stage, a.proactive_service_programme,
  s.supplier_id, s.supplier_name, s.supplier_type, s.preferred_supplier
FROM fitco_gold.fact_maintenance_ticket t
JOIN fitco_gold.dim_club c ON c.club_id = t.club_id
LEFT JOIN fitco_gold.dim_asset a ON a.asset_id = t.asset_id
LEFT JOIN fitco_gold.dim_supplier s ON s.supplier_id = t.supplier_id;

-- COMMAND ----------

CREATE OR REPLACE VIEW fitco_gold.sem_cost AS
SELECT
  f.line_id, f.invoice_id, f.ticket_id, f.line_type, f.line_net_eur, f.cost_class, f.cost_borne_by,
  f.invoice_date, f.invoice_month, f.invoice_status, f.is_overcharge, f.overcharge_eur,
  c.club_id, c.club_name, c.country, c.ownership_model, c.maturity,
  a.asset_id, a.asset_category, a.manufacturer, a.model_code, a.life_stage, a.proactive_service_programme,
  s.supplier_id, s.supplier_name, s.supplier_type,
  t.intervention_type, t.priority, t.ticket_month
FROM fitco_gold.fact_maintenance_cost f
JOIN fitco_gold.dim_club c ON c.club_id = f.club_id
LEFT JOIN fitco_gold.dim_asset a ON a.asset_id = f.asset_id
LEFT JOIN fitco_gold.dim_supplier s ON s.supplier_id = f.supplier_id
LEFT JOIN fitco_gold.fact_maintenance_ticket t ON t.ticket_id = f.ticket_id;

-- COMMAND ----------

-- Reconciliation guards: the semantic views must not change row counts or totals.
SELECT 'sem_ticket rows'  AS chk, (SELECT COUNT(*) FROM fitco_gold.sem_ticket) AS got, (SELECT COUNT(*) FROM fitco_gold.fact_maintenance_ticket) AS expected
UNION ALL SELECT 'sem_cost rows', (SELECT COUNT(*) FROM fitco_gold.sem_cost), (SELECT COUNT(*) FROM fitco_gold.fact_maintenance_cost)
UNION ALL SELECT 'sem_cost realised EUR', (SELECT ROUND(SUM(line_net_eur)) FROM fitco_gold.sem_cost WHERE cost_class='REALISED'), (SELECT ROUND(SUM(line_net_eur)) FROM fitco_gold.fact_maintenance_cost WHERE cost_class='REALISED');