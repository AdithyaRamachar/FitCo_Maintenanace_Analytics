-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 02 · Silver — typed, validated, flagged
-- MAGIC **Rules applied here (documented in `docs/metric_definitions.md`):**
-- MAGIC * Types are enforced; unparseable values become NULL and are flagged, never silently dropped.
-- MAGIC * Duplicate tickets: same `source_ref` → keep the earliest `opened_at`, flag the rest `is_duplicate`.
-- MAGIC * Supplier names are normalised to a key (upper-case, punctuation and legal suffix removed) and mapped to `supplier_id`.
-- MAGIC * If `closed_at < opened_at` the two timestamps are swapped and the row flagged `timestamps_swapped`.
-- MAGIC * SLA target is re-derived from priority (policy: P1 24h, P2 72h, P3 168h, P4 336h) and the raw value is only used to flag gaps.
-- MAGIC * Invoice lines: status/currency upper-cased, mixed date formats parsed, exact duplicate lines and orphan ticket references flagged.
-- MAGIC * Nothing is deleted. Gold decides which flagged rows are excluded.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.clubs AS
SELECT
  club_id, club_name, city, country, ownership_model, operating_model,
  CAST(opened_date AS DATE)            AS opened_date,
  CAST(opening_hours_per_week AS INT)  AS opening_hours_per_week,
  CAST(staffed_hours_per_week AS INT)  AS staffed_hours_per_week,
  CAST(floor_area_m2 AS INT)           AS floor_area_m2
FROM fitco_bronze.clubs;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.suppliers AS
SELECT
  supplier_id, supplier_name, supplier_type, countries_served, categories_served,
  preferred_supplier, active,
  regexp_replace(regexp_replace(upper(supplier_name), '[^A-Z0-9]', ''), '(BV|SL|SAS|GMBH)$', '') AS name_key
FROM fitco_bronze.suppliers;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.supplier_contracts AS
SELECT
  contract_id, supplier_id, country, asset_category,
  CAST(labour_rate_eur_hr AS DOUBLE)          AS labour_rate_eur_hr,
  CAST(callout_fee_eur AS DOUBLE)             AS callout_fee_eur,
  CAST(preventive_visit_fee_eur AS DOUBLE)    AS preventive_visit_fee_eur,
  CAST(sla_p1_hours AS INT) AS sla_p1_hours, CAST(sla_p2_hours AS INT) AS sla_p2_hours,
  CAST(sla_p3_hours AS INT) AS sla_p3_hours, CAST(sla_p4_hours AS INT) AS sla_p4_hours,
  CAST(contract_start AS DATE) AS contract_start, CAST(contract_end AS DATE) AS contract_end
FROM fitco_bronze.supplier_contracts;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.assets AS
SELECT
  asset_id, club_id, asset_category, asset_type, manufacturer, model_code, serial_number,
  CAST(install_date AS DATE)                AS install_date,
  CAST(warranty_end_date AS DATE)           AS warranty_end_date,
  CAST(expected_life_years AS INT)          AS expected_life_years,
  proactive_service_programme, criticality,
  CAST(replacement_cost_eur AS DECIMAL(12,2)) AS replacement_cost_eur,
  operating_status,
  CAST(decommission_date AS DATE)           AS decommission_date,
  (warranty_end_date IS NULL)               AS warranty_missing
FROM fitco_bronze.assets;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.maintenance_plans AS
SELECT
  plan_id, asset_id, club_id, task_type,
  CAST(planned_date AS DATE)   AS planned_date,
  CAST(due_date AS DATE)       AS due_date,
  CAST(completed_date AS DATE) AS completed_date,
  status                       AS status_raw,
  linked_ticket_id
FROM fitco_bronze.maintenance_plans;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.maintenance_tickets AS
WITH parsed AS (
  SELECT
    ticket_id, source_ref, asset_id, club_id, intervention_type, priority,
    status AS status_raw, raised_channel,
    try_cast(opened_at AS TIMESTAMP) AS opened_at_raw,
    try_cast(closed_at AS TIMESTAMP) AS closed_at_raw,
    supplier_id AS supplier_id_raw, supplier_name_raw,
    regexp_replace(regexp_replace(upper(supplier_name_raw), '[^A-Z0-9]', ''), '(BV|SL|SAS|GMBH)$', '') AS supplier_name_key,
    symptom, root_cause, root_cause_confirmed,
    try_cast(downtime_hours AS DOUBLE)  AS downtime_hours,
    try_cast(sla_target_hours AS INT)   AS sla_target_hours_raw,
    plan_id
  FROM fitco_bronze.maintenance_tickets
),
ranked AS (
  SELECT p.*,
    row_number() OVER (PARTITION BY source_ref ORDER BY opened_at_raw, ticket_id) AS rn_source
  FROM parsed p
),
fixed AS (
  SELECT r.*,
    CASE WHEN closed_at_raw IS NOT NULL AND closed_at_raw < opened_at_raw THEN closed_at_raw ELSE opened_at_raw END AS opened_at,
    CASE WHEN closed_at_raw IS NOT NULL AND closed_at_raw < opened_at_raw THEN opened_at_raw ELSE closed_at_raw END AS closed_at,
    (closed_at_raw IS NOT NULL AND closed_at_raw < opened_at_raw) AS timestamps_swapped
  FROM ranked r
)
SELECT
  f.ticket_id, f.source_ref, f.asset_id, f.club_id, f.intervention_type, f.priority,
  f.status_raw,
  CASE WHEN f.status_raw = 'Cancelled' THEN 'Cancelled'
       WHEN f.status_raw = 'Closed'    THEN 'Closed'
       ELSE 'Open' END                                   AS status_group,
  f.raised_channel, f.opened_at, f.closed_at,
  COALESCE(f.supplier_id_raw, s.supplier_id)             AS supplier_id,
  f.supplier_name_raw,
  f.symptom, f.root_cause, f.root_cause_confirmed, f.downtime_hours,
  f.sla_target_hours_raw,
  CASE WHEN f.intervention_type = 'Corrective' THEN
         CASE f.priority WHEN 'P1' THEN 24 WHEN 'P2' THEN 72 WHEN 'P3' THEN 168 WHEN 'P4' THEN 336 END
  END                                                    AS sla_target_hours,
  f.plan_id,
  CASE WHEN f.closed_at IS NOT NULL
       THEN (unix_timestamp(f.closed_at) - unix_timestamp(f.opened_at)) / 3600.0 END AS resolution_hours,
  (f.rn_source > 1)                                      AS is_duplicate,
  (a.asset_id IS NULL)                                   AS is_orphan_asset,
  (f.status_raw = 'Closed' AND f.closed_at_raw IS NULL)  AS missing_closed_at,
  f.timestamps_swapped,
  (f.supplier_id_raw IS NULL AND f.supplier_name_raw IS NOT NULL AND s.supplier_id IS NULL) AS supplier_unresolved,
  (f.supplier_id_raw IS NULL AND f.supplier_name_raw IS NULL)  AS is_in_house,
  (f.supplier_id_raw IS NULL AND s.supplier_id IS NOT NULL)    AS supplier_resolved_by_name,
  (f.intervention_type = 'Corrective' AND f.sla_target_hours_raw IS NULL) AS sla_missing,
  (f.intervention_type = 'Corrective' AND f.sla_target_hours_raw IS NOT NULL AND f.sla_target_hours_raw <>
     CASE f.priority WHEN 'P1' THEN 24 WHEN 'P2' THEN 72 WHEN 'P3' THEN 168 WHEN 'P4' THEN 336 END) AS sla_inconsistent,
  (f.status_raw = 'Closed' AND f.downtime_hours IS NULL) AS downtime_missing
FROM fixed f
LEFT JOIN fitco_silver.suppliers s ON s.name_key = f.supplier_name_key
LEFT JOIN fitco_silver.assets a    ON a.asset_id = f.asset_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.invoice_lines AS
WITH parsed AS (
  SELECT
    line_id, invoice_id, ticket_id, supplier_id, line_type,
    CAST(quantity AS DOUBLE)            AS quantity,
    CAST(unit_rate_eur AS DECIMAL(14,2)) AS unit_rate_eur,
    CAST(line_net_eur AS DECIMAL(14,2))  AS line_net_eur,
    CAST(tax_rate AS DOUBLE)            AS tax_rate,
    CAST(tax_eur AS DECIMAL(14,2))      AS tax_eur,
    CAST(line_total_eur AS DECIMAL(14,2)) AS line_total_eur,
    upper(trim(currency))               AS currency,
    CASE WHEN invoice_date LIKE '____-__-__' THEN CAST(invoice_date AS DATE)
         WHEN invoice_date LIKE '__/__/____' THEN try_to_date(invoice_date, 'dd/MM/yyyy')
    END                                 AS invoice_date,
    upper(trim(invoice_status))         AS invoice_status,
    contract_id, cost_borne_by
  FROM fitco_bronze.invoice_lines
),
ranked AS (
  SELECT p.*,
    row_number() OVER (PARTITION BY invoice_id, ticket_id, line_type, line_net_eur, quantity
                       ORDER BY line_id) AS rn_dup
  FROM parsed p
)
SELECT
  r.line_id, r.invoice_id, r.ticket_id, r.supplier_id, r.line_type, r.quantity, r.unit_rate_eur,
  r.line_net_eur, r.tax_rate, r.tax_eur, r.line_total_eur, r.currency, r.invoice_date,
  r.invoice_status, r.contract_id, r.cost_borne_by,
  (r.rn_dup > 1)                                         AS is_duplicate_line,
  (vt.ticket_id IS NULL)                                 AS is_orphan_ticket,
  (r.invoice_date IS NULL)                               AS invoice_date_unparsed,
  (r.currency <> 'EUR')                                  AS currency_not_eur,
  (abs(CAST(r.line_net_eur + r.tax_eur - r.line_total_eur AS DOUBLE)) > 0.02) AS total_mismatch
FROM ranked r
LEFT JOIN (SELECT DISTINCT ticket_id FROM fitco_silver.maintenance_tickets WHERE NOT is_duplicate) vt
  ON vt.ticket_id = r.ticket_id;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.club_monthly_kpis AS
SELECT
  club_id, CAST(month_start AS DATE) AS month_start,
  CAST(members_end AS INT) AS members_end, CAST(new_members AS INT) AS new_members,
  CAST(cancellations AS INT) AS cancellations, CAST(avg_members AS DOUBLE) AS avg_members,
  CAST(arpu_eur AS DOUBLE) AS arpu_eur,
  CAST(membership_revenue_eur AS DOUBLE) AS membership_revenue_eur,
  CAST(other_revenue_eur AS DOUBLE) AS other_revenue_eur,
  CAST(staff_cost_eur AS DOUBLE) AS staff_cost_eur, CAST(rent_eur AS DOUBLE) AS rent_eur,
  CAST(other_opex_eur AS DOUBLE) AS other_opex_eur, CAST(member_visits AS INT) AS member_visits
FROM fitco_bronze.club_monthly_kpis;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.energy_monthly AS
SELECT
  club_id, CAST(month_start AS DATE) AS month_start, CAST(kwh AS BIGINT) AS kwh,
  CAST(avg_price_eur_kwh AS DOUBLE) AS avg_price_eur_kwh, CAST(energy_cost_eur AS DOUBLE) AS energy_cost_eur
FROM fitco_bronze.energy_monthly;

-- COMMAND ----------

CREATE OR REPLACE TABLE fitco_silver.maintenance_responsibility_matrix AS
SELECT asset_category, ownership_model, responsible_party, service_provider, cost_borne_by, notes
FROM fitco_bronze.maintenance_responsibility_matrix;