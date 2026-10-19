# Databricks notebook source
# MAGIC %md
# MAGIC # 05 · Export Gold tables (backup / Power BI)
# MAGIC Writes every Gold table and key view to the `gold_exports` volume as **CSV** (one file each, Power BI / Excel friendly)
# MAGIC and **Parquet**, then builds one zip per format. Download from *Catalog → workspace → fitco_gold → Volumes → gold_exports*.

# COMMAND ----------

import os, shutil, zipfile
CATALOG, SCHEMA = "workspace", "fitco_gold"
EXPORT = "/Volumes/workspace/fitco_landing/gold_exports"
TABLES = ["dim_date", "dim_club", "dim_supplier", "dim_asset",
          "fact_maintenance_ticket", "fact_maintenance_cost", "fact_preventive_plan", "fact_club_month"]
VIEWS = ["v_kpi_monthly", "v_backlog_open", "v_preventive_completion", "v_headline_kpis",
         "v_asset_reliability", "v_model_failure_rates", "v_symptom_root_cause",
         "v_supplier_scorecard", "v_supplier_invoice_variance", "v_supplier_monthly_sla", "v_supplier_deterioration",
         "v_cost_monthly_invoice", "v_cost_by_ticket_month", "v_cost_driver_decomposition", "v_lifecycle_economics",
         "v_downtime_impact_estimate", "v_uninvoiced_tickets", "v_reconciliation_summary", "v_exceptions",
         "v_country_unit_economics", "v_club_scorecard_latest"]
batch = spark.sql("select date_format(current_timestamp(), 'yyyyMMdd_HHmm') b").first()["b"]
print("batch", batch)

# COMMAND ----------

def export_one(name):
    df = spark.table(f"{CATALOG}.{SCHEMA}.{name}")
    n = df.count()
    stage = f"/tmp/export_{batch}/{name}"
    # single CSV file (data is small), written to local /tmp first, then copied to the volume
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(f"file:{stage}_csv")
    df.coalesce(1).write.mode("overwrite").parquet(f"file:{stage}_pq")
    csv_part = [f for f in os.listdir(f"{stage}_csv") if f.endswith(".csv")][0]
    pq_part = [f for f in os.listdir(f"{stage}_pq") if f.endswith(".parquet")][0]
    os.makedirs(f"{EXPORT}/{batch}/csv", exist_ok=True)
    os.makedirs(f"{EXPORT}/{batch}/parquet", exist_ok=True)
    shutil.copy(f"{stage}_csv/{csv_part}", f"{EXPORT}/{batch}/csv/{name}.csv")
    shutil.copy(f"{stage}_pq/{pq_part}", f"{EXPORT}/{batch}/parquet/{name}.parquet")
    return name, n

results = [export_one(t) for t in TABLES + VIEWS]
for r in results:
    print(f"{r[0]:35s} {r[1]:>10,}")

# COMMAND ----------

# one zip per format, built in /tmp then copied (zip cannot be written directly into a volume)
for fmt in ("csv", "parquet"):
    zpath = f"/tmp/fitco_gold_{fmt}_{batch}.zip"
    with zipfile.ZipFile(zpath, "w", zipfile.ZIP_DEFLATED) as z:
        for f in sorted(os.listdir(f"{EXPORT}/{batch}/{fmt}")):
            z.write(f"{EXPORT}/{batch}/{fmt}/{f}", arcname=f)
    shutil.copy(zpath, f"{EXPORT}/fitco_gold_{fmt}_{batch}.zip")
display(dbutils.fs.ls(EXPORT))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Restore from backup (new workspace)
# MAGIC ```python
# MAGIC # upload the parquet folder to a volume, then:
# MAGIC for f in dbutils.fs.ls("/Volumes/workspace/fitco_landing/gold_exports/<batch>/parquet"):
# MAGIC     name = f.name.replace(".parquet", "")
# MAGIC     spark.read.parquet(f.path).write.mode("overwrite").saveAsTable(f"workspace.fitco_gold.{name}")
# MAGIC ```
# MAGIC Views are recreated by re-running `sql/10`–`15`.
