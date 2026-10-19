# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 01 · Bronze — raw ingestion
# MAGIC Loads the 10 generated CSVs from the `raw_csv` volume into Delta tables **exactly as received**:
# MAGIC every column stays a string, nothing is cleaned, and three lineage columns are added
# MAGIC (`_source_file`, `_batch_id`, `_ingested_at`). Re-running overwrites Bronze with a new batch id.

# COMMAND ----------

from datetime import datetime
from pyspark.sql import functions as F

CATALOG = "workspace"          # change if your Free Edition catalog has another name
RAW_PATH = f"/Volumes/{CATALOG}/fitco_landing/raw_csv"
BATCH_ID = datetime.utcnow().strftime("%Y%m%d%H%M%S")

spark.sql(f"USE CATALOG {CATALOG}")

TABLES = [
    "clubs", "assets", "suppliers", "supplier_contracts", "maintenance_tickets",
    "maintenance_plans", "invoice_lines", "club_monthly_kpis", "energy_monthly",
    "maintenance_responsibility_matrix",
]

# COMMAND ----------

results = []
for t in TABLES:
    path = f"{RAW_PATH}/{t}.csv"
    df = (
        spark.read.option("header", True)
        .option("inferSchema", False)          # keep raw strings; typing happens in Silver
        .option("multiLine", False)
        .csv(path)
    )
    df = (
        df.withColumn("_source_file", F.col("_metadata.file_path"))
        .withColumn("_batch_id", F.lit(BATCH_ID))
        .withColumn("_ingested_at", F.current_timestamp())
    )
    (
        df.write.format("delta")
        .mode("overwrite")
        .option("overwriteSchema", "true")
        .saveAsTable(f"fitco_bronze.{t}")
    )
    results.append((t, spark.table(f"fitco_bronze.{t}").count()))

display(spark.createDataFrame(results, ["bronze_table", "row_count"]))

# COMMAND ----------

# MAGIC %md
# MAGIC **Expected row counts** (seed 42): see `data/raw/_generation_manifest.json`. Small differences are
# MAGIC only possible if you regenerated the data with another seed.