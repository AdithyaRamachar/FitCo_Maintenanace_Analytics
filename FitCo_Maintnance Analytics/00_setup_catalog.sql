-- Databricks notebook source
-- MAGIC %md
-- MAGIC # 00 · Setup — schemas and volumes
-- MAGIC Run once. Assumes the default Free Edition catalog is `workspace`. If yours differs
-- MAGIC (check **Catalog** in the left sidebar), replace `workspace` in every notebook.

-- COMMAND ----------

USE CATALOG workspace;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS fitco_landing;

-- COMMAND ----------

-- SPARK_ONLY
CREATE VOLUME IF NOT EXISTS fitco_landing.raw_csv;

-- COMMAND ----------

-- SPARK_ONLY
CREATE VOLUME IF NOT EXISTS fitco_landing.gold_exports;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS fitco_bronze;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS fitco_silver;

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS fitco_gold;