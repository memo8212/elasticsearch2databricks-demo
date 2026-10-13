# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Land all department files (Bronze) + file registry
# MAGIC Migrated from the ES `_bulk` loader + ingest-pipeline trigger. Auto Loader
# MAGIC ingests mixed formats into Bronze; a file registry records every file
# MAGIC (hash, size, status) for file-level lineage.

# COMMAND ----------

from pyspark.sql import functions as F
import hashlib

dbutils.widgets.text("catalog", "bank")
dbutils.widgets.text("schema", "bronze")
catalog = dbutils.widgets.get("catalog")
schema = dbutils.widgets.get("schema")
landing = f"/Volumes/{catalog}/landing/dept"

# COMMAND ----------

# MAGIC %md ## Auto Loader per format (NDJSON departments)
depts = ["cards", "loans", "wealth"]
streams = []
for d in depts:
    streams.append(
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.schemaLocation", f"{landing}/_schema/{d}")
        .load(f"{landing}/{d}")
        .withColumn("_source_dept", F.lit(d)))

# COMMAND ----------

# MAGIC %md ## CSV departments (batch read)
csv_depts = ["payments", "deposits"]
csv_frames = [spark.read.option("header", True).csv(f"{landing}/{d}").withColumn("_source_dept", F.lit(d))
              for d in csv_depts]

# COMMAND ----------

# MAGIC %md ## Union + write Bronze (with audit)
from functools import reduce
frames = streams + csv_frames
bronze = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), frames)
bronze = bronze.withColumn("_ingested_at", F.current_timestamp())
bronze.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.{schema}.dept_txn")
print("bronze rows:", spark.table(f"{catalog}.{schema}.dept_txn").count())

# COMMAND ----------

# MAGIC %md ## File registry (file-level lineage)
rows = []
for f in dbutils.fs.ls(landing):
    if f.isDir():
        for sub in dbutils.fs.ls(f.path):
            rows.append((sub.path, f.name, sub.size))
reg = spark.createDataFrame(rows, ["path", "dept", "size"]).withColumn("status", F.lit("LOADED"))
reg.write.mode("overwrite").saveAsTable(f"{catalog}.{schema}.file_registry")
reg.show(truncate=False)
