# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Apply the ingest pipeline (Bronze -> Silver)
# MAGIC Migrated from the Elasticsearch `dept_ingest` pipeline processors.

# COMMAND ----------

from pyspark.sql import functions as F

dbutils.widgets.text("catalog", "bank")
catalog = dbutils.widgets.get("catalog")

bronze = spark.table(f"{catalog}.bronze.dept_txn")

# COMMAND ----------

# MAGIC %md ## Pipeline processors: rename / lowercase / date / convert / gsub / set / script / grok
silver = (bronze
    .withColumnRenamed("DEPT_CODE", "dept")
    .withColumnRenamed("TXN_ID", "doc_id")
    .withColumnRenamed("TXN_DATE", "txn_date")
    .withColumn("dept", F.lower(F.col("dept")))
    .withColumn("txn_date", F.to_date(F.col("txn_date"), "yyyy-MM-dd"))
    .withColumn("amount", F.col("AMOUNT").cast("double"))
    .withColumn("status", F.lower(F.regexp_replace(F.col("STATUS"), r"\s+", "")))
    .withColumn("ccy", F.lower(F.col("CCY")))
    .withColumn("base_ccy", F.lit("CAD"))
    .withColumn("base_amount", F.col("amount") * F.lit(1.0))
    .withColumn("txn_kind", F.split(F.col("NARRATIVE"), " ")[0])
    .withColumn("tags", F.lit("bank,dept-feed")))

silver.write.mode("overwrite").option("overwriteSchema", "true").saveAsTable(f"{catalog}.silver.dept_txn")
print("silver rows:", spark.table(f"{catalog}.silver.dept_txn").count())
