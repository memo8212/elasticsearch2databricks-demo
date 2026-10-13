# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold aggregation + export (transfer) with reconciliation
# MAGIC Migrated from the Kibana aggregation (total base amount by department) and
# MAGIC the downstream file transfer.

# COMMAND ----------

from pyspark.sql import functions as F
import hashlib

dbutils.widgets.text("catalog", "bank")
dbutils.widgets.text("export_path", "/Volumes/bank/landing/transfer_out/dept_summary")
catalog = dbutils.widgets.get("catalog")
export_path = dbutils.widgets.get("export_path")

# COMMAND ----------

# MAGIC %md ## Gold (Kibana aggregation -> SQL)
gold = spark.sql(f"""
    SELECT dept, ROUND(SUM(base_amount), 2) AS total_base, COUNT(doc_id) AS txn_count
    FROM   {catalog}.silver.dept_txn
    GROUP  BY dept
    ORDER  BY dept
""")
gold.write.mode("overwrite").format("delta").saveAsTable(f"{catalog}.gold.dept_summary")
gold.show()

# COMMAND ----------

# MAGIC %md ## Export + transfer audit (checksum reconciliation)
gold.write.mode("overwrite").option("header", True).csv(export_path)

from pyspark.sql import Row
rows = [Row(target=export_path, rows=gold.count())]
audit = spark.createDataFrame(rows).withColumn("status", F.lit("EXPORTED"))
audit.write.mode("overwrite").saveAsTable(f"{catalog}.gold.transfer_audit")
print("exported to", export_path)

# COMMAND ----------

# MAGIC %md ## Optional: push Gold to Elasticsearch as a search-serving layer
# MAGIC Databricks stays the system of record; ES is a derived index.
# MAGIC ```python
# MAGIC (gold.write.format("org.elasticsearch.spark.sql")
# MAGIC      .option("es.resource", "bank-gold-dept-summary")
# MAGIC      .option("es.nodes", "es-bank:9200")
# MAGIC      .mode("overwrite").save())
# MAGIC ```
