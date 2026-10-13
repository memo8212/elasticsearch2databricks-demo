# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Push Gold to Elasticsearch (search-serving layer)
# MAGIC Databricks stays the **system of record**; Elasticsearch is a **derived,
# MAGIC rebuildable index** for search UIs (transaction/document lookup). We push a
# MAGIC curated Gold table and demonstrate a search query.

# COMMAND ----------

# MAGIC %md ## 1. Push Gold -> Elasticsearch (ES-Hadoop connector)
# MAGIC Requires the connector jar on the cluster (`spark.jars.packages =
# MAGIC org.elasticsearch:elasticsearch-spark-30_2.12:<version>`).

# COMMAND ----------

gold = spark.table("bank.gold.dept_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC ```python
# MAGIC (gold.write.format("org.elasticsearch.spark.sql")
# MAGIC      .option("es.nodes", "es-bank")
# MAGIC      .option("es.port", "9200")
# MAGIC      .option("es.resource", "bank-gold-dept-summary/_doc")
# MAGIC      .option("es.mapping.id", "dept")          # stable _id for upserts
# MAGIC      .mode("overwrite")
# MAGIC      .save())
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 2. Rebuildable index
# MAGIC Because Gold is the source of truth, you can delete and reindex ES at any
# MAGIC time — no data is lost:
# MAGIC ```bash
# MAGIC curl -X DELETE "http://es-bank:9200/bank-gold-dept-summary"
# MAGIC # then re-run the cell above
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 3. Example search UI query (against the index)
# MAGIC The search UI runs queries like this against Elasticsearch:
# MAGIC ```json
# MAGIC GET bank-gold-dept-summary/_search
# MAGIC {
# MAGIC   "query": { "range": { "total_base": { "gte": 20000000 } } },
# MAGIC   "sort": [ { "total_base": "desc" } ]
# MAGIC }
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## 4. Search over Databricks too (no ES needed for analytics)
# MAGIC For analytical retrieval, query Gold directly:
spark.sql("""
    SELECT dept, total_base, txn_count
    FROM   bank.gold.dept_summary
    WHERE  total_base >= 20000000
    ORDER  BY total_base DESC
""").show()
