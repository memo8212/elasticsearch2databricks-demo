# Conversion Report — Elasticsearch -> Databricks

## Component mapping

| Elasticsearch | Databricks |
|---|---|
| Index | Delta table (Bronze/Silver) |
| Mapping (properties) | StructType schema |
| text field | StringType (Vector Search for semantic search) |
| keyword field | StringType (exact match / GROUP BY) |
| date field | TimestampType |
| object / nested | StructType / ArrayType |
| Ingest pipeline processors | PySpark transforms (rename/lowercase/date/convert/gsub/set/grok) |
| _bulk API / Logstash | Auto Loader / Lakeflow Connect |
| Saved search (query DSL) | Spark SQL WHERE |
| Aggregation (terms/histogram/metrics) | SQL GROUP BY |
| Kibana dashboard | Databricks SQL dashboard (Lakeview) |
| Index template / ILM | DDL + Delta retention (VACUUM, table properties) |
| Alias | View |
| Reindex | CTAS / Delta rewrite |
| _id | surrogate key |
| Near-real-time refresh | Auto Loader trigger / streaming |

## Index mappings -> schemas

| Field | ES type | Spark type |
|---|---|---|
| doc_id | keyword | StringType |
| dept | keyword | StringType |
| account_no | keyword | StringType |
| customer_id | keyword | StringType |
| product | keyword | StringType |
| amount | double | DoubleType |
| base_amount | double | DoubleType |
| ccy | keyword | StringType |
| txn_date | date | TimestampType |
| status | keyword | StringType |
| narrative | text | StringType |
| tags | keyword | StringType |
| statement_id | keyword | StringType |
| account_no | keyword | StringType |
| dept | keyword | StringType |
| period | keyword | StringType |
| body | text | StringType |
| attachment | object | StructType |
| generated_at | date | TimestampType |

## Ingest pipeline -> PySpark

| Processor | Field | Generated |
|---|---|---|
| rename | DEPT_CODE -> dept | withColumnRenamed |
| lowercase | dept | F.lower |
| rename | TXN_ID -> doc_id | withColumnRenamed |
| rename | TXN_DATE -> txn_date | withColumnRenamed |
| date | txn_date | F.to_date |
| convert | AMOUNT | cast(double) |
| gsub | STATUS | F.regexp_replace |
| lowercase | CCY | F.lower |
| set | base_ccy | F.lit |
| script | ctx.base_amount = ctx.amount * params.rate | withColumn |
| grok | NARRATIVE | F.regexp_extract |
| set | tags | F.lit |

## Saved searches -> SQL

| Search | Index | WHERE |
|---|---|---|
| high_value_cards | bank-dept-* | `((dept = "cards") AND (amount >= 1000)) AND NOT (status = "reversal")` |
| suspicious_narratives | bank-statements-* | `((lower(body) LIKE '%overdraft%') OR (lower(body) LIKE '%unauthorized%'))` |

## Dashboard aggregations -> SQL

| Panel | Agg | SQL |
|---|---|---|
| Total base amount by department | by_dept | `SELECT dept, SUM(base_amount) AS total_base, COUNT(doc_id) A` |
| Daily transaction count | by_day | `SELECT date_trunc('day', txn_date) AS day, AVG(amount) AS av` |

## Notes
- The lakehouse (Delta + Unity Catalog) becomes the system of record.
- Elasticsearch can be kept as a *search-serving* layer, fed from Gold.
- File-level lineage + a file registry cover the 'one place' requirement.
- ILM delete (2555d) -> Delta retention policy + VACUUM.