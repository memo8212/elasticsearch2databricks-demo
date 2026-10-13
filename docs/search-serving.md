# Search Serving — Keeping Elasticsearch (fed from Databricks)

The lakehouse is the **system of record**. If you have search UIs (transaction
lookup, case management, compliance investigations), keep Elasticsearch — but make
it a **derived, rebuildable index** fed from Gold.

```
 dept files → Databricks (Bronze → Silver → Gold) ── system of record
                                   │  curated, PII-masked
                                   ▼
                          Elasticsearch / OpenSearch ── search UIs, APIs
                                   │
                                   └── rebuildable: delete + reindex from Gold
```

## When to keep ES
- Full-text / fuzzy search, relevance ranking, facets, autocomplete, highlighting.
- Sub-100ms point lookups for apps.
- Real-time-ish search consoles (fraud/AML investigations).

## When to drop ES
- It was only doing dashboards/aggregations → move those to Databricks SQL.
- No search UI depends on it → retire and save the RAM/licensing cost.

## Push Gold → Elasticsearch (ES-Hadoop connector)

```python
gold = spark.table("bank.gold.dept_summary")
(gold.write.format("org.elasticsearch.spark.sql")
     .option("es.nodes", "es-bank")
     .option("es.port", "9200")
     .option("es.resource", "bank-gold-dept-summary/_doc")
     .option("es.mapping.id", "dept")           # stable _id -> upserts
     .mode("overwrite")
     .save())
```

Cluster config: `spark.jars.packages = org.elasticsearch:elasticsearch-spark-30_2.12:<version>`.

## Search UI example

Index a searchable document per account/transaction (denormalized in Gold), then
the UI queries:

```json
GET bank-txn/_search
{
  "query": {
    "bool": {
      "must":     [ { "term": { "dept": "cards" } },
                    { "range": { "amount": { "gte": 1000 } } } ],
      "must_not": [ { "term": { "status": "reversal" } } ]
    }
  },
  "sort": [ { "txn_date": "desc" } ],
  "size": 20
}
```

The same predicate, expressed in Spark SQL on Databricks (used by the demo):

```sql
SELECT * FROM bank.silver.dept_txn
WHERE dept = 'cards' AND amount >= 1000 AND NOT (status = 'reversal');
```

## PII in the search index
- Index **masked/tokenized** values (see `resources/pii_masking.sql`), not raw PII.
- Restrict the index with ES field-level/document-level security if needed.
- Never make ES the source of truth for PII — derive it from masked Gold.

## Sync strategy
| Approach | When |
|---|---|
| Batch push (ES-Hadoop `overwrite`/`append`) | nightly/periodic index refresh |
| Streaming push (Structured Streaming foreachBatch → ES) | near-real-time |
| Kafka/CDC bridge | when ES already consumes from Kafka |
| Reindex from Gold | after schema change / index corruption |

## Why this is safe
- **Rebuildable:** Gold is authoritative; ES can be dropped and rebuilt anytime.
- **Governed:** lineage/PII/retention live in Unity Catalog, not the index.
- **Cost-contained:** index only what the UI needs, not the whole lake.
