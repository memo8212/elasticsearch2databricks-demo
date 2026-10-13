# Elasticsearch → Databricks — Deep Dive

How to move an Elasticsearch-based bank file/document store to a Databricks
lakehouse, keeping ES only as an optional search-serving layer. Pairs with
`converter/es2dbx_converter.py` (parses ES artifacts) and `runner/run_demo.py`
(loads all department files into one governed place + lineage + transfer).

---

## 1. Why migrate (and when not to)

**Move to Databricks when ES is being used as a data store/analytics layer:**
- one governed copy of all department files (Delta + Unity Catalog),
- automatic column/table **lineage** + a **file registry** for file lineage,
- SQL + ML on the same data, cheaper long-term storage, 7-year retention,
- no mapping/shard/refresh tuning.

**Keep Elasticsearch when you need search:**
- full-text / fuzzy / relevance ranking, autocomplete, highlighting, faceting,
- sub-100ms point lookups for apps (case management, transaction search UIs).

The winning pattern: **Databricks = system of record + compute; ES = derived
search index**, fed from Gold.

```
 dept files ─▶ LANDING (UC Volume) ─▶ BRONZE ─▶ SILVER ─▶ GOLD ─▶ transfer / share
                                                             └──▶ (optional) Elasticsearch
```

---

## 2. Component mapping

| Elasticsearch | Databricks |
|---|---|
| Index | Delta table (Bronze/Silver) |
| Mapping `properties` | `StructType` schema |
| `text` field | `StringType` (use **Vector Search** for semantic) |
| `keyword` field | `StringType` (exact match / `GROUP BY`) |
| `date` field | `TimestampType` |
| `object` / `nested` | `StructType` / `ArrayType` |
| `_id` | surrogate key |
| `_source` | Bronze table (raw + audit) |
| `_bulk` API / Logstash | **Auto Loader** / Lakeflow Connect |
| Ingest pipeline processors | PySpark transforms |
| Saved search (query DSL) | Spark SQL `WHERE` |
| Aggregation (terms/date_histogram/metrics) | SQL `GROUP BY` |
| Kibana dashboard | Databricks SQL dashboard (Lakeview) |
| Index template / settings | DDL + Delta table properties |
| ILM policy (hot/warm/cold/delete) | Delta retention + `VACUUM` + `OPTIMIZE` |
| Alias | View |
| Reindex | `CTAS` / Delta rewrite |
| Refresh interval / near-real-time | Auto Loader trigger / streaming |
| Shards / replicas | `repartition` / Liquid Clustering |
| Scroll / deep pagination | `LIMIT`/keyset pagination on Delta |
| Relevance scoring | Vector Search (embeddings) — separate concern |

---

## 3. Field types

`converter/es2dbx_converter.py` maps ES types to Spark:

| ES type | Spark |
|---|---|
| `keyword`, `text`, `ip` | `StringType` |
| `long` | `LongType` |
| `integer`, `short`, `byte` | `IntegerType` |
| `double`, `float`, `scaled_float` | `DoubleType` |
| `date` | `TimestampType` |
| `boolean` | `BooleanType` |
| `object` | `StructType` |
| `nested` | `ArrayType(StructType(...))` |

Note: ES `text` is analyzed for search; in Databricks you keep the raw string and
do full-text with **Databricks Vector Search** (semantic) or push to ES.

---

## 4. Ingest pipeline processors → PySpark

| Processor | PySpark |
|---|---|
| `rename` | `withColumnRenamed` |
| `lowercase` / `uppercase` | `F.lower` / `F.upper` |
| `date` | `F.to_date(col, fmt)` |
| `convert` | `col.cast(type)` |
| `gsub` | `F.regexp_replace(col, pattern, repl)` |
| `set` | `F.lit(value)` |
| `grok` | `F.regexp_extract(col, regex, 1)` |
| `script` | `withColumn(target, <expr>)` |
| `remove` | `drop` |

---

## 5. Query DSL → SQL

| Query DSL | SQL |
|---|---|
| `term {f: v}` | `f = v` |
| `terms {f: [..]}` | `f IN (..)` |
| `range {f: {gte,lte}}` | `f >= .. AND f <= ..` |
| `match {f: text}` | `lower(f) LIKE '%text%'` |
| `exists {f}` | `f IS NOT NULL` |
| `bool must` | `AND` |
| `bool should` | `OR` |
| `bool must_not` | `NOT (...)` |

Aggregations → SQL:

| Aggregation | SQL |
|---|---|
| `terms` | `GROUP BY field` |
| `date_histogram` | `date_trunc('day', field) GROUP BY 1` |
| `sum/avg/min/max` | `SUM/AVG/MIN/MAX` |
| `value_count` | `COUNT` |
| `cardinality` | `COUNT(DISTINCT ...)` |

---

## 6. "All department files in one place"

```
 file-mover → bank.landing.dept/<dept>/          (raw bytes, immutable)
                    │  Auto Loader (mixed CSV/NDJSON)
                    ▼
              bank.bronze.dept_txn  (+ _source_dept, _ingested_at)
                    │  ingest pipeline (rename/…/grok)
                    ▼
              bank.silver.dept_txn
                    ▼
              bank.gold.dept_summary  ──▶ transfer_out / Delta Sharing
```

**File registry** (`bank.bronze.file_registry`) — one row per file:
`path, dept, size, md5, rows, status, target_table`. This is **file-level
lineage**: prove which file produced which rows.

**Table/column lineage** — Unity Catalog records it automatically from the
notebooks/job; combine with the registry for end-to-end `file → column` lineage.

**Transfer** — export Gold (CSV/Parquet) to a governed location, record a
`transfer_audit` row (rows + checksum), and reconcile by re-reading. Cross-org →
**Delta Sharing**; partner SFTP → a task with checksum verification.

---

## 7. Keeping Elasticsearch (the search-serving layer)

If you have a search UI, don't delete ES — feed it from Gold:

```python
(gold.write.format("org.elasticsearch.spark.sql")
     .option("es.resource", "bank-gold-dept-summary")
     .option("es.nodes", "es-bank:9200")
     .mode("overwrite").save())
```

Databricks owns the data; ES is a rebuildable index (you can reindex from Gold).

---

## 8. Finance guardrails

- **Immutability/retention:** landing is write-once; ILM delete (2555d) → Delta
  retention (`deletedFileRetentionDuration`) + `VACUUM`.
- **PII:** mask/tokenize in Silver; UC column masks + row filters.
- **Lineage/audit:** registry + UC lineage + Delta `DESCRIBE HISTORY` (BCBS 239).
- **Reconciliation:** file count/size/hash per batch; transfer checksums.
- **Access:** Gold-only grants to analysts; landing restricted to data ops.

---

## 9. Gotchas

1. **Mapping explosion** → use schema evolution + a schema registry, not
   thousands of ad-hoc fields.
2. **Nested cost** → `nested` becomes arrays of structs; join/explode carefully.
3. **Refresh vs streaming** → ES near-real-time ≈ Auto Loader `trigger`.
4. **Scroll/deep pagination** → replace with keyset pagination or Delta reads.
5. **Relevance** → SQL has no BM25; use Vector Search or keep ES for ranking.
6. **Analyzers** → tokenization/analysis isn't a Delta feature; store raw text.
7. **`_id` collisions** → pick a real business key + `MERGE`.
8. **Shard sizing** → `repartition` / Liquid Clustering instead of shard counts.
9. **ILM phases** → Delta retention + `OPTIMIZE` (hot) / cheaper storage (cold).
10. **Aggregation precision** → ES `sum` on doubles ≈ SQL `SUM`; reconcile with a
    tolerance and integer cents for money.

---

## 10. What the converter automates

`es2dbx_converter.py` reads the ES artifacts and emits:

- `schema.py` — index mappings → `StructType`,
- `notebook_dept_txn.py` — ingest pipeline → PySpark,
- `sql_saved_searches.sql` — query DSL → SQL,
- `sql_aggregations.sql` — Kibana aggregations → SQL,
- `dashboard.json` — dashboard definition,
- `conversion_report.md` — the component mapping.

As with the other converters in this workspace, anything it can't translate is
flagged rather than silently dropped — review the report before production use.
