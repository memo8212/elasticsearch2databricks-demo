# Elasticsearch → Databricks Migration POC (bank department files)

End-to-end demo: **all department files from a bank land in one governed place**
(a Databricks lakehouse) — with a **file registry, file/table lineage, transfer
with reconciliation, and analytics** — migrating the Elasticsearch store to
Databricks and keeping ES only as an optional **search-serving layer**.

Same shape as the `sas2databricks-demo`, `mainframe2databricks-demo` and
`informatica2databricks-demo` repos.

## The legacy application (Elasticsearch)

Each bank department (CARDS, PAYMENTS, LOANS, DEPOSITS, WEALTH) drops files
(mixed **NDJSON / CSV**) that a `_bulk` loader indexes into `bank-dept-*` indices
through the `dept_ingest` pipeline; Kibana dashboards and saved searches run on
top. Documents are also stored in `bank-statements-*` for narrative search.

```
file-mover → /landing/<dept>/*.ndjson|csv
   → ES _bulk + dept_ingest pipeline → bank-dept-<dept>   (system of record today)
   → Kibana aggregations + saved searches
```

## The migrated target (Databricks)

```
file-mover → bank.landing.dept/<dept>/   (UC Volume, raw + immutable)
   → Auto Loader (mixed CSV/NDJSON) → bank.bronze.dept_txn (+ file_registry)
   → ingest pipeline (rename/…/grok) → bank.silver.dept_txn
   → aggregation (Kibana terms) → bank.gold.dept_summary
   → transfer_out + transfer_audit (checksum reconciliation)
   → (optional) push Gold → Elasticsearch for search UIs
```

## Component mapping (Elasticsearch → Databricks)

| Elasticsearch | Databricks |
|---|---|
| Index | Delta table (Bronze/Silver) |
| Mapping `properties` | `StructType` schema |
| `text` / `keyword` | `StringType` (Vector Search for semantic) |
| `date` | `TimestampType` |
| `object` / `nested` | `StructType` / `ArrayType` |
| Ingest pipeline processors | PySpark transforms |
| `_bulk` / Logstash | Auto Loader / Lakeflow Connect |
| Saved search (query DSL) | Spark SQL `WHERE` |
| Aggregation (terms/histogram/metrics) | SQL `GROUP BY` |
| Kibana dashboard | Databricks SQL dashboard (Lakeview) |
| ILM policy | Delta retention + `VACUUM` |
| Alias | View |

Full reference + gotchas: **`docs/es2dbx-components.md`**.

## Structure

```
legacy/es/                 Elasticsearch artifacts
  mappings/dept_txn.json, statements.json      (index templates)
  pipelines/dept_ingest.json                   (ingest pipeline)
  queries/saved_searches.json                  (query DSL)
  dashboards/kibana_dashboard.json             (aggregations)
  settings/ilm_policy.json                     (retention)
  loader/bulk_load.sh                          (_bulk loader)
converter/es2dbx_converter.py                  ES artifacts -> Databricks artifacts
runner/run_demo.py                             all files -> one place + registry + lineage + transfer + sharing + PII + search index
data/generate_department_files.py              synthetic dept files (NDJSON + CSV, with PII)
notebooks/                                     01 ingest, 02 silver, 03 gold+export, 04 PII masking, 05 push to ES
resources/                                     bundle job, uc_setup.sql, pii_masking.sql, sharing.sql
databricks.yml                                 Databricks Asset Bundle root
docs/es2dbx-components.md                      component deep dive
docs/search-serving.md                         keeping ES as a derived search index
tests/                                         pytest (converter + runner)
artifacts/                                     generated outputs
```

## Run

```bash
pip install pyspark pyarrow pandas pytest

python data/generate_department_files.py     # synthetic department files (mixed formats)
python converter/es2dbx_converter.py         # ES artifacts -> schema/notebook/SQL/report
python runner/run_demo.py                    # one-place ingest + registry + lineage + transfer + validation
pytest -q tests/                             # converter tests
```

## Outputs (in `artifacts/`)

- `generated/schema.py` — index mappings → StructTypes
- `generated/notebook_dept_txn.py` — ingest pipeline → PySpark
- `generated/sql_saved_searches.sql` — query DSL → SQL
- `generated/sql_aggregations.sql` — Kibana aggregations → SQL
- `generated/dashboard.json` — dashboard definition
- `output/file_registry.csv` — file-level lineage (path, size, md5, status)
- `output/lineage.json` / `lineage.md` — file → bronze → silver → gold → transfer
- `output/transfer_out/` + `transfer_audit.csv` — export + checksum reconciliation
- `output/search_index.json` — search-serving index built from Gold
- `output/sharing_manifest.json` + `share_audit.csv` — Delta Sharing record
- `output/validation_report.md` — ES store vs lakehouse parity

## Governance & serving

**PII masking (Silver).** The holder name is masked and the account number is
tokenized; Unity Catalog column masks enforce it for everyone without the
`pii_readers` group. See `resources/pii_masking.sql` and `notebooks/04_pii_masking.py`.

```sql
CREATE FUNCTION bank.silver.mask_name(name STRING) RETURNS STRING
RETURN CASE WHEN is_account_group_member('pii_readers') THEN name
            ELSE concat(substring(name,1,1),'***') END;
ALTER TABLE bank.silver.dept_txn ALTER COLUMN holder_name SET MASK bank.silver.mask_name;
```

**Delta Sharing.** Expose Gold to a partner/regulator **without copying** data —
replaces "export a file and email it" for governed sharing (`resources/sharing.sql`).

```sql
CREATE SHARE bank_dept_share;
ALTER SHARE bank_dept_share ADD TABLE bank.gold.dept_summary;
GRANT SELECT ON SHARE bank_dept_share TO RECIPIENT partner_regulator;
```

**Keep Elasticsearch (search-serving).** Databricks is the system of record; ES is
a derived, rebuildable index fed from Gold. See `docs/search-serving.md` and
`notebooks/05_push_to_elasticsearch.py`.

```python
(gold.write.format("org.elasticsearch.spark.sql")
     .option("es.nodes", "es-bank").option("es.resource", "bank-gold-dept-summary/_doc")
     .option("es.mapping.id", "dept").mode("overwrite").save())
```

## Deploy to Databricks (Asset Bundle)

```bash
databricks bundle validate -t dev
databricks bundle deploy   -t dev
databricks bundle run bank_dept_feeds -t dev
```

`resources/bank_es_job.yml` runs `ingest → silver → gold_export`;
`resources/uc_setup.sql` creates the catalog/schemas/volume/tables.

## What the demo proves

1. **All files land in one place** — mixed CSV/NDJSON → Bronze (Auto Loader).
2. **The ingest pipeline migrates** — ES processors → PySpark transforms.
3. **File-level lineage** — a registry with hashes + `file → table` edges.
4. **Transfer is reconciled** — export with checksum + row-count verification.
5. **Search migrates too** — query DSL → SQL, Kibana aggregations → SQL.
6. **Parity is provable** — legacy ES store vs Databricks gold, row-by-row.

## Notes / scope

- The converter is a **POC**: it parses the common ES artifacts (index templates,
  ingest pipeline, saved searches, dashboard aggregations) to demonstrate mapping,
  not a full ES reindex engine. Review the report before production use.
- The runner re-implements the loader/pipeline/aggregation in **pandas** (legacy
  ES stand-in) and **PySpark** (target) so both sides run locally.
- Elasticsearch is **not deleted** where a search UI exists — it becomes a
  derived index fed from Gold.

## License

MIT — see `LICENSE`.
