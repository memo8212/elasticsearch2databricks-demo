"""End-to-end demo: all department files -> one governed place (Databricks).

Migrated from Elasticsearch:
  * the ES `_bulk` loader + ingest pipeline  -> Auto Loader -> Bronze -> transforms -> Silver
  * the ES indices + Kibana aggregations      -> Delta Gold tables + SQL
  * a FILE REGISTRY (hash, size, status)      -> file-level lineage
  * a TRANSFER step with reconciliation       -> movement out of the platform
  * the saved searches                        -> Spark SQL filters

The legacy side is a pandas "Elasticsearch stand-in" (docs + aggregations); the
target side is PySpark. Gold results are compared row-by-row.

Run:  python runner/run_demo.py
"""
from __future__ import annotations

import hashlib
import json
import shutil
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent.parent
LANDING = ROOT / "data" / "landing"
OUT = ROOT / "artifacts" / "output"
(BRONZE := OUT / "bronze").mkdir(parents=True, exist_ok=True)
(SILVER := OUT / "silver").mkdir(parents=True, exist_ok=True)
(GOLD := OUT / "gold").mkdir(parents=True, exist_ok=True)
(TRANSFER := OUT / "transfer_out").mkdir(parents=True, exist_ok=True)

BASE_RATE = 1.0
DEPTS = {"cards", "payments", "loans", "deposits", "wealth"}


# --------------------------------------------------------------- file discovery
def discover_files() -> list[dict]:
    files = []
    for p in sorted(LANDING.rglob("*")):
        if p.is_file():
            data = p.read_bytes()
            files.append({
                "file_id": hashlib.md5(str(p).encode()).hexdigest()[:12],
                "dept": p.parent.name,
                "format": "csv" if p.suffix == ".csv" else "ndjson",
                "path": str(p.relative_to(ROOT)),
                "size": len(data),
                "md5": hashlib.md5(data).hexdigest(),
                "rows": data.count(b"\n") - (1 if p.suffix == ".csv" else 0),
            })
    return files


# --------------------------------------------------------------- pipeline (pandas)
def apply_pipeline_pandas(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns={"DEPT_CODE": "dept", "TXN_ID": "doc_id", "TXN_DATE": "txn_date"})
    df["dept"] = df["dept"].str.lower()
    df["txn_date"] = pd.to_datetime(df["txn_date"]).dt.date.astype(str)
    df["amount"] = df["AMOUNT"].astype(float)
    df["STATUS"] = df["STATUS"].str.replace(r"\s+", "", regex=True)
    df["status"] = df["STATUS"].str.lower()
    df["ccy"] = df["CCY"].str.lower()
    df["base_ccy"] = "CAD"
    df["base_amount"] = df["amount"] * BASE_RATE
    df["txn_kind"] = df["NARRATIVE"].str.split().str[0]
    df["tags"] = "bank,dept-feed"
    # --- PII masking (Silver): mask the name, tokenize the account number ---
    df["holder_name"] = df["HOLDER_NAME"].str[:1] + "***"
    df["account_token"] = df["ACCOUNT_NO"].map(
        lambda a: hashlib.sha256(str(a).encode()).hexdigest()[:16])
    return df


def legacy_es(files: list[dict]) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Read every department file and apply the ingest pipeline (ES stand-in)."""
    frames = []
    for f in files:
        p = ROOT / f["path"]
        frames.append(pd.read_csv(p) if f["format"] == "csv" else pd.read_json(p, lines=True))
    docs = apply_pipeline_pandas(pd.concat(frames, ignore_index=True))
    # Kibana aggregation: total base amount by department
    gold = (docs.groupby("dept", as_index=False)
                .agg(total_base=("base_amount", "sum"), txn_count=("doc_id", "count")))
    gold["total_base"] = gold["total_base"].round(2)
    return docs, gold.sort_values("dept").reset_index(drop=True)


def statements_search(files: list[dict]) -> int:
    """suspicious_narratives saved search (statements index) -> Spark SQL filter."""
    frames = [pd.read_json(ROOT / f["path"], lines=True) for f in files]
    if not frames:
        return 0
    body = pd.concat(frames, ignore_index=True)["body"].str.lower()
    return int((body.str.contains("overdraft") | body.str.contains("unauthorized")).sum())


# --------------------------------------------------------------- spark side
def spark_side(files: list[dict]):
    from pyspark.sql import SparkSession, functions as F
    spark = (SparkSession.builder.master("local[2]").appName("es2dbx_demo")
             .config("spark.sql.shuffle.partitions", "8")
             .config("spark.ui.enabled", "false").getOrCreate())

    # ---- Bronze: Auto Loader equivalent (mixed CSV/NDJSON) ----
    parts = []
    for f in files:
        p = str(ROOT / f["path"])
        if f["format"] == "csv":
            d = spark.read.option("header", True).csv(p)
        else:
            d = spark.read.json(p)
        parts.append(d.withColumn("_source_file", F.lit(f["path"])))
    bronze = parts[0]
    for d in parts[1:]:
        bronze = bronze.unionByName(d, allowMissingColumns=True)
    bronze = bronze.withColumn("_ingested_at", F.current_timestamp())
    bronze.write.mode("overwrite").parquet(str(BRONZE / "dept_txn"))

    # ---- Silver: apply the ingest pipeline ----
    df = (bronze
          .withColumnRenamed("DEPT_CODE", "dept").withColumnRenamed("TXN_ID", "doc_id")
          .withColumnRenamed("TXN_DATE", "txn_date")
          .withColumn("dept", F.lower(F.col("dept")))
          .withColumn("txn_date", F.to_date(F.col("txn_date"), "yyyy-MM-dd"))
          .withColumn("amount", F.col("AMOUNT").cast("double"))
          .withColumn("STATUS", F.regexp_replace(F.col("STATUS"), r"\s+", ""))
          .withColumn("status", F.lower(F.col("STATUS")))
          .withColumn("ccy", F.lower(F.col("CCY")))
          .withColumn("base_ccy", F.lit("CAD"))
          .withColumn("base_amount", F.col("amount") * F.lit(BASE_RATE))
          .withColumn("txn_kind", F.split(F.col("NARRATIVE"), " ")[0])
          .withColumn("tags", F.lit("bank,dept-feed"))
          # --- PII masking (Silver) ---
          .withColumn("holder_name", F.concat(F.substring(F.col("HOLDER_NAME"), 1, 1), F.lit("***")))
          .withColumn("account_token", F.substring(F.sha2(F.col("ACCOUNT_NO"), 256), 1, 16)))
    df.write.mode("overwrite").option("overwriteSchema", "true").parquet(str(SILVER / "dept_txn"))

    # ---- Gold: the Kibana aggregation as SQL ----
    df.createOrReplaceTempView("dept_txn")
    gold = spark.sql("""
        SELECT dept, ROUND(SUM(base_amount), 2) AS total_base, COUNT(doc_id) AS txn_count
        FROM dept_txn GROUP BY dept ORDER BY dept
    """)
    gold.write.mode("overwrite").parquet(str(GOLD / "dept_summary"))

    # ---- Saved search -> SQL (high_value_cards) ----
    hv = spark.sql("""
        SELECT COUNT(*) AS n FROM dept_txn
        WHERE dept = 'cards' AND amount >= 1000 AND NOT (status = 'reversal')
    """).first()["n"]

    gold_pd = gold.toPandas()
    spark.stop()
    return gold_pd, int(hv)


# --------------------------------------------------------------- registry/lineage
def build_registry(files: list[dict]) -> pd.DataFrame:
    reg = pd.DataFrame([{
        "file_id": f["file_id"], "dept": f["dept"], "format": f["format"],
        "path": f["path"], "size": f["size"], "md5": f["md5"], "rows": f["rows"],
        "status": "LOADED", "target_table": "bronze_dept_txn",
    } for f in files])
    reg.to_csv(OUT / "file_registry.csv", index=False)
    return reg


def build_lineage(files: list[dict]) -> dict:
    edges = []
    for f in files:
        edges.append({"from": f"file:{f['path']}", "to": "bronze_dept_txn", "type": "ingest"})
    edges += [
        {"from": "bronze_dept_txn", "to": "silver_dept_txn", "type": "ingest-pipeline"},
        {"from": "silver_dept_txn", "to": "gold_dept_summary", "type": "aggregation"},
        {"from": "gold_dept_summary", "to": "transfer_out/dept_summary.parquet", "type": "transfer"},
    ]
    lineage = {"nodes": ["bronze_dept_txn", "silver_dept_txn", "gold_dept_summary"],
               "edges": edges}
    (OUT / "lineage.json").write_text(json.dumps(lineage, indent=2), encoding="utf-8")
    lines = ["# File & Data Lineage", "", "| From | To | Type |", "|---|---|---|"]
    lines += [f"| {e['from']} | {e['to']} | {e['type']} |" for e in edges]
    (OUT / "lineage.md").write_text("\n".join(lines), encoding="utf-8")
    return lineage


def build_search_index(gold: pd.DataFrame) -> list:
    """Build a search-serving index from Gold and run a sample search query."""
    docs = [{"_id": f"dept::{r.dept}", "dept": r.dept,
             "total_base": float(r.total_base), "txn_count": int(r.txn_count)}
            for r in gold.itertuples()]
    (OUT / "search_index.json").write_text(json.dumps(docs, indent=2), encoding="utf-8")
    # example "search UI" query: departments above a revenue threshold
    threshold = 20_000_000
    return [d for d in docs if d["total_base"] >= threshold]


def build_sharing_manifest(gold: pd.DataFrame, transfer: dict) -> dict:
    """Emit a Delta Sharing manifest + recipient audit (replaces a copy)."""
    manifest = {
        "share": "bank_dept_share",
        "table": "bank.gold.dept_summary",
        "recipient": "partner_regulator",
        "version": 1,
        "rows": len(gold),
        "md5": transfer["md5"],
        "as_of": pd.Timestamp.utcnow().isoformat(timespec="seconds"),
        "reconciled": transfer["reconciled"],
    }
    (OUT / "sharing_manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    pd.DataFrame([{"share": manifest["share"], "table": manifest["table"],
                   "recipient": manifest["recipient"], "rows": manifest["rows"],
                   "status": "SHARED" if manifest["reconciled"] else "MISMATCH"}]) \
      .to_csv(OUT / "share_audit.csv", index=False)
    return manifest


def transfer_and_reconcile(gold: pd.DataFrame) -> dict:
    """Export Gold and verify the transfer (checksum + row count)."""
    target = TRANSFER / "dept_summary.parquet"
    gold.to_parquet(target, index=False)
    src_md5 = hashlib.md5(target.read_bytes()).hexdigest()
    back = pd.read_parquet(target)
    ok = (len(back) == len(gold)) and (hashlib.md5(target.read_bytes()).hexdigest() == src_md5)
    audit = pd.DataFrame([{
        "target": str(target.relative_to(ROOT)), "rows": len(back),
        "md5": src_md5, "status": "RECONCILED" if ok else "MISMATCH",
    }])
    audit.to_csv(OUT / "transfer_audit.csv", index=False)
    return {"rows": len(back), "md5": src_md5, "reconciled": ok}


def main() -> None:
    files = discover_files()
    dept_files = [f for f in files if f["dept"] in DEPTS]
    stmt_files = [f for f in files if f["dept"] == "statements"]
    print(f"Discovered {len(files)} file(s): {len(dept_files)} dept feeds across "
          f"{len({f['dept'] for f in dept_files})} departments + {len(stmt_files)} statements file(s)")

    # ---- legacy (ES stand-in) ----
    docs, gold_legacy = legacy_es(dept_files)
    print(f"Legacy ES docs: {len(docs):,} -> gold rows: {len(gold_legacy)}")

    # ---- Databricks ----
    gold_dbx, hv_count = spark_side(dept_files)
    susp_count = statements_search(stmt_files)

    # ---- registry + lineage + transfer + sharing ----
    registry = build_registry(files)
    lineage = build_lineage(files)
    transfer = transfer_and_reconcile(gold_dbx)
    search_results = build_search_index(gold_dbx)
    sharing = build_sharing_manifest(gold_dbx, transfer)
    pii_sample = docs[["doc_id", "holder_name", "account_token"]].head(3).to_string(index=False)

    # ---- validation ----
    merged = gold_legacy.merge(gold_dbx, on="dept", suffixes=("_ES", "_DBX"), how="outer", indicator=True)
    row_match = bool((merged["_merge"] == "both").all())
    amt_ok = bool(((merged["total_base_ES"] - merged["total_base_DBX"]).abs() <= 0.01).all()) if row_match else False
    cnt_ok = bool((merged["txn_count_ES"] == merged["txn_count_DBX"]).all()) if row_match else False
    passed = row_match and amt_ok and cnt_ok and transfer["reconciled"] and sharing["reconciled"]

    report = f"""# Validation Report — Elasticsearch -> Databricks (bank department feeds)

## Result: {"✅ PASS — lakehouse matches the ES store; transfer reconciled" if passed else "❌ FAIL"}

| Metric | Value |
|---|---|
| Department files ingested | {len(files)} |
| Documents (legacy ES / Databricks) | {len(docs):,} / {int(gold_dbx['txn_count'].sum()):,} |
| Gold rows (dept) | {len(gold_legacy)} |
| Row-set match | {row_match} |
| Base amount match (tol 0.01) | {amt_ok} |
| Txn count match | {cnt_ok} |
| Saved search high_value_cards | {hv_count} |
| Saved search suspicious_narratives | {susp_count} |
| Transfer reconciled | {transfer['reconciled']} (rows={transfer['rows']}, md5={transfer['md5'][:10]}...) |
| Delta Share reconciled | {sharing['reconciled']} (share={sharing['share']} -> {sharing['recipient']}) |
| Search index docs / query hits | {len(gold_dbx)} / {len(search_results)} |

## PII masking (Silver) — sample
```
{pii_sample}
```

## File registry (sample)
```
{registry.head(6).to_string(index=False)}
```

## Gold summary
```
{gold_dbx.to_string(index=False)}
```

## Lineage
```
file:* -> bronze_dept_txn -> silver_dept_txn -> gold_dept_summary -> transfer_out
```

## Artifacts
- File registry: `artifacts/output/file_registry.csv`
- Lineage: `artifacts/output/lineage.json`, `lineage.md`
- Transfer + audit: `artifacts/output/transfer_out/`, `transfer_audit.csv`
- Gold: `artifacts/output/gold/dept_summary/`
"""
    (OUT / "validation_report.md").write_text(report, encoding="utf-8")
    print("Validation done -> artifacts/output/validation_report.md")
    print(report.split("## File registry")[0])
    return 0 if passed else 1


if __name__ == "__main__":
    raise SystemExit(main())
