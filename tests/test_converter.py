"""Unit tests for the Elasticsearch -> Databricks converter."""
import json
from pathlib import Path

from converter.es2dbx_converter import (
    build_aggregations, build_saved_searches, build_schema, pipeline_code,
    query_to_sql, agg_to_sql,
)

ROOT = Path(__file__).resolve().parents[1]
ES = ROOT / "legacy" / "es"


def test_mapping_to_schema():
    src, rows = build_schema()
    assert "DEPT_TXN_SCHEMA" in src and "STATEMENTS_SCHEMA" in src
    assert 'StructField("amount", DoubleType(), True)' in src
    assert 'StructField("txn_date", TimestampType(), True)' in src
    assert 'StructField("dept", StringType(), True)' in src
    assert "attachment" in src  # nested object
    assert len(rows) >= 18


def test_pipeline_processors_to_pyspark():
    pipe = json.loads((ES / "pipelines" / "dept_ingest.json").read_text())
    code, rows = pipeline_code(pipe["processors"])
    joined = "\n".join(code)
    assert 'withColumnRenamed("DEPT_CODE", "dept")' in joined
    assert 'F.lower(F.col("dept"))' in joined
    assert "F.to_date(" in joined
    assert 'F.col("AMOUNT").cast("double")' in joined
    assert "F.regexp_replace(" in joined
    assert "F.regexp_extract(" in joined          # grok
    assert 'F.col("amount") * 1.0' in joined      # script
    ops = {r[0] for r in rows}
    assert {"rename", "lowercase", "date", "convert", "gsub", "set", "grok", "script"} <= ops


def test_query_dsl_to_sql():
    q = {"bool": {"must": [{"term": {"dept": "cards"}},
                           {"range": {"amount": {"gte": 1000}}}],
                  "must_not": [{"term": {"status": "reversal"}}]}}
    sql = query_to_sql(q)
    assert 'dept = "cards"' in sql
    assert "amount >= 1000" in sql
    assert "NOT (status = \"reversal\")" in sql
    assert query_to_sql({"match": {"body": "Overdraft"}}) == "lower(body) LIKE '%overdraft%'"
    assert query_to_sql({"terms": {"dept": ["a", "b"]}}) == 'dept IN ("a", "b")'


def test_aggregations_to_sql():
    terms = {"by_dept": {"terms": {"field": "dept"},
                         "aggs": {"total_base": {"sum": {"field": "base_amount"}},
                                  "txn_count": {"value_count": {"field": "doc_id"}}}}}
    sql = agg_to_sql(terms)
    assert "GROUP BY dept" in sql
    assert "SUM(base_amount) AS total_base" in sql
    assert "COUNT(doc_id) AS txn_count" in sql

    hist = {"by_day": {"date_histogram": {"field": "txn_date", "calendar_interval": "day"},
                       "aggs": {"avg_amount": {"avg": {"field": "amount"}}}}}
    assert "date_trunc('day', txn_date)" in agg_to_sql(hist)


def test_saved_searches_and_dashboard():
    sql, rows = build_saved_searches()
    assert len(rows) == 2
    assert "high_value_cards" in sql and "suspicious_narratives" in sql

    agg_sql, agg_rows = build_aggregations()
    assert len(agg_rows) == 2
    assert "SUM(base_amount)" in agg_sql
