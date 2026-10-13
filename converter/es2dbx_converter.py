"""Elasticsearch -> Databricks converter (POC).

Reads the ES artifacts under legacy/es/ and emits Databricks-ready outputs:

  artifacts/generated/schema.py                 index mappings -> StructTypes
  artifacts/generated/notebook_dept_txn.py      ingest pipeline -> PySpark
  artifacts/generated/sql_saved_searches.sql    query DSL -> Spark SQL
  artifacts/generated/sql_aggregations.sql      ES aggregations -> SQL
  artifacts/generated/dashboard.json            Kibana dashboard -> Databricks dashboard
  artifacts/generated/conversion_report.md      component mapping

Mapping rules:
  index + mapping            -> Delta table + StructType (Bronze/Silver)
  keyword/text               -> StringType        (text -> Vector Search for semantics)
  date                       -> TimestampType
  object/nested              -> StructType/ArrayType
  ingest pipeline processors -> PySpark transforms (rename/lowercase/date/convert/gsub/set/grok)
  _bulk / Logstash           -> Auto Loader / Lakeflow Connect
  saved search (query DSL)   -> Spark SQL WHERE
  aggregation (terms/histogram/metrics) -> SQL GROUP BY
  Kibana dashboard           -> Databricks SQL dashboard (Lakeview)
  index template / ILM       -> DDL + Delta retention (VACUUM / table properties)
  alias                      -> view
  reindex                    -> CTAS / Delta rewrite

Run:  python converter/es2dbx_converter.py
"""
from __future__ import annotations

import json
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
ES = ROOT / "legacy" / "es"
GEN = ROOT / "artifacts" / "generated"
GEN.mkdir(parents=True, exist_ok=True)


# --------------------------------------------------------------- mapping
ES_TO_SPARK = {
    "keyword": "StringType()", "text": "StringType()", "constant_keyword": "StringType()",
    "long": "LongType()", "integer": "IntegerType()", "short": "IntegerType()",
    "byte": "IntegerType()", "double": "DoubleType()", "float": "DoubleType()",
    "half_float": "DoubleType()", "scaled_float": "DoubleType()",
    "date": "TimestampType()", "boolean": "BooleanType()", "ip": "StringType()",
}


def _spark_type(prop: dict) -> str:
    t = prop.get("type", "object")
    if t == "object" or "properties" in prop:
        inner = ",\n".join(f'        StructField("{n}", {_spark_type(p)}, True)'
                           for n, p in prop.get("properties", {}).items())
        return f"StructType([\n{inner}\n    ])"
    if t == "nested":
        inner = ",\n".join(f'        StructField("{n}", {_spark_type(p)}, True)'
                           for n, p in prop.get("properties", {}).items())
        return f"ArrayType(StructType([\n{inner}\n    ]))"
    return ES_TO_SPARK.get(t, "StringType()")


def build_schema() -> tuple[str, list]:
    lines = ['"""StructTypes derived from Elasticsearch index mappings."""',
             "from pyspark.sql.types import (StructType, StructField, StringType, LongType,",
             "                               IntegerType, DoubleType, BooleanType, TimestampType,",
             "                               ArrayType)", ""]
    rows = []
    for p in sorted((ES / "mappings").glob("*.json")):
        doc = json.loads(p.read_text())
        props = doc["template"]["mappings"]["properties"]
        name = p.stem.upper()
        lines.append(f"{name}_SCHEMA = StructType([")
        for fname, prop in props.items():
            lines.append(f'    StructField("{fname}", {_spark_type(prop)}, True),')
            rows.append((fname, prop.get("type", "object"), _spark_type(prop).split("(")[0]))
        lines.append("])")
        lines.append("")
    return "\n".join(lines), rows


# --------------------------------------------------------------- ingest pipeline
def _grok_regex(pattern: str) -> str:
    # %{WORD:name} -> (?P<name>\w+); %{NUMBER:name} -> (?P<name>[0-9.]+)
    import re
    m = re.match(r"%\{(\w+):(\w+)\}", pattern)
    if not m:
        return pattern
    kind, name = m.group(1), m.group(2)
    cls = {"WORD": r"\w+", "NUMBER": r"[0-9.]+", "DATA": r".+", "GREEDYDATA": r".+"}.get(kind, r"\w+")
    return f"(?P<{name}>{cls})"


def pipeline_code(processors: list) -> tuple[list, list]:
    lines, rows = [], []
    for pr in processors:
        (op, body), = pr.items()
        if op == "rename":
            lines.append(f'df = df.withColumnRenamed("{body["field"]}", "{body["target_field"]}")')
            rows.append((op, f'{body["field"]} -> {body["target_field"]}', "withColumnRenamed"))
        elif op == "lowercase":
            tgt = body.get("target_field", body["field"])
            lines.append(f'df = df.withColumn("{tgt}", F.lower(F.col("{body["field"]}")))')
            rows.append((op, body["field"], "F.lower"))
        elif op == "uppercase":
            tgt = body.get("target_field", body["field"])
            lines.append(f'df = df.withColumn("{tgt}", F.upper(F.col("{body["field"]}")))')
            rows.append((op, body["field"], "F.upper"))
        elif op == "date":
            fmt = (body.get("formats") or ["yyyy-MM-dd"])[0]
            tgt = body.get("target_field", body["field"])
            spark_fmt = fmt.replace("yyyy", "yyyy").replace("MM", "MM").replace("dd", "dd")
            lines.append(f'df = df.withColumn("{tgt}", F.to_date(F.col("{body["field"]}"), "{spark_fmt}"))')
            rows.append((op, body["field"], "F.to_date"))
        elif op == "convert":
            tgt = body.get("target_field", body["field"])
            cast = {"double": "double", "long": "long", "integer": "int", "boolean": "boolean"}.get(body["type"], "string")
            lines.append(f'df = df.withColumn("{tgt}", F.col("{body["field"]}").cast("{cast}"))')
            rows.append((op, body["field"], f'cast({cast})'))
        elif op == "gsub":
            lines.append(f'df = df.withColumn("{body["field"]}", F.regexp_replace(F.col("{body["field"]}"), r"{body["pattern"]}", "{body["replacement"]}"))')
            rows.append((op, body["field"], "F.regexp_replace"))
        elif op == "set":
            val = body["value"]
            lit = json.dumps(val) if not isinstance(val, list) else f"F.array({', '.join(json.dumps(v) for v in val)})"
            lines.append(f'df = df.withColumn("{body["field"]}", F.lit({lit}))')
            rows.append((op, body["field"], "F.lit"))
        elif op == "grok":
            pat = _grok_regex((body.get("patterns") or ["%{WORD:val}"])[0])
            name = pat.split("?P<")[1].split(">")[0] if "?P<" in pat else "grok_field"
            lines.append(f'df = df.withColumn("{name}", F.regexp_extract(F.col("{body["field"]}"), r"{pat}", 1))')
            rows.append((op, body["field"], "F.regexp_extract"))
        elif op == "script":
            import re
            m = re.match(r"ctx\.(\w+)\s*=\s*(.+)", body["source"])
            if m:
                target, expr = m.group(1), m.group(2)
                for k, v in body.get("params", {}).items():
                    expr = expr.replace(f"params.{k}", str(v))
                expr = re.sub(r"ctx\.(\w+)", r'F.col("\1")', expr)
                lines.append(f'df = df.withColumn("{target}", {expr})')
                rows.append((op, body["source"], "withColumn"))
        elif op == "remove":
            lines.append(f'df = df.drop("{body["field"]}")')
            rows.append((op, body["field"], "drop"))
    return lines, rows


def build_notebook(pipe: dict) -> tuple[str, list]:
    code, rows = pipeline_code(pipe["processors"])
    body = "\n".join(code)
    nb = f'''# Databricks notebook source
# MAGIC %md
# MAGIC # dept_ingest (migrated from the Elasticsearch ingest pipeline)
# MAGIC The `_bulk` + ingest pipeline becomes Auto Loader -> Bronze -> these
# MAGIC transforms -> Silver. Generated by es2dbx.

# COMMAND ----------
from pyspark.sql import functions as F

raw = (spark.readStream.format("cloudFiles")
       .option("cloudFiles.format", "json")
       .option("cloudFiles.schemaLocation", "/Volumes/bank/landing/_schema/dept")
       .load("/Volumes/bank/landing/dept")
       .withColumn("_ingested_at", F.current_timestamp()))

# COMMAND ----------
# MAGIC %md ## Ingest pipeline processors
def apply_pipeline(df):
{chr(10).join("    " + c for c in code)}
    return df

# COMMAND ----------
silver = apply_pipeline(raw)
silver.writeStream.format("delta").outputMode("append") \\
      .option("checkpointLocation", "/Volumes/bank/landing/_ckpt/dept") \\
      .trigger(availableNow=True).toTable("bank.silver.dept_txn")
print("ingested")
'''
    return nb, rows


# --------------------------------------------------------------- query DSL
def _term(field, value):
    return f"{field} = {json.dumps(value)}" if not isinstance(value, (int, float)) else f"{field} = {value}"


def query_to_sql(q: dict) -> str:
    if "bool" in q:
        b = q["bool"]
        parts = []
        for m in b.get("must", []):
            parts.append(f"({query_to_sql(m)})")
        must = " AND ".join(parts)
        should = b.get("should", [])
        if should:
            ors = " OR ".join(f"({query_to_sql(m)})" for m in should)
            must = f"({must}) AND ({ors})" if must else f"({ors})"
        for m in b.get("must_not", []):
            must = f"({must}) AND NOT ({query_to_sql(m)})" if must else f"NOT ({query_to_sql(m)})"
        return must or "1=1"
    if "term" in q:
        (field, value), = q["term"].items()
        return _term(field, value)
    if "terms" in q:
        (field, values), = q["terms"].items()
        return f"{field} IN ({', '.join(json.dumps(v) if isinstance(v, str) else str(v) for v in values)})"
    if "range" in q:
        (field, r), = q["range"].items()
        ops = {"gte": ">=", "lte": "<=", "gt": ">", "lt": "<"}
        return " AND ".join(f"{field} {ops[k]} {v}" for k, v in r.items() if k in ops)
    if "match" in q:
        (field, text), = q["match"].items()
        return f"lower({field}) LIKE '%{text.lower()}%'"
    if "exists" in q:
        return f"{q['exists']['field']} IS NOT NULL"
    return "1=1"


def build_saved_searches() -> tuple[str, list]:
    doc = json.loads((ES / "queries" / "saved_searches.json").read_text())
    lines = ["-- Saved searches (Elasticsearch query DSL) -> Spark SQL",
             "-- Generated by es2dbx", ""]
    rows = []
    for s in doc["searches"]:
        where = query_to_sql(s["query"])
        lines += [f"-- {s['name']} (index {s['index']})",
                  f"SELECT * FROM {s['index'].replace('-', '_').replace('*', 'all')}",
                  f"WHERE {where};", ""]
        rows.append((s["name"], s["index"], where))
    return "\n".join(lines), rows


# --------------------------------------------------------------- aggregations
def _metric_expr(sub_body: dict) -> str:
    (mtype, mbody), = sub_body.items()
    field = mbody["field"]
    fn = {"sum": "SUM", "avg": "AVG", "min": "MIN", "max": "MAX",
          "value_count": "COUNT", "cardinality": "COUNT"}.get(mtype, "SUM")
    return f"COUNT(DISTINCT {field})" if mtype == "cardinality" else f"{fn}({field})"


def agg_to_sql(agg: dict) -> str:
    (name, body), = agg.items()
    agg_type = next((k for k in body if k in ("terms", "date_histogram", "histogram", "range")), None)
    if agg_type is None:
        return f"-- unsupported aggregation: {name}"
    inner = body[agg_type]
    subaggs = body.get("aggs", {})
    metrics = [f"{_metric_expr(b)} AS {sub_name}" for sub_name, b in subaggs.items()]
    if agg_type == "terms":
        field = inner["field"]
        cols = field + (", " + ", ".join(metrics) if metrics else "")
        return f"SELECT {cols} FROM dept_txn GROUP BY {field} ORDER BY 2 DESC"
    if agg_type in ("date_histogram", "histogram"):
        field = inner["field"]
        return (f"SELECT date_trunc('day', {field}) AS day, {', '.join(metrics) or '*'} "
                f"FROM dept_txn GROUP BY 1 ORDER BY 1")
    return f"-- unsupported aggregation: {name}"


def build_aggregations() -> tuple[str, list]:
    doc = json.loads((ES / "dashboards" / "kibana_dashboard.json").read_text())
    lines = ["-- Kibana dashboard aggregations -> Spark SQL", ""]
    rows = []
    for panel in doc["panels"]:
        sql = agg_to_sql(panel["aggs"])
        lines += [f"-- {panel['title']}", sql + ";", ""]
        rows.append((panel["title"], list(panel["aggs"])[0], sql[:60]))
    return "\n".join(lines), rows


def build_dashboard() -> dict:
    doc = json.loads((ES / "dashboards" / "kibana_dashboard.json").read_text())
    return {"name": doc["title"] + " (Databricks)",
            "datasets": [{"name": "dept_txn", "table": "bank.gold.dept_txn"}],
            "pages": [{"name": "Overview",
                       "widgets": [{"title": p["title"], "type": "chart"} for p in doc["panels"]]}]}


# --------------------------------------------------------------- report
COMPONENT_MAP = [
    ("Index", "Delta table (Bronze/Silver)"),
    ("Mapping (properties)", "StructType schema"),
    ("text field", "StringType (Vector Search for semantic search)"),
    ("keyword field", "StringType (exact match / GROUP BY)"),
    ("date field", "TimestampType"),
    ("object / nested", "StructType / ArrayType"),
    ("Ingest pipeline processors", "PySpark transforms (rename/lowercase/date/convert/gsub/set/grok)"),
    ("_bulk API / Logstash", "Auto Loader / Lakeflow Connect"),
    ("Saved search (query DSL)", "Spark SQL WHERE"),
    ("Aggregation (terms/histogram/metrics)", "SQL GROUP BY"),
    ("Kibana dashboard", "Databricks SQL dashboard (Lakeview)"),
    ("Index template / ILM", "DDL + Delta retention (VACUUM, table properties)"),
    ("Alias", "View"),
    ("Reindex", "CTAS / Delta rewrite"),
    ("_id", "surrogate key"),
    ("Near-real-time refresh", "Auto Loader trigger / streaming"),
]


def build_report(schema_rows, pipeline_rows, search_rows, agg_rows) -> str:
    lines = ["# Conversion Report — Elasticsearch -> Databricks", "",
             "## Component mapping", "", "| Elasticsearch | Databricks |", "|---|---|"]
    lines += [f"| {a} | {b} |" for a, b in COMPONENT_MAP]

    lines += ["", "## Index mappings -> schemas", "",
              "| Field | ES type | Spark type |", "|---|---|---|"]
    lines += [f"| {a} | {b} | {c} |" for a, b, c in schema_rows]

    lines += ["", "## Ingest pipeline -> PySpark", "",
              "| Processor | Field | Generated |", "|---|---|---|"]
    lines += [f"| {a} | {b} | {c} |" for a, b, c in pipeline_rows]

    lines += ["", "## Saved searches -> SQL", "",
              "| Search | Index | WHERE |", "|---|---|---|"]
    lines += [f"| {a} | {b} | `{c}` |" for a, b, c in search_rows]

    lines += ["", "## Dashboard aggregations -> SQL", "",
              "| Panel | Agg | SQL |", "|---|---|---|"]
    lines += [f"| {a} | {b} | `{c}` |" for a, b, c in agg_rows]

    lines += ["", "## Notes",
              "- The lakehouse (Delta + Unity Catalog) becomes the system of record.",
              "- Elasticsearch can be kept as a *search-serving* layer, fed from Gold.",
              "- File-level lineage + a file registry cover the 'one place' requirement.",
              "- ILM delete (2555d) -> Delta retention policy + VACUUM."]
    return "\n".join(lines)


def main() -> None:
    schema_src, schema_rows = build_schema()
    pipe = json.loads((ES / "pipelines" / "dept_ingest.json").read_text())
    notebook, pipeline_rows = build_notebook(pipe)
    searches_sql, search_rows = build_saved_searches()
    aggs_sql, agg_rows = build_aggregations()

    (GEN / "schema.py").write_text(schema_src, encoding="utf-8")
    (GEN / "notebook_dept_txn.py").write_text(notebook, encoding="utf-8")
    (GEN / "sql_saved_searches.sql").write_text(searches_sql, encoding="utf-8")
    (GEN / "sql_aggregations.sql").write_text(aggs_sql, encoding="utf-8")
    (GEN / "dashboard.json").write_text(json.dumps(build_dashboard(), indent=2), encoding="utf-8")
    (GEN / "conversion_report.md").write_text(
        build_report(schema_rows, pipeline_rows, search_rows, agg_rows), encoding="utf-8")

    print("Converted Elasticsearch artifacts:")
    for f in sorted(GEN.iterdir()):
        print(f"  - {f}")
    print(f"  {len(schema_rows)} fields, {len(pipeline_rows)} pipeline processors, "
          f"{len(search_rows)} saved searches, {len(agg_rows)} aggregations")


if __name__ == "__main__":
    main()
