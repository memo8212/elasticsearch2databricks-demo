#!/usr/bin/env bash
# ============================================================================
# Legacy loader: bulk-index each department's landed files into Elasticsearch.
# The bank's file-mover drops department files in /landing/<dept>/ and cron
# runs this script.
# ============================================================================
set -euo pipefail

ES="${ES_HOST:-http://es-bank:9200}"
PIPELINE="dept_ingest"
LANDING="${LANDING:-/landing}"

# create the index template + ingest pipeline (idempotent)
curl -s -X PUT "$ES/_index_template/bank-dept" \
  -H 'Content-Type: application/json' \
  --data-binary @legacy/es/mappings/dept_txn.json >/dev/null

curl -s -X PUT "$ES/_ingest/pipeline/$PIPELINE" \
  -H 'Content-Type: application/json' \
  --data-binary @legacy/es/pipelines/dept_ingest.json >/dev/null

for dept in cards payments loans deposits wealth; do
  idx="bank-dept-${dept}"
  echo "[$(date)] indexing $dept into $idx"
  # convert each NDJSON file into a _bulk request with the ingest pipeline
  for f in "$LANDING/$dept"/*.ndjson; do
    [ -e "$f" ] || continue
    { while IFS= read -r line; do
        printf '{"index":{"_index":"%s","pipeline":"%s"}}\n%s\n' "$idx" "$PIPELINE" "$line"
      done < "$f"
    } | curl -s -X POST "$ES/_bulk" \
          -H 'Content-Type: application/x-ndjson' --data-binary @- >/dev/null
  done
done

echo "[$(date)] refresh + report"
curl -s -X POST "$ES/bank-dept-*/_refresh" >/dev/null
curl -s "$ES/_cat/indices/bank-dept-*?h=index,docs.count,store.size"
