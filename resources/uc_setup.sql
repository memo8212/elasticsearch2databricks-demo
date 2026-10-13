-- Unity Catalog setup: one governed place for all bank department files.
-- Replaces the Elasticsearch indices/aliases with Delta tables + a landing volume.

CREATE CATALOG IF NOT EXISTS bank;
CREATE SCHEMA IF NOT EXISTS bank.landing COMMENT 'raw department file landing';
CREATE SCHEMA IF NOT EXISTS bank.bronze  COMMENT 'ingested, audit columns';
CREATE SCHEMA IF NOT EXISTS bank.silver  COMMENT 'conformed (ingest pipeline applied)';
CREATE SCHEMA IF NOT EXISTS bank.gold    COMMENT 'analytics + exports';

CREATE VOLUME IF NOT EXISTS bank.landing.dept
  COMMENT 'Department feed files (mixed CSV/NDJSON), one folder per department';

-- Bronze (was the ES index _source)
CREATE TABLE IF NOT EXISTS bank.bronze.dept_txn (
  TXN_ID STRING, ACCOUNT_NO STRING, DEPT_CODE STRING, PRODUCT STRING,
  AMOUNT STRING, CCY STRING, TXN_DATE STRING, STATUS STRING, NARRATIVE STRING,
  _source_dept STRING, _ingested_at TIMESTAMP);

-- File registry (file-level lineage)
CREATE TABLE IF NOT EXISTS bank.bronze.file_registry (
  path STRING, dept STRING, size BIGINT, status STRING, md5 STRING);

-- Silver (ingest pipeline output; typed)
CREATE TABLE IF NOT EXISTS bank.silver.dept_txn (
  doc_id STRING, dept STRING, account_no STRING, customer_id STRING, product STRING,
  amount DOUBLE, base_amount DOUBLE, ccy STRING, base_ccy STRING, txn_date DATE,
  status STRING, txn_kind STRING, tags STRING)
COMMENT 'Ingest pipeline: rename/lowercase/date/convert/gsub/set/script/grok';

-- Gold (Kibana aggregation) + transfer audit
CREATE TABLE IF NOT EXISTS bank.gold.dept_summary (
  dept STRING, total_base DECIMAL(18,2), txn_count BIGINT);
CREATE TABLE IF NOT EXISTS bank.gold.transfer_audit (
  target STRING, rows BIGINT, status STRING);

-- Retention: ES ILM delete (2555d) -> Delta retention policy.
-- ALTER TABLE bank.silver.dept_txn SET TBLPROPERTIES ('delta.deletedFileRetentionDuration' = 'interval 2555 days');

-- Consumers read Gold only.
-- GRANT SELECT ON TABLE bank.gold.dept_summary TO `analysts`;

SELECT 'bank setup complete' AS status;
