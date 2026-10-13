# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — PII masking in Silver (Unity Catalog column masks)
# MAGIC Mask the holder name and tokenize the account number so consumers never see
# MAGIC raw PII. Apply once; UC enforces the mask for everyone without `pii_readers`.

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE FUNCTION IF NOT EXISTS bank.silver.mask_name(name STRING)
# MAGIC RETURNS STRING
# MAGIC RETURN CASE WHEN is_account_group_member('pii_readers') THEN name
# MAGIC             ELSE concat(substring(name, 1, 1), '***') END;

# COMMAND ----------

# MAGIC %sql
# MAGIC ALTER TABLE bank.silver.dept_txn
# MAGIC   ALTER COLUMN holder_name SET MASK bank.silver.mask_name;

# COMMAND ----------

# MAGIC %md ## Tokenize the account number (join without exposing it)
# MAGIC ```sql
# MAGIC CREATE FUNCTION IF NOT EXISTS bank.silver.tokenize_id(x STRING)
# MAGIC RETURNS STRING RETURN sha2(x, 256);
# MAGIC ALTER TABLE bank.silver.dept_txn
# MAGIC   ALTER COLUMN account_no SET MASK bank.silver.tokenize_id;
# MAGIC ```

# COMMAND ----------

# MAGIC %md ## Row filter: a group that only sees the cards department
# MAGIC ```sql
# MAGIC CREATE FUNCTION IF NOT EXISTS bank.silver.dept_filter(dept STRING)
# MAGIC RETURNS BOOLEAN
# MAGIC RETURN is_account_group_member('dept_all') OR dept = 'cards';
# MAGIC ALTER TABLE bank.silver.dept_txn
# MAGIC   SET ROW FILTER bank.silver.dept_filter ON (dept);
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC -- a non-privileged user sees masked values
# MAGIC SELECT doc_id, holder_name, account_no
# MAGIC FROM   bank.silver.dept_txn
# MAGIC LIMIT  5;
