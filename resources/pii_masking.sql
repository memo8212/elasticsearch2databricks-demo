-- ============================================================================
-- PII masking + row filters (Unity Catalog) for the bank department feeds.
-- Apply once after creating the Silver tables. Requires a UC-enabled workspace.
-- ============================================================================

-- Mask a person's name: keep the first initial, hide the rest (unless privileged).
CREATE FUNCTION IF NOT EXISTS bank.silver.mask_name(name STRING)
RETURNS STRING
RETURN CASE
    WHEN is_account_group_member('pii_readers') THEN name
    ELSE concat(substring(name, 1, 1), '***')
END;

-- Tokenize an account number (one-way hash) for joins/analytics without exposing it.
CREATE FUNCTION IF NOT EXISTS bank.silver.tokenize_id(x STRING)
RETURNS STRING
RETURN sha2(x, 256);

-- Apply the mask to the Silver column.
ALTER TABLE bank.silver.dept_txn
  ALTER COLUMN holder_name SET MASK bank.silver.mask_name;

-- Keep the raw account number restricted: expose only the token to most users.
-- (Store the raw value in a restricted table/column; grant the token column widely.)
ALTER TABLE bank.silver.dept_txn
  ALTER COLUMN account_no SET MASK bank.silver.tokenize_id;

-- Optional row-level filter: analysts in 'dept_cards_only' see only cards.
CREATE FUNCTION IF NOT EXISTS bank.silver.dept_filter(dept STRING)
RETURNS BOOLEAN
RETURN is_account_group_member('dept_all') OR dept = 'cards';

ALTER TABLE bank.silver.dept_txn
  SET ROW FILTER bank.silver.dept_filter ON (dept);

-- Grants: consumers never see raw PII.
-- GRANT USAGE ON SCHEMA bank.silver TO `analysts`;
-- GRANT SELECT ON TABLE bank.silver.dept_txn TO `analysts`;

SELECT 'pii masking applied' AS status;
