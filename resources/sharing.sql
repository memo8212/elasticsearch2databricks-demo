-- ============================================================================
-- Delta Sharing: expose Gold to a partner/regulator WITHOUT copying data.
-- Replaces "export a file and email it" (the transfer step) for governed sharing.
-- ============================================================================

CREATE SHARE IF NOT EXISTS bank_dept_share
  COMMENT 'Bank department summary shared with the regulator';

ALTER SHARE bank_dept_share
  ADD TABLE bank.gold.dept_summary;

-- Recipient (an external organization's sharing identifier).
-- In practice this is created by the recipient and you paste their identifier:
--   CREATE RECIPIENT partner_regulator;
CREATE RECIPIENT IF NOT EXISTS partner_regulator;

GRANT SELECT ON SHARE bank_dept_share TO RECIPIENT partner_regulator;

-- The recipient reads it with a profile file — no data leaves your storage:
--   SELECT * FROM bank_dept_share.bank.gold.dept_summary;

-- Revoke when the engagement ends:
--   REVOKE SELECT ON SHARE bank_dept_share FROM RECIPIENT partner_regulator;

SELECT 'share configured' AS status;
