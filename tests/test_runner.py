"""Tests for the runner helpers: PII masking, search index, sharing manifest."""
import pandas as pd

from runner.run_demo import (
    apply_pipeline_pandas, build_search_index, build_sharing_manifest,
)


def _dept_frame():
    return pd.DataFrame([
        {"TXN_ID": "CARD00000001", "ACCOUNT_NO": "ACC00000042", "HOLDER_NAME": "JOHN SMITH",
         "DEPT_CODE": "CARDS", "PRODUCT": "CREDIT", "AMOUNT": 100.0, "CCY": "CAD",
         "TXN_DATE": "2026-01-05", "STATUS": "POSTED", "NARRATIVE": "ONLINE TRANSFER"},
        {"TXN_ID": "CARD00000002", "ACCOUNT_NO": "ACC00000042", "HOLDER_NAME": "MARIA GARCIA",
         "DEPT_CODE": "CARDS", "PRODUCT": "CREDIT", "AMOUNT": 250.0, "CCY": "USD",
         "TXN_DATE": "2026-01-06", "STATUS": "REVERSAL", "NARRATIVE": "ATM WITHDRAWAL"},
    ])


def test_pii_masking():
    df = apply_pipeline_pandas(_dept_frame())
    # name masked to first initial
    assert list(df["holder_name"]) == ["J***", "M***"]
    # account number tokenized (sha256 hex prefix, stable, 16 chars)
    assert df["account_token"].str.len().eq(16).all()
    assert df["account_token"].nunique() == 1  # same account -> same token
    # raw PII not present as-is
    assert "JOHN SMITH" not in set(df["holder_name"])


def test_search_index_build_and_query():
    gold = pd.DataFrame([{"dept": "cards", "total_base": 30_000_000, "txn_count": 10},
                         {"dept": "loans", "total_base": 5_000_000, "txn_count": 3}])
    hits = build_search_index(gold)
    assert len(hits) == 1 and hits[0]["dept"] == "cards"
    assert hits[0]["_id"] == "dept::cards"


def test_sharing_manifest():
    gold = pd.DataFrame([{"dept": "cards", "total_base": 1.0, "txn_count": 1}])
    manifest = build_sharing_manifest(gold, {"md5": "abc123", "reconciled": True})
    assert manifest["share"] == "bank_dept_share"
    assert manifest["table"] == "bank.gold.dept_summary"
    assert manifest["recipient"] == "partner_regulator"
    assert manifest["rows"] == 1 and manifest["reconciled"] is True
