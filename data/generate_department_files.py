"""Generate bank department feed files in mixed formats (NDJSON + CSV).

The bank's file-mover drops each department's files under data/landing/<dept>/.
Some departments ship NDJSON, some CSV — the lakehouse ingests both.

  data/landing/cards/*.ndjson
  data/landing/payments/*.csv
  data/landing/loans/*.ndjson
  data/landing/deposits/*.csv
  data/landing/wealth/*.ndjson
  data/landing/statements/*.ndjson

Run:  python data/generate_department_files.py
"""
from __future__ import annotations

import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parent
LANDING = ROOT / "landing"

DEPTS = {
    "cards": ("ndjson", ["CURRENT", "CREDIT"]),
    "payments": ("csv", ["CURRENT", "SAVINGS"]),
    "loans": ("ndjson", ["MORTGAGE", "CREDIT"]),
    "deposits": ("csv", ["SAVINGS", "CURRENT"]),
    "wealth": ("ndjson", ["INVEST"]),
}
CCYS = ["CAD", "USD", "EUR", "GBP"]
STATUS = ["POSTED", "POSTED", "POSTED", "REVERSAL", "PENDING"]
NARR = ["CARD PURCHASE STORE 4412", "ONLINE TRANSFER", "ATM WITHDRAWAL",
        "PAYROLL DEPOSIT", "BILL PAYMENT HYDRO", "INTERAC E-TRANSFER"]


def gen_txn(dept: str, fmt: str, products: list, n: int, seed: int) -> Path:
    rng = random.Random(seed)
    d = LANDING / dept
    d.mkdir(parents=True, exist_ok=True)
    start = date(2026, 1, 1)
    rows = []
    for i in range(1, n + 1):
        rows.append({
            "TXN_ID": f"{dept[:3].upper()}{i:08d}",
            "ACCOUNT_NO": f"ACC{rng.randint(1, 800):08d}",
            "DEPT_CODE": dept.upper(),
            "PRODUCT": rng.choice(products),
            "AMOUNT": round(rng.uniform(5, 20000), 2),
            "CCY": rng.choice(CCYS),
            "TXN_DATE": (start + timedelta(days=rng.randrange(31))).strftime("%Y-%m-%d"),
            "STATUS": rng.choice(STATUS),
            "NARRATIVE": rng.choice(NARR),
        })
    if fmt == "ndjson":
        p = d / f"{dept}_20260115.ndjson"
        with open(p, "w") as f:
            for r in rows:
                f.write(json.dumps(r) + "\n")
    else:
        p = d / f"{dept}_20260115.csv"
        with open(p, "w", newline="") as f:
            w = csv.DictWriter(f, fieldnames=list(rows[0].keys()))
            w.writeheader()
            w.writerows(rows)
    return p


def gen_statements(n: int = 400, seed: int = 99) -> Path:
    rng = random.Random(seed)
    d = LANDING / "statements"
    d.mkdir(parents=True, exist_ok=True)
    p = d / "statements_20260115.ndjson"
    bodies = ["Account overdraft notice issued", "Unauthorized access flagged",
              "Monthly statement ready", "Wire transfer confirmation",
              "Interest posted", "Fee reversal applied"]
    with open(p, "w") as f:
        for i in range(1, n + 1):
            f.write(json.dumps({
                "statement_id": f"ST{i:07d}",
                "account_no": f"ACC{rng.randint(1, 800):08d}",
                "dept": rng.choice(list(DEPTS)),
                "period": "2026-01",
                "body": rng.choice(bodies),
                "attachment": {"name": f"stmt_{i:07d}.pdf", "size": rng.randint(20_000, 900_000),
                               "pages": rng.randint(1, 12)},
                "generated_at": "2026-01-15T02:00:00Z",
            }) + "\n")
    return p


if __name__ == "__main__":
    for i, (dept, (fmt, products)) in enumerate(DEPTS.items()):
        print("wrote", gen_txn(dept, fmt, products, 2000, seed=40 + i))
    print("wrote", gen_statements())
