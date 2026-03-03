#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

from docx import Document  # type: ignore


def _norm(value: str) -> str:
    return re.sub(r"\s+", " ", (value or "").strip().lower())


def _table_header(table) -> list[str]:
    if not table.rows:
        return []
    return [_norm(cell.text) for cell in table.rows[0].cells]


def _row_count(table) -> int:
    if len(table.rows) <= 1:
        return 0
    count = 0
    for row in table.rows[1:]:
        values = [cell.text.strip() for cell in row.cells]
        if any(values):
            count += 1
    return count


def _find_table(doc: Document, expected_header: list[str]):
    expected = [_norm(part) for part in expected_header]
    for table in doc.tables:
        header = _table_header(table)
        if len(header) < len(expected):
            continue
        if header[: len(expected)] == expected:
            return table
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description="Audit BA Brief DOCX structure/content.")
    parser.add_argument("docx_path", help="Path to BA Brief .docx")
    parser.add_argument("--expected-risk-rows", type=int, default=0, help="Expected number of risk register rows (exact match).")
    args = parser.parse_args()

    docx_path = Path(args.docx_path)
    if not docx_path.exists():
        print(f"[FAIL] File not found: {docx_path}")
        return 1

    doc = Document(str(docx_path))
    failures: list[str] = []
    checks: list[str] = []

    risk_header = ["ID", "Severity", "Form", "Description", "Recommended Action"]
    risk_table = _find_table(doc, risk_header)
    if risk_table is None:
        failures.append("Risk register table not found.")
    else:
        risk_rows = _row_count(risk_table)
        checks.append(f"risk_rows={risk_rows}")
        if args.expected_risk_rows > 0 and risk_rows != args.expected_risk_rows:
            failures.append(
                f"Risk row mismatch: expected {args.expected_risk_rows}, found {risk_rows}."
            )
        if risk_rows <= 0:
            failures.append("Risk register has no data rows.")
        if risk_rows > 0:
            empty_actions = 0
            for row in risk_table.rows[1:]:
                form = row.cells[2].text.strip() if len(row.cells) > 2 else ""
                action = row.cells[4].text.strip() if len(row.cells) > 4 else ""
                if form or action:
                    if not action:
                        empty_actions += 1
            checks.append(f"risk_rows_empty_action={empty_actions}")
            if empty_actions > 0:
                failures.append(f"{empty_actions} risk rows have empty Recommended Action.")

    q_header = ["Form", "Project", "Events", "SQL", "Rules", "Risk", "Score"]
    q_table = _find_table(doc, q_header)
    if q_table is None:
        failures.append("Q traceability table not found.")
    else:
        q_rows = _row_count(q_table)
        checks.append(f"q_rows={q_rows}")
        if q_rows <= 0:
            failures.append("Q traceability table has no data rows.")

    s_header = ["Form", "Sprint", "Depends On", "Shared Components", "Rationale"]
    sprint_table = _find_table(doc, s_header)
    if sprint_table is None:
        failures.append("Sprint dependency map table not found.")
    else:
        sprint_rows = _row_count(sprint_table)
        checks.append(f"sprint_rows={sprint_rows}")
        if sprint_rows <= 0:
            failures.append("Sprint dependency map has no data rows.")

    if failures:
        print("[FAIL] BA audit failed")
        for issue in failures:
            print(f"- {issue}")
        if checks:
            print("[INFO] " + " | ".join(checks))
        return 1

    print("ALL_CHECKS_PASSED")
    print("[INFO] " + " | ".join(checks))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
