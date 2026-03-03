#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
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
    parser = argparse.ArgumentParser(description="Audit Technical Workbook DOCX structure/content.")
    parser.add_argument("docx_path", help="Path to Technical Workbook .docx")
    parser.add_argument("--expected-sql-rows", type=int, default=0, help="Expected SQL catalog rows (exact match).")
    parser.add_argument("--expected-risk-rows", type=int, default=0, help="Expected risk register rows (exact match).")
    args = parser.parse_args()

    docx_path = Path(args.docx_path)
    if not docx_path.exists():
        print(f"[FAIL] File not found: {docx_path}")
        return 1

    doc = Document(str(docx_path))
    failures: list[str] = []
    checks: list[str] = []

    deps_header = ["Component", "Type", "GUID / Reference", "Used By Forms", "Risk", "Migration Action"]
    deps_table = _find_table(doc, deps_header)
    if deps_table is None:
        failures.append("Dependencies table not found.")
    else:
        deps_rows = _row_count(deps_table)
        checks.append(f"dependency_rows={deps_rows}")
        if deps_rows <= 0:
            failures.append("Dependencies table has no data rows.")

    sql_header = ["SQL ID", "Handler", "Operation", "Tables", "Columns", "Notes"]
    sql_table = _find_table(doc, sql_header)
    if sql_table is None:
        failures.append("SQL catalog table not found.")
    else:
        sql_rows = _row_count(sql_table)
        checks.append(f"sql_rows={sql_rows}")
        if args.expected_sql_rows > 0 and sql_rows != args.expected_sql_rows:
            failures.append(f"SQL row mismatch: expected {args.expected_sql_rows}, found {sql_rows}.")
        if sql_rows <= 0:
            failures.append("SQL catalog has no data rows.")

    risk_header = ["ID", "Severity", "Form", "Technical Description", "Recommended Action"]
    risk_table = _find_table(doc, risk_header)
    if risk_table is None:
        failures.append("Technical risk register table not found.")
    else:
        risk_rows = _row_count(risk_table)
        checks.append(f"risk_rows={risk_rows}")
        if args.expected_risk_rows > 0 and risk_rows != args.expected_risk_rows:
            failures.append(
                f"Risk row mismatch: expected {args.expected_risk_rows}, found {risk_rows}."
            )
        if risk_rows <= 0:
            failures.append("Technical risk register has no data rows.")

    dep_map_header = ["From", "To", "Link Type", "Evidence", "Blocks Sprint"]
    dep_map_table = _find_table(doc, dep_map_header)
    if dep_map_table is None:
        failures.append("Dependency map table not found.")
    else:
        dep_map_rows = _row_count(dep_map_table)
        checks.append(f"dependency_map_rows={dep_map_rows}")
        if dep_map_rows <= 0:
            failures.append("Dependency map has no data rows.")

    if failures:
        print("[FAIL] Tech workbook audit failed")
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
