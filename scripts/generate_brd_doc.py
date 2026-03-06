#!/usr/bin/env python3
"""Generate BRD DOCX (and BRD artifacts) from an Analyst MD without using UI.

Usage:
  python scripts/generate_brd_doc.py \
    --md run_artifacts/<run-id>/docgen_exports/<ts>/analyst-output.md \
    --out run_artifacts/manual_exports/brd_check
"""

from __future__ import annotations

import argparse
import json
import subprocess
import sys
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
DOCGEN_DIR = ROOT / "synthetix-docgen"
DOCGEN_INDEX = DOCGEN_DIR / "index.js"


def _load_json(path: Path) -> dict:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_meta_file(args: argparse.Namespace, out_dir: Path) -> Path | None:
    payload = {
        "template_family": args.template_family,
        "client_name": args.client_name,
        "project_name": args.project_name,
        "document_id": args.document_id,
        "version": args.version,
    }
    clean = {k: str(v).strip() for k, v in payload.items() if str(v or "").strip()}
    if not clean:
        return None
    meta_path = out_dir / "brd_meta.json"
    meta_path.write_text(json.dumps(clean, indent=2, ensure_ascii=True), encoding="utf-8")
    return meta_path


def run() -> int:
    ap = argparse.ArgumentParser(description="Generate BRD DOCX from analyst markdown (no UI).")
    ap.add_argument("--md", required=True, help="Path to analyst-output.md")
    ap.add_argument("--out", required=True, help="Output directory for generated files")
    ap.add_argument("--template-family", default="JHA_OpenAnywhere_v1", help="BRD template family")
    ap.add_argument("--client-name", default="", help="Optional client name override")
    ap.add_argument("--project-name", default="", help="Optional project name override")
    ap.add_argument("--document-id", default="", help="Optional BRD document ID override")
    ap.add_argument("--version", default="", help="Optional BRD version override")
    args = ap.parse_args()

    md_path = Path(args.md).expanduser().resolve()
    out_dir = Path(args.out).expanduser().resolve()

    if not md_path.exists():
        print(f"[error] analyst MD not found: {md_path}")
        return 2
    if not DOCGEN_INDEX.exists():
        print(f"[error] docgen entrypoint not found: {DOCGEN_INDEX}")
        return 2

    out_dir.mkdir(parents=True, exist_ok=True)
    meta_path = _write_meta_file(args, out_dir)

    cmd = [
        "node",
        str(DOCGEN_INDEX),
        "--md",
        str(md_path),
        "--out",
        str(out_dir),
    ]
    if meta_path:
        cmd.extend(["--meta", str(meta_path)])

    print(f"[info] running docgen: {' '.join(cmd)}")
    proc = subprocess.run(
        cmd,
        cwd=str(DOCGEN_DIR),
        capture_output=True,
        text=True,
    )
    if proc.stdout.strip():
        print(proc.stdout.strip())
    if proc.stderr.strip():
        print(proc.stderr.strip(), file=sys.stderr)
    if proc.returncode != 0:
        print(f"[error] docgen failed with exit code {proc.returncode}")
        return proc.returncode

    brd_path = out_dir / "brd.docx"
    qa_path = out_dir / "brd_qa_report_v1.json"
    bundle_path = out_dir / "brd_package_v1.json"
    manifest_path = out_dir / "brd_render_manifest_v1.json"

    qa = _load_json(qa_path)
    status = str(qa.get("status", "unknown")).upper()
    blockers = qa.get("blocking_errors", []) if isinstance(qa.get("blocking_errors"), list) else []

    print("[ok] BRD artifacts generated")
    print(f"  BRD package: {bundle_path if bundle_path.exists() else '(missing)'}")
    print(f"  BRD QA:      {qa_path if qa_path.exists() else '(missing)'} [status={status}]")
    print(f"  Manifest:    {manifest_path if manifest_path.exists() else '(missing)'}")
    print(f"  BRD DOCX:    {brd_path if brd_path.exists() else '(not generated)'}")

    if blockers:
        print("[warn] BRD QA blockers:")
        for b in blockers:
            print(f"  - {b}")

    # Return non-zero only when QA failed hard and DOCX was not generated.
    if status == "FAIL" and not brd_path.exists():
        return 3
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

