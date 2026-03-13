#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen


DEFAULT_OBJECTIVE = "Modernize the brownfield application while preserving required business capability."


def _http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    data = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method.upper())
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read().decode("utf-8")
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc}") from exc
    try:
        payload_obj = json.loads(body)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Non-JSON response from {url}: {body[:400]}") from exc
    if not isinstance(payload_obj, dict):
        raise RuntimeError(f"Unexpected response shape from {url}: {type(payload_obj).__name__}")
    return payload_obj


def _http_bytes(method: str, url: str, timeout: int = 60) -> tuple[bytes, dict[str, str]]:
    req = Request(url, headers={"Accept": "*/*"}, method=method.upper())
    try:
        with urlopen(req, timeout=timeout) as response:
            body = response.read()
            headers = {str(key).lower(): str(value) for key, value in response.headers.items()}
    except HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code} for {url}: {detail}") from exc
    except URLError as exc:
        raise RuntimeError(f"Request failed for {url}: {exc}") from exc
    return body, headers


def _write_json(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str) + "\n", encoding="utf-8")


def _safe_name(value: str) -> str:
    cleaned = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in str(value or "").strip())
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "artifact"


def _write_bytes(path: Path, payload: bytes) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(payload)


def _prepare_output_dir(out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    components_dir = out_dir / "components"
    if components_dir.exists():
        for path in components_dir.glob("*.json"):
            path.unlink()


def _run_payload(args: argparse.Namespace) -> dict[str, Any]:
    architect_controls: dict[str, Any] = {}
    approved_adr_ids = [str(value).strip() for value in getattr(args, "approved_adrs", []) if str(value).strip()]
    approved_services = [str(value).strip() for value in getattr(args, "approved_services", []) if str(value).strip()]
    deferred_services = [str(value).strip() for value in getattr(args, "deferred_services", []) if str(value).strip()]
    phase_overrides: dict[str, int] = {}
    for value in getattr(args, "phase_overrides", []) or []:
        raw = str(value).strip()
        if "=" not in raw:
            raise ValueError(f"Invalid --phase-override value `{raw}`; expected ServiceName=PhaseNumber")
        service_name, phase_text = [part.strip() for part in raw.split("=", 1)]
        if not service_name or not phase_text.isdigit():
            raise ValueError(f"Invalid --phase-override value `{raw}`; expected ServiceName=PhaseNumber")
        phase_overrides[service_name] = int(phase_text)
    if approved_adr_ids:
        architect_controls["approved_adr_ids"] = approved_adr_ids
    if approved_services:
        architect_controls["approved_services"] = approved_services
    if deferred_services:
        architect_controls["deferred_services"] = deferred_services
    if phase_overrides:
        architect_controls["service_phase_overrides"] = phase_overrides
    return {
        "use_case": "code_modernization",
        "objectives": args.objectives.strip() or DEFAULT_OBJECTIVE,
        "legacy_code": "",
        "modernization_language": args.target_language,
        "deployment_target": "local",
        "human_approval": False,
        "strict_security_mode": False,
        "provider": args.provider,
        "model": args.model,
        "integration_context": {
            "project_state_mode": "brownfield",
            "project_state_detected": "brownfield",
            "brownfield": {
                "repo_provider": "github",
                "repo_url": args.repo_url,
                "issue_provider": "",
                "issue_project": "",
                "docs_url": "",
                "runtime_telemetry": False,
            },
            "scan_scope": {
                "analysis_depth": args.analysis_depth,
                "telemetry_mode": "off",
                "modernization_source_mode": "repo_scan",
                "include_paths": [],
                "exclude_paths": [],
            },
            "sample_dataset_enabled": False,
            "architect_controls": architect_controls,
        },
    }


def _stage_two_ready(run: dict[str, Any]) -> bool:
    pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
    architect_output = pipeline_state.get("architect_output", {}) if isinstance(pipeline_state.get("architect_output", {}), dict) else {}
    return bool(
        isinstance(architect_output.get("architect_package", {}), dict)
        and architect_output.get("architect_package")
        and isinstance(architect_output.get("architect_handoff_package", {}), dict)
        and architect_output.get("architect_handoff_package")
    )


def _try_fetch_stage_two_artifacts(base_url: str, run_id: str) -> tuple[dict[str, Any], dict[str, Any]]:
    try:
        architect_package_payload = _http_json("GET", f"{base_url}/api/runs/{run_id}/architect-package", timeout=30)
        architect_handoff_payload = _http_json("GET", f"{base_url}/api/runs/{run_id}/architect-handoff", timeout=30)
    except RuntimeError:
        return {}, {}
    architect_package = architect_package_payload.get("architect_package", {}) if isinstance(architect_package_payload.get("architect_package", {}), dict) else {}
    architect_handoff = architect_handoff_payload.get("architect_handoff_package", {}) if isinstance(architect_handoff_payload.get("architect_handoff_package", {}), dict) else {}
    return architect_package, architect_handoff


def _fetch_component_handoffs(base_url: str, run_id: str, architect_handoff: dict[str, Any]) -> dict[str, dict[str, Any]]:
    outputs: dict[str, dict[str, Any]] = {}
    for spec in architect_handoff.get("component_specs", []):
        if not isinstance(spec, dict):
            continue
        component_name = str(spec.get("component_name", "")).strip()
        if not component_name:
            continue
        query = urlencode({"component": component_name})
        payload = _http_json(
            "GET",
            f"{base_url}/api/runs/{run_id}/architect-handoff/component?{query}",
            timeout=60,
        )
        handoff = payload.get("component_handoff", {}) if isinstance(payload.get("component_handoff", {}), dict) else {}
        if handoff:
            outputs[component_name] = handoff
    return outputs


def _fetch_hld_docx(base_url: str, run_id: str, doc_type: str) -> tuple[bytes, str]:
    content, headers = _http_bytes("GET", f"{base_url}/api/runs/{run_id}/architect-hld-docx?type={doc_type}", timeout=120)
    disposition = headers.get("content-disposition", "")
    filename = ""
    if "filename=" in disposition:
        filename = disposition.split("filename=", 1)[-1].strip().strip('"')
    return content, filename


def run(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")
    out_dir = Path(args.output_dir).resolve()
    _prepare_output_dir(out_dir)

    health = _http_json("GET", f"{base_url}/api/health", timeout=20)
    if not bool(health.get("ok")):
        raise RuntimeError(f"Health check failed: {health}")

    preflight = _http_json("POST", f"{base_url}/api/runs/preflight", payload=_run_payload(args), timeout=60)
    if not bool(preflight.get("ok")):
        raise RuntimeError(f"Preflight failed: {preflight}")

    started = _http_json("POST", f"{base_url}/api/runs", payload=_run_payload(args), timeout=120)
    if not bool(started.get("ok")):
        raise RuntimeError(f"Run start failed: {started}")
    run_id = str(started.get("run_id", "")).strip()
    if not run_id:
        raise RuntimeError(f"Run start response missing run_id: {started}")

    deadline = time.time() + max(60, int(args.timeout_seconds))
    final_run: dict[str, Any] | None = None
    architect_package: dict[str, Any] = {}
    architect_handoff: dict[str, Any] = {}
    while time.time() < deadline:
        row = _http_json("GET", f"{base_url}/api/runs/{run_id}", timeout=30)
        if not bool(row.get("ok")):
            raise RuntimeError(f"Run read failed: {row}")
        run = row.get("run", {}) if isinstance(row.get("run", {}), dict) else {}
        status = str(run.get("status", "")).strip().lower()
        current_stage = int(run.get("current_stage", 0) or 0)
        print(f"[info] run={run_id} status={status or 'unknown'} stage={current_stage}")
        if _stage_two_ready(run):
            final_run = run
            pipeline_state = final_run.get("pipeline_state", {}) if isinstance(final_run.get("pipeline_state", {}), dict) else {}
            architect_output = pipeline_state.get("architect_output", {}) if isinstance(pipeline_state.get("architect_output", {}), dict) else {}
            architect_package = architect_output.get("architect_package", {}) if isinstance(architect_output.get("architect_package", {}), dict) else {}
            architect_handoff = architect_output.get("architect_handoff_package", {}) if isinstance(architect_output.get("architect_handoff_package", {}), dict) else {}
            break
        if current_stage >= 2 or status in {"completed", "failed", "aborted"}:
            architect_package, architect_handoff = _try_fetch_stage_two_artifacts(base_url, run_id)
            if architect_package and architect_handoff:
                final_run = run
                break
        if status in {"completed", "failed", "aborted"}:
            final_run = run
            break
        time.sleep(max(1, int(args.poll_seconds)))

    if final_run is None:
        raise RuntimeError(f"Timed out waiting for Stage 2 artifacts for run {run_id}")

    if not _stage_two_ready(final_run):
        architect_package, architect_handoff = _try_fetch_stage_two_artifacts(base_url, run_id)
        if not architect_package or not architect_handoff:
            raise RuntimeError(f"Run {run_id} ended before Stage 2 architect artifacts were available: {final_run.get('status')}")

    if not architect_package or not architect_handoff:
        raise RuntimeError(f"Stage 2 artifact download failed for run {run_id}")

    component_handoffs = _fetch_component_handoffs(base_url, run_id, architect_handoff)
    hld_outputs: dict[str, str] = {}
    for doc_type in ("legacy", "target"):
        try:
            content, filename = _fetch_hld_docx(base_url, run_id, doc_type)
        except RuntimeError:
            continue
        safe_filename = _safe_name(filename or f"{doc_type}-hld-{run_id}.docx")
        _write_bytes(out_dir / safe_filename, content)
        hld_outputs[doc_type] = safe_filename
    pipeline_state = final_run.get("pipeline_state", {}) if isinstance(final_run.get("pipeline_state", {}), dict) else {}
    architect_output = pipeline_state.get("architect_output", {}) if isinstance(pipeline_state.get("architect_output", {}), dict) else {}

    _write_json(out_dir / "run.json", final_run)
    _write_json(out_dir / "architect_output.json", architect_output)
    _write_json(out_dir / "architect_package.json", architect_package)
    _write_json(out_dir / "architect_handoff_package.json", architect_handoff)
    for component_name, payload in component_handoffs.items():
        _write_json(out_dir / "components" / f"{_safe_name(component_name)}.json", payload)
    _write_json(
        out_dir / "summary.json",
        {
            "run_id": run_id,
            "status": final_run.get("status", ""),
            "current_stage": final_run.get("current_stage", 0),
            "architect_package_artifact_count": len(architect_package.get("artifacts", {})) if isinstance(architect_package.get("artifacts", {}), dict) else 0,
            "component_count": len(architect_handoff.get("component_specs", [])) if isinstance(architect_handoff.get("component_specs", []), list) else 0,
            "component_handoff_count": len(component_handoffs),
            "hld_documents": hld_outputs,
            "output_dir": str(out_dir),
        },
    )

    print(f"[info] wrote architect artifacts for run {run_id} to {out_dir}")

    if args.abort_after_export:
        try:
            _http_json("POST", f"{base_url}/api/runs/{run_id}/abort", payload={"reason": "Architect artifacts exported via CLI"}, timeout=30)
            print(f"[info] run {run_id} aborted after architect export")
        except Exception as exc:
            print(f"[warn] failed to abort run {run_id}: {exc}", file=sys.stderr)

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Export Stage 2 architect artifacts through the Synthetix API.")
    parser.add_argument("--base-url", required=True, help="Synthetix base URL, e.g. https://synthetix.example.com")
    parser.add_argument("--repo-url", required=True, help="GitHub repository URL to modernize")
    parser.add_argument("--provider", default="openai")
    parser.add_argument("--model", default="gpt-5")
    parser.add_argument("--target-language", default="C#")
    parser.add_argument("--objectives", default=DEFAULT_OBJECTIVE)
    parser.add_argument("--analysis-depth", default="deep")
    parser.add_argument("--poll-seconds", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=int, default=3600)
    parser.add_argument("--output-dir", default="architect_export")
    parser.add_argument("--abort-after-export", action="store_true")
    parser.add_argument("--approved-adr", action="append", dest="approved_adrs", default=[], help="Explicitly mark an ADR id as accepted for the architect run")
    parser.add_argument("--approved-service", action="append", dest="approved_services", default=[], help="Mark the ADR covering a service as accepted")
    parser.add_argument("--defer-service", action="append", dest="deferred_services", default=[], help="Defer a service to a later dispatch phase")
    parser.add_argument("--phase-override", action="append", dest="phase_overrides", default=[], help="Override a service phase, e.g. CustomerService=2")
    return parser


if __name__ == "__main__":
    parser = build_parser()
    arguments = parser.parse_args()
    try:
        raise SystemExit(run(arguments))
    except Exception as exc:
        print(f"[error] {exc}", file=sys.stderr)
        raise SystemExit(1)
