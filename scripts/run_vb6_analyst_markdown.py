#!/usr/bin/env python3
from __future__ import annotations

import argparse
import importlib.util
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

_ANALYST_REPORT_PATH = ROOT / "utils" / "analyst_report.py"
_ANALYST_REPORT_SPEC = importlib.util.spec_from_file_location("synthetix_analyst_report", _ANALYST_REPORT_PATH)
if _ANALYST_REPORT_SPEC is None or _ANALYST_REPORT_SPEC.loader is None:
    raise RuntimeError(f"Unable to load analyst_report module from {_ANALYST_REPORT_PATH}")
_ANALYST_REPORT_MODULE = importlib.util.module_from_spec(_ANALYST_REPORT_SPEC)
_ANALYST_REPORT_SPEC.loader.exec_module(_ANALYST_REPORT_MODULE)
build_analyst_report_v2 = _ANALYST_REPORT_MODULE.build_analyst_report_v2


DEFAULT_BASE_URL = "http://127.0.0.1:8788"
DEFAULT_REPO_URL = "https://github.com/vishakjai/TestVBProject1"
DEFAULT_OBJECTIVE = (
    "Analyze this legacy VB6 repository, identify all projects/forms/modules/ActiveX dependencies/"
    "business rules/SQL behaviors, and produce a detailed modernization-ready analyst artifact."
)
VB6_EXTENSIONS = {".cls", ".frm", ".frx", ".bas", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".ocx", ".dcx", ".dca", ".dsr"}
VB6_TEXT_EXTENSIONS = {".cls", ".frm", ".bas", ".ctl", ".vbp", ".vbg", ".dca", ".dcx", ".dsr"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _http_json(method: str, url: str, payload: dict[str, Any] | None = None, timeout: int = 60) -> dict[str, Any]:
    data: bytes | None = None
    headers = {"Accept": "application/json"}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = Request(url, data=data, headers=headers, method=method)
    try:
        with urlopen(req, timeout=timeout) as resp:
            raw = resp.read().decode("utf-8", errors="replace")
    except TimeoutError as exc:
        raise RuntimeError(f"Request timed out on {url} (timeout={timeout}s)") from exc
    except HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace") if exc.fp else ""
        raise RuntimeError(f"HTTP {exc.code} on {url}: {body}") from exc
    except URLError as exc:
        raise RuntimeError(f"URL error on {url}: {exc}") from exc

    try:
        parsed = json.loads(raw or "{}")
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Invalid JSON from {url}: {raw[:500]}") from exc
    if not isinstance(parsed, dict):
        raise RuntimeError(f"Unexpected response shape from {url}")
    return parsed


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _escape_pipe(value: Any) -> str:
    return _clean(value).replace("|", "\\|")


def _normalize_open_question(row: Any, idx: int) -> dict[str, str]:
    if isinstance(row, str):
        return {
            "id": f"Q-{idx + 1:03d}",
            "question": _clean(row) or "Clarification required",
            "owner": "Unassigned",
            "severity": "medium",
            "context": "",
        }
    if not isinstance(row, dict):
        return {
            "id": f"Q-{idx + 1:03d}",
            "question": "Clarification required",
            "owner": "Unassigned",
            "severity": "medium",
            "context": "",
        }
    severity = _clean(row.get("severity") or row.get("priority") or "medium").lower()
    if severity not in {"blocker", "high", "medium", "low"}:
        severity = "medium"
    return {
        "id": _clean(row.get("id")) or f"Q-{idx + 1:03d}",
        "question": _clean(row.get("question") or row.get("text") or row.get("summary")) or "Clarification required",
        "owner": _clean(row.get("owner") or row.get("assignee")) or "Unassigned",
        "severity": severity,
        "context": _clean(row.get("context") or row.get("impact")),
    }


def build_full_markdown(output: dict[str, Any], mode: str = "full") -> str:
    revised = _clean(output.get("human_revised_document_markdown"))
    if revised:
        return revised

    report = build_analyst_report_v2(output)
    metadata = _as_dict(report.get("metadata"))
    project = _as_dict(metadata.get("project"))
    brief = _as_dict(report.get("decision_brief"))
    glance = _as_dict(brief.get("at_a_glance"))
    inventory = _as_dict(glance.get("inventory_summary"))
    strategy = _as_dict(brief.get("recommended_strategy"))
    decisions = _as_dict(brief.get("decisions_required"))
    delivery_spec = _as_dict(report.get("delivery_spec"))
    testing = _as_dict(delivery_spec.get("testing_and_evidence"))
    backlog = _as_list(_as_dict(delivery_spec.get("backlog")).get("items"))
    appendix = _as_dict(report.get("appendix"))
    open_questions = _as_list(delivery_spec.get("open_questions"))

    lines: list[str] = [
        f"# Modernization Brief - {_clean(project.get('name')) or 'Untitled Project'}",
        "",
        "## Header",
        f"- Objective: {_clean(project.get('objective')) or 'Not provided'}",
        f"- Domain: {_clean(project.get('domain')) or 'software'}",
        f"- Repo: {_clean(_as_dict(metadata.get('context_reference')).get('repo')) or 'n/a'} @ {_clean(_as_dict(metadata.get('context_reference')).get('branch')) or 'main'} ({_clean(_as_dict(metadata.get('context_reference')).get('commit_sha')) or 'n/a'})",
        f"- SIL Versions: SCM {_clean(_as_dict(metadata.get('context_reference')).get('scm_version')) or '1.0'} / CP {_clean(_as_dict(metadata.get('context_reference')).get('cp_version')) or '1.0'} / HA {_clean(_as_dict(metadata.get('context_reference')).get('ha_version')) or '1.0'}",
        f"- Generated At: {_clean(metadata.get('generated_at'))}",
        "",
        "## Decision Brief",
        "",
        "| Category | Summary |",
        "|---|---|",
        f"| Modernization readiness | {_clean(glance.get('readiness_score') or 'n/a')}/100 |",
        f"| Risk tier | {_clean(glance.get('risk_tier') or 'n/a')} |",
        f"| Inventory | {_clean(inventory.get('projects') or 0)} project(s), {_clean(inventory.get('forms') or 0)} forms/usercontrols, {_clean(inventory.get('dependencies') or 0)} dependencies |",
        f"| Data touchpoints | {', '.join(_as_list(inventory.get('tables_touched')))} |",
        f"| Headline | {_escape_pipe(glance.get('headline'))} |",
        "",
        "### Recommended strategy",
        f"- {_clean(strategy.get('name')) or 'Phased modernization'}: {_clean(strategy.get('rationale'))}",
    ]

    for phase in _as_list(strategy.get("phases")):
        phase_d = _as_dict(phase)
        lines.append(
            f"- {_clean(phase_d.get('id'))} {_clean(phase_d.get('title'))}: {_clean(phase_d.get('outcome'))}"
        )

    lines.extend(["", "### Decisions Required (Blocking)"])
    blocking = _as_list(decisions.get("blocking"))
    if blocking:
        for row in blocking:
            row_d = _as_dict(row)
            lines.append(f"- {_clean(row_d.get('id')) or 'DEC'}: {_clean(row_d.get('question'))}")
            lines.append(f"  - Recommendation: {_clean(row_d.get('default_recommendation'))}")
    else:
        lines.append("- None")

    lines.extend(["", "### Decisions Required (Non-blocking)"])
    non_blocking = _as_list(decisions.get("non_blocking"))
    if non_blocking:
        for row in non_blocking:
            row_d = _as_dict(row)
            lines.append(f"- {_clean(row_d.get('id')) or 'DEC'}: {_clean(row_d.get('question'))}")
    else:
        lines.append("- None")

    lines.extend(["", "## Delivery Spec", "", "### Backlog"])
    lines.append("| ID | Pri | Type | Outcome | Acceptance |")
    lines.append("|---|---|---|---|---|")
    if backlog:
        for item in backlog[:80]:
            item_d = _as_dict(item)
            ac = " / ".join(_as_list(item_d.get("acceptance_criteria"))[:2])
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _clean(item_d.get("id")),
                    _clean(item_d.get("priority")),
                    _clean(item_d.get("type")),
                    _escape_pipe(item_d.get("title") or item_d.get("outcome")),
                    _escape_pipe(ac or "n/a"),
                )
            )
    else:
        lines.append("| - | - | - | No backlog items generated | - |")

    lines.extend(["", "### Testing and Evidence", "- Golden flows:"])
    golden_flows = _as_list(testing.get("golden_flows"))
    if golden_flows:
        for flow in golden_flows:
            flow_d = _as_dict(flow)
            lines.append(
                f"  - {_clean(flow_d.get('id')) or 'GF'}: {_clean(flow_d.get('name'))} | entry={_clean(flow_d.get('entrypoint'))}"
            )
    else:
        lines.append("  - None")

    lines.append("- Quality gates:")
    quality_gates = _as_list(testing.get("quality_gates"))
    if quality_gates:
        for gate in quality_gates:
            gate_d = _as_dict(gate)
            lines.append(
                f"  - {_clean(gate_d.get('id')) or 'gate'}: {(_clean(gate_d.get('result')) or 'warn').upper()} | {_clean(gate_d.get('description'))}"
            )
    else:
        lines.append("  - None")

    lines.extend(["", "### Open Questions"])
    if open_questions:
        for idx, q in enumerate(open_questions):
            row = _normalize_open_question(q, idx)
            lines.append(
                f"- [{row['severity'].upper()}] {row['id']}: {row['question']} (owner: {row['owner']})"
            )
            if row["context"]:
                lines.append(f"  - Context: {row['context']}")
    else:
        lines.append("- None")

    lines.extend(["", "## Evidence Appendix"])
    refs = _as_dict(appendix.get("artifact_refs"))
    for key, value in refs.items():
        if _clean(value):
            lines.append(f"- {key}: {_clean(value)}")
    lines.append("- High-volume sections included in structured artifact (inventory, dependencies, event map, SQL catalog, business rules).")

    lines.extend(["", "## Appendix Snapshot"])
    hv = _as_dict(appendix.get("high_volume_sections"))
    raw = _as_dict(output.get("raw_artifacts"))
    raw_event_map = _as_list(_as_dict(raw.get("event_map")).get("entries"))
    raw_sql = _as_list(_as_dict(raw.get("sql_catalog")).get("statements"))
    raw_sql_map = _as_list(_as_dict(raw.get("sql_map")).get("entries"))
    raw_procedures = _as_list(_as_dict(raw.get("procedure_summary")).get("procedures"))
    raw_form_dossiers = _as_list(_as_dict(raw.get("form_dossier")).get("dossiers"))
    raw_deps = _as_list(_as_dict(raw.get("dependency_inventory")).get("dependencies"))
    raw_rules = _as_list(_as_dict(raw.get("business_rule_catalog")).get("rules"))
    raw_risks = _as_list(_as_dict(raw.get("risk_register")).get("risks"))
    raw_orphans = _as_list(_as_dict(raw.get("orphan_analysis")).get("orphans"))
    raw_landscape = _as_list(_as_dict(raw.get("repo_landscape")).get("projects"))
    raw_variant_inventory = _as_list(_as_dict(raw.get("variant_inventory")).get("variants"))
    raw_constitution = _as_list(_as_dict(raw.get("delivery_constitution")).get("principles"))
    raw_variant_diff = _as_dict(raw.get("variant_diff_report"))

    def _base_form_name(value: Any) -> str:
        text = _clean(value)
        if not text:
            return ""
        if "::" in text:
            text = text.split("::")[-1]
        text = text.split(":")[0]
        text = text.rsplit("/", 1)[-1]
        if text.lower().endswith(".frm"):
            text = text[:-4]
        return text.lower()

    def _project_from_scoped(value: Any) -> str:
        text = _clean(value)
        if "::" in text:
            return _clean(text.split("::", 1)[0])
        return ""

    def _form_key(project_name: Any, form_name: Any) -> str:
        form = _base_form_name(form_name)
        project = _clean(project_name).lower()
        return f"{project}::{form}" if project and form else form

    def _qualified_form_name(project_name: Any, form_name: Any) -> str:
        project = _clean(project_name)
        form = _clean(form_name)
        if project and form:
            return f"{project}::{form}"
        return form or "n/a"

    def _project_label(value: Any, project_path_map: dict[str, str]) -> str:
        name = _clean(value)
        if not name:
            return "n/a"
        path = _clean(project_path_map.get(name))
        return f"{name} [{path}]" if path else name

    def _extract_forms_from_text(value: Any) -> list[str]:
        text = _clean(value)
        if not text:
            return []
        forms = re.findall(r"([A-Za-z0-9_./-]+)", text)
        out: list[str] = []
        for form in forms:
            low = form.lower()
            if low.endswith((".bas", ".cls", ".ctl", ".vbp", ".vbg", ".res", ".dsr", ".mdl", ".mod")):
                continue
            if ("frm" not in low and not low.startswith("form")):
                continue
            normalized = form.rsplit("/", 1)[-1]
            normalized = normalized[:-4] if normalized.lower().endswith(".frm") else normalized
            normalized = normalized.rstrip(".,;:()[]{}")
            nlow = normalized.lower()
            if re.search(r"_(click|change|load|keypress|gotfocus|lostfocus|activate|deactivate)$", nlow):
                continue
            if normalized not in out:
                out.append(normalized)
        return out

    def _infer_form_type(*, form_name: str, purpose: str, procedures: list[dict[str, Any]], controls: list[Any], tables: set[str]) -> str:
        form_low = form_name.lower()
        purpose_low = purpose.lower()
        control_text = " ".join(_clean(c).lower() for c in controls)
        proc_names = {_clean(_as_dict(p).get("procedure_name")).lower() for p in procedures}
        if "splash" in form_low or "splash" in purpose_low:
            return "Splash"
        if (
            form_low in {"main", "mdiform"}
            or form_low.startswith("mdi")
            or "toolbar" in control_text
            or any("toolbar" in x for x in proc_names)
        ):
            return "MDI_Host"
        if (
            "login" in form_low
            or "auth" in purpose_low
            or ("form9" in form_low and ({"logi", "login"} & {t.lower() for t in tables}))
        ):
            return "Login"
        if form_low.startswith(("rpt", "datareport")):
            return "Report"
        return "Child"

    def _base_only_key(form_name: Any) -> str:
        base = _base_form_name(form_name)
        return f"__base__::{base}" if base else ""

    def _form_keys(project_name: Any, form_name: Any) -> list[str]:
        keys: list[str] = []
        scoped = _form_key(project_name, form_name)
        if scoped:
            keys.append(scoped)
        base_key = _base_only_key(form_name)
        if base_key and base_key not in keys:
            keys.append(base_key)
        return keys

    def _lookup_rows(mapping: dict[str, list[dict[str, Any]]], project_name: Any, form_name: Any) -> list[dict[str, Any]]:
        scoped = _form_key(project_name, form_name)
        base_key = _base_only_key(form_name)
        if scoped and scoped in mapping and mapping[scoped]:
            return mapping[scoped]
        if base_key and base_key in mapping and mapping[base_key]:
            return mapping[base_key]
        return []

    def _lookup_set(mapping: dict[str, set[str]], project_name: Any, form_name: Any) -> set[str]:
        scoped = _form_key(project_name, form_name)
        base_key = _base_only_key(form_name)
        if scoped and scoped in mapping and mapping[scoped]:
            return mapping[scoped]
        if base_key and base_key in mapping and mapping[base_key]:
            return mapping[base_key]
        return set()

    def _lookup_control_map(mapping: dict[str, dict[str, str]], project_name: Any, form_name: Any) -> dict[str, str]:
        scoped = _form_key(project_name, form_name)
        base_key = _base_only_key(form_name)
        if scoped and scoped in mapping and mapping[scoped]:
            return mapping[scoped]
        if base_key and base_key in mapping and mapping[base_key]:
            return mapping[base_key]
        return {}

    project_path_by_name: dict[str, str] = {}
    project_dependencies_by_name: dict[str, set[str]] = {}
    project_tables_by_name: dict[str, set[str]] = {}
    for row in raw_landscape:
        r = _as_dict(row)
        raw_id = _clean(r.get("id"))
        path = _clean(r.get("path"))
        left = raw_id.split("|", 1)[0] if "|" in raw_id else raw_id
        names = [left, _clean(r.get("name")), raw_id]
        deps = set(_as_list(r.get("dependencies")))
        tables = set(_clean(t) for t in _as_list(r.get("db_touchpoints")) if _clean(t))
        for name in names:
            key = _clean(name)
            if not key:
                continue
            if key not in project_path_by_name and path:
                project_path_by_name[key] = path
            project_dependencies_by_name.setdefault(key, set()).update(dep for dep in deps if _clean(dep))
            project_tables_by_name.setdefault(key, set()).update(tables)

    form_sql_rows: dict[str, list[dict[str, Any]]] = {}
    form_db_tables: dict[str, set[str]] = {}
    sql_map_row_key: dict[int, str] = {}
    for row in raw_sql_map:
        r = _as_dict(row)
        tables = set(_clean(t) for t in _as_list(r.get("tables")) if _clean(t))
        project_name = _clean(r.get("variant")) or _project_from_scoped(r.get("form"))
        form_name = _clean(r.get("form_base")) or _clean(r.get("form"))
        keys = _form_keys(project_name, form_name)
        if not keys:
            continue
        sql_map_row_key[id(row)] = keys[0]
        for key in keys:
            form_sql_rows.setdefault(key, []).append(r)
            form_db_tables.setdefault(key, set()).update(tables)
        if project_name:
            project_tables_by_name.setdefault(project_name, set()).update(tables)

    form_event_rows: dict[str, list[dict[str, Any]]] = {}
    event_handler_by_key_proc: dict[tuple[str, str], set[str]] = {}
    form_shared_components: dict[str, set[str]] = {}
    for row in raw_event_map:
        r = _as_dict(row)
        container = _clean(r.get("container")) or _clean(r.get("name"))
        project_name = _project_from_scoped(container) or _project_from_scoped(_as_dict(r.get("handler")).get("symbol"))
        form_name = _base_form_name(container) or _base_form_name(_as_dict(r.get("handler")).get("symbol"))
        keys = _form_keys(project_name, form_name)
        if not keys:
            continue
        for key in keys:
            form_event_rows.setdefault(key, []).append(r)
        for call in [_clean(c) for c in _as_list(r.get("calls")) if _clean(c)]:
            for key in keys:
                event_handler_by_key_proc.setdefault((key, call), set()).add(_clean(_as_dict(r.get("handler")).get("symbol")) or _clean(r.get("entry_id")))

    form_proc_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_procedures:
        r = _as_dict(row)
        project_name = _project_from_scoped(r.get("form"))
        form_name = _base_form_name(r.get("form"))
        keys = _form_keys(project_name, form_name)
        for key in keys:
            form_proc_rows.setdefault(key, []).append(r)

    form_risk_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_risks:
        r = _as_dict(row)
        texts = [_clean(r.get("description")), _clean(r.get("recommended_action"))]
        for ev in _as_list(r.get("evidence")):
            e = _as_dict(ev)
            texts.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
            texts.append(_clean(_as_dict(e.get("file_span")).get("path")))
        forms: set[str] = set()
        for text in texts:
            for form_name in _extract_forms_from_text(text):
                forms.add(_base_form_name(form_name))
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            if _base_form_name(d.get("form_name")) not in forms:
                continue
            for key in _form_keys(d.get("project_name"), d.get("form_name")):
                form_risk_rows.setdefault(key, []).append(r)

    form_rule_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_rules:
        r = _as_dict(row)
        texts = [
            _clean(_as_dict(r.get("scope")).get("component_id")),
            _clean(r.get("statement")),
        ]
        for ev in _as_list(r.get("evidence")):
            e = _as_dict(ev)
            texts.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
            texts.append(_clean(_as_dict(e.get("file_span")).get("path")))
        forms: set[str] = set()
        for text in texts:
            for form_name in _extract_forms_from_text(text):
                forms.add(_base_form_name(form_name))
        for dossier in raw_form_dossiers:
            d = _as_dict(dossier)
            if _base_form_name(d.get("form_name")) not in forms:
                continue
            for key in _form_keys(d.get("project_name"), d.get("form_name")):
                form_rule_rows.setdefault(key, []).append(r)

    # control lookup for per-form/per-procedure ActiveX trigger checks
    form_control_type_by_key: dict[str, dict[str, str]] = {}
    for row in raw_form_dossiers:
        r = _as_dict(row)
        keys = _form_keys(r.get("project_name"), r.get("form_name"))
        if not keys:
            continue
        for key in keys:
            mapping = form_control_type_by_key.setdefault(key, {})
            for ctl in _as_list(r.get("controls")):
                ctl_text = _clean(ctl)
                if not ctl_text:
                    continue
                if ":" in ctl_text:
                    ctl_type, ctl_name = ctl_text.split(":", 1)
                else:
                    ctl_type, ctl_name = ctl_text, ctl_text
                mapping[_clean(ctl_name).lower()] = _clean(ctl_type)

    # map shared components by form
    shared_module_procs = {
        _clean(_as_dict(proc).get("procedure_name"))
        for proc in raw_procedures
        if _base_form_name(_as_dict(proc).get("form")) == "shared_module"
    }
    for key, events in form_event_rows.items():
        for e in events:
            ev = _as_dict(e)
            for call in [_clean(c) for c in _as_list(ev.get("calls")) if _clean(c)]:
                if call in shared_module_procs:
                    form_shared_components.setdefault(key, set()).add(call)

    # reports/shared tables summary
    table_to_projects: dict[str, set[str]] = {}
    for project_name, tables in project_tables_by_name.items():
        for t in tables:
            table_to_projects.setdefault(t, set()).add(project_name)

    dependency_to_forms: dict[str, set[str]] = {}
    for dossier in raw_form_dossiers:
        d = _as_dict(dossier)
        project_name = _clean(d.get("project_name"))
        qualified = _qualified_form_name(project_name, d.get("form_name"))
        deps = set(project_dependencies_by_name.get(project_name, set()))
        for ctl in _as_list(d.get("controls")):
            ctl_text = _clean(ctl)
            if not ctl_text:
                continue
            ctl_type = ctl_text.split(":", 1)[0]
            if ctl_type and not ctl_type.upper().startswith("VB"):
                deps.add(ctl_type)
        for dep in deps:
            dep_name = _clean(dep)
            if not dep_name:
                continue
            dependency_to_forms.setdefault(dep_name.lower(), set()).add(qualified)

    lines.append(f"- Legacy inventory: {'present' if raw.get('legacy_inventory') or hv.get('legacy_inventory') else 'missing'}")
    lines.append(f"- Event map rows: {len(raw_event_map) or len(_as_list(hv.get('event_map')))}")
    lines.append(f"- SQL catalog rows: {len(raw_sql) or len(_as_list(hv.get('sql_catalog')))}")
    lines.append(f"- SQL map rows: {len(raw_sql_map)}")
    lines.append(f"- Procedure summaries: {len(raw_procedures)}")
    lines.append(f"- Form dossiers: {len(raw_form_dossiers)}")
    lines.append(f"- Dependency rows: {len(raw_deps) or len(_as_list(hv.get('dependencies')))}")
    lines.append(f"- Business rules: {len(raw_rules) or len(_as_list(hv.get('business_rules')))}")
    lines.append(f"- Risk register rows: {len(raw_risks)}")
    lines.append(f"- Orphan analysis rows: {len(raw_orphans)}")
    lines.append(f"- Repo landscape variants: {len(raw_landscape)}")
    lines.append(f"- Variant inventory rows: {len(raw_variant_inventory)}")
    lines.append(f"- Constitution principles: {len(raw_constitution)}")

    include_detailed_appendix = mode != "summary"
    if not include_detailed_appendix:
        return "\n".join(lines)

    raw_legacy = _as_dict(raw.get("legacy_inventory") or hv.get("legacy_inventory"))
    projects = _as_list(raw_legacy.get("projects"))
    event_rows = raw_event_map or _as_list(hv.get("event_map"))
    sql_rows = raw_sql or _as_list(hv.get("sql_catalog"))
    dep_rows = raw_deps or _as_list(hv.get("dependencies"))
    rule_rows = raw_rules or _as_list(hv.get("business_rules"))
    detector_rows = _as_list(_as_dict(raw.get("detector_findings")).get("findings"))
    artifact_index_rows = _as_list(_as_dict(raw.get("artifact_index")).get("artifacts"))

    lines.extend(["", "## Detailed Appendix", "", "### A. Legacy Inventory"])
    lines.append(f"- Projects: {len(projects)}")
    lines.append(f"- Data touchpoints: {', '.join(_as_list(_as_dict(raw_legacy.get('summary')).get('data_touchpoints'))) or 'None detected'}")
    if projects:
        lines.append("| Project | Type | Startup | Members | Forms | Reports | Dependencies | Shared tables |")
        lines.append("|---|---|---|---:|---:|---:|---:|---|")
        for project in projects[:250]:
            p = _as_dict(project)
            project_name = _clean(p.get("name") or p.get("project_id"))
            members_rows = _as_list(p.get("members"))
            reports_count = sum(
                1
                for m in members_rows
                if (
                    _clean(_as_dict(m).get("kind")).lower() in {"report", "designer"}
                    or "report" in _clean(_as_dict(m).get("path")).lower()
                    or _clean(_as_dict(m).get("path")).lower().endswith(".dsr")
                )
            )
            shared_tables = sorted(
                t for t in project_tables_by_name.get(project_name, set())
                if len(table_to_projects.get(t, set())) > 1
            )
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(project_name),
                    _escape_pipe(p.get("type")),
                    _escape_pipe(p.get("startup")),
                    len(members_rows),
                    len(_as_list(p.get("ui_assets"))),
                    reports_count,
                    len(_as_list(p.get("dependencies"))),
                    _escape_pipe(", ".join(shared_tables[:8]) or "none"),
                )
            )
    else:
        lines.append("- No project rows available.")

    lines.extend(["", "### B. Dependency Inventory"])
    if dep_rows:
        lines.append("| Name | Kind | Risk | Recommended action | Forms mapped |")
        lines.append("|---|---|---|---|---|")
        for dep in dep_rows[:500]:
            d = _as_dict(dep)
            risk = _clean(_as_dict(d.get("risk")).get("tier") or d.get("tier") or "unknown")
            action = _clean(_as_dict(d.get("risk")).get("recommended_action") or d.get("recommended_action")) or "n/a"
            dep_name = _clean(d.get("name") or dep)
            mapped_forms = sorted(dependency_to_forms.get(dep_name.lower(), set()))
            lines.append(
                f"| {_escape_pipe(dep_name)} | {_escape_pipe(d.get('kind'))} | {_escape_pipe(risk)} | {_escape_pipe(action)} | {_escape_pipe(', '.join(mapped_forms[:6]) or 'n/a')} |"
            )
    else:
        lines.append("- No dependency rows available.")

    lines.extend(["", "### C. Event Map"])
    if event_rows:
        lines.append("| Entry | Container | Trigger | Calls | Side effects |")
        lines.append("|---|---|---|---|---|")
        for entry in event_rows[:600]:
            e = _as_dict(entry)
            calls = ", ".join(_as_list(e.get("calls") or e.get("procedure_calls"))[:4])
            side_effects = ", ".join(
                _as_list(_as_dict(e.get("side_effects")).get("tables_or_files") or e.get("sql_touches"))[:4]
            )
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(e.get("name") or e.get("entry_id") or e.get("event_handler")),
                    _escape_pipe(e.get("container") or e.get("form")),
                    _escape_pipe(_as_dict(e.get("trigger")).get("event") or e.get("event")),
                    _escape_pipe(calls or "n/a"),
                    _escape_pipe(side_effects or "n/a"),
                )
            )
    else:
        lines.append("- No event map rows available.")

    lines.extend(["", "### D. SQL Catalog"])
    if sql_rows:
        lines.append("| SQL ID | Kind | Tables | Query |")
        lines.append("|---|---|---|---|")
        for row in sql_rows[:700]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("sql_id") or "n/a"),
                    _escape_pipe(r.get("kind") or "unknown"),
                    _escape_pipe(", ".join(_as_list(r.get("tables"))[:6]) or "n/a"),
                    _escape_pipe(r.get("raw") or row),
                )
            )
    else:
        lines.append("- No SQL rows available.")

    lines.extend(["", "### E. Business Rules"])
    if rule_rows:
        lines.append("| Rule ID | Form | Layer | Category | Statement | Evidence |")
        lines.append("|---|---|---|---|---|---|")
        for row in rule_rows[:700]:
            r = _as_dict(row)
            evidence = ""
            if isinstance(r.get("evidence"), list):
                evidence = ", ".join(
                    [
                        _clean(_as_dict(e).get("external_ref", {}).get("ref") or _as_dict(e).get("file_span", {}).get("path") or _as_dict(e).get("ref"))
                        for e in _as_list(r.get("evidence"))
                    ][:3]
                )
            else:
                evidence = _clean(r.get("evidence"))
            rule_forms: list[str] = []
            for source in [
                _clean(r.get("statement")),
                evidence,
            ]:
                for form_name in _extract_forms_from_text(source):
                    if form_name not in rule_forms:
                        rule_forms.append(form_name)
            form_value = ", ".join(rule_forms[:4]) or "n/a"
            category = _clean(r.get("category") or r.get("rule_type") or "other")
            layer = "Presentation"
            evidence_low = f"{_clean(r.get('statement'))} {evidence}".lower()
            if category.lower() in {"data_persistence", "calculation_logic", "threshold_rule"} or any(x in evidence_low for x in ["select ", "insert ", "update ", "delete ", "table"]):
                layer = "Data"
            if any(x in evidence_low for x in [".bas", "module", "shared"]):
                layer = "Shared"
            lines.append(
                "| {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("rule_id") or r.get("id") or "n/a"),
                    _escape_pipe(form_value),
                    _escape_pipe(layer),
                    _escape_pipe(category),
                    _escape_pipe(r.get("statement") or "n/a"),
                    _escape_pipe(evidence or "n/a"),
                )
            )
    else:
        lines.append("- No business rules available.")

    lines.extend(["", "### F. Detector Findings"])
    if detector_rows:
        lines.append("| Detector | Severity | Count | Summary | Required actions |")
        lines.append("|---|---|---:|---|---|")
        for row in detector_rows[:500]:
            r = _as_dict(row)
            actions = ", ".join(_as_list(r.get("required_actions"))[:4]) or "n/a"
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("detector_id") or "n/a"),
                    _escape_pipe(r.get("severity") or "medium"),
                    int(r.get("count") or 0),
                    _escape_pipe(r.get("summary") or ""),
                    _escape_pipe(actions),
                )
            )
    else:
        lines.append("- No detector findings available.")

    lines.extend(["", "### G. Artifact Index"])
    if artifact_index_rows:
        lines.append("| Type | Ref |")
        lines.append("|---|---|")
        for row in artifact_index_rows[:200]:
            r = _as_dict(row)
            lines.append(f"| {_escape_pipe(r.get('type'))} | {_escape_pipe(r.get('ref'))} |")
    else:
        lines.append("- No artifact index entries.")

    lines.extend(["", "### H. SQL Map"])
    if raw_sql_map:
        lines.append("| Form | Procedure | Operation | Tables | Risks | activex_trigger | trace_complete |")
        lines.append("|---|---|---|---|---|---|---|")
        for row in raw_sql_map[:700]:
            r = _as_dict(row)
            project_name = _clean(r.get("variant")) or _project_from_scoped(r.get("form"))
            form_name = _clean(r.get("form_base")) or _clean(r.get("form"))
            related_events = [
                _as_dict(e)
                for e in _lookup_rows(form_event_rows, project_name, form_name)
                if _clean(r.get("procedure")) and _clean(r.get("procedure")) in _clean(_as_dict(e.get("handler")).get("symbol"))
            ]
            control_map = _lookup_control_map(form_control_type_by_key, project_name, form_name)
            activex_hits: list[str] = []
            for e in related_events:
                trigger_control = _clean(_as_dict(e.get("trigger")).get("control"))
                if not trigger_control:
                    continue
                ctl_type = _clean(control_map.get(trigger_control.lower()))
                if ctl_type and not ctl_type.upper().startswith("VB"):
                    activex_hits.append(f"{trigger_control}:{ctl_type}")
            has_sql = bool(_clean(r.get("sql_id")))
            has_tables = bool(_as_list(r.get("tables")))
            trace_complete = has_sql and has_tables
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(_qualified_form_name(project_name, form_name)),
                    _escape_pipe(r.get("procedure") or "n/a"),
                    _escape_pipe(r.get("operation") or "unknown"),
                    _escape_pipe(", ".join(_as_list(r.get("tables"))[:6]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("risk_flags"))[:6]) or "none"),
                    _escape_pipe(", ".join(sorted(set(activex_hits))[:4]) or "n/a"),
                    "yes" if trace_complete else "no",
                )
            )
    else:
        lines.append("- No SQL map rows available.")

    lines.extend(["", "### I. Procedure Summaries"])
    if raw_procedures:
        lines.append("| Procedure | Form | SQL IDs | Steps | Risks |")
        lines.append("|---|---|---|---|---|")
        for row in raw_procedures[:700]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("procedure_name") or r.get("procedure_id") or "n/a"),
                    _escape_pipe(r.get("form") or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("sql_ids"))[:6]) or "n/a"),
                    _escape_pipe(" / ".join(_as_list(r.get("steps"))[:2]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("risks"))[:5]) or "none"),
                )
            )
    else:
        lines.append("- No procedure summaries available.")

    lines.extend(["", "### J. Delivery Constitution"])
    if raw_constitution:
        lines.extend([f"- {_clean(x)}" for x in raw_constitution[:100]])
    else:
        lines.append("- No delivery constitution principles available.")

    lines.extend(["", "### K. Form Dossiers"])
    if raw_form_dossiers:
        lines.append("| Form | Project | form_type | Purpose | Inputs | Outputs | ActiveX used | DB tables | Actions | Coverage | Confidence |")
        lines.append("|---|---|---|---|---|---|---|---|---:|---:|---:|")
        for row in raw_form_dossiers[:700]:
            r = _as_dict(row)
            project_name = _clean(r.get("project_name"))
            form_name = _clean(r.get("form_name"))
            coverage = float(_as_dict(r.get("coverage")).get("coverage_score") or 0)
            confidence = float(_as_dict(r.get("coverage")).get("confidence_score") or 0)
            proc_rows = _lookup_rows(form_proc_rows, project_name, form_name)
            input_values: set[str] = set()
            output_values: set[str] = set()
            for proc in proc_rows:
                p = _as_dict(proc)
                output_values.update(_clean(x) for x in _as_list(p.get("tables_touched")) if _clean(x))
                output_values.update(_clean(x) for x in _as_list(p.get("data_mutations")) if _clean(x))
                output_values.update(_clean(x) for x in _as_list(p.get("navigation_side_effects")) if _clean(x))

            project_deps = project_dependencies_by_name.get(project_name, set())
            form_controls = _as_list(r.get("controls"))
            form_activex: set[str] = set()
            for ctl in form_controls:
                ctl_name = _clean(ctl)
                if not ctl_name:
                    continue
                control_id = _clean(ctl_name.split(":", 1)[-1]).lower()
                if control_id.startswith(("txt", "cbo", "cmb", "dtp", "msk", "chk", "opt", "lst")):
                    input_values.add(control_id)
                prefix = ctl_name.split(":", 1)[0]
                if prefix and not prefix.upper().startswith("VB"):
                    form_activex.add(prefix)
            for dep in project_deps:
                dep_name = _clean(dep)
                if dep_name.lower().endswith((".ocx", ".dll")) or "MSCOM" in dep_name.upper() or "MSFLEX" in dep_name.upper():
                    form_activex.add(dep_name)

            db_tables = sorted(_lookup_set(form_db_tables, project_name, form_name))
            form_type = _infer_form_type(
                form_name=form_name or "n/a",
                purpose=_clean(r.get("purpose")),
                procedures=proc_rows,
                controls=form_controls,
                tables=set(db_tables),
            )
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} | {} | {:.2f} | {:.2f} |".format(
                    _escape_pipe(form_name or "n/a"),
                    _escape_pipe(_project_label(project_name, project_path_by_name)),
                    _escape_pipe(form_type),
                    _escape_pipe(r.get("purpose") or ""),
                    _escape_pipe(", ".join(sorted(input_values)[:6]) or "n/a"),
                    _escape_pipe(", ".join(sorted(output_values)[:6]) or "n/a"),
                    _escape_pipe(", ".join(sorted(form_activex)[:6]) or "n/a"),
                    _escape_pipe(", ".join(db_tables[:8]) or "n/a"),
                    len(_as_list(r.get("actions"))),
                    coverage,
                    confidence,
                )
            )
    else:
        lines.append("- No form dossier rows available.")

    lines.extend(["", "### L. Risk Register"])
    if raw_risks:
        lines.append("| Risk ID | Severity | Description | Recommended action |")
        lines.append("|---|---|---|---|")
        for row in raw_risks[:700]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("risk_id") or "n/a"),
                    _escape_pipe(r.get("severity") or "medium"),
                    _escape_pipe(r.get("description") or ""),
                    _escape_pipe(r.get("recommended_action") or ""),
                )
            )
    else:
        lines.append("- No risk register rows available.")

    lines.extend(["", "### M. Orphan Analysis"])
    if raw_orphans:
        lines.append("| Path | SQL IDs | Tables touched | Recommendation |")
        lines.append("|---|---|---|---|")
        for row in raw_orphans[:500]:
            r = _as_dict(row)
            lines.append(
                "| {} | {} | {} | {} |".format(
                    _escape_pipe(r.get("path") or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("sql_ids"))[:6]) or "n/a"),
                    _escape_pipe(", ".join(_as_list(r.get("tables_touched"))[:6]) or "n/a"),
                    _escape_pipe(r.get("recommendation") or "verify"),
                )
            )
    else:
        lines.append("- No orphan analysis rows available.")

    lines.extend(["", "### N. Repository Landscape and Variant Inventory"])
    if raw_landscape:
        lines.append("| Variant | Path | Startup | Forms | Members | Dependencies |")
        lines.append("|---|---|---|---:|---:|---:|")
        for row in raw_landscape[:200]:
            r = _as_dict(row)
            counts = _as_dict(r.get("counts"))
            lines.append(
                "| {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(((_clean(r.get("id")).split("|", 1)[0] if "|" in _clean(r.get("id")) else _clean(r.get("id"))) or "variant")),
                    _escape_pipe(r.get("path") or ""),
                    _escape_pipe(r.get("startup") or ""),
                    int(counts.get("forms") or 0),
                    int(counts.get("members") or 0),
                    int(counts.get("dependencies") or 0),
                )
            )
    else:
        lines.append("- No repository landscape rows available.")

    if raw_variant_inventory:
        lines.extend(["", "| Variant | Forms | Modules | Tables touched | Dependency summary |", "|---|---:|---:|---:|---|"])
        for row in raw_variant_inventory[:200]:
            r = _as_dict(row)
            dep_summary = _as_dict(r.get("dependencies_summary"))
            lines.append(
                "| {} | {} | {} | {} | total={}, ocx={}, dll={} |".format(
                    _escape_pipe(r.get("name") or r.get("id") or "variant"),
                    len(_as_list(r.get("forms"))),
                    len(_as_list(r.get("modules"))),
                    len(_as_list(r.get("tables_touched"))),
                    int(dep_summary.get("total") or 0),
                    int(dep_summary.get("ocx") or 0),
                    int(dep_summary.get("dll") or 0),
                )
            )

    lines.extend(["", "### O. Project Dependency Map"])
    dependency_rows: list[dict[str, str]] = []
    seen_dependency_keys: set[tuple[str, str, str, str]] = set()

    for entry in raw_event_map:
        e = _as_dict(entry)
        source = _clean(e.get("container") or e.get("form") or e.get("name")) or "n/a"
        source_trigger = _clean(_as_dict(e.get("trigger")).get("control"))
        calls = [_clean(c) for c in _as_list(e.get("calls")) if _clean(c)]
        for call in calls:
            dep_type = ""
            if call in shared_module_procs:
                dep_type = "shared_module_call"
            elif (
                "main" in source.lower()
                or "toolbar" in source.lower()
                or "toolbar" in source_trigger.lower()
            ) and call.lower().startswith(("frm", "form", "rpt", "datareport")):
                dep_type = "mdi_navigation"
            if not dep_type:
                continue
            stable_evidence = _clean(_as_dict(e.get("handler")).get("symbol")) or f"{source}->{call}"
            key = (source, call, dep_type, stable_evidence)
            if key in seen_dependency_keys:
                continue
            seen_dependency_keys.add(key)
            blocks_sprint = "Sprint 1"
            if dep_type == "cross_variant_schema_conflict":
                blocks_sprint = "Sprint 0"
            dependency_rows.append(
                {
                    "from": source,
                    "to": call,
                    "type": dep_type,
                    "evidence": stable_evidence,
                    "blocks_sprint": blocks_sprint,
                }
            )

    schema_divergence = _as_dict(raw_variant_diff.get("schema_divergence"))
    for pair in _as_list(schema_divergence.get("blocking_pairs") or schema_divergence.get("pairs")):
        p = _as_dict(pair)
        left = _clean(p.get("left_project"))
        right = _clean(p.get("right_project"))
        if not left or not right:
            continue
        alias_count = len(_as_list(p.get("alias_mismatches")))
        near_miss_count = len(_as_list(p.get("near_miss_names")))
        txn_conflict = bool(p.get("transaction_schema_conflict"))
        evidence = f"alias_mismatches={alias_count}, near_miss={near_miss_count}, transaction_conflict={'yes' if txn_conflict else 'no'}"
        key = (left, right, "cross_variant_schema_conflict", evidence)
        if key in seen_dependency_keys:
            continue
        seen_dependency_keys.add(key)
        dependency_rows.append(
            {
                "from": left,
                "to": right,
                "type": "cross_variant_schema_conflict",
                "evidence": evidence,
                "blocks_sprint": "Sprint 0",
            }
        )

    if dependency_rows:
        lines.append("| From | To | Type | Evidence | Blocks Sprint |")
        lines.append("|---|---|---|---|---|")
        for row in dependency_rows[:800]:
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(row.get("from")),
                    _escape_pipe(row.get("to")),
                    _escape_pipe(row.get("type")),
                    _escape_pipe(row.get("evidence")),
                    _escape_pipe(row.get("blocks_sprint") or "Sprint 1"),
                )
            )
    else:
        lines.append("- No project dependency rows available.")

    lines.extend(["", "### P. Form Flow Traces"])
    if raw_form_dossiers:
        for row in raw_form_dossiers[:250]:
            r = _as_dict(row)
            form_name = _clean(r.get("form_name")) or "n/a"
            project_name = _clean(r.get("project_name"))
            form_controls = _as_list(r.get("controls"))
            control_map = _lookup_control_map(form_control_type_by_key, project_name, form_name)

            events = _lookup_rows(form_event_rows, project_name, form_name)
            procedures = _lookup_rows(form_proc_rows, project_name, form_name)
            sql_entries = _lookup_rows(form_sql_rows, project_name, form_name)
            procedure_names: set[str] = set()
            for proc in procedures:
                name = _clean(_as_dict(proc).get("procedure_name"))
                if name:
                    procedure_names.add(name)
            for sql_entry in sql_entries:
                name = _clean(_as_dict(sql_entry).get("procedure"))
                if name:
                    procedure_names.add(name)

            lines.extend(
                [
                    f"#### {form_name} ({_project_label(project_name, project_path_by_name)})",
                    "| Procedure | Event | ActiveX | SQL IDs | Tables | Trace status |",
                    "|---|---|---|---|---|---|",
                ]
            )

            if not procedure_names:
                lines.append(
                    "| n/a | n/a | {} | n/a | {} | TRACE_GAP |".format(
                        "n/a",
                        _escape_pipe(", ".join(sorted(_lookup_set(form_db_tables, project_name, form_name))[:8]) or "n/a"),
                    )
                )
                continue

            for proc_name in sorted(procedure_names)[:120]:
                related_events = [
                    _as_dict(e)
                    for e in events
                    if proc_name in _clean(_as_dict(e).get("handler", {}).get("symbol"))
                ]
                related_sql = [
                    _as_dict(s)
                    for s in sql_entries
                    if _clean(_as_dict(s).get("procedure")) == proc_name
                ]
                activex_hits: list[str] = []
                for e in related_events:
                    trigger_control = _clean(_as_dict(e.get("trigger")).get("control"))
                    ctl_type = _clean(control_map.get(trigger_control.lower()))
                    if trigger_control and ctl_type and not ctl_type.upper().startswith("VB"):
                        activex_hits.append(f"{trigger_control}:{ctl_type}")
                sql_ids = sorted({_clean(x.get("sql_id")) for x in related_sql if _clean(x.get("sql_id"))})
                table_names: set[str] = set()
                for sql_row in related_sql:
                    table_names.update(_clean(t) for t in _as_list(sql_row.get("tables")) if _clean(t))
                trace_ok = bool(sql_ids) and bool(table_names)
                lines.append(
                    "| {} | {} | {} | {} | {} | {} |".format(
                        _escape_pipe(proc_name),
                        _escape_pipe(", ".join(_clean(_as_dict(e.get("handler")).get("symbol")) for e in related_events[:3]) or "n/a"),
                        _escape_pipe(", ".join(sorted(set(activex_hits))[:5]) or "n/a"),
                        _escape_pipe(", ".join(sql_ids[:6]) or "n/a"),
                        _escape_pipe(", ".join(sorted(table_names)[:8]) or "n/a"),
                        "OK" if trace_ok else "TRACE_GAP",
                    )
                )
    else:
        lines.append("- No form dossiers available for flow traces.")

    lines.extend(["", "### Q. Form Traceability Matrix"])
    traceability_rows: list[dict[str, Any]] = []
    if raw_form_dossiers:
        lines.append("| Form | Project | has_event_map | has_sql_map | has_business_rules | has_risk_entry | completeness_score | missing_links |")
        lines.append("|---|---|---|---|---|---|---:|---|")
        seen_form_keys: set[str] = set()
        for row in raw_form_dossiers[:400]:
            r = _as_dict(row)
            form_name = _clean(r.get("form_name")) or "n/a"
            project_name = _clean(r.get("project_name"))
            form_key = _form_key(project_name, form_name)
            if form_key in seen_form_keys:
                continue
            seen_form_keys.add(form_key)
            has_event = bool(_lookup_rows(form_event_rows, project_name, form_name))
            has_sql = bool(_lookup_rows(form_sql_rows, project_name, form_name))
            has_rules = bool(_lookup_rows(form_rule_rows, project_name, form_name))
            has_risk = bool(_lookup_rows(form_risk_rows, project_name, form_name))
            has_proc = bool(_lookup_rows(form_proc_rows, project_name, form_name))
            missing: list[str] = []
            if not has_event:
                missing.append("event_map")
            if not has_sql:
                missing.append("sql_map")
            if not has_rules:
                missing.append("business_rules")
            if not has_risk:
                missing.append("risk_register")
            if not has_proc:
                missing.append("procedure_summary")
            completeness_score = int((int(has_event) + int(has_sql) + int(has_rules) + int(has_risk) + int(has_proc)) * 20)
            lines.append(
                "| {} | {} | {} | {} | {} | {} | {} | {} |".format(
                    _escape_pipe(_qualified_form_name(project_name, form_name)),
                    _escape_pipe(_project_label(project_name, project_path_by_name)),
                    "yes" if has_event else "no",
                    "yes" if has_sql else "no",
                    "yes" if has_rules else "no",
                    "yes" if has_risk else "no",
                    completeness_score,
                    _escape_pipe(", ".join(missing) or "none"),
                )
            )
            traceability_rows.append(
                {
                    "form_name": form_name,
                    "project_name": project_name,
                    "has_event": has_event,
                    "has_sql": has_sql,
                    "has_rules": has_rules,
                    "has_risk": has_risk,
                    "form_key": form_key,
                    "qualified_form": _qualified_form_name(project_name, form_name),
                    "completeness_score": completeness_score,
                    "missing": missing,
                    "risk_ids": [
                        _clean(_as_dict(rr).get("risk_id"))
                        for rr in _lookup_rows(form_risk_rows, project_name, form_name)[:4]
                        if _clean(_as_dict(rr).get("risk_id"))
                    ],
                }
            )
    else:
        lines.append("- No traceability rows available.")

    lines.extend(["", "### R. Sprint Dependency Map"])
    if traceability_rows:
        lines.append("| Form | Suggested sprint | Depends on | Shared Components Required | Rationale |")
        lines.append("|---|---|---|---|---|")
        variant_gate_needed = bool(raw_variant_diff.get("decision_required"))
        sorted_rows = sorted(
            traceability_rows,
            key=lambda row: (
                len(_as_list(row.get("missing"))),
                0 if _as_list(row.get("risk_ids")) else 1,
                _clean(row.get("qualified_form")),
            ),
            reverse=True,
        )
        emitted: set[str] = set()
        for row in sorted_rows[:400]:
            form_key = _clean(row.get("form_key"))
            if not form_key or form_key in emitted:
                continue
            emitted.add(form_key)
            missing = _as_list(row.get("missing"))
            risk_ids = _as_list(row.get("risk_ids"))
            depends_on: list[str] = []
            if variant_gate_needed:
                depends_on.append("DEC-VARIANT-001")
            if "sql_map" in missing:
                depends_on.append("Q.sql_map")
            if "event_map" in missing:
                depends_on.append("Q.event_map")
            if "business_rules" in missing:
                depends_on.append("Q.business_rules")
            if risk_ids:
                depends_on.extend(risk_ids[:2])

            if "event_map" in missing or "sql_map" in missing:
                sprint = "Sprint 0 (Discovery closure)"
                rationale = "Close traceability gaps before modernization changes."
            elif risk_ids:
                sprint = "Sprint 1 (Risk-first modernization)"
                rationale = "Implement remediation-first changes for high-risk legacy behavior."
            else:
                sprint = "Sprint 2 (Parity hardening)"
                rationale = "Form has baseline traceability and can move into parity build/test."

            shared_required = sorted(_lookup_set(form_shared_components, row.get("project_name"), row.get("form_name")))
            lines.append(
                "| {} | {} | {} | {} | {} |".format(
                    _escape_pipe(_clean(row.get("qualified_form")) or _clean(row.get("form_name"))),
                    _escape_pipe(sprint),
                    _escape_pipe(", ".join(depends_on) or "none"),
                    _escape_pipe(", ".join(shared_required[:5]) or "none"),
                    _escape_pipe(rationale),
                )
            )
    else:
        lines.append("- No sprint dependency rows available.")

    return "\n".join(lines)


def _latest_stage_output(run: dict[str, Any], stage: int = 1) -> dict[str, Any]:
    pipeline_state = _as_dict(run.get("pipeline_state"))
    results = _as_list(pipeline_state.get("agent_results"))
    for row in reversed(results):
        row_d = _as_dict(row)
        if int(row_d.get("stage") or 0) == stage and isinstance(row_d.get("output"), dict):
            return _as_dict(row_d.get("output"))
    fallback = pipeline_state.get("analyst_output")
    return _as_dict(fallback)


def _has_stage_result(run: dict[str, Any], stage: int = 1) -> bool:
    pipeline_state = _as_dict(run.get("pipeline_state"))
    results = _as_list(pipeline_state.get("agent_results"))
    return any(int(_as_dict(r).get("stage") or 0) == stage for r in results)


def _resolve_api_key(provider: str, explicit: str) -> tuple[str, str]:
    if explicit.strip():
        return explicit.strip(), ""
    from utils.settings_store import SettingsStore

    settings = SettingsStore(str(ROOT / "team_data"))
    cfg = settings.resolve_llm_credentials(provider, requested_model="")
    if str(cfg.get("api_key", "")).strip():
        return str(cfg.get("api_key", "")).strip(), "settings"
    env_var = "OPENAI_API_KEY" if provider == "openai" else "ANTHROPIC_API_KEY"
    env_val = os.getenv(env_var, "").strip()
    if env_val:
        return env_val, env_var
    return "", ""


def _build_pipeline_config(provider: str, model: str, api_key: str):
    from config import LLMProvider, PipelineConfig

    if provider == "openai":
        return PipelineConfig(
            provider=LLMProvider.OPENAI,
            openai_api_key=api_key,
            openai_model=model or "gpt-4o",
            anthropic_api_key="",
            live_deploy=False,
            max_retries=2,
            developer_parallel_agents=3,
            temperature=0.2,
        )
    return PipelineConfig(
        provider=LLMProvider.ANTHROPIC,
        anthropic_api_key=api_key,
        anthropic_model=model or "claude-sonnet-4-20250514",
        openai_api_key="",
        live_deploy=False,
        max_retries=2,
        developer_parallel_agents=3,
        temperature=0.2,
    )


def _repo_url_from_settings() -> str:
    settings_path = ROOT / "team_data" / "settings_state.json"
    if not settings_path.exists():
        return ""
    try:
        data = json.loads(settings_path.read_text(encoding="utf-8"))
    except Exception:
        return ""
    github = _as_dict(_as_dict(data.get("integrations")).get("github"))
    owner = _clean(github.get("owner"))
    repo = _clean(github.get("repository"))
    if owner and repo:
        return f"https://github.com/{owner}/{repo}"
    return ""


def _repo_url_from_latest_pipeline_run() -> str:
    runs_root = ROOT / "pipeline_runs"
    if not runs_root.exists():
        return ""
    candidates = sorted(
        [p for p in runs_root.iterdir() if p.is_dir()],
        key=lambda p: p.stat().st_mtime,
        reverse=True,
    )
    for run_dir in candidates[:120]:
        state_path = run_dir / "state.json"
        if not state_path.exists():
            continue
        try:
            state = json.loads(state_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        pipeline_state = _as_dict(state.get("pipeline_state"))
        integration = _as_dict(pipeline_state.get("integration_context"))
        if _as_dict(integration.get("local_bundle_inventory")):
            # Skip local CLI-direct runs; prefer web/api brownfield runs.
            continue
        brownfield = _as_dict(integration.get("brownfield"))
        repo_url = _clean(
            brownfield.get("repo_url")
            or pipeline_state.get("repo_url")
            or _as_dict(pipeline_state.get("context_reference")).get("repo_url")
        )
        if repo_url:
            return repo_url
    return ""


def _resolve_repo_url(repo_arg: str) -> tuple[str, str]:
    explicit = _clean(repo_arg)
    if explicit and explicit.lower() not in {"auto", "ui", "web", "latest"}:
        return explicit, "cli"
    from_runs = _repo_url_from_latest_pipeline_run()
    if from_runs:
        return from_runs, "latest_pipeline_run"
    from_settings = _repo_url_from_settings()
    if from_settings:
        return from_settings, "settings"
    return DEFAULT_REPO_URL, "default"


def _clone_repo(repo_url: str) -> Path:
    temp_root = Path(tempfile.mkdtemp(prefix="synthetix-vb6-"))
    repo_dir = temp_root / "repo"
    try:
        subprocess.run(
            ["git", "clone", "--depth", "1", repo_url, str(repo_dir)],
            check=True,
            capture_output=True,
            text=True,
            timeout=240,
        )
    except subprocess.TimeoutExpired as exc:
        shutil.rmtree(temp_root, ignore_errors=True)
        raise RuntimeError(f"git clone timed out for {repo_url}") from exc
    except subprocess.CalledProcessError as exc:
        shutil.rmtree(temp_root, ignore_errors=True)
        stderr = (exc.stderr or "").strip()
        raise RuntimeError(f"git clone failed for {repo_url}: {stderr}") from exc
    return repo_dir


def _build_legacy_bundle_from_repo(repo_dir: Path, max_text_chars: int = 40000) -> tuple[str, dict[str, Any]]:
    sections: list[str] = []
    file_rows: list[dict[str, Any]] = []

    files = sorted(
        [p for p in repo_dir.rglob("*") if p.is_file() and p.suffix.lower() in VB6_EXTENSIONS],
        key=lambda p: p.as_posix().lower(),
    )
    if not files:
        raise RuntimeError(f"No VB6 files found in repository: {repo_dir}")

    for path in files:
        rel = path.relative_to(repo_dir).as_posix()
        ext = path.suffix.lower()
        size = path.stat().st_size
        row = {"path": rel, "extension": ext, "size_bytes": size, "is_text": ext in VB6_TEXT_EXTENSIONS}
        file_rows.append(row)

        header = f"===== FILE: {rel} ====="
        if ext in VB6_TEXT_EXTENSIONS:
            text = path.read_text(encoding="utf-8", errors="replace")
            if len(text) > max_text_chars:
                text = text[:max_text_chars] + "\n... [truncated]"
            sections.append(f"{header}\n{text}")
        else:
            sections.append(f"{header}\n[binary file: {size} bytes]")

    counts_by_ext: dict[str, int] = {}
    for row in file_rows:
        ext = str(row["extension"])
        counts_by_ext[ext] = counts_by_ext.get(ext, 0) + 1

    summary = {
        "total_files": len(file_rows),
        "counts_by_extension": counts_by_ext,
        "text_files": sum(1 for r in file_rows if r["is_text"]),
        "binary_files": sum(1 for r in file_rows if not r["is_text"]),
    }
    bundle = "\n\n".join(sections)
    return bundle, {"summary": summary, "files": file_rows}


def _discover_prefetch(base_url: str, objectives: str, repo_url: str) -> dict[str, Any]:
    integration = {
        "project_state_mode": "brownfield",
        "project_state_detected": "brownfield",
        "brownfield": {
            "repo_provider": "github",
            "repo_url": repo_url,
            "issue_provider": "",
            "issue_project": "",
            "docs_url": "",
            "runtime_telemetry": False,
        },
        "scan_scope": {
            "analysis_depth": "deep",
            "telemetry_mode": "off",
            "modernization_source_mode": "repo_scan",
            "include_paths": [],
            "exclude_paths": [],
        },
        "sample_dataset_enabled": False,
    }

    payload = {
        "use_case": "code_modernization",
        "objectives": objectives,
        "repo_provider": "github",
        "repo_url": repo_url,
        "integration_context": integration,
    }
    data = _http_json("POST", f"{base_url}/api/discover/analyst-brief", payload=payload, timeout=300)
    if not bool(data.get("ok")):
        raise RuntimeError(f"Discover analyst brief failed: {_clean(data.get('error')) or data}")

    summary = _as_dict(_as_dict(data.get("analyst_brief")).get("summary"))
    aas = _as_dict(data.get("aas"))
    requirements_pack = _as_dict(data.get("requirements_pack"))
    discover_cache = {
        "analyst_source": _clean(data.get("source")),
        "analyst_repo": _as_dict(data.get("repo")),
        "analyst_thread_id": _clean(data.get("thread_id") or aas.get("thread_id")),
        "analyst_aas_summary": _clean(data.get("assistant_summary") or aas.get("assistant_summary")),
        "analyst_summary": {
            "overview": _clean(summary.get("overview")),
            "likely_capabilities": _as_list(summary.get("likely_capabilities"))[:12],
            "key_components": _as_list(summary.get("key_components"))[:20],
            "evidence_files": _as_list(summary.get("evidence_files"))[:80],
            "input_output_contracts": _as_list(summary.get("input_output_contracts"))[:24],
            "domain_functions": _as_list(summary.get("domain_functions"))[:80],
            "data_entities": _as_list(summary.get("data_entities"))[:80],
            "legacy_skill_profile": _as_dict(summary.get("legacy_skill_profile")),
            "vb6_analysis": _as_dict(summary.get("vb6_analysis")),
        },
        "analyst_requirements_pack": requirements_pack or None,
    }

    integration["discover_cache"] = discover_cache
    return integration


def _write_outputs(
    *,
    analyst_output: dict[str, Any],
    mode: str,
    output_dir: str,
    run_id_hint: str,
) -> tuple[Path, Path]:
    markdown = build_full_markdown(analyst_output, mode=mode)
    out_dir = Path(output_dir).expanduser().resolve()
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
    safe_run_id = _clean(run_id_hint) or f"local-{stamp}"
    md_path = out_dir / f"analyst-tech-req-{mode}-{safe_run_id}-{stamp}.md"
    json_path = out_dir / f"analyst-output-{safe_run_id}-{stamp}.json"

    md_path.write_text(markdown, encoding="utf-8")
    json_path.write_text(json.dumps(analyst_output, indent=2, ensure_ascii=True, default=str), encoding="utf-8")
    return md_path, json_path


def run_and_export_api(args: argparse.Namespace) -> int:
    base_url = args.base_url.rstrip("/")
    objectives = args.objectives.strip() or DEFAULT_OBJECTIVE

    # health check
    health = _http_json("GET", f"{base_url}/api/health", timeout=20)
    if not bool(health.get("ok", True)):
        raise RuntimeError(f"Health check failed: {health}")

    if not args.skip_discover_prefetch:
        try:
            integration_context = _discover_prefetch(base_url, objectives, args.repo_url)
            print("[info] discover prefetch completed")
        except Exception as exc:
            print(f"[warn] discover prefetch failed, continuing with direct repo-scan start: {exc}")
            integration_context = {
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
                    "analysis_depth": "deep",
                    "telemetry_mode": "off",
                    "modernization_source_mode": "repo_scan",
                    "include_paths": [],
                    "exclude_paths": [],
                },
                "sample_dataset_enabled": False,
            }
    else:
        print("[info] discover prefetch skipped by flag")
        integration_context = {
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
                "analysis_depth": "deep",
                "telemetry_mode": "off",
                "modernization_source_mode": "repo_scan",
                "include_paths": [],
                "exclude_paths": [],
            },
            "sample_dataset_enabled": False,
        }

    run_payload = {
        "use_case": "code_modernization",
        "objectives": objectives,
        "legacy_code": "",
        "modernization_language": args.target_language,
        "deployment_target": "local",
        "human_approval": False,
        "strict_security_mode": False,
        "provider": args.provider,
        "model": args.model,
        "integration_context": integration_context,
    }

    run_start = _http_json("POST", f"{base_url}/api/runs", payload=run_payload, timeout=120)
    if not bool(run_start.get("ok")):
        raise RuntimeError(f"Run start failed: {_clean(run_start.get('error')) or run_start}")

    run_id = _clean(run_start.get("run_id"))
    if not run_id:
        raise RuntimeError(f"Run start response missing run_id: {run_start}")

    print(f"[info] run started: {run_id}")
    deadline = time.time() + max(30, int(args.timeout_seconds))
    final_run: dict[str, Any] | None = None

    while time.time() < deadline:
        row = _http_json("GET", f"{base_url}/api/runs/{run_id}", timeout=30)
        if not bool(row.get("ok")):
            raise RuntimeError(f"Failed to read run status: {row}")
        run = _as_dict(row.get("run"))
        status = _clean(run.get("status")).lower()
        has_stage1 = _has_stage_result(run, stage=1)

        current_stage = int(run.get("current_stage") or 0)
        print(f"[info] status={status or 'unknown'} stage={current_stage} stage1_ready={has_stage1}")

        if has_stage1:
            final_run = run
            if args.stop_after_analyst:
                try:
                    _http_json(
                        "POST",
                        f"{base_url}/api/runs/{run_id}/abort",
                        payload={"reason": "Analyst stage exported via CLI script"},
                        timeout=30,
                    )
                    print("[info] run aborted after analyst stage export")
                except Exception as exc:
                    print(f"[warn] could not abort run after stage 1: {exc}")
            break

        if status in {"completed", "failed", "aborted"}:
            final_run = run
            break

        time.sleep(max(1, int(args.poll_seconds)))

    if final_run is None:
        raise RuntimeError(f"Timed out waiting for analyst stage for run {run_id}")

    analyst_output = _latest_stage_output(final_run, stage=1)
    if not analyst_output:
        raise RuntimeError(
            f"Run {run_id} did not produce analyst output. Final status={_clean(final_run.get('status'))}."
        )

    md_path, json_path = _write_outputs(
        analyst_output=analyst_output,
        mode=args.mode,
        output_dir=args.output_dir,
        run_id_hint=run_id,
    )

    print(f"[ok] markdown: {md_path}")
    print(f"[ok] analyst output json: {json_path}")
    return 0


def run_and_export_direct(args: argparse.Namespace) -> int:
    from orchestrator.pipeline import make_initial_state, run_single_stage

    objectives = args.objectives.strip() or DEFAULT_OBJECTIVE
    print("[warn] direct mode may diverge from web UI synthesis path. Use --execution-mode api for strict parity.")
    api_key, source = _resolve_api_key(args.provider, args.api_key)
    if not api_key:
        raise RuntimeError(
            f"No API key configured for provider `{args.provider}`. "
            f"Set it in Settings > LLM, pass --api-key, or set environment variable."
        )
    print(f"[info] using {args.provider} credentials from {source or 'cli flag'}")

    repo_dir = _clone_repo(args.repo_url)
    temp_root = repo_dir.parent
    try:
        legacy_code, inventory = _build_legacy_bundle_from_repo(repo_dir, max_text_chars=max(5000, int(args.max_text_chars)))
        print(
            "[info] legacy bundle prepared: "
            f"{inventory.get('summary', {}).get('total_files', 0)} files "
            f"({inventory.get('summary', {}).get('text_files', 0)} text / "
            f"{inventory.get('summary', {}).get('binary_files', 0)} binary)"
        )

        cfg = _build_pipeline_config(args.provider, args.model, api_key)
        state = make_initial_state(objectives)
        state["run_id"] = f"cli_{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        state["use_case"] = "code_modernization"
        state["legacy_code"] = legacy_code
        state["modernization_language"] = args.target_language
        state["deployment_target"] = "local"
        state["integration_context"] = {
            "project_state_mode": "brownfield",
            "project_state_detected": "brownfield",
            "brownfield": {"repo_provider": "github", "repo_url": args.repo_url},
            "scan_scope": {"modernization_source_mode": "repo_scan", "analysis_depth": "deep"},
            "local_bundle_inventory": inventory.get("summary", {}),
            "sample_dataset_enabled": False,
        }

        result_state = run_single_stage(cfg, state, stage_index=0)
        analyst_output = _as_dict(result_state.get("analyst_output"))
        if not analyst_output:
            stage_results = _as_list(result_state.get("agent_results"))
            msg = "unknown failure"
            for row in reversed(stage_results):
                r = _as_dict(row)
                if int(r.get("stage") or 0) == 1:
                    msg = _clean(r.get("summary")) or msg
                    break
            raise RuntimeError(f"Analyst stage did not produce output: {msg}")

        md_path, json_path = _write_outputs(
            analyst_output=analyst_output,
            mode=args.mode,
            output_dir=args.output_dir,
            run_id_hint=_clean(result_state.get("run_id")) or "local",
        )
        print(f"[ok] markdown: {md_path}")
        print(f"[ok] analyst output json: {json_path}")
        return 0
    finally:
        shutil.rmtree(temp_root, ignore_errors=True)


def run_and_export(args: argparse.Namespace) -> int:
    resolved_repo, source = _resolve_repo_url(args.repo_url)
    args.repo_url = resolved_repo
    print(f"[info] repo source={source} url={resolved_repo}")
    mode = _clean(args.execution_mode).lower() or "direct"
    if mode == "api":
        return run_and_export_api(args)
    return run_and_export_direct(args)


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Run Synthetix Analyst stage against a VB6 GitHub repo and export detailed markdown locally."
    )
    parser.add_argument(
        "--execution-mode",
        default="api",
        choices=["direct", "api"],
        help="api (default): call running Synthetix server endpoints; direct: run analyst stage locally without API server",
    )
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL, help="Synthetix API base URL")
    parser.add_argument(
        "--repo-url",
        default="auto",
        help="Legacy VB6 GitHub repository URL. Use 'auto' to reuse the latest web/API run repo (default).",
    )
    parser.add_argument("--objectives", default=DEFAULT_OBJECTIVE, help="Business objective text for the run")
    parser.add_argument("--provider", default="openai", choices=["openai", "anthropic"], help="LLM provider")
    parser.add_argument("--model", default="gpt-4o", help="LLM model")
    parser.add_argument("--api-key", default="", help="Optional LLM API key override for direct mode")
    parser.add_argument("--target-language", default="C#", help="Target modernization language")
    parser.add_argument("--mode", default="full", choices=["summary", "full"], help="Markdown detail level")
    parser.add_argument("--output-dir", default=str(ROOT / "run_artifacts" / "manual_exports"), help="Output directory")
    parser.add_argument("--max-text-chars", type=int, default=40000, help="Per-file text cap when building local legacy bundle")
    parser.add_argument("--timeout-seconds", type=int, default=1200, help="Max wait time for stage output")
    parser.add_argument("--poll-seconds", type=int, default=5, help="Polling interval")
    parser.add_argument(
        "--skip-discover-prefetch",
        action="store_true",
        help="API mode only: skip /api/discover/analyst-brief prefetch and start run directly.",
    )
    parser.add_argument(
        "--stop-after-analyst",
        action="store_true",
        default=True,
        help="Abort run immediately after stage 1 output is available (default true)",
    )
    parser.add_argument(
        "--continue-pipeline",
        action="store_true",
        help="Do not abort after analyst stage (overrides --stop-after-analyst).",
    )

    args = parser.parse_args(argv)
    if args.continue_pipeline:
        args.stop_after_analyst = False
    return args


def main(argv: list[str]) -> int:
    args = parse_args(argv)
    try:
        return run_and_export(args)
    except Exception as exc:
        print(f"[error] {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
