"""
Build a structured Analyst report artifact (v2) from Stage 1 output.

This keeps the output artifact-first and UI-renderable with three layers:
- Decision Brief
- Delivery Spec
- Evidence Appendix
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from typing import Any


GENERIC_BDD_MARKERS = (
    "given requirement",
    "when requirement",
    "then requirement",
)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _normalize_open_question(entry: Any, index: int) -> dict[str, Any]:
    if isinstance(entry, str):
        text = _clean(entry) or "Clarification required"
        return {
            "id": f"Q-{index + 1:03d}",
            "question": text,
            "owner": "Unassigned",
            "severity": "medium",
            "context": "",
        }
    if not isinstance(entry, dict):
        return {
            "id": f"Q-{index + 1:03d}",
            "question": "Clarification required",
            "owner": "Unassigned",
            "severity": "medium",
            "context": "",
        }
    severity = _clean(entry.get("severity") or entry.get("priority") or "medium").lower()
    if severity not in {"blocker", "high", "medium", "low"}:
        severity = "medium"
    return {
        "id": _clean(entry.get("id")) or f"Q-{index + 1:03d}",
        "question": _clean(entry.get("question") or entry.get("text") or entry.get("summary")) or "Clarification required",
        "owner": _clean(entry.get("owner") or entry.get("assignee")) or "Unassigned",
        "severity": severity,
        "context": _clean(entry.get("context") or entry.get("impact")),
    }


def _extract_tables_from_sql_catalog(sql_catalog: list[Any]) -> list[str]:
    seen: set[str] = set()
    tables: list[str] = []
    for row in sql_catalog:
        text = _clean(row)
        if not text:
            continue
        matches = re.findall(r"\b(?:from|join|into|update)\s+([a-zA-Z_][\w.]*)", text, flags=re.IGNORECASE)
        for name in matches:
            key = name.lower()
            if key in seen:
                continue
            seen.add(key)
            tables.append(name)
    return tables[:24]


def _derive_golden_flows(
    ui_event_map: list[Any],
    legacy_forms: list[Any],
    bdd_features: list[Any],
) -> list[dict[str, Any]]:
    seen: set[str] = set()
    flows: list[dict[str, Any]] = []
    for idx, row in enumerate(ui_event_map[:12], start=1):
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler"))
        form = _clean(row.get("form"))
        control = _clean(row.get("control"))
        event = _clean(row.get("event"))
        key = "|".join([form.lower(), control.lower(), event.lower(), handler.lower()])
        if not key or key in seen:
            continue
        seen.add(key)
        sql_touches = _as_list(row.get("sql_touches"))
        touched = [_clean(x) for x in sql_touches if _clean(x)]
        linked: list[str] = []
        for feat in bdd_features:
            if not isinstance(feat, dict):
                continue
            fid = _clean(feat.get("id"))
            gherkin = _clean(feat.get("gherkin")).lower()
            if not fid or not gherkin:
                continue
            if (form and form.lower() in gherkin) or (handler and handler.lower() in gherkin):
                linked.append(fid)
        flows.append(
            {
                "id": f"GF-{idx:03d}",
                "name": handler or f"{form or 'Form'} {event or 'flow'}".strip(),
                "entrypoint": "::".join([x for x in [form, handler or event] if x]) or "legacy-flow",
                "tables_touched": touched[:6],
                "expected_outcome": "Behavior matches legacy flow with equivalent side effects.",
                "bdd_scenario_ids": linked[:4],
            }
        )
    if flows:
        return flows[:10]
    for idx, form in enumerate(legacy_forms[:5], start=1):
        form_name = _clean(form.get("form_name")) if isinstance(form, dict) else _clean(form)
        if not form_name:
            form_name = f"Form-{idx}"
        flows.append(
            {
                "id": f"GF-{idx:03d}",
                "name": f"{form_name} critical flow",
                "entrypoint": f"{form_name}::primary_event",
                "tables_touched": [],
                "expected_outcome": "Behavior matches legacy flow with equivalent side effects.",
                "bdd_scenario_ids": [],
            }
        )
    return flows


def _is_generic_bdd_feature(feature: Any) -> bool:
    if not isinstance(feature, dict):
        return True
    text = _clean(feature.get("gherkin")).lower()
    if not text:
        return True
    return any(marker in text for marker in GENERIC_BDD_MARKERS)


def _normalize_priority(value: Any) -> str:
    priority = _clean(value).upper()
    return priority if priority in {"P0", "P1", "P2", "P3"} else "P1"


def _to_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _resolve_legacy_inventory(output: dict[str, Any], req_pack: dict[str, Any]) -> dict[str, Any]:
    inv = output.get("legacy_code_inventory", {})
    if isinstance(inv, dict) and inv:
        return inv
    nested = req_pack.get("legacy_code_inventory", {})
    return nested if isinstance(nested, dict) else {}


def _resolve_vb6_analysis(output: dict[str, Any], legacy_inventory: dict[str, Any]) -> dict[str, Any]:
    vb6 = output.get("vb6_analysis", {})
    if isinstance(vb6, dict) and vb6:
        return vb6
    nested = legacy_inventory.get("vb6_analysis", {})
    return nested if isinstance(nested, dict) else {}


def _resolve_context_ref(output: dict[str, Any], req_pack: dict[str, Any]) -> dict[str, Any]:
    context = output.get("context_reference", {})
    if isinstance(context, dict) and context:
        return context
    nested = req_pack.get("context_reference", {})
    return nested if isinstance(nested, dict) else {}


def _guess_member_kind(path: str) -> str:
    lower = str(path or "").strip().lower()
    if lower.endswith(".frm"):
        return "form"
    if lower.endswith(".bas"):
        return "module"
    if lower.endswith(".cls"):
        return "class"
    if lower.endswith(".ctl"):
        return "screen"
    if lower.endswith(".vbp"):
        return "program"
    if lower.endswith(".dsr"):
        return "report"
    return "other"


def _parse_sql_kind(sql_text: str) -> str:
    text = _clean(sql_text).lower()
    if not text:
        return "unknown"
    for marker in ("select", "insert", "update", "delete"):
        if text.startswith(marker):
            return marker
    if text.startswith(("create", "alter", "drop", "truncate")):
        return "ddl"
    return "unknown"


def _sql_template(sql_text: str) -> str:
    text = _clean(sql_text)
    if not text:
        return ""
    templated = re.sub(r"'[^']*'", "':value'", text)
    templated = re.sub(r"\b\d+\b", ":num", templated)
    return templated


def _sql_risk_flags(sql_text: str) -> list[str]:
    text = _clean(sql_text)
    if not text:
        return []
    lower = text.lower()
    flags: list[str] = []
    if "select *" in lower:
        flags.append("select_star")
    if "+'" in lower or "'+" in lower or "+'" in lower or '" &' in lower or "& '" in lower:
        flags.append("string_concatenation")
        flags.append("possible_injection")
    if "execute(" in lower or "open " in lower and " where " not in lower and "update " in lower:
        flags.append("dynamic_sql")
    if lower.startswith("update ") and " where " not in lower:
        flags.append("missing_where_clause")
    if lower.startswith("delete ") and " where " not in lower:
        flags.append("missing_where_clause")
    if "password" in lower or "pass=" in lower or " pass " in lower:
        flags.append("sensitive_credential_query")
    unique: list[str] = []
    seen: set[str] = set()
    for flag in flags:
        if flag in seen:
            continue
        seen.add(flag)
        unique.append(flag)
    return unique


def _derive_procedure_steps(
    *,
    form: str,
    handler: str,
    control: str,
    event: str,
    calls: list[str],
    sql_touches: list[str],
) -> list[str]:
    steps: list[str] = []
    trigger = " ".join([x for x in [control, event] if x]).strip() or handler or "event"
    steps.append(f"Triggered from {trigger}.")
    if calls:
        steps.append("Invokes procedures: " + ", ".join(calls[:6]) + ".")
    if sql_touches:
        steps.append(f"Executes {len(sql_touches)} SQL statement(s) affecting transactional state.")
    if form:
        steps.append(f"Runs in form context {form}.")
    return steps[:12]


def _dependency_kind(name: str) -> str:
    lower = _clean(name).lower()
    if lower.endswith(".ocx"):
        return "ocx"
    if lower.endswith(".dll"):
        return "dll"
    if lower.endswith(".dcx"):
        return "db_query_definition"
    if lower.endswith(".dca"):
        return "db_connection_definition"
    if "com" in lower or "typelib" in lower:
        return "com_typelib"
    return "other"


def build_raw_artifact_set_v1(output: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    """
    Build standardized raw extracted artifacts for Analyst v2 composition.
    """

    safe = _as_dict(output)
    req_pack = _as_dict(safe.get("requirements_pack"))
    legacy_inventory = _resolve_legacy_inventory(safe, req_pack)
    vb6_analysis = _resolve_vb6_analysis(safe, legacy_inventory)
    context_ref = _resolve_context_ref(safe, req_pack)
    source_target_profile = _as_dict(safe.get("source_target_modernization_profile")) or _as_dict(
        req_pack.get("source_target_modernization_profile")
    )
    source_profile = _as_dict(source_target_profile.get("source"))
    skill = _as_dict(safe.get("legacy_skill_profile")) or _as_dict(req_pack.get("legacy_skill_profile"))

    generated = generated_at or _utc_now()
    repo = _clean(context_ref.get("repo") or context_ref.get("repo_url") or source_profile.get("repo"))
    branch = _clean(context_ref.get("branch")) or "main"
    commit_sha = _clean(context_ref.get("commit_sha"))
    version_id = _clean(context_ref.get("version_id"))
    source_language = _clean(source_profile.get("language")) or _clean(safe.get("source_language")) or "unknown"
    ecosystem = _clean(source_profile.get("framework")) or _clean(skill.get("selected_skill_id")) or "legacy"
    skill_id = _clean(skill.get("selected_skill_id")) or "generic_legacy"
    skill_version = _clean(skill.get("version")) or "1.0.0"
    skill_confidence = _to_float(skill.get("confidence"), 0.0)

    metadata_common = {
        "generated_at": generated,
        "skill_pack": {
            "id": skill_id,
            "version": skill_version,
            "confidence": skill_confidence,
        },
        "context_reference": {
            "repo": repo,
            "branch": branch,
            "commit_sha": commit_sha,
            "version_id": version_id,
        },
    }

    vb6_projects = _as_list(legacy_inventory.get("vb6_projects"))
    forms = _as_list(legacy_inventory.get("forms"))
    controls = _as_list(legacy_inventory.get("controls"))
    event_handlers = _as_list(legacy_inventory.get("event_handlers"))
    database_tables = [_clean(x) for x in _as_list(legacy_inventory.get("database_tables")) if _clean(x)]
    sql_catalog_rows = _as_list(legacy_inventory.get("sql_query_catalog")) or _as_list(vb6_analysis.get("sql_query_catalog"))
    ui_event_rows = _as_list(legacy_inventory.get("ui_event_map")) or _as_list(vb6_analysis.get("ui_event_map"))
    readiness = _as_dict(legacy_inventory.get("modernization_readiness")) or _as_dict(vb6_analysis.get("modernization_readiness"))

    sql_statements: list[dict[str, Any]] = []
    sql_raw_to_id: dict[str, str] = {}
    sql_id_index: dict[str, dict[str, Any]] = {}
    sql_counter = 1
    for row in sql_catalog_rows:
        raw = ""
        tables: list[str] = []
        columns: list[str] = []
        if isinstance(row, dict):
            raw = _clean(row.get("raw") or row.get("sql") or row.get("statement"))
            tables = [_clean(x) for x in _as_list(row.get("tables")) if _clean(x)]
            columns = [_clean(x) for x in _as_list(row.get("columns")) if _clean(x)]
        else:
            raw = _clean(row)
        if not raw:
            continue
        sql_id = f"sql:{sql_counter}"
        sql_counter += 1
        if not tables:
            tables = _extract_tables_from_sql_catalog([raw])
        risk_flags = _sql_risk_flags(raw)
        normalized = _sql_template(raw) or raw
        kind = _parse_sql_kind(raw)
        statement = {
            "sql_id": sql_id,
            "kind": kind,
            "raw": raw,
            "normalized": normalized,
            "tables": tables,
            "columns": columns,
            "parameters": [],
            "usage_sites": [],
            "risk_flags": risk_flags,
        }
        sql_statements.append(
            statement
        )
        sql_raw_to_id[raw.lower()] = sql_id
        sql_id_index[sql_id] = statement

    event_entries: list[dict[str, Any]] = []
    procedure_summaries: list[dict[str, Any]] = []
    sql_map_entries: list[dict[str, Any]] = []
    sql_map_counter = 1
    for idx, row in enumerate(ui_event_rows, start=1):
        if not isinstance(row, dict):
            continue
        form = _clean(row.get("form"))
        handler = _clean(row.get("event_handler"))
        control = _clean(row.get("control"))
        event = _clean(row.get("event"))
        symbol = "::".join([x for x in [form, handler] if x]) or handler or f"entry_{idx}"
        touches = [_clean(x) for x in _as_list(row.get("sql_touches")) if _clean(x)]
        sql_ids: list[str] = []
        entry_risk_flags: list[str] = []
        write_tables: list[str] = []
        read_tables: list[str] = []
        for touch in touches:
            sid = sql_raw_to_id.get(touch.lower())
            if sid:
                sql_ids.append(sid)
                statement = sql_id_index.get(sid, {})
                usage_site = {
                    "type": "external_ref",
                    "external_ref": {
                        "ref": symbol,
                        "description": "event/procedure usage site",
                    },
                    "confidence": 0.8,
                }
                statement_usage = _as_list(statement.get("usage_sites"))
                statement_usage.append(usage_site)
                statement["usage_sites"] = statement_usage[:40]
                for risk in _as_list(statement.get("risk_flags")):
                    rf = _clean(risk)
                    if rf and rf not in entry_risk_flags:
                        entry_risk_flags.append(rf)
                for tbl in _as_list(statement.get("tables")):
                    table_name = _clean(tbl)
                    if not table_name:
                        continue
                    kind = _clean(statement.get("kind")).lower()
                    if kind in {"insert", "update", "delete"}:
                        if table_name not in write_tables:
                            write_tables.append(table_name)
                    else:
                        if table_name not in read_tables:
                            read_tables.append(table_name)
                sql_map_entries.append(
                    {
                        "map_id": f"sqlmap:{sql_map_counter}",
                        "entry_id": f"event:{idx}",
                        "form": form,
                        "procedure": handler or symbol,
                        "sql_id": sid,
                        "operation": _clean(statement.get("kind")) or "unknown",
                        "tables": [_clean(x) for x in _as_list(statement.get("tables")) if _clean(x)],
                        "risk_flags": [_clean(x) for x in _as_list(statement.get("risk_flags")) if _clean(x)],
                        "usage_sites": [_clean(symbol)],
                    }
                )
                sql_map_counter += 1
        tables = _extract_tables_from_sql_catalog(touches)
        event_entries.append(
            {
                "entry_id": f"event:{idx}",
                "entry_kind": "ui_event",
                "name": f"{form or 'form'}:{event or 'event'}",
                "container": form,
                "trigger": {"control": control, "event": event},
                "handler": {"symbol": symbol, "evidence": []},
                "calls": [_clean(x) for x in _as_list(row.get("procedure_calls")) if _clean(x)],
                "side_effects": {
                    "sql_ids": sql_ids,
                    "tables_or_files": tables,
                    "writes": write_tables,
                    "reads": read_tables or tables,
                    "messages": [],
                },
                "risk_flags": entry_risk_flags,
            }
        )
        procedure_summaries.append(
            {
                "procedure_id": f"proc:{idx}",
                "form": form,
                "procedure_name": handler or symbol,
                "trigger": {
                    "control": control,
                    "event": event,
                },
                "summary": (
                    f"{handler or symbol} orchestrates {event or 'event'} flow in {form or 'legacy form'} "
                    f"with {len(sql_ids)} SQL touchpoint(s)."
                ),
                "inputs": [x for x in [control, event] if _clean(x)],
                "steps": _derive_procedure_steps(
                    form=form,
                    handler=handler,
                    control=control,
                    event=event,
                    calls=[_clean(x) for x in _as_list(row.get("procedure_calls")) if _clean(x)],
                    sql_touches=touches,
                ),
                "sql_ids": sql_ids,
                "tables_touched": tables,
                "data_mutations": write_tables,
                "navigation_side_effects": [
                    c for c in [_clean(x) for x in _as_list(row.get("procedure_calls")) if _clean(x)]
                    if c.lower().endswith(".show") or c.lower().endswith(".hide") or "show" in c.lower()
                ][:6],
                "validation_signals": [],
                "risks": entry_risk_flags,
                "suggested_test_seeds": [
                    f"{handler or symbol} happy path",
                    f"{handler or symbol} invalid input path",
                ],
                "evidence": [
                    {
                        "type": "external_ref",
                        "external_ref": {
                            "ref": symbol,
                            "description": "ui_event_map linkage",
                        },
                        "confidence": 0.8,
                    }
                ],
            }
        )

    if not event_entries and sql_statements:
        fallback_forms: list[str] = []
        for row in forms:
            if isinstance(row, dict):
                form_name = _clean(row.get("form_name") or row.get("name") or row.get("file"))
            else:
                form_name = _clean(row)
            if form_name:
                fallback_forms.append(form_name)
        if not fallback_forms:
            fallback_forms = ["legacy-form"]
        for idx, statement in enumerate(sql_statements[:180], start=1):
            form_name = fallback_forms[(idx - 1) % len(fallback_forms)]
            sql_id = _clean(statement.get("sql_id"))
            kind = _clean(statement.get("kind")) or "unknown"
            tables = [_clean(x) for x in _as_list(statement.get("tables")) if _clean(x)]
            risks = [_clean(x) for x in _as_list(statement.get("risk_flags")) if _clean(x)]
            entry_id = f"event:inferred:{idx}"
            procedure_name = f"inferred_sql_flow_{idx:03d}"
            event_entries.append(
                {
                    "entry_id": entry_id,
                    "entry_kind": "ui_event",
                    "name": f"{form_name}:inferred",
                    "container": form_name,
                    "trigger": {"control": "", "event": "inferred"},
                    "handler": {"symbol": f"{form_name}::{procedure_name}", "evidence": []},
                    "calls": [],
                    "side_effects": {
                        "sql_ids": [sql_id] if sql_id else [],
                        "tables_or_files": tables,
                        "writes": tables if kind in {"insert", "update", "delete"} else [],
                        "reads": tables if kind not in {"insert", "update", "delete"} else [],
                        "messages": [],
                    },
                    "risk_flags": risks,
                }
            )
            procedure_summaries.append(
                {
                    "procedure_id": f"proc:inferred:{idx}",
                    "form": form_name,
                    "procedure_name": procedure_name,
                    "trigger": {"control": "", "event": "inferred"},
                    "summary": f"Inferred procedure for SQL contract {sql_id or idx}.",
                    "inputs": [],
                    "steps": [
                        "Inferred from SQL catalog because explicit event map rows were unavailable.",
                        f"Executes {kind.upper()} against {', '.join(tables) if tables else 'unknown tables'}.",
                    ],
                    "sql_ids": [sql_id] if sql_id else [],
                    "tables_touched": tables,
                    "data_mutations": tables if kind in {"insert", "update", "delete"} else [],
                    "navigation_side_effects": [],
                    "validation_signals": [],
                    "risks": risks,
                    "suggested_test_seeds": [f"{procedure_name} data contract parity"],
                    "evidence": [],
                }
            )
            sql_map_entries.append(
                {
                    "map_id": f"sqlmap:inferred:{idx}",
                    "entry_id": entry_id,
                    "form": form_name,
                    "procedure": procedure_name,
                    "sql_id": sql_id,
                    "operation": kind,
                    "tables": tables,
                    "risk_flags": risks,
                    "usage_sites": [f"{form_name}::{procedure_name}"],
                }
            )

    dep_candidates = [
        *_as_list(legacy_inventory.get("activex_controls")),
        *_as_list(legacy_inventory.get("dll_dependencies")),
        *_as_list(legacy_inventory.get("ocx_dependencies")),
        *_as_list(legacy_inventory.get("dcx_dependencies")),
        *_as_list(legacy_inventory.get("dca_dependencies")),
        *_as_list(legacy_inventory.get("dependencies")),
    ]
    dep_seen: set[str] = set()
    dependencies: list[dict[str, Any]] = []
    dep_idx = 1
    for dep in dep_candidates:
        name = _clean(dep)
        if not name:
            continue
        key = name.lower()
        if key in dep_seen:
            continue
        dep_seen.add(key)
        dependencies.append(
            {
                "dependency_id": f"dep:{dep_idx}",
                "name": name,
                "kind": _dependency_kind(name),
                "version": "",
                "source": "legacy_inventory_scan",
                "usage": {"used_by": [], "usage_sites": []},
                "surface": {
                    "prog_ids": [],
                    "class_ids": [],
                    "late_binding_sites": 0,
                    "callbyname_sites": 0,
                },
                "risk": {"tier": "medium", "notes": "", "recommended_action": "Assess replacement/interop strategy."},
            }
        )
        dep_idx += 1

    rule_rows = _as_list(legacy_inventory.get("business_rules_catalog")) or _as_list(safe.get("business_rules_catalog"))
    rules: list[dict[str, Any]] = []
    for idx, row in enumerate(rule_rows, start=1):
        if not isinstance(row, dict):
            continue
        confidence = _to_float(row.get("confidence"), 0.6)
        rules.append(
            {
                "rule_id": _clean(row.get("id")) or f"rule:{idx}",
                "category": _clean(row.get("rule_type")) or "other",
                "statement": _clean(row.get("statement")) or "Business rule extracted from legacy code.",
                "scope": {
                    "project_id": _clean(row.get("project_id")),
                    "component_id": _clean(row.get("scope")),
                },
                "evidence": (
                    [
                        {
                            "type": "external_ref",
                            "external_ref": {
                                "ref": _clean(row.get("evidence")),
                                "description": "Legacy rule evidence",
                            },
                            "confidence": confidence,
                        }
                    ]
                    if _clean(row.get("evidence"))
                    else []
                ),
                "confidence": confidence,
                "tags": [_clean(x) for x in _as_list(row.get("tags")) if _clean(x)],
            }
        )

    pitfall_detectors = _as_list(legacy_inventory.get("pitfall_detectors")) or _as_list(vb6_analysis.get("pitfall_detectors"))
    error_profile = _as_dict(legacy_inventory.get("error_handling_profile")) or _as_dict(vb6_analysis.get("error_handling_profile"))
    detector_findings: list[dict[str, Any]] = []
    for idx, row in enumerate(pitfall_detectors, start=1):
        if not isinstance(row, dict):
            continue
        severity = _clean(row.get("severity")).lower() or "medium"
        if severity not in {"low", "medium", "high"}:
            severity = "medium"
        evidence = _clean(row.get("evidence"))
        detector_findings.append(
            {
                "detector_id": _clean(row.get("id")) or f"det:{idx}",
                "severity": severity,
                "count": int(row.get("count", 0) or 0),
                "summary": evidence or "Detector finding",
                "required_actions": [_clean(x) for x in _as_list(row.get("requires")) if _clean(x)],
                "evidence": (
                    [
                        {
                            "type": "external_ref",
                            "external_ref": {"ref": evidence, "description": "Detector evidence"},
                            "confidence": 0.9,
                        }
                    ]
                    if evidence
                    else []
                ),
            }
        )

    projects_out: list[dict[str, Any]] = []
    for pidx, project in enumerate(vb6_projects, start=1):
        if not isinstance(project, dict):
            continue
        members_raw = _as_list(project.get("member_files"))
        forms_raw = _as_list(project.get("forms"))
        members: list[dict[str, Any]] = []
        mcounter = 1
        for path in members_raw:
            clean_path = _clean(path)
            if not clean_path:
                continue
            members.append(
                {
                    "id": f"{_clean(project.get('project_name')) or f'project_{pidx}'}:member:{mcounter}",
                    "kind": _guess_member_kind(clean_path),
                    "path": clean_path,
                    "display_name": clean_path.split("/")[-1],
                    "evidence": [],
                }
            )
            mcounter += 1
        ui_assets: list[dict[str, Any]] = []
        for fidx, form in enumerate(forms_raw, start=1):
            name = _clean(form)
            if not name:
                continue
            ui_assets.append(
                {
                    "id": f"{_clean(project.get('project_name')) or f'project_{pidx}'}:ui:{fidx}",
                    "kind": "form",
                    "name": name,
                    "controls_count": 0,
                    "event_handlers_count": 0,
                }
            )
        projects_out.append(
            {
                "project_id": _clean(project.get("project_name")) or f"project:{pidx}",
                "name": _clean(project.get("project_name")) or f"Project {pidx}",
                "type": _clean(project.get("project_type")) or "legacy_project",
                "startup": _clean(project.get("startup_object")),
                "file": _clean(project.get("project_file")),
                "business_hypothesis": _clean(project.get("business_objective_hypothesis")),
                "capabilities": [_clean(x) for x in _as_list(project.get("key_business_capabilities")) if _clean(x)],
                "members": members,
                "ui_assets": ui_assets,
                "dependencies": [_clean(x) for x in _as_list(project.get("activex_dependencies")) if _clean(x)],
                "file_coverage": {
                    "by_type": _as_dict(legacy_inventory.get("vb6_file_type_coverage")).get("counts", {}),
                    "binary_companions": [_clean(x) for x in _as_list(legacy_inventory.get("binary_companion_files")) if _clean(x)],
                },
            }
        )

    forms_count = max(len(forms), sum((int(_as_dict(p).get("forms_count", 0) or 0) for p in vb6_projects if isinstance(p, dict))))
    controls_count = len(controls)
    if controls_count == 0:
        controls_count = sum((len(_as_list(_as_dict(p).get("controls"))) for p in vb6_projects if isinstance(p, dict)))
    total_deps = len(dependencies)
    inferred_tables = _extract_tables_from_sql_catalog([row.get("raw", "") for row in sql_statements])
    touchpoints = [*database_tables, *[x for x in inferred_tables if x not in set(database_tables)]][:20]

    legacy_inventory_artifact = {
        "artifact_type": "legacy_inventory",
        "artifact_version": "1.0",
        "metadata": {
            **metadata_common,
            "source_language": source_language,
            "ecosystem": ecosystem,
        },
        "summary": {
            "counts": {
                "projects": len(vb6_projects),
                "programs": len([m for m in _as_list(legacy_inventory.get("project_members")) if _clean(m).lower().endswith(".vbp")]),
                "modules": len([m for m in _as_list(legacy_inventory.get("project_members")) if _clean(m).lower().endswith(".bas")]),
                "forms_or_screens": forms_count,
                "controls": controls_count,
                "dependencies": total_deps,
                "event_handlers": len(event_handlers),
            },
            "data_touchpoints": touchpoints,
            "modernization_readiness": {
                "score": int(readiness.get("score", 0) or 0),
                "tier": _clean(readiness.get("risk_tier")) or "medium",
                "recommended_strategy": _clean(_as_dict(readiness.get("recommended_strategy")).get("name")),
                "required_actions": [_clean(x) for x in _as_list(readiness.get("required_actions")) if _clean(x)],
            },
        },
        "projects": projects_out,
    }

    dependency_inventory_artifact = {
        "artifact_type": "dependency_inventory",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "dependencies": dependencies,
    }

    event_map_artifact = {
        "artifact_type": "event_map",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "entries": event_entries,
    }

    sql_catalog_artifact = {
        "artifact_type": "sql_catalog",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "statements": sql_statements,
    }

    business_rule_catalog_artifact = {
        "artifact_type": "business_rule_catalog",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "rules": rules,
    }

    detector_findings_artifact = {
        "artifact_type": "detector_findings",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "findings": detector_findings,
        "profiles": {
            "error_handling": {
                "on_error_resume_next": int(error_profile.get("on_error_resume_next", 0) or 0),
                "on_error_goto": int(error_profile.get("on_error_goto", 0) or 0),
                "late_bound_com_calls": int(error_profile.get("late_bound_com_calls", 0) or 0),
                "default_instance_refs": int(error_profile.get("default_instance_references", 0) or 0),
                "control_array_markers": int(error_profile.get("control_array_index_markers", 0) or 0),
                "win32_declares": len(_as_list(legacy_inventory.get("win32_declares")) or _as_list(vb6_analysis.get("win32_declares"))),
            }
        },
    }

    procedure_summary_artifact = {
        "artifact_type": "procedure_summary",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "procedures": procedure_summaries[:600],
    }

    sql_map_artifact = {
        "artifact_type": "sql_map",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "entries": sql_map_entries[:900],
    }

    domain_pack = _as_dict(safe.get("domain_pack")) or _as_dict(_as_dict(req_pack.get("project")).get("domain_pack"))
    global_directives = _as_list(safe.get("global_directives"))
    memory_constraints = _as_list(safe.get("memory_constraints"))
    directive_principles: list[str] = []
    for row in [*global_directives, *memory_constraints]:
        if isinstance(row, dict):
            text = _clean(row.get("text") or row.get("statement") or row.get("title"))
        else:
            text = _clean(row)
        if text and text not in directive_principles:
            directive_principles.append(text)
    delivery_constitution_artifact = {
        "artifact_type": "delivery_constitution",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "principles": [
            "Preserve critical legacy behavior first; modernization must prove functional equivalence.",
            "Every modernization decision must map to explicit evidence (code, query, event, or rule).",
            "No breaking change to data contracts without approved migration path and rollback evidence.",
            *directive_principles[:12],
        ],
        "policy_context": {
            "domain_pack_id": _clean(domain_pack.get("id")),
            "domain_pack_version": _clean(domain_pack.get("version")),
            "governance_tier": _clean(safe.get("governance_tier") or _as_dict(req_pack.get("project")).get("governance_tier")),
        },
        "sources": [
            "legacy_inventory",
            "convention_profile",
            "domain_pack",
            "memory_constraints",
        ],
    }

    refs = {
        "legacy_inventory": "artifact://analyst/raw/legacy_inventory/v1",
        "dependency_inventory": "artifact://analyst/raw/dependency_inventory/v1",
        "event_map": "artifact://analyst/raw/event_map/v1",
        "sql_catalog": "artifact://analyst/raw/sql_catalog/v1",
        "sql_map": "artifact://analyst/raw/sql_map/v1",
        "procedure_summary": "artifact://analyst/raw/procedure_summary/v1",
        "business_rule_catalog": "artifact://analyst/raw/business_rule_catalog/v1",
        "detector_findings": "artifact://analyst/raw/detector_findings/v1",
        "delivery_constitution": "artifact://analyst/raw/delivery_constitution/v1",
    }
    artifact_index = {
        "artifact_type": "artifact_index",
        "artifact_version": "1.0",
        "metadata": {
            "generated_at": generated,
            "context_reference": {"repo": repo, "commit_sha": commit_sha},
        },
        "artifacts": [
            {"type": "legacy_inventory", "ref": refs["legacy_inventory"]},
            {"type": "dependency_inventory", "ref": refs["dependency_inventory"]},
            {"type": "event_map", "ref": refs["event_map"]},
            {"type": "sql_catalog", "ref": refs["sql_catalog"]},
            {"type": "sql_map", "ref": refs["sql_map"]},
            {"type": "procedure_summary", "ref": refs["procedure_summary"]},
            {"type": "business_rule_catalog", "ref": refs["business_rule_catalog"]},
            {"type": "detector_findings", "ref": refs["detector_findings"]},
            {"type": "delivery_constitution", "ref": refs["delivery_constitution"]},
        ],
    }

    return {
        "legacy_inventory": legacy_inventory_artifact,
        "dependency_inventory": dependency_inventory_artifact,
        "event_map": event_map_artifact,
        "sql_catalog": sql_catalog_artifact,
        "sql_map": sql_map_artifact,
        "procedure_summary": procedure_summary_artifact,
        "business_rule_catalog": business_rule_catalog_artifact,
        "detector_findings": detector_findings_artifact,
        "delivery_constitution": delivery_constitution_artifact,
        "artifact_index": artifact_index,
        "artifact_refs": refs,
    }


def build_analyst_report_v2(output: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    """
    Build or pass through analyst_report_v2.

    If `output.analyst_report_v2` already looks valid, it is returned unchanged.
    """

    safe = _as_dict(output)
    prebuilt = _as_dict(safe.get("analyst_report_v2"))
    if (
        _clean(prebuilt.get("artifact_type")) == "analyst_report"
        and _clean(prebuilt.get("artifact_version")) == "2.0"
    ):
        return prebuilt

    req_pack = _as_dict(safe.get("requirements_pack"))
    walkthrough = _as_dict(safe.get("analysis_walkthrough"))
    legacy_inventory = _resolve_legacy_inventory(safe, req_pack)
    vb6_analysis = _resolve_vb6_analysis(safe, legacy_inventory)
    context_ref = _resolve_context_ref(safe, req_pack)
    readiness = _as_dict(legacy_inventory.get("modernization_readiness"))
    if not readiness:
        readiness = _as_dict(vb6_analysis.get("modernization_readiness"))
    skill = _as_dict(safe.get("legacy_skill_profile"))
    if not skill:
        skill = _as_dict(req_pack.get("legacy_skill_profile"))

    raw_artifacts = _as_dict(safe.get("raw_artifacts"))
    if not raw_artifacts:
        raw_artifacts = build_raw_artifact_set_v1(safe, generated_at=generated_at)

    legacy_forms = _as_list(legacy_inventory.get("forms"))
    vb6_projects = _as_list(legacy_inventory.get("vb6_projects"))
    ui_event_map = _as_list(legacy_inventory.get("ui_event_map")) or _as_list(vb6_analysis.get("ui_event_map"))
    sql_catalog = _as_list(legacy_inventory.get("sql_query_catalog")) or _as_list(vb6_analysis.get("sql_query_catalog"))
    pitfall_detectors = _as_list(legacy_inventory.get("pitfall_detectors")) or _as_list(vb6_analysis.get("pitfall_detectors"))
    activex_controls = _as_list(legacy_inventory.get("activex_controls"))
    dll_dependencies = _as_list(legacy_inventory.get("dll_dependencies"))
    ocx_dependencies = _as_list(legacy_inventory.get("ocx_dependencies"))
    dcx_dependencies = _as_list(legacy_inventory.get("dcx_dependencies"))
    dca_dependencies = _as_list(legacy_inventory.get("dca_dependencies"))
    event_handlers = _as_list(legacy_inventory.get("event_handlers"))
    business_rules = _as_list(legacy_inventory.get("business_rules_catalog"))

    controls_count = 0
    for project in vb6_projects:
        if isinstance(project, dict):
            controls_count += len(_as_list(project.get("controls")))
    if controls_count == 0:
        controls_count = len(_as_list(legacy_inventory.get("controls")))

    project_form_count = 0
    for project in vb6_projects:
        if not isinstance(project, dict):
            continue
        forms_count = project.get("forms_count")
        if isinstance(forms_count, int) and forms_count > 0:
            project_form_count += forms_count
        else:
            project_form_count += len(_as_list(project.get("forms")))
    forms_count = max(len(legacy_forms), project_form_count)

    db_tables = [_clean(x) for x in _as_list(legacy_inventory.get("database_tables")) if _clean(x)]
    inferred_tables = _extract_tables_from_sql_catalog(sql_catalog)
    tables_touched: list[str] = []
    seen_tables: set[str] = set()
    for table in [*db_tables, *inferred_tables]:
        key = table.lower()
        if key in seen_tables:
            continue
        seen_tables.add(key)
        tables_touched.append(table)
    tables_touched = tables_touched[:12]

    readiness_score = readiness.get("score")
    try:
        score = int(round(float(readiness_score)))
    except (TypeError, ValueError):
        score = 60
    score = max(0, min(100, score))

    risk_tier = _clean(readiness.get("risk_tier")).lower()
    if risk_tier not in {"low", "medium", "high"}:
        risk_tier = "high" if score < 45 else ("medium" if score < 75 else "low")

    top_risks: list[dict[str, Any]] = []
    sorted_detectors = sorted(
        [d for d in pitfall_detectors if isinstance(d, dict)],
        key=lambda item: int(item.get("count", 0) or 0),
        reverse=True,
    )
    for idx, det in enumerate(sorted_detectors[:3], start=1):
        severity = _clean(det.get("severity")).lower() or "medium"
        if severity not in {"low", "medium", "high"}:
            severity = "medium"
        evidence_text = _clean(det.get("evidence"))
        top_risks.append(
            {
                "id": _clean(det.get("id")) or f"DET-{idx}",
                "severity": severity,
                "description": (
                    f"{_clean(det.get('id')) or f'Detector-{idx}'} "
                    f"count={int(det.get('count', 0) or 0)}"
                    + (f" | {evidence_text}" if evidence_text else "")
                ),
                "mitigation": "Apply targeted migration playbook and parity tests for this detector pattern.",
                "evidence_refs": [_clean(det.get("id"))] if _clean(det.get("id")) else [],
            }
        )

    for idx, risk in enumerate(_as_list(safe.get("risks"))[:3], start=1):
        if not isinstance(risk, dict):
            continue
        severity = _clean(risk.get("impact")).lower() or "medium"
        if severity == "critical":
            severity = "high"
        if severity not in {"low", "medium", "high"}:
            severity = "medium"
        top_risks.append(
            {
                "id": _clean(risk.get("id")) or f"RISK-{idx}",
                "severity": severity,
                "description": _clean(risk.get("description")) or "Legacy modernization risk identified.",
                "mitigation": _clean(risk.get("mitigation")) or "Add explicit mitigation plan and gate checks.",
                "evidence_refs": [],
            }
        )
    top_risks = top_risks[:8]

    open_questions_raw = _as_list(safe.get("open_questions")) or _as_list(req_pack.get("open_questions"))
    open_questions = [_normalize_open_question(row, idx) for idx, row in enumerate(open_questions_raw)]

    blocking_decisions: list[dict[str, Any]] = [
        {
            "id": "DEC-UI-001",
            "question": "Target UI framework selection for migrated forms",
            "options": ["WinForms", "WPF", "Web UI"],
            "default_recommendation": "WinForms for lowest event-model delta from VB6 unless UX redesign is in scope.",
            "impact_if_wrong": "High rework risk in form/event parity and control migration.",
        },
        {
            "id": "DEC-OCX-001",
            "question": "ActiveX/OCX replacement strategy by dependency",
            "options": ["Replace", "Wrap temporarily", "Isolate and defer"],
            "default_recommendation": "Replace common controls and isolate only high-risk dependencies behind adapters.",
            "impact_if_wrong": "Runtime regressions and release delays from unresolved control behavior.",
        },
        {
            "id": "DEC-DB-001",
            "question": "Database contract strategy during migration",
            "options": ["Preserve schema/queries", "Introduce migration layer", "Redesign schema"],
            "default_recommendation": "Preserve contracts initially and modernize behind a compatibility layer.",
            "impact_if_wrong": "Business rule drift and data-side regressions.",
        },
    ]
    for question in open_questions:
        if question["severity"] not in {"blocker", "high"}:
            continue
        blocking_decisions.append(
            {
                "id": _clean(question.get("id")) or "DEC-Q",
                "question": _clean(question.get("question")) or "Open clarification",
                "options": [],
                "default_recommendation": "Resolve with product/business owner before implementation commitment.",
                "impact_if_wrong": "Execution ambiguity and acceptance-test churn.",
            }
        )
    blocking_decisions = blocking_decisions[:8]
    non_blocking_decisions = [
        {
            "id": "DEC-OBS-001",
            "question": "Logging and observability stack for migrated runtime",
            "options": ["OpenTelemetry + structured logs", "Basic logs only"],
            "default_recommendation": "OpenTelemetry + structured logs for parity troubleshooting.",
            "impact_if_wrong": "Lower diagnosability during phased cutover.",
        }
    ]

    functional = _as_list(safe.get("functional_requirements"))
    non_functional = _as_list(safe.get("non_functional_requirements"))
    backlog_items: list[dict[str, Any]] = []
    for idx, item in enumerate(functional, start=1):
        if not isinstance(item, dict):
            continue
        rid = _clean(item.get("id")) or f"FR-{idx:03d}"
        acceptance = [_clean(x) for x in _as_list(item.get("acceptance_criteria")) if _clean(x)]
        backlog_items.append(
            {
                "id": rid,
                "type": "functional",
                "priority": _normalize_priority(item.get("priority")),
                "title": _clean(item.get("title")) or rid,
                "outcome": _clean(item.get("description")) or "Deliver functional parity for this requirement.",
                "acceptance_criteria": acceptance,
                "depends_on": [],
                "evidence_expected": ["traceability_matrix", "functional_test_report"],
            }
        )
    for idx, item in enumerate(non_functional, start=1):
        if not isinstance(item, dict):
            continue
        rid = _clean(item.get("id")) or f"NFR-{idx:03d}"
        acceptance = [_clean(x) for x in _as_list(item.get("acceptance_criteria")) if _clean(x)]
        backlog_items.append(
            {
                "id": rid,
                "type": "non_functional",
                "priority": "P1",
                "title": _clean(item.get("title")) or rid,
                "outcome": _clean(item.get("description")) or "Deliver non-functional controls.",
                "acceptance_criteria": acceptance,
                "depends_on": [],
                "evidence_expected": ["nfr_validation_report", "quality_gate_report"],
            }
        )
    backlog_items = backlog_items[:80]

    bdd_contract = _as_dict(safe.get("bdd_contract"))
    if not bdd_contract:
        bdd_contract = _as_dict(req_pack.get("bdd_contract"))
    bdd_features = _as_list(bdd_contract.get("features"))

    golden_flows = _derive_golden_flows(ui_event_map, legacy_forms, bdd_features)
    generic_bdd_count = sum(1 for feature in bdd_features if _is_generic_bdd_feature(feature))

    quality_gates: list[dict[str, Any]] = []
    for idx, gate in enumerate(_as_list(safe.get("quality_gates")) or _as_list(req_pack.get("quality_gates")), start=1):
        if not isinstance(gate, dict):
            continue
        status = _clean(gate.get("status")).lower()
        result = "pass" if status == "pass" else ("fail" if status == "fail" else "warn")
        quality_gates.append(
            {
                "id": _clean(gate.get("id") or gate.get("name")) or f"gate_{idx}",
                "result": result,
                "description": _clean(gate.get("message") or gate.get("name")) or "Quality gate result",
                "remediation": "Address gate failure before progression." if result == "fail" else "",
            }
        )
    quality_gates.append(
        {
            "id": "bdd_flow_grounding",
            "result": "warn" if generic_bdd_count > 0 else "pass",
            "description": (
                f"{generic_bdd_count} BDD feature(s) appear generic; ground scenarios in real form/event entrypoints."
                if generic_bdd_count > 0
                else "BDD scenarios are grounded in extracted legacy flows."
            ),
            "remediation": "Regenerate BDD from UI Event Map golden flows." if generic_bdd_count > 0 else "",
        }
    )
    quality_gates = quality_gates[:20]

    acceptance_map = _as_list(safe.get("acceptance_test_mapping")) or _as_list(req_pack.get("acceptance_test_mapping"))
    test_matrix: list[dict[str, Any]] = []
    for row in acceptance_map:
        if not isinstance(row, dict):
            continue
        rid = _clean(row.get("requirement_id"))
        if not rid:
            continue
        test_types = [_clean(x).lower() for x in _as_list(row.get("test_types")) if _clean(x)]
        scenarios = [_clean(x) for x in _as_list(row.get("bdd_scenarios")) if _clean(x)]
        test_matrix.append(
            {
                "requirement_id": rid,
                "test_types": test_types,
                "scenario_ids": scenarios,
            }
        )

    trace_links: list[dict[str, Any]] = []
    trace = _as_dict(req_pack.get("traceability"))
    for row in _as_list(trace.get("links")):
        if not isinstance(row, dict):
            continue
        from_id = _clean(row.get("from"))
        to_id = _clean(row.get("to"))
        link_type = _clean(row.get("type"))
        if not from_id or not to_id or not link_type:
            continue
        trace_links.append({"from": from_id, "to": to_id, "type": link_type})

    source_target_profile = _as_dict(safe.get("source_target_modernization_profile"))
    if not source_target_profile:
        source_target_profile = _as_dict(req_pack.get("source_target_modernization_profile"))
    source_profile = _as_dict(source_target_profile.get("source"))
    repo_hint = _clean(context_ref.get("repo") or context_ref.get("repo_url") or source_profile.get("repo"))

    project_name = _clean(safe.get("project_name")) or _clean(_as_dict(req_pack.get("project")).get("name")) or "Untitled"
    objective = (
        _clean(walkthrough.get("business_objective_summary"))
        or _clean(safe.get("executive_summary"))
        or _clean(req_pack.get("business_objective_summary"))
        or "Objective not captured."
    )
    domain = _clean(_as_dict(req_pack.get("project")).get("domain")) or "software"

    dep_unique = {
        _clean(x).lower()
        for x in [
            *activex_controls,
            *dll_dependencies,
            *ocx_dependencies,
            *dcx_dependencies,
            *dca_dependencies,
            *_as_list(legacy_inventory.get("dependencies")),
        ]
        if _clean(x)
    }

    strategy = _as_dict(readiness.get("recommended_strategy"))
    strategy_name = _clean(strategy.get("name")) or "Phased modernization"
    strategy_rationale = _clean(strategy.get("rationale")) or "Preserve behavior first, then modernize in controlled phases."

    refs = _as_dict(raw_artifacts.get("artifact_refs"))
    artifact_index_ref = "artifact://analyst/raw/artifact_index/v1"
    procedure_rows = _as_list(_as_dict(raw_artifacts.get("procedure_summary")).get("procedures"))
    sql_map_rows = _as_list(_as_dict(raw_artifacts.get("sql_map")).get("entries"))
    constitution = _as_dict(raw_artifacts.get("delivery_constitution"))
    clarification_markers = [
        {
            "id": _clean(q.get("id")) or f"Q-{idx + 1:03d}",
            "question": _clean(q.get("question")) or "Clarification required",
            "severity": _clean(q.get("severity")) or "medium",
            "owner": _clean(q.get("owner")) or "Unassigned",
        }
        for idx, q in enumerate(open_questions)
    ]
    report = {
        "artifact_type": "analyst_report",
        "artifact_version": "2.0",
        "metadata": {
            "project": {
                "name": project_name,
                "objective": objective,
                "domain": domain,
                "audience_modes": ["client", "engineering"],
            },
            "generated_at": generated_at or _utc_now(),
            "skill_pack": {
                "id": _clean(skill.get("selected_skill_id")) or "generic_legacy",
                "name": _clean(skill.get("selected_skill_name")) or "Generic Legacy Skill",
                "version": _clean(skill.get("version")) or "1.0.0",
                "confidence": _to_float(skill.get("confidence"), 0.0),
                "rationale": " | ".join([_clean(x) for x in _as_list(skill.get("reasons")) if _clean(x)]),
            },
            "context_reference": {
                "repo": repo_hint,
                "branch": _clean(context_ref.get("branch")) or "main",
                "commit_sha": _clean(context_ref.get("commit_sha")),
                "version_id": _clean(context_ref.get("version_id")),
                "scm_version": _clean(context_ref.get("scm_version")) or "1.0",
                "cp_version": _clean(context_ref.get("cp_version")) or "1.0",
                "ha_version": _clean(context_ref.get("ha_version")) or "1.0",
            },
        },
        "decision_brief": {
            "at_a_glance": {
                "readiness_score": score,
                "risk_tier": risk_tier,
                "inventory_summary": {
                    "projects": len(vb6_projects),
                    "forms": forms_count,
                    "controls": controls_count,
                    "dependencies": len(dep_unique),
                    "event_handlers": len(event_handlers),
                    "tables_touched": tables_touched,
                },
                "headline": f"{strategy_name} recommended.",
            },
            "recommended_strategy": {
                "name": strategy_name,
                "rationale": strategy_rationale,
                "phases": [
                    {
                        "id": "PH0",
                        "title": "Baseline and equivalence harness",
                        "outcome": "Capture golden flows and baseline outputs.",
                        "exit_criteria": ["Golden flows agreed", "Baseline outputs captured", "Parity checks defined"],
                    },
                    {
                        "id": "PH1",
                        "title": "Incremental migration and dependency replacement",
                        "outcome": "Migrate forms/modules with dependency risk controls.",
                        "exit_criteria": ["P0 flows migrated", "Critical dependencies addressed", "Regression suite passing"],
                    },
                    {
                        "id": "PH2",
                        "title": "Hardening and release evidence",
                        "outcome": "Finalize quality gates and publish evidence pack.",
                        "exit_criteria": ["Quality gates pass", "Traceability complete", "Release readiness approved"],
                    },
                ],
            },
            "decisions_required": {
                "blocking": blocking_decisions,
                "non_blocking": non_blocking_decisions,
            },
            "top_risks": top_risks,
            "next_steps": [
                {
                    "id": "NS-001",
                    "title": "Confirm blocking decisions and freeze modernization scope",
                    "owner_role": "Tech Lead",
                    "done_when": ["Blocking decisions approved", "Backlog dependencies resolved"],
                },
                {
                    "id": "NS-002",
                    "title": "Implement golden-flow harness for parity validation",
                    "owner_role": "QA Lead",
                    "done_when": ["Golden flow tests created", "Baseline artifacts stored"],
                },
            ],
        },
        "delivery_spec": {
            "scope": {
                "in_scope": [
                    "Preserve legacy business behavior and workflows",
                    "Migrate UI and code to target stack",
                    "Control dependency and data-side risks during migration",
                ],
                "out_of_scope": [_clean(x) for x in _as_list(req_pack.get("out_of_scope")) if _clean(x)],
            },
            "constraints": {
                "musts": [
                    "No critical workflow regression for P0 flows",
                    "Traceability from requirements to tests and evidence artifacts",
                ],
                "shoulds": [
                    "Phased rollout with rollback points",
                    "Preserve DB contracts unless explicitly approved to change",
                ],
            },
            "backlog": {"items": backlog_items},
            "testing_and_evidence": {
                "golden_flows": golden_flows,
                "test_matrix": test_matrix,
                "evidence_outputs": [
                    {
                        "type": "traceability_matrix",
                        "path_hint": "artifacts/evidence/traceability-matrix.json",
                        "description": "Requirement-to-test traceability",
                    },
                    {
                        "type": "quality_gate_report",
                        "path_hint": "artifacts/evidence/quality-gates.json",
                        "description": "Gate outcomes and remediation",
                    },
                    {
                        "type": "golden_flow_diff",
                        "path_hint": "artifacts/evidence/golden-flow-diff.json",
                        "description": "Legacy vs modernized output parity",
                    },
                ],
                "quality_gates": quality_gates,
            },
            "traceability": {"links": trace_links},
            "open_questions": open_questions,
        },
        "appendix": {
            "artifact_refs": {
                "legacy_inventory_ref": _clean(refs.get("legacy_inventory")) or "artifact://analyst/raw/legacy_inventory/v1",
                "event_map_ref": _clean(refs.get("event_map")) or "artifact://analyst/raw/event_map/v1",
                "sql_catalog_ref": _clean(refs.get("sql_catalog")) or "artifact://analyst/raw/sql_catalog/v1",
                "sql_map_ref": _clean(refs.get("sql_map")) or "artifact://analyst/raw/sql_map/v1",
                "procedure_summary_ref": _clean(refs.get("procedure_summary")) or "artifact://analyst/raw/procedure_summary/v1",
                "dependency_list_ref": _clean(refs.get("dependency_inventory")) or "artifact://analyst/raw/dependency_inventory/v1",
                "dependency_inventory_ref": _clean(refs.get("dependency_inventory")) or "artifact://analyst/raw/dependency_inventory/v1",
                "business_rules_ref": _clean(refs.get("business_rule_catalog")) or "artifact://analyst/raw/business_rule_catalog/v1",
                "detector_findings_ref": _clean(refs.get("detector_findings")) or "artifact://analyst/raw/detector_findings/v1",
                "delivery_constitution_ref": _clean(refs.get("delivery_constitution")) or "artifact://analyst/raw/delivery_constitution/v1",
                "artifact_index_ref": artifact_index_ref,
            },
            "high_volume_sections": {},
        },
        "spec_kit_decomposition": {
            "artifact_type": "spec_kit_projection",
            "artifact_version": "1.0",
            "discovery_spec": {
                "title": "Legacy discovery spec",
                "objective": objective,
                "inventory_counts": {
                    "projects": len(vb6_projects),
                    "forms": forms_count,
                    "dependencies": len(dep_unique),
                    "procedures": len(procedure_rows),
                    "sql_map_entries": len(sql_map_rows),
                },
                "key_user_stories": [
                    f"As a modernization engineer, I need a deterministic map of {forms_count} UI flows to avoid behavioral drift.",
                    "As a delivery lead, I need explicit clarification markers to avoid speculative implementation.",
                    "As QA, I need event-to-query evidence to build equivalence tests.",
                ],
                "needs_clarification": clarification_markers,
                "evidence_refs": {
                    "legacy_inventory_ref": _clean(refs.get("legacy_inventory")),
                    "procedure_summary_ref": _clean(refs.get("procedure_summary")),
                    "sql_map_ref": _clean(refs.get("sql_map")),
                },
            },
            "modernization_plan": {
                "title": "Modernization implementation plan",
                "strategy": strategy_name,
                "rationale": strategy_rationale,
                "phases": [
                    {"id": "PH0", "title": "Baseline and equivalence harness"},
                    {"id": "PH1", "title": "Incremental migration and dependency replacement"},
                    {"id": "PH2", "title": "Hardening and release evidence"},
                ],
                "phase_outline": [
                    "Phase 0: baseline and equivalence harness",
                    "Phase 1: form/module migration with dependency replacement",
                    "Phase 2: hardening, governance gates, and evidence publication",
                ],
                "backlog_items": len(backlog_items),
                "blocking_decisions": len(blocking_decisions),
                "constitution_ref": _clean(refs.get("delivery_constitution")) or "artifact://analyst/raw/delivery_constitution/v1",
            },
            "executable_contracts": {
                "title": "Executable contracts",
                "golden_flow_count": len(golden_flows),
                "test_matrix_rows": len(test_matrix),
                "quality_gate_count": len(quality_gates),
                "grounding_status": "needs_improvement" if generic_bdd_count > 0 else "grounded",
                "traceability_links": len(trace_links),
                "contract_refs": {
                    "event_map_ref": _clean(refs.get("event_map")),
                    "sql_catalog_ref": _clean(refs.get("sql_catalog")),
                    "sql_map_ref": _clean(refs.get("sql_map")),
                },
            },
            "constitution": {
                "principles": _as_list(constitution.get("principles"))[:12],
                "source_ref": _clean(refs.get("delivery_constitution")),
            },
        },
    }
    return report
