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
from difflib import SequenceMatcher
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

from utils.analyst_qa import build_qa_report_v1


GENERIC_BDD_MARKERS = (
    "given requirement",
    "when requirement",
    "then requirement",
)
QA_RUNTIME_VERSION = "1.2.0"


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _mermaid_safe_token(value: Any, *, default: str = "item") -> str:
    raw = _clean(value)
    if not raw:
        return default
    token = re.sub(r"[^A-Za-z0-9_]", "_", raw)
    token = re.sub(r"_+", "_", token).strip("_")
    if not token:
        token = default
    if token[0].isdigit():
        token = f"n_{token}"
    return token


def _mermaid_safe_label(value: Any) -> str:
    return _clean(value).replace('"', "'")


def _attach_qa_report_v1(
    report: dict[str, Any],
    *,
    output: dict[str, Any],
    raw_artifacts: dict[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    out = dict(report) if isinstance(report, dict) else {}
    raw_mut = dict(_as_dict(raw_artifacts))

    qa_report = build_qa_report_v1(
        {**_as_dict(output), "raw_artifacts": raw_mut, "analyst_report_v2": out},
        report=out,
        generated_at=generated_at,
    )
    applied_fixes: list[str] = []

    for _ in range(3):
        cycle_fixes: list[str] = []
        deterministic = _apply_deterministic_qa_fixes(out, raw_mut, qa_report)
        if deterministic:
            cycle_fixes.extend(deterministic)
        semantic = _apply_semantic_safe_fixes(raw_mut, qa_report)
        if semantic:
            cycle_fixes.extend(semantic)
        if not cycle_fixes:
            break
        applied_fixes.extend(cycle_fixes)
        qa_report = build_qa_report_v1(
            {**_as_dict(output), "raw_artifacts": raw_mut, "analyst_report_v2": out},
            report=out,
            generated_at=generated_at,
        )

    qa_summary = _as_dict(qa_report.get("summary"))
    auto_fixes_existing = _as_list(qa_summary.get("auto_fixes_applied"))
    dedup_fixes: list[str] = []
    for row in auto_fixes_existing + applied_fixes:
        text = _clean(row)
        if text and text not in dedup_fixes:
            dedup_fixes.append(text)
    qa_summary["auto_fixes_applied"] = dedup_fixes
    qa_report["summary"] = qa_summary
    qa_report["qa_runtime_version"] = QA_RUNTIME_VERSION

    semantic_checks = _as_list(_as_dict(qa_report.get("semantic")).get("checks"))
    suggestions: list[dict[str, Any]] = []
    for row in semantic_checks:
        if not isinstance(row, dict):
            continue
        cid = _clean(row.get("check_id") or row.get("id")) or f"sem_{len(suggestions) + 1}"
        suggestions.append(
            {
                "change_id": f"suggest_{cid}",
                "status": "suggested",
                "type": "semantic_cleanup",
                "title": _clean(row.get("detail"))[:220],
                "suggested_fix": _clean(row.get("suggested_fix"))[:320],
            }
        )
    meaning_validation = _as_dict(qa_report.get("meaning_validation"))
    meaning_validation["status"] = "SUGGESTED" if suggestions else "NOT_RUN"
    meaning_validation["changes"] = suggestions[:40]
    qa_report["meaning_validation"] = meaning_validation

    out["qa_report_v1"] = qa_report
    qa_gates = _as_list(qa_report.get("quality_gates"))
    testing = _as_dict(_as_dict(out.get("delivery_spec")).get("testing_and_evidence"))
    existing_quality = _as_list(testing.get("quality_gates"))
    for gate in qa_gates:
        if not isinstance(gate, dict):
            continue
        gate_id = _clean(gate.get("id")).lower()
        replacement = {
            "id": _clean(gate.get("id")) or f"qa_gate_{len(existing_quality) + 1}",
            "result": _clean(gate.get("result")).lower() or "warn",
            "description": _clean(gate.get("description")) or "QA gate result",
            "remediation": _clean(gate.get("remediation")),
        }
        replaced = False
        if gate_id:
            for idx, existing in enumerate(existing_quality):
                if not isinstance(existing, dict):
                    continue
                existing_id = _clean(existing.get("id")).lower()
                if existing_id == gate_id:
                    existing_quality[idx] = replacement
                    replaced = True
                    break
        if not replaced:
            existing_quality.append(replacement)
    testing["quality_gates"] = existing_quality[:24]
    delivery_spec = _as_dict(out.get("delivery_spec"))
    delivery_spec["testing_and_evidence"] = testing
    out["delivery_spec"] = delivery_spec

    appendix = _as_dict(out.get("appendix"))
    appendix_refs = _as_dict(appendix.get("artifact_refs"))
    appendix_refs.setdefault("qa_report_ref", "embedded://analyst_report_v2/qa_report_v1")
    appendix["artifact_refs"] = appendix_refs
    out["appendix"] = appendix
    out["raw_artifacts"] = raw_mut
    return out


def _is_non_form_reference(value: Any) -> bool:
    token = _clean(value)
    if not token:
        return True
    low = token.lower()
    if low in {"n/a", "na", "none", "unknown"}:
        return True
    if low.endswith((".bas", ".cls", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".dsr", ".dca", ".dcx")):
        return True
    if "project1 (" in low or low in {"project1", "bank_system"}:
        return True
    if "/" in token and not low.endswith(".frm"):
        return True
    return False


def _normalize_open_questions_in_report(report: dict[str, Any]) -> bool:
    delivery = _as_dict(report.get("delivery_spec"))
    if not delivery:
        return False
    open_questions = _as_list(delivery.get("open_questions"))
    if not open_questions:
        return False
    normalized = [_normalize_open_question(row, idx) for idx, row in enumerate(open_questions)]
    changed = normalized != open_questions
    if changed:
        delivery["open_questions"] = normalized
        report["delivery_spec"] = delivery
    return changed


def _fix_rule_form_refs(raw_artifacts: dict[str, Any]) -> bool:
    catalog = _as_dict(raw_artifacts.get("business_rule_catalog"))
    rules = _as_list(catalog.get("rules"))
    if not rules:
        return False
    changed = False
    for row in rules:
        if not isinstance(row, dict):
            continue
        scope = _as_dict(row.get("scope"))
        row_changed = False
        for key in ("form", "form_key", "component_id"):
            value = _clean(scope.get(key))
            if value and _is_non_form_reference(value):
                scope[key] = "n/a"
                row_changed = True
        forms = _as_list(scope.get("forms"))
        filtered_forms = [x for x in forms if not _is_non_form_reference(x)]
        if forms and filtered_forms != forms:
            scope["forms"] = filtered_forms
            row_changed = True
        form_keys = _as_list(scope.get("form_keys"))
        filtered_keys = [x for x in form_keys if not _is_non_form_reference(x)]
        if form_keys and filtered_keys != form_keys:
            scope["form_keys"] = filtered_keys
            row_changed = True
        if row_changed:
            row["scope"] = scope
        top_form = _clean(row.get("form"))
        if top_form and _is_non_form_reference(top_form):
            row["form"] = "n/a"
            row_changed = True
        if row_changed:
            changed = True
    if changed:
        catalog["rules"] = rules
        raw_artifacts["business_rule_catalog"] = catalog
    return changed


def _fix_event_handler_count(report: dict[str, Any], raw_artifacts: dict[str, Any]) -> bool:
    event_count = len(_as_list(_as_dict(raw_artifacts.get("event_map")).get("entries")))
    if event_count <= 0:
        return False
    changed = False
    legacy = _as_dict(raw_artifacts.get("legacy_inventory"))
    summary = _as_dict(legacy.get("summary"))
    counts = _as_dict(summary.get("counts"))
    if int(counts.get("event_handlers", 0) or 0) != event_count:
        counts["event_handlers"] = event_count
        summary["counts"] = counts
        legacy["summary"] = summary
        raw_artifacts["legacy_inventory"] = legacy
        changed = True

    brief = _as_dict(report.get("decision_brief"))
    glance = _as_dict(brief.get("at_a_glance"))
    inv = _as_dict(glance.get("inventory_summary"))
    if int(inv.get("event_handlers", 0) or 0) != event_count:
        inv["event_handlers"] = event_count
        glance["inventory_summary"] = inv
        brief["at_a_glance"] = glance
        report["decision_brief"] = brief
        changed = True
    return changed


def _fix_form_count_fidelity(report: dict[str, Any], raw_artifacts: dict[str, Any]) -> bool:
    dossiers = _as_list(_as_dict(raw_artifacts.get("form_dossier")).get("dossiers"))
    discovered = len(dossiers)
    if discovered <= 0:
        return False
    brief = _as_dict(report.get("decision_brief"))
    glance = _as_dict(brief.get("at_a_glance"))
    inv = _as_dict(glance.get("inventory_summary"))
    current = int(inv.get("forms", 0) or 0)
    if current == discovered:
        return False
    inv["forms"] = discovered
    glance["inventory_summary"] = inv
    brief["at_a_glance"] = glance
    report["decision_brief"] = brief
    return True


def _fix_missing_artifact_refs(report: dict[str, Any]) -> bool:
    appendix = _as_dict(report.get("appendix"))
    refs = _as_dict(appendix.get("artifact_refs"))
    changed = False
    for key, value in list(refs.items()):
        if _clean(value):
            continue
        refs[key] = f"embedded://analyst_report_v2/{key}"
        changed = True
    if changed:
        appendix["artifact_refs"] = refs
        report["appendix"] = appendix
    return changed


def _apply_deterministic_qa_fixes(
    report: dict[str, Any],
    raw_artifacts: dict[str, Any],
    qa_report: dict[str, Any],
) -> list[str]:
    fixes: list[str] = []
    checks = _as_list(_as_dict(qa_report.get("structural")).get("checks"))
    for row in checks:
        check = _as_dict(row)
        cid = _clean(check.get("check_id") or check.get("id")).lower()
        result = _clean(check.get("result")).lower()
        if result not in {"fail", "warn"}:
            continue
        if cid == "cross_event_handler_reconciliation":
            if _fix_event_handler_count(report, raw_artifacts):
                fixes.append("Aligned event handler count to event-map entry count for deterministic reconciliation.")
        elif cid == "render_form_count_fidelity":
            if _fix_form_count_fidelity(report, raw_artifacts):
                fixes.append("Aligned rendered form count with discovered form dossier count.")
        elif cid == "render_open_questions_serialization":
            if _normalize_open_questions_in_report(report):
                fixes.append("Normalized open questions into structured objects.")
        elif cid == "render_artifact_refs_present":
            if _fix_missing_artifact_refs(report):
                fixes.append("Filled missing appendix artifact refs with embedded placeholders.")
        elif cid == "struct_form_ref_integrity":
            if _fix_rule_form_refs(raw_artifacts):
                fixes.append("Normalized non-form rule scope references (module/project tokens) to non-blocking placeholders.")
    return fixes


def _normalize_form_label(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if "::" in text:
        text = text.split("::", 1)[1]
    text = text.split("/")[-1]
    text = re.sub(r"\.(frm|ctl|cls|bas)$", "", text, flags=re.IGNORECASE)
    text = text.rstrip(".,;:()[]{}")
    if not text:
        return ""
    low = text.lower()
    if re.search(r"_(click|change|load|keypress|keydown|keyup|gotfocus|lostfocus|activate|deactivate)$", low):
        return ""
    if _is_non_form_reference(text):
        return ""
    if ("frm" in low) or low.startswith("form") or low in {"main", "mdiform"}:
        return text
    return ""


def _extract_rule_form_labels(rule: dict[str, Any]) -> list[str]:
    labels: list[str] = []
    scope = _as_dict(rule.get("scope"))
    candidates: list[Any] = [
        scope.get("form"),
        scope.get("form_key"),
        scope.get("component_id"),
        rule.get("form"),
    ]
    candidates.extend(_as_list(scope.get("forms")))
    candidates.extend(_as_list(scope.get("form_keys")))
    for candidate in candidates:
        normalized = _normalize_form_label(candidate)
        if normalized and normalized not in labels:
            labels.append(normalized)
    text_candidates = [
        _clean(rule.get("statement")),
        _clean(rule.get("evidence")),
    ]
    for ev in _as_list(rule.get("evidence")):
        e = _as_dict(ev)
        text_candidates.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
        text_candidates.append(_clean(_as_dict(e.get("file_span")).get("path")))
    for text in text_candidates:
        for token in re.findall(r"([A-Za-z0-9_./-]+)", text):
            normalized = _normalize_form_label(token)
            if normalized and normalized not in labels:
                labels.append(normalized)
    return labels


def _norm_form_key(value: Any) -> str:
    return _normalize_form_label(value).lower()


def _rule_signature(statement: Any) -> str:
    text = _clean(statement).lower()
    if not text:
        return ""
    text = re.sub(r"project1\s*\([^)]*\)", "project_variant", text)
    text = re.sub(r"\b(form|frm)\d+\b", "formN", text)
    text = re.sub(r"\b(br|risk|sql)[:-]?\d+\b", "idN", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _rule_theme_signature(statement: Any) -> str:
    text = _rule_signature(statement)
    if not text:
        return ""
    if (
        ("balance is recalculated" in text and ("entered amount" in text or "ui-derived source" in text))
        or ("currbalance" in text)
        or ("lblbalance.caption" in text)
        or ("ccur(" in text and "balance" in text)
        or ("computed value rule" in text and "balance" in text)
    ):
        return "balance_recalculation"
    if (
        ("recordset" in text or "connection" in text)
        and "active" in text
        and any(tok in text for tok in ("action proceeds", "workflow continues", "only when"))
    ):
        return "recordset_active_guard"
    if "pressing enter triggers" in text and "primary button" in text:
        return "enter_key_primary_action"
    return ""


def _classify_form_types(raw_artifacts: dict[str, Any]) -> dict[str, str]:
    form_types: dict[str, str] = {}
    for row in _as_list(_as_dict(raw_artifacts.get("form_dossier")).get("dossiers")):
        if not isinstance(row, dict):
            continue
        form_name = _clean(row.get("form_name"))
        purpose = _clean(row.get("purpose")).lower()
        controls = " ".join(_clean(x).lower() for x in _as_list(row.get("controls")) if _clean(x))
        form_low = form_name.lower()
        kind = "data_entry"
        if "splash" in form_low or "splash" in purpose or "progressbar" in controls:
            kind = "splash_loading"
        elif (
            form_low in {"main", "mdiform"}
            or form_low.startswith("mdi")
            or "toolbar" in controls
            or any(tok in purpose for tok in ("navigation", "menu", "routing"))
        ):
            kind = "navigation_menu"
        elif any(tok in purpose for tok in ("report", "reporting")):
            kind = "reporting"
        key = _norm_form_key(form_name)
        if key and key not in form_types:
            form_types[key] = kind
    return form_types


def _parse_saturated_signatures(qa_report: dict[str, Any]) -> tuple[set[str], set[str]]:
    signatures: set[str] = set()
    themes: set[str] = set()
    for row in _as_list(_as_dict(qa_report.get("semantic")).get("checks")):
        if not isinstance(row, dict):
            continue
        cid = _clean(row.get("check_id") or row.get("id")).lower()
        detail = _clean(row.get("detail"))
        if cid.startswith("sem_template_saturation_"):
            match = re.search(r"'(.+?)'\s+repeated", detail)
            if match:
                sig = _rule_signature(match.group(1))
                if sig:
                    signatures.add(sig)
        if cid.startswith("sem_template_theme_saturation_"):
            theme = cid.split("sem_template_theme_saturation_", 1)[-1]
            if theme:
                themes.add(theme)
    return signatures, themes


def _apply_semantic_safe_fixes(raw_artifacts: dict[str, Any], qa_report: dict[str, Any]) -> list[str]:
    catalog = _as_dict(raw_artifacts.get("business_rule_catalog"))
    rules = _as_list(catalog.get("rules"))
    if not rules:
        return []

    saturated_signatures, saturated_themes = _parse_saturated_signatures(qa_report)
    if not saturated_signatures and not saturated_themes:
        return []

    rules_norm: list[dict[str, Any]] = [row for row in rules if isinstance(row, dict)]
    sig_to_indexes: dict[str, list[int]] = {}
    theme_to_indexes: dict[str, list[int]] = {}
    form_types = _classify_form_types(raw_artifacts)
    for idx, row in enumerate(rules_norm):
        sig = _rule_signature(row.get("statement"))
        if sig:
            sig_to_indexes.setdefault(sig, []).append(idx)
        theme = _rule_theme_signature(row.get("statement"))
        if theme:
            theme_to_indexes.setdefault(theme, []).append(idx)

    remove_indexes: set[int] = set()
    fixes: list[str] = []
    finance_tokens = {"balance", "deposit", "withdraw", "transaction", "ledger"}

    def _suppress_implausible(indexes: list[int]) -> None:
        for idx in indexes:
            if idx in remove_indexes:
                continue
            row = rules_norm[idx]
            statement = _rule_signature(row.get("statement"))
            if not any(tok in statement for tok in finance_tokens):
                continue
            labels = _extract_rule_form_labels(row)
            if not labels:
                continue
            if any(form_types.get(label.lower()) in {"splash_loading", "navigation_menu"} for label in labels):
                remove_indexes.add(idx)

    # 1) Suppress implausible financial rules on splash/navigation contexts.
    for sig in saturated_signatures:
        _suppress_implausible(sig_to_indexes.get(sig, []))
    for theme in saturated_themes:
        _suppress_implausible(theme_to_indexes.get(theme, []))
    if remove_indexes:
        fixes.append(
            f"Suppressed {len(remove_indexes)} implausible repeated rule row(s) for splash/navigation contexts."
        )

    # 2) Canonicalize repeated saturated signatures into one shared entry per signature.
    for sig in saturated_signatures:
        indexes = [i for i in sig_to_indexes.get(sig, []) if i not in remove_indexes]
        if len(indexes) < 3:
            continue
        keep = indexes[0]
        occurrence_forms: list[str] = []
        for idx in indexes:
            for label in _extract_rule_form_labels(rules_norm[idx]):
                if label and label not in occurrence_forms:
                    occurrence_forms.append(label)
        canonical = rules_norm[keep]
        scope = _as_dict(canonical.get("scope"))
        if occurrence_forms:
            scope["forms"] = occurrence_forms[:40]
            scope["form"] = occurrence_forms[0]
        scope["component_id"] = "n/a"
        canonical["scope"] = scope
        canonical["occurrence_count"] = len(indexes)
        for idx in indexes[1:]:
            remove_indexes.add(idx)
        fixes.append(
            f"Canonicalized saturated rule template '{sig[:90]}' into one shared rule with {len(indexes)} occurrences."
        )

    # 3) De-duplicate same rule signature per form for any saturated theme.
    for theme in saturated_themes:
        indexes = [i for i in theme_to_indexes.get(theme, []) if i not in remove_indexes]
        seen_per_form: set[tuple[str, str]] = set()
        for idx in indexes:
            row = rules_norm[idx]
            sig = _rule_signature(row.get("statement"))
            labels = _extract_rule_form_labels(row) or ["n/a"]
            primary = labels[0].lower()
            key = (primary, sig)
            if key in seen_per_form:
                remove_indexes.add(idx)
                continue
            seen_per_form.add(key)

    # 4) Canonicalize saturated themes (pattern-agnostic across wording variants).
    for theme in saturated_themes:
        indexes = [i for i in theme_to_indexes.get(theme, []) if i not in remove_indexes]
        if len(indexes) < 5:
            continue
        keep = indexes[0]
        canonical = rules_norm[keep]
        occurrence_forms: list[str] = []
        for idx in indexes:
            for label in _extract_rule_form_labels(rules_norm[idx]):
                if label and label not in occurrence_forms:
                    occurrence_forms.append(label)
        scope = _as_dict(canonical.get("scope"))
        if occurrence_forms:
            scope["forms"] = occurrence_forms[:50]
            scope["form"] = occurrence_forms[0]
        scope["component_id"] = "n/a"
        canonical["scope"] = scope
        canonical["occurrence_count"] = len(indexes)
        canonical["canonicalized_theme"] = theme
        for idx in indexes[1:]:
            remove_indexes.add(idx)
        fixes.append(
            f"Canonicalized saturated rule theme '{theme}' into one shared rule with {len(indexes)} occurrences."
        )

    if not remove_indexes and not fixes:
        return []

    new_rules = [row for idx, row in enumerate(rules_norm) if idx not in remove_indexes]
    catalog["rules"] = new_rules
    raw_artifacts["business_rule_catalog"] = catalog
    if remove_indexes and not any("Canonicalized saturated rule template" in f for f in fixes):
        fixes.append(f"Removed {len(remove_indexes)} duplicate saturated rule row(s).")
    return fixes


def _is_probable_table_name(value: str) -> bool:
    token = _clean(value)
    if not token:
        return False
    lower = token.lower()
    if lower in {
        "con",
        "cn",
        "rs",
        "rst",
        "cmd",
        "label",
        "textbox",
        "list1",
        "list2",
        "form",
        "frm",
        "main",
    }:
        return False
    if "/" in token or "\\" in token or ":" in token:
        return False
    if lower.endswith((
        ".frm",
        ".frx",
        ".bas",
        ".cls",
        ".ctl",
        ".ctx",
        ".vbp",
        ".vbg",
        ".res",
        ".ocx",
        ".dcx",
        ".dca",
    )):
        return False
    if any(marker in lower for marker in ("command", "connection", "dataenvironment", "module", "form")):
        return False
    if re.match(r"^(cmd|txt|lbl|frm|opt|chk|cbo|dtp|btn|con|cn|rs|ado|db)[a-z0-9_]*$", lower):
        return False
    if len(lower) <= 2:
        return False
    if "." in token and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*", token) is None:
        return False
    return True


def _artifact_ref(artifact: dict[str, Any], fallback: str) -> str:
    atype = _clean(artifact.get("artifact_type"))
    version = _clean(artifact.get("artifact_version"))
    artifact_id = _clean(artifact.get("artifact_id"))
    if atype and version and artifact_id:
        return f"artifact://{atype}/{version}/{artifact_id}"
    return fallback


def _apply_common_envelope(
    artifact: dict[str, Any],
    *,
    run_id: str,
    generated_at: str,
    producer: dict[str, Any],
    context: dict[str, Any],
) -> dict[str, Any]:
    out = dict(artifact)
    atype = _clean(out.get("artifact_type")) or "artifact"
    out.setdefault("artifact_id", f"art_{atype}_{uuid4().hex[:16]}")
    out.setdefault("run_id", run_id)
    out.setdefault("generated_at", generated_at)
    out.setdefault("producer", producer)
    out.setdefault("context", context)
    return out


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
            if not _is_probable_table_name(name):
                continue
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
    event_map_entries: list[Any] | None = None,
) -> list[dict[str, Any]]:
    handler_form_hints: dict[str, str] = {}
    if isinstance(event_map_entries, list):
        for entry in event_map_entries:
            if not isinstance(entry, dict):
                continue
            container = _clean(entry.get("container"))
            handler_symbol = _clean(_as_dict(entry.get("handler")).get("symbol"))
            if not container and "::" in handler_symbol:
                container = _clean(handler_symbol.rsplit("::", 1)[0])
            handler_name = _clean(handler_symbol.rsplit("::", 1)[-1]) if handler_symbol else ""
            if container and handler_name and handler_name.lower() not in handler_form_hints:
                handler_form_hints[handler_name.lower()] = container
    for form in legacy_forms:
        if not isinstance(form, dict):
            continue
        form_name = (
            _clean(form.get("form_name"))
            or _clean(form.get("base_form_name"))
            or _clean(form.get("name"))
        )
        if not form_name:
            continue
        for handler_name in _as_list(form.get("event_handlers")):
            token = _clean(handler_name).lower()
            if token and token not in handler_form_hints:
                handler_form_hints[token] = form_name

    seen: set[str] = set()
    flows: list[dict[str, Any]] = []
    flow_idx = 1
    for row in ui_event_map[:40]:
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler"))
        form = _clean(row.get("form"))
        if not form and handler:
            form = _clean(handler_form_hints.get(handler.lower()))
        control = _clean(row.get("control"))
        event = _clean(row.get("event"))
        handler_key = handler.lower()
        form_key = form.lower()
        if handler_key in {"frm", "dd", "dd1"} and form_key in {"shared_module", "module", ""}:
            continue
        if not form and not handler and not control:
            continue
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
            feature_text = _bdd_feature_text(feat).lower()
            if not fid or not feature_text:
                continue
            if (
                (form and form.lower() in feature_text)
                or (handler and handler.lower() in feature_text)
                or (control and control.lower() in feature_text)
            ):
                linked.append(fid)
        flow_form = form or "legacy_form"
        flow_name = ""
        if form and handler:
            flow_name = f"{form} primary flow"
        elif form and event:
            flow_name = f"{form} primary flow"
        elif form and control:
            flow_name = f"{form} primary flow"
        elif form:
            flow_name = f"{form} workflow"
        elif handler and event:
            flow_name = f"{handler} {event} flow"
        elif handler:
            flow_name = handler
        elif control and event:
            flow_name = f"{control} {event} flow"
        else:
            flow_name = "Legacy workflow"
        entry_symbol = handler or event or control or "legacy_event"
        flows.append(
            {
                "id": f"GF-{flow_idx:03d}",
                "name": flow_name,
                "entrypoint": f"{flow_form}::{entry_symbol}",
                "tables_touched": touched[:6],
                "expected_outcome": "Behavior matches legacy flow with equivalent side effects.",
                "bdd_scenario_ids": linked[:4],
            }
        )
        flow_idx += 1
        if len(flows) >= 10:
            break
    if flows:
        return flows
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
    text = _bdd_feature_text(feature).lower()
    if not text:
        return True
    return any(marker in text for marker in GENERIC_BDD_MARKERS)


def _bdd_feature_text(feature: Any) -> str:
    if not isinstance(feature, dict):
        return ""
    chunks: list[str] = []
    top_level = _clean(feature.get("gherkin"))
    if top_level:
        chunks.append(top_level)
    for scenario in _as_list(feature.get("scenarios")):
        if not isinstance(scenario, dict):
            continue
        chunks.append(_clean(scenario.get("name")))
        gherkin = scenario.get("gherkin")
        if isinstance(gherkin, list):
            chunks.extend(_clean(line) for line in gherkin if _clean(line))
        else:
            text = _clean(gherkin)
            if text:
                chunks.append(text)
    return "\n".join(chunk for chunk in chunks if chunk)


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


def _resolve_repo_hint(context_ref: dict[str, Any], source_profile: dict[str, Any]) -> str:
    context_repo_url = _clean(context_ref.get("repo_url"))
    if context_repo_url:
        return context_repo_url
    source_repo = _clean(source_profile.get("repo"))
    context_repo = _clean(context_ref.get("repo"))
    if context_repo.startswith(("http://", "https://", "git@")):
        return context_repo
    if source_repo:
        return source_repo
    return context_repo


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
    if (
        "+'" in lower
        or "'+" in lower
        or '"+' in lower
        or '" &' in lower
        or "& '" in lower
        or re.search(r"(?i)\b(where|and|or)\b[^;\n]*=\s*(?:'|$)", lower)
        or (lower.count("'") % 2 == 1)
    ):
        flags.append("string_concatenation")
        flags.append("possible_injection")
    if lower.startswith("delete *"):
        flags.append("jet_delete_wildcard")
    if "execute(" in lower or "open " in lower and " where " not in lower and "update " in lower:
        flags.append("dynamic_sql")
    if lower.startswith("update ") and " where " not in lower:
        flags.append("missing_where_clause")
    if lower.startswith("delete ") and " where " not in lower:
        flags.append("missing_where_clause")
    if re.search(r"(?i)\b(pass|password|pwd)\b", lower):
        flags.append("sensitive_credential_query")
    unique: list[str] = []
    seen: set[str] = set()
    for flag in flags:
        if flag in seen:
            continue
        seen.add(flag)
        unique.append(flag)
    return unique


def _to_snake_case(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    text = text.replace("-", "_").replace(" ", "_")
    text = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", text)
    text = re.sub(r"[^a-zA-Z0-9_]", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text.lower()


def _guess_column_type(column_name: str) -> str:
    token = _clean(column_name).lower()
    if not token:
        return "text"
    if token.endswith("_id") or token in {"id", "customerid", "accountid", "transactionid"}:
        return "integer"
    if any(tok in token for tok in ("date", "time", "timestamp", "joined", "created", "updated")):
        return "timestamp"
    if any(tok in token for tok in ("amount", "balance", "rate", "interest", "total", "price", "debit", "credit")):
        return "numeric(18,2)"
    if any(tok in token for tok in ("is_", "_flag", "enabled", "active", "deleted")):
        return "boolean"
    if any(tok in token for tok in ("count", "qty", "quantity", "number", "no", "code")):
        return "integer"
    if any(tok in token for tok in ("name", "address", "email", "phone", "status", "type", "description", "narration", "mode")):
        return "varchar(255)"
    return "text"


def _column_business_meaning(column_name: str) -> str:
    token = _to_snake_case(column_name)
    if not token:
        return "Business meaning requires verification."
    if token.endswith("_id") or token == "id":
        return "Identifier key used to reference a business entity."
    mapping = {
        "customer_id": "Customer identifier used to associate records with a customer.",
        "account_id": "Account identifier used to group account-level activity.",
        "transaction_id": "Transaction identifier for unique ledger records.",
        "amount": "Monetary amount captured for debit/credit operations.",
        "balance": "Running or current balance used for account state.",
        "date": "Business date captured for transaction timing.",
        "dated": "Business date captured for transaction timing.",
        "created_at": "Audit timestamp for record creation.",
        "updated_at": "Audit timestamp for latest record update.",
        "mode": "Transaction mode indicator (for example cash/cheque).",
        "status": "Record lifecycle or approval state.",
        "narration": "Free-text description attached to transaction activity.",
    }
    if token in mapping:
        return mapping[token]
    if "name" in token:
        return "Human-readable name used in UI and reports."
    if "date" in token or "time" in token:
        return "Date/time field used in filtering and reconciliation."
    if "amount" in token or "balance" in token or "rate" in token:
        return "Financial value used in calculation and posting logic."
    return "Business meaning inferred from query usage; verify with SME."


def _extract_column_tokens_from_sql(raw_sql: str, kind: str) -> list[str]:
    text = _clean(raw_sql)
    if not text:
        return []
    lower_kind = _clean(kind).lower()
    cols: list[str] = []
    if lower_kind == "select":
        m = re.search(r"(?is)\bselect\b(.*?)\bfrom\b", text)
        if m:
            segment = _clean(m.group(1))
            for part in segment.split(","):
                token = _clean(part)
                if not token or token == "*" or token.endswith(".*"):
                    continue
                token = re.sub(r"(?is)\bas\b\s+[a-zA-Z_][\w$]*", "", token).strip()
                token = token.split(".")[-1].strip(" []`\"")
                if token and token not in cols:
                    cols.append(token)
    elif lower_kind == "insert":
        m = re.search(r"(?is)\binsert\s+into\b\s+[a-zA-Z_][\w.]*\s*\((.*?)\)", text)
        if m:
            for part in _clean(m.group(1)).split(","):
                token = _clean(part).strip(" []`\"")
                if token and token not in cols:
                    cols.append(token)
    elif lower_kind == "update":
        m = re.search(r"(?is)\bset\b(.*?)(?:\bwhere\b|$)", text)
        if m:
            assignments = _clean(m.group(1)).split(",")
            for part in assignments:
                token = _clean(part.split("=", 1)[0]).split(".")[-1].strip(" []`\"")
                if token and token not in cols:
                    cols.append(token)
    else:
        # DELETE often references key columns in WHERE clause.
        where_cols = re.findall(r"(?is)\bwhere\b(.*)$", text)
        if where_cols:
            for wc in where_cols:
                for token in re.findall(r"([a-zA-Z_][\w$]*)\s*=", wc):
                    col = _clean(token).split(".")[-1]
                    if col and col not in cols:
                        cols.append(col)
    return cols[:60]


_ACCESS_TYPE_MAP: dict[str, str] = {
    "text": "varchar(255)",
    "memo": "text",
    "byte": "tinyint",
    "integer": "smallint",
    "long integer": "integer",
    "single": "real",
    "double": "double precision",
    "currency": "decimal(19,4)",
    "date/time": "timestamp",
    "boolean": "boolean",
    "yes/no": "boolean",
    "ole object": "blob",
    "autonumber": "integer",
}


def _normalize_sql_identifier(value: Any) -> str:
    token = _clean(value)
    if not token:
        return ""
    token = token.strip("[]`\"")
    token = token.split(".")[-1]
    return token.strip("[]`\"")


def _split_sql_csv(segment: str) -> list[str]:
    text = _clean(segment)
    if not text:
        return []
    out: list[str] = []
    buff: list[str] = []
    depth = 0
    in_single = False
    in_double = False
    for ch in text:
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        if not in_single and not in_double:
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth = max(0, depth - 1)
            elif ch == "," and depth == 0:
                token = _clean("".join(buff))
                if token:
                    out.append(token)
                buff = []
                continue
        buff.append(ch)
    token = _clean("".join(buff))
    if token:
        out.append(token)
    return out


def _normalize_access_data_type(raw_type: str) -> str:
    token = _clean(raw_type).lower()
    if not token:
        return "text"
    token = re.sub(r"\s+", " ", token)
    if token in _ACCESS_TYPE_MAP:
        return _ACCESS_TYPE_MAP[token]
    if token.startswith("varchar") or token.startswith("char"):
        return token
    if token.startswith("decimal") or token.startswith("numeric"):
        return token
    if token.startswith("datetime") or token.startswith("timestamp"):
        return "timestamp"
    if token.startswith("int"):
        return "integer"
    return token


def _parse_track_a_schema(database_schema_text: str) -> dict[str, Any]:
    text = str(database_schema_text or "").strip()
    if not text:
        return {"route": "code_only", "tables": [], "relationships": []}

    route = "ddl_text"
    lower_text = text.lower()
    if "parsed with: mdbtools" in lower_text or "source database: microsoft access" in lower_text:
        route = "mdb_direct_read"

    table_map: dict[str, dict[str, Any]] = {}
    rel_rows: list[dict[str, Any]] = []
    rel_seen: set[str] = set()

    def ensure_table(name: str) -> dict[str, Any]:
        tname = _normalize_sql_identifier(name)
        key = tname.lower()
        if key not in table_map:
            table_map[key] = {
                "name": tname,
                "columns_map": {},
                "primary_key_candidates": [],
            }
        return table_map[key]

    def add_rel(from_table: str, from_col: str, to_table: str, to_col: str, confidence: float = 0.95) -> None:
        ft = _normalize_sql_identifier(from_table)
        fc = _normalize_sql_identifier(from_col)
        tt = _normalize_sql_identifier(to_table)
        tc = _normalize_sql_identifier(to_col)
        if not (ft and fc and tt and tc):
            return
        key = f"{ft.lower()}|{fc.lower()}|{tt.lower()}|{tc.lower()}"
        if key in rel_seen:
            return
        rel_seen.add(key)
        rel_rows.append(
            {
                "relationship_id": f"src_rel:{len(rel_rows) + 1}",
                "kind": "enforced_fk",
                "from_table": ft,
                "from_column": fc,
                "to_table": tt,
                "to_column": tc,
                "confidence": round(confidence, 2),
                "evidence_sql_ids": [],
                "enforced_in_db": True,
            }
        )

    for m in re.finditer(r"(?is)\bcreate\s+table\b\s+([^\s(]+)\s*\((.*?)\)\s*;", text):
        table_name = _normalize_sql_identifier(m.group(1))
        body = m.group(2)
        if not table_name:
            continue
        trow = ensure_table(table_name)
        parts = _split_sql_csv(body)
        for part in parts:
            line = _clean(part).rstrip(",")
            if not line:
                continue
            lline = line.lower()
            if "foreign key" in lline and "references" in lline:
                fk_m = re.search(
                    r"(?is)foreign\s+key\s*\(\s*([^\)]+)\s*\)\s*references\s+([^\s(]+)\s*\(\s*([^\)]+)\s*\)",
                    line,
                )
                if fk_m:
                    add_rel(table_name, fk_m.group(1), fk_m.group(2), fk_m.group(3))
                continue
            if "primary key" in lline and ("constraint" in lline or line.strip().lower().startswith("primary key")):
                pk_m = re.search(r"(?is)primary\s+key\s*\((.*?)\)", line)
                if pk_m:
                    for col_tok in _split_sql_csv(pk_m.group(1)):
                        col_name = _normalize_sql_identifier(col_tok)
                        if not col_name:
                            continue
                        pk = _as_list(trow.get("primary_key_candidates"))
                        if col_name not in pk:
                            pk.append(col_name)
                        trow["primary_key_candidates"] = pk[:12]
                continue

            col_m = re.match(r"(?is)^\s*([^\s]+)\s+(.+)$", line)
            if not col_m:
                continue
            col_name = _normalize_sql_identifier(col_m.group(1))
            remainder = _clean(col_m.group(2))
            if not col_name or col_name.lower() in {"constraint", "primary", "foreign", "unique", "check"}:
                continue

            type_m = re.match(
                r"(?is)^(.+?)(?:\s+not\s+null|\s+null|\s+default\b|\s+constraint\b|\s+primary\s+key|\s+references\b|$)",
                remainder,
            )
            raw_type = _clean(type_m.group(1) if type_m else remainder)
            inferred_type = _normalize_access_data_type(raw_type)
            nullable = "not null" not in remainder.lower()
            is_pk_inline = "primary key" in remainder.lower()
            ref_m = re.search(r"(?is)\breferences\s+([^\s(]+)\s*\(\s*([^\)]+)\s*\)", remainder)
            ref_value = ""
            if ref_m:
                ref_value = f"{_normalize_sql_identifier(ref_m.group(1))}.{_normalize_sql_identifier(ref_m.group(2))}"
                add_rel(table_name, col_name, ref_m.group(1), ref_m.group(2))
            col_key = col_name.lower()
            cmap = _as_dict(trow.get("columns_map"))
            cmap[col_key] = {
                "name": col_name,
                "inferred_type": inferred_type or _guess_column_type(col_name),
                "nullable": bool(nullable),
                "is_primary_key": bool(is_pk_inline),
                "is_foreign_key": bool(ref_value),
                "fk_references": ref_value or None,
                "inferred_from": "track_a_mdb" if route == "mdb_direct_read" else "track_a_ddl",
            }
            if is_pk_inline:
                pk = _as_list(trow.get("primary_key_candidates"))
                if col_name not in pk:
                    pk.append(col_name)
                trow["primary_key_candidates"] = pk[:12]
            trow["columns_map"] = cmap

    for m in re.finditer(
        r"(?is)\balter\s+table\b\s+([^\s]+)\s+add\s+constraint\s+[^\s]+\s+foreign\s+key\s*\(\s*([^\)]+)\)\s+references\s+([^\s(]+)\s*\(\s*([^\)]+)\s*\)",
        text,
    ):
        add_rel(m.group(1), m.group(2), m.group(3), m.group(4))

    tables_out: list[dict[str, Any]] = []
    for table in sorted(table_map.values(), key=lambda x: _clean(x.get("name")).lower()):
        columns_map = _as_dict(table.get("columns_map"))
        table["columns"] = [
            _as_dict(columns_map.get(k))
            for k in sorted(columns_map.keys())
            if isinstance(columns_map.get(k), dict)
        ]
        table.pop("columns_map", None)
        tables_out.append(table)

    return {"route": route, "tables": tables_out, "relationships": rel_rows}


def _coalesce_sql_catalog_rows(sql_catalog_rows: list[Any]) -> list[Any]:
    if not isinstance(sql_catalog_rows, list) or not sql_catalog_rows:
        return sql_catalog_rows
    out: list[dict[str, Any]] = []
    buffer: dict[str, Any] | None = None
    ui_like = re.compile(r"(?i)^(please|error|warning|click|save|cancel|ok|yes|no)\b")
    start_pat = re.compile(r"(?i)\b(select|insert\s+into|update\s+\w|delete\s+from|delete\s+\*)\b")
    for row in sql_catalog_rows:
        if isinstance(row, dict):
            raw = _clean(row.get("raw") or row.get("sql") or row.get("statement"))
            tables = [_clean(x) for x in _as_list(row.get("tables")) if _clean(x)]
            columns = [_clean(x) for x in _as_list(row.get("columns")) if _clean(x)]
            base = dict(row)
        else:
            raw = _clean(row)
            tables = []
            columns = []
            base = {"raw": raw}
        if not raw:
            continue
        raw_clean = raw.strip()
        looks_start = bool(start_pat.search(raw_clean))
        looks_fragment = bool(re.fullmatch(r"(?i)(select|from|where|and|or|insert|into|update|set|delete|\&delete|\"delete\")", raw_clean))
        looks_ui = bool(ui_like.search(raw_clean))

        if looks_start:
            if buffer:
                out.append(buffer)
            buffer = {"raw": raw_clean, "tables": tables[:], "columns": columns[:]}
            continue

        if buffer and (looks_fragment or (len(raw_clean) <= 80 and not looks_ui)):
            buffer["raw"] = _clean(f"{_clean(buffer.get('raw'))} {raw_clean}")
            merged_tables = [_clean(x) for x in _as_list(buffer.get("tables")) if _clean(x)]
            for t in tables:
                if t not in merged_tables:
                    merged_tables.append(t)
            merged_cols = [_clean(x) for x in _as_list(buffer.get("columns")) if _clean(x)]
            for c in columns:
                if c not in merged_cols:
                    merged_cols.append(c)
            buffer["tables"] = merged_tables
            buffer["columns"] = merged_cols
            continue

        if buffer:
            out.append(buffer)
            buffer = None
        out.append(base if isinstance(row, dict) else {"raw": raw_clean, "tables": [], "columns": []})
    if buffer:
        out.append(buffer)
    return out


def _build_source_schema_model(
    *,
    metadata_common: dict[str, Any],
    sql_statements: list[dict[str, Any]],
    database_tables: list[str],
    database_schema_text: str = "",
) -> dict[str, Any]:
    table_map: dict[str, dict[str, Any]] = {}
    relationships: list[dict[str, Any]] = []
    rel_seen: set[str] = set()
    unknown_query_count = 0
    track_b_tables_seen: set[str] = set()
    track_a = _parse_track_a_schema(database_schema_text)

    def _ensure_table(table_name: str) -> dict[str, Any]:
        key = _clean(table_name).lower()
        if key not in table_map:
            table_map[key] = {
                "table_id": f"src_tbl:{len(table_map) + 1}",
                "name": _clean(table_name),
                "columns_map": {},
                "primary_key_candidates": [],
                "read_query_count": 0,
                "write_query_count": 0,
                "risk_flags": set(),
                "evidence_sql_ids": set(),
            }
        return table_map[key]

    # Track A: schema-first extraction from DDL/mdbtools output.
    for t in _as_list(track_a.get("tables")):
        if not isinstance(t, dict):
            continue
        tname = _clean(t.get("name"))
        if not tname:
            continue
        trow = _ensure_table(tname)
        pk_seed = [_clean(x) for x in _as_list(t.get("primary_key_candidates")) if _clean(x)]
        if pk_seed:
            trow["primary_key_candidates"] = sorted(set(_as_list(trow.get("primary_key_candidates")) + pk_seed))[:12]
        cmap = _as_dict(trow.get("columns_map"))
        for col in _as_list(t.get("columns")):
            if not isinstance(col, dict):
                continue
            cname = _clean(col.get("name"))
            if not cname:
                continue
            ckey = cname.lower()
            cmap[ckey] = {
                "column_id": f"{trow['table_id']}:col:{len(cmap) + 1}",
                "name": cname,
                "inferred_type": _clean(col.get("inferred_type")) or _guess_column_type(cname),
                "nullable": bool(col.get("nullable", True)),
                "evidence_sql_ids": [],
                "confidence": 0.92,
                "business_meaning": _column_business_meaning(cname),
                "inferred_from": _clean(col.get("inferred_from")) or ("track_a_mdb" if _clean(track_a.get("route")) == "mdb_direct_read" else "track_a_ddl"),
                "is_primary_key": bool(col.get("is_primary_key", False)),
                "is_foreign_key": bool(col.get("is_foreign_key", False)),
                "fk_references": _clean(col.get("fk_references")) or None,
            }
        trow["columns_map"] = cmap

    for rel in _as_list(track_a.get("relationships")):
        if not isinstance(rel, dict):
            continue
        ft = _clean(rel.get("from_table"))
        fc = _clean(rel.get("from_column"))
        tt = _clean(rel.get("to_table"))
        tc = _clean(rel.get("to_column"))
        if not (ft and fc and tt and tc):
            continue
        rel_key = f"{ft.lower()}|{fc.lower()}|{tt.lower()}|{tc.lower()}"
        if rel_key in rel_seen:
            continue
        rel_seen.add(rel_key)
        relationships.append(
            {
                "relationship_id": _clean(rel.get("relationship_id")) or f"src_rel:{len(relationships) + 1}",
                "kind": _clean(rel.get("kind")) or "enforced_fk",
                "from_table": ft,
                "from_column": fc,
                "to_table": tt,
                "to_column": tc,
                "confidence": round(_to_float(rel.get("confidence"), 0.95), 2),
                "evidence_sql_ids": [_clean(x) for x in _as_list(rel.get("evidence_sql_ids")) if _clean(x)][:30],
                "enforced_in_db": bool(rel.get("enforced_in_db", True)),
            }
        )

    for table in database_tables:
        t = _clean(table)
        if t:
            _ensure_table(t)

    for stmt in sql_statements[:5000]:
        if not isinstance(stmt, dict):
            continue
        sql_id = _clean(stmt.get("sql_id"))
        raw = _clean(stmt.get("raw"))
        kind = _clean(stmt.get("kind")).lower() or _parse_sql_kind(raw)
        tables = [_clean(x) for x in _as_list(stmt.get("tables")) if _clean(x)]
        columns = [_clean(x) for x in _as_list(stmt.get("columns")) if _clean(x)]
        if not columns:
            columns = _extract_column_tokens_from_sql(raw, kind)

        if not tables:
            unknown_query_count += 1
            continue

        op_is_write = kind in {"insert", "update", "delete", "ddl"}
        for table in tables:
            trow = _ensure_table(table)
            track_b_tables_seen.add(_clean(table).lower())
            if op_is_write:
                trow["write_query_count"] += 1
            else:
                trow["read_query_count"] += 1
            if sql_id:
                trow["evidence_sql_ids"].add(sql_id)
            for flag in _as_list(stmt.get("risk_flags")):
                f = _clean(flag)
                if f:
                    trow["risk_flags"].add(f)
            for col in columns:
                cc = _clean(col).split(".")[-1].strip(" []`\"")
                if not cc or cc == "*":
                    continue
                ckey = cc.lower()
                cmap = _as_dict(trow.get("columns_map"))
                if ckey not in cmap:
                    cmap[ckey] = {
                        "column_id": f"{trow['table_id']}:col:{len(cmap) + 1}",
                        "name": cc,
                        "inferred_type": _guess_column_type(cc),
                        "nullable": True,
                        "evidence_sql_ids": [],
                        "confidence": 0.66,
                        "business_meaning": _column_business_meaning(cc),
                    }
                col_row = _as_dict(cmap.get(ckey))
                evidence = _as_list(col_row.get("evidence_sql_ids"))
                if sql_id and sql_id not in evidence:
                    evidence.append(sql_id)
                col_row["evidence_sql_ids"] = evidence[:30]
                col_row["inferred_from"] = _clean(col_row.get("inferred_from")) or "track_b_sql"
                col_row["confidence"] = max(_to_float(col_row.get("confidence"), 0.66), 0.74)
                if cc.lower() in {"id", "customerid", "accountid", "transactionid"} or cc.lower().endswith("_id"):
                    pk = _as_list(trow.get("primary_key_candidates"))
                    if cc not in pk:
                        pk.append(cc)
                    trow["primary_key_candidates"] = pk[:8]
                    col_row["nullable"] = False
                    col_row["confidence"] = max(_to_float(col_row.get("confidence"), 0.66), 0.78)
                cmap[ckey] = col_row
                trow["columns_map"] = cmap

        # Join-based relationship inference.
        alias_map: dict[str, str] = {}
        for tbl, alias in re.findall(r"(?is)\b(?:from|join)\s+([a-zA-Z_][\w.]*)\s+(?:as\s+)?([a-zA-Z_][\w]*)", raw):
            alias_map[_clean(alias).lower()] = _clean(tbl)
        for left_alias, left_col, right_alias, right_col in re.findall(
            r"(?is)\b([a-zA-Z_][\w]*)\.([a-zA-Z_][\w$]*)\s*=\s*([a-zA-Z_][\w]*)\.([a-zA-Z_][\w$]*)",
            raw,
        ):
            lt = alias_map.get(_clean(left_alias).lower(), _clean(left_alias))
            rt = alias_map.get(_clean(right_alias).lower(), _clean(right_alias))
            if not _clean(lt) or not _clean(rt):
                continue
            rel_key = f"{lt.lower()}|{_clean(left_col).lower()}|{rt.lower()}|{_clean(right_col).lower()}"
            if rel_key in rel_seen:
                continue
            rel_seen.add(rel_key)
            confidence = 0.86 if ("id" in _clean(left_col).lower() or "id" in _clean(right_col).lower()) else 0.68
            relationships.append(
                {
                    "relationship_id": f"src_rel:{len(relationships) + 1}",
                    "kind": "soft_fk",
                    "from_table": _clean(lt),
                    "from_column": _clean(left_col),
                    "to_table": _clean(rt),
                    "to_column": _clean(right_col),
                    "confidence": round(confidence, 2),
                    "evidence_sql_ids": [sql_id] if sql_id else [],
                }
            )

    tables_out: list[dict[str, Any]] = []
    total_columns = 0
    for table_row in sorted(table_map.values(), key=lambda x: _clean(x.get("name")).lower()):
        columns_map = _as_dict(table_row.pop("columns_map"))
        columns = sorted(columns_map.values(), key=lambda x: _clean(_as_dict(x).get("name")).lower())
        total_columns += len(columns)
        table_row["columns"] = columns[:500]
        table_row["evidence_sql_ids"] = sorted(_as_list(table_row.get("evidence_sql_ids")) or list(table_row.get("evidence_sql_ids", set())))[:80]
        table_row["risk_flags"] = sorted(_as_list(table_row.get("risk_flags")) or list(table_row.get("risk_flags", set())))[:40]
        tables_out.append(table_row)

    return {
        "artifact_type": "source_schema_model",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "extraction_tracks": {
            "track_a": {
                "enabled": bool(_clean(database_schema_text)),
                "route": _clean(track_a.get("route")) or "code_only",
                "table_count": len(_as_list(track_a.get("tables"))),
                "relationship_count": len(_as_list(track_a.get("relationships"))),
            },
            "track_b": {
                "enabled": True,
                "table_count": len(track_b_tables_seen),
                "query_count": len(sql_statements),
            },
        },
        "summary": {
            "tables": len(tables_out),
            "columns": total_columns,
            "relationships": len(relationships),
            "unknown_query_count": int(unknown_query_count),
        },
        "tables": tables_out[:800],
        "relationships": relationships[:1200],
    }


def _build_source_query_catalog(
    *,
    metadata_common: dict[str, Any],
    sql_statements: list[dict[str, Any]],
    sql_map_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    usage_by_sql: dict[str, list[dict[str, Any]]] = {}
    for row in sql_map_entries[:6000]:
        if not isinstance(row, dict):
            continue
        sql_id = _clean(row.get("sql_id"))
        if not sql_id:
            continue
        usage_by_sql.setdefault(sql_id, []).append(
            {
                "form": _clean(row.get("form")),
                "procedure": _clean(row.get("procedure")),
                "operation": _clean(row.get("operation")),
            }
        )
    queries: list[dict[str, Any]] = []
    for stmt in sql_statements[:5000]:
        if not isinstance(stmt, dict):
            continue
        sql_id = _clean(stmt.get("sql_id"))
        queries.append(
            {
                "query_id": sql_id or f"q:{len(queries) + 1}",
                "kind": _clean(stmt.get("kind")) or _parse_sql_kind(_clean(stmt.get("raw"))),
                "raw": _clean(stmt.get("raw")),
                "normalized": _clean(stmt.get("normalized")),
                "tables": [_clean(x) for x in _as_list(stmt.get("tables")) if _clean(x)],
                "columns": [_clean(x) for x in _as_list(stmt.get("columns")) if _clean(x)],
                "risk_flags": [_clean(x) for x in _as_list(stmt.get("risk_flags")) if _clean(x)],
                "usage_sites": usage_by_sql.get(sql_id, [])[:20],
            }
        )
    return {
        "artifact_type": "source_query_catalog",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "queries": queries[:6000],
    }


def _build_source_relationship_candidates(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
) -> dict[str, Any]:
    relationships = [_as_dict(x) for x in _as_list(source_schema_model.get("relationships")) if isinstance(x, dict)]
    table_rows = [_as_dict(x) for x in _as_list(source_schema_model.get("tables")) if isinstance(x, dict)]
    col_index: dict[str, set[str]] = {}
    for table in table_rows:
        tname = _clean(table.get("name"))
        cols = {_clean(_as_dict(c).get("name")).lower() for c in _as_list(table.get("columns")) if _clean(_as_dict(c).get("name"))}
        if tname and cols:
            col_index[tname] = cols

    # Add heuristic candidates for shared *id columns across tables.
    seen = {
        f"{_clean(r.get('from_table')).lower()}|{_clean(r.get('from_column')).lower()}|{_clean(r.get('to_table')).lower()}|{_clean(r.get('to_column')).lower()}"
        for r in relationships
    }
    table_names = sorted(col_index.keys())
    for left in table_names:
        for right in table_names:
            if left == right:
                continue
            shared_id_cols = [c for c in col_index[left].intersection(col_index[right]) if c.endswith("id") or c.endswith("_id")]
            for col in shared_id_cols[:4]:
                key = f"{left.lower()}|{col}|{right.lower()}|{col}"
                if key in seen:
                    continue
                seen.add(key)
                relationships.append(
                    {
                        "relationship_id": f"src_rel:{len(relationships) + 1}",
                        "kind": "candidate_fk",
                        "from_table": left,
                        "from_column": col,
                        "to_table": right,
                        "to_column": col,
                        "confidence": 0.58,
                        "evidence_sql_ids": [],
                    }
                )
    return {
        "artifact_type": "source_relationship_candidates",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "candidates": relationships[:2000],
    }


def _build_source_data_dictionary(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for table in _as_list(source_schema_model.get("tables")):
        if not isinstance(table, dict):
            continue
        tname = _clean(table.get("name"))
        for col in _as_list(table.get("columns")):
            if not isinstance(col, dict):
                continue
            cname = _clean(col.get("name"))
            if not tname or not cname:
                continue
            rows.append(
                {
                    "table": tname,
                    "column": cname,
                    "inferred_type": _clean(col.get("inferred_type")) or "text",
                    "business_meaning": _clean(col.get("business_meaning")) or _column_business_meaning(cname),
                    "evidence_sql_ids": [_clean(x) for x in _as_list(col.get("evidence_sql_ids")) if _clean(x)][:20],
                    "confidence": round(_to_float(col.get("confidence"), 0.6), 2),
                    "verification_required": _to_float(col.get("confidence"), 0.6) < 0.7,
                }
            )
    return {
        "artifact_type": "source_data_dictionary",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "rows": rows[:12000],
    }


def _build_source_erd(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    source_relationship_candidates: dict[str, Any] | None = None,
) -> dict[str, Any]:
    tables = [_as_dict(x) for x in _as_list(source_schema_model.get("tables")) if isinstance(x, dict)]
    base_relationships = [_as_dict(x) for x in _as_list(source_schema_model.get("relationships")) if isinstance(x, dict)]
    candidate_relationships = [
        _as_dict(x)
        for x in _as_list(_as_dict(source_relationship_candidates).get("candidates"))
        if isinstance(x, dict)
    ]
    relationships = base_relationships if base_relationships else candidate_relationships
    lines: list[str] = ["erDiagram"]
    for table in tables[:600]:
        tname = _mermaid_safe_token(table.get("name"), default="table")
        if not tname:
            continue
        lines.append(f"    {tname} {{")
        for col in _as_list(table.get("columns"))[:160]:
            crow = _as_dict(col)
            cname = _mermaid_safe_token(crow.get("name"), default="column")
            if not cname:
                continue
            ctype = _mermaid_safe_token(crow.get("inferred_type"), default="text")
            lines.append(f"        {ctype} {cname}")
        lines.append("    }")
    for rel in relationships[:1600]:
        ft = _mermaid_safe_token(rel.get("from_table"), default="table")
        fc = _mermaid_safe_label(rel.get("from_column"))
        tt = _mermaid_safe_token(rel.get("to_table"), default="table")
        tc = _mermaid_safe_label(rel.get("to_column"))
        if not (ft and fc and tt and tc):
            continue
        lines.append(f"    {ft} ||--o{{ {tt} : \"{fc} -> {tc}\"")
    return {
        "artifact_type": "source_erd",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "format": "mermaid",
        "mermaid": "\n".join(lines),
    }


def _build_source_data_dictionary_markdown(
    *,
    metadata_common: dict[str, Any],
    source_data_dictionary: dict[str, Any],
    source_schema_model: dict[str, Any],
) -> dict[str, Any]:
    rows = [_as_dict(x) for x in _as_list(source_data_dictionary.get("rows")) if isinstance(x, dict)]
    by_table: dict[str, list[dict[str, Any]]] = {}
    for row in rows:
        t = _clean(row.get("table"))
        if not t:
            continue
        by_table.setdefault(t, []).append(row)

    rel_refs: dict[str, list[str]] = {}
    for rel in _as_list(source_schema_model.get("relationships")):
        if not isinstance(rel, dict):
            continue
        ft = _clean(rel.get("from_table"))
        fc = _clean(rel.get("from_column"))
        tt = _clean(rel.get("to_table"))
        tc = _clean(rel.get("to_column"))
        if ft and fc and tt and tc:
            rel_refs.setdefault(f"{ft}.{fc}".lower(), []).append(f"{tt}.{tc}")

    lines: list[str] = [
        "# Source Schema - Data Dictionary",
        "",
        "_Generated by Synthetix SourceSchemaAgent_",
        "",
        f"**Total tables:** {len(by_table)}  ",
        f"**Total columns:** {len(rows)}",
        "",
        "---",
        "",
    ]
    for table_name in sorted(by_table.keys(), key=lambda x: x.lower()):
        lines.append(f"## {table_name}")
        lines.append("")
        lines.append("| Column | Type | FK Ref | Confidence | Access Evidence | Meaning |")
        lines.append("|---|---|---|---:|---|---|")
        table_rows = sorted(by_table[table_name], key=lambda x: _clean(x.get("column")).lower())
        for row in table_rows:
            col = _clean(row.get("column"))
            ctype = _clean(row.get("inferred_type")) or "text"
            key = f"{table_name}.{col}".lower()
            fk_ref = ", ".join(rel_refs.get(key, [])[:3]) or "n/a"
            conf = round(_to_float(row.get("confidence"), 0.0), 2)
            evidence = ", ".join([_clean(x) for x in _as_list(row.get("evidence_sql_ids")) if _clean(x)][:4]) or "n/a"
            meaning = _clean(row.get("business_meaning")) or "n/a"
            lines.append(
                f"| {col} | {ctype} | {fk_ref} | {conf} | {evidence} | {meaning} |"
            )
        lines.append("")
    return {
        "artifact_type": "source_data_dictionary_markdown",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "markdown": "\n".join(lines).strip() + "\n",
    }


def _build_source_hotspot_report(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    source_query_catalog: dict[str, Any],
) -> dict[str, Any]:
    query_rows = [_as_dict(x) for x in _as_list(source_query_catalog.get("queries")) if isinstance(x, dict)]
    risk_by_table: dict[str, int] = {}
    for q in query_rows:
        flags = [_clean(x) for x in _as_list(q.get("risk_flags")) if _clean(x)]
        if not flags:
            continue
        for table in _as_list(q.get("tables")):
            t = _clean(table)
            if not t:
                continue
            risk_by_table[t] = int(risk_by_table.get(t, 0) or 0) + len(flags)

    hotspots: list[dict[str, Any]] = []
    for table in _as_list(source_schema_model.get("tables")):
        if not isinstance(table, dict):
            continue
        tname = _clean(table.get("name"))
        reads = int(table.get("read_query_count", 0) or 0)
        writes = int(table.get("write_query_count", 0) or 0)
        risks = int(risk_by_table.get(tname, 0) or 0)
        score = (reads * 1.0) + (writes * 1.4) + (risks * 1.8)
        hotspots.append(
            {
                "table": tname,
                "reads": reads,
                "writes": writes,
                "risk_signals": risks,
                "hotspot_score": round(score, 2),
                "blast_radius": "high" if score >= 14 else ("medium" if score >= 6 else "low"),
            }
        )
    hotspots.sort(key=lambda x: (_to_float(x.get("hotspot_score"), 0.0), int(x.get("risk_signals", 0))), reverse=True)
    return {
        "artifact_type": "source_hotspot_report",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "hotspots": hotspots[:300],
    }


def _build_source_db_profile(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    source_query_catalog: dict[str, Any],
    source_relationship_candidates: dict[str, Any],
    source_hotspot_report: dict[str, Any],
) -> dict[str, Any]:
    queries = _as_list(source_query_catalog.get("queries"))
    kinds: dict[str, int] = {}
    unknown = 0
    for q in queries:
        if not isinstance(q, dict):
            continue
        kind = _clean(q.get("kind")).lower() or "unknown"
        kinds[kind] = int(kinds.get(kind, 0) or 0) + 1
        if kind == "unknown":
            unknown += 1
    dominant = "unknown"
    if kinds:
        dominant = max(kinds.items(), key=lambda kv: kv[1])[0]
    profile = {
        "artifact_type": "source_db_profile",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "tables": int(_as_dict(source_schema_model.get("summary")).get("tables", 0) or 0),
            "columns": int(_as_dict(source_schema_model.get("summary")).get("columns", 0) or 0),
            "relationships": len(_as_list(source_relationship_candidates.get("candidates"))),
            "queries": len(queries),
            "unknown_queries": int(unknown),
            "dominant_query_kind": dominant,
            "hotspots": len(_as_list(source_hotspot_report.get("hotspots"))),
        },
        "query_kind_distribution": kinds,
        "db_type_guess": "microsoft_access_or_ado" if any(k in kinds for k in ("select", "update", "delete")) else "unknown",
        "archaeology_route": "code_derived_sql_mining",
    }
    return profile


def _build_target_schema_model(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    source_relationship_candidates: dict[str, Any],
    target_database: str,
) -> dict[str, Any]:
    src_tables = [_as_dict(x) for x in _as_list(source_schema_model.get("tables")) if isinstance(x, dict)]
    rel_rows = [_as_dict(x) for x in _as_list(source_relationship_candidates.get("candidates")) if isinstance(x, dict)]
    target_tables: list[dict[str, Any]] = []
    table_name_map: dict[str, str] = {}
    for src in src_tables:
        sname = _clean(src.get("name"))
        if not sname:
            continue
        tname = _to_snake_case(sname) or sname.lower()
        table_name_map[sname.lower()] = tname
        target_cols: list[dict[str, Any]] = []
        source_cols = [_as_dict(c) for c in _as_list(src.get("columns")) if isinstance(c, dict)]
        for col in source_cols:
            cname = _clean(col.get("name"))
            if not cname:
                continue
            tcol = _to_snake_case(cname) or cname.lower()
            src_type = _clean(col.get("inferred_type")) or _guess_column_type(cname)
            target_type = src_type
            if src_type == "timestamp" and _clean(target_database).lower() in {"sql server"}:
                target_type = "datetime2"
            elif src_type == "boolean" and _clean(target_database).lower() in {"oracle"}:
                target_type = "number(1)"
            target_cols.append(
                {
                    "name": tcol,
                    "type": target_type,
                    "nullable": bool(col.get("nullable", True)),
                    "source_column": cname,
                    "source_type": src_type,
                }
            )
        existing = {row["name"] for row in target_cols}
        for audit_col, audit_type in (("created_at", "timestamp"), ("updated_at", "timestamp")):
            if audit_col not in existing:
                target_cols.append(
                    {
                        "name": audit_col,
                        "type": audit_type,
                        "nullable": True,
                        "source_column": "",
                        "source_type": "",
                    }
                )
        pk_candidates = [_to_snake_case(x) for x in _as_list(src.get("primary_key_candidates")) if _clean(x)]
        if not pk_candidates and any(col["name"] == "id" for col in target_cols):
            pk_candidates = ["id"]
        indexes = [col["name"] for col in target_cols if col["name"].endswith("_id")][:20]
        target_tables.append(
            {
                "table_id": f"tgt_tbl:{len(target_tables) + 1}",
                "name": tname,
                "source_table": sname,
                "columns": target_cols[:800],
                "primary_key": pk_candidates[:4],
                "indexes": indexes,
            }
        )

    constraints: list[dict[str, Any]] = []
    for rel in rel_rows:
        conf = _to_float(rel.get("confidence"), 0.0)
        if conf < 0.7:
            continue
        ftable = _clean(rel.get("from_table"))
        ttable = _clean(rel.get("to_table"))
        fcol = _to_snake_case(rel.get("from_column"))
        tcol = _to_snake_case(rel.get("to_column"))
        if not ftable or not ttable or not fcol or not tcol:
            continue
        constraints.append(
            {
                "constraint_id": f"fk:{len(constraints) + 1}",
                "type": "foreign_key",
                "from_table": table_name_map.get(ftable.lower(), _to_snake_case(ftable)),
                "from_column": fcol,
                "to_table": table_name_map.get(ttable.lower(), _to_snake_case(ttable)),
                "to_column": tcol,
                "source_relationship_id": _clean(rel.get("relationship_id")),
            }
        )

    total_cols = sum(len(_as_list(t.get("columns"))) for t in target_tables)
    return {
        "artifact_type": "target_schema_model",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "target_database": _clean(target_database) or "PostgreSQL",
        "summary": {
            "tables": len(target_tables),
            "columns": total_cols,
            "constraints": len(constraints),
        },
        "tables": target_tables[:800],
        "constraints": constraints[:1600],
    }


def _build_target_erd(
    *,
    metadata_common: dict[str, Any],
    target_schema_model: dict[str, Any],
) -> dict[str, Any]:
    tables = [_as_dict(x) for x in _as_list(target_schema_model.get("tables")) if isinstance(x, dict)]
    constraints = [_as_dict(x) for x in _as_list(target_schema_model.get("constraints")) if isinstance(x, dict)]
    lines: list[str] = ["erDiagram"]
    for table in tables[:180]:
        tname = _mermaid_safe_token(table.get("name"), default="table")
        if not tname:
            continue
        lines.append(f"  {tname} {{")
        pk_set = {_mermaid_safe_token(x, default="column") for x in _as_list(table.get("primary_key")) if _clean(x)}
        for col in _as_list(table.get("columns"))[:120]:
            if not isinstance(col, dict):
                continue
            cname = _mermaid_safe_token(col.get("name"), default="column")
            ctype = _mermaid_safe_token(col.get("type"), default="text")
            if not cname:
                continue
            suffix = " PK" if cname in pk_set else ""
            lines.append(f"    {ctype} {cname}{suffix}")
        lines.append("  }")
    for fk in constraints[:300]:
        ftable = _mermaid_safe_token(fk.get("from_table"), default="table")
        ttable = _mermaid_safe_token(fk.get("to_table"), default="table")
        if not ftable or not ttable:
            continue
        lines.append(f"  {ttable} ||--o{{ {ftable} : references")
    return {
        "artifact_type": "target_erd",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "format": "mermaid",
        "mermaid": "\n".join(lines),
        "tables": [t.get("name") for t in tables[:300]],
        "relationships": len(constraints),
    }


def _build_target_data_dictionary(
    *,
    metadata_common: dict[str, Any],
    target_schema_model: dict[str, Any],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    for table in _as_list(target_schema_model.get("tables")):
        if not isinstance(table, dict):
            continue
        tname = _clean(table.get("name"))
        source_table = _clean(table.get("source_table"))
        for col in _as_list(table.get("columns")):
            if not isinstance(col, dict):
                continue
            cname = _clean(col.get("name"))
            if not cname:
                continue
            rows.append(
                {
                    "target_table": tname,
                    "target_column": cname,
                    "target_type": _clean(col.get("type")) or "text",
                    "nullable": bool(col.get("nullable", True)),
                    "source_table": source_table,
                    "source_column": _clean(col.get("source_column")),
                    "business_meaning": _column_business_meaning(cname),
                }
            )
    return {
        "artifact_type": "target_data_dictionary",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "rows": rows[:12000],
    }


def _build_schema_mapping_matrix(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    target_schema_model: dict[str, Any],
) -> dict[str, Any]:
    target_by_source: dict[tuple[str, str], dict[str, Any]] = {}
    for t in _as_list(target_schema_model.get("tables")):
        if not isinstance(t, dict):
            continue
        src_table = _clean(t.get("source_table"))
        tgt_table = _clean(t.get("name"))
        for c in _as_list(t.get("columns")):
            if not isinstance(c, dict):
                continue
            src_col = _clean(c.get("source_column"))
            if not src_table or not src_col:
                continue
            target_by_source[(src_table.lower(), src_col.lower())] = {
                "target_table": tgt_table,
                "target_column": _clean(c.get("name")),
                "target_type": _clean(c.get("type")) or "text",
            }

    mappings: list[dict[str, Any]] = []
    for st in _as_list(source_schema_model.get("tables")):
        if not isinstance(st, dict):
            continue
        src_table = _clean(st.get("name"))
        for sc in _as_list(st.get("columns")):
            if not isinstance(sc, dict):
                continue
            src_col = _clean(sc.get("name"))
            if not src_table or not src_col:
                continue
            tgt = target_by_source.get((src_table.lower(), src_col.lower()), {})
            src_type = _clean(sc.get("inferred_type")) or "text"
            tgt_type = _clean(tgt.get("target_type")) or src_type
            rule = "identity"
            if _to_snake_case(src_col) != _clean(tgt.get("target_column")):
                rule = "rename_case_normalization"
            if src_type != tgt_type:
                rule = "cast_type"
            mappings.append(
                {
                    "mapping_id": f"map:{len(mappings) + 1}",
                    "source_table": src_table,
                    "source_column": src_col,
                    "source_type": src_type,
                    "target_table": _clean(tgt.get("target_table")) or "",
                    "target_column": _clean(tgt.get("target_column")) or "",
                    "target_type": tgt_type,
                    "transform_rule": rule,
                    "confidence": round(_to_float(sc.get("confidence"), 0.65), 2),
                    "verification_required": not bool(tgt),
                }
            )
    return {
        "artifact_type": "schema_mapping_matrix",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "mappings": mappings[:24000],
    }


def _build_data_migration_plan(
    *,
    metadata_common: dict[str, Any],
    schema_mapping_matrix: dict[str, Any],
    target_schema_model: dict[str, Any],
) -> dict[str, Any]:
    mappings = [_as_dict(x) for x in _as_list(schema_mapping_matrix.get("mappings")) if isinstance(x, dict)]
    unresolved = [m for m in mappings if bool(m.get("verification_required"))]
    target_tables = [_clean(_as_dict(t).get("name")) for t in _as_list(target_schema_model.get("tables")) if _clean(_as_dict(t).get("name"))]
    return {
        "artifact_type": "migration_plan",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "strategy": {
            "cutover": "phased",
            "backfill": "table_by_table_with_reconciliation",
            "rollback": "retain_source_snapshot_per_batch",
        },
        "sequence": [
            {"step": 1, "title": "Create target schema and constraints in staging environment."},
            {"step": 2, "title": "Backfill reference/master tables, then transaction/history tables."},
            {"step": 3, "title": "Run reconciliation checks (row counts, financial aggregates, spot checks)."},
            {"step": 4, "title": "Perform controlled cutover and monitor verification dashboard."},
        ],
        "target_tables": target_tables[:300],
        "unresolved_mappings": len(unresolved),
        "blocking_items": unresolved[:120],
    }


def _build_validation_harness_spec(
    *,
    metadata_common: dict[str, Any],
    schema_mapping_matrix: dict[str, Any],
    source_hotspot_report: dict[str, Any],
) -> dict[str, Any]:
    mappings = [_as_dict(x) for x in _as_list(schema_mapping_matrix.get("mappings")) if isinstance(x, dict)]
    high_hotspots = [
        _as_dict(x)
        for x in _as_list(source_hotspot_report.get("hotspots"))
        if isinstance(x, dict) and _clean(x.get("blast_radius")).lower() == "high"
    ]
    return {
        "artifact_type": "validation_harness_spec",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "checks": [
            {
                "id": "VAL-001",
                "name": "Row count reconciliation",
                "description": "Validate source/target row counts per migrated table within accepted threshold.",
            },
            {
                "id": "VAL-002",
                "name": "Critical aggregate parity",
                "description": "Validate key financial aggregates (balance, debit, credit, transaction totals).",
            },
            {
                "id": "VAL-003",
                "name": "Column-level checksum",
                "description": "Compute deterministic checksums for mapped columns in sampled ranges.",
            },
            {
                "id": "VAL-004",
                "name": "Workflow-level equivalence",
                "description": "Replay golden legacy workflows and confirm target side effects are equivalent.",
            },
        ],
        "coverage": {
            "mapped_columns": len([m for m in mappings if _clean(m.get("target_column"))]),
            "unmapped_columns": len([m for m in mappings if not _clean(m.get("target_column"))]),
            "high_blast_tables": len(high_hotspots),
        },
        "focus_tables": [_clean(x.get("table")) for x in high_hotspots[:20] if _clean(x.get("table"))],
    }


def _build_db_qa_report(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    source_query_catalog: dict[str, Any],
    source_relationship_candidates: dict[str, Any],
    schema_mapping_matrix: dict[str, Any],
    target_schema_model: dict[str, Any],
) -> dict[str, Any]:
    checks: list[dict[str, Any]] = []

    def add(check_id: str, status: str, detail: str, blocking: bool = False) -> None:
        checks.append(
            {
                "id": check_id,
                "status": _clean(status).upper() if _clean(status).upper() in {"PASS", "WARN", "FAIL"} else "WARN",
                "blocking": bool(blocking),
                "detail": _clean(detail),
            }
        )

    src_tables = {_clean(_as_dict(t).get("name")).lower() for t in _as_list(source_schema_model.get("tables")) if _clean(_as_dict(t).get("name"))}
    query_rows = [_as_dict(x) for x in _as_list(source_query_catalog.get("queries")) if isinstance(x, dict)]
    unresolved_tables: set[str] = set()
    for q in query_rows:
        for tbl in _as_list(q.get("tables")):
            t = _clean(tbl)
            if t and t.lower() not in src_tables:
                unresolved_tables.add(t)
    if unresolved_tables:
        add(
            "db_struct_query_table_resolution",
            "FAIL",
            f"Query catalog references unknown source tables: {', '.join(sorted(unresolved_tables)[:10])}.",
            True,
        )
    else:
        add("db_struct_query_table_resolution", "PASS", "All query table references resolve to source schema model.")

    src_cols = {
        (_clean(_as_dict(t).get("name")).lower(), _clean(_as_dict(c).get("name")).lower())
        for t in _as_list(source_schema_model.get("tables"))
        if isinstance(t, dict)
        for c in _as_list(_as_dict(t).get("columns"))
        if isinstance(c, dict) and _clean(_as_dict(t).get("name")) and _clean(_as_dict(c).get("name"))
    }
    map_rows = [_as_dict(x) for x in _as_list(schema_mapping_matrix.get("mappings")) if isinstance(x, dict)]
    mapped_cols = {
        (_clean(m.get("source_table")).lower(), _clean(m.get("source_column")).lower())
        for m in map_rows
        if _clean(m.get("source_table")) and _clean(m.get("source_column")) and _clean(m.get("target_column"))
    }
    coverage = (len(mapped_cols) / float(len(src_cols))) if src_cols else 1.0
    if coverage < 0.8:
        add(
            "db_struct_mapping_coverage",
            "FAIL",
            f"Schema mapping coverage is {coverage:.2%}, below required 80%.",
            True,
        )
    elif coverage < 0.95:
        add("db_struct_mapping_coverage", "WARN", f"Schema mapping coverage is {coverage:.2%}; verify unresolved columns.")
    else:
        add("db_struct_mapping_coverage", "PASS", f"Schema mapping coverage is {coverage:.2%}.")

    tgt_tables = [_as_dict(x) for x in _as_list(target_schema_model.get("tables")) if isinstance(x, dict)]
    pk_missing = [t for t in tgt_tables if not _as_list(t.get("primary_key"))]
    if pk_missing:
        add(
            "db_struct_target_pk_presence",
            "WARN",
            f"{len(pk_missing)} target table(s) missing explicit primary key proposal.",
        )
    else:
        add("db_struct_target_pk_presence", "PASS", "All target tables have primary key proposal.")

    rel_rows = [_as_dict(x) for x in _as_list(source_relationship_candidates.get("candidates")) if isinstance(x, dict)]
    rel_unresolved = 0
    src_table_cols: dict[str, set[str]] = {}
    for t in _as_list(source_schema_model.get("tables")):
        if not isinstance(t, dict):
            continue
        tname = _clean(t.get("name")).lower()
        cols = {_clean(_as_dict(c).get("name")).lower() for c in _as_list(t.get("columns")) if _clean(_as_dict(c).get("name"))}
        if tname:
            src_table_cols[tname] = cols
    for rel in rel_rows:
        ft = _clean(rel.get("from_table")).lower()
        fc = _clean(rel.get("from_column")).lower()
        tt = _clean(rel.get("to_table")).lower()
        tc = _clean(rel.get("to_column")).lower()
        if ft and fc and (fc not in src_table_cols.get(ft, set())):
            rel_unresolved += 1
            continue
        if tt and tc and (tc not in src_table_cols.get(tt, set())):
            rel_unresolved += 1
    if rel_unresolved:
        add(
            "db_struct_relationship_resolution",
            "WARN",
            f"{rel_unresolved} relationship candidate(s) reference columns not present in schema model.",
        )
    else:
        add("db_struct_relationship_resolution", "PASS", "Relationship candidates resolve to schema columns.")

    overall = "PASS"
    if any(_clean(x.get("status")).upper() == "FAIL" and bool(x.get("blocking")) for x in checks):
        overall = "FAIL"
    elif any(_clean(x.get("status")).upper() in {"FAIL", "WARN"} for x in checks):
        overall = "WARN"

    return {
        "artifact_type": "db_qa_report",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "overall_status": overall,
        "checks": checks,
    }


def _build_schema_approval_record(
    *,
    metadata_common: dict[str, Any],
    db_qa_report: dict[str, Any],
) -> dict[str, Any]:
    qa_status = _clean(db_qa_report.get("overall_status")).upper() or "WARN"
    approval_status = "REQUIRED"
    if qa_status == "PASS":
        approval_status = "PENDING_APPROVAL"
    elif qa_status == "FAIL":
        approval_status = "BLOCKED"
    return {
        "artifact_type": "schema_approval_record",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "status": approval_status,
        "qa_status": qa_status,
        "approved_by": "",
        "approved_at": "",
        "notes": (
            "Approve source/target schema baseline for planning and implementation only after DB QA passes."
        ),
    }


def _build_schema_drift_report(
    *,
    metadata_common: dict[str, Any],
    source_schema_model: dict[str, Any],
    target_schema_model: dict[str, Any],
) -> dict[str, Any]:
    src_summary = _as_dict(source_schema_model.get("summary"))
    tgt_summary = _as_dict(target_schema_model.get("summary"))
    table_delta = int(tgt_summary.get("tables", 0) or 0) - int(src_summary.get("tables", 0) or 0)
    col_delta = int(tgt_summary.get("columns", 0) or 0) - int(src_summary.get("columns", 0) or 0)
    return {
        "artifact_type": "schema_drift_report",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "status": "WARN" if abs(table_delta) > 0 or abs(col_delta) > 0 else "PASS",
        "drift_summary": {
            "source_tables": int(src_summary.get("tables", 0) or 0),
            "target_tables": int(tgt_summary.get("tables", 0) or 0),
            "table_delta": table_delta,
            "source_columns": int(src_summary.get("columns", 0) or 0),
            "target_columns": int(tgt_summary.get("columns", 0) or 0),
            "column_delta": col_delta,
        },
        "notes": [
            "Drift report compares source reconstruction vs target proposal.",
            "Large deltas require explicit ADR and migration justification.",
        ],
    }


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


def _extract_guid(reference: Any) -> str:
    text = _clean(reference)
    if not text:
        return ""
    m = re.search(r"\{[0-9A-Fa-f-]{36}\}", text)
    return m.group(0) if m else ""


def _parse_dependency_identity(value: Any) -> tuple[str, str]:
    if isinstance(value, dict):
        name = _clean(
            value.get("name")
            or value.get("dependency")
            or value.get("binary")
            or value.get("binary_ref")
        )
        reference = _clean(
            value.get("reference")
            or value.get("guid_reference")
            or value.get("object_ref")
            or value.get("raw_reference")
        )
        raw = _clean(value.get("raw") or value.get("line"))
        if raw:
            parsed_name, parsed_ref = _parse_dependency_identity(raw)
            if not name:
                name = parsed_name
            if not reference:
                reference = parsed_ref
        if not reference and _extract_guid(name):
            reference = _extract_guid(name)
        return name, reference

    text = _clean(value)
    if not text:
        return "", ""

    name = text
    reference = ""

    obj_match = re.search(r'(?i)^\s*object\s*=\s*"([^"]+)"\s*;\s*"([^"]+)"\s*$', text)
    if obj_match:
        reference = _clean(obj_match.group(1))
        name = _clean(obj_match.group(2))
    else:
        pair_match = re.search(r'"([^"]+)"\s*;\s*"([^"]+)"', text)
        if pair_match:
            reference = _clean(pair_match.group(1))
            name = _clean(pair_match.group(2))

    if not reference:
        suffix_ref = re.search(r"\(([^)]*\{[0-9A-Fa-f-]{36}[^)]*)\)\s*$", name)
        if suffix_ref:
            reference = _clean(suffix_ref.group(1))
            name = re.sub(r"\s*\([^)]*\{[0-9A-Fa-f-]{36}[^)]*\)\s*$", "", name).strip()

    if not reference:
        reference_line = re.search(r"(?i)^reference\s*=\s*(.+)$", text)
        if reference_line:
            reference = _clean(reference_line.group(1))

    ext_match = re.search(r"([A-Za-z0-9_.-]+\.(?:ocx|dll|dcx|dca))", name, flags=re.IGNORECASE)
    if not ext_match:
        ext_match = re.search(r"([A-Za-z0-9_.-]+\.(?:ocx|dll|dcx|dca))", text, flags=re.IGNORECASE)
    if ext_match:
        name = _clean(ext_match.group(1))

    name = name.strip().strip('"')
    if not reference:
        reference = _extract_guid(text)
    return name, reference


def _normalize_form_name_token(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if "::" in text:
        text = text.split("::", 1)[1].strip()
    if ":" in text:
        text = text.split(":", 1)[1].strip()
    return text.lower()


def _split_variant_form(value: Any) -> tuple[str, str]:
    text = _clean(value)
    if not text:
        return ("", "")
    if "::" not in text:
        return ("", text)
    variant, base = text.split("::", 1)
    return (_clean(variant), _clean(base))


def _strip_form_extension(name: str) -> str:
    lower = _clean(name).lower()
    for ext in (".frm", ".ctl", ".cls", ".bas"):
        if lower.endswith(ext):
            return lower[: -len(ext)]
    return lower


def _canonical_form_key(name: Any) -> str:
    token = _normalize_form_name_token(name)
    token = _strip_form_extension(token)
    token = token.replace(" ", "")
    return token


def _build_form_profiles(forms: list[Any]) -> tuple[list[dict[str, Any]], dict[str, set[str]]]:
    profiles: list[dict[str, Any]] = []
    handler_index: dict[str, set[str]] = {}
    seen: set[str] = set()
    for row in forms[:1200]:
        if not isinstance(row, dict):
            continue
        form_name_scoped = _clean(row.get("form_name") or row.get("name") or row.get("base_form_name"))
        if not form_name_scoped:
            continue
        canonical = _canonical_form_key(form_name_scoped)
        if not canonical:
            continue
        project_name = _clean(row.get("project_name"))
        scoped_key = form_name_scoped.lower()
        profile_id = f"{project_name}::{scoped_key}" if project_name else scoped_key
        if profile_id in seen:
            continue
        seen.add(profile_id)
        handlers_exact = {_clean(x) for x in _as_list(row.get("event_handlers")) if _clean(x)}
        handlers_lower = {h.lower() for h in handlers_exact}
        use_text = _clean(row.get("business_use"))
        keyword_blob = " ".join(
            [
                canonical,
                _clean(form_name_scoped).lower(),
                _clean(row.get("base_form_name")).lower(),
                use_text.lower(),
                project_name.lower(),
            ]
        )
        profiles.append(
            {
                "profile_id": profile_id,
                "canonical": canonical,
                "form_name": form_name_scoped,
                "base_form_name": _clean(row.get("base_form_name")),
                "project_name": project_name,
                "business_use": use_text,
                "handlers_exact": handlers_exact,
                "handlers_lower": handlers_lower,
                "keywords": keyword_blob,
            }
        )
        for handler in handlers_exact:
            handler_index.setdefault(f"exact:{handler}", set()).add(profile_id)
        for handler in handlers_lower:
            handler_index.setdefault(f"lower:{handler}", set()).add(profile_id)
    return profiles, handler_index


def _infer_form_for_event(
    *,
    row: dict[str, Any],
    handler: str,
    form_profiles: list[dict[str, Any]],
    handler_index: dict[str, set[str]],
) -> str:
    explicit_form = _clean(row.get("form"))
    if explicit_form:
        return explicit_form

    profiles_by_id: dict[str, dict[str, Any]] = {
        _clean(profile.get("profile_id")): profile
        for profile in form_profiles
        if isinstance(profile, dict) and _clean(profile.get("profile_id"))
    }
    handler_exact = _clean(handler)
    handler_key = handler_exact.lower()
    direct_candidates = set(handler_index.get(f"exact:{handler_exact}", set()))
    if not direct_candidates:
        direct_candidates = set(handler_index.get(f"lower:{handler_key}", set()))

    if handler_key and "_" in handler_key:
        prefix = handler_key.split("_", 1)[0]
        for profile in form_profiles:
            profile_id = _clean(profile.get("profile_id"))
            canonical = _clean(profile.get("canonical"))
            if canonical and (canonical.startswith(prefix) or prefix.startswith(canonical)):
                direct_candidates.add(profile_id)

    touches = [_clean(x) for x in _as_list(row.get("sql_touches")) if _clean(x)]
    table_hints = _extract_tables_from_sql_catalog(touches)
    table_canon = {_canonical_table_name(t) for t in table_hints if _canonical_table_name(t)}
    if not direct_candidates:
        for profile in form_profiles:
            profile_id = _clean(profile.get("profile_id"))
            canonical = _clean(profile.get("canonical"))
            if not canonical:
                continue
            for tc in table_canon:
                if tc and (tc in canonical or canonical in tc):
                    direct_candidates.add(profile_id)
                    break

    if not direct_candidates:
        return ""
    if len(direct_candidates) == 1:
        selected = next(iter(direct_candidates))
    else:
        scored: list[tuple[int, str]] = []
        for candidate in sorted(direct_candidates):
            profile = profiles_by_id.get(candidate, {})
            score = 0
            handlers_exact = profile.get("handlers_exact")
            if not isinstance(handlers_exact, set):
                handlers_exact = set(_clean(x) for x in _as_list(handlers_exact) if _clean(x))
            handlers_lower = profile.get("handlers_lower")
            if not isinstance(handlers_lower, set):
                handlers_lower = {h.lower() for h in handlers_exact}
            if handler_exact and handler_exact in handlers_exact:
                score += 6
            elif handler_key and handler_key in handlers_lower:
                score += 4
            keyword_blob = _clean(profile.get("keywords")).lower()
            for tc in table_canon:
                if not tc:
                    continue
                if tc in keyword_blob:
                    score += 3
                    continue
                if tc in candidate or candidate in tc:
                    score += 2
                    continue
                if SequenceMatcher(None, tc, candidate).ratio() >= 0.72:
                    score += 2
            base_name = _clean(profile.get("base_form_name")).lower()
            if (candidate.startswith("main") or base_name.startswith("main")) and handler_key.startswith("mdiform"):
                score += 2
            scored.append((score, candidate))
        scored.sort(key=lambda item: (item[0], item[1]), reverse=True)
        selected = scored[0][1] if scored else sorted(direct_candidates)[0]

    profile = profiles_by_id.get(selected, {})
    return _clean(profile.get("form_name")) or _clean(profile.get("base_form_name")) or selected


def _infer_form_purpose(
    *,
    form_name: str,
    current_purpose: str,
    actions: list[dict[str, Any]],
    sql_rows: list[dict[str, Any]],
    procedures: list[dict[str, Any]],
) -> str:
    form_tokens = _clean(form_name).lower()
    form_specific_mappings = [
        (("login", "logi"), "Authentication and credential validation workflow."),
        (("deposit",), "Deposit capture and balance posting workflow."),
        (("withdraw",), "Withdrawal processing and balance deduction workflow."),
        (("transaction", "transction"), "Transaction ledger management and adjustment workflow."),
        (("accounttype", "acctype"), "Account type maintenance and account setup workflow."),
        (("customer",), "Customer profile onboarding and maintenance workflow."),
        (("search",), "Record search and retrieval workflow."),
        (("report",), "Operational reporting and statement generation workflow."),
        (("main", "mdiform"), "Application navigation and module routing workflow."),
    ]
    for keys, description in form_specific_mappings:
        if any(token in form_tokens for token in keys):
            return description

    text_tokens = " ".join(
        [
            _clean(form_name).lower(),
            _clean(current_purpose).lower(),
            " ".join(_clean(_as_dict(a).get("event_handler")).lower() for a in actions),
            " ".join(_clean(_as_dict(a).get("control")).lower() for a in actions),
            " ".join(_clean(_as_dict(r).get("procedure")).lower() for r in sql_rows),
            " ".join(_clean(_as_dict(p).get("procedure_name")).lower() for p in procedures),
            " ".join(
                _clean(tbl).lower()
                for r in sql_rows
                for tbl in _as_list(_as_dict(r).get("tables"))
            ),
        ]
    )
    mappings = [
        (("login", "logi", "password", "username"), "Authentication and credential validation workflow."),
        (("txtpass", "pass1", "credential"), "Password and credential management workflow."),
        (("deposit", "credit"), "Deposit capture and balance posting workflow."),
        (("withdraw", "debit"), "Withdrawal processing and balance deduction workflow."),
        (("transaction", "transction", "tbltransaction"), "Transaction ledger management and adjustment workflow."),
        (("accounttype", "acctype"), "Account type maintenance and account setup workflow."),
        (("customer", "tblcustomer"), "Customer profile onboarding and maintenance workflow."),
        (("balance", "balancedt", "tblbalance"), "Balance inquiry and reconciliation workflow."),
        (("report", "datareport", "dataenvironment"), "Operational reporting and statement generation workflow."),
        (("search", "find"), "Record search and retrieval workflow."),
        (("main", "mdiform", "toolbar"), "Application navigation and module routing workflow."),
    ]
    for keys, description in mappings:
        if any(token in text_tokens for token in keys):
            return description
    if _clean(current_purpose):
        return current_purpose
    return "Insufficient behavioral evidence from the available analysis source to derive a business-specific workflow."


def _infer_form_alias(
    *,
    form_name: str,
    purpose: str,
    sql_rows: list[dict[str, Any]],
    procedures: list[dict[str, Any]],
    controls: list[str] | None = None,
) -> str:
    form_token = _clean(form_name).lower()
    if form_token in {"main", "mdiform"}:
        return "Navigation Hub"
    is_generic_form = bool(re.fullmatch(r"(form\d+|frm\d+)", form_token))
    purpose_low = _clean(purpose).lower()
    if "deposit capture" in purpose_low:
        return "Deposit Capture"
    if "withdrawal processing" in purpose_low:
        return "Withdrawal Processing"
    if "customer profile" in purpose_low:
        return "Customer Management"
    if "transaction ledger" in purpose_low:
        return "Transaction Ledger"
    if "account type maintenance" in purpose_low:
        return "Account Type Maintenance"
    token_blob = " ".join(
        [
            form_token,
            _clean(purpose).lower(),
            " ".join(_clean(_as_dict(r).get("procedure")).lower() for r in sql_rows),
            " ".join(_clean(t).lower() for r in sql_rows for t in _as_list(_as_dict(r).get("tables"))),
            " ".join(_clean(_as_dict(p).get("procedure_name")).lower() for p in procedures),
            " ".join(_clean(c).lower() for c in (controls or [])),
        ]
    )
    if any(token in token_blob for token in ("login", "logi", "username", "password", "txtpass", "pass1", "credential")):
        return "Password Management" if any(token in token_blob for token in ("txtpass", "pass1", "credential")) else "Authentication"
    has_strong_transaction_signal = any(
        token in token_blob for token in ("transction", "tbltransaction", "transaction ledger", "ledger")
    )
    # Transaction/ledger semantics should win only on strong signals.
    if has_strong_transaction_signal or ("debit" in token_blob and "credit" in token_blob):
        return "Transaction Ledger"
    if any(token in token_blob for token in ("withdraw", "debit")):
        return "Withdrawal Processing"
    if any(token in token_blob for token in ("deposit", "credit", "balancedt")) and not any(
        token in token_blob for token in ("transaction", "transction", "debit")
    ):
        return "Deposit Capture"
    if any(token in token_blob for token in ("customer", "tblcustomer")) and any(
        token in token_blob for token in ("interest", "min balance", "account type", "acctype")
    ):
        return "Customer Management"
    if any(token in token_blob for token in ("accounttype", "acctype")):
        return "Account Type Maintenance"
    if any(token in token_blob for token in ("customer", "tblcustomer")):
        return "Customer Management"
    if any(token in token_blob for token in ("report", "datareport", "dataenvironment")):
        return "Reporting"
    if any(token in token_blob for token in ("search", "lookup", "find")):
        return "Search"
    if any(token in token_blob for token in ("main", "mdiform", "toolbar")):
        return "Navigation Hub"
    if any(token in token_blob for token in ("balance", "tblbalance")):
        return "Balance Inquiry"
    if any(token in token_blob for token in ("timer", "progressbar", "splash")):
        return "Splash/Loading"
    if is_generic_form and form_token == "form9":
        return "Authentication Entry"
    if is_generic_form and form_token == "form1":
        return "Navigation/Menu"
    if is_generic_form and any(token in token_blob for token in ("dated", "datejoined", "dtpicker", "date 1", "from date", "to date")):
        return "Date/Period Entry"
    fallback = _clean(purpose).replace("workflow.", "").replace("workflow", "").strip(" -.")
    if fallback.lower() in {
        "business executed through event-driven ui controls",
        "business workflow executed through event-driven ui controls",
        "application navigation and module routing",
    }:
        return ""
    return fallback


def _infer_form_type(
    *,
    form_name: str,
    purpose: str,
    controls: list[str],
    procedures: list[dict[str, Any]],
    table_hints: list[str],
) -> str:
    form_low = _clean(form_name).lower()
    purpose_low = _clean(purpose).lower()
    control_text = " ".join(_clean(c).lower() for c in controls)
    proc_names = {_clean(_as_dict(p).get("procedure_name")).lower() for p in procedures}
    table_text = " ".join(_clean(t).lower() for t in table_hints)
    if "splash" in form_low or "splash" in purpose_low:
        return "Splash"
    if form_low in {"main", "mdiform"} or form_low.startswith("mdi") or "toolbar" in control_text or any("toolbar" in p for p in proc_names):
        return "MDI_Host"
    if "login" in form_low or "auth" in purpose_low or ("form9" in form_low and ("logi" in table_text or "login" in table_text)):
        return "Login"
    if form_low.startswith(("rpt", "datareport")):
        return "Report"
    return "Child"


def _canonical_table_name(value: Any) -> str:
    token = _clean(value).lower()
    if not token:
        return ""
    token = token.strip("[]`\"'")
    if "." in token:
        token = token.split(".")[-1]
    token = re.sub(r"[^a-z0-9_]", "", token)
    token = re.sub(r"_+", "_", token).strip("_")
    if token.startswith("tbl") and len(token) > 3:
        token = token[3:]
    if token.endswith("s") and len(token) > 6:
        token = token[:-1]
    return token


def _is_transaction_like_name(value: Any) -> bool:
    token = _canonical_table_name(value)
    if not token:
        return False
    return bool(re.search(r"trans(act|action|actions|ction|ctions|action)?", token))


def _similar_name(left: str, right: str) -> bool:
    l = _canonical_table_name(left)
    r = _canonical_table_name(right)
    if not l or not r or l == r:
        return False
    ratio = SequenceMatcher(None, l, r).ratio()
    return ratio >= 0.86


def _project_disambiguation_hint(project: dict[str, Any], index: int) -> str:
    path = _clean(project.get("project_file") or project.get("file"))
    if not path:
        return f"variant-{index}"
    norm = path.replace("\\", "/").strip("/")
    parts = [p for p in norm.split("/") if p]
    if not parts:
        return f"variant-{index}"
    if len(parts) >= 2:
        return "/".join(parts[-2:])
    return parts[-1]


def _derive_form_coverage_rows(
    *,
    forms: list[Any],
    ui_event_rows: list[Any],
    sql_map_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    provided_rows = [row for row in forms if isinstance(row, dict) and ("coverage_score" in row or "expected_handlers_count" in row)]
    if provided_rows:
        out: list[dict[str, Any]] = []
        for row in provided_rows:
            out.append(
                {
                    "form_name": _clean(row.get("form_name") or row.get("base_form_name")),
                    "project_name": _clean(row.get("project_name")),
                    "coverage_score": round(_to_float(row.get("coverage_score"), 0.0), 4),
                    "confidence_score": round(_to_float(row.get("confidence_score"), 0.0), 4),
                    "expected_handlers_count": int(row.get("expected_handlers_count", 0) or 0),
                    "extracted_handlers_count": int(row.get("extracted_handlers_count", 0) or 0),
                    "explained_handlers_count": int(row.get("explained_handlers_count", 0) or 0),
                    "sql_touched_count": int(row.get("sql_touched_count", 0) or 0),
                    "source_loc": int(row.get("source_loc", 0) or 0),
                    "risk_count": int(row.get("risk_count", 0) or 0),
                }
            )
        return out[:300]

    event_by_form: dict[str, set[str]] = {}
    for row in ui_event_rows:
        if not isinstance(row, dict):
            continue
        form = _normalize_form_name_token(row.get("form"))
        if not form:
            continue
        handler = _clean(row.get("event_handler")) or _clean(_as_dict(row.get("handler")).get("symbol"))
        if handler:
            event_by_form.setdefault(form, set()).add(handler)
    sql_by_form: dict[str, int] = {}
    for row in sql_map_rows:
        if not isinstance(row, dict):
            continue
        form = _normalize_form_name_token(row.get("form"))
        if not form:
            continue
        sql_by_form[form] = int(sql_by_form.get(form, 0) or 0) + 1

    out: list[dict[str, Any]] = []
    for form_row in forms:
        if isinstance(form_row, dict):
            form_name = _clean(form_row.get("form_name") or form_row.get("name") or form_row.get("base_form_name"))
            project_name = _clean(form_row.get("project_name"))
            handlers = [_clean(x) for x in _as_list(form_row.get("event_handlers")) if _clean(x)]
        else:
            form_name = _clean(form_row)
            project_name = ""
            handlers = []
        if not form_name:
            continue
        norm = _normalize_form_name_token(form_name)
        extracted = len(handlers) if handlers else len(event_by_form.get(norm, set()))
        expected = max(extracted, len(event_by_form.get(norm, set())))
        coverage = 1.0 if expected <= 0 else min(1.0, extracted / float(expected))
        confidence = min(0.99, 0.55 + (0.25 * coverage) + (0.08 if int(sql_by_form.get(norm, 0) or 0) > 0 else 0.0))
        out.append(
            {
                "form_name": form_name,
                "project_name": project_name,
                "coverage_score": round(coverage, 4),
                "confidence_score": round(confidence, 4),
                "expected_handlers_count": expected,
                "extracted_handlers_count": extracted,
                "explained_handlers_count": extracted,
                "sql_touched_count": int(sql_by_form.get(norm, 0) or 0),
                "source_loc": int(_as_dict(form_row).get("source_loc", 0) or 0),
                "risk_count": 0,
            }
        )
    return out[:300]


def _build_variant_diff_report(
    *,
    metadata_common: dict[str, Any],
    projects: list[dict[str, Any]],
) -> dict[str, Any]:
    comparisons: list[dict[str, Any]] = []
    schema_divergence_rows: list[dict[str, Any]] = []
    for i, left in enumerate(projects[:16]):
        if not isinstance(left, dict):
            continue
        lname = _clean(left.get("name") or left.get("project_id") or f"project_{i + 1}")
        lforms = {_clean(x).lower() for x in _as_list(left.get("forms")) if _clean(x)}
        lmembers = {_clean(x).lower() for x in _as_list(left.get("member_files")) if _clean(x)}
        ltables_raw = [_clean(x) for x in _as_list(_as_dict(left.get("data_touchpoints")).get("tables")) if _clean(x)]
        ltables = {x.lower() for x in ltables_raw}
        left_tbl_prefixed = sum(1 for t in ltables_raw if _clean(t).lower().startswith("tbl"))
        left_tbl_ratio = 0.0 if not ltables_raw else left_tbl_prefixed / float(len(ltables_raw))
        left_canonical_map: dict[str, set[str]] = {}
        for table in ltables_raw:
            canon = _canonical_table_name(table)
            if not canon:
                continue
            left_canonical_map.setdefault(canon, set()).add(table)
        for j, right in enumerate(projects[i + 1 : 16], start=i + 2):
            if not isinstance(right, dict):
                continue
            rname = _clean(right.get("name") or right.get("project_id") or f"project_{j}")
            rforms = {_clean(x).lower() for x in _as_list(right.get("forms")) if _clean(x)}
            rmembers = {_clean(x).lower() for x in _as_list(right.get("member_files")) if _clean(x)}
            rtables_raw = [_clean(x) for x in _as_list(_as_dict(right.get("data_touchpoints")).get("tables")) if _clean(x)]
            rtables = {x.lower() for x in rtables_raw}
            right_tbl_prefixed = sum(1 for t in rtables_raw if _clean(t).lower().startswith("tbl"))
            right_tbl_ratio = 0.0 if not rtables_raw else right_tbl_prefixed / float(len(rtables_raw))
            right_canonical_map: dict[str, set[str]] = {}
            for table in rtables_raw:
                canon = _canonical_table_name(table)
                if not canon:
                    continue
                right_canonical_map.setdefault(canon, set()).add(table)

            canonical_overlap = sorted(set(left_canonical_map).intersection(set(right_canonical_map)))[:120]
            alias_mismatches: list[dict[str, Any]] = []
            for canonical in canonical_overlap:
                left_names = sorted(left_canonical_map.get(canonical, set()))
                right_names = sorted(right_canonical_map.get(canonical, set()))
                if set(name.lower() for name in left_names) == set(name.lower() for name in right_names):
                    continue
                alias_mismatches.append(
                    {
                        "canonical": canonical,
                        "left_names": left_names[:8],
                        "right_names": right_names[:8],
                    }
                )

            near_miss_names: list[dict[str, Any]] = []
            left_flat = [item for items in left_canonical_map.values() for item in items]
            right_flat = [item for items in right_canonical_map.values() for item in items]
            for lname_raw in left_flat[:120]:
                for rname_raw in right_flat[:120]:
                    if _canonical_table_name(lname_raw) == _canonical_table_name(rname_raw):
                        continue
                    if not _similar_name(lname_raw, rname_raw):
                        continue
                    near_miss_names.append({"left": lname_raw, "right": rname_raw})
                    if len(near_miss_names) >= 20:
                        break
                if len(near_miss_names) >= 20:
                    break

            naming_style_divergence = (
                len(ltables_raw) >= 3
                and len(rtables_raw) >= 3
                and abs(left_tbl_ratio - right_tbl_ratio) >= 0.5
            )
            transaction_schema_conflict = any(
                _is_transaction_like_name(_as_dict(row).get("left") or _as_dict(row).get("canonical"))
                or _is_transaction_like_name(_as_dict(row).get("right"))
                for row in [*alias_mismatches, *near_miss_names]
            )
            pair_schema_divergent = bool(alias_mismatches or near_miss_names or naming_style_divergence)
            shared_forms = len(lforms.intersection(rforms))
            form_delta = len(lforms.symmetric_difference(rforms))
            member_delta = len(lmembers.symmetric_difference(rmembers))
            table_delta = len(ltables.symmetric_difference(rtables))
            overlap_den = max(1, len(lforms.union(rforms)))
            similarity = shared_forms / float(overlap_den)
            comparisons.append(
                {
                    "pair_id": f"variant_pair:{i + 1}:{j}",
                    "left_project": lname,
                    "right_project": rname,
                    "shared_forms": shared_forms,
                    "form_delta": form_delta,
                    "member_delta": member_delta,
                    "table_delta": table_delta,
                    "similarity_score": round(similarity, 4),
                    "schema_divergence_detected": pair_schema_divergent,
                    "risk_tier": (
                        "high"
                        if pair_schema_divergent or form_delta > 8 or member_delta > 20
                        else ("medium" if form_delta > 0 or table_delta > 0 else "low")
                    ),
                }
            )
            if pair_schema_divergent:
                schema_divergence_rows.append(
                    {
                        "pair_id": f"variant_pair:{i + 1}:{j}",
                        "left_project": lname,
                        "right_project": rname,
                        "alias_mismatches": alias_mismatches[:20],
                        "near_miss_names": near_miss_names[:20],
                        "naming_style_divergence": naming_style_divergence,
                        "transaction_schema_conflict": transaction_schema_conflict,
                        "left_tbl_prefix_ratio": round(left_tbl_ratio, 3),
                        "right_tbl_prefix_ratio": round(right_tbl_ratio, 3),
                    }
                )
    unresolved = len(projects) > 1
    schema_divergence_detected = bool(schema_divergence_rows)
    blocking_rows = [row for row in schema_divergence_rows if bool(row.get("transaction_schema_conflict"))]
    status = "FAIL" if unresolved or schema_divergence_detected else "PASS"
    prompt = (
        "Multiple legacy project variants detected. Confirm canonical scope (single variant, merge strategy, or independent modernizations)."
        if unresolved
        else "Single project variant detected; no variant scope decision required."
    )
    if schema_divergence_detected:
        prompt = (
            "Variant schema divergence detected (table aliases/prefix patterns differ across project variants). "
            "Resolve canonical schema strategy before planning."
        )
    return {
        "artifact_type": "variant_diff_report",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "project_count": len(projects),
        "comparisons": sorted(comparisons, key=lambda row: (float(row.get("similarity_score", 0.0)), -int(row.get("form_delta", 0))), reverse=True)[:40],
        "decision_required": unresolved,
        "status": status,
        "decision_prompt": prompt,
        "schema_divergence": {
            "detected": schema_divergence_detected,
            "pairs": schema_divergence_rows[:24],
            "blocking_pairs": blocking_rows[:12],
        },
    }


def _build_reporting_model(
    *,
    metadata_common: dict[str, Any],
    projects: list[dict[str, Any]],
    sql_statements: list[dict[str, Any]],
) -> dict[str, Any]:
    data_environments: set[str] = set()
    data_reports: set[str] = set()
    report_entrypoints: list[dict[str, Any]] = []
    hardcoded_paths: set[str] = set()

    for project in projects:
        if not isinstance(project, dict):
            continue
        for member in _as_list(project.get("member_files")):
            path = _clean(member)
            lower = path.lower()
            if "dataenvironment" in lower:
                data_environments.add(path.split("/")[-1] or path)
            if "datareport" in lower:
                data_reports.add(path.split("/")[-1] or path)
            if ".dsr" in lower and ("report" in lower or "dataenvironment" in lower):
                report_entrypoints.append(
                    {
                        "project": _clean(project.get("name") or project.get("project_id")),
                        "entry": path.split("/")[-1] or path,
                        "source": path,
                    }
                )
    for stmt in sql_statements:
        raw = _clean(stmt.get("raw"))
        if not raw:
            continue
        if re.search(r"\b[A-Za-z]:\\", raw):
            hardcoded_paths.add(raw[:220])

    mapped_reports = len(data_reports.intersection(data_environments))
    unknown_environments = sorted([x for x in data_environments if x not in data_reports])[:80]
    status = "PASS"
    if unknown_environments:
        status = "WARN"
    if hardcoded_paths:
        status = "FAIL"

    return {
        "artifact_type": "reporting_model",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "data_environments": sorted(data_environments)[:120],
        "data_reports": sorted(data_reports)[:120],
        "report_entrypoints": report_entrypoints[:200],
        "mapping": {
            "mapped_count": mapped_reports,
            "unknown_data_environments": unknown_environments,
        },
        "connection_path_mismatches": sorted(hardcoded_paths)[:40],
        "status": status,
    }


def _build_identity_access_model(
    *,
    metadata_common: dict[str, Any],
    sql_statements: list[dict[str, Any]],
    database_touchpoints: list[str],
) -> dict[str, Any]:
    auth_tables: set[str] = set()
    has_role_column = False
    has_plaintext_credential_signal = False
    sensitive_queries: list[str] = []
    for table in database_touchpoints:
        token = _clean(table).lower()
        if token and any(x in token for x in ("user", "login", "auth", "credential", "account", "logi")):
            auth_tables.add(_clean(table))
    for stmt in sql_statements:
        raw = _clean(stmt.get("raw"))
        lower = raw.lower()
        if not raw:
            continue
        if any(tok in lower for tok in ("role", "permission", "access_level", "is_admin")):
            has_role_column = True
        if any(tok in lower for tok in ("pass", "password", "pwd")):
            has_plaintext_credential_signal = True
            if len(sensitive_queries) < 20:
                sensitive_queries.append(raw[:240])
        for table in _as_list(stmt.get("tables")):
            t = _clean(table)
            tl = t.lower()
            if t and any(x in tl for x in ("user", "login", "auth", "credential", "account", "logi")):
                auth_tables.add(t)
    status = "PASS"
    if not has_role_column:
        status = "WARN"
    if has_plaintext_credential_signal:
        status = "FAIL"
    return {
        "artifact_type": "identity_access_model",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "what_we_found": {
            "auth_tables": sorted(auth_tables)[:80],
            "role_model_present": has_role_column,
            "plaintext_credential_signal": has_plaintext_credential_signal,
        },
        "what_we_did_not_find": [
            "Explicit role/permission model in schema or queries."
        ] if not has_role_column else [],
        "questions_to_confirm": [
            "Is this system single-user or multi-user under concurrent teller/ops usage?",
            "What authorization model should be preserved or introduced during modernization?",
        ],
        "sensitive_query_samples": sensitive_queries,
        "status": status,
    }


def _build_discover_review_checklist(
    *,
    metadata_common: dict[str, Any],
    form_coverage_rows: list[dict[str, Any]],
    variant_diff_report: dict[str, Any],
    reporting_model: dict[str, Any],
    identity_access_model: dict[str, Any],
    db_qa_report: dict[str, Any],
    sql_statements: list[dict[str, Any]],
    detector_findings: list[dict[str, Any]],
    business_rules: list[dict[str, Any]],
    procedure_summaries: list[dict[str, Any]],
    sql_map_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    low_coverage = [row for row in form_coverage_rows if _to_float(row.get("coverage_score"), 0.0) < 0.85]
    injection_or_credential = False
    sql_safety_signals = 0
    for stmt in sql_statements:
        risks = {_clean(x).lower() for x in _as_list(stmt.get("risk_flags")) if _clean(x)}
        raw = _clean(stmt.get("raw")).lower()
        if risks.intersection({"possible_injection", "string_concatenation", "sensitive_credential_query", "jet_delete_wildcard"}):
            injection_or_credential = True
            sql_safety_signals += 1
            continue
        if (
            "select *" in raw
            and (" where " in raw)
            and (raw.endswith("'") or raw.endswith("=") or " pass" in raw or "password" in raw)
        ):
            injection_or_credential = True
            sql_safety_signals += 1
            continue
        if raw.startswith("delete ") and "transaction" in raw and "customer" in raw and "where" in raw:
            sql_safety_signals += 1
            continue
    if not injection_or_credential:
        for det in detector_findings:
            text = f"{_clean(det.get('detector_id'))} {_clean(det.get('summary'))}".lower()
            if any(tok in text for tok in ("injection", "credential", "security")):
                injection_or_credential = True
                sql_safety_signals += 1
                break

    def _delete_by_customer_without_transaction_key(stmt: dict[str, Any]) -> bool:
        raw = _clean(stmt.get("raw")).lower()
        kind = _clean(stmt.get("kind")).lower()
        tables = [_clean(x).lower() for x in _as_list(stmt.get("tables")) if _clean(x)]
        is_delete = kind == "delete" or raw.startswith("delete ")
        if not is_delete:
            return False
        transaction_table_detected = any(_is_transaction_like_name(tbl) for tbl in tables)
        if not transaction_table_detected:
            transaction_table_detected = bool(
                re.search(r"\bdelete(?:\s+\*)?\s+from?\s+[a-z0-9_]*trans[a-z0-9_]*", raw)
            ) or bool(re.search(r"\bdelete\b.*\btrans[a-z0-9_]*\b", raw))
        if not transaction_table_detected:
            return False
        has_where = " where " in raw
        if not has_where:
            return True
        has_customer_condition = any(token in raw for token in ("customerid", "customer_id", "txtcustomerid"))
        has_transaction_key = any(token in raw for token in ("transactionid", "transaction_id", "txnid", "txn_id", "transid"))
        if has_customer_condition and not has_transaction_key:
            return True
        if not has_customer_condition and not has_transaction_key:
            return True
        return False

    delete_by_customer_without_key = False
    for stmt in sql_statements:
        if _delete_by_customer_without_transaction_key(stmt if isinstance(stmt, dict) else {}):
            delete_by_customer_without_key = True
            break

    def _contains_any(text: str, tokens: tuple[str, ...]) -> bool:
        lower = _clean(text).lower()
        if not lower:
            return False
        return any(token in lower for token in tokens)

    transaction_tokens = ("transaction", "transactions", "transction", "tbltransaction", "tbltransactions")
    customer_tokens = ("customerid", "customer_id", "txtcustomerid", "by customer", "customer")
    transaction_key_tokens = ("transactionid", "transaction_id", "txnid", "txn_id", "transid")

    delete_signal_count = 0
    fragmented_delete_signals = 0
    transaction_context_detected = False
    customer_scope_detected = False
    transaction_key_detected = False
    transaction_table_stmt_count = 0
    transaction_table_stmt_with_key_count = 0
    transaction_table_customer_no_key_count = 0
    delete_context_blobs: list[str] = []

    for stmt in sql_statements:
        if not isinstance(stmt, dict):
            continue
        raw = _clean(stmt.get("raw"))
        normalized = _clean(stmt.get("normalized"))
        kind = _clean(stmt.get("kind")).lower()
        tables = [_clean(x) for x in _as_list(stmt.get("tables")) if _clean(x)]
        blob = " ".join([raw, normalized, " ".join(tables)])
        is_delete = kind == "delete" or _contains_any(raw, ("delete",))
        if is_delete:
            delete_signal_count += 1
            raw_l = raw.lower()
            if "from " not in raw_l or " where " not in raw_l:
                fragmented_delete_signals += 1
            delete_context_blobs.append(blob)
        is_transaction_stmt = any(_is_transaction_like_name(tbl) for tbl in tables) or _contains_any(blob, transaction_tokens)
        if is_transaction_stmt:
            transaction_context_detected = True
            transaction_table_stmt_count += 1
            if _contains_any(blob, transaction_key_tokens):
                transaction_table_stmt_with_key_count += 1
            if _contains_any(blob, customer_tokens) and not _contains_any(blob, transaction_key_tokens):
                transaction_table_customer_no_key_count += 1
        if _contains_any(blob, customer_tokens):
            customer_scope_detected = True
        if _contains_any(blob, transaction_key_tokens):
            transaction_key_detected = True

    for entry in sql_map_entries[:2400]:
        if not isinstance(entry, dict):
            continue
        operation = _clean(entry.get("operation")).lower()
        tables = [_clean(x) for x in _as_list(entry.get("tables")) if _clean(x)]
        proc = _clean(entry.get("procedure"))
        form = _clean(entry.get("form"))
        blob = " ".join([operation, proc, form, " ".join(tables)])
        is_delete = operation == "delete" or _contains_any(blob, ("delete",))
        if is_delete:
            delete_signal_count += 1
            delete_context_blobs.append(blob)
        if any(_is_transaction_like_name(tbl) for tbl in tables) or _contains_any(blob, transaction_tokens):
            transaction_context_detected = True
        if _contains_any(blob, customer_tokens):
            customer_scope_detected = True
        if _contains_any(blob, transaction_key_tokens):
            transaction_key_detected = True

    for proc in procedure_summaries[:1800]:
        if not isinstance(proc, dict):
            continue
        steps = " ".join(_clean(x) for x in _as_list(proc.get("steps")) if _clean(x))
        tables = " ".join(_clean(x) for x in _as_list(proc.get("tables_touched")) if _clean(x))
        blob = " ".join(
            [
                _clean(proc.get("form")),
                _clean(proc.get("procedure_name")),
                _clean(proc.get("summary")),
                steps,
                tables,
            ]
        )
        is_delete = _contains_any(blob, ("delete",))
        if is_delete:
            delete_signal_count += 1
            delete_context_blobs.append(blob)
        if _contains_any(blob, transaction_tokens):
            transaction_context_detected = True
        if _contains_any(blob, customer_tokens):
            customer_scope_detected = True
        if _contains_any(blob, transaction_key_tokens):
            transaction_key_detected = True

    for rule in business_rules[:1200]:
        if not isinstance(rule, dict):
            continue
        blob = " ".join(
            [
                _clean(rule.get("statement")),
                _clean(_as_dict(rule.get("scope")).get("component_id")),
                _clean(_as_dict(rule.get("scope")).get("project_id")),
            ]
        )
        is_delete = _contains_any(blob, ("delete",))
        if is_delete:
            delete_signal_count += 1
            delete_context_blobs.append(blob)
        if _contains_any(blob, transaction_tokens):
            transaction_context_detected = True
        if _contains_any(blob, customer_tokens):
            customer_scope_detected = True
        if _contains_any(blob, transaction_key_tokens):
            transaction_key_detected = True

    delete_context_transaction_without_key = any(
        _contains_any(blob, transaction_tokens) and not _contains_any(blob, transaction_key_tokens)
        for blob in delete_context_blobs
    )
    delete_context_customer_transaction_without_key = any(
        _contains_any(blob, transaction_tokens)
        and _contains_any(blob, customer_tokens)
        and not _contains_any(blob, transaction_key_tokens)
        for blob in delete_context_blobs
    )
    weak_transaction_keying_signal = (
        transaction_table_stmt_count > 0
        and transaction_table_stmt_with_key_count == 0
        and transaction_table_customer_no_key_count > 0
    )

    secondary_hazard_strong = (
        not delete_by_customer_without_key
        and delete_signal_count > 0
        and (
            delete_context_customer_transaction_without_key
            or weak_transaction_keying_signal
            or (
                transaction_context_detected
                and customer_scope_detected
                and not transaction_key_detected
                and fragmented_delete_signals > 0
            )
        )
    )
    secondary_hazard_warn = (
        not delete_by_customer_without_key
        and not secondary_hazard_strong
        and (
            (
                delete_signal_count > 0
                and (
                    delete_context_transaction_without_key
                    or (transaction_context_detected and not transaction_key_detected)
                )
            )
            or (
                fragmented_delete_signals > 0
                and transaction_context_detected
                and customer_scope_detected
            )
        )
    )

    variant_schema = _as_dict(variant_diff_report.get("schema_divergence"))
    variant_schema_detected = bool(variant_schema.get("detected"))
    variant_schema_pairs = _as_list(variant_schema.get("pairs"))
    variant_schema_blocking = _as_list(variant_schema.get("blocking_pairs"))

    checks = [
        {
            "id": "handler_inventory_completeness",
            "title": "Handler Inventory Completeness",
            "status": "FAIL" if low_coverage else "PASS",
            "detail": (
                f"{len(low_coverage)} form(s) below 85% handler coverage."
                if low_coverage
                else "All analyzed forms meet handler coverage threshold."
            ),
        },
        {
            "id": "report_model_reconciled",
            "title": "Report Model Reconciled",
            "status": _clean(reporting_model.get("status")).upper() or "WARN",
            "detail": (
                "Reporting model contains unmapped DataEnvironment/DataReport objects or path mismatches."
                if _clean(reporting_model.get("status")).upper() in {"WARN", "FAIL"}
                else "Reporting model and entrypoints reconciled."
            ),
        },
        {
            "id": "variant_resolution",
            "title": "Variant Resolution",
            "status": "FAIL" if variant_schema_detected else (_clean(variant_diff_report.get("status")).upper() or "WARN"),
            "detail": (
                "Variant scope unresolved and schema divergence detected across project variants. "
                "Resolve DEC-VARIANT-001 before planning."
                if variant_schema_detected
                else (_clean(variant_diff_report.get("decision_prompt")) or "Variant scope decision required.")
            ),
        },
        {
            "id": "variant_schema_divergence",
            "title": "Variant Schema Divergence",
            "status": (
                "FAIL"
                if variant_schema_detected and variant_schema_blocking
                else ("WARN" if variant_schema_detected else "PASS")
            ),
            "detail": (
                f"Schema naming divergence detected in {len(variant_schema_pairs)} variant pair(s); "
                f"{len(variant_schema_blocking)} pair(s) include transaction-like table conflicts."
                if variant_schema_detected
                else "No cross-variant schema naming divergence detected."
            ),
        },
        {
            "id": "key_safety_issues_identified",
            "title": "Key Safety Issues Identified",
            "status": "PASS" if injection_or_credential else "FAIL",
            "detail": (
                f"Risk signals include SQL injection/credential handling issues ({sql_safety_signals} signal(s))."
                if injection_or_credential
                else "No explicit SQL/credential safety signals detected in extracted artifacts. Detection is incomplete and blocks progression."
            ),
        },
        {
            "id": "schema_key_verification",
            "title": "Schema Key Verification",
            "status": (
                "FAIL"
                if (delete_by_customer_without_key or secondary_hazard_strong)
                else ("WARN" if secondary_hazard_warn else "PASS")
            ),
            "detail": (
                "Delete-by-customer pattern detected in transaction scope without explicit transaction key."
                if delete_by_customer_without_key
                else (
                    "Fragmented delete/query evidence indicates transaction delete risk without explicit transaction key."
                    if secondary_hazard_strong
                    else (
                        "Transaction delete signals detected, but transaction key evidence is incomplete; requires manual verification."
                        if secondary_hazard_warn
                        else "No delete-by-customer transaction key hazard detected."
                    )
                )
            ),
        },
        {
            "id": "identity_access_model",
            "title": "Identity & Access Model",
            "status": _clean(identity_access_model.get("status")).upper() or "WARN",
            "detail": (
                "Role model or credential handling requires confirmation."
                if _clean(identity_access_model.get("status")).upper() in {"WARN", "FAIL"}
                else "Identity/access model signals are sufficiently captured."
            ),
        },
        {
            "id": "database_archaeology_ready",
            "title": "Database Archaeology & Mapping Readiness",
            "status": _clean(db_qa_report.get("overall_status")).upper() or "WARN",
            "detail": (
                "Source schema, target schema, and mapping matrix passed DB QA checks."
                if _clean(db_qa_report.get("overall_status")).upper() == "PASS"
                else (
                    "DB QA detected blocking or warning issues in schema reconstruction/mapping."
                )
            ),
        },
    ]
    overall = "PASS"
    if any(_clean(item.get("status")).upper() == "FAIL" for item in checks):
        overall = "FAIL"
    elif any(_clean(item.get("status")).upper() == "WARN" for item in checks):
        overall = "WARN"
    return {
        "artifact_type": "discover_review_checklist",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "overall_status": overall,
        "checks": checks,
        "low_coverage_forms": low_coverage[:80],
    }


def _build_repo_landscape(
    *,
    metadata_common: dict[str, Any],
    projects: list[dict[str, Any]],
) -> dict[str, Any]:
    landscape_projects: list[dict[str, Any]] = []
    shared_dependencies: set[str] = set()
    shared_modules: set[str] = set()
    for idx, project in enumerate(projects, start=1):
        if not isinstance(project, dict):
            continue
        forms = [_clean(x) for x in _as_list(project.get("forms")) if _clean(x)]
        members = [_clean(x) for x in _as_list(project.get("member_files")) if _clean(x)]
        dependencies = [_clean(x) for x in _as_list(project.get("dependencies")) if _clean(x)]
        tables = [_clean(x) for x in _as_list(_as_dict(project.get("data_touchpoints")).get("tables")) if _clean(x)]
        if idx == 1:
            shared_dependencies = set(d.lower() for d in dependencies)
            shared_modules = set(m.lower() for m in members)
        else:
            shared_dependencies &= set(d.lower() for d in dependencies)
            shared_modules &= set(m.lower() for m in members)
        landscape_projects.append(
            {
                "id": _clean(project.get("project_id")) or f"variant_{idx}",
                "path": _clean(project.get("file")) or _clean(project.get("project_file")),
                "startup": _clean(project.get("startup")) or _clean(project.get("startup_object")),
                "counts": {
                    "forms": len(forms),
                    "members": len(members),
                    "dependencies": len(dependencies),
                },
                "dependencies": dependencies[:80],
                "db_touchpoints": tables[:60],
                "drift_signals": [],
            }
        )
    return {
        "artifact_type": "repo_landscape",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "projects": landscape_projects,
        "shared_artifacts": {
            "shared_db_files": [],
            "shared_modules": sorted(shared_modules)[:120],
            "shared_dependencies": sorted(shared_dependencies)[:120],
        },
    }


def _build_scope_lock(
    *,
    metadata_common: dict[str, Any],
    projects: list[dict[str, Any]],
) -> dict[str, Any]:
    decision = "root_only" if len(projects) <= 1 else "pending"
    return {
        "artifact_type": "scope_lock",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "decision": decision,
        "rationale": (
            "Single canonical project detected."
            if decision != "pending"
            else "Multiple variants detected; choose root_only, variant_only, merge_first, or both_separate."
        ),
        "approver": "",
        "approved_at": "",
        "status": "LOCKED" if decision != "pending" else "REQUIRED",
    }


def _build_variant_inventory(
    *,
    metadata_common: dict[str, Any],
    projects: list[dict[str, Any]],
) -> dict[str, Any]:
    variants: list[dict[str, Any]] = []
    for idx, project in enumerate(projects, start=1):
        if not isinstance(project, dict):
            continue
        dependencies = [_clean(x) for x in _as_list(project.get("dependencies")) if _clean(x)]
        dep_summary = {
            "total": len(dependencies),
            "ocx": len([x for x in dependencies if x.lower().endswith(".ocx")]),
            "dll": len([x for x in dependencies if x.lower().endswith(".dll")]),
        }
        variants.append(
            {
                "id": _clean(project.get("project_id")) or f"variant_{idx}",
                "name": _clean(project.get("name")) or f"Variant {idx}",
                "forms": [_clean(x) for x in _as_list(project.get("forms")) if _clean(x)][:200],
                "modules": [_clean(x) for x in _as_list(project.get("member_files")) if _clean(x) and _clean(x).lower().endswith(".bas")][:120],
                "tables_touched": [_clean(x) for x in _as_list(_as_dict(project.get("data_touchpoints")).get("tables")) if _clean(x)][:120],
                "dependencies_summary": dep_summary,
            }
        )
    return {
        "artifact_type": "variant_inventory",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "variants": variants,
    }


def _estimate_proc_complexity(proc: dict[str, Any]) -> int:
    text = " ".join(
        [
            _clean(_as_dict(proc).get("procedure_name")),
            _clean(_as_dict(proc).get("summary")),
            " ".join(_clean(x) for x in _as_list(_as_dict(proc).get("steps")) if _clean(x)),
        ]
    ).lower()
    if not text:
        return 1
    # Bound analysis to keep deterministic scan cost stable on very large procedure summaries.
    text = text[:4000]
    tokens = [tok for tok in re.split(r"[^a-z0-9_]+", text) if tok]
    token_count = {
        "if": 0,
        "elseif": 0,
        "case": 0,
        "for": 0,
        "while": 0,
        "do": 0,
    }
    for tok in tokens:
        if tok in token_count:
            token_count[tok] += 1
    branch_hits = (
        token_count["if"]
        + token_count["elseif"]
        + token_count["case"]
        + token_count["for"]
        + token_count["while"]
    )
    # Handle simple two-token constructs.
    lowered = " ".join(tokens)
    branch_hits += lowered.count("select case")
    branch_hits += lowered.count("for each")
    branch_hits += lowered.count("do while")
    branch_hits += lowered.count("do until")
    return max(1, min(50, 1 + branch_hits))


def _is_event_handler_name(name: str) -> bool:
    token = _clean(name).lower()
    if not token:
        return False
    return bool(
        re.search(
            r"_(click|dblclick|change|load|initialize|keypress|keydown|keyup|gotfocus|lostfocus|timer|buttonclick|buttonmenuclick)$",
            token,
        )
    )


def _build_engineering_quality_baseline(
    *,
    metadata_common: dict[str, Any],
    run_id: str,
    generated_at: str,
    repo: str,
    branch: str,
    commit_sha: str,
    projects: list[dict[str, Any]],
    forms: list[Any],
    event_entries: list[dict[str, Any]],
    sql_statements: list[dict[str, Any]],
    sql_map_entries: list[dict[str, Any]],
    procedure_summaries: list[dict[str, Any]],
    dependencies: list[dict[str, Any]],
    form_coverage_rows: list[dict[str, Any]],
    source_loc_by_file: dict[str, int],
    orphan_analysis: dict[str, Any],
    form_dossier: dict[str, Any],
) -> dict[str, dict[str, Any]]:
    project_forms: dict[str, set[str]] = {}
    project_members: dict[str, list[str]] = {}
    for p in projects:
        if not isinstance(p, dict):
            continue
        pname = _clean(p.get("name") or p.get("project_id")) or "n/a"
        forms_in_project = {
            _normalize_form_name_token(x)
            for x in _as_list(p.get("forms"))
            if _normalize_form_name_token(x)
        }
        project_forms[pname] = forms_in_project
        project_members[pname] = [_clean(x) for x in _as_list(p.get("member_files")) if _clean(x)]

    form_rows: list[dict[str, Any]] = [row for row in forms if isinstance(row, dict)]
    known_form_norms: set[str] = set()
    form_project_by_norm: dict[str, str] = {}
    form_source_loc: dict[tuple[str, str], int] = {}
    form_controls: dict[tuple[str, str], list[str]] = {}
    form_event_handlers: dict[tuple[str, str], list[str]] = {}
    for row in form_rows:
        form_name = _clean(row.get("base_form_name") or row.get("form_name") or row.get("name"))
        norm = _normalize_form_name_token(form_name)
        if not norm:
            continue
        project_name = _clean(row.get("project_name")) or next(
            (p for p, fset in project_forms.items() if norm in fset),
            "n/a",
        )
        key = (project_name, norm)
        known_form_norms.add(norm)
        form_project_by_norm.setdefault(norm, project_name)
        form_source_loc[key] = int(row.get("source_loc", 0) or 0)
        form_controls[key] = [_clean(x) for x in _as_list(row.get("controls")) if _clean(x)]
        form_event_handlers[key] = [_clean(x) for x in _as_list(row.get("event_handlers")) if _clean(x)]

    def _project_for_form(norm: str, scoped: str = "") -> str:
        scoped_project, scoped_form = _split_variant_form(scoped)
        scoped_norm = _normalize_form_name_token(scoped_form)
        if scoped_project and scoped_norm == norm:
            return scoped_project
        return form_project_by_norm.get(norm, "n/a")

    proc_by_form: dict[tuple[str, str], list[dict[str, Any]]] = {}
    called_proc_names: set[str] = set()
    for proc in procedure_summaries[:4000]:
        if not isinstance(proc, dict):
            continue
        form_ref = _clean(proc.get("form"))
        _, base = _split_variant_form(form_ref)
        norm = _normalize_form_name_token(base or form_ref)
        if not norm:
            continue
        project_name = _project_for_form(norm, form_ref)
        proc_by_form.setdefault((project_name, norm), []).append(proc)
        pname = _clean(proc.get("procedure_name")).lower()
        if pname:
            for nav in _as_list(proc.get("navigation_side_effects")):
                nav_token = _clean(nav).lower()
                if nav_token and nav_token == pname:
                    called_proc_names.add(nav_token)

    event_trigger_controls: dict[tuple[str, str], set[str]] = {}
    shared_module_calls: set[str] = {
        _clean(proc.get("procedure_name")).lower()
        for proc in procedure_summaries
        if isinstance(proc, dict) and _normalize_form_name_token(proc.get("form")) in {"sharedmodule", "module", "shared_module"}
    }
    for entry in event_entries[:5000]:
        if not isinstance(entry, dict):
            continue
        source = _clean(entry.get("container") or entry.get("form") or _clean(_as_dict(entry.get("handler")).get("symbol")).rsplit("::", 1)[0])
        _, base = _split_variant_form(source)
        src_norm = _normalize_form_name_token(base or source)
        if not src_norm:
            continue
        src_project = _project_for_form(src_norm, source)
        key = (src_project, src_norm)
        control = _clean(_as_dict(entry.get("trigger")).get("control")).lower()
        if control:
            event_trigger_controls.setdefault(key, set()).add(control)
        for call in [_clean(c) for c in _as_list(entry.get("calls")) if _clean(c)]:
            call_l = call.lower()
            if call_l in shared_module_calls:
                called_proc_names.add(call_l)

    sql_by_form: dict[tuple[str, str], dict[str, set[str]]] = {}
    for row in sql_map_entries[:6000]:
        if not isinstance(row, dict):
            continue
        form_ref = _clean(row.get("form"))
        _, base = _split_variant_form(form_ref)
        norm = _normalize_form_name_token(base or form_ref)
        if not norm:
            continue
        project_name = _project_for_form(norm, form_ref)
        key = (project_name, norm)
        bucket = sql_by_form.setdefault(key, {"sql_ids": set(), "tables": set()})
        sid = _clean(row.get("sql_id"))
        if sid:
            bucket["sql_ids"].add(sid)
        for t in _as_list(row.get("tables")):
            clean_t = _clean(t)
            if clean_t:
                bucket["tables"].add(clean_t)

    type_dependency_edges: list[dict[str, Any]] = []
    runtime_dependency_edges: list[dict[str, Any]] = []
    dep_seen: set[str] = set()
    runtime_seen: set[str] = set()

    def _add_type_edge(source: str, target: str, dep_class: str, evidence: str, confidence: float = 0.78) -> None:
        if not source or not target:
            return
        key = f"{source}|{target}|{dep_class}|{evidence}".lower()
        if key in dep_seen:
            return
        dep_seen.add(key)
        type_dependency_edges.append(
            {
                "edge_id": f"tdep:{len(type_dependency_edges) + 1}",
                "from_type": source,
                "to_type": target,
                "dependency_class": dep_class,
                "evidence": evidence,
                "confidence": round(max(0.0, min(1.0, confidence)), 3),
            }
        )

    for entry in event_entries[:5000]:
        if not isinstance(entry, dict):
            continue
        source = _clean(entry.get("container") or entry.get("form") or _clean(_as_dict(entry.get("handler")).get("symbol")).rsplit("::", 1)[0])
        src_proj, src_base = _split_variant_form(source)
        src_norm = _normalize_form_name_token(src_base or source)
        if not src_norm:
            continue
        project_name = src_proj or _project_for_form(src_norm, source)
        src_type = f"{project_name}::{src_norm}"
        evidence = _clean(_as_dict(entry.get("handler")).get("symbol")) or f"{source}::event"
        for call in [_clean(c) for c in _as_list(entry.get("calls")) if _clean(c)]:
            call_low = call.lower()
            if call_low in {"end", "quit", "app.end", "endapp"}:
                _add_type_edge(src_type, "runtime::end", "navigation_termination", evidence, 0.82)
                continue
            if call_low in {"frm", "form"}:
                _add_type_edge(src_type, "runtime::unresolved_form_ref", "navigation_unresolved", evidence, 0.6)
                continue
            if call_low in shared_module_calls:
                _add_type_edge(src_type, f"shared_module::{call}", "shared_module_call", evidence, 0.86)
                continue
            if call_low.startswith(("rpt", "datareport")):
                _add_type_edge(src_type, f"report::{call}", "report_navigation", evidence, 0.8)
                continue
            target_norm = _normalize_form_name_token(call)
            if target_norm and (target_norm in known_form_norms or call_low.startswith(("frm", "form")) or call_low == "main"):
                target_project = _project_for_form(target_norm, call)
                _add_type_edge(src_type, f"{target_project}::{target_norm}", "form_navigation", evidence, 0.82)

    for dep in dependencies[:1200]:
        if not isinstance(dep, dict):
            continue
        dep_name = _clean(dep.get("name"))
        if not dep_name:
            continue
        dep_kind = _clean(dep.get("kind")) or "dependency"
        forms_mapped = [_clean(x) for x in _as_list(_as_dict(dep.get("usage")).get("used_by")) if _clean(x)]
        for mapped in forms_mapped:
            p, base = _split_variant_form(mapped)
            norm = _normalize_form_name_token(base or mapped)
            if not norm:
                continue
            src_project = p or _project_for_form(norm, mapped)
            src_type = f"{src_project}::{norm}"
            key = f"{src_type}|{dep_name}|{dep_kind}".lower()
            if key in runtime_seen:
                continue
            runtime_seen.add(key)
            runtime_dependency_edges.append(
                {
                    "edge_id": f"rdep:{len(runtime_dependency_edges) + 1}",
                    "from_type": src_type,
                    "to_runtime": dep_name,
                    "runtime_class": dep_kind,
                    "confidence": 0.82,
                }
            )

    dossiers = _as_list(_as_dict(form_dossier).get("dossiers"))
    for d in dossiers[:1200]:
        if not isinstance(d, dict):
            continue
        form_name = _clean(d.get("form_name"))
        norm = _normalize_form_name_token(form_name)
        if not norm:
            continue
        project_name = _clean(d.get("project_name")) or _project_for_form(norm, form_name)
        src_type = f"{project_name}::{norm}"
        for dep_name in [_clean(x) for x in _clean(d.get("activex")).split(",") if _clean(x)]:
            key = f"{src_type}|{dep_name}|activex".lower()
            if key in runtime_seen:
                continue
            runtime_seen.add(key)
            runtime_dependency_edges.append(
                {
                    "edge_id": f"rdep:{len(runtime_dependency_edges) + 1}",
                    "from_type": src_type,
                    "to_runtime": dep_name,
                    "runtime_class": "activex_dependency",
                    "confidence": 0.84,
                }
            )
        for table in [_clean(x) for x in _clean(d.get("db_tables")).split(",") if _clean(x)]:
            key = f"{src_type}|{table}|database_table".lower()
            if key in runtime_seen:
                continue
            runtime_seen.add(key)
            runtime_dependency_edges.append(
                {
                    "edge_id": f"rdep:{len(runtime_dependency_edges) + 1}",
                    "from_type": src_type,
                    "to_runtime": table,
                    "runtime_class": "database_table",
                    "confidence": 0.86,
                }
            )

    inbound_edges: dict[str, set[str]] = {}
    outbound_edges: dict[str, set[str]] = {}
    for edge in type_dependency_edges:
        src = _clean(edge.get("from_type"))
        dst = _clean(edge.get("to_type"))
        if not src or not dst:
            continue
        outbound_edges.setdefault(src, set()).add(dst)
        inbound_edges.setdefault(dst, set()).add(src)

    type_metrics_rows: list[dict[str, Any]] = []
    for key, loc in sorted(form_source_loc.items()):
        project_name, norm = key
        controls = form_controls.get(key, [])
        handlers = form_event_handlers.get(key, [])
        related_procs = proc_by_form.get(key, [])
        method_count = max(len(set([x.lower() for x in handlers if x])), len({_clean(p.get("procedure_name")).lower() for p in related_procs if _clean(p.get("procedure_name"))}))
        method_count = max(method_count, len(related_procs))
        sql_bucket = sql_by_form.get(key, {"sql_ids": set(), "tables": set()})
        sql_touch_count = len(sql_bucket.get("sql_ids", set()))
        table_touch_count = len(sql_bucket.get("tables", set()))
        complexities = [_estimate_proc_complexity(p) for p in related_procs]
        cyclomatic = max(1, int(round(sum(complexities) / float(len(complexities))))) if complexities else 1
        field_count = len(controls)
        lcom = 0.0
        if method_count > 1:
            if table_touch_count <= 0 and field_count > 0:
                lcom = 0.95
            else:
                lcom = max(0.0, min(1.0, 1.0 - (min(table_touch_count, method_count) / float(max(1, method_count)))))
        type_key = f"{project_name}::{norm}"
        aff = len(inbound_edges.get(type_key, set()))
        eff = len(outbound_edges.get(type_key, set()))
        dep_count = eff + len([e for e in runtime_dependency_edges if _clean(e.get("from_type")) == type_key])
        type_metrics_rows.append(
            {
                "type_id": f"type:{len(type_metrics_rows) + 1}",
                "project": project_name,
                "type_name": norm,
                "kind": "form",
                "loc": int(loc or 0),
                "comment_lines": 0,
                "comment_percentage": 0.0,
                "cyclomatic_complexity": cyclomatic,
                "afferent_coupling": aff,
                "efferent_coupling": eff,
                "field_count": field_count,
                "method_count": method_count,
                "lack_of_cohesion": round(lcom, 3),
                "dependency_count": dep_count,
                "ui_control_count": field_count,
                "sql_touch_count": sql_touch_count,
                "table_touch_count": table_touch_count,
            }
        )

    for project_name, members in project_members.items():
        for member in members:
            lower = member.lower()
            if not (lower.endswith(".bas") or lower.endswith(".cls")):
                continue
            base = member.replace("\\", "/").split("/")[-1]
            kind = "module" if lower.endswith(".bas") else "class"
            module_name = re.sub(r"\.(bas|cls)$", "", base, flags=re.IGNORECASE)
            loc = int(source_loc_by_file.get(member, source_loc_by_file.get(base, 0)) or 0)
            norm = _normalize_form_name_token(module_name) or module_name.lower()
            related_procs = [p for p in procedure_summaries if isinstance(p, dict) and _normalize_form_name_token(_clean(p.get("form"))) == norm]
            complexities = [_estimate_proc_complexity(p) for p in related_procs]
            cyclomatic = max(1, int(round(sum(complexities) / float(len(complexities))))) if complexities else 1
            type_key = f"{project_name}::{norm}"
            aff = len(inbound_edges.get(type_key, set()))
            eff = len(outbound_edges.get(type_key, set()))
            type_metrics_rows.append(
                {
                    "type_id": f"type:{len(type_metrics_rows) + 1}",
                    "project": project_name,
                    "type_name": norm,
                    "kind": kind,
                    "loc": loc,
                    "comment_lines": 0,
                    "comment_percentage": 0.0,
                    "cyclomatic_complexity": cyclomatic,
                    "afferent_coupling": aff,
                    "efferent_coupling": eff,
                    "field_count": 0,
                    "method_count": len(related_procs),
                    "lack_of_cohesion": 0.0,
                    "dependency_count": eff,
                    "ui_control_count": 0,
                    "sql_touch_count": 0,
                    "table_touch_count": 0,
                }
            )

    dead_type_candidates: list[dict[str, Any]] = []
    for row in _as_list(form_coverage_rows):
        if not isinstance(row, dict):
            continue
        coverage = _to_float(row.get("coverage_score"), 0.0)
        sql_touched = int(row.get("sql_touched_count", 0) or 0)
        loc = int(row.get("source_loc", 0) or 0)
        if coverage <= 0.05 and sql_touched == 0 and loc > 0:
            dead_type_candidates.append(
                {
                    "name": _clean(row.get("form_name")),
                    "project": _clean(row.get("project_name")) or "n/a",
                    "reason": "No extracted handlers or SQL touchpoints at near-zero traceability coverage.",
                    "confidence": 0.72,
                }
            )
    for orphan in _as_list(_as_dict(orphan_analysis).get("orphans"))[:120]:
        if not isinstance(orphan, dict):
            continue
        rec = _clean(orphan.get("recommendation")).lower()
        if rec not in {"exclude_or_defer", "reconcile_project_membership", "verify"}:
            continue
        dead_type_candidates.append(
            {
                "name": _clean(orphan.get("form") or orphan.get("path")),
                "project": "n/a",
                "reason": _clean(orphan.get("reason")) or "Orphaned/not mapped to active project members.",
                "confidence": 0.64 if rec == "verify" else 0.78,
            }
        )
    dedup_dead_types: list[dict[str, Any]] = []
    seen_dead_type: set[str] = set()
    for row in dead_type_candidates:
        key = f"{_clean(row.get('project'))}|{_clean(row.get('name')).lower()}"
        if not key or key in seen_dead_type:
            continue
        seen_dead_type.add(key)
        dedup_dead_types.append(row)

    dead_method_candidates: list[dict[str, Any]] = []
    for proc in procedure_summaries[:3000]:
        if not isinstance(proc, dict):
            continue
        pname = _clean(proc.get("procedure_name"))
        if not pname:
            continue
        pl = pname.lower()
        if _is_event_handler_name(pname):
            continue
        if pl in called_proc_names:
            continue
        if _as_list(proc.get("sql_ids")) or _as_list(proc.get("tables_touched")):
            continue
        if _as_list(proc.get("navigation_side_effects")):
            continue
        dead_method_candidates.append(
            {
                "name": pname,
                "form": _clean(proc.get("form")),
                "reason": "No inbound call evidence and no SQL/navigation side effects.",
                "confidence": 0.66,
            }
        )

    dead_field_candidates: list[dict[str, Any]] = []
    for key, controls in form_controls.items():
        project_name, norm = key
        triggers = event_trigger_controls.get(key, set())
        if not controls:
            continue
        for ctl in controls:
            ctl_low = _clean(ctl).lower()
            if not ctl_low:
                continue
            if ctl_low in triggers:
                continue
            dead_field_candidates.append(
                {
                    "name": ctl,
                    "form": norm,
                    "project": project_name,
                    "reason": "Control not observed in event trigger map; verify designer-only/unused field.",
                    "confidence": 0.55,
                }
            )
            if len(dead_field_candidates) >= 240:
                break

    third_party_rows: list[dict[str, Any]] = []
    runtime_by_dep: dict[str, set[str]] = {}
    for edge in runtime_dependency_edges:
        dep_name = _clean(edge.get("to_runtime"))
        src = _clean(edge.get("from_type"))
        if not dep_name or not src:
            continue
        runtime_by_dep.setdefault(dep_name.lower(), set()).add(src)

    for dep in dependencies[:1200]:
        if not isinstance(dep, dict):
            continue
        dep_name = _clean(dep.get("name"))
        if not dep_name:
            continue
        kind = _clean(dep.get("kind")) or "dependency"
        forms_using = sorted(runtime_by_dep.get(dep_name.lower(), set()))
        methods_using = 0
        for entry in event_entries[:5000]:
            if not isinstance(entry, dict):
                continue
            source = _clean(entry.get("container") or entry.get("form"))
            src_proj, src_base = _split_variant_form(source)
            src_norm = _normalize_form_name_token(src_base or source)
            if not src_norm:
                continue
            src_project = src_proj or _project_for_form(src_norm, source)
            if f"{src_project}::{src_norm}" in forms_using:
                methods_using += 1
        replaceability = 0.8
        if kind in {"ocx", "activex", "com_typelib", "dca", "dcx"}:
            replaceability = 0.45
        elif kind == "dll":
            replaceability = 0.62
        if len(forms_using) > 8:
            replaceability -= 0.15
        elif len(forms_using) > 3:
            replaceability -= 0.08
        replaceability = max(0.1, min(0.95, replaceability))
        runtime_critical = any("::main" in f.lower() or "::login" in f.lower() for f in forms_using) or methods_using >= 6
        usage_intensity = round((len(forms_using) * 1.0) + (methods_using * 0.15), 2)
        third_party_rows.append(
            {
                "dependency": dep_name,
                "kind": kind,
                "forms_using_count": len(forms_using),
                "methods_using_count": methods_using,
                "runtime_critical": runtime_critical,
                "replaceability_score": round(replaceability, 3),
                "usage_intensity": usage_intensity,
                "forms_sample": forms_using[:10],
            }
        )

    quality_rules = [
        {"rule_id": "CQ-R001", "title": "High cyclomatic complexity", "severity": "high", "threshold": "cyclomatic_complexity > 15"},
        {"rule_id": "CQ-R002", "title": "High coupling", "severity": "medium", "threshold": "afferent_coupling + efferent_coupling > 12"},
        {"rule_id": "CQ-R003", "title": "Low cohesion", "severity": "medium", "threshold": "lack_of_cohesion > 0.9"},
        {"rule_id": "CQ-R004", "title": "Mega form / god module", "severity": "high", "threshold": "ui_control_count > 25 or method_count > 40"},
        {"rule_id": "CQ-R005", "title": "SQL string concatenation risk", "severity": "high", "threshold": "sql.risk_flags includes string_concatenation"},
        {"rule_id": "CQ-R006", "title": "Default form instance usage", "severity": "medium", "threshold": "detector/default_instance_refs > 0"},
        {"rule_id": "CQ-R007", "title": "Control array complexity", "severity": "medium", "threshold": "detector/control_array_index_markers > 0"},
        {"rule_id": "CQ-R008", "title": "Manual ID generation", "severity": "medium", "threshold": "query contains MAX(id)+1 style pattern"},
    ]

    violations: list[dict[str, Any]] = []
    for row in type_metrics_rows[:6000]:
        if not isinstance(row, dict):
            continue
        name = f"{_clean(row.get('project'))}::{_clean(row.get('type_name'))}"
        if int(row.get("cyclomatic_complexity", 0) or 0) > 15:
            violations.append({"violation_id": f"qv:{len(violations)+1}", "rule_id": "CQ-R001", "severity": "high", "subject": name, "detail": f"Cyclomatic complexity={int(row.get('cyclomatic_complexity', 0) or 0)}"})
        coupling = int(row.get("afferent_coupling", 0) or 0) + int(row.get("efferent_coupling", 0) or 0)
        if coupling > 12:
            violations.append({"violation_id": f"qv:{len(violations)+1}", "rule_id": "CQ-R002", "severity": "medium", "subject": name, "detail": f"Coupling score={coupling}"})
        if _to_float(row.get("lack_of_cohesion"), 0.0) > 0.9:
            violations.append({"violation_id": f"qv:{len(violations)+1}", "rule_id": "CQ-R003", "severity": "medium", "subject": name, "detail": f"Lack of cohesion={_to_float(row.get('lack_of_cohesion'), 0.0):.2f}"})
        if int(row.get("ui_control_count", 0) or 0) > 25 or int(row.get("method_count", 0) or 0) > 40:
            violations.append({"violation_id": f"qv:{len(violations)+1}", "rule_id": "CQ-R004", "severity": "high", "subject": name, "detail": "High control/method density indicates mega-form or god-module risk."})

    for stmt in sql_statements[:4000]:
        if not isinstance(stmt, dict):
            continue
        flags = {_clean(x).lower() for x in _as_list(stmt.get("risk_flags")) if _clean(x)}
        sid = _clean(stmt.get("sql_id")) or "sql"
        if "string_concatenation" in flags or "possible_injection" in flags:
            violations.append({"violation_id": f"qv:{len(violations)+1}", "rule_id": "CQ-R005", "severity": "high", "subject": sid, "detail": "SQL statement uses concatenation/injection-prone pattern."})
        raw = _clean(stmt.get("raw")).lower()
        if re.search(r"\bmax\s*\([^)]*id[^)]*\)\s*\+\s*1\b", raw):
            violations.append({"violation_id": f"qv:{len(violations)+1}", "rule_id": "CQ-R008", "severity": "medium", "subject": sid, "detail": "Manual ID generation pattern detected."})

    high_violation_count = sum(1 for v in violations if _clean(v.get("severity")).lower() == "high")
    max_complexity = max((int(r.get("cyclomatic_complexity", 0) or 0) for r in type_metrics_rows), default=0)
    avg_complexity = (
        round(sum(int(r.get("cyclomatic_complexity", 0) or 0) for r in type_metrics_rows) / float(max(1, len(type_metrics_rows))), 3)
        if type_metrics_rows
        else 0.0
    )
    dead_volume = len(dedup_dead_types) + len(dead_method_candidates) + len(dead_field_candidates)
    active_x_usage = sum(1 for row in third_party_rows if _clean(row.get("kind")).lower() in {"ocx", "activex", "com_typelib"} and int(row.get("forms_using_count", 0) or 0) > 0)

    project_metrics_rows: list[dict[str, Any]] = []
    for project_name in sorted(project_forms.keys() or {"n/a"}):
        rows = [r for r in type_metrics_rows if _clean(_as_dict(r).get("project")) == project_name]
        forms_count = len([r for r in rows if _clean(_as_dict(r).get("kind")) == "form"])
        modules_count = len([r for r in rows if _clean(_as_dict(r).get("kind")) in {"module", "class"}])
        types_count = len(rows)
        loc_total = sum(int(_as_dict(r).get("loc", 0) or 0) for r in rows)
        comment_total = sum(int(_as_dict(r).get("comment_lines", 0) or 0) for r in rows)
        comment_pct = 0.0 if loc_total <= 0 else round((comment_total / float(loc_total)) * 100.0, 3)
        avg_complex = round(sum(int(_as_dict(r).get("cyclomatic_complexity", 0) or 0) for r in rows) / float(max(1, len(rows))), 3) if rows else 0.0
        max_complex = max((int(_as_dict(r).get("cyclomatic_complexity", 0) or 0) for r in rows), default=0)
        project_type_keys = {f"{project_name}::{_clean(_as_dict(r).get('type_name'))}" for r in rows}
        eff = len([e for e in type_dependency_edges if _clean(_as_dict(e).get("from_type")) in project_type_keys and _clean(_as_dict(e).get("to_type")) not in project_type_keys])
        aff = len([e for e in type_dependency_edges if _clean(_as_dict(e).get("to_type")) in project_type_keys and _clean(_as_dict(e).get("from_type")) not in project_type_keys])
        internal_edges = len([e for e in type_dependency_edges if _clean(_as_dict(e).get("from_type")) in project_type_keys and _clean(_as_dict(e).get("to_type")) in project_type_keys])
        density = 0.0 if types_count <= 1 else round(internal_edges / float(types_count * (types_count - 1)), 4)
        dep_count = len({_clean(_as_dict(d).get("name")).lower() for d in dependencies if _clean(_as_dict(d).get("name"))})
        dead_count = len([d for d in dedup_dead_types if _clean(_as_dict(d).get("project")) in {project_name, "", "n/a"}])
        third_party_count = len([t for t in third_party_rows if int(_as_dict(t).get("forms_using_count", 0) or 0) > 0])
        project_metrics_rows.append(
            {
                "project": project_name,
                "loc": loc_total,
                "comment_lines": comment_total,
                "comment_percentage": comment_pct,
                "types_count": types_count,
                "forms_count": forms_count,
                "modules_count": modules_count,
                "afferent_coupling": aff,
                "efferent_coupling": eff,
                "dependency_density": density,
                "avg_complexity": avg_complex,
                "max_complexity": max_complex,
                "dead_code_candidates": dead_count,
                "third_party_dependency_count": third_party_count if third_party_count > 0 else dep_count,
            }
        )

    snapshot = {
        "snapshot_id": f"trend:{generated_at}",
        "run_id": run_id,
        "repo": repo,
        "branch": branch,
        "commit_sha": commit_sha,
        "captured_at": generated_at,
        "metrics": {
            "loc_total": sum(int(_as_dict(r).get("loc", 0) or 0) for r in type_metrics_rows),
            "max_complexity": max_complexity,
            "avg_complexity": avg_complexity,
            "quality_violations": len(violations),
            "critical_violations": high_violation_count,
            "dead_code_volume": dead_volume,
            "activex_usage": active_x_usage,
            "third_party_dependencies": len(third_party_rows),
            "dependency_edges": len(type_dependency_edges),
            "hotspot_count": len([r for r in type_metrics_rows if int(_as_dict(r).get("cyclomatic_complexity", 0) or 0) > 15]),
        },
    }

    return {
        "project_metrics": {
            "artifact_type": "project_metrics",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "rows": project_metrics_rows[:120],
        },
        "type_metrics": {
            "artifact_type": "type_metrics",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "rows": type_metrics_rows[:2400],
        },
        "type_dependency_matrix": {
            "artifact_type": "type_dependency_matrix",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "edges": type_dependency_edges[:6000],
        },
        "runtime_dependency_matrix": {
            "artifact_type": "runtime_dependency_matrix",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "edges": runtime_dependency_edges[:6000],
        },
        "dead_code_report": {
            "artifact_type": "dead_code_report",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "summary": {
                "dead_type_candidates": len(dedup_dead_types),
                "dead_method_candidates": len(dead_method_candidates),
                "dead_field_candidates": len(dead_field_candidates),
            },
            "probable_dead_types": dedup_dead_types[:300],
            "probable_dead_methods": dead_method_candidates[:600],
            "probable_dead_fields": dead_field_candidates[:300],
            "verify_needed": [
                "VB6 event handlers are excluded from dead-method candidates unless corroborated by stronger evidence.",
            ],
        },
        "third_party_usage": {
            "artifact_type": "third_party_usage",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "rows": sorted(third_party_rows, key=lambda r: float(_as_dict(r).get("usage_intensity", 0.0)), reverse=True)[:600],
        },
        "code_quality_rules": {
            "artifact_type": "code_quality_rules",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "rules": quality_rules,
        },
        "quality_violation_report": {
            "artifact_type": "quality_violation_report",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "summary": {
                "total_violations": len(violations),
                "critical_violations": high_violation_count,
            },
            "violations": violations[:2000],
        },
        "trend_snapshot": {
            "artifact_type": "trend_snapshot",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "snapshot": snapshot,
        },
        "trend_series": {
            "artifact_type": "trend_series",
            "artifact_version": "1.0",
            "metadata": metadata_common,
            "series": [snapshot],
        },
    }


def _build_static_forensics_layer(
    *,
    metadata_common: dict[str, Any],
    project_metrics: dict[str, Any],
    type_metrics: dict[str, Any],
    type_dependency_matrix: dict[str, Any],
    runtime_dependency_matrix: dict[str, Any],
    dead_code_report: dict[str, Any],
    code_quality_rules: dict[str, Any],
    quality_violation_report: dict[str, Any],
    trend_snapshot: dict[str, Any],
    trend_series: dict[str, Any],
) -> dict[str, Any]:
    project_rows = _as_list(_as_dict(project_metrics).get("rows"))
    type_rows = _as_list(_as_dict(type_metrics).get("rows"))
    type_edges = _as_list(_as_dict(type_dependency_matrix).get("edges"))
    runtime_edges = _as_list(_as_dict(runtime_dependency_matrix).get("edges"))
    dead_summary = _as_dict(_as_dict(dead_code_report).get("summary"))
    rule_rows = _as_list(_as_dict(code_quality_rules).get("rules"))
    violation_rows = _as_list(_as_dict(quality_violation_report).get("violations"))
    trend_snap = _as_dict(_as_dict(trend_snapshot).get("snapshot"))
    trend_metrics = _as_dict(trend_snap.get("metrics"))
    trend_points = _as_list(_as_dict(trend_series).get("series"))
    dead_total = (
        int(dead_summary.get("dead_type_candidates", 0) or 0)
        + int(dead_summary.get("dead_method_candidates", 0) or 0)
        + int(dead_summary.get("dead_field_candidates", 0) or 0)
    )
    checks = [
        {
            "id": "sf_project_metrics",
            "label": "Project metrics baseline",
            "status": "pass" if len(project_rows) > 0 else "warn",
            "detail": f"Rows={len(project_rows)}",
        },
        {
            "id": "sf_type_metrics",
            "label": "Type metrics baseline",
            "status": "pass" if len(type_rows) > 0 else "warn",
            "detail": f"Rows={len(type_rows)}",
        },
        {
            "id": "sf_dependency_matrix",
            "label": "Dependency matrix coverage",
            "status": "pass" if (len(type_edges) + len(runtime_edges)) > 0 else "warn",
            "detail": f"Type edges={len(type_edges)} | Runtime edges={len(runtime_edges)}",
        },
        {
            "id": "sf_dead_code",
            "label": "Dead-code candidates",
            "status": "pass" if dead_summary else "warn",
            "detail": f"Candidates={dead_total}",
        },
        {
            "id": "sf_quality_rules",
            "label": "Quality rules loaded",
            "status": "pass" if len(rule_rows) > 0 else "warn",
            "detail": f"Rules={len(rule_rows)}",
        },
        {
            "id": "sf_violations",
            "label": "Quality violations analyzed",
            "status": "pass" if _as_dict(quality_violation_report) else "warn",
            "detail": f"Violations={len(violation_rows)}",
        },
        {
            "id": "sf_trends",
            "label": "Trend baseline",
            "status": "pass" if trend_snap else "warn",
            "detail": f"Snapshots={len(trend_points) or (1 if trend_snap else 0)} | LOC={int(trend_metrics.get('loc_total', 0) or 0)}",
        },
    ]
    fail_count = len([c for c in checks if _clean(_as_dict(c).get("status")).lower() == "fail"])
    warn_count = len([c for c in checks if _clean(_as_dict(c).get("status")).lower() == "warn"])
    overall_status = "PASS" if fail_count == 0 and warn_count == 0 else ("WARN" if fail_count == 0 else "FAIL")
    return {
        "artifact_type": "static_forensics_layer",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "scope": "discover_baseline",
        "summary": {
            "overall_status": overall_status,
            "projects": len(project_rows),
            "types": len(type_rows),
            "type_dependency_edges": len(type_edges),
            "runtime_dependency_edges": len(runtime_edges),
            "dead_code_candidates": dead_total,
            "quality_rules": len(rule_rows),
            "quality_violations": len(violation_rows),
            "trend_points": len(trend_points) or (1 if trend_snap else 0),
            "loc_total": int(trend_metrics.get("loc_total", 0) or 0),
        },
        "checks": checks,
    }


def _normalize_repo_path(value: Any) -> str:
    path = _clean(value).replace("\\", "/")
    path = re.sub(r"/+", "/", path).lstrip("./")
    return path


def _extract_project_member_path(value: Any) -> str:
    raw = _clean(value)
    if not raw:
        return ""
    if ":" in raw and not re.match(r"^[A-Za-z]:[\\/]", raw):
        prefix, remainder = raw.split(":", 1)
        if _clean(prefix).lower() in {"form", "module", "class", "designer", "mdiform", "control"} and _clean(remainder):
            raw = remainder
    return _normalize_repo_path(raw)


def _basename_without_ext(path: Any) -> str:
    name = _normalize_repo_path(path).split("/")[-1]
    if "." in name:
        return name.rsplit(".", 1)[0]
    return name


def _extract_db_paths(text: Any) -> list[str]:
    blob = _clean(text)
    if not blob:
        return []
    matches = re.findall(r"([A-Za-z0-9_./\\:-]+\.(?:mdb|accdb))", blob, flags=re.IGNORECASE)
    out: list[str] = []
    for value in matches:
        norm = _normalize_repo_path(value)
        if norm and norm.lower() not in {x.lower() for x in out}:
            out.append(norm)
    return out


def _looks_like_connection_string(text: Any) -> bool:
    token = _clean(text).lower()
    if not token:
        return False
    markers = (
        "provider=",
        "jet.oledb",
        "dbq=",
        "data source=",
        "datasource=",
        "driver=",
        ".mdb",
        ".accdb",
        "user id=",
        "uid=",
        "password=",
        "pwd=",
    )
    return any(marker in token for marker in markers)


def _normalize_connection_pattern(text: Any) -> str:
    blob = _clean(text)
    if not blob:
        return ""
    blob = re.sub(r"'[^']*'", "':value'", blob)
    blob = re.sub(r"\b\d+\b", ":num", blob)
    blob = re.sub(r"([A-Za-z0-9_./\\:-]+\.(?:mdb|accdb))", "<db-file>", blob, flags=re.IGNORECASE)
    blob = re.sub(r"\s+", " ", blob).strip()
    return blob


def _connection_variant_risks(text: Any) -> list[str]:
    token = _clean(text).lower()
    risks: list[str] = []
    if "jet.oledb" in token or "dao" in token:
        risks.append("legacy_access_provider")
    if any(k in token for k in ("password=", "pwd=", "user id=", "uid=")):
        risks.append("embedded_credentials")
    if re.search(r"(?i)(?:\.\.[\\/]|[A-Za-z0-9._-]+\.mdb\b|[A-Za-z0-9._-]+\.accdb\b)", token):
        if not re.search(r"^[a-z]:[\\/]", token):
            risks.append("relative_db_path")
    return risks


def _build_mdb_inventory(
    *,
    metadata_common: dict[str, Any],
    source_loc_rows: list[Any],
    project_members: list[Any],
    sql_rows: list[Any],
    ui_event_rows: list[Any],
    forms: list[Any],
    db_ref_rows: list[Any] | None = None,
    connection_string_rows: list[Any] | None = None,
    binary_companion_rows: list[Any] | None = None,
) -> dict[str, Any]:
    known_forms = {
        _normalize_form_name_token(_as_dict(f).get("form_name") or _as_dict(f).get("base_form_name") or _as_dict(f).get("name"))
        for f in forms
        if isinstance(f, dict)
    }
    known_forms = {x for x in known_forms if x}
    db_rows: dict[str, dict[str, Any]] = {}

    def _resolve_db_key(norm_path: str) -> str:
        norm_low = norm_path.lower()
        base = norm_low.split("/")[-1]
        # Prefer existing keyed path when basename matches a previously-seen entry.
        matches = [k for k, row in db_rows.items() if _clean(_as_dict(row).get("name")).lower() == base]
        if norm_low in db_rows:
            return norm_low
        if matches:
            if len(matches) == 1:
                return matches[0]
            # If one match is bare filename and another is fully qualified, prefer qualified key.
            qualified = [m for m in matches if "/" in m]
            if qualified:
                return sorted(qualified, key=len, reverse=True)[0]
            return matches[0]
        return norm_low

    def upsert_path(path: str, *, detected_from: str, evidence: str = "", source_loc: int = 0) -> None:
        norm = _normalize_repo_path(path)
        if not norm or not norm.lower().endswith((".mdb", ".accdb")):
            return
        key = _resolve_db_key(norm)
        row = db_rows.setdefault(
            key,
            {
                "db_id": f"mdb:{len(db_rows) + 1}",
                "path": norm,
                "name": norm.split("/")[-1],
                "extension": ".accdb" if norm.lower().endswith(".accdb") else ".mdb",
                "source_loc_proxy": 0,
                "detected_from": [],
                "referenced_by_forms": [],
                "referenced_by_modules": [],
                "evidence_refs": [],
            },
        )
        # Prefer fully qualified repo-relative path over bare filename aliases.
        if "/" in norm and ("/" not in _clean(row.get("path")) or len(norm) > len(_clean(row.get("path")))):
            row["path"] = norm
            row["name"] = norm.split("/")[-1]
        if detected_from and detected_from not in _as_list(row.get("detected_from")):
            row["detected_from"] = [*_as_list(row.get("detected_from")), detected_from]
        if evidence and evidence not in _as_list(row.get("evidence_refs")):
            row["evidence_refs"] = [*_as_list(row.get("evidence_refs")), evidence]
        if source_loc > 0:
            row["source_loc_proxy"] = max(int(row.get("source_loc_proxy", 0) or 0), int(source_loc))

    for row in source_loc_rows[:12000]:
        if not isinstance(row, dict):
            continue
        upsert_path(
            _clean(row.get("path")),
            detected_from="source_loc_by_file",
            evidence=_clean(row.get("path")),
            source_loc=int(row.get("loc", 0) or 0),
        )

    for member in project_members[:12000]:
        path = _extract_project_member_path(member)
        upsert_path(path, detected_from="project_member", evidence=_clean(member))

    for row in (db_ref_rows or [])[:12000]:
        if isinstance(row, dict):
            db_path = _clean(row.get("path") or row.get("db_path") or row.get("database"))
            evidence = _clean(row.get("evidence") or row.get("source") or row.get("handler"))
            form_token = _normalize_form_name_token(row.get("form"))
            module_token = _clean(row.get("module") or row.get("handler"))
        else:
            db_path = _clean(row)
            evidence = _clean(row)
            form_token = ""
            module_token = ""
        if not db_path:
            continue
        upsert_path(db_path, detected_from="db_reference", evidence=evidence)
        current = db_rows.get(_normalize_repo_path(db_path).lower())
        if not current:
            continue
        if form_token and form_token in known_forms:
            forms_ref = _as_list(current.get("referenced_by_forms"))
            if form_token not in forms_ref:
                current["referenced_by_forms"] = [*forms_ref, form_token]
        elif module_token:
            modules_ref = _as_list(current.get("referenced_by_modules"))
            if module_token not in modules_ref:
                current["referenced_by_modules"] = [*modules_ref, module_token]

    for idx, row in enumerate(sql_rows[:12000], start=1):
        raw = _clean(_as_dict(row).get("raw") or _as_dict(row).get("sql") or _as_dict(row).get("statement") or row)
        if not raw:
            continue
        for db_path in _extract_db_paths(raw):
            upsert_path(db_path, detected_from="sql_catalog", evidence=f"sql:{idx}")

    for idx, value in enumerate((connection_string_rows or [])[:12000], start=1):
        record = _as_dict(value)
        raw = _clean(record.get("raw") or record.get("connection_string") or value)
        if not raw:
            continue
        form_token = _normalize_form_name_token(record.get("form"))
        module_token = _clean(record.get("module") or record.get("handler") or record.get("source_file"))
        for db_path in _extract_db_paths(raw):
            upsert_path(db_path, detected_from="connection_string", evidence=f"conn:{idx}")
            current = db_rows.get(_resolve_db_key(_normalize_repo_path(db_path)))
            if not current:
                continue
            if form_token and form_token in known_forms:
                forms_ref = _as_list(current.get("referenced_by_forms"))
                if form_token not in forms_ref:
                    current["referenced_by_forms"] = [*forms_ref, form_token]
            elif module_token:
                modules_ref = _as_list(current.get("referenced_by_modules"))
                if module_token not in modules_ref:
                    current["referenced_by_modules"] = [*modules_ref, module_token]

    for idx, row in enumerate(ui_event_rows[:12000], start=1):
        if not isinstance(row, dict):
            continue
        form_token = _normalize_form_name_token(row.get("form"))
        handler = _clean(row.get("event_handler"))
        refs = [*_as_list(row.get("sql_touches")), *_as_list(row.get("procedure_calls"))]
        for ref in refs:
            text = _clean(ref)
            if not text:
                continue
            for db_path in _extract_db_paths(text):
                upsert_path(db_path, detected_from="ui_event_map", evidence=f"event:{idx}:{handler or 'handler'}")
                current = db_rows.get(_normalize_repo_path(db_path).lower())
                if not current:
                    continue
                if form_token and form_token in known_forms:
                    forms_ref = _as_list(current.get("referenced_by_forms"))
                    if form_token not in forms_ref:
                        current["referenced_by_forms"] = [*forms_ref, form_token]
                elif handler:
                    modules_ref = _as_list(current.get("referenced_by_modules"))
                    if handler not in modules_ref:
                        current["referenced_by_modules"] = [*modules_ref, handler]

    for row in (binary_companion_rows or [])[:12000]:
        if not isinstance(row, dict):
            continue
        ext = _clean(row.get("extension")).lower()
        if ext not in {".mdb", ".accdb"}:
            continue
        path = _clean(row.get("path"))
        if not path:
            continue
        upsert_path(path, detected_from="binary_companion", evidence=path)

    rows = sorted(db_rows.values(), key=lambda x: _clean(x.get("path")).lower())
    return {
        "artifact_type": "mdb_inventory",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "database_files_detected": len(rows),
            "forms_with_db_refs": len({f for r in rows for f in _as_list(_as_dict(r).get("referenced_by_forms")) if _clean(f)}),
            "module_refs": sum(len(_as_list(_as_dict(r).get("referenced_by_modules"))) for r in rows),
        },
        "databases": rows[:300],
    }


def _build_form_loc_profile(
    *,
    metadata_common: dict[str, Any],
    forms: list[Any],
    vb6_projects: list[Any],
    source_loc_by_file: dict[str, int],
) -> dict[str, Any]:
    project_forms: dict[str, set[str]] = {}
    member_paths: set[str] = set()
    for project in vb6_projects[:500]:
        if not isinstance(project, dict):
            continue
        pname = _clean(project.get("project_name")).lower()
        forms_set = project_forms.setdefault(pname, set())
        for token in _as_list(project.get("forms")):
            clean_token = _clean(token)
            if ":" in clean_token:
                clean_token = clean_token.split(":", 1)[1]
            clean_token = _strip_form_extension(clean_token).lower()
            if clean_token:
                forms_set.add(clean_token)
        for path in _as_list(project.get("member_files")):
            norm = _normalize_repo_path(path).lower()
            if norm:
                member_paths.add(norm)

    loc_candidates: list[tuple[str, int]] = sorted(
        (_normalize_repo_path(path), int(loc or 0)) for path, loc in source_loc_by_file.items()
    )
    designer_rows: list[dict[str, Any]] = []
    designer_loc_total = 0
    designer_loc_script_total = 0
    designer_loc_definition_total = 0
    for path, loc in loc_candidates:
        norm = _normalize_repo_path(path)
        low = norm.lower()
        if low.endswith((".dca", ".dcx", ".dsr")):
            if low.endswith(".dsr"):
                kind = "designer_script"
                designer_loc_script_total += int(loc or 0)
            else:
                kind = "designer_definition"
                designer_loc_definition_total += int(loc or 0)
            designer_rows.append(
                {
                    "file": norm,
                    "kind": kind,
                    "loc": int(loc or 0),
                }
            )
            designer_loc_total += int(loc or 0)

    rows: list[dict[str, Any]] = []
    represented_source_files: set[str] = set()
    for idx, form in enumerate(forms[:1500], start=1):
        if not isinstance(form, dict):
            continue
        form_name = _clean(form.get("form_name") or form.get("name"))
        if not form_name:
            continue
        project_name = _clean(form.get("project_name")) or _split_variant_form(form_name)[0] or "n/a"
        _, base_form = _split_variant_form(form_name)
        base_form = base_form or _clean(form.get("base_form_name")) or form_name
        base_form_name = _strip_form_extension(base_form)
        source_loc = int(form.get("source_loc", 0) or 0)
        expected_file = f"{base_form_name}.frm".lower()
        matched_path = ""
        matched_loc = 0
        for path, loc in loc_candidates:
            norm = _normalize_repo_path(path)
            if not norm.lower().endswith(".frm"):
                continue
            if norm.split("/")[-1].lower() != expected_file:
                continue
            matched_path = norm
            matched_loc = int(loc or 0)
            project_hint = project_name.lower()
            if project_hint and project_hint not in {"n/a", "unknown"} and project_hint in norm.lower():
                break
        effective_loc = source_loc or matched_loc
        project_forms_set = project_forms.get(project_name.lower(), set())
        in_vbp = bool(
            (matched_path and matched_path.lower() in member_paths)
            or (base_form_name.lower() in project_forms_set)
        )
        if project_name.lower() in {"(unmapped)", "unmapped", "n/a", "unknown"} and not (
            matched_path and matched_path.lower() in member_paths
        ):
            in_vbp = False
        if matched_path:
            represented_source_files.add(matched_path.lower())
        rows.append(
            {
                "form_id": f"form_loc:{idx}",
                "form": form_name,
                "base_form": base_form_name,
                "project": project_name,
                "source_file": matched_path or "",
                "loc": int(effective_loc or 0),
                "in_vbp": in_vbp,
                "active_or_orphan": "active" if in_vbp else "orphan",
                "confidence": round(_to_float(form.get("confidence_score"), 0.6), 2),
            }
        )

    # Ensure every discovered .frm file is represented, even if form dossiers missed one.
    next_id = len(rows) + 1
    all_project_forms = {token for tokens in project_forms.values() for token in tokens}
    for path, loc in loc_candidates:
        norm = _normalize_repo_path(path)
        low = norm.lower()
        if not low.endswith(".frm"):
            continue
        if low in represented_source_files:
            continue
        base = _strip_form_extension(norm.split("/")[-1]).strip()
        if not base:
            continue
        base_low = base.lower()
        in_vbp = bool(low in member_paths or base_low in all_project_forms)
        project_guess = "(unmapped)"
        if in_vbp:
            for pname, forms_set in project_forms.items():
                if base_low in forms_set:
                    project_guess = pname or "n/a"
                    break
        scoped_name = f"{project_guess}::{base}" if project_guess not in {"", "n/a"} else base
        rows.append(
            {
                "form_id": f"form_loc:{next_id}",
                "form": scoped_name,
                "base_form": base,
                "project": project_guess or "n/a",
                "source_file": norm,
                "loc": int(loc or 0),
                "in_vbp": in_vbp,
                "active_or_orphan": "active" if in_vbp else "orphan",
                "confidence": 0.45,
            }
        )
        next_id += 1

    # Deduplicate by concrete source file path first, then by form token.
    deduped: dict[str, dict[str, Any]] = {}
    for row in rows:
        if not isinstance(row, dict):
            continue
        source_file = _clean(row.get("source_file")).lower()
        form_name = _clean(row.get("form")).lower()
        key = source_file or form_name
        if not key:
            continue
        existing = deduped.get(key)
        if not existing:
            deduped[key] = row
            continue
        if int(row.get("loc", 0) or 0) > int(existing.get("loc", 0) or 0):
            deduped[key] = row
    rows = list(deduped.values())
    frm_file_locs = {
        _normalize_repo_path(path): int(loc or 0)
        for path, loc in loc_candidates
        if _normalize_repo_path(path).lower().endswith(".frm")
    }
    all_project_forms = {token for tokens in project_forms.values() for token in tokens}
    normalized_rows: list[dict[str, Any]] = []
    for path in sorted(frm_file_locs.keys(), key=lambda x: x.lower()):
        low_path = path.lower()
        base = _strip_form_extension(path.split("/")[-1]).strip()
        base_low = base.lower()
        candidates = [
            _as_dict(r)
            for r in rows
            if _normalize_repo_path(_as_dict(r).get("source_file")).lower() == low_path
        ]
        if not candidates:
            candidates = [
                _as_dict(r)
                for r in rows
                if _strip_form_extension(_clean(_as_dict(r).get("base_form"))).lower() == base_low
                or _strip_form_extension(_clean(_as_dict(r).get("form")).split("::", 1)[-1]).lower() == base_low
            ]
        if candidates:
            pick = sorted(
                candidates,
                key=lambda r: (
                    1 if bool(r.get("in_vbp")) else 0,
                    _to_float(r.get("confidence"), 0.0),
                    int(r.get("loc", 0) or 0),
                ),
                reverse=True,
            )[0]
            row = dict(pick)
        else:
            inferred_in_vbp = bool(low_path in member_paths or base_low in all_project_forms)
            inferred_project = "(unmapped)"
            if inferred_in_vbp:
                for pname, forms_set in project_forms.items():
                    if base_low in forms_set:
                        inferred_project = pname or "n/a"
                        break
            scoped_name = f"{inferred_project}::{base}" if inferred_project not in {"", "n/a"} else base
            row = {
                "form_id": f"form_loc:auto:{len(normalized_rows) + 1}",
                "form": scoped_name,
                "base_form": base,
                "project": inferred_project or "n/a",
                "in_vbp": inferred_in_vbp,
                "active_or_orphan": "active" if inferred_in_vbp else "orphan",
                "confidence": 0.45,
            }
        row["source_file"] = path
        row["loc"] = int(frm_file_locs.get(path, 0) or 0)
        row["base_form"] = _clean(row.get("base_form")) or base
        in_vbp = bool(low_path in member_paths or _strip_form_extension(_clean(row.get("base_form"))).lower() in all_project_forms)
        row["in_vbp"] = in_vbp
        row["active_or_orphan"] = "active" if in_vbp else "orphan"
        normalized_rows.append(row)
    rows = normalized_rows
    rows.sort(key=lambda r: (_clean(_as_dict(r).get("project")).lower(), _clean(_as_dict(r).get("source_file")).lower()))

    active_count = len([r for r in rows if bool(_as_dict(r).get("in_vbp"))])
    orphan_count = max(0, len(rows) - active_count)
    return {
        "artifact_type": "form_loc_profile",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "forms_discovered": len(rows),
            "forms_active": active_count,
            "forms_orphan": orphan_count,
            "forms_loc_total": sum(int(_as_dict(r).get("loc", 0) or 0) for r in rows),
            "designer_loc_total": designer_loc_total,
            "designer_loc_script_total": designer_loc_script_total,
            "designer_loc_definition_total": designer_loc_definition_total,
            "designer_file_count": len(designer_rows),
        },
        "forms": rows[:2000],
        "designer_files": designer_rows[:2000],
    }


def _build_connection_string_variants(
    *,
    metadata_common: dict[str, Any],
    sql_rows: list[Any],
    ui_event_rows: list[Any],
    connection_string_rows: list[Any] | None = None,
) -> dict[str, Any]:
    variants: dict[str, dict[str, Any]] = {}

    def add_variant(raw: str, source_ref: str) -> None:
        if not _looks_like_connection_string(raw):
            return
        normalized = _normalize_connection_pattern(raw)
        if not normalized:
            return
        key = normalized.lower()
        row = variants.setdefault(
            key,
            {
                "variant_id": f"conn:{len(variants) + 1}",
                "normalized_pattern": normalized,
                "risk_flags": [],
                "examples": [],
                "source_refs": [],
            },
        )
        for risk in _connection_variant_risks(raw):
            if risk not in _as_list(row.get("risk_flags")):
                row["risk_flags"] = [*_as_list(row.get("risk_flags")), risk]
        if raw and raw not in _as_list(row.get("examples")):
            row["examples"] = [*_as_list(row.get("examples")), raw]
        if source_ref and source_ref not in _as_list(row.get("source_refs")):
            row["source_refs"] = [*_as_list(row.get("source_refs")), source_ref]

    for idx, row in enumerate(sql_rows[:12000], start=1):
        raw = _clean(_as_dict(row).get("raw") or _as_dict(row).get("sql") or _as_dict(row).get("statement") or row)
        if raw:
            add_variant(raw, f"sql:{idx}")

    for idx, row in enumerate(ui_event_rows[:12000], start=1):
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler")) or f"event:{idx}"
        for candidate in [*_as_list(row.get("sql_touches")), *_as_list(row.get("procedure_calls"))]:
            raw = _clean(candidate)
            if raw:
                add_variant(raw, handler)

    for idx, row in enumerate((connection_string_rows or [])[:12000], start=1):
        raw = _clean(_as_dict(row).get("raw") or _as_dict(row).get("connection_string") or row)
        if raw:
            add_variant(raw, f"conn:{idx}")

    rows = sorted(variants.values(), key=lambda x: len(_as_list(_as_dict(x).get("source_refs"))), reverse=True)
    return {
        "artifact_type": "connection_string_variants",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "variant_count": len(rows),
            "relative_path_risks": len([r for r in rows if "relative_db_path" in _as_list(_as_dict(r).get("risk_flags"))]),
            "embedded_credential_risks": len([r for r in rows if "embedded_credentials" in _as_list(_as_dict(r).get("risk_flags"))]),
        },
        "variants": rows[:400],
    }


def _build_module_global_inventory(
    *,
    metadata_common: dict[str, Any],
    bas_module_summary: dict[str, Any],
    ui_event_rows: list[Any],
    bas_global_rows: list[Any] | None = None,
) -> dict[str, Any]:
    modules = [_clean(m) for m in _as_list(_as_dict(bas_module_summary).get("modules")) if _clean(m)]
    symbol_rows: dict[str, dict[str, Any]] = {}
    likely_types = {
        "rs": "ADODB.Recordset",
        "con": "ADODB.Connection",
        "cn": "ADODB.Connection",
        "cnbank": "ADODB.Connection",
        "db": "DAO.Database",
        "cmd": "ADODB.Command",
    }
    for row in (bas_global_rows or [])[:12000]:
        if not isinstance(row, dict):
            continue
        symbol = _clean(row.get("symbol"))
        if not symbol:
            continue
        row_key = symbol.lower()
        declared_type = _clean(row.get("declared_type")) or "Variant"
        scope = _clean(row.get("scope")) or "module_shared_candidate"
        source_file = _normalize_repo_path(row.get("source_file"))
        line_no = int(row.get("line", 0) or 0)
        declaration = _clean(row.get("declaration"))
        entry = symbol_rows.setdefault(
            row_key,
            {
                "symbol": symbol,
                "declared_type": declared_type,
                "scope": scope,
                "inferred_purpose": "Global/module-level declaration extracted from .bas module.",
                "evidence_refs": [],
            },
        )
        if declared_type and _clean(entry.get("declared_type")) in {"", "Variant"}:
            entry["declared_type"] = declared_type
        evidence = source_file
        if line_no > 0 and source_file:
            evidence = f"{source_file}:{line_no}"
        elif declaration:
            evidence = declaration
        if evidence and evidence not in _as_list(entry.get("evidence_refs")):
            entry["evidence_refs"] = [*_as_list(entry.get("evidence_refs")), evidence]
    for idx, row in enumerate(ui_event_rows[:12000], start=1):
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler")) or f"event:{idx}"
        calls = [_clean(c) for c in _as_list(row.get("procedure_calls")) if _clean(c)]
        for call in calls:
            token = _clean(call).strip().strip("()")
            if not token:
                continue
            low = token.lower()
            is_candidate = (
                token.isupper()
                or low in {"rs", "rs1", "rs2", "con", "cn", "cnbank", "db", "cmd", "frm"}
                or low.startswith("g_")
                or low.startswith("global")
            )
            if not is_candidate:
                continue
            inferred_type = "Variant"
            for key, dtype in likely_types.items():
                if low == key or low.startswith(key):
                    inferred_type = dtype
                    break
            row_key = low
            entry = symbol_rows.setdefault(
                row_key,
                {
                    "symbol": token,
                    "declared_type": inferred_type,
                    "scope": "module_shared_candidate",
                    "inferred_purpose": "Shared state or helper object referenced from UI events.",
                    "evidence_refs": [],
                },
            )
            evidence = f"{handler}"
            if evidence not in _as_list(entry.get("evidence_refs")):
                entry["evidence_refs"] = [*_as_list(entry.get("evidence_refs")), evidence]

    rows = sorted(symbol_rows.values(), key=lambda x: _clean(_as_dict(x).get("symbol")).lower())
    return {
        "artifact_type": "module_global_inventory",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "module_count": len(modules),
            "global_candidates": len(rows),
            "extraction_status": "declared_plus_inferred" if bas_global_rows else "partial_inferred",
        },
        "modules": [{"module": _normalize_repo_path(m)} for m in modules[:500]],
        "globals": rows[:1200],
        "notes": [
            (
                "Global inventory merges direct .bas declarations with inferred shared-module symbols from event call sites."
                if bas_global_rows
                else "Global declarations are inferred from shared-module call sites because direct .bas declaration blocks are not yet emitted in legacy inventory payloads."
            )
        ],
    }


def _build_dead_form_refs(
    *,
    metadata_common: dict[str, Any],
    ui_event_rows: list[Any],
    forms: list[Any],
    form_profiles: list[dict[str, Any]],
    handler_form_index: dict[str, set[str]],
) -> dict[str, Any]:
    known_forms: dict[str, str] = {}
    for form in forms[:2000]:
        if not isinstance(form, dict):
            continue
        scoped = _clean(form.get("form_name") or form.get("name") or form.get("base_form_name"))
        canonical = _canonical_form_key(scoped)
        if canonical and canonical not in known_forms:
            known_forms[canonical] = scoped
        base = _clean(form.get("base_form_name"))
        if base:
            known_forms.setdefault(_canonical_form_key(base), base)

    unresolved: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, row in enumerate(ui_event_rows[:12000], start=1):
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler"))
        inferred_form = _infer_form_for_event(
            row=row,
            handler=handler,
            form_profiles=form_profiles,
            handler_index=handler_form_index,
        ) or _clean(row.get("form")) or "unknown"
        for raw_target in _as_list(row.get("procedure_calls")):
            token = _clean(raw_target)
            if not token:
                continue
            token = re.sub(r"(?i)\.(show|hide|load|unload)$", "", token).strip()
            token = token.split("(")[0].strip()
            if not token:
                continue
            low = token.lower()
            looks_formish = (
                low == "frm"
                or low.startswith("frm")
                or low.startswith("form")
                or low in {"main", "mdi", "login", "splash", "report"}
            )
            if not looks_formish:
                continue
            canonical = _canonical_form_key(token)
            if canonical in known_forms:
                continue
            key = f"{inferred_form}|{handler}|{token}".lower()
            if key in seen:
                continue
            seen.add(key)
            unresolved.append(
                {
                    "ref_id": f"dead_form_ref:{len(unresolved) + 1}",
                    "caller_form": inferred_form,
                    "caller_handler": handler or f"event:{idx}",
                    "target_token": token,
                    "status": "unresolved",
                    "rationale": "Target form token was referenced but no matching discovered form dossier exists.",
                    "evidence_ref": f"event:{idx}",
                }
            )

    return {
        "artifact_type": "dead_form_refs",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "unresolved_reference_count": len(unresolved),
            "callers_impacted": len({r.get("caller_form") for r in unresolved if _clean(_as_dict(r).get("caller_form"))}),
        },
        "references": unresolved[:1200],
    }


def _build_dataenvironment_report_mapping(
    *,
    metadata_common: dict[str, Any],
    project_members: list[Any],
    ui_event_rows: list[Any],
    form_profiles: list[dict[str, Any]],
    handler_form_index: dict[str, set[str]],
) -> dict[str, Any]:
    designer_members = [_extract_project_member_path(x) for x in project_members[:12000] if _extract_project_member_path(x)]
    environment_names: list[str] = []
    report_names: list[str] = []
    for member in designer_members:
        low = member.lower()
        base = _basename_without_ext(member)
        if low.endswith((".dsr", ".dca")):
            if "dataenvironment" in low or base.lower().startswith("de"):
                if base not in environment_names:
                    environment_names.append(base)
            if "datareport" in low or base.lower().startswith("rpt") or "report" in low:
                if base not in report_names:
                    report_names.append(base)

    def guess_environment(report_name: str) -> str:
        low = _clean(report_name).lower()
        numeric = re.search(r"(\d+)$", low)
        if numeric:
            suffix = numeric.group(1)
            for env in environment_names:
                if _clean(env).lower().endswith(suffix):
                    return env
        for env in environment_names:
            if _clean(env).lower().startswith("debank"):
                return env
        return environment_names[0] if environment_names else ""

    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, row in enumerate(ui_event_rows[:12000], start=1):
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler"))
        inferred_form = _infer_form_for_event(
            row=row,
            handler=handler,
            form_profiles=form_profiles,
            handler_index=handler_form_index,
        ) or _clean(row.get("form")) or "unknown"
        for target in _as_list(row.get("procedure_calls")):
            token = _clean(target).strip()
            if not token:
                continue
            normalized = token.split("(")[0].strip()
            low = normalized.lower()
            is_report_target = low.startswith("rpt") or low.startswith("datareport")
            if not is_report_target:
                if normalized not in report_names and _basename_without_ext(normalized) not in report_names:
                    continue
            report_name = normalized
            env_name = guess_environment(report_name)
            key = f"{inferred_form}|{handler}|{report_name}|{env_name}".lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "mapping_id": f"de_map:{len(rows) + 1}",
                    "caller_form": inferred_form,
                    "caller_handler": handler or f"event:{idx}",
                    "report_object": report_name,
                    "dataenvironment_object": env_name or "n/a",
                    "mapping_kind": "command_to_report",
                    "confidence": 0.72 if env_name else 0.55,
                    "evidence_ref": f"event:{idx}",
                }
            )

    return {
        "artifact_type": "dataenvironment_report_mapping",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "dataenvironment_count": len(environment_names),
            "report_object_count": len(report_names),
            "mapped_calls": len(rows),
        },
        "designer_assets": {
            "dataenvironments": environment_names[:200],
            "reports": report_names[:400],
        },
        "mappings": rows[:2000],
    }


def _build_static_risk_detectors(
    *,
    metadata_common: dict[str, Any],
    sql_statements: list[dict[str, Any]],
    business_rules: list[dict[str, Any]],
    ui_event_rows: list[Any] | None = None,
) -> dict[str, Any]:
    def _contains_word(text: str, term: str) -> bool:
        return bool(re.search(rf"(?i)\b{re.escape(term)}\b", text))

    def _contains_phrase(text: str, phrase: str) -> bool:
        parts = [re.escape(chunk) for chunk in phrase.split() if chunk]
        if not parts:
            return False
        pattern = r"(?i)\b" + r"\s+".join(parts) + r"\b"
        return bool(re.search(pattern, text))

    def _matches_any(text: str, words: list[str], phrases: list[str] | None = None) -> bool:
        if any(_contains_word(text, word) for word in words):
            return True
        return any(_contains_phrase(text, phrase) for phrase in (phrases or []))

    findings: list[dict[str, Any]] = []
    rule_statements = [
        _clean(_as_dict(rule).get("statement")).lower()
        for rule in business_rules
        if isinstance(rule, dict) and _clean(_as_dict(rule).get("statement"))
    ]
    write_statements = [
        _as_dict(stmt) for stmt in sql_statements
        if _clean(_as_dict(stmt).get("kind")).lower() in {"insert", "update", "delete"}
    ]
    write_tables = sorted(
        {
            _clean(tbl).lower()
            for stmt in write_statements
            for tbl in _as_list(_as_dict(stmt).get("tables"))
            if _clean(tbl)
        }
    )
    has_txn_control = any(
        re.search(r"(?i)\b(begin\s+trans|begintransaction|commit|rollback)\b", _clean(_as_dict(stmt).get("raw")))
        for stmt in sql_statements
        if isinstance(stmt, dict)
    ) or any(
        _matches_any(text, ["commit", "rollback"], ["begin transaction", "begin trans"])
        for text in rule_statements
    )
    event_tokens = [
        _clean(token).lower()
        for row in (ui_event_rows or [])[:12000]
        if isinstance(row, dict)
        for token in [*_as_list(row.get("procedure_calls")), *_as_list(row.get("side_effects"))]
        if _clean(token)
    ]
    has_txn_control = has_txn_control or any(
        any(k in token for k in ("begintrans", "begin transaction", "commit", "rollback"))
        for token in event_tokens
    )
    multi_write_rule_signals = sum(
        1
        for text in rule_statements
        if _matches_any(
            text,
            [],
            [
                "balance updated",
                "balance recalculated",
                "transaction recorded",
                "transaction history updated",
                "deposit transaction",
                "withdrawal transaction",
            ],
        )
    )
    multi_write_sql_signals = len(write_statements) >= 2 or len(write_tables) > 1
    write_event_tokens = [
        token
        for token in event_tokens
        if any(
            marker in token
            for marker in ("save", "addnew", "insert", "update", "deposit", "withdraw", "delete", "remove")
        )
    ]
    unique_write_event_tokens = sorted(set(write_event_tokens))
    multi_write_event_signals = len(unique_write_event_tokens)

    if (
        multi_write_sql_signals
        or (multi_write_rule_signals >= 2)
        or (len(write_statements) >= 1 and multi_write_event_signals >= 4)
    ) and not has_txn_control:
        findings.append(
            {
                "detector_id": "no_rollback_on_multi_write",
                "severity": "high",
                "summary": "Multiple write operations were detected without explicit transaction/rollback guards.",
                "evidence": {
                    "write_statement_count": len(write_statements),
                    "write_table_count": len(write_tables),
                    "write_tables": write_tables[:20],
                    "rule_signals": multi_write_rule_signals,
                    "event_write_signals": multi_write_event_signals,
                    "event_write_signal_samples": unique_write_event_tokens[:10],
                },
            }
        )

    delete_stmts = [
        _as_dict(stmt) for stmt in sql_statements
        if _clean(_as_dict(stmt).get("kind")).lower() == "delete"
    ]
    # Capture delete statements that failed strict kind classification (e.g., quoted/fragmented SQL).
    for stmt in sql_statements:
        if not isinstance(stmt, dict):
            continue
        if _clean(_as_dict(stmt).get("kind")).lower() == "delete":
            continue
        raw = _clean(_as_dict(stmt).get("raw"))
        if raw and re.search(r"""(?i)^\s*["']?\s*delete\b""", raw):
            delete_stmts.append(_as_dict(stmt))
    archival_keywords = {"archive", "audit", "history", "log", "backup"}
    archival_writes = [
        tbl for tbl in write_tables if any(k in tbl for k in archival_keywords)
    ]
    delete_rule_signals = [
        text for text in rule_statements
        if _matches_any(text, ["delete", "deleted", "remove", "deactivate"], ["close account"])
    ]
    delete_event_signals = [
        token for token in event_tokens
        if "delete" in token or "remove" in token or "closeaccount" in token
    ]
    ado_delete_call_count = sum(1 for token in event_tokens if "delete" in token)
    archival_rule_signals = [
        text for text in rule_statements
        if _matches_any(text, ["archive", "audit", "history", "log", "backup"])
    ]
    has_delete_like = bool(delete_stmts) or bool(delete_rule_signals) or bool(delete_event_signals)
    if has_delete_like and not archival_writes and not archival_rule_signals:
        findings.append(
            {
                "detector_id": "delete_without_archival",
                "severity": "high",
                "summary": "DELETE operations were found without corresponding archival/audit write patterns.",
                "evidence": {
                    "delete_count": len(delete_stmts) + ado_delete_call_count,
                    "sql_delete_count": len(delete_stmts),
                    "ado_delete_call_count": ado_delete_call_count,
                    "delete_sql_ids": [_clean(stmt.get("sql_id")) for stmt in delete_stmts[:20] if _clean(stmt.get("sql_id"))],
                    "delete_rule_signals": len(delete_rule_signals),
                    "delete_event_signals": len(delete_event_signals),
                },
            }
        )

    max_id_patterns = [
        _clean(_as_dict(stmt).get("sql_id"))
        for stmt in sql_statements
        if isinstance(stmt, dict) and re.search(r"(?i)\bselect\s+max\s*\(", _clean(_as_dict(stmt).get("raw")))
    ]
    if max_id_patterns:
        findings.append(
            {
                "detector_id": "manual_id_generation_concurrency_risk",
                "severity": "medium",
                "summary": "Manual ID generation pattern (SELECT MAX(...)) detected; this can cause concurrency collisions.",
                "evidence": {
                    "sql_ids": [x for x in max_id_patterns[:40] if x],
                },
            }
        )

    caption_driven = [
        _clean(_as_dict(rule).get("rule_id"))
        for rule in business_rules
        if isinstance(rule, dict) and (
            "caption" in _clean(_as_dict(rule).get("statement")).lower()
            or "displayed balance label" in _clean(_as_dict(rule).get("statement")).lower()
        )
    ]
    if caption_driven:
        findings.append(
            {
                "detector_id": "ui_caption_as_system_of_record",
                "severity": "medium",
                "summary": "Business logic appears to rely on UI caption/label state as a data source.",
                "evidence": {
                    "rule_ids": [x for x in caption_driven[:40] if x],
                },
            }
        )

    return {
        "artifact_type": "static_risk_detectors",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "summary": {
            "detector_count": 4,
            "findings_count": len(findings),
            "high_findings": len([f for f in findings if _clean(_as_dict(f).get("severity")).lower() == "high"]),
        },
        "findings": findings,
    }


def _build_data_access_map(
    *,
    metadata_common: dict[str, Any],
    sql_map_entries: list[dict[str, Any]],
) -> dict[str, Any]:
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for idx, row in enumerate(sql_map_entries[:2000], start=1):
        if not isinstance(row, dict):
            continue
        form = _clean(row.get("form"))
        handler = _clean(row.get("procedure"))
        operation = _clean(row.get("operation")).upper() or "UNKNOWN"
        source_ref = _clean(row.get("sql_id") or row.get("source_ref") or row.get("map_id"))
        tables = [_clean(x) for x in _as_list(row.get("tables")) if _clean(x)]
        if not tables:
            tables = ["unknown"]
        for table in tables[:20]:
            key = f"{form}|{handler}|{operation}|{table}|{source_ref}".lower()
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "row_id": f"dam:{idx}:{len(rows) + 1}",
                    "container": form,
                    "handler": handler,
                    "operation": operation,
                    "table": table,
                    "source_ref": source_ref,
                    "line": "",
                }
            )
    coverage = 1.0 if rows else 0.0
    return {
        "artifact_type": "data_access_map",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "rows": rows[:2500],
        "complete": bool(rows),
        "coverage_score": coverage,
    }


def _build_recordset_ops(
    *,
    metadata_common: dict[str, Any],
    procedure_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    ops: list[dict[str, Any]] = []
    for idx, proc in enumerate(procedure_summaries[:1200], start=1):
        if not isinstance(proc, dict):
            continue
        summary_blob = " ".join(
            [
                _clean(proc.get("summary")),
                " ".join(_clean(x) for x in _as_list(proc.get("steps")) if _clean(x)),
                _clean(proc.get("procedure_name")),
            ]
        ).lower()
        if "recordset" not in summary_blob and "rs." not in summary_blob:
            continue
        action = "UPDATE"
        if "delete" in summary_blob:
            action = "DELETE"
        tables = [_clean(x) for x in _as_list(proc.get("tables_touched")) if _clean(x)] or ["unknown"]
        for table in tables[:10]:
            ops.append(
                {
                    "op_id": f"rsop:{idx}:{len(ops) + 1}",
                    "table": table,
                    "open_statement": "",
                    "fields_set": [],
                    "action": action,
                    "evidence": [],
                }
            )
    return {
        "artifact_type": "recordset_ops",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "ops": ops[:1200],
    }


def _build_risk_register(
    *,
    metadata_common: dict[str, Any],
    detector_findings: list[dict[str, Any]],
    sql_statements: list[dict[str, Any]],
    business_rules: list[dict[str, Any]],
) -> dict[str, Any]:
    risks: list[dict[str, Any]] = []
    risk_id = 1
    caption_balance_risk_emitted = False
    for finding in detector_findings[:120]:
        if not isinstance(finding, dict):
            continue
        severity = _clean(finding.get("severity")).lower() or "medium"
        if severity not in {"low", "medium", "high"}:
            severity = "medium"
        desc = _clean(finding.get("summary")) or _clean(finding.get("detector_id")) or "Detector risk"
        risks.append(
            {
                "risk_id": f"RISK-{risk_id:03d}",
                "severity": severity,
                "description": desc,
                "consequence": "Potential behavior or modernization risk.",
                "evidence": _as_list(finding.get("evidence")),
                "recommended_action": ", ".join([_clean(x) for x in _as_list(finding.get("required_actions")) if _clean(x)]) or "Add targeted remediation task.",
            }
        )
        risk_id += 1
    for stmt in sql_statements[:300]:
        if not isinstance(stmt, dict):
            continue
        flags = [_clean(x) for x in _as_list(stmt.get("risk_flags")) if _clean(x)]
        if not flags:
            continue
        risks.append(
            {
                "risk_id": f"RISK-{risk_id:03d}",
                "severity": "high" if any(f in {"possible_injection", "sensitive_credential_query"} for f in flags) else "medium",
                "description": f"SQL risk flags for {(_clean(stmt.get('sql_id')) or 'statement')}: {', '.join(flags)}",
                "consequence": "Data integrity, security, or portability risk.",
                "evidence": _as_list(stmt.get("usage_sites")),
                "recommended_action": "Parameterize query and align dialect/validation rules before migration.",
            }
        )
        risk_id += 1

    for rule in business_rules[:400]:
        if not isinstance(rule, dict):
            continue
        statement = _clean(rule.get("statement"))
        lower = statement.lower()
        if not statement:
            continue
        if (
            "lblbalance.caption" in lower
            or "ccur(lblbalance.caption)" in lower
            or ("caption" in lower and "balance" in lower and "ccur(" in lower)
        ):
            if caption_balance_risk_emitted:
                continue
            evidence = _as_list(rule.get("evidence"))
            risks.append(
                {
                    "risk_id": f"RISK-{risk_id:03d}",
                    "severity": "high",
                    "description": "Balance calculation depends on UI caption value instead of persisted balance source.",
                    "consequence": "Financial correctness risk during migration and runtime parity validation.",
                    "evidence": evidence,
                    "recommended_action": (
                        "Refactor balance calculations to use persisted/accounting source of truth and add parity tests "
                        "covering caption/display mismatch scenarios."
                    ),
                }
            )
            risk_id += 1
            caption_balance_risk_emitted = True
    return {
        "artifact_type": "risk_register",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "risks": risks[:300],
    }


def _derive_risk_backlog_items(
    *,
    risk_rows: list[dict[str, Any]],
    review_by_id: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    seen_titles: set[str] = set()

    def _add_item(title: str, *, outcome: str, priority: str = "P0", evidence_expected: list[str] | None = None) -> None:
        key = _clean(title).lower()
        if not key or key in seen_titles:
            return
        seen_titles.add(key)
        idx = len(items) + 1
        items.append(
            {
                "id": f"RM-{idx:03d}",
                "type": "risk_remediation",
                "priority": _normalize_priority(priority),
                "title": title,
                "outcome": outcome,
                "acceptance_criteria": [
                    "Remediation implemented and validated against affected legacy flow.",
                    "Evidence artifacts updated with before/after traceability.",
                ],
                "depends_on": [],
                "evidence_expected": evidence_expected or ["risk_remediation_report", "traceability_matrix"],
                "grounding": {
                    "classification": "derived_from_legacy",
                    "reason": "derived from discovered risk and review artifacts",
                },
            }
        )

    for row in risk_rows[:240]:
        if not isinstance(row, dict):
            continue
        text = " ".join(
            [
                _clean(row.get("description")),
                _clean(row.get("recommended_action")),
                _clean(row.get("consequence")),
            ]
        ).lower()
        if any(token in text for token in ("possible_injection", "injection", "string_concatenation", "password", "credential", "plaintext")):
            _add_item(
                "Parameterize SQL and secure credential handling",
                outcome="Eliminate SQL injection patterns and remove plaintext credential handling behavior.",
                priority="P0",
                evidence_expected=["security_test_report", "sql_parameterization_audit", "traceability_matrix"],
            )
        if "delete-by-customer" in text or ("delete" in text and "customer" in text and "transaction" in text):
            _add_item(
                "Resolve transaction delete-key behavior",
                outcome="Introduce explicit transaction-key deletion semantics with compatibility safeguards.",
                priority="P0",
                evidence_expected=["schema_key_decision_record", "data_integrity_test_report", "traceability_matrix"],
            )
        if "overdraft" in text or ("withdraw" in text and "validation" in text):
            _add_item(
                "Add withdrawal guardrail parity checks",
                outcome="Validate withdrawal behavior against overdraft and balance business rules from legacy code.",
                priority="P0",
            )
        if "caption value" in text or ("caption" in text and "balance" in text):
            _add_item(
                "Replace UI-caption-based balance arithmetic",
                outcome="Ensure balance calculations use persisted balance source and parity tests cover UI display mismatch.",
                priority="P0",
                evidence_expected=["balance_calculation_parity_tests", "risk_remediation_report", "traceability_matrix"],
            )
        if "report" in text or "dataenvironment" in text or "datareport" in text:
            _add_item(
                "Reconcile reporting model and migration coverage",
                outcome="Map report entrypoints/data environments and define parity acceptance tests for reporting flows.",
                priority="P1",
                evidence_expected=["reporting_model_reconciliation", "report_parity_test_report", "traceability_matrix"],
            )

    def _is_fail_or_warn(check_id: str) -> bool:
        check = _as_dict(review_by_id.get(check_id))
        return _clean(check.get("status")).lower() in {"fail", "warn"}

    if _is_fail_or_warn("identity_access_model"):
        _add_item(
            "Define identity and access model for modernization scope",
            outcome="Confirm role model, credential policy, and multi-user assumptions before implementation.",
            priority="P0",
            evidence_expected=["identity_access_decision_record", "authorization_test_plan", "traceability_matrix"],
        )
    if _is_fail_or_warn("report_model_reconciled"):
        _add_item(
            "Resolve report model reconciliation gaps",
            outcome="Close DataEnvironment/DataReport unknown mappings and path mismatches.",
            priority="P0",
        )
    if _is_fail_or_warn("variant_schema_divergence"):
        _add_item(
            "Resolve cross-variant schema naming divergence",
            outcome="Agree canonical schema naming strategy across project variants and capture merge/split decision.",
            priority="P0",
            evidence_expected=["variant_schema_decision_record", "variant_diff_report", "traceability_matrix"],
        )
    if _is_fail_or_warn("schema_key_verification"):
        _add_item(
            "Remediate transaction schema key hazard",
            outcome="Prevent ambiguous delete-by-customer behavior by introducing explicit transaction key semantics.",
            priority="P0",
            evidence_expected=["schema_migration_plan", "data_integrity_test_report", "traceability_matrix"],
        )

    return items[:16]


def _build_orphan_analysis(
    *,
    metadata_common: dict[str, Any],
    projects: list[dict[str, Any]],
    forms: list[Any],
    sql_map_entries: list[dict[str, Any]],
    procedure_summaries: list[dict[str, Any]],
    unmapped_form_file_count: int = 0,
) -> dict[str, Any]:
    mapped_forms = {
        _normalize_form_name_token(form)
        for project in projects
        if isinstance(project, dict)
        for form in _as_list(project.get("forms"))
        if _normalize_form_name_token(form)
    }
    startup_forms = {
        _normalize_form_name_token(_clean(project.get("startup")))
        for project in projects
        if isinstance(project, dict) and _normalize_form_name_token(_clean(project.get("startup")))
    }
    form_names = {
        _normalize_form_name_token(_clean(row.get("base_form_name") or row.get("form_name")))
        for row in forms
        if isinstance(row, dict) and _normalize_form_name_token(_clean(row.get("base_form_name") or row.get("form_name")))
    }
    all_forms = {name for name in form_names.union(mapped_forms) if name}
    nav_edges: dict[str, set[str]] = {name: set() for name in all_forms}
    for proc in procedure_summaries[:1500]:
        if not isinstance(proc, dict):
            continue
        source = _normalize_form_name_token(_clean(proc.get("form")))
        if not source:
            continue
        calls = [_clean(x).lower() for x in _as_list(proc.get("navigation_side_effects")) if _clean(x)]
        if not calls:
            continue
        for candidate in all_forms:
            cl = candidate.lower()
            if any(cl in call for call in calls):
                nav_edges.setdefault(source, set()).add(candidate)

    roots = set(startup_forms)
    if not roots:
        roots = set(sorted(mapped_forms)[:2])
    reachable: set[str] = set()
    queue = list(roots)
    while queue:
        cur = queue.pop(0)
        if not cur or cur in reachable:
            continue
        reachable.add(cur)
        for nxt in sorted(nav_edges.get(cur, set())):
            if nxt not in reachable:
                queue.append(nxt)

    unmapped_member_paths: list[str] = []
    seen_unmapped_paths: set[str] = set()
    for project in projects:
        if not isinstance(project, dict):
            continue
        for member in _as_list(project.get("member_files")):
            path = _clean(member).replace("\\", "/")
            if not path:
                continue
            lower = path.lower()
            if not (lower.endswith(".frm") or lower.endswith(".ctl")):
                continue
            base = _canonical_form_key(path.split("/")[-1])
            if base and base in mapped_forms:
                continue
            if path.lower() in seen_unmapped_paths:
                continue
            seen_unmapped_paths.add(path.lower())
            unmapped_member_paths.append(path)

    orphans: list[dict[str, Any]] = []
    seen: set[str] = set()
    for row in forms[:600]:
        if not isinstance(row, dict):
            continue
        form_name = _clean(row.get("base_form_name") or row.get("form_name"))
        norm = _normalize_form_name_token(form_name)
        if not norm or norm in seen:
            continue
        in_project_membership = norm in mapped_forms
        reachable_from_startup = norm in reachable if reachable else False
        if in_project_membership:
            continue
        if not in_project_membership and not mapped_forms:
            continue
        seen.add(norm)
        related_sql = [
            _clean(entry.get("sql_id"))
            for entry in sql_map_entries
            if isinstance(entry, dict) and _normalize_form_name_token(entry.get("form")) == norm and _clean(entry.get("sql_id"))
        ]
        tables_touched = [
            _clean(table)
            for entry in sql_map_entries
            if isinstance(entry, dict) and _normalize_form_name_token(entry.get("form")) == norm
            for table in _as_list(entry.get("tables"))
            if _clean(table)
        ]
        if not related_sql or not tables_touched:
            proc_sql = [
                _clean(sql_id)
                for proc in procedure_summaries
                if isinstance(proc, dict) and _normalize_form_name_token(proc.get("form")) == norm
                for sql_id in _as_list(proc.get("sql_ids"))
                if _clean(sql_id)
            ]
            proc_tables = [
                _clean(table)
                for proc in procedure_summaries
                if isinstance(proc, dict) and _normalize_form_name_token(proc.get("form")) == norm
                for table in _as_list(proc.get("tables_touched"))
                if _clean(table)
            ]
            if proc_sql:
                related_sql = sorted(set([*related_sql, *proc_sql]))[:80]
            if proc_tables:
                tables_touched = sorted(set([*tables_touched, *proc_tables]))[:80]
        if not related_sql or not tables_touched:
            form_hint = _canonical_form_key(form_name)
            form_hint = re.sub(r"^(frm|form)", "", form_hint)
            inferred_rows = []
            for entry in sql_map_entries:
                if not isinstance(entry, dict):
                    continue
                proc = _clean(entry.get("procedure")).lower()
                table_tokens = [_clean(t) for t in _as_list(entry.get("tables")) if _clean(t)]
                if form_hint and any(form_hint in _canonical_table_name(t) or _canonical_table_name(t) in form_hint for t in table_tokens):
                    inferred_rows.append(entry)
                    continue
                if form_hint and form_hint in proc:
                    inferred_rows.append(entry)
            if inferred_rows:
                related_sql = sorted(
                    {
                        *_as_list(related_sql),
                        *[_clean(r.get("sql_id")) for r in inferred_rows if _clean(r.get("sql_id"))],
                    }
                )[:80]
                tables_touched = sorted(
                    {
                        *_as_list(tables_touched),
                        *[
                            _clean(t)
                            for r in inferred_rows
                            for t in _as_list(r.get("tables"))
                            if _clean(t)
                        ],
                    }
                )[:80]
        tables_unique = sorted(set(tables_touched))[:80]
        sql_unique = sorted(set(related_sql))[:80]
        table_blob = " ".join(tables_unique).lower()
        recommendation = "verify"
        if not sql_unique and not tables_unique:
            recommendation = "exclude_or_defer"
        elif any(tok in table_blob for tok in ("logi", "login", "credential", "user")):
            recommendation = "escalate_security_review"
        elif sql_unique and tables_unique:
            recommendation = "prioritize_migration"
        elif sql_unique:
            recommendation = "verify_sql_mapping"
        orphans.append(
            {
                "form": form_name,
                "path": form_name,
                "reason": (
                    "not_reachable_from_startup"
                    if in_project_membership
                    else "not_mapped_to_project_variant"
                ),
                "sql_ids": sql_unique,
                "tables_touched": tables_unique,
                "behavior_summary": _clean(row.get("business_use")) or "Potential orphan flow detected.",
                "divergence_signals": [],
                "recommendation": recommendation,
                "reachable_from_startup": reachable_from_startup,
                "in_project_membership": in_project_membership,
            }
        )
    if (not orphans) and int(unmapped_form_file_count or 0) > 0:
        sample_paths = sorted(unmapped_member_paths)[:40]
        sample_suffix = ""
        if sample_paths:
            sample_suffix = f" Sample paths: {', '.join(sample_paths[:5])}."
        orphans.append(
            {
                "form": "(unmapped_form_files)",
                "path": sample_paths[0] if sample_paths else "(unmapped_form_files)",
                "reason": "unmapped_form_file_detected",
                "sql_ids": [],
                "tables_touched": [],
                "behavior_summary": (
                    f"{int(unmapped_form_file_count)} discovered form files are not mapped to project membership entries."
                    + sample_suffix
                ),
                "divergence_signals": ["project_membership_gap"],
                "recommendation": "reconcile_project_membership",
                "reachable_from_startup": False,
                "in_project_membership": False,
                "unmapped_form_count": int(unmapped_form_file_count),
                "candidate_paths": sample_paths,
            }
        )

    return {
        "artifact_type": "orphan_analysis",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "orphans": orphans[:200],
        "analysis": {
            "startup_roots": sorted(startup_forms)[:40],
            "reachable_forms": sorted(reachable)[:200],
            "mapped_form_count": len(mapped_forms),
        },
    }


def _build_form_dossiers(
    *,
    metadata_common: dict[str, Any],
    forms: list[Any],
    event_entries: list[dict[str, Any]],
    sql_map_entries: list[dict[str, Any]],
    procedure_summaries: list[dict[str, Any]],
) -> dict[str, Any]:
    dossiers: list[dict[str, Any]] = []
    for idx, row in enumerate(forms[:500], start=1):
        if not isinstance(row, dict):
            continue
        form_name = _clean(row.get("base_form_name") or row.get("form_name"))
        if not form_name:
            continue
        scoped_form_name = _clean(row.get("form_name"))
        scoped_norm = scoped_form_name.lower()
        norm = _normalize_form_name_token(form_name)

        def _matches_form(candidate_form: Any, candidate_symbol: Any = "") -> bool:
            candidate = _clean(candidate_form)
            symbol = _clean(candidate_symbol)
            if scoped_form_name and "::" in scoped_form_name:
                scoped_lower = scoped_form_name.lower()
                if candidate and candidate.lower() == scoped_lower:
                    return True
                if symbol and symbol.lower().startswith(scoped_lower + "::"):
                    return True
            cand_norm = _normalize_form_name_token(candidate)
            if cand_norm and cand_norm == norm:
                return True
            if symbol:
                symbol_base = _normalize_form_name_token(symbol.rsplit("::", 1)[0])
                if symbol_base and symbol_base == norm:
                    return True
            return False

        form_events = [
            entry for entry in event_entries
            if isinstance(entry, dict)
            and _matches_form(
                entry.get("container"),
                _as_dict(entry.get("handler")).get("symbol"),
            )
        ]
        if not form_events:
            form_events = [
                entry for entry in event_entries
                if isinstance(entry, dict)
                and _matches_form(
                    _clean(_as_dict(entry.get("handler")).get("symbol")).rsplit("::", 1)[0],
                    _as_dict(entry.get("handler")).get("symbol"),
                )
            ]
        sql_rows = [
            item for item in sql_map_entries
            if isinstance(item, dict)
            and _matches_form(item.get("form"))
        ]
        procs = [
            proc for proc in procedure_summaries
            if isinstance(proc, dict)
            and _matches_form(proc.get("form"))
        ]
        actions: list[dict[str, Any]] = []
        for eidx, event in enumerate(form_events[:20], start=1):
            actions.append(
                {
                    "id": f"{norm}:action:{eidx}",
                    "event_handler": _clean(_as_dict(event.get("handler")).get("symbol")) or _clean(event.get("name")),
                    "event": _clean(_as_dict(event.get("trigger")).get("event")),
                    "control": _clean(_as_dict(event.get("trigger")).get("control")),
                    "sql_ids": [_clean(x) for x in _as_list(_as_dict(event.get("side_effects")).get("sql_ids")) if _clean(x)][:20],
                    "tables_touched": [_clean(x) for x in _as_list(_as_dict(event.get("side_effects")).get("tables_or_files")) if _clean(x)][:20],
                }
            )
        if not actions:
            for eidx, item in enumerate(sql_rows[:20], start=1):
                actions.append(
                    {
                        "id": f"{norm}:sql:{eidx}",
                        "event_handler": _clean(item.get("procedure")),
                        "event": "",
                        "control": "",
                        "sql_ids": [_clean(item.get("sql_id"))] if _clean(item.get("sql_id")) else [],
                        "tables_touched": [_clean(x) for x in _as_list(item.get("tables")) if _clean(x)][:20],
                    }
                )
        purpose = _infer_form_purpose(
            form_name=form_name,
            current_purpose=_clean(row.get("business_use")),
            actions=actions,
            sql_rows=sql_rows,
            procedures=procs,
        )
        table_hints = sorted(
            {
                _clean(tbl)
                for item in sql_rows
                for tbl in _as_list(_as_dict(item).get("tables"))
                if _clean(tbl)
            }
        )
        alias = _infer_form_alias(
            form_name=form_name,
            purpose=purpose,
            sql_rows=sql_rows,
            procedures=procs,
            controls=[_clean(x) for x in _as_list(row.get("controls")) if _clean(x)],
        )
        generic_name = bool(re.fullmatch(r"(form\d+|frm\d+)", form_name.lower()))
        display_name = f"{form_name} [{alias}]" if generic_name and _clean(alias) else form_name
        form_type = _infer_form_type(
            form_name=form_name,
            purpose=purpose,
            controls=[_clean(x) for x in _as_list(row.get("controls")) if _clean(x)],
            procedures=procs,
            table_hints=table_hints,
        )
        expected_handlers = int(row.get("expected_handlers_count", 0) or 0)
        extracted_handlers = int(row.get("extracted_handlers_count", 0) or 0)
        if expected_handlers <= 0:
            expected_handlers = max(
                extracted_handlers,
                len([_clean(x) for x in _as_list(row.get("event_handlers")) if _clean(x)]),
                len(actions),
                1,
            )
        if extracted_handlers <= 0:
            extracted_handlers = max(
                len([_clean(x) for x in _as_list(row.get("event_handlers")) if _clean(x)]),
                len(actions),
            )
        coverage_score = _to_float(row.get("coverage_score"), 0.0)
        if coverage_score <= 0:
            coverage_score = min(1.0, extracted_handlers / float(expected_handlers or 1))
        action_count = len(actions)
        sql_action_count = sum(
            1
            for action in actions
            if _as_list(_as_dict(action).get("sql_ids")) or _as_list(_as_dict(action).get("tables_touched"))
        )
        generic_purpose = _clean(purpose).lower() in {
            "business workflow executed through event-driven ui controls.",
            "business workflow executed through event-driven ui controls",
            "potential orphan flow detected.",
            "potential orphan flow detected",
        }
        coverage_clamped = max(0.0, min(1.0, coverage_score))
        confidence = 0.22 + (0.45 * coverage_clamped)
        confidence += min(0.14, 0.02 * action_count)
        confidence += min(0.08, 0.015 * len(procs))
        confidence += min(0.08, 0.02 * len(table_hints))
        confidence += 0.09 if sql_action_count > 0 else -0.08
        confidence += 0.08 if not generic_purpose else -0.12
        if action_count == 0:
            confidence -= 0.16
        if extracted_handlers == 0:
            confidence -= 0.1
        if generic_name and not _clean(alias):
            confidence -= 0.08
        if len([x for x in _as_list(row.get("controls")) if _clean(x)]) <= 1:
            confidence -= 0.04
        row_conf = _to_float(row.get("confidence_score"), 0.0)
        # Some upstream scans emit a fixed confidence (e.g., 0.92) for every form.
        # Ignore that placeholder so dossier confidence reflects per-form evidence quality.
        if 0 < row_conf <= 1 and abs(row_conf - 0.92) > 1e-4:
            confidence = (confidence * 0.8) + (row_conf * 0.2)
        confidence = max(0.1, min(0.98, confidence))

        dossiers.append(
            {
                "dossier_id": f"form_dossier:{idx}",
                "form_name": form_name,
                "display_name": display_name,
                "project_name": _clean(row.get("project_name")),
                "form_type": form_type,
                "status": "mapped",
                "purpose": purpose,
                "controls": [_clean(x) for x in _as_list(row.get("controls")) if _clean(x)][:120],
                "event_handlers": [_clean(x) for x in _as_list(row.get("event_handlers")) if _clean(x)][:120],
                "actions": actions,
                "procedure_summaries": [_clean(proc.get("procedure_name")) for proc in procs[:20] if _clean(proc.get("procedure_name"))],
                "coverage": {
                    "expected_handlers_count": expected_handlers,
                    "extracted_handlers_count": extracted_handlers,
                    "coverage_score": round(coverage_score, 4),
                    "confidence_score": round(confidence, 4),
                },
                "source_loc": int(row.get("source_loc", 0) or 0),
            }
        )
    return {
        "artifact_type": "form_dossier",
        "artifact_version": "1.0",
        "metadata": metadata_common,
        "dossiers": dossiers[:500],
    }


def build_raw_artifact_set_v1(output: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    """
    Build standardized raw extracted artifacts for Analyst v2 composition.
    """

    safe = _as_dict(output)
    req_pack = _as_dict(safe.get("requirements_pack"))
    legacy_inventory = _resolve_legacy_inventory(safe, req_pack)
    vb6_analysis = _resolve_vb6_analysis(safe, legacy_inventory)
    php_analysis = _as_dict(legacy_inventory.get("php_analysis"))
    context_ref = _resolve_context_ref(safe, req_pack)
    source_target_profile = _as_dict(safe.get("source_target_modernization_profile")) or _as_dict(
        req_pack.get("source_target_modernization_profile")
    )
    source_profile = _as_dict(source_target_profile.get("source"))
    target_profile = _as_dict(source_target_profile.get("target"))
    skill = _as_dict(safe.get("legacy_skill_profile")) or _as_dict(req_pack.get("legacy_skill_profile"))

    generated = generated_at or _utc_now()
    repo = _resolve_repo_hint(context_ref, source_profile)
    branch = _clean(context_ref.get("branch")) or "main"
    commit_sha = _clean(context_ref.get("commit_sha"))
    version_id = _clean(context_ref.get("version_id"))
    source_language = _clean(source_profile.get("language")) or _clean(safe.get("source_language")) or "unknown"
    ecosystem = _clean(source_profile.get("framework")) or _clean(skill.get("selected_skill_id")) or "legacy"
    skill_id = _clean(skill.get("selected_skill_id")) or "generic_legacy"
    skill_version = _clean(skill.get("version")) or "1.0.0"
    skill_confidence = _to_float(skill.get("confidence"), 0.0)
    run_id = _clean(safe.get("run_id") or context_ref.get("run_id")) or f"run_{uuid4().hex[:12]}"
    producer = {
        "agent": "Analyst Agent",
        "skill_pack": skill_id,
        "skill_version": skill_version,
    }

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
    project_members = _as_list(legacy_inventory.get("project_members"))
    forms = _as_list(legacy_inventory.get("forms"))
    controls = _as_list(legacy_inventory.get("controls"))
    event_handlers = _as_list(legacy_inventory.get("event_handlers"))
    bas_module_summary = _as_dict(legacy_inventory.get("bas_module_summary")) or _as_dict(vb6_analysis.get("bas_module_summary"))
    database_tables = [_clean(x) for x in _as_list(legacy_inventory.get("database_tables")) if _clean(x)]
    sql_catalog_rows = _as_list(legacy_inventory.get("sql_query_catalog")) or _as_list(vb6_analysis.get("sql_query_catalog"))
    sql_catalog_rows = _coalesce_sql_catalog_rows(sql_catalog_rows)
    connection_string_rows = (
        _as_list(legacy_inventory.get("connection_string_rows"))
        or _as_list(vb6_analysis.get("connection_string_rows"))
        or _as_list(legacy_inventory.get("connection_strings"))
        or _as_list(vb6_analysis.get("connection_strings"))
    )
    db_ref_rows = (
        _as_list(legacy_inventory.get("database_file_reference_rows"))
        or _as_list(vb6_analysis.get("database_file_reference_rows"))
        or _as_list(legacy_inventory.get("database_file_references"))
        or _as_list(vb6_analysis.get("database_file_references"))
        or _as_list(vb6_analysis.get("database_file_refs"))
    )
    bas_global_rows = (
        _as_list(legacy_inventory.get("module_global_declarations"))
        or _as_list(vb6_analysis.get("module_global_declarations"))
    )
    binary_companion_rows = (
        _as_list(legacy_inventory.get("binary_companion_files"))
        or _as_list(vb6_analysis.get("binary_companion_files"))
    )
    ui_event_rows = _as_list(legacy_inventory.get("ui_event_map")) or _as_list(vb6_analysis.get("ui_event_map"))
    readiness = _as_dict(legacy_inventory.get("modernization_readiness")) or _as_dict(vb6_analysis.get("modernization_readiness"))
    database_schema_text = _clean(safe.get("database_schema_input") or safe.get("database_schema"))

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

    # Recover a key legacy hazard when SQL extraction is fragmented into tokens like "Delete"/"&Delete".
    has_explicit_transaction_delete = any(
        isinstance(stmt, dict)
        and _clean(stmt.get("kind")).lower() == "delete"
        and any(_is_transaction_like_name(tbl) for tbl in _as_list(stmt.get("tables")))
        for stmt in sql_statements
    )
    delete_fragments = [
        _clean(stmt.get("raw")).strip().lower()
        for stmt in sql_statements
        if isinstance(stmt, dict)
        and _clean(stmt.get("raw")).strip()
        and _clean(stmt.get("raw")).strip().lower() in {"delete", "\"delete\"", "&delete"}
    ]
    transaction_context_seen = any(
        isinstance(stmt, dict)
        and (
            any(_is_transaction_like_name(tbl) for tbl in _as_list(stmt.get("tables")))
            or "trans" in _clean(stmt.get("raw")).lower()
        )
        for stmt in sql_statements
    )
    customer_context_seen = any(
        isinstance(stmt, dict)
        and "customer" in _clean(stmt.get("raw")).lower()
        for stmt in sql_statements
    )
    if (not has_explicit_transaction_delete) and delete_fragments and transaction_context_seen and customer_context_seen:
        inferred_sql_id = f"sql:{sql_counter}"
        sql_counter += 1
        inferred_raw = "delete from transactions where CustomerID=:expr /* inferred from fragmented legacy SQL */"
        inferred_stmt = {
            "sql_id": inferred_sql_id,
            "kind": "delete",
            "raw": inferred_raw,
            "normalized": inferred_raw,
            "tables": ["transactions"],
            "columns": ["CustomerID"],
            "parameters": [{"name": "CustomerID", "source": "variable"}],
            "usage_sites": [],
            "risk_flags": ["string_concatenation", "possible_injection"],
        }
        sql_statements.append(inferred_stmt)
        sql_raw_to_id[inferred_raw.lower()] = inferred_sql_id
        sql_id_index[inferred_sql_id] = inferred_stmt

    form_profiles, handler_form_index = _build_form_profiles(forms)

    event_entries: list[dict[str, Any]] = []
    procedure_summaries: list[dict[str, Any]] = []
    sql_map_entries: list[dict[str, Any]] = []
    sql_map_counter = 1
    for idx, row in enumerate(ui_event_rows, start=1):
        if not isinstance(row, dict):
            continue
        handler = _clean(row.get("event_handler"))
        source_file = _clean(row.get("source_file"))
        line_no = int(row.get("line", 0) or 0)
        form = _infer_form_for_event(
            row=row,
            handler=handler,
            form_profiles=form_profiles,
            handler_index=handler_form_index,
        )
        if not form:
            form = "shared_module"
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
                        "variant": _split_variant_form(form)[0],
                        "form_base": _split_variant_form(form)[1] or form,
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
                "source_file": source_file,
                "line": line_no,
                "trigger": {"control": control, "event": event},
                "handler": {
                    "symbol": symbol,
                    "evidence": (
                        [
                            {
                                "type": "file_span",
                                "file_span": {
                                    "path": source_file,
                                    "line_start": line_no,
                                    "line_end": line_no,
                                },
                                "confidence": 0.95,
                            }
                        ]
                        if source_file and line_no > 0
                        else []
                    ),
                },
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
                    },
                    *(
                        [
                            {
                                "type": "file_span",
                                "file_span": {
                                    "path": source_file,
                                    "line_start": line_no,
                                    "line_end": line_no,
                                },
                                "confidence": 0.95,
                            }
                        ]
                        if source_file and line_no > 0
                        else []
                    ),
                ],
            }
        )

    procedure_form_lookup: dict[str, str] = {}
    for proc in procedure_summaries:
        if not isinstance(proc, dict):
            continue
        pname = _clean(proc.get("procedure_name")).lower()
        pform = _clean(proc.get("form"))
        if pname and pform and pname not in procedure_form_lookup:
            procedure_form_lookup[pname] = pform
    for entry in sql_map_entries:
        if not isinstance(entry, dict):
            continue
        if _clean(entry.get("form")):
            continue
        proc_name = _clean(entry.get("procedure")).lower()
        inferred_form = _clean(procedure_form_lookup.get(proc_name))
        if inferred_form:
            entry["form"] = inferred_form

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
                    "variant": _split_variant_form(form_name)[0],
                    "form_base": _split_variant_form(form_name)[1] or form_name,
                    "procedure": procedure_name,
                    "sql_id": sql_id,
                    "operation": kind,
                    "tables": tables,
                    "risk_flags": risks,
                    "usage_sites": [f"{form_name}::{procedure_name}"],
                }
            )

    form_coverage_rows = _derive_form_coverage_rows(
        forms=forms,
        ui_event_rows=ui_event_rows,
        sql_map_rows=sql_map_entries,
    )

    dep_candidates = [
        *_as_list(legacy_inventory.get("activex_controls")),
        *_as_list(legacy_inventory.get("dll_dependencies")),
        *_as_list(legacy_inventory.get("ocx_dependencies")),
        *_as_list(legacy_inventory.get("dcx_dependencies")),
        *_as_list(legacy_inventory.get("dca_dependencies")),
        *_as_list(legacy_inventory.get("dependencies")),
        *_as_list(legacy_inventory.get("dependency_references")),
        *_as_list(vb6_analysis.get("dependency_references")),
        *_as_list(_as_dict(vb6_analysis.get("com_surface_map")).get("references")),
    ]
    dep_seen: set[str] = set()
    dependencies: list[dict[str, Any]] = []
    dep_index_by_key: dict[str, dict[str, Any]] = {}
    dep_idx = 1
    for dep in dep_candidates:
        dep_source = "legacy_inventory_scan"
        if isinstance(dep, dict):
            dep_source = _clean(dep.get("source") or dep.get("project_file") or dep.get("project_name")) or dep_source
        name, reference = _parse_dependency_identity(dep)
        if not name:
            continue
        key = name.lower()
        if key in dep_seen:
            row = dep_index_by_key.get(key)
            if row is not None:
                if dep_source and not _clean(row.get("source")):
                    row["source"] = dep_source
                if reference and not _clean(row.get("reference")):
                    row["reference"] = reference
                    row["guid"] = _extract_guid(reference)
                elif reference and _clean(row.get("reference")) and reference != _clean(row.get("reference")):
                    existing_ref = _clean(row.get("reference"))
                    row["reference"] = "; ".join([existing_ref, reference]) if reference not in existing_ref else existing_ref
                    row["guid"] = _extract_guid(row["reference"])
            continue
        dep_seen.add(key)
        row = {
            "dependency_id": f"dep:{dep_idx}",
            "name": name,
            "kind": _dependency_kind(name),
            "version": "",
            "source": dep_source,
            "reference": reference,
            "guid": _extract_guid(reference),
            "usage": {"used_by": [], "usage_sites": []},
            "surface": {
                "prog_ids": [],
                "class_ids": [],
                "late_binding_sites": 0,
                "callbyname_sites": 0,
            },
            "risk": {"tier": "medium", "notes": "", "recommended_action": "Assess replacement/interop strategy."},
        }
        dependencies.append(row)
        dep_index_by_key[key] = row
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
    static_risk_detectors_artifact = _build_static_risk_detectors(
        metadata_common=metadata_common,
        sql_statements=sql_statements,
        business_rules=rules,
        ui_event_rows=ui_event_rows,
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
        summary = (
            _clean(row.get("description"))
            or _clean(row.get("title"))
            or evidence
            or "Detector finding"
        )
        recommended = _clean(row.get("recommended_action"))
        detector_findings.append(
            {
                "detector_id": _clean(row.get("id")) or f"det:{idx}",
                "severity": severity,
                "count": int(row.get("count", 0) or 0),
                "summary": summary,
                "required_actions": (
                    [_clean(x) for x in _as_list(row.get("requires")) if _clean(x)]
                    or ([recommended] if recommended else [])
                ),
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

    disambiguated_project_labels: list[str] = []
    project_name_counts: dict[str, int] = {}
    for pidx, project in enumerate(vb6_projects, start=1):
        if not isinstance(project, dict):
            disambiguated_project_labels.append(f"Project {pidx}")
            continue
        base_name = _clean(project.get("project_name")) or f"Project {pidx}"
        key = base_name.lower()
        project_name_counts[key] = int(project_name_counts.get(key, 0) or 0) + 1
        disambiguated_project_labels.append(base_name)
    if any(count > 1 for count in project_name_counts.values()):
        seen_disambiguated: set[str] = set()
        for pidx, project in enumerate(vb6_projects, start=1):
            if not isinstance(project, dict):
                continue
            base_name = _clean(project.get("project_name")) or f"Project {pidx}"
            key = base_name.lower()
            if int(project_name_counts.get(key, 0) or 0) <= 1:
                continue
            hint = _project_disambiguation_hint(project, pidx)
            candidate = f"{base_name} ({hint})"
            unique_candidate = candidate
            suffix = 2
            while unique_candidate.lower() in seen_disambiguated:
                unique_candidate = f"{candidate} #{suffix}"
                suffix += 1
            disambiguated_project_labels[pidx - 1] = unique_candidate
            seen_disambiguated.add(unique_candidate.lower())

    source_loc_rows = _as_list(legacy_inventory.get("source_loc_by_file")) or _as_list(vb6_analysis.get("source_loc_by_file"))
    source_loc_by_file: dict[str, int] = {}
    for row in source_loc_rows[:5000]:
        if not isinstance(row, dict):
            continue
        path = _clean(row.get("path"))
        if not path:
            continue
        source_loc_by_file[path] = int(row.get("loc", 0) or 0)
    source_loc_total = int(
        legacy_inventory.get("source_loc_total", 0)
        or vb6_analysis.get("source_loc_total", 0)
        or sum(source_loc_by_file.values())
    )
    source_loc_forms = int(
        legacy_inventory.get("source_loc_forms", 0)
        or vb6_analysis.get("source_loc_forms", 0)
        or sum(loc for path, loc in source_loc_by_file.items() if path.lower().endswith((".frm", ".ctl")))
    )
    source_loc_modules = int(
        legacy_inventory.get("source_loc_modules", 0)
        or vb6_analysis.get("source_loc_modules", 0)
        or sum(loc for path, loc in source_loc_by_file.items() if path.lower().endswith(".bas"))
    )
    source_loc_classes = int(
        legacy_inventory.get("source_loc_classes", 0)
        or vb6_analysis.get("source_loc_classes", 0)
        or sum(loc for path, loc in source_loc_by_file.items() if path.lower().endswith(".cls"))
    )
    source_files_scanned = int(
        legacy_inventory.get("source_files_scanned", 0)
        or vb6_analysis.get("source_files_scanned", 0)
        or len(source_loc_by_file)
    )
    mdb_inventory_artifact = _build_mdb_inventory(
        metadata_common=metadata_common,
        source_loc_rows=source_loc_rows,
        project_members=project_members,
        sql_rows=sql_catalog_rows,
        ui_event_rows=ui_event_rows,
        forms=forms,
        db_ref_rows=db_ref_rows,
        connection_string_rows=connection_string_rows,
        binary_companion_rows=binary_companion_rows,
    )
    form_loc_profile_artifact = _build_form_loc_profile(
        metadata_common=metadata_common,
        forms=forms,
        vb6_projects=vb6_projects,
        source_loc_by_file=source_loc_by_file,
    )
    connection_string_variants_artifact = _build_connection_string_variants(
        metadata_common=metadata_common,
        sql_rows=sql_catalog_rows,
        ui_event_rows=ui_event_rows,
        connection_string_rows=connection_string_rows,
    )
    module_global_inventory_artifact = _build_module_global_inventory(
        metadata_common=metadata_common,
        bas_module_summary=bas_module_summary,
        ui_event_rows=ui_event_rows,
        bas_global_rows=bas_global_rows,
    )
    dead_form_refs_artifact = _build_dead_form_refs(
        metadata_common=metadata_common,
        ui_event_rows=ui_event_rows,
        forms=forms,
        form_profiles=form_profiles,
        handler_form_index=handler_form_index,
    )
    dataenvironment_report_mapping_artifact = _build_dataenvironment_report_mapping(
        metadata_common=metadata_common,
        project_members=project_members,
        ui_event_rows=ui_event_rows,
        form_profiles=form_profiles,
        handler_form_index=handler_form_index,
    )

    projects_out: list[dict[str, Any]] = []
    for pidx, project in enumerate(vb6_projects, start=1):
        if not isinstance(project, dict):
            continue
        project_name_resolved = _clean(disambiguated_project_labels[pidx - 1]) or _clean(project.get("project_name")) or f"Project {pidx}"
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
                    "id": f"{project_name_resolved}:member:{mcounter}",
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
                    "id": f"{project_name_resolved}:ui:{fidx}",
                    "kind": "form",
                    "name": name,
                    "controls_count": 0,
                    "event_handlers_count": 0,
                }
            )
        project_id = _clean(project.get("project_name")) or f"project:{pidx}"
        project_file = _clean(project.get("project_file"))
        if project_file:
            project_file_norm = project_file.replace("\\", "/")
            project_id = f"{project_id}|{project_file_norm}"
        projects_out.append(
            {
                "project_id": project_id,
                "name": project_name_resolved,
                "name_original": _clean(project.get("project_name_original")) or _clean(project.get("project_name")) or f"Project {pidx}",
                "type": _clean(project.get("project_type")) or "legacy_project",
                "startup": _clean(project.get("startup_object")),
                "file": _clean(project.get("project_file")),
                "source_loc_total": int(project.get("source_loc_total", 0) or 0),
                "source_loc_forms": int(project.get("source_loc_forms", 0) or 0),
                "source_loc_modules": int(project.get("source_loc_modules", 0) or 0),
                "source_loc_classes": int(project.get("source_loc_classes", 0) or 0),
                "forms": [_clean(x) for x in _as_list(project.get("forms")) if _clean(x)],
                "member_files": [_clean(x) for x in _as_list(project.get("member_files")) if _clean(x)],
                "data_touchpoints": _as_dict(project.get("data_touchpoints")),
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

    forms_count = max(
        len(forms),
        int(_as_dict(legacy_inventory).get("form_count_discovered_files", 0) or 0),
        sum((int(_as_dict(p).get("forms_count", 0) or 0) for p in vb6_projects if isinstance(p, dict))),
    )
    controls_count = len(controls)
    if controls_count == 0:
        controls_count = sum((len(_as_list(_as_dict(p).get("controls"))) for p in vb6_projects if isinstance(p, dict)))
    total_deps = len(dependencies)
    inferred_tables = _extract_tables_from_sql_catalog([row.get("raw", "") for row in sql_statements])
    inferred_keys = {name.lower() for name in inferred_tables}
    preferred_touchpoint_tables = inferred_tables[:] if inferred_tables else database_tables[:]
    if inferred_tables:
        preferred_touchpoint_tables.extend([name for name in database_tables if _clean(name).lower() in inferred_keys])
    touchpoints: list[str] = []
    seen_touchpoints: set[str] = set()
    for name in preferred_touchpoint_tables:
        clean_name = _clean(name)
        key = clean_name.lower()
        if not clean_name or key in seen_touchpoints or not _is_probable_table_name(clean_name):
            continue
        seen_touchpoints.add(key)
        touchpoints.append(clean_name)
        if len(touchpoints) >= 20:
            break
    event_handler_count_exact = int(
        legacy_inventory.get("event_handler_count_exact", 0)
        or vb6_analysis.get("event_handler_count_exact", 0)
        or len(_as_list(legacy_inventory.get("event_handler_keys")))
        or len(_as_list(vb6_analysis.get("event_handler_keys")))
        or len(event_entries)
        or len(event_handlers)
    )

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
                "forms_referenced": int(
                    _as_dict(legacy_inventory).get("form_count_referenced", 0) or 0
                ),
                "forms_unmapped": int(
                    _as_dict(legacy_inventory).get("form_count_unmapped_files", 0) or 0
                ),
                "source_loc_total": source_loc_total,
                "source_loc_forms": source_loc_forms,
                "source_loc_modules": source_loc_modules,
                "source_loc_classes": source_loc_classes,
                "source_files_scanned": source_files_scanned,
                "controls": controls_count,
                "dependencies": total_deps,
                "event_handlers": event_handler_count_exact,
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
        "form_coverage": form_coverage_rows,
        "source_loc_by_file": [
            {"path": path, "loc": int(loc or 0)}
            for path, loc in sorted(source_loc_by_file.items())
        ][:2000],
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

    source_schema_model_artifact = _build_source_schema_model(
        metadata_common=metadata_common,
        sql_statements=sql_statements,
        database_tables=database_tables,
        database_schema_text=database_schema_text,
    )
    source_query_catalog_artifact = _build_source_query_catalog(
        metadata_common=metadata_common,
        sql_statements=sql_statements,
        sql_map_entries=sql_map_entries,
    )
    source_relationship_candidates_artifact = _build_source_relationship_candidates(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
    )
    source_data_dictionary_artifact = _build_source_data_dictionary(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
    )
    source_erd_artifact = _build_source_erd(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        source_relationship_candidates=source_relationship_candidates_artifact,
    )
    source_data_dictionary_markdown_artifact = _build_source_data_dictionary_markdown(
        metadata_common=metadata_common,
        source_data_dictionary=source_data_dictionary_artifact,
        source_schema_model=source_schema_model_artifact,
    )
    source_hotspot_report_artifact = _build_source_hotspot_report(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        source_query_catalog=source_query_catalog_artifact,
    )
    source_db_profile_artifact = _build_source_db_profile(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        source_query_catalog=source_query_catalog_artifact,
        source_relationship_candidates=source_relationship_candidates_artifact,
        source_hotspot_report=source_hotspot_report_artifact,
    )
    target_schema_model_artifact = _build_target_schema_model(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        source_relationship_candidates=source_relationship_candidates_artifact,
        target_database=_clean(target_profile.get("database")) or "PostgreSQL",
    )
    target_erd_artifact = _build_target_erd(
        metadata_common=metadata_common,
        target_schema_model=target_schema_model_artifact,
    )
    target_data_dictionary_artifact = _build_target_data_dictionary(
        metadata_common=metadata_common,
        target_schema_model=target_schema_model_artifact,
    )
    schema_mapping_matrix_artifact = _build_schema_mapping_matrix(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        target_schema_model=target_schema_model_artifact,
    )
    migration_plan_artifact = _build_data_migration_plan(
        metadata_common=metadata_common,
        schema_mapping_matrix=schema_mapping_matrix_artifact,
        target_schema_model=target_schema_model_artifact,
    )
    validation_harness_spec_artifact = _build_validation_harness_spec(
        metadata_common=metadata_common,
        schema_mapping_matrix=schema_mapping_matrix_artifact,
        source_hotspot_report=source_hotspot_report_artifact,
    )
    db_qa_report_artifact = _build_db_qa_report(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        source_query_catalog=source_query_catalog_artifact,
        source_relationship_candidates=source_relationship_candidates_artifact,
        schema_mapping_matrix=schema_mapping_matrix_artifact,
        target_schema_model=target_schema_model_artifact,
    )
    schema_approval_record_artifact = _build_schema_approval_record(
        metadata_common=metadata_common,
        db_qa_report=db_qa_report_artifact,
    )
    schema_drift_report_artifact = _build_schema_drift_report(
        metadata_common=metadata_common,
        source_schema_model=source_schema_model_artifact,
        target_schema_model=target_schema_model_artifact,
    )

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
    variant_diff_report_artifact = _build_variant_diff_report(
        metadata_common=metadata_common,
        projects=projects_out,
    )
    reporting_model_artifact = _build_reporting_model(
        metadata_common=metadata_common,
        projects=projects_out,
        sql_statements=sql_statements,
    )
    identity_access_model_artifact = _build_identity_access_model(
        metadata_common=metadata_common,
        sql_statements=sql_statements,
        database_touchpoints=touchpoints,
    )
    discover_review_checklist_artifact = _build_discover_review_checklist(
        metadata_common=metadata_common,
        form_coverage_rows=form_coverage_rows,
        variant_diff_report=variant_diff_report_artifact,
        reporting_model=reporting_model_artifact,
        identity_access_model=identity_access_model_artifact,
        db_qa_report=db_qa_report_artifact,
        sql_statements=sql_statements,
        detector_findings=detector_findings,
        business_rules=rules,
        procedure_summaries=procedure_summaries,
        sql_map_entries=sql_map_entries,
    )
    repo_landscape_artifact = _build_repo_landscape(
        metadata_common=metadata_common,
        projects=projects_out,
    )
    scope_lock_artifact = _build_scope_lock(
        metadata_common=metadata_common,
        projects=projects_out,
    )
    variant_inventory_artifact = _build_variant_inventory(
        metadata_common=metadata_common,
        projects=projects_out,
    )
    data_access_map_artifact = _build_data_access_map(
        metadata_common=metadata_common,
        sql_map_entries=sql_map_entries,
    )
    recordset_ops_artifact = _build_recordset_ops(
        metadata_common=metadata_common,
        procedure_summaries=procedure_summaries,
    )
    risk_register_artifact = _build_risk_register(
        metadata_common=metadata_common,
        detector_findings=detector_findings,
        sql_statements=sql_statements,
        business_rules=rules,
    )
    orphan_analysis_artifact = _build_orphan_analysis(
        metadata_common=metadata_common,
        projects=projects_out,
        forms=forms,
        sql_map_entries=sql_map_entries,
        procedure_summaries=procedure_summaries,
        unmapped_form_file_count=int(_as_dict(legacy_inventory).get("form_count_unmapped_files", 0) or 0),
    )
    form_dossier_artifact = _build_form_dossiers(
        metadata_common=metadata_common,
        forms=forms,
        event_entries=event_entries,
        sql_map_entries=sql_map_entries,
        procedure_summaries=procedure_summaries,
    )
    engineering_quality_artifacts = _build_engineering_quality_baseline(
        metadata_common=metadata_common,
        run_id=run_id,
        generated_at=generated,
        repo=repo,
        branch=branch,
        commit_sha=commit_sha,
        projects=projects_out,
        forms=forms,
        event_entries=event_entries,
        sql_statements=sql_statements,
        sql_map_entries=sql_map_entries,
        procedure_summaries=procedure_summaries,
        dependencies=dependencies,
        form_coverage_rows=form_coverage_rows,
        source_loc_by_file=source_loc_by_file,
        orphan_analysis=orphan_analysis_artifact,
        form_dossier=form_dossier_artifact,
    )
    project_metrics_artifact = _as_dict(engineering_quality_artifacts.get("project_metrics"))
    type_metrics_artifact = _as_dict(engineering_quality_artifacts.get("type_metrics"))
    type_dependency_matrix_artifact = _as_dict(engineering_quality_artifacts.get("type_dependency_matrix"))
    runtime_dependency_matrix_artifact = _as_dict(engineering_quality_artifacts.get("runtime_dependency_matrix"))
    dead_code_report_artifact = _as_dict(engineering_quality_artifacts.get("dead_code_report"))
    third_party_usage_artifact = _as_dict(engineering_quality_artifacts.get("third_party_usage"))
    code_quality_rules_artifact = _as_dict(engineering_quality_artifacts.get("code_quality_rules"))
    quality_violation_report_artifact = _as_dict(engineering_quality_artifacts.get("quality_violation_report"))
    trend_snapshot_artifact = _as_dict(engineering_quality_artifacts.get("trend_snapshot"))
    trend_series_artifact = _as_dict(engineering_quality_artifacts.get("trend_series"))
    static_forensics_layer_artifact = _build_static_forensics_layer(
        metadata_common=metadata_common,
        project_metrics=project_metrics_artifact,
        type_metrics=type_metrics_artifact,
        type_dependency_matrix=type_dependency_matrix_artifact,
        runtime_dependency_matrix=runtime_dependency_matrix_artifact,
        dead_code_report=dead_code_report_artifact,
        code_quality_rules=code_quality_rules_artifact,
        quality_violation_report=quality_violation_report_artifact,
        trend_snapshot=trend_snapshot_artifact,
        trend_series=trend_series_artifact,
    )
    php_route_inventory_artifact = {
        **_as_dict(php_analysis.get("route_inventory")),
        "metadata": metadata_common,
    }
    php_controller_inventory_artifact = {
        **_as_dict(php_analysis.get("controller_inventory")),
        "metadata": metadata_common,
    }
    php_template_inventory_artifact = {
        **_as_dict(php_analysis.get("template_inventory")),
        "metadata": metadata_common,
    }
    php_sql_catalog_artifact = {
        **_as_dict(php_analysis.get("sql_catalog")),
        "metadata": metadata_common,
    }
    php_session_state_inventory_artifact = {
        **_as_dict(php_analysis.get("session_state_inventory")),
        "metadata": metadata_common,
    }
    php_authz_authn_inventory_artifact = {
        **_as_dict(php_analysis.get("authz_authn_inventory")),
        "metadata": metadata_common,
    }
    php_include_graph_artifact = {
        **_as_dict(php_analysis.get("include_graph")),
        "metadata": metadata_common,
    }
    php_background_job_inventory_artifact = {
        **_as_dict(php_analysis.get("background_job_inventory")),
        "metadata": metadata_common,
    }
    php_file_io_inventory_artifact = {
        **_as_dict(php_analysis.get("file_io_inventory")),
        "metadata": metadata_common,
    }
    php_validation_rules_artifact = {
        **_as_dict(php_analysis.get("validation_rules")),
        "metadata": metadata_common,
    }
    canonical_project = _clean(_as_dict(projects_out[0]).get("name")) if projects_out else "Project 1"
    artifact_context = {
        "repo": repo,
        "branch": branch,
        "commit_sha": commit_sha,
        "scope_lock_ref": "artifact://analyst/raw/scope_lock/v1",
        "canonical_project": canonical_project,
        "target_stack": {
            "language": _clean(target_profile.get("language")) or "Unknown",
            "ui": _clean(target_profile.get("framework") or target_profile.get("ui_framework")) or "Unknown",
            "db": _clean(target_profile.get("database")) or "Unknown",
        },
    }

    refs = {
        "legacy_inventory": "artifact://analyst/raw/legacy_inventory/v1",
        "repo_landscape": "artifact://analyst/raw/repo_landscape/v1",
        "scope_lock": "artifact://analyst/raw/scope_lock/v1",
        "variant_inventory": "artifact://analyst/raw/variant_inventory/v1",
        "dependency_inventory": "artifact://analyst/raw/dependency_inventory/v1",
        "event_map": "artifact://analyst/raw/event_map/v1",
        "sql_catalog": "artifact://analyst/raw/sql_catalog/v1",
        "sql_map": "artifact://analyst/raw/sql_map/v1",
        "data_access_map": "artifact://analyst/raw/data_access_map/v1",
        "recordset_ops": "artifact://analyst/raw/recordset_ops/v1",
        "procedure_summary": "artifact://analyst/raw/procedure_summary/v1",
        "form_dossier": "artifact://analyst/raw/form_dossier/v1",
        "business_rule_catalog": "artifact://analyst/raw/business_rule_catalog/v1",
        "detector_findings": "artifact://analyst/raw/detector_findings/v1",
        "risk_register": "artifact://analyst/raw/risk_register/v1",
        "orphan_analysis": "artifact://analyst/raw/orphan_analysis/v1",
        "project_metrics": "artifact://analyst/raw/project_metrics/v1",
        "static_forensics_layer": "artifact://analyst/raw/static_forensics_layer/v1",
        "type_metrics": "artifact://analyst/raw/type_metrics/v1",
        "type_dependency_matrix": "artifact://analyst/raw/type_dependency_matrix/v1",
        "runtime_dependency_matrix": "artifact://analyst/raw/runtime_dependency_matrix/v1",
        "dead_code_report": "artifact://analyst/raw/dead_code_report/v1",
        "third_party_usage": "artifact://analyst/raw/third_party_usage/v1",
        "code_quality_rules": "artifact://analyst/raw/code_quality_rules/v1",
        "quality_violation_report": "artifact://analyst/raw/quality_violation_report/v1",
        "trend_snapshot": "artifact://analyst/raw/trend_snapshot/v1",
        "trend_series": "artifact://analyst/raw/trend_series/v1",
        "mdb_inventory": "artifact://analyst/raw/mdb_inventory/v1",
        "form_loc_profile": "artifact://analyst/raw/form_loc_profile/v1",
        "connection_string_variants": "artifact://analyst/raw/connection_string_variants/v1",
        "module_global_inventory": "artifact://analyst/raw/module_global_inventory/v1",
        "dead_form_refs": "artifact://analyst/raw/dead_form_refs/v1",
        "dataenvironment_report_mapping": "artifact://analyst/raw/dataenvironment_report_mapping/v1",
        "static_risk_detectors": "artifact://analyst/raw/static_risk_detectors/v1",
        "delivery_constitution": "artifact://analyst/raw/delivery_constitution/v1",
        "source_db_profile": "artifact://analyst/raw/source_db_profile/v1",
        "source_schema_model": "artifact://analyst/raw/source_schema_model/v1",
        "source_query_catalog": "artifact://analyst/raw/source_query_catalog/v1",
        "source_relationship_candidates": "artifact://analyst/raw/source_relationship_candidates/v1",
        "source_data_dictionary": "artifact://analyst/raw/source_data_dictionary/v1",
        "source_data_dictionary_markdown": "artifact://analyst/raw/source_data_dictionary_markdown/v1",
        "source_erd": "artifact://analyst/raw/source_erd/v1",
        "source_hotspot_report": "artifact://analyst/raw/source_hotspot_report/v1",
        "target_schema_model": "artifact://analyst/raw/target_schema_model/v1",
        "target_erd": "artifact://analyst/raw/target_erd/v1",
        "target_data_dictionary": "artifact://analyst/raw/target_data_dictionary/v1",
        "schema_mapping_matrix": "artifact://analyst/raw/schema_mapping_matrix/v1",
        "migration_plan": "artifact://analyst/raw/migration_plan/v1",
        "validation_harness_spec": "artifact://analyst/raw/validation_harness_spec/v1",
        "db_qa_report": "artifact://analyst/raw/db_qa_report/v1",
        "schema_approval_record": "artifact://analyst/raw/schema_approval_record/v1",
        "schema_drift_report": "artifact://analyst/raw/schema_drift_report/v1",
        "variant_diff_report": "artifact://analyst/raw/variant_diff_report/v1",
        "reporting_model": "artifact://analyst/raw/reporting_model/v1",
        "identity_access_model": "artifact://analyst/raw/identity_access_model/v1",
        "discover_review_checklist": "artifact://analyst/raw/discover_review_checklist/v1",
        "php_route_inventory": "artifact://analyst/raw/php_route_inventory/v1",
        "php_controller_inventory": "artifact://analyst/raw/php_controller_inventory/v1",
        "php_template_inventory": "artifact://analyst/raw/php_template_inventory/v1",
        "php_sql_catalog": "artifact://analyst/raw/php_sql_catalog/v1",
        "php_session_state_inventory": "artifact://analyst/raw/php_session_state_inventory/v1",
        "php_authz_authn_inventory": "artifact://analyst/raw/php_authz_authn_inventory/v1",
        "php_include_graph": "artifact://analyst/raw/php_include_graph/v1",
        "php_background_job_inventory": "artifact://analyst/raw/php_background_job_inventory/v1",
        "php_file_io_inventory": "artifact://analyst/raw/php_file_io_inventory/v1",
        "php_validation_rules": "artifact://analyst/raw/php_validation_rules/v1",
        "artifact_index": "artifact://analyst/raw/artifact_index/v1",
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
            {"type": "repo_landscape", "ref": refs["repo_landscape"]},
            {"type": "scope_lock", "ref": refs["scope_lock"]},
            {"type": "variant_inventory", "ref": refs["variant_inventory"]},
            {"type": "dependency_inventory", "ref": refs["dependency_inventory"]},
            {"type": "event_map", "ref": refs["event_map"]},
            {"type": "sql_catalog", "ref": refs["sql_catalog"]},
            {"type": "sql_map", "ref": refs["sql_map"]},
            {"type": "data_access_map", "ref": refs["data_access_map"]},
            {"type": "recordset_ops", "ref": refs["recordset_ops"]},
            {"type": "procedure_summary", "ref": refs["procedure_summary"]},
            {"type": "form_dossier", "ref": refs["form_dossier"]},
            {"type": "business_rule_catalog", "ref": refs["business_rule_catalog"]},
            {"type": "detector_findings", "ref": refs["detector_findings"]},
            {"type": "risk_register", "ref": refs["risk_register"]},
            {"type": "orphan_analysis", "ref": refs["orphan_analysis"]},
            {"type": "project_metrics", "ref": refs["project_metrics"]},
            {"type": "static_forensics_layer", "ref": refs["static_forensics_layer"]},
            {"type": "type_metrics", "ref": refs["type_metrics"]},
            {"type": "type_dependency_matrix", "ref": refs["type_dependency_matrix"]},
            {"type": "runtime_dependency_matrix", "ref": refs["runtime_dependency_matrix"]},
            {"type": "dead_code_report", "ref": refs["dead_code_report"]},
            {"type": "third_party_usage", "ref": refs["third_party_usage"]},
            {"type": "code_quality_rules", "ref": refs["code_quality_rules"]},
            {"type": "quality_violation_report", "ref": refs["quality_violation_report"]},
            {"type": "trend_snapshot", "ref": refs["trend_snapshot"]},
            {"type": "trend_series", "ref": refs["trend_series"]},
            {"type": "mdb_inventory", "ref": refs["mdb_inventory"]},
            {"type": "form_loc_profile", "ref": refs["form_loc_profile"]},
            {"type": "connection_string_variants", "ref": refs["connection_string_variants"]},
            {"type": "module_global_inventory", "ref": refs["module_global_inventory"]},
            {"type": "dead_form_refs", "ref": refs["dead_form_refs"]},
            {"type": "dataenvironment_report_mapping", "ref": refs["dataenvironment_report_mapping"]},
            {"type": "static_risk_detectors", "ref": refs["static_risk_detectors"]},
            {"type": "delivery_constitution", "ref": refs["delivery_constitution"]},
            {"type": "source_db_profile", "ref": refs["source_db_profile"]},
            {"type": "source_schema_model", "ref": refs["source_schema_model"]},
            {"type": "source_query_catalog", "ref": refs["source_query_catalog"]},
            {"type": "source_relationship_candidates", "ref": refs["source_relationship_candidates"]},
            {"type": "source_data_dictionary", "ref": refs["source_data_dictionary"]},
            {"type": "source_data_dictionary_markdown", "ref": refs["source_data_dictionary_markdown"]},
            {"type": "source_erd", "ref": refs["source_erd"]},
            {"type": "source_hotspot_report", "ref": refs["source_hotspot_report"]},
            {"type": "target_schema_model", "ref": refs["target_schema_model"]},
            {"type": "target_erd", "ref": refs["target_erd"]},
            {"type": "target_data_dictionary", "ref": refs["target_data_dictionary"]},
            {"type": "schema_mapping_matrix", "ref": refs["schema_mapping_matrix"]},
            {"type": "migration_plan", "ref": refs["migration_plan"]},
            {"type": "validation_harness_spec", "ref": refs["validation_harness_spec"]},
            {"type": "db_qa_report", "ref": refs["db_qa_report"]},
            {"type": "schema_approval_record", "ref": refs["schema_approval_record"]},
            {"type": "schema_drift_report", "ref": refs["schema_drift_report"]},
            {"type": "variant_diff_report", "ref": refs["variant_diff_report"]},
            {"type": "reporting_model", "ref": refs["reporting_model"]},
            {"type": "identity_access_model", "ref": refs["identity_access_model"]},
            {"type": "discover_review_checklist", "ref": refs["discover_review_checklist"]},
            *(
                [
                    {"type": "php_route_inventory", "ref": refs["php_route_inventory"]},
                    {"type": "php_controller_inventory", "ref": refs["php_controller_inventory"]},
                    {"type": "php_template_inventory", "ref": refs["php_template_inventory"]},
                    {"type": "php_sql_catalog", "ref": refs["php_sql_catalog"]},
                    {"type": "php_session_state_inventory", "ref": refs["php_session_state_inventory"]},
                    {"type": "php_authz_authn_inventory", "ref": refs["php_authz_authn_inventory"]},
                    {"type": "php_include_graph", "ref": refs["php_include_graph"]},
                    {"type": "php_background_job_inventory", "ref": refs["php_background_job_inventory"]},
                    {"type": "php_file_io_inventory", "ref": refs["php_file_io_inventory"]},
                    {"type": "php_validation_rules", "ref": refs["php_validation_rules"]},
                ]
                if php_analysis
                else []
            ),
        ],
    }

    artifacts = {
        "legacy_inventory": legacy_inventory_artifact,
        "repo_landscape": repo_landscape_artifact,
        "scope_lock": scope_lock_artifact,
        "variant_inventory": variant_inventory_artifact,
        "dependency_inventory": dependency_inventory_artifact,
        "event_map": event_map_artifact,
        "sql_catalog": sql_catalog_artifact,
        "sql_map": sql_map_artifact,
        "data_access_map": data_access_map_artifact,
        "recordset_ops": recordset_ops_artifact,
        "procedure_summary": procedure_summary_artifact,
        "form_dossier": form_dossier_artifact,
        "business_rule_catalog": business_rule_catalog_artifact,
        "detector_findings": detector_findings_artifact,
        "risk_register": risk_register_artifact,
        "orphan_analysis": orphan_analysis_artifact,
        "project_metrics": project_metrics_artifact,
        "static_forensics_layer": static_forensics_layer_artifact,
        "type_metrics": type_metrics_artifact,
        "type_dependency_matrix": type_dependency_matrix_artifact,
        "runtime_dependency_matrix": runtime_dependency_matrix_artifact,
        "dead_code_report": dead_code_report_artifact,
        "third_party_usage": third_party_usage_artifact,
        "code_quality_rules": code_quality_rules_artifact,
        "quality_violation_report": quality_violation_report_artifact,
        "trend_snapshot": trend_snapshot_artifact,
        "trend_series": trend_series_artifact,
        "mdb_inventory": mdb_inventory_artifact,
        "form_loc_profile": form_loc_profile_artifact,
        "connection_string_variants": connection_string_variants_artifact,
        "module_global_inventory": module_global_inventory_artifact,
        "dead_form_refs": dead_form_refs_artifact,
        "dataenvironment_report_mapping": dataenvironment_report_mapping_artifact,
        "static_risk_detectors": static_risk_detectors_artifact,
        "delivery_constitution": delivery_constitution_artifact,
        "source_db_profile": source_db_profile_artifact,
        "source_schema_model": source_schema_model_artifact,
        "source_query_catalog": source_query_catalog_artifact,
        "source_relationship_candidates": source_relationship_candidates_artifact,
        "source_data_dictionary": source_data_dictionary_artifact,
        "source_data_dictionary_markdown": source_data_dictionary_markdown_artifact,
        "source_erd": source_erd_artifact,
        "source_hotspot_report": source_hotspot_report_artifact,
        "target_schema_model": target_schema_model_artifact,
        "target_erd": target_erd_artifact,
        "target_data_dictionary": target_data_dictionary_artifact,
        "schema_mapping_matrix": schema_mapping_matrix_artifact,
        "migration_plan": migration_plan_artifact,
        "validation_harness_spec": validation_harness_spec_artifact,
        "db_qa_report": db_qa_report_artifact,
        "schema_approval_record": schema_approval_record_artifact,
        "schema_drift_report": schema_drift_report_artifact,
        "variant_diff_report": variant_diff_report_artifact,
        "reporting_model": reporting_model_artifact,
        "identity_access_model": identity_access_model_artifact,
        "discover_review_checklist": discover_review_checklist_artifact,
        **(
            {
                "php_route_inventory": php_route_inventory_artifact,
                "php_controller_inventory": php_controller_inventory_artifact,
                "php_template_inventory": php_template_inventory_artifact,
                "php_sql_catalog": php_sql_catalog_artifact,
                "php_session_state_inventory": php_session_state_inventory_artifact,
                "php_authz_authn_inventory": php_authz_authn_inventory_artifact,
                "php_include_graph": php_include_graph_artifact,
                "php_background_job_inventory": php_background_job_inventory_artifact,
                "php_file_io_inventory": php_file_io_inventory_artifact,
                "php_validation_rules": php_validation_rules_artifact,
            }
            if php_analysis
            else {}
        ),
        "artifact_index": artifact_index,
    }
    for key, artifact in list(artifacts.items()):
        artifacts[key] = _apply_common_envelope(
            artifact,
            run_id=run_id,
            generated_at=generated,
            producer=producer,
            context=artifact_context,
        )
    refs = {
        key: _artifact_ref(artifacts[key], fallback)
        for key, fallback in refs.items()
        if key in artifacts
    }
    for artifact in artifacts.values():
        artifact_context_ref = _as_dict(artifact.get("context"))
        artifact_context_ref["scope_lock_ref"] = refs.get("scope_lock", artifact_context_ref.get("scope_lock_ref", ""))
        artifact["context"] = artifact_context_ref
    artifacts["artifact_index"]["artifacts"] = [
        {"type": entry["type"], "ref": refs.get(entry["type"], entry["ref"])}
        for entry in _as_list(artifacts["artifact_index"].get("artifacts"))
        if isinstance(entry, dict)
    ]
    return {
        **artifacts,
        "artifact_refs": refs,
        "raw_compiler_version": "2.7.0",
    }


def build_analyst_report_v2(output: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    """
    Build or pass through analyst_report_v2.

    If `output.analyst_report_v2` already looks valid, it is returned unchanged.
    """

    safe = _as_dict(output)
    prebuilt = _as_dict(safe.get("analyst_report_v2"))
    prebuilt_compiler = _clean(_as_dict(prebuilt.get("metadata")).get("compiler_version"))
    prebuilt_is_current = (
        _clean(prebuilt.get("artifact_type")) == "analyst_report"
        and _clean(prebuilt.get("artifact_version")) == "2.0"
        and prebuilt_compiler == "2.7.0"
    )
    prebuilt_qa = _as_dict(prebuilt.get("qa_report_v1"))
    prebuilt_qa_is_current = _clean(prebuilt_qa.get("qa_runtime_version")) == QA_RUNTIME_VERSION
    if prebuilt_is_current and prebuilt_qa and prebuilt_qa_is_current:
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
    if _clean(raw_artifacts.get("raw_compiler_version")) != "2.7.0":
        raw_artifacts = build_raw_artifact_set_v1(safe, generated_at=generated_at)
    if prebuilt_is_current:
        return _attach_qa_report_v1(
            prebuilt,
            output=safe,
            raw_artifacts=raw_artifacts,
            generated_at=generated_at,
        )
    raw_legacy_inventory = _as_dict(raw_artifacts.get("legacy_inventory"))
    raw_event_map_entries = _as_list(_as_dict(raw_artifacts.get("event_map")).get("entries"))
    variant_diff_report = _as_dict(raw_artifacts.get("variant_diff_report"))
    reporting_model = _as_dict(raw_artifacts.get("reporting_model"))
    identity_access_model = _as_dict(raw_artifacts.get("identity_access_model"))
    risk_register = _as_dict(raw_artifacts.get("risk_register"))
    discover_review_checklist = _as_dict(raw_artifacts.get("discover_review_checklist"))
    source_db_profile = _as_dict(raw_artifacts.get("source_db_profile"))
    source_schema_model = _as_dict(raw_artifacts.get("source_schema_model"))
    schema_mapping_matrix = _as_dict(raw_artifacts.get("schema_mapping_matrix"))
    db_qa_report = _as_dict(raw_artifacts.get("db_qa_report"))

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
    form_coverage_rows = _as_list(legacy_inventory.get("form_coverage")) or _as_list(raw_legacy_inventory.get("form_coverage"))
    forms_count = max(
        len(legacy_forms),
        project_form_count,
        len(form_coverage_rows),
        int(legacy_inventory.get("form_count_discovered_files", 0) or 0),
    )
    forms_referenced = int(
        legacy_inventory.get("form_count_referenced", 0) or project_form_count
    )
    forms_unmapped = int(
        legacy_inventory.get("form_count_unmapped_files", 0)
        or max(0, forms_count - forms_referenced)
    )
    source_loc_rows = _as_list(legacy_inventory.get("source_loc_by_file")) or _as_list(vb6_analysis.get("source_loc_by_file"))
    source_loc_by_file: dict[str, int] = {}
    for row in source_loc_rows[:5000]:
        if not isinstance(row, dict):
            continue
        path = _clean(row.get("path"))
        if not path:
            continue
        source_loc_by_file[path] = int(row.get("loc", 0) or 0)
    source_loc_total = int(
        legacy_inventory.get("source_loc_total", 0)
        or vb6_analysis.get("source_loc_total", 0)
        or sum(source_loc_by_file.values())
    )
    source_loc_forms = int(
        legacy_inventory.get("source_loc_forms", 0)
        or vb6_analysis.get("source_loc_forms", 0)
        or sum(loc for path, loc in source_loc_by_file.items() if path.lower().endswith((".frm", ".ctl")))
    )
    source_loc_modules = int(
        legacy_inventory.get("source_loc_modules", 0)
        or vb6_analysis.get("source_loc_modules", 0)
        or sum(loc for path, loc in source_loc_by_file.items() if path.lower().endswith(".bas"))
    )
    source_loc_classes = int(
        legacy_inventory.get("source_loc_classes", 0)
        or vb6_analysis.get("source_loc_classes", 0)
        or sum(loc for path, loc in source_loc_by_file.items() if path.lower().endswith(".cls"))
    )
    source_files_scanned = int(
        legacy_inventory.get("source_files_scanned", 0)
        or vb6_analysis.get("source_files_scanned", 0)
        or len(source_loc_by_file)
    )
    event_handler_count_exact = int(
        legacy_inventory.get("event_handler_count_exact", 0)
        or vb6_analysis.get("event_handler_count_exact", 0)
        or len(_as_list(legacy_inventory.get("event_handler_keys")))
        or len(_as_list(vb6_analysis.get("event_handler_keys")))
        or len(ui_event_map)
        or len(event_handlers)
    )

    db_tables = [_clean(x) for x in _as_list(legacy_inventory.get("database_tables")) if _clean(x)]
    inferred_tables = _extract_tables_from_sql_catalog(sql_catalog)
    inferred_keys = {name.lower() for name in inferred_tables}
    preferred_tables = inferred_tables[:] if inferred_tables else db_tables[:]
    if inferred_tables:
        preferred_tables.extend([name for name in db_tables if name.lower() in inferred_keys])
    tables_touched: list[str] = []
    seen_tables: set[str] = set()
    for table in preferred_tables:
        if not _is_probable_table_name(table):
            continue
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

    variant_schema = _as_dict(variant_diff_report.get("schema_divergence"))
    if bool(variant_schema.get("detected")):
        blocking_pairs = _as_list(variant_schema.get("blocking_pairs"))
        pair_count = len(_as_list(variant_schema.get("pairs")))
        top_risks.append(
            {
                "id": "RISK-VARIANT-SCHEMA",
                "severity": "high",
                "description": (
                    f"Cross-variant schema naming divergence detected in {pair_count} pair(s)"
                    + (f", including {len(blocking_pairs)} transaction-like conflict pair(s)." if blocking_pairs else ".")
                ),
                "mitigation": "Resolve canonical variant/schema strategy (DEC-VARIANT-001) before planning and backlog lock.",
                "evidence_refs": ["variant_diff_report", "variant_schema_divergence"],
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
    review_checks = _as_list(discover_review_checklist.get("checks"))
    review_by_id = {
        _clean(row.get("id")).lower(): row
        for row in review_checks
        if isinstance(row, dict) and _clean(row.get("id"))
    }
    variant_check = _as_dict(review_by_id.get("variant_resolution"))
    variant_schema_check = _as_dict(review_by_id.get("variant_schema_divergence"))
    variant_schema_flagged = _clean(variant_schema_check.get("status")).upper() in {"FAIL", "WARN"}
    if _clean(variant_check.get("status")).upper() in {"FAIL", "WARN"} or _clean(variant_diff_report.get("status")).upper() in {"FAIL", "WARN"}:
        blocking_decisions.append(
            {
                "id": "DEC-VARIANT-001",
                "question": (
                    "Resolve legacy project variant scope before planning execution"
                    + (" and address cross-variant schema divergence." if variant_schema_flagged else ".")
                ),
                "options": ["Canonical project only", "Merge variants", "Modernize variants separately"],
                "default_recommendation": "Select canonical variant and capture explicit merge/out-of-scope decision in Review.",
                "impact_if_wrong": "Inaccurate scope, duplicated effort, and parity gaps across variants.",
            }
        )
    reporting_check = _as_dict(review_by_id.get("report_model_reconciled"))
    if _clean(reporting_check.get("status")).upper() in {"FAIL", "WARN"}:
        blocking_decisions.append(
            {
                "id": "DEC-REPORT-001",
                "question": "Reconcile DataEnvironment/DataReport mappings and path assumptions for report parity.",
                "options": ["Keep current report model", "Remap report model during migration", "De-scope legacy reports"],
                "default_recommendation": "Map all active report entrypoints and resolve unknown DataEnvironments before build planning.",
                "impact_if_wrong": "Hidden reporting scope and release-time report regressions.",
            }
        )
    identity_check = _as_dict(review_by_id.get("identity_access_model"))
    if _clean(identity_check.get("status")).upper() in {"FAIL", "WARN"}:
        blocking_decisions.append(
            {
                "id": "DEC-IAM-001",
                "question": "Confirm identity/access model (role model, multi-user assumptions, and credential handling).",
                "options": ["Single-user parity", "Multi-role RBAC model", "Hybrid transitional model"],
                "default_recommendation": "Define target role model and credential policy before implementation.",
                "impact_if_wrong": "Authorization defects and security/compliance regressions.",
            }
        )
    schema_check = _as_dict(review_by_id.get("schema_key_verification"))
    if _clean(schema_check.get("status")).upper() in {"FAIL", "WARN"}:
        blocking_decisions.append(
            {
                "id": "DEC-SCHEMA-KEY-001",
                "question": "Transaction delete-key behavior requires explicit decision.",
                "options": ["Preserve existing delete-by-customer behavior", "Introduce transaction key and migrate behavior"],
                "default_recommendation": "Adopt explicit transaction key with migration plan and backward-compatibility checks.",
                "impact_if_wrong": "Data integrity risk and unintended bulk transaction deletion.",
            }
        )
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
    grounding_terms: set[str] = set()
    for src in (
        tables_touched,
        [_clean(x) for x in _as_list(legacy_inventory.get("database_tables")) if _clean(x)],
        [_clean(x) for x in _as_list(legacy_inventory.get("procedures")) if _clean(x)],
    ):
        for item in src:
            for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", _clean(item).lower()):
                grounding_terms.add(token)
    for form in legacy_forms:
        if isinstance(form, dict):
            text = " ".join([
                _clean(form.get("form_name")),
                _clean(form.get("base_form_name")),
                _clean(form.get("business_use")),
            ])
        else:
            text = _clean(form)
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", text.lower()):
            grounding_terms.add(token)
    for rule in business_rules[:240]:
        if not isinstance(rule, dict):
            continue
        for token in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", _clean(rule.get("statement")).lower()):
            grounding_terms.add(token)

    proposed_markers = (
        "payment gateway",
        "horizontal scaling",
        "kubernetes",
        "microservice",
        "1000 concurrent",
        "10,000 concurrent",
        "99.9%",
        "failover mechanism",
    )
    discovery_only_markers = (
        "identify all",
        "document all",
        "extract",
        "compile legacy",
        "legacy inventory",
        "inventory report",
        "map activex dependencies",
        "document business rules",
        "analyze legacy",
    )
    risk_rows = _as_list(risk_register.get("risks"))
    proposed_extensions: list[dict[str, Any]] = []
    def classify_grounding_text(text: str) -> tuple[str, str]:
        lowered = _clean(text).lower()
        if any(marker in lowered for marker in proposed_markers):
            return ("proposed_extension", "template-style capability/scale target not grounded in legacy evidence")
        tokens = {t for t in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", lowered)}
        overlap = len(tokens.intersection(grounding_terms))
        if overlap >= 2:
            return ("derived_from_legacy", f"matched {overlap} legacy evidence terms")
        if overlap >= 1:
            return ("derived_from_legacy", "matched legacy evidence terms")
        return ("proposed_extension", "no direct evidence linkage found in extracted legacy artifacts")
    backlog_items: list[dict[str, Any]] = []
    for idx, item in enumerate(functional, start=1):
        if not isinstance(item, dict):
            continue
        rid = _clean(item.get("id")) or f"FR-{idx:03d}"
        acceptance = [_clean(x) for x in _as_list(item.get("acceptance_criteria")) if _clean(x)]
        title = _clean(item.get("title")) or rid
        outcome = _clean(item.get("description")) or "Deliver functional parity for this requirement."
        classification, reason = classify_grounding_text(" ".join([title, outcome, " ".join(acceptance)]))
        row = {
            "id": rid,
            "type": "functional",
            "priority": _normalize_priority(item.get("priority")),
            "title": title,
            "outcome": outcome,
            "acceptance_criteria": acceptance,
            "depends_on": [],
            "evidence_expected": ["traceability_matrix", "functional_test_report"],
            "grounding": {"classification": classification, "reason": reason},
        }
        combined_lower = " ".join([title, outcome, " ".join(acceptance)]).lower()
        if any(marker in combined_lower for marker in discovery_only_markers):
            row["grounding"] = {
                "classification": "proposed_extension",
                "reason": "analysis-time/discovery task; excluded from modernization delivery backlog",
            }
            proposed_extensions.append(row)
            continue
        if classification == "derived_from_legacy":
            backlog_items.append(row)
        else:
            proposed_extensions.append(row)
    for idx, item in enumerate(non_functional, start=1):
        if not isinstance(item, dict):
            continue
        rid = _clean(item.get("id")) or f"NFR-{idx:03d}"
        acceptance = [_clean(x) for x in _as_list(item.get("acceptance_criteria")) if _clean(x)]
        title = _clean(item.get("title")) or rid
        outcome = _clean(item.get("description")) or "Deliver non-functional controls."
        classification, reason = classify_grounding_text(" ".join([title, outcome, " ".join(acceptance)]))
        row = {
            "id": rid,
            "type": "non_functional",
            "priority": "P1",
            "title": title,
            "outcome": outcome,
            "acceptance_criteria": acceptance,
            "depends_on": [],
            "evidence_expected": ["nfr_validation_report", "quality_gate_report"],
            "grounding": {"classification": classification, "reason": reason},
        }
        if classification == "derived_from_legacy":
            backlog_items.append(row)
        else:
            proposed_extensions.append(row)

    risk_backlog_items = _derive_risk_backlog_items(
        risk_rows=[row for row in risk_rows if isinstance(row, dict)],
        review_by_id={k: _as_dict(v) for k, v in review_by_id.items()},
    )
    existing_titles = {_clean(item.get("title")).lower() for item in backlog_items if isinstance(item, dict) and _clean(item.get("title"))}
    for row in risk_backlog_items:
        title_key = _clean(_as_dict(row).get("title")).lower()
        if not title_key or title_key in existing_titles:
            continue
        existing_titles.add(title_key)
        backlog_items.append(row)

    if not backlog_items and proposed_extensions:
        backlog_items = proposed_extensions[:8]
    backlog_items = backlog_items[:80]

    bdd_contract = _as_dict(safe.get("bdd_contract"))
    if not bdd_contract:
        bdd_contract = _as_dict(req_pack.get("bdd_contract"))
    bdd_features = _as_list(bdd_contract.get("features"))

    golden_flows = _derive_golden_flows(
        ui_event_map,
        legacy_forms,
        bdd_features,
        raw_event_map_entries,
    )
    generic_bdd_count = sum(1 for feature in bdd_features if _is_generic_bdd_feature(feature))
    bdd_scenario_count = 0
    for feature in bdd_features:
        if not isinstance(feature, dict):
            continue
        scenarios = _as_list(feature.get("scenarios"))
        if scenarios:
            bdd_scenario_count += len(scenarios)
            continue
        gherkin = _clean(feature.get("gherkin"))
        if gherkin:
            bdd_scenario_count += len(re.findall(r"(?im)^\s*scenario(?:\s+outline)?\s*:", gherkin))
    linked_flow_count = sum(1 for flow in golden_flows if _as_list(_as_dict(flow).get("bdd_scenario_ids")))
    handler_named_flows = sum(
        1
        for flow in golden_flows
        if re.fullmatch(r"[A-Za-z_]+\d*_(click|change|load|keypress|gotfocus|lostfocus)", _clean(_as_dict(flow).get("name")).lower())
    )

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
    bdd_grounding_result = "pass"
    bdd_grounding_description = "BDD scenarios are grounded in extracted legacy flows."
    bdd_grounding_remediation = ""
    if not bdd_features:
        bdd_grounding_result = "fail"
        bdd_grounding_description = "No BDD features were generated."
        bdd_grounding_remediation = "Generate BDD scenarios from top legacy golden flows before planning."
    elif bdd_scenario_count == 0:
        bdd_grounding_result = "fail"
        bdd_grounding_description = "BDD features exist, but no explicit scenarios were emitted."
        bdd_grounding_remediation = "Emit Scenario blocks mapped to golden flows and SQL touchpoints."
    elif generic_bdd_count > 0:
        bdd_grounding_result = "fail" if generic_bdd_count >= max(1, len(bdd_features) // 2) else "warn"
        bdd_grounding_description = (
            f"{generic_bdd_count} BDD feature(s) appear generic; scenarios are not sufficiently tied to legacy flow evidence."
        )
        bdd_grounding_remediation = "Regenerate BDD from UI Event Map golden flows and SQL-map evidence."
    elif linked_flow_count == 0:
        bdd_grounding_result = "fail"
        bdd_grounding_description = "BDD scenarios are not linked to any extracted golden flow."
        bdd_grounding_remediation = "Link scenarios to real entrypoints (form::handler) and SQL touchpoints."
    elif handler_named_flows == len(golden_flows) and linked_flow_count < max(1, len(golden_flows) // 3):
        bdd_grounding_result = "warn"
        bdd_grounding_description = "Golden flows are mostly raw handler names with weak scenario linkage."
        bdd_grounding_remediation = "Promote business scenario names and map them to handler evidence."
    quality_gates.append(
        {
            "id": "bdd_flow_grounding",
            "result": bdd_grounding_result,
            "description": bdd_grounding_description,
            "remediation": bdd_grounding_remediation,
        }
    )
    for row in review_checks:
        if not isinstance(row, dict):
            continue
        status = _clean(row.get("status")).lower()
        result = "pass" if status == "pass" else ("fail" if status == "fail" else "warn")
        quality_gates.append(
            {
                "id": _clean(row.get("id")) or f"review_gate_{len(quality_gates) + 1}",
                "result": result,
                "description": _clean(row.get("detail") or row.get("title")) or "Discover review check",
                "remediation": "Resolve review checklist item before downstream planning." if result in {"warn", "fail"} else "",
            }
        )

    derived_backlog_count = sum(
        1
        for row in backlog_items
        if isinstance(row, dict)
        and _clean(_as_dict(row.get("grounding")).get("classification")) == "derived_from_legacy"
        and _clean(row.get("type")) in {"functional", "non_functional", "risk_remediation"}
    )
    risk_remediation_count = sum(
        1 for row in backlog_items if isinstance(row, dict) and _clean(row.get("type")) == "risk_remediation"
    )
    high_risk_count = sum(
        1
        for row in risk_rows
        if isinstance(row, dict) and _clean(row.get("severity")).lower() in {"high", "critical"}
    )
    minimum_required = 3 if forms_count <= 12 else (4 if forms_count <= 30 else 6)
    expected_quality_result = "pass"
    expected_quality_desc = (
        f"Backlog grounded in discovered behavior ({derived_backlog_count} derived item(s), threshold {minimum_required})."
    )
    expected_quality_remediation = ""
    if derived_backlog_count < minimum_required:
        expected_quality_result = "fail"
        expected_quality_desc = (
            f"Derived backlog coverage below dynamic threshold ({derived_backlog_count}/{minimum_required}) "
            f"for discovered scope ({forms_count} forms)."
        )
        expected_quality_remediation = "Add missing parity requirements from form dossiers, SQL map, and risk register."
    elif high_risk_count > 0 and risk_remediation_count == 0:
        expected_quality_result = "warn"
        expected_quality_desc = (
            "Backlog is legacy-grounded but lacks explicit risk-remediation items for discovered high-severity risks."
        )
        expected_quality_remediation = "Add risk-driven remediation backlog items before planning lock."

    req_gate_idx = next(
        (
            idx
            for idx, row in enumerate(quality_gates)
            if _clean(_as_dict(row).get("id")).lower() == "requirements_completeness"
        ),
        None,
    )
    if req_gate_idx is None:
        quality_gates.append(
            {
                "id": "requirements_completeness",
                "result": expected_quality_result,
                "description": expected_quality_desc,
                "remediation": expected_quality_remediation,
            }
        )
    else:
        row = _as_dict(quality_gates[req_gate_idx])
        row["result"] = expected_quality_result
        row["description"] = expected_quality_desc
        row["remediation"] = expected_quality_remediation
        quality_gates[req_gate_idx] = row

    compliance_gate_idx = next(
        (
            idx
            for idx, row in enumerate(quality_gates)
            if _clean(_as_dict(row).get("id")).lower() == "compliance_constraints_applied"
        ),
        None,
    )
    if compliance_gate_idx is not None:
        triggered_controls = _as_list(_as_dict(req_pack.get("compliance")).get("controls_triggered")) or _as_list(safe.get("regulatory_constraints"))
        has_security_risks = any(
            isinstance(risk, dict)
            and any(tok in _clean(risk.get("description")).lower() for tok in ("injection", "credential", "mask", "privacy", "audit"))
            for risk in risk_rows
        )
        if (not triggered_controls) and has_security_risks:
            row = _as_dict(quality_gates[compliance_gate_idx])
            row["result"] = "fail"
            row["description"] = (
                "No explicit compliance constraints were linked, but security/privacy risks were detected in legacy behavior."
            )
            row["remediation"] = (
                "Map applicable controls to requirements/tests (e.g., credential handling, audit logging, masking) before planning lock."
            )
            quality_gates[compliance_gate_idx] = row

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
    target_profile = _as_dict(source_target_profile.get("target"))
    repo_hint = _resolve_repo_hint(context_ref, source_profile)

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
    artifact_index_ref = _clean(refs.get("artifact_index")) or "artifact://artifact_index/1.0/unknown"
    run_id = _clean(safe.get("run_id") or context_ref.get("run_id")) or f"run_{uuid4().hex[:12]}"
    generated = generated_at or _utc_now()
    canonical_project = _clean(_as_dict(_as_list(vb6_projects)[0]).get("project_name")) if vb6_projects else "Project 1"
    procedure_rows = _as_list(_as_dict(raw_artifacts.get("procedure_summary")).get("procedures"))
    form_dossier_rows = _as_list(_as_dict(raw_artifacts.get("form_dossier")).get("dossiers"))
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
    sql_rows_for_conf = _as_list(_as_dict(raw_artifacts.get("sql_catalog")).get("statements")) or sql_catalog
    event_rows_for_conf = _as_list(_as_dict(raw_artifacts.get("event_map")).get("entries")) or ui_event_map
    evidence_signals = sum(
        1
        for section in (sql_rows_for_conf, event_rows_for_conf, business_rules, form_dossier_rows, procedure_rows)
        if isinstance(section, list) and len(section) > 0
    )
    unmapped_ratio = 0.0 if forms_count <= 0 else min(1.0, forms_unmapped / float(max(forms_count, 1)))
    readiness_confidence = round(
        min(0.99, 0.45 + (0.08 * evidence_signals) + (0.28 * (1.0 - unmapped_ratio))),
        2,
    )
    high_risk_count = sum(
        1
        for row in risk_rows
        if isinstance(row, dict) and _clean(row.get("severity")).lower() in {"high", "critical"}
    )
    failed_review_count = sum(
        1
        for row in review_checks
        if isinstance(row, dict) and _clean(row.get("status")).lower() == "fail"
    )
    readiness_delta_note = (
        f"Score is weighted by discovered risk/blocker density (risks={len(risk_rows)}, high_severity={high_risk_count}, "
        f"failed_review_checks={failed_review_count}) and extraction coverage. Lower scores across runs usually indicate improved "
        "detection coverage, not model instability."
    )
    report = {
        "artifact_type": "analyst_report",
        "artifact_version": "2.0",
        "artifact_id": f"art_analyst_report_{uuid4().hex[:16]}",
        "run_id": run_id,
        "generated_at": generated,
        "producer": {
            "agent": "Analyst Agent",
            "skill_pack": _clean(skill.get("selected_skill_id")) or "generic_legacy",
            "skill_version": _clean(skill.get("version")) or "1.0.0",
        },
        "context": {
            "repo": repo_hint,
            "branch": _clean(context_ref.get("branch")) or "main",
            "commit_sha": _clean(context_ref.get("commit_sha")),
            "scope_lock_ref": _clean(refs.get("scope_lock")) or "artifact://analyst/raw/scope_lock/v1",
            "canonical_project": canonical_project,
            "target_stack": {
                "language": _clean(target_profile.get("language")) or "Unknown",
                "ui": _clean(target_profile.get("framework") or target_profile.get("ui_framework")) or "Unknown",
                "db": _clean(target_profile.get("database")) or "Unknown",
            },
        },
        "metadata": {
            "compiler_version": "2.7.0",
            "project": {
                "name": project_name,
                "objective": objective,
                "domain": domain,
                "audience_modes": ["client", "engineering"],
            },
            "generated_at": generated,
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
                "readiness_confidence": readiness_confidence,
                "readiness_delta_note": readiness_delta_note,
                "readiness_drivers": {
                    "risk_register_rows": len(risk_rows),
                    "high_severity_risks": high_risk_count,
                    "failed_review_checks": failed_review_count,
                    "forms_unmapped": forms_unmapped,
                },
                "inventory_summary": {
                    "projects": len(vb6_projects),
                    "forms": forms_count,
                    "forms_referenced": forms_referenced,
                    "forms_unmapped": forms_unmapped,
                    "source_loc_total": source_loc_total,
                    "source_loc_forms": source_loc_forms,
                    "source_loc_modules": source_loc_modules,
                    "source_loc_classes": source_loc_classes,
                    "source_files_scanned": source_files_scanned,
                    "controls": controls_count,
                    "dependencies": len(dep_unique),
                    "event_handlers": event_handler_count_exact,
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
            "proposed_extensions": proposed_extensions[:80],
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
        "discover_review": {
            "overall_status": _clean(discover_review_checklist.get("overall_status")) or "WARN",
            "checks": review_checks,
            "variant_diff_summary": {
                "project_count": int(variant_diff_report.get("project_count", len(vb6_projects)) or len(vb6_projects)),
                "status": _clean(variant_diff_report.get("status")) or "WARN",
                "schema_divergence_pairs": len(_as_list(_as_dict(variant_diff_report.get("schema_divergence")).get("pairs"))),
                "schema_divergence_blocking_pairs": len(_as_list(_as_dict(variant_diff_report.get("schema_divergence")).get("blocking_pairs"))),
            },
            "database_archaeology_summary": {
                "status": _clean(db_qa_report.get("overall_status")) or "WARN",
                "tables": int(_as_dict(source_db_profile.get("summary")).get("tables", 0) or int(_as_dict(source_schema_model.get("summary")).get("tables", 0) or 0)),
                "columns": int(_as_dict(source_db_profile.get("summary")).get("columns", 0) or int(_as_dict(source_schema_model.get("summary")).get("columns", 0) or 0)),
                "queries": int(_as_dict(source_db_profile.get("summary")).get("queries", 0) or 0),
                "mapping_rows": len(_as_list(schema_mapping_matrix.get("mappings"))),
            },
            "form_coverage": form_coverage_rows[:120],
        },
        "appendix": {
            "artifact_refs": {
                "legacy_inventory_ref": _clean(refs.get("legacy_inventory")) or "artifact://analyst/raw/legacy_inventory/v1",
                "repo_landscape_ref": _clean(refs.get("repo_landscape")) or "artifact://analyst/raw/repo_landscape/v1",
                "scope_lock_ref": _clean(refs.get("scope_lock")) or "artifact://analyst/raw/scope_lock/v1",
                "variant_inventory_ref": _clean(refs.get("variant_inventory")) or "artifact://analyst/raw/variant_inventory/v1",
                "event_map_ref": _clean(refs.get("event_map")) or "artifact://analyst/raw/event_map/v1",
                "sql_catalog_ref": _clean(refs.get("sql_catalog")) or "artifact://analyst/raw/sql_catalog/v1",
                "sql_map_ref": _clean(refs.get("sql_map")) or "artifact://analyst/raw/sql_map/v1",
                "data_access_map_ref": _clean(refs.get("data_access_map")) or "artifact://analyst/raw/data_access_map/v1",
                "recordset_ops_ref": _clean(refs.get("recordset_ops")) or "artifact://analyst/raw/recordset_ops/v1",
                "procedure_summary_ref": _clean(refs.get("procedure_summary")) or "artifact://analyst/raw/procedure_summary/v1",
                "form_dossier_ref": _clean(refs.get("form_dossier")) or "artifact://analyst/raw/form_dossier/v1",
                "dependency_list_ref": _clean(refs.get("dependency_inventory")) or "artifact://analyst/raw/dependency_inventory/v1",
                "dependency_inventory_ref": _clean(refs.get("dependency_inventory")) or "artifact://analyst/raw/dependency_inventory/v1",
                "business_rules_ref": _clean(refs.get("business_rule_catalog")) or "artifact://analyst/raw/business_rule_catalog/v1",
                "detector_findings_ref": _clean(refs.get("detector_findings")) or "artifact://analyst/raw/detector_findings/v1",
                "risk_register_ref": _clean(refs.get("risk_register")) or "artifact://analyst/raw/risk_register/v1",
                "orphan_analysis_ref": _clean(refs.get("orphan_analysis")) or "artifact://analyst/raw/orphan_analysis/v1",
                "delivery_constitution_ref": _clean(refs.get("delivery_constitution")) or "artifact://analyst/raw/delivery_constitution/v1",
                "source_db_profile_ref": _clean(refs.get("source_db_profile")) or "artifact://analyst/raw/source_db_profile/v1",
                "source_schema_model_ref": _clean(refs.get("source_schema_model")) or "artifact://analyst/raw/source_schema_model/v1",
                "source_query_catalog_ref": _clean(refs.get("source_query_catalog")) or "artifact://analyst/raw/source_query_catalog/v1",
                "source_relationship_candidates_ref": _clean(refs.get("source_relationship_candidates")) or "artifact://analyst/raw/source_relationship_candidates/v1",
                "source_data_dictionary_ref": _clean(refs.get("source_data_dictionary")) or "artifact://analyst/raw/source_data_dictionary/v1",
                "source_data_dictionary_markdown_ref": _clean(refs.get("source_data_dictionary_markdown")) or "artifact://analyst/raw/source_data_dictionary_markdown/v1",
                "source_erd_ref": _clean(refs.get("source_erd")) or "artifact://analyst/raw/source_erd/v1",
                "source_hotspot_report_ref": _clean(refs.get("source_hotspot_report")) or "artifact://analyst/raw/source_hotspot_report/v1",
                "target_schema_model_ref": _clean(refs.get("target_schema_model")) or "artifact://analyst/raw/target_schema_model/v1",
                "target_erd_ref": _clean(refs.get("target_erd")) or "artifact://analyst/raw/target_erd/v1",
                "target_data_dictionary_ref": _clean(refs.get("target_data_dictionary")) or "artifact://analyst/raw/target_data_dictionary/v1",
                "schema_mapping_matrix_ref": _clean(refs.get("schema_mapping_matrix")) or "artifact://analyst/raw/schema_mapping_matrix/v1",
                "migration_plan_ref": _clean(refs.get("migration_plan")) or "artifact://analyst/raw/migration_plan/v1",
                "validation_harness_spec_ref": _clean(refs.get("validation_harness_spec")) or "artifact://analyst/raw/validation_harness_spec/v1",
                "db_qa_report_ref": _clean(refs.get("db_qa_report")) or "artifact://analyst/raw/db_qa_report/v1",
                "schema_approval_record_ref": _clean(refs.get("schema_approval_record")) or "artifact://analyst/raw/schema_approval_record/v1",
                "schema_drift_report_ref": _clean(refs.get("schema_drift_report")) or "artifact://analyst/raw/schema_drift_report/v1",
                "variant_diff_report_ref": _clean(refs.get("variant_diff_report")) or "artifact://analyst/raw/variant_diff_report/v1",
                "reporting_model_ref": _clean(refs.get("reporting_model")) or "artifact://analyst/raw/reporting_model/v1",
                "identity_access_model_ref": _clean(refs.get("identity_access_model")) or "artifact://analyst/raw/identity_access_model/v1",
                "discover_review_checklist_ref": _clean(refs.get("discover_review_checklist")) or "artifact://analyst/raw/discover_review_checklist/v1",
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
                    "source_loc_total": source_loc_total,
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
    return _attach_qa_report_v1(
        report,
        output=safe,
        raw_artifacts=raw_artifacts,
        generated_at=generated,
    )
