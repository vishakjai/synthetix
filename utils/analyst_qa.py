"""
Deterministic QA checks for Analyst artifacts.

Phase 1 scope:
- structural integrity assertions
- cross-section consistency checks
- serialization sanity checks
"""

from __future__ import annotations

import re
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _norm_token(value: Any) -> str:
    text = _clean(value).strip()
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip().lower()


def _norm_form_name(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if "::" in text:
        text = text.split("::", 1)[1]
    if ":" in text:
        left, right = text.split(":", 1)
        if _norm_token(left) in {"form", "mdiform", "screen"}:
            text = right
    text = text.split("/")[-1]
    text = re.sub(r"\.(frm|ctl|cls|bas)$", "", text, flags=re.IGNORECASE)
    return _norm_token(text)


def _compound_form_key(project: Any, form: Any) -> str:
    p = _norm_token(project)
    f = _norm_form_name(form)
    if p and f:
        return f"{p}::{f}"
    return f or p


def _truthy(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    s = _norm_token(value)
    return s in {"true", "yes", "1", "y", "pass", "passed"}


def _is_non_form_scope_ref(value: Any) -> bool:
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


def _collect_known_forms(raw_artifacts: dict[str, Any]) -> set[str]:
    known: set[str] = set()
    form_dossier = _as_dict(raw_artifacts.get("form_dossier"))
    for row in _as_list(form_dossier.get("dossiers")):
        if not isinstance(row, dict):
            continue
        project = row.get("project_name")
        form = row.get("form_name")
        ck = _compound_form_key(project, form)
        if ck:
            known.add(ck)
        fn = _norm_form_name(form)
        if fn:
            known.add(fn)

    legacy = _as_dict(raw_artifacts.get("legacy_inventory"))
    for row in _as_list(legacy.get("form_coverage")):
        if not isinstance(row, dict):
            continue
        name = _clean(row.get("form_name"))
        if name:
            known.add(_norm_token(name))
            known.add(_norm_form_name(name))
            if "::" in name:
                project, form = name.split("::", 1)
                ck = _compound_form_key(project, form)
                if ck:
                    known.add(ck)

    for project in _as_list(legacy.get("projects")):
        if not isinstance(project, dict):
            continue
        pname = project.get("name")
        for form in _as_list(project.get("forms")):
            ck = _compound_form_key(pname, form)
            if ck:
                known.add(ck)
            fn = _norm_form_name(form)
            if fn:
                known.add(fn)
        for asset in _as_list(project.get("ui_assets")):
            if not isinstance(asset, dict):
                continue
            form = asset.get("name")
            ck = _compound_form_key(pname, form)
            if ck:
                known.add(ck)
            fn = _norm_form_name(form)
            if fn:
                known.add(fn)
        for member in _as_list(project.get("members")):
            if not isinstance(member, dict):
                continue
            kind = _norm_token(member.get("kind"))
            path = _clean(member.get("path"))
            if kind != "form" and not path.lower().endswith(".frm"):
                continue
            form = path.split("/")[-1]
            ck = _compound_form_key(pname, form)
            if ck:
                known.add(ck)
            fn = _norm_form_name(form)
            if fn:
                known.add(fn)
    return {x for x in known if x}


def _collect_sql_forms(raw_artifacts: dict[str, Any]) -> set[str]:
    forms: set[str] = set()
    sql_map = _as_dict(raw_artifacts.get("sql_map"))
    for row in _as_list(sql_map.get("entries")):
        if not isinstance(row, dict):
            continue
        form = row.get("form")
        project = row.get("project_name")
        ck = _compound_form_key(project, form)
        if ck:
            forms.add(ck)
        fn = _norm_form_name(form)
        if fn:
            forms.add(fn)
        usage_sites = _as_list(row.get("usage_sites"))
        for site in usage_sites:
            site_text = _clean(site)
            if "::" in site_text:
                form_hint = site_text.split("::", 1)[0]
                fn2 = _norm_form_name(form_hint)
                if fn2:
                    forms.add(fn2)
    return {x for x in forms if x}


def _is_data_input_control(control_id: str) -> bool:
    cid = _norm_token(control_id)
    return cid.startswith(("txt", "cbo", "cmb", "dtp", "msk", "lst", "opt", "chk"))


def _extract_control_name(raw_control: Any) -> str:
    text = _clean(raw_control)
    if not text:
        return ""
    if ":" in text:
        text = text.split(":", 1)[1]
    return _norm_token(text)


def _detect_form_type(dossier: dict[str, Any], sql_count: int) -> str:
    form_name = _norm_form_name(dossier.get("form_name"))
    purpose = _norm_token(dossier.get("purpose"))
    controls = [_extract_control_name(x) for x in _as_list(dossier.get("controls"))]
    control_blob = " ".join(controls)
    if "mdi" in form_name or "toolbar" in control_blob or form_name == "main":
        return "mdi_host"
    if any(tok in purpose for tok in ("splash", "loading")) or "progressbar" in control_blob:
        return "splash_loading"
    if any(tok in purpose for tok in ("navigation", "menu", "routing")) and sql_count == 0:
        return "navigation_menu"
    if any(tok in purpose for tok in ("authentication", "login", "credential", "password")):
        return "auth"
    if any(tok in purpose for tok in ("report", "reporting")):
        return "reporting"
    if any(tok in purpose for tok in ("deposit", "withdraw", "transaction", "customer", "ledger", "account")):
        return "data_entry"
    return "unknown"


def _rule_semantic_signature(statement: str) -> str:
    text = _norm_token(statement)
    if not text:
        return ""
    text = re.sub(r"project1\s*\([^)]*\)", "project_variant", text)
    text = re.sub(r"\b(form|frm)\d+\b", "formN", text)
    text = re.sub(r"\b(br|risk|sql)[:-]?\d+\b", "idN", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def _rule_theme_signature(statement: str) -> str:
    text = _norm_token(statement)
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


def _form_rule_rows(rule_rows: list[dict[str, Any]], form_name_norm: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for row in rule_rows:
        if not isinstance(row, dict):
            continue
        scope = _as_dict(row.get("scope"))
        candidates: list[Any] = [
            scope.get("form"),
            scope.get("form_key"),
            scope.get("component_id"),
            row.get("form"),
        ]
        candidates.extend(_as_list(scope.get("forms")))
        candidates.extend(_as_list(scope.get("form_keys")))
        matched = False
        for cand in candidates:
            cf = _norm_form_name(cand)
            if cf and cf == form_name_norm:
                matched = True
                break
        if matched:
            out.append(row)
            continue
        # fallback: statement explicitly mentions form token
        stmt = _norm_token(row.get("statement"))
        if form_name_norm and form_name_norm in stmt:
            out.append(row)
    return out


def build_qa_report_v1(
    output: dict[str, Any],
    *,
    report: dict[str, Any] | None = None,
    generated_at: str | None = None,
) -> dict[str, Any]:
    safe_output = _as_dict(output)
    raw_artifacts = _as_dict(safe_output.get("raw_artifacts"))
    report_obj = _as_dict(report) if isinstance(report, dict) else _as_dict(safe_output.get("analyst_report_v2"))
    checks: list[dict[str, Any]] = []

    def add_check(
        *,
        check_id: str,
        title: str,
        result: str,
        detail: str,
        blocking: bool = False,
        refs: list[str] | None = None,
    ) -> None:
        checks.append(
            {
                "id": check_id,
                "title": title,
                "result": result,
                "detail": detail,
                "blocking": bool(blocking),
                "refs": refs or [],
            }
        )

    # Structural: duplicate IDs in critical catalogs.
    rule_rows = [
        row
        for row in _as_list(_as_dict(raw_artifacts.get("business_rule_catalog")).get("rules"))
        if isinstance(row, dict)
    ]
    rule_ids = [_clean(row.get("rule_id") or row.get("id")) for row in rule_rows if _clean(row.get("rule_id") or row.get("id"))]
    dup_rule_ids = sorted({rid for rid in rule_ids if rule_ids.count(rid) > 1})
    if dup_rule_ids:
        add_check(
            check_id="struct_rule_ids_unique",
            title="Business rule IDs are unique",
            result="fail",
            detail=f"Duplicate rule IDs found: {', '.join(dup_rule_ids[:10])}",
            blocking=True,
            refs=["raw_artifacts.business_rule_catalog.rules"],
        )
    else:
        add_check(
            check_id="struct_rule_ids_unique",
            title="Business rule IDs are unique",
            result="pass",
            detail=f"{len(rule_ids)} rule IDs validated.",
            refs=["raw_artifacts.business_rule_catalog.rules"],
        )

    risk_rows = [
        row
        for row in _as_list(_as_dict(raw_artifacts.get("risk_register")).get("risks"))
        if isinstance(row, dict)
    ]
    risk_ids = [_clean(row.get("risk_id") or row.get("id")) for row in risk_rows if _clean(row.get("risk_id") or row.get("id"))]
    dup_risk_ids = sorted({rid for rid in risk_ids if risk_ids.count(rid) > 1})
    if dup_risk_ids:
        add_check(
            check_id="struct_risk_ids_unique",
            title="Risk IDs are unique",
            result="fail",
            detail=f"Duplicate risk IDs found: {', '.join(dup_risk_ids[:10])}",
            blocking=True,
            refs=["raw_artifacts.risk_register.risks"],
        )
    else:
        add_check(
            check_id="struct_risk_ids_unique",
            title="Risk IDs are unique",
            result="pass",
            detail=f"{len(risk_ids)} risk IDs validated.",
            refs=["raw_artifacts.risk_register.risks"],
        )

    # Structural: referenced risks in rule rows must exist.
    known_risks = {rid.lower() for rid in risk_ids}
    referenced_risk_ids: set[str] = set()
    for row in rule_rows:
        risk_links = _as_list(row.get("risk_links"))
        for risk_id in risk_links:
            rid = _clean(risk_id)
            if rid:
                referenced_risk_ids.add(rid)
        tags = _as_list(row.get("tags"))
        for tag in tags:
            tag_text = _clean(tag)
            if re.match(r"^RISK-\d+$", tag_text, flags=re.IGNORECASE):
                referenced_risk_ids.add(tag_text)
    missing_risk_ids = sorted([rid for rid in referenced_risk_ids if rid.lower() not in known_risks])
    if missing_risk_ids:
        add_check(
            check_id="struct_risk_ref_integrity",
            title="Rule-to-risk references resolve",
            result="fail",
            detail=f"Missing risk references: {', '.join(missing_risk_ids[:12])}",
            blocking=True,
            refs=["raw_artifacts.business_rule_catalog.rules", "raw_artifacts.risk_register.risks"],
        )
    else:
        add_check(
            check_id="struct_risk_ref_integrity",
            title="Rule-to-risk references resolve",
            result="pass",
            detail="All referenced risk IDs are present.",
            refs=["raw_artifacts.business_rule_catalog.rules", "raw_artifacts.risk_register.risks"],
        )

    # Structural: form references in rule scope must exist.
    known_forms = _collect_known_forms(raw_artifacts)
    missing_rule_forms: set[str] = set()
    for row in rule_rows:
        scope = _as_dict(row.get("scope"))
        form_refs = []
        form_refs.extend(_as_list(scope.get("form_keys")))
        form_refs.extend(_as_list(scope.get("forms")))
        for key in ("form_key", "form", "component_id"):
            value = _clean(scope.get(key))
            if value:
                form_refs.append(value)
        for raw_ref in form_refs:
            ref = _clean(raw_ref)
            if not ref:
                continue
            if _is_non_form_scope_ref(ref):
                continue
            n1 = _norm_token(ref)
            n2 = _norm_form_name(ref)
            if n1 in known_forms or n2 in known_forms:
                continue
            # project::form style can still be valid against form-name-only known forms.
            if "::" in ref and _norm_form_name(ref.split("::", 1)[1]) in known_forms:
                continue
            missing_rule_forms.add(ref)
    if missing_rule_forms:
        add_check(
            check_id="struct_form_ref_integrity",
            title="Rule form references resolve",
            result="warn",
            detail=f"Unresolved form references detected: {', '.join(sorted(missing_rule_forms)[:10])}",
            refs=["raw_artifacts.business_rule_catalog.rules", "raw_artifacts.form_dossier.dossiers"],
        )
    else:
        add_check(
            check_id="struct_form_ref_integrity",
            title="Rule form references resolve",
            result="pass",
            detail="Rule scope form references resolve against discovered form inventory.",
            refs=["raw_artifacts.business_rule_catalog.rules", "raw_artifacts.form_dossier.dossiers"],
        )

    # Cross-section: form_coverage.has_sql_map consistency against sql_map.
    sql_forms = _collect_sql_forms(raw_artifacts)
    coverage_rows = _as_list(_as_dict(raw_artifacts.get("legacy_inventory")).get("form_coverage"))
    sql_expectation_gaps: list[str] = []
    for row in coverage_rows:
        if not isinstance(row, dict):
            continue
        has_sql = _truthy(row.get("has_sql_map"))
        if not has_sql:
            continue
        form_name = _clean(row.get("form_name"))
        if not form_name:
            continue
        n1 = _norm_token(form_name)
        n2 = _norm_form_name(form_name)
        if n1 in sql_forms or n2 in sql_forms:
            continue
        sql_expectation_gaps.append(form_name)
    if sql_expectation_gaps:
        add_check(
            check_id="cross_form_sql_consistency",
            title="Form SQL coverage flags reconcile with SQL map",
            result="fail",
            detail=f"Forms marked has_sql_map=true but missing SQL map entries: {', '.join(sql_expectation_gaps[:10])}",
            blocking=True,
            refs=["raw_artifacts.legacy_inventory.form_coverage", "raw_artifacts.sql_map.entries"],
        )
    else:
        add_check(
            check_id="cross_form_sql_consistency",
            title="Form SQL coverage flags reconcile with SQL map",
            result="pass",
            detail="Form SQL coverage flags match SQL map evidence.",
            refs=["raw_artifacts.legacy_inventory.form_coverage", "raw_artifacts.sql_map.entries"],
        )

    # Cross-section: event handler counts should reconcile with event map size.
    legacy_inventory = _as_dict(raw_artifacts.get("legacy_inventory"))
    summary_counts = _as_dict(_as_dict(legacy_inventory.get("summary")).get("counts"))
    expected_handlers = int(summary_counts.get("event_handlers", 0) or 0)
    event_entries = _as_list(_as_dict(raw_artifacts.get("event_map")).get("entries"))
    event_count = len(event_entries)
    if expected_handlers > 0 and event_count > 0:
        delta = abs(expected_handlers - event_count)
        tolerance = max(2, int(round(expected_handlers * 0.15)))
        if delta > tolerance:
            add_check(
                check_id="cross_event_handler_reconciliation",
                title="Event handler counts reconcile with Event Map",
                result="fail",
                detail=(
                    f"event_handlers={expected_handlers} vs event_map_entries={event_count} "
                    f"(delta={delta}, tolerance={tolerance})"
                ),
                blocking=True,
                refs=["raw_artifacts.legacy_inventory.summary.counts", "raw_artifacts.event_map.entries"],
            )
        else:
            add_check(
                check_id="cross_event_handler_reconciliation",
                title="Event handler counts reconcile with Event Map",
                result="pass",
                detail=f"event_handlers={expected_handlers}, event_map_entries={event_count}.",
                refs=["raw_artifacts.legacy_inventory.summary.counts", "raw_artifacts.event_map.entries"],
            )
    else:
        add_check(
            check_id="cross_event_handler_reconciliation",
            title="Event handler counts reconcile with Event Map",
            result="warn",
            detail="Insufficient handler count evidence to reconcile (one side missing).",
            refs=["raw_artifacts.legacy_inventory.summary.counts", "raw_artifacts.event_map.entries"],
        )

    # Report serialization sanity.
    open_questions = _as_list(_as_dict(_as_dict(report_obj.get("delivery_spec")).get("open_questions")))
    invalid_open_question = False
    for row in open_questions:
        if not isinstance(row, dict):
            invalid_open_question = True
            break
        question_text = _clean(row.get("question"))
        if not question_text or "[object object]" in question_text.lower():
            invalid_open_question = True
            break
    if invalid_open_question:
        add_check(
            check_id="render_open_questions_serialization",
            title="Open questions serialization is valid",
            result="fail",
            detail="Open questions contain non-object rows or serialized '[object Object]' text.",
            blocking=True,
            refs=["analyst_report_v2.delivery_spec.open_questions"],
        )
    else:
        add_check(
            check_id="render_open_questions_serialization",
            title="Open questions serialization is valid",
            result="pass",
            detail="Open questions are serialized as structured objects.",
            refs=["analyst_report_v2.delivery_spec.open_questions"],
        )

    # Render fidelity proxy: decision-brief form count vs discovered forms.
    reported_forms = int(
        _as_dict(_as_dict(report_obj.get("decision_brief")).get("at_a_glance"))
        .get("inventory_summary", {})
        .get("forms", 0)
        or 0
    )
    discovered_form_names = {k for k in known_forms if "::" not in k}
    discovered_forms = len(discovered_form_names) if discovered_form_names else len(known_forms)
    if reported_forms > 0 and discovered_forms > 0:
        delta = abs(reported_forms - discovered_forms)
        if delta > 2:
            add_check(
                check_id="render_form_count_fidelity",
                title="Rendered form counts match discovered inventory",
                result="warn",
                detail=f"Report forms={reported_forms}, discovered_forms={discovered_forms} (delta={delta}).",
                refs=["analyst_report_v2.decision_brief.at_a_glance.inventory_summary", "raw_artifacts.form_dossier.dossiers"],
            )
        else:
            add_check(
                check_id="render_form_count_fidelity",
                title="Rendered form counts match discovered inventory",
                result="pass",
                detail=f"Report forms={reported_forms}, discovered_forms={discovered_forms}.",
                refs=["analyst_report_v2.decision_brief.at_a_glance.inventory_summary", "raw_artifacts.form_dossier.dossiers"],
            )
    else:
        add_check(
            check_id="render_form_count_fidelity",
            title="Rendered form counts match discovered inventory",
            result="warn",
            detail="Insufficient evidence to compare rendered and discovered form counts.",
            refs=["analyst_report_v2.decision_brief.at_a_glance.inventory_summary", "raw_artifacts.form_dossier.dossiers"],
        )

    # Appendix artifact refs presence.
    artifact_refs = _as_dict(_as_dict(report_obj.get("appendix")).get("artifact_refs"))
    missing_refs = sorted([key for key, value in artifact_refs.items() if not _clean(value)])
    if missing_refs:
        add_check(
            check_id="render_artifact_refs_present",
            title="Appendix artifact references are populated",
            result="fail",
            detail=f"Missing artifact refs: {', '.join(missing_refs[:12])}",
            blocking=True,
            refs=["analyst_report_v2.appendix.artifact_refs"],
        )
    else:
        add_check(
            check_id="render_artifact_refs_present",
            title="Appendix artifact references are populated",
            result="pass",
            detail=f"{len(artifact_refs)} artifact references present.",
            refs=["analyst_report_v2.appendix.artifact_refs"],
        )

    pass_count = sum(1 for row in checks if _norm_token(row.get("result")) == "pass")
    warn_count = sum(1 for row in checks if _norm_token(row.get("result")) == "warn")
    fail_count = sum(1 for row in checks if _norm_token(row.get("result")) == "fail")
    blocker_count = sum(
        1 for row in checks if _norm_token(row.get("result")) == "fail" and bool(row.get("blocking"))
    )
    structural_status = "FAIL" if blocker_count > 0 else ("WARN" if (fail_count > 0 or warn_count > 0) else "PASS")

    semantic_checks: list[dict[str, Any]] = []

    def add_semantic(
        *,
        check_id: str,
        severity: str,
        confidence: float,
        detail: str,
        suggested_fix: str,
        refs: list[str] | None = None,
    ) -> None:
        semantic_checks.append(
            {
                "id": check_id,
                "severity": _norm_token(severity) or "medium",
                "confidence": max(0.0, min(1.0, float(confidence))),
                "detail": detail,
                "suggested_fix": suggested_fix,
                "refs": refs or [],
            }
        )

    form_dossiers = [
        row
        for row in _as_list(_as_dict(raw_artifacts.get("form_dossier")).get("dossiers"))
        if isinstance(row, dict)
    ]
    sql_map_entries = [
        row
        for row in _as_list(_as_dict(raw_artifacts.get("sql_map")).get("entries"))
        if isinstance(row, dict)
    ]
    sql_count_by_form: dict[str, int] = {}
    for row in sql_map_entries:
        form = _norm_form_name(row.get("form"))
        if form:
            sql_count_by_form[form] = sql_count_by_form.get(form, 0) + 1
    rule_rows_for_semantic = [
        row
        for row in _as_list(_as_dict(raw_artifacts.get("business_rule_catalog")).get("rules"))
        if isinstance(row, dict)
    ]

    # Semantic plausibility by form type.
    for dossier in form_dossiers:
        form_norm = _norm_form_name(dossier.get("form_name"))
        if not form_norm:
            continue
        sql_count = int(sql_count_by_form.get(form_norm, 0))
        form_type = _detect_form_type(dossier, sql_count)
        controls = [_extract_control_name(x) for x in _as_list(dossier.get("controls"))]
        data_inputs = [c for c in controls if _is_data_input_control(c)]
        mapped_rules = _form_rule_rows(rule_rows_for_semantic, form_norm)
        rule_text = " ".join(_norm_token(row.get("statement")) for row in mapped_rules)

        if form_type in {"splash_loading", "navigation_menu"} and any(
            tok in rule_text for tok in ("balance", "deposit", "withdraw", "ledger", "transaction")
        ):
            add_semantic(
                check_id=f"sem_rule_plausibility_{form_norm}",
                severity="high",
                confidence=0.86,
                detail=(
                    f"Form '{_clean(dossier.get('form_name'))}' looks like {form_type} but has financial/business rules."
                ),
                suggested_fix="Reclassify these statements as UI/technical behavior or remap to the correct transactional form.",
                refs=["raw_artifacts.form_dossier.dossiers", "raw_artifacts.business_rule_catalog.rules"],
            )

        if form_type == "splash_loading" and sql_count > 0:
            add_semantic(
                check_id=f"sem_splash_sql_{form_norm}",
                severity="high",
                confidence=0.82,
                detail=f"Splash/loading form '{_clean(dossier.get('form_name'))}' has SQL touches ({sql_count}).",
                suggested_fix="Verify form classification or move SQL behavior attribution to the actual data-entry form.",
                refs=["raw_artifacts.form_dossier.dossiers", "raw_artifacts.sql_map.entries"],
            )

        if form_type == "navigation_menu" and sql_count > 0:
            add_semantic(
                check_id=f"sem_menu_sql_{form_norm}",
                severity="medium",
                confidence=0.74,
                detail=f"Navigation/menu form '{_clean(dossier.get('form_name'))}' has SQL touches ({sql_count}).",
                suggested_fix="Confirm whether this form only routes UI actions; if yes, remove direct SQL attribution.",
                refs=["raw_artifacts.form_dossier.dossiers", "raw_artifacts.sql_map.entries"],
            )

        if form_type in {"splash_loading", "navigation_menu", "mdi_host"} and len(data_inputs) > 2:
            add_semantic(
                check_id=f"sem_form_input_plausibility_{form_norm}",
                severity="medium",
                confidence=0.71,
                detail=(
                    f"Form '{_clean(dossier.get('form_name'))}' ({form_type}) has data-input controls "
                    f"({', '.join(data_inputs[:6])})."
                ),
                suggested_fix="Validate whether these controls are true data inputs; otherwise classify them as triggers/navigation controls.",
                refs=["raw_artifacts.form_dossier.dossiers"],
            )

    # Template saturation: near-identical rules repeated widely.
    sig_forms: dict[str, set[str]] = {}
    sig_counts: dict[str, int] = {}
    for row in rule_rows_for_semantic:
        sig = _rule_semantic_signature(_clean(row.get("statement")))
        if not sig:
            continue
        sig_counts[sig] = sig_counts.get(sig, 0) + 1
        scope = _as_dict(row.get("scope"))
        form = _norm_form_name(scope.get("form") or scope.get("component_id") or row.get("form"))
        if sig not in sig_forms:
            sig_forms[sig] = set()
        if form:
            sig_forms[sig].add(form)
    noisy = sorted(
        [
            (sig, sig_counts[sig], len(sig_forms.get(sig, set())))
            for sig in sig_counts
            if sig_counts[sig] >= 5 and len(sig_forms.get(sig, set())) >= 3
        ],
        key=lambda x: (x[1], x[2]),
        reverse=True,
    )
    for idx, (sig, count, forms_span) in enumerate(noisy[:4], start=1):
        add_semantic(
            check_id=f"sem_template_saturation_{idx}",
            severity="medium",
            confidence=min(0.95, 0.6 + (count * 0.02)),
            detail=f"Rule template saturation detected: '{sig[:120]}' repeated {count} times across {forms_span} forms.",
            suggested_fix="Canonicalize into one rule with occurrence references and suppress duplicate per-form boilerplate entries.",
            refs=["raw_artifacts.business_rule_catalog.rules"],
        )

    # Thematic saturation catches near-identical statements with slight wording drift.
    theme_forms: dict[str, set[str]] = {}
    theme_counts: dict[str, int] = {}
    for row in rule_rows_for_semantic:
        statement = _clean(row.get("statement"))
        theme = _rule_theme_signature(statement)
        if not theme:
            continue
        theme_counts[theme] = theme_counts.get(theme, 0) + 1
        scope = _as_dict(row.get("scope"))
        candidates = [scope.get("form"), scope.get("form_key"), scope.get("component_id"), row.get("form")]
        candidates.extend(_as_list(scope.get("forms")))
        candidates.extend(_as_list(scope.get("form_keys")))
        for cand in candidates:
            cf = _norm_form_name(cand)
            if not cf:
                continue
            theme_forms.setdefault(theme, set()).add(cf)
    for theme, count in sorted(theme_counts.items(), key=lambda kv: kv[1], reverse=True):
        forms_span = len(theme_forms.get(theme, set()))
        if count < 5 or forms_span < 3:
            continue
        add_semantic(
            check_id=f"sem_template_theme_saturation_{theme}",
            severity="medium",
            confidence=min(0.95, 0.62 + (count * 0.02)),
            detail=f"Rule theme saturation detected: '{theme}' repeated {count} times across {forms_span} forms.",
            suggested_fix="Canonicalize as one shared rule with occurrence references, then suppress repetitive per-form boilerplate.",
            refs=["raw_artifacts.business_rule_catalog.rules"],
        )

    # Confidence signal sanity: if all dossiers have same confidence, warn.
    confidence_values = []
    for row in form_dossiers:
        coverage = _as_dict(row.get("coverage"))
        try:
            cv = float(coverage.get("confidence_score"))
        except (TypeError, ValueError):
            continue
        confidence_values.append(round(cv, 2))
    unique_conf = sorted(set(confidence_values))
    if len(confidence_values) >= 8 and len(unique_conf) <= 2:
        add_semantic(
            check_id="sem_confidence_signal_variance",
            severity="high",
            confidence=0.9,
            detail=(
                f"Form dossier confidence scores show low variance ({len(unique_conf)} unique value(s): "
                f"{', '.join(str(x) for x in unique_conf[:4])})."
            ),
            suggested_fix="Derive per-form confidence from handler coverage, SQL linkage, rule mapping, and evidence quality.",
            refs=["raw_artifacts.form_dossier.dossiers"],
        )

    # Cross-section semantic alignment: forms marked has_business_rules should map at least one rule.
    coverage_rows = _as_list(_as_dict(raw_artifacts.get("legacy_inventory")).get("form_coverage"))
    missing_rule_alignment = []
    for row in coverage_rows:
        if not isinstance(row, dict):
            continue
        if not _truthy(row.get("has_business_rules")):
            continue
        form_name = _clean(row.get("form_name"))
        form_norm = _norm_form_name(form_name)
        if not form_norm:
            continue
        mapped_rules = _form_rule_rows(rule_rows_for_semantic, form_norm)
        if not mapped_rules:
            missing_rule_alignment.append(form_name)
    if missing_rule_alignment:
        add_semantic(
            check_id="sem_rule_coverage_alignment",
            severity="high",
            confidence=0.84,
            detail=(
                f"Forms marked has_business_rules=true but no mapped rule rows found: "
                f"{', '.join(missing_rule_alignment[:8])}"
            ),
            suggested_fix="Align rule-form keys across artifacts (form key normalization) or adjust coverage flags.",
            refs=["raw_artifacts.legacy_inventory.form_coverage", "raw_artifacts.business_rule_catalog.rules"],
        )

    semantic_warn = len(semantic_checks)
    semantic_status = "WARN" if semantic_warn else "PASS"
    semantic_result = "warn" if semantic_status == "WARN" else "pass"
    semantic_desc = (
        f"Semantic plausibility checks flagged {semantic_warn} issue(s)."
        if semantic_warn
        else "Semantic plausibility checks passed with no issues."
    )

    quality_gates = [
        {
            "id": "qa_structural_integrity",
            "result": "fail" if structural_status == "FAIL" else ("warn" if structural_status == "WARN" else "pass"),
            "description": (
                f"QA structural checks: pass={pass_count}, warn={warn_count}, fail={fail_count}, blockers={blocker_count}."
            ),
            "remediation": (
                "Resolve blocking QA assertions before publishing client-facing artifacts."
                if blocker_count > 0
                else ("Review QA warnings for consistency hardening." if warn_count > 0 else "")
            ),
        }
    ]
    quality_gates.append(
        {
            "id": "qa_semantic_plausibility",
            "result": semantic_result,
            "description": semantic_desc,
            "remediation": (
                "Review semantic warnings and accept/reject corrections before client-facing export."
                if semantic_warn
                else ""
            ),
        }
    )

    return {
        "artifact_type": "qa_report",
        "artifact_version": "1.0",
        "generated_at": generated_at or _utc_now(),
        "summary": {
            "status": "FAIL" if structural_status == "FAIL" else ("WARN" if semantic_warn or structural_status == "WARN" else "PASS"),
            "pass_count": pass_count,
            "warn_count": warn_count + semantic_warn,
            "fail_count": fail_count,
            "blocker_count": blocker_count,
            "semantic_warn_count": semantic_warn,
            "auto_fixes_applied": [],
        },
        "structural": {
            "status": structural_status,
            "checks": checks,
        },
        "semantic": {
            "status": semantic_status,
            "checks": semantic_checks,
        },
        "meaning_validation": {
            "status": "NOT_RUN",
            "changes": [],
        },
        "amendment_diff_v1": [],
        "quality_gates": quality_gates,
    }
