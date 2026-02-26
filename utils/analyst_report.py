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
    return "Business workflow executed through event-driven UI controls."


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
        confidence = 0.35 + (0.35 * max(0.0, min(1.0, coverage_score)))
        confidence += min(0.2, 0.02 * action_count)
        confidence += 0.08 if sql_action_count > 0 else -0.04
        confidence += 0.08 if not generic_purpose else -0.1
        if action_count == 0:
            confidence -= 0.12
        if extracted_handlers == 0:
            confidence -= 0.08
        row_conf = _to_float(row.get("confidence_score"), 0.0)
        if 0 < row_conf <= 1:
            confidence = (confidence * 0.8) + (row_conf * 0.2)
        confidence = max(0.1, min(0.98, confidence))

        dossiers.append(
            {
                "dossier_id": f"form_dossier:{idx}",
                "form_name": form_name,
                "project_name": _clean(row.get("project_name")),
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
            project_id = f"{project_id}|{project_file.replace('\\', '/')}"
        projects_out.append(
            {
                "project_id": project_id,
                "name": project_name_resolved,
                "name_original": _clean(project.get("project_name_original")) or _clean(project.get("project_name")) or f"Project {pidx}",
                "type": _clean(project.get("project_type")) or "legacy_project",
                "startup": _clean(project.get("startup_object")),
                "file": _clean(project.get("project_file")),
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
        "delivery_constitution": "artifact://analyst/raw/delivery_constitution/v1",
        "variant_diff_report": "artifact://analyst/raw/variant_diff_report/v1",
        "reporting_model": "artifact://analyst/raw/reporting_model/v1",
        "identity_access_model": "artifact://analyst/raw/identity_access_model/v1",
        "discover_review_checklist": "artifact://analyst/raw/discover_review_checklist/v1",
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
            {"type": "delivery_constitution", "ref": refs["delivery_constitution"]},
            {"type": "variant_diff_report", "ref": refs["variant_diff_report"]},
            {"type": "reporting_model", "ref": refs["reporting_model"]},
            {"type": "identity_access_model", "ref": refs["identity_access_model"]},
            {"type": "discover_review_checklist", "ref": refs["discover_review_checklist"]},
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
        "delivery_constitution": delivery_constitution_artifact,
        "variant_diff_report": variant_diff_report_artifact,
        "reporting_model": reporting_model_artifact,
        "identity_access_model": identity_access_model_artifact,
        "discover_review_checklist": discover_review_checklist_artifact,
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
    refs = {key: _artifact_ref(artifacts[key], refs[key]) for key in refs}
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
        "raw_compiler_version": "2.2.0",
    }


def build_analyst_report_v2(output: dict[str, Any], *, generated_at: str | None = None) -> dict[str, Any]:
    """
    Build or pass through analyst_report_v2.

    If `output.analyst_report_v2` already looks valid, it is returned unchanged.
    """

    safe = _as_dict(output)
    prebuilt = _as_dict(safe.get("analyst_report_v2"))
    prebuilt_compiler = _clean(_as_dict(prebuilt.get("metadata")).get("compiler_version"))
    if (
        _clean(prebuilt.get("artifact_type")) == "analyst_report"
        and _clean(prebuilt.get("artifact_version")) == "2.0"
        and prebuilt_compiler == "2.2.0"
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
    if _clean(raw_artifacts.get("raw_compiler_version")) != "2.2.0":
        raw_artifacts = build_raw_artifact_set_v1(safe, generated_at=generated_at)
    raw_legacy_inventory = _as_dict(raw_artifacts.get("legacy_inventory"))
    raw_event_map_entries = _as_list(_as_dict(raw_artifacts.get("event_map")).get("entries"))
    variant_diff_report = _as_dict(raw_artifacts.get("variant_diff_report"))
    reporting_model = _as_dict(raw_artifacts.get("reporting_model"))
    identity_access_model = _as_dict(raw_artifacts.get("identity_access_model"))
    risk_register = _as_dict(raw_artifacts.get("risk_register"))
    discover_review_checklist = _as_dict(raw_artifacts.get("discover_review_checklist"))

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
            "compiler_version": "2.2.0",
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
