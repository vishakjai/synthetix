from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_HALF_DOWN
from typing import Any

from estimations.calibration import load_team_model_library
from estimations.types import TeamModelLibrary, load_artifact_json


DEFAULT_BROWNFIELD_TASKS = [
    {"kind": "DISCOVERY", "pct": 0.2},
    {"kind": "DESIGN", "pct": 0.1},
    {"kind": "IMPLEMENT", "pct": 0.5},
    {"kind": "TEST", "pct": 0.15},
    {"kind": "DEPLOY", "pct": 0.05},
]

DECISION_TASKS = [
    {"kind": "DISCOVERY", "pct": 0.45},
    {"kind": "DESIGN", "pct": 0.35},
    {"kind": "TEST", "pct": 0.10},
    {"kind": "DEPLOY", "pct": 0.10},
]

BACKLOG_TASKS = [
    {"kind": "DISCOVERY", "pct": 0.15},
    {"kind": "DESIGN", "pct": 0.15},
    {"kind": "IMPLEMENT", "pct": 0.50},
    {"kind": "TEST", "pct": 0.15},
    {"kind": "DEPLOY", "pct": 0.05},
]

REMEDIATION_TASKS = [
    {"kind": "DISCOVERY", "pct": 0.10},
    {"kind": "DESIGN", "pct": 0.15},
    {"kind": "IMPLEMENT", "pct": 0.45},
    {"kind": "TEST", "pct": 0.20},
    {"kind": "DEPLOY", "pct": 0.10},
]

SHARED_WBS_ITEMS = [
    {
        "id": "WBS-SHARED-INFRA",
        "kind": "SHARED",
        "title": "Shared: CI/CD, environments, and baseline observability",
        "estimated_hours_likely": 120.0,
        "tasks": [
            {"kind": "DISCOVERY", "pct": 0.15},
            {"kind": "IMPLEMENT", "pct": 0.55},
            {"kind": "TEST", "pct": 0.2},
            {"kind": "DEPLOY", "pct": 0.1},
        ],
    },
    {
        "id": "WBS-SHARED-GOV",
        "kind": "SHARED",
        "title": "Shared: security, governance, and audit evidence pack",
        "estimated_hours_likely": 80.0,
        "tasks": [
            {"kind": "DISCOVERY", "pct": 0.25},
            {"kind": "DESIGN", "pct": 0.25},
            {"kind": "IMPLEMENT", "pct": 0.3},
            {"kind": "TEST", "pct": 0.2},
        ],
    },
]

ROLE_LABELS = {
    "BA": "Business Analyst",
    "LSA": "Legacy Systems Analyst",
    "ARCH": "Solution Architect",
    "DEV": "Senior Developer",
    "QA": "QA Engineer",
    "DEVOPS": "DevOps Engineer",
    "DBA": "Database Developer",
}


def _traceability_multiplier(score: int) -> float:
    if score >= 80:
        return 0.90
    if score >= 60:
        return 1.00
    if score >= 40:
        return 1.15
    return 1.30


def _risk_counts(risks: list[dict[str, Any]], chunk_id: str) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for risk in risks:
        if chunk_id not in (risk.get("applies_to_chunks") or []):
            continue
        severity = str(risk.get("severity", "")).lower()
        if severity in counts:
            counts[severity] += 1
    return counts


def _risk_multiplier(counts: dict[str, int]) -> float:
    return 1 + (0.15 * counts["high"]) + (0.08 * counts["medium"]) + (0.03 * counts["low"])


def _chunk_title(chunk_name: str) -> str:
    return f"Migrate {chunk_name} subsystem"


def _as_list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}


def _safe_slug(value: str) -> str:
    cleaned = "".join(ch.lower() if ch.isalnum() else "-" for ch in str(value or ""))
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned.strip("-") or "item"


def _fr_complexity_hours(item: dict[str, Any]) -> float:
    title = str(item.get("title") or item.get("summary") or item.get("description") or "Functional requirement")
    priority = str(item.get("priority") or item.get("business_priority") or "").upper()
    acceptance = _as_list(item.get("acceptance_criteria"))
    lower = title.lower()
    base = 44.0
    if priority in {"P0", "HIGH", "CRITICAL"}:
        base += 12.0
    elif priority in {"P1", "MEDIUM"}:
        base += 6.0
    base += min(len(acceptance), 8) * 4.0
    if any(token in lower for token in ("transaction", "deposit", "withdraw", "ledger", "statement")):
        base += 18.0
    if any(token in lower for token in ("report", "export", "pdf", "excel")):
        base += 14.0
    if any(token in lower for token in ("customer", "account", "management", "maintenance")):
        base += 10.0
    return round(base, 1)


def _decision_hours(item: dict[str, Any], *, blocking: bool) -> float:
    question = str(item.get("question") or item.get("description") or item.get("decision") or "")
    lower = question.lower()
    hours = 12.0 if blocking else 6.0
    if any(token in lower for token in ("variant", "schema", "database", "identity", "auth", "integration")):
        hours += 8.0 if blocking else 4.0
    return round(hours, 1)


def _gate_hours(item: dict[str, Any]) -> float:
    gate_id = str(item.get("id") or "gate")
    result = str(item.get("result") or item.get("status") or "WARN").upper()
    title = str(item.get("title") or item.get("description") or gate_id)
    hours = 12.0 if result == "FAIL" else 6.0
    if "compliance" in title.lower() or "identity" in title.lower():
        hours += 8.0
    return round(hours, 1)


def _legacy_form_count(context: dict[str, Any]) -> int:
    inventory = _as_dict(context.get("legacy_inventory"))
    candidates = [
        inventory.get("form_count_referenced"),
        inventory.get("active_form_count"),
        inventory.get("form_count"),
        inventory.get("forms"),
    ]
    for value in candidates:
        try:
            count = int(value or 0)
        except Exception:
            count = 0
        if count > 0:
            return count
    return 0


def _dependency_count(context: dict[str, Any]) -> int:
    inventory = _as_dict(context.get("legacy_inventory"))
    for key in ("dependency_count", "dependencies", "dependency_total"):
        try:
            count = int(inventory.get(key) or 0)
        except Exception:
            count = 0
        if count > 0:
            return count
    return 0


def _phase_for_kind(kind: str) -> str:
    kind_upper = str(kind or "").upper()
    if kind_upper in {"DISCOVERY_CLOSURE", "PROJECT_MANAGEMENT"}:
        return "Phase 0 - Decision Resolution"
    if kind_upper in {"FOUNDATION", "DATA_MODEL", "QA_AUTOMATION"}:
        return "Phase 1 - Environment, Harness & Baseline"
    if kind_upper == "MODULE_MIGRATION":
        return "Phase 2 - Core Migration"
    if kind_upper in {"RISK_REMEDIATION", "SECURITY_HARDENING"}:
        return "Phase 3 - Risk Remediations"
    if kind_upper in {"PARITY_VALIDATION", "EVIDENCE_PACK"}:
        return "Phase 4 - QA & Evidence"
    if kind_upper == "CUTOVER":
        return "Phase 5 - Cutover"
    return "Phase 2 - Core Migration"


def _brownfield_phase_capacity(role_hours: dict[str, float], phase: str) -> float:
    phase_roles = {
        "Discovery": ("BA", "ARCH", "DBA"),
        "Design": ("BA", "ARCH", "DBA", "DEV"),
        "Build": ("DEV", "DBA", "ARCH"),
        "Verify": ("QA", "DEV", "BA"),
        "Deploy": ("DEVOPS", "DEV", "QA"),
        "Cutover": ("DEVOPS", "DEV", "BA", "QA"),
    }
    active = phase_roles.get(phase, tuple(role_hours.keys()))
    return round(sum(float(role_hours.get(role) or 0.0) for role in active), 4)


def build_brownfield_wbs(
    chunk_manifest: dict[str, Any],
    risk_register: dict[str, Any],
    traceability_scores: dict[str, Any],
    analysis_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    context = _as_dict(analysis_context)
    traceability_by_chunk = dict((traceability_scores or {}).get("by_chunk") or {})
    risks = list((risk_register or {}).get("risks") or [])

    for chunk in list((chunk_manifest or {}).get("chunks") or []):
        chunk_id = str(chunk["chunk_id"])
        line_count = int(chunk.get("line_count") or 0)
        complexity_score = float(chunk.get("complexity_score") or 0.0)
        complexity_points = round((line_count / 1000.0) * complexity_score, 3)
        risk_counts = _risk_counts(risks, chunk_id)
        traceability_score = int(traceability_by_chunk.get(chunk_id, 0))
        estimated_hours_likely = round(
            complexity_points * 10.0 * _risk_multiplier(risk_counts) * _traceability_multiplier(traceability_score),
            1,
        )
        items.append(
            {
                "id": f"WBS-{chunk_id.upper()}",
                "kind": "BROWNFIELD_CHUNK",
                "title": _chunk_title(str(chunk.get("name") or chunk_id)),
                "chunk_ref": chunk_id,
                "line_count": line_count,
                "tables_owned": list(chunk.get("tables_owned") or []),
                "estimated_hours_likely": estimated_hours_likely,
                "complexity_points": complexity_points,
                "risk_counts": risk_counts,
                "traceability_score": traceability_score,
                "tasks": [dict(task) for task in DEFAULT_BROWNFIELD_TASKS],
            }
        )

    blocking_decisions = _as_list(_as_dict(context.get("decisions")).get("blocking"))
    non_blocking_decisions = _as_list(_as_dict(context.get("decisions")).get("non_blocking"))
    functional_requirements = _as_list(context.get("functional_requirements"))
    remediation_items = _as_list(context.get("remediation_items"))
    quality_gates = _as_list(context.get("quality_gates"))
    modernization_readiness = _as_dict(context.get("modernization_readiness"))
    sql_statement_count = int(context.get("sql_statement_count") or 0)
    golden_flow_count = int(context.get("golden_flow_count") or 0)
    high_risk_count = len([row for row in risks if str(_as_dict(row).get("severity", "")).lower() == "high"])
    legacy_form_count = _legacy_form_count(context)
    dependency_count = _dependency_count(context)
    failing_gates = [row for row in quality_gates if str(_as_dict(row).get("result") or _as_dict(row).get("status") or "").upper() == "FAIL"]

    for idx, row in enumerate(blocking_decisions, start=1):
        item = _as_dict(row)
        item_id = str(item.get("id") or f"DEC-{idx:03d}").strip()
        question = str(item.get("question") or item.get("description") or item_id).strip()
        items.append(
            {
                "id": f"WBS-DECISION-{_safe_slug(item_id)}",
                "kind": "DISCOVERY_CLOSURE",
                "title": f"Resolve blocking decision: {question}",
                "decision_ref": item_id,
                "estimated_hours_likely": _decision_hours(item, blocking=True),
                "risk_counts": {"high": 1, "medium": 0, "low": 0},
                "traceability_score": 30,
                "tasks": [dict(task) for task in DECISION_TASKS],
            }
        )
    for idx, row in enumerate(non_blocking_decisions, start=1):
        item = _as_dict(row)
        item_id = str(item.get("id") or f"DEC-NB-{idx:03d}").strip()
        question = str(item.get("question") or item.get("description") or item_id).strip()
        items.append(
            {
                "id": f"WBS-DECISION-NB-{_safe_slug(item_id)}",
                "kind": "PROJECT_MANAGEMENT",
                "title": f"Resolve planning decision: {question}",
                "decision_ref": item_id,
                "estimated_hours_likely": _decision_hours(item, blocking=False),
                "risk_counts": {"high": 0, "medium": 1, "low": 0},
                "traceability_score": 50,
                "tasks": [dict(task) for task in DECISION_TASKS],
            }
        )

    if blocking_decisions:
        decision_coordination_hours = 20.0 + len(blocking_decisions) * 6.0
        items.append(
            {
                "id": "WBS-DECISION-COORDINATION",
                "kind": "PROJECT_MANAGEMENT",
                "title": "Coordinate client workshops and decision closure for unresolved modernization blockers",
                "estimated_hours_likely": round(decision_coordination_hours, 1),
                "risk_counts": {"high": 1, "medium": max(len(blocking_decisions) - 1, 0), "low": 0},
                "traceability_score": 35,
                "tasks": [dict(task) for task in DECISION_TASKS],
            }
        )

    if sql_statement_count > 0:
        db_hours = 56.0 + min(sql_statement_count, 250) * 0.35
        if any("db" in str(_as_dict(row).get("id") or "").lower() or "schema" in str(_as_dict(row).get("question") or "").lower() for row in blocking_decisions):
            db_hours += 20.0
        items.append(
            {
                "id": "WBS-DATA-MODEL",
                "kind": "DATA_MODEL",
                "title": "Canonical schema reconciliation and data model alignment",
                "estimated_hours_likely": round(db_hours, 1),
                "risk_counts": {"high": 1 if high_risk_count else 0, "medium": 1, "low": 0},
                "traceability_score": 40,
                "tasks": [dict(task) for task in BACKLOG_TASKS],
            }
        )

    baseline_hours = 24.0 + min(max(golden_flow_count, 1), 12) * 3.5 + min(sql_statement_count, 120) * 0.12
    if legacy_form_count > 20:
        baseline_hours += 24.0
    if dependency_count > 0:
        baseline_hours += min(dependency_count, 15) * 4.0
    items.append(
        {
            "id": "WBS-FOUNDATION-BASELINE",
            "kind": "FOUNDATION",
            "title": "Environment setup, migration harness, and baseline engineering controls",
            "estimated_hours_likely": round(baseline_hours, 1),
            "risk_counts": {"high": 1 if dependency_count else 0, "medium": 1, "low": 0},
            "traceability_score": 55,
            "tasks": [dict(task) for task in BACKLOG_TASKS],
        }
    )

    if golden_flow_count > 0 or any("bdd" in str(_as_dict(row).get("id") or "").lower() for row in quality_gates):
        qa_hours = 28.0 + min(max(golden_flow_count, 1), 12) * 5.0
        items.append(
            {
                "id": "WBS-QA-AUTOMATION",
                "kind": "QA_AUTOMATION",
                "title": "Golden-flow and parity harness buildout",
                "estimated_hours_likely": round(qa_hours, 1),
                "risk_counts": {"high": 0, "medium": 1, "low": 0},
                "traceability_score": 55,
                "tasks": [dict(task) for task in BACKLOG_TASKS],
            }
        )

    if golden_flow_count > 0 or failing_gates:
        evidence_hours = 30.0 + min(max(golden_flow_count, 1), 12) * 4.0 + len(failing_gates) * 8.0
        items.append(
            {
                "id": "WBS-EVIDENCE-PACK",
                "kind": "EVIDENCE_PACK",
                "title": "Functional parity validation, evidence pack assembly, and UAT support",
                "estimated_hours_likely": round(evidence_hours, 1),
                "risk_counts": {"high": 1 if failing_gates else 0, "medium": 1, "low": 0},
                "traceability_score": 45,
                "tasks": [dict(task) for task in REMEDIATION_TASKS],
            }
        )

    for idx, row in enumerate(functional_requirements, start=1):
        item = _as_dict(row)
        fr_id = str(item.get("id") or f"FR-{idx:03d}").strip()
        title = str(item.get("title") or item.get("summary") or item.get("description") or fr_id).strip()
        items.append(
            {
                "id": f"WBS-BACKLOG-{_safe_slug(fr_id)}",
                "kind": "MODULE_MIGRATION",
                "title": title,
                "requirement_ref": fr_id,
                "estimated_hours_likely": _fr_complexity_hours(item),
                "risk_counts": {"high": 0, "medium": 1 if str(item.get("priority") or "").upper() in {"P0", "P1"} else 0, "low": 0},
                "traceability_score": 60,
                "tasks": [dict(task) for task in BACKLOG_TASKS],
            }
        )

    if legacy_form_count > len(functional_requirements):
        tail_count = legacy_form_count - len(functional_requirements)
        tail_hours = max(60.0, tail_count * 12.0)
        items.append(
            {
                "id": "WBS-BACKLOG-LEGACY-TAIL",
                "kind": "MODULE_MIGRATION",
                "title": f"Remaining {tail_count} legacy forms and supporting modules",
                "estimated_hours_likely": round(tail_hours, 1),
                "risk_counts": {"high": 0, "medium": 1 if tail_count > 5 else 0, "low": 1},
                "traceability_score": 50,
                "tasks": [dict(task) for task in BACKLOG_TASKS],
            }
        )

    if remediation_items:
        for idx, row in enumerate(remediation_items, start=1):
            item = _as_dict(row)
            rid = str(item.get("id") or f"RM-{idx:03d}").strip()
            title = str(item.get("title") or item.get("description") or rid).strip()
            sev = str(item.get("severity") or "medium").lower()
            base = 20.0 + (12.0 if sev == "high" else 6.0 if sev == "medium" else 0.0)
            items.append(
                {
                    "id": f"WBS-REMEDIATION-{_safe_slug(rid)}",
                    "kind": "RISK_REMEDIATION",
                    "title": title,
                    "risk_ref": rid,
                    "estimated_hours_likely": round(base, 1),
                    "risk_counts": {"high": 1 if sev == "high" else 0, "medium": 1 if sev == "medium" else 0, "low": 1 if sev == "low" else 0},
                    "traceability_score": 50,
                    "tasks": [dict(task) for task in REMEDIATION_TASKS],
                }
            )
    elif high_risk_count:
        items.append(
            {
                "id": "WBS-RISK-REMEDIATION",
                "kind": "RISK_REMEDIATION",
                "title": "Risk remediation and hardening from analyst register",
                "estimated_hours_likely": round(32.0 + (high_risk_count * 6.0), 1),
                "risk_counts": {"high": high_risk_count, "medium": 0, "low": 0},
                "traceability_score": 45,
                "tasks": [dict(task) for task in REMEDIATION_TASKS],
            }
        )

    for row in failing_gates:
        item = _as_dict(row)
        gid = str(item.get("id") or "gate").strip()
        title = str(item.get("title") or item.get("description") or gid).strip()
        kind = "SECURITY_HARDENING" if any(tok in title.lower() for tok in ("security", "identity", "compliance")) else "DISCOVERY_CLOSURE"
        items.append(
            {
                "id": f"WBS-GATE-{_safe_slug(gid)}",
                "kind": kind,
                "title": f"Close quality gate: {title}",
                "gate_ref": gid,
                "estimated_hours_likely": _gate_hours(item),
                "risk_counts": {"high": 1, "medium": 0, "low": 0},
                "traceability_score": 35,
                "tasks": [dict(task) for task in REMEDIATION_TASKS],
            }
        )

    readiness_score = int(modernization_readiness.get("score") or modernization_readiness.get("readiness_score") or 0)
    if readiness_score and readiness_score < 65:
        contingency_hours = 40.0 if readiness_score >= 50 else 72.0
        contingency_hours += len(blocking_decisions) * 6.0 + len(failing_gates) * 8.0
        contingency_hours += max(0, legacy_form_count - len(functional_requirements)) * 2.5
        items.append(
            {
                "id": "WBS-DISCOVERY-CONTINGENCY",
                "kind": "DISCOVERY_CLOSURE",
                "title": "Discovery contingency for unresolved scope and evidence gaps",
                "estimated_hours_likely": round(contingency_hours, 1),
                "risk_counts": {"high": 1 if failing_gates else 0, "medium": 1, "low": 0},
                "traceability_score": 30,
                "tasks": [dict(task) for task in DECISION_TASKS],
            }
        )

    items.extend(SHARED_WBS_ITEMS)
    items = [item for item in items if float(item.get("estimated_hours_likely") or 0.0) > 0.0]
    return {
        "generated_at": (chunk_manifest or {}).get("generated_at") or (risk_register or {}).get("generated_at"),
        "source": "chunk_manifest + risk_register + traceability_scores",
        "wbs_items": items,
    }


def build_brownfield_wbs_from_files(
    chunk_manifest_path: str,
    risk_register_path: str,
    traceability_scores_path: str,
) -> dict[str, Any]:
    return build_brownfield_wbs(
        load_artifact_json(chunk_manifest_path),
        load_artifact_json(risk_register_path),
        load_artifact_json(traceability_scores_path),
    )


def _estimate_range(likely_hours: float) -> dict[str, float]:
    return {
        "best": round(likely_hours * 0.85, 1),
        "likely": round(likely_hours, 1),
        "worst": round(likely_hours * 1.25, 1),
    }


def _round_tenth_half_down(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.1"), rounding=ROUND_HALF_DOWN))


def _scale_half_down(value: float, factor: str, epsilon: str = "0.0") -> float:
    scaled = (Decimal(str(value)) * Decimal(factor)) + Decimal(epsilon)
    return float(scaled.quantize(Decimal("0.1"), rounding=ROUND_HALF_DOWN))


def _scale_floor_tenth(value: float, factor: str) -> float:
    scaled = Decimal(str(value)) * Decimal(factor)
    return float(scaled.quantize(Decimal("0.1"), rounding=ROUND_DOWN))


def _effective_task_multiplier(tasks: list[dict[str, Any]], acceleration: dict[str, Any]) -> float:
    effective = 0.0
    for task in tasks:
        task_kind = str(task["kind"])
        pct = float(task["pct"])
        accel = float(acceleration.get(task_kind, 1.0))
        effective += pct / accel
    return effective


def _weekly_team_capacity(team: dict[str, Any], weekly_capacity_hours: float) -> float:
    return round(sum(float(fte) for fte in team.values()) * float(weekly_capacity_hours), 4)


def _buffer_weeks(wbs: dict[str, Any]) -> float:
    high_risk_count = 0
    for item in list(wbs.get("wbs_items") or []):
        risk_counts = item.get("risk_counts") or {}
        high_risk_count += int(risk_counts.get("high") or 0)
    return 1.0 if high_risk_count >= 2 else 0.5


def _architect_required(wbs: dict[str, Any]) -> bool:
    chunk_count = 0
    high_risk_count = 0
    low_traceability_count = 0
    for item in list(wbs.get("wbs_items") or []):
        if str(item.get("kind", "")).upper() == "BROWNFIELD_CHUNK":
            chunk_count += 1
        risk_counts = item.get("risk_counts") or {}
        high_risk_count += int(risk_counts.get("high") or 0)
        traceability_score = int(item.get("traceability_score") or 100)
        if traceability_score < 40:
            low_traceability_count += 1
    return chunk_count >= 3 or high_risk_count >= 2 or low_traceability_count >= 2


def _lsa_required(wbs: dict[str, Any]) -> bool:
    decision_items = 0
    low_traceability_count = 0
    for item in list(wbs.get("wbs_items") or []):
        kind = str(item.get("kind", "")).upper()
        if kind in {"DISCOVERY_CLOSURE", "PROJECT_MANAGEMENT"}:
            decision_items += 1
        if int(item.get("traceability_score") or 100) < 45:
            low_traceability_count += 1
    return decision_items >= 2 or low_traceability_count >= 3


def _dba_required(wbs: dict[str, Any]) -> bool:
    for item in list(wbs.get("wbs_items") or []):
        if str(item.get("kind", "")).upper() in {"DATA_MODEL", "DATA_MIGRATION"}:
            return True
        if list(item.get("tables_owned") or []):
            return True
    return False


def _normalized_team_profile(
    default_team: dict[str, Any],
    role_split: dict[str, Any],
    *,
    lsa_required: bool,
    architect_required: bool,
    dba_required: bool,
) -> tuple[dict[str, Any], dict[str, Any], list[str]]:
    team = {str(role): float(fte) for role, fte in default_team.items()}
    split = {str(role): float(share) for role, share in role_split.items()}
    notes: list[str] = []
    if lsa_required:
        notes.append("Legacy systems analyst activated by unresolved decisions and low-traceability work.")
        team["LSA"] = max(float(team.get("LSA", 0.0) or 0.0), 1.0)
        split["LSA"] = max(float(split.get("LSA", 0.0) or 0.0), 0.12)
        split["DEV"] = max(float(split.get("DEV", 0.0) or 0.0) - 0.06, 0.28)
        split["BA"] = max(float(split.get("BA", 0.0) or 0.0), 0.12)
    if architect_required:
        notes.append("Architect role activated by chunk/risk/traceability thresholds.")
        team["ARCH"] = max(float(team.get("ARCH", 0.0) or 0.0), 0.5)
        split["ARCH"] = max(float(split.get("ARCH", 0.0) or 0.0), 0.10)
        split["DEV"] = max(float(split.get("DEV", 0.0) or 0.0) - 0.05, 0.35)
        split["QA"] = max(float(split.get("QA", 0.0) or 0.0), 0.20)
    if dba_required:
        notes.append("Database specialist activated by data-model and SQL complexity signals.")
        team["DBA"] = max(float(team.get("DBA", 0.0) or 0.0), 0.5)
        split["DBA"] = max(float(split.get("DBA", 0.0) or 0.0), 0.10)
        split["DEV"] = max(float(split.get("DEV", 0.0) or 0.0) - 0.05, 0.30)
    total_share = sum(float(v) for v in split.values()) or 1.0
    split = {role: round(float(share) / total_share, 6) for role, share in split.items()}
    return team, split, notes


def apply_team_model_to_wbs(wbs: dict[str, Any], library: TeamModelLibrary, model_key: str) -> dict[str, Any]:
    models = library.models
    if model_key not in models:
        raise KeyError(f"Unknown team model: {model_key}")

    model = models[model_key]
    acceleration = dict(model.get("acceleration") or {})
    role_split = dict(model.get("role_split") or {})
    default_team = dict(model.get("default_team") or {})
    lsa_required = _lsa_required(wbs)
    architect_required = _architect_required(wbs)
    dba_required = _dba_required(wbs)
    default_team, role_split, model_notes = _normalized_team_profile(
        default_team,
        role_split,
        lsa_required=lsa_required,
        architect_required=architect_required,
        dba_required=dba_required,
    )
    weekly_capacity_hours = float(library.weekly_capacity_hours)

    item_summaries: list[dict[str, Any]] = []
    total_likely_raw = 0.0

    for item in list(wbs.get("wbs_items") or []):
        likely_raw = float(item["estimated_hours_likely"]) * _effective_task_multiplier(list(item.get("tasks") or []), acceleration)
        likely = round(likely_raw, 1)
        total_likely_raw += likely_raw
        item_summaries.append(
            {
                "id": item["id"],
                "title": item["title"],
                "kind": item.get("kind"),
                "hours_likely": likely,
            }
        )

    total_likely = round(total_likely_raw, 1)
    total_best = _scale_floor_tenth(total_likely, "0.85")
    total_worst = _scale_half_down(total_likely, "1.25")

    team_capacity = _weekly_team_capacity(default_team, weekly_capacity_hours)
    phase_hours: dict[str, float] = {}
    for item in list(wbs.get("wbs_items") or []):
        phase = "Build"
        kind = str(item.get("kind", "")).upper()
        if kind in {"DISCOVERY_CLOSURE", "PROJECT_MANAGEMENT"}:
            phase = "Discovery"
        elif kind in {"DATA_MODEL", "FOUNDATION", "QA_AUTOMATION"}:
            phase = "Design"
        elif kind in {"SECURITY_HARDENING", "RISK_REMEDIATION", "PARITY_VALIDATION", "EVIDENCE_PACK"}:
            phase = "Verify"
        elif kind == "CUTOVER":
            phase = "Cutover"
        phase_hours[phase] = phase_hours.get(phase, 0.0) + float(item.get("estimated_hours_likely") or 0.0)
    phase_sequence = ["Discovery", "Design", "Build", "Verify", "Cutover"]
    phase_breakdown: list[dict[str, Any]] = []
    timeline_likely = 0.0
    timeline_best = 0.0
    timeline_worst = 0.0
    for phase in phase_sequence:
        hours = phase_hours.get(phase, 0.0)
        if hours <= 0:
            continue
        capacity = max(_brownfield_phase_capacity(default_team, phase) * weekly_capacity_hours, 1.0)
        likely_weeks = round(hours / capacity, 1)
        best_weeks = _scale_floor_tenth(likely_weeks, "0.85")
        worst_weeks = _scale_half_down(likely_weeks, "1.25")
        phase_breakdown.append(
            {
                "phase": phase,
                "hours_likely": round(hours, 1),
                "weeks_best": best_weeks,
                "weeks_likely": likely_weeks,
                "weeks_worst": worst_weeks,
            }
        )
        timeline_likely += likely_weeks
        timeline_best += best_weeks
        timeline_worst += worst_weeks
    buffer = _buffer_weeks(wbs) + (1.0 if lsa_required else 0.0)
    timeline_likely = round(timeline_likely + buffer, 1)
    timeline_best = _scale_floor_tenth(timeline_likely, "0.85")
    timeline_worst = _scale_half_down(timeline_likely, "1.25")
    role_totals = {
        role: round(total_likely * float(share), 1)
        for role, share in role_split.items()
    }

    return {
        "model_key": model_key,
        "model_name": model.get("name", model_key),
        "team": default_team,
        "lsa_required": lsa_required,
        "architect_required": architect_required,
        "dba_required": dba_required,
        "notes": model_notes,
        "weekly_capacity_hours": int(weekly_capacity_hours) if weekly_capacity_hours.is_integer() else weekly_capacity_hours,
        "total_hours_likely": total_likely,
        "total_hours_best": total_best,
        "total_hours_worst": total_worst,
        "timeline_weeks_likely": timeline_likely,
        "timeline_weeks_best": timeline_best,
        "timeline_weeks_worst": timeline_worst,
        "phase_breakdown": phase_breakdown,
        "hours_by_role": {role: round(hours, 1) for role, hours in role_totals.items()},
        "items": item_summaries,
    }


def apply_team_model_to_wbs_from_files(wbs_path: str, team_models_path: str, model_key: str) -> dict[str, Any]:
    return apply_team_model_to_wbs(
        load_artifact_json(wbs_path),
        load_team_model_library(team_models_path),
        model_key,
    )
