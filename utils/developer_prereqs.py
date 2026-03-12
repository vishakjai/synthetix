from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_text(value: Any) -> str:
    return str(value or "").strip()


def _normalize_name(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _looks_mutating(contract: dict[str, Any]) -> bool:
    if not isinstance(contract, dict):
        return False
    operation = _normalize_text(contract.get("operation")).lower()
    path = _normalize_text(contract.get("path")).lower()
    signal = f"{operation} {path}"
    mutating_tokens = (
        "create",
        "update",
        "delete",
        "close",
        "deposit",
        "withdraw",
        "save",
        "expire",
        "transfer",
        "post",
        "put",
        "patch",
    )
    return any(token in signal for token in mutating_tokens)


def _blocking_review_items(review_queue: list[Any]) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    for item in review_queue:
        if not isinstance(item, dict) or not item.get("blocking"):
            continue
        items.append(
            {
                "item": _normalize_text(item.get("item")),
                "reason": _normalize_text(item.get("reason")),
                "priority": _normalize_text(item.get("priority")),
            }
        )
    return items


def evaluate_component_prerequisites(component_handoff: dict[str, Any]) -> dict[str, Any]:
    handoff = _as_dict(component_handoff)
    component_name = _normalize_text(handoff.get("component_name"))
    component_spec = _as_dict(handoff.get("component_spec"))
    coding_policy = _as_dict(handoff.get("coding_policy"))
    validation_status = _as_dict(handoff.get("validation_status"))
    brownfield_context = _as_dict(handoff.get("brownfield_context"))
    analyst_evidence = _as_dict(handoff.get("analyst_evidence"))
    interface_contracts = [row for row in _as_list(handoff.get("interface_contracts")) if isinstance(row, dict)]
    wbs_items = [row for row in _as_list(handoff.get("wbs_items")) if isinstance(row, dict)]
    data_ownership = [row for row in _as_list(handoff.get("data_ownership")) if isinstance(row, dict)]
    data_entities = [row for row in _as_list(analyst_evidence.get("data_entities")) if isinstance(row, dict)]
    business_rules = [row for row in _as_list(brownfield_context.get("business_rules")) if isinstance(row, dict)]
    anchors = [row for row in _as_list(brownfield_context.get("regression_test_anchors")) if isinstance(row, dict)]
    sql_rows = [row for row in _as_list(analyst_evidence.get("sql_reference_rows")) if isinstance(row, dict)]
    review_queue = [row for row in _as_list(handoff.get("human_review_queue")) if isinstance(row, dict)]
    nfr_constraints = _as_dict(handoff.get("nfr_constraints"))

    hard_blockers: list[dict[str, Any]] = []
    soft_blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    if not coding_policy:
        hard_blockers.append(
            {
                "gap_id": "GAP-CODING-001",
                "category": "coding_policy",
                "description": "coding_policy is missing. Developer agent cannot choose libraries, security posture, or style safely.",
                "resolution": "Architect stage must emit coding_policy for this component.",
                "blocks": ["implementation", "dependency selection", "test generation"],
            }
        )

    if not wbs_items:
        hard_blockers.append(
            {
                "gap_id": "GAP-WBS-001",
                "category": "wbs",
                "description": "wbs_items is empty for the assigned component. Developer agent has no scoped work manifest.",
                "resolution": "Architect stage must emit WBS refs for the component before dispatch.",
                "blocks": ["planning", "implementation", "test mapping"],
            }
        )

    if not interface_contracts:
        hard_blockers.append(
            {
                "gap_id": "GAP-CONTRACTS-001",
                "category": "interface_contracts",
                "description": "No scoped interface contracts were supplied for the component. Developer would invent service boundaries and method signatures.",
                "resolution": "Architect stage must emit component-scoped interface contracts before Developer dispatch.",
                "blocks": ["controller generation", "contract tests", "integration wiring"],
            }
        )

    if sql_rows and not data_entities and not data_ownership:
        hard_blockers.append(
            {
                "gap_id": "GAP-DOMAIN-001",
                "category": "domain_model",
                "description": "SQL evidence exists but data_entities/data_ownership are empty. Repository and migration code would invent the schema.",
                "resolution": "Architect stage must derive entity definitions and ownership from the SQL catalog for this component.",
                "blocks": ["DAL generation", "schema migration", "integration tests"],
            }
        )

    for contract in interface_contracts:
        contract_id = _normalize_text(contract.get("contract_id"))
        spec_content = _as_dict(contract.get("spec_content"))
        if not spec_content:
            hard_blockers.append(
                {
                    "gap_id": f"GAP-CONTRACT-{contract_id or 'MISSING'}",
                    "category": "interface_contracts",
                    "description": f"Contract {contract_id or '(unnamed)'} has no spec_content. Method, payload, and error semantics are undefined.",
                    "resolution": "Architect stage must emit full spec_content for every contract before Developer dispatch.",
                    "blocks": ["controller generation", "contract tests", "client integration"],
                }
            )
            continue
        method = _normalize_text(spec_content.get("method")).upper()
        if method == "GET" and _looks_mutating(contract):
            hard_blockers.append(
                {
                    "gap_id": f"GAP-CONTRACT-METHOD-{contract_id or 'MISSING'}",
                    "category": "interface_contracts",
                    "description": f"Contract {contract_id or '(unnamed)'} uses GET for a mutating operation.",
                    "resolution": "Architect stage must correct HTTP semantics and regenerate the contract.",
                    "blocks": ["controller generation", "API parity", "contract tests"],
                }
            )

    is_brownfield = bool(brownfield_context or _normalize_text(validation_status.get("source_type")).lower() == "brownfield")
    if is_brownfield and not business_rules:
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-001",
                "category": "brownfield_context",
                "description": "brownfield_context.business_rules is empty. Domain logic would be fabricated during generation.",
                "resolution": "Architect stage must carry forward extracted business rules for the assigned component.",
                "blocks": ["service logic", "validation rules", "acceptance parity"],
            }
        )
    if is_brownfield and not anchors:
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-002",
                "category": "brownfield_context",
                "description": "regression_test_anchors is empty for a brownfield component. There is no parity target for generated behavior.",
                "resolution": "Architect stage must map golden flows or regression anchors into the component handoff.",
                "blocks": ["parity testing", "behavior validation", "developer acceptance"],
            }
        )

    for review in _blocking_review_items(review_queue):
        soft_blockers.append(
            {
                "gap_id": f"GAP-REVIEW-{len(soft_blockers) + 1:03d}",
                "category": "human_review_queue",
                "description": f"Blocking review item remains unresolved: {review.get('item') or 'unnamed review item'}.",
                "resolution": review.get("reason") or "Architect or orchestrator must resolve the blocking review item.",
                "escalation_path": "Orchestrator may authorize proceeding only with an explicit documented decision.",
            }
        )

    if sql_rows and not data_ownership:
        soft_blockers.append(
            {
                "gap_id": f"GAP-SHARED-DATA-{len(soft_blockers) + 1:03d}",
                "category": "data_ownership",
                "description": "SQL reference rows exist but component-scoped data ownership is empty.",
                "resolution": "Architect stage should assign owned writes vs shared reads before code generation.",
                "escalation_path": "Orchestrator may allow conservative repository generation with explicit ownership TODOs.",
            }
        )

    performance = _as_dict(nfr_constraints.get("performance"))
    observability = _as_dict(nfr_constraints.get("observability"))
    testing = _as_dict(coding_policy.get("testing"))
    if not performance:
        warnings.append(
            {
                "code": "WARN-NFR-PERF",
                "description": "Performance targets are absent. Developer will assume conservative defaults.",
            }
        )
    if not observability:
        warnings.append(
            {
                "code": "WARN-NFR-OBS",
                "description": "Observability requirements are absent. Developer will generate structured logging defaults.",
            }
        )
    if not testing:
        warnings.append(
            {
                "code": "WARN-TEST-POLICY",
                "description": "Testing policy is absent. Developer will apply default unit/integration coverage assumptions.",
            }
        )

    checks = 7
    passed = 0
    if coding_policy:
        passed += 1
    if wbs_items:
        passed += 1
    if not sql_rows or data_entities or data_ownership:
        passed += 1
    if interface_contracts and not any(gap["category"] == "interface_contracts" for gap in hard_blockers):
        passed += 1
    if not is_brownfield or business_rules:
        passed += 1
    if not is_brownfield or anchors:
        passed += 1
    if not soft_blockers:
        passed += 1
    completeness_score = round(passed / checks, 2)

    estimated_rework_risk = "LOW"
    if hard_blockers:
        estimated_rework_risk = "HIGH"
    elif soft_blockers:
        estimated_rework_risk = "MEDIUM"

    status = "READY"
    if hard_blockers or soft_blockers:
        status = "BLOCKED"

    return {
        "status": status,
        "component_id": component_name or _normalize_text(component_spec.get("component_name")),
        "ahp_id": _normalize_text(handoff.get("source_ahp_id")),
        "evaluated_at": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
        "hard_blockers": hard_blockers,
        "soft_blockers": soft_blockers,
        "warnings": warnings,
        "completeness_score": completeness_score,
        "estimated_rework_risk": estimated_rework_risk,
    }


def is_component_blocked(gap_report: dict[str, Any]) -> bool:
    report = _as_dict(gap_report)
    return bool(_as_list(report.get("hard_blockers")) or _as_list(report.get("soft_blockers")))
