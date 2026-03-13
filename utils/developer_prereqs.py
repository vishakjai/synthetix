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


def _safe_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


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


def _has_meaningful_shape(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_normalize_text(value))
    if isinstance(value, dict):
        return any(_normalize_text(v) for v in value.values())
    if isinstance(value, list):
        return any(_normalize_text(v) if not isinstance(v, (dict, list)) else _has_meaningful_shape(v) for v in value)
    return False


def _has_required_value(value: Any) -> bool:
    if isinstance(value, str):
        return bool(_normalize_text(value))
    if isinstance(value, list):
        return any(_has_required_value(item) for item in value)
    if isinstance(value, dict):
        return any(_has_required_value(item) for item in value.values())
    return value is not None


def _contract_semantic_gaps(contract: dict[str, Any]) -> list[str]:
    spec_content = _as_dict(contract.get("spec_content"))
    gaps: list[str] = []
    method = _normalize_text(spec_content.get("method")).upper()
    path = _normalize_text(spec_content.get("path"))
    request_body = _as_dict(spec_content.get("request_body"))
    response_body = _as_dict(spec_content.get("response_body"))
    error_contract = _as_dict(spec_content.get("error_contract"))
    auth = _as_dict(spec_content.get("auth"))
    if not method:
        gaps.append("missing HTTP method")
    if not path:
        gaps.append("missing path")
    if not _has_meaningful_shape(response_body.get("shape")):
        gaps.append("missing response shape")
    if not _has_meaningful_shape(error_contract):
        gaps.append("missing error contract")
    if not auth or "required" not in auth:
        gaps.append("missing auth requirement")
    if _looks_mutating(contract):
        if method == "GET":
            gaps.append("uses GET for a mutating operation")
        if not _has_meaningful_shape(request_body.get("shape")):
            gaps.append("missing request body shape for mutating operation")
    return gaps


def _component_is_residual_dumping_ground(component_name: str, component_spec: dict[str, Any]) -> bool:
    name = _normalize_name(component_name or component_spec.get("component_name"))
    module_structure = [row for row in _as_list(component_spec.get("module_structure")) if isinstance(row, dict)]
    return name == "legacycoreservice" and len(module_structure) >= 5


def _approved_architectural_decisions(handoff: dict[str, Any], component_spec: dict[str, Any]) -> list[dict[str, Any]]:
    decisions = [
        row for row in _as_list(_as_dict(handoff.get("system_context")).get("architectural_decisions"))
        if isinstance(row, dict) and str(row.get("status", "")).strip().lower() in {"accepted", "approved"}
    ]
    adr_refs = {
        str(ref).strip()
        for ref in _as_list(component_spec.get("adr_refs"))
        if str(ref).strip()
    }
    if adr_refs:
        decisions = [row for row in decisions if str(row.get("decision_id", "")).strip() in adr_refs]
    return decisions


def _has_rule_semantics(rule: dict[str, Any]) -> bool:
    return (
        _has_required_value(rule.get("target_service"))
        and _has_required_value(rule.get("category"))
        and _has_required_value(rule.get("source_module"))
        and _has_required_value(rule.get("acceptance_criteria"))
    )


def _has_anchor_semantics(anchor: dict[str, Any]) -> bool:
    return (
        _has_required_value(anchor.get("golden_flow_ref"))
        and _has_required_value(anchor.get("entry_point"))
        and _has_required_value(anchor.get("expected_output"))
        and _has_required_value(anchor.get("target_endpoint"))
    )


def _sql_row_is_write(row: dict[str, Any]) -> bool:
    if bool(row.get("is_write")):
        return True
    kind = _normalize_text(row.get("kind")).lower()
    if kind in {"insert", "update", "delete", "merge", "replace", "ddl", "upsert"}:
        return True
    return bool([value for value in _as_list(row.get("data_mutations")) if _normalize_text(value)])


def _missing_refs(rows: list[dict[str, Any]], refs: list[str], primary_keys: tuple[str, ...]) -> list[str]:
    if not refs:
        return []
    row_ids = {
        _normalize_text(row.get(key))
        for row in rows
        if isinstance(row, dict)
        for key in primary_keys
        if _normalize_text(row.get(key))
    }
    return [ref for ref in refs if _normalize_text(ref) and _normalize_text(ref) not in row_ids]


def _sql_tables_covered(sql_rows: list[dict[str, Any]], data_entities: list[dict[str, Any]], data_ownership: list[dict[str, Any]]) -> bool:
    tracked_tables = {
        _normalize_name(table)
        for row in sql_rows
        if isinstance(row, dict)
        for table in _as_list(row.get("data_mutations")) + _as_list(row.get("tables"))
        if _normalize_name(table)
    }
    if not tracked_tables:
        return True
    covered = set()
    for entity in data_entities:
        if not isinstance(entity, dict):
            continue
        for table in _as_list(entity.get("legacy_tables")):
            if _normalize_name(table):
                covered.add(_normalize_name(table))
    for row in data_ownership:
        if not isinstance(row, dict):
            continue
        if _normalize_name(row.get("entity_name")):
            covered.add(_normalize_name(row.get("entity_name")))
    return tracked_tables.issubset(covered)


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
    golden_flow_like_count = max(
        len(anchors),
        len([row for row in _as_list(analyst_evidence.get("regression_test_anchors")) if isinstance(row, dict)]),
    )
    approved_decisions = _approved_architectural_decisions(handoff, component_spec)
    business_rule_refs = [str(ref).strip() for ref in _as_list(component_spec.get("business_rule_refs")) if str(ref).strip()]
    regression_anchor_refs = [str(ref).strip() for ref in _as_list(component_spec.get("regression_anchor_refs")) if str(ref).strip()]
    write_sql_rows = [row for row in sql_rows if _sql_row_is_write(row)]
    missing_rule_refs = _missing_refs(business_rules, business_rule_refs, ("rule_id", "id"))
    missing_anchor_refs = _missing_refs(anchors, regression_anchor_refs, ("anchor_id", "id"))
    referenced_business_rules = [
        row for row in business_rules
        if not business_rule_refs or _normalize_text(row.get("rule_id")) in {_normalize_text(ref) for ref in business_rule_refs}
    ]
    referenced_anchors = [
        row for row in anchors
        if not regression_anchor_refs or _normalize_text(row.get("anchor_id") or row.get("id")) in {_normalize_text(ref) for ref in regression_anchor_refs}
    ]

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
    if write_sql_rows and not data_ownership:
        hard_blockers.append(
            {
                "gap_id": "GAP-DOMAIN-002",
                "category": "domain_model",
                "description": "Mutating SQL evidence exists but component-scoped data ownership is empty. Write-path ownership would be invented during implementation.",
                "resolution": "Architect stage must reconcile SQL write paths into explicit owning_service rows before Developer dispatch.",
                "blocks": ["repository generation", "transaction boundaries", "migration scripts"],
            }
        )
    if sql_rows and not _sql_tables_covered(sql_rows, data_entities, data_ownership):
        hard_blockers.append(
            {
                "gap_id": "GAP-DOMAIN-003",
                "category": "domain_model",
                "description": "Component SQL tables are not fully covered by scoped entity and ownership metadata.",
                "resolution": "Architect stage must reconcile every scoped SQL table to data_entities/data_ownership before Developer dispatch.",
                "blocks": ["repository generation", "schema migration", "parity verification"],
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
        semantic_gaps = _contract_semantic_gaps(contract)
        if semantic_gaps:
            hard_blockers.append(
                {
                    "gap_id": f"GAP-CONTRACT-SEM-{contract_id or 'MISSING'}",
                    "category": "interface_contracts",
                    "description": (
                        f"Contract {contract_id or '(unnamed)'} is semantically incomplete: "
                        f"{'; '.join(semantic_gaps)}."
                    ),
                    "resolution": "Architect stage must emit a complete contract with correct HTTP semantics, auth, payload, and error models.",
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
    if is_brownfield and not business_rule_refs:
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-004",
                "category": "brownfield_context",
                "description": "component_spec.business_rule_refs is empty. Business rules are not explicitly routed into this component.",
                "resolution": "Architect stage must assign business_rule_refs for the component before Developer dispatch.",
                "blocks": ["service logic", "validation rules", "acceptance parity"],
            }
        )
    if is_brownfield and missing_rule_refs:
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-008",
                "category": "brownfield_context",
                "description": f"Referenced business rules are missing from the scoped handoff: {', '.join(missing_rule_refs[:5])}.",
                "resolution": "Architect stage must carry every referenced business rule into the component handoff before Developer dispatch.",
                "blocks": ["service logic", "acceptance tests", "behavioral parity"],
            }
        )
    if is_brownfield and referenced_business_rules and not all(_has_rule_semantics(rule) for rule in referenced_business_rules):
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-005",
                "category": "brownfield_context",
                "description": "Scoped business rules lack semantic fields such as target_service, category, source_module, and acceptance_criteria.",
                "resolution": "Architect stage must interpret and route business rules per component before Developer dispatch.",
                "blocks": ["service logic", "acceptance tests", "behavioral parity"],
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
    if is_brownfield and not regression_anchor_refs:
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-006",
                "category": "brownfield_context",
                "description": "component_spec.regression_anchor_refs is empty. Brownfield parity anchors are not explicitly routed into this component.",
                "resolution": "Architect stage must assign regression_anchor_refs for the component before Developer dispatch.",
                "blocks": ["parity testing", "behavior validation", "developer acceptance"],
            }
        )
    if is_brownfield and missing_anchor_refs:
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-009",
                "category": "brownfield_context",
                "description": f"Referenced regression anchors are missing from the scoped handoff: {', '.join(missing_anchor_refs[:5])}.",
                "resolution": "Architect stage must carry every referenced regression anchor into the component handoff before Developer dispatch.",
                "blocks": ["parity testing", "integration tests", "developer acceptance"],
            }
        )
    if is_brownfield and referenced_anchors and not all(_has_anchor_semantics(anchor) for anchor in referenced_anchors):
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-007",
                "category": "brownfield_context",
                "description": "Scoped regression anchors lack semantic fields such as golden_flow_ref, entry_point, expected_output, and target_endpoint.",
                "resolution": "Architect stage must enrich regression anchors from golden flows before Developer dispatch.",
                "blocks": ["parity testing", "integration tests", "developer acceptance"],
            }
        )
    if is_brownfield and business_rules and golden_flow_like_count and len(anchors) < max(1, min(2, golden_flow_like_count)):
        hard_blockers.append(
            {
                "gap_id": "GAP-BROWNFIELD-003",
                "category": "brownfield_context",
                "description": "Regression anchors are materially below the available brownfield flow evidence for this component.",
                "resolution": "Architect stage must carry forward golden flows or equivalent regression anchors before Developer dispatch.",
                "blocks": ["parity testing", "behavior validation", "developer acceptance"],
            }
        )
    if is_brownfield and not approved_decisions:
        hard_blockers.append(
            {
                "gap_id": "GAP-ARCH-APPROVAL-001",
                "category": "architectural_decisions",
                "description": "No approved architectural decisions exist for this component. Developer dispatch would proceed without approved architecture.",
                "resolution": "Architect stage must approve at least one ADR covering this component before Developer dispatch.",
                "blocks": ["component generation", "API implementation", "repository design"],
            }
        )
    if _component_is_residual_dumping_ground(component_name, component_spec):
        hard_blockers.append(
            {
                "gap_id": "GAP-COMPONENT-LEGACYCORE-001",
                "category": "component_spec",
                "description": "LegacyCoreService remains an unresolved residual dumping ground with too many mixed responsibilities.",
                "resolution": "Architect stage must decompose the residual service into smaller bounded components before Developer dispatch.",
                "blocks": ["service generation", "repository design", "regression planning"],
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

    checks = 15
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
    if not is_brownfield or business_rule_refs:
        passed += 1
    if not is_brownfield or regression_anchor_refs:
        passed += 1
    if not is_brownfield or (referenced_business_rules and all(_has_rule_semantics(rule) for rule in referenced_business_rules)):
        passed += 1
    if not is_brownfield or (referenced_anchors and all(_has_anchor_semantics(anchor) for anchor in referenced_anchors)):
        passed += 1
    if not is_brownfield or approved_decisions:
        passed += 1
    if not _component_is_residual_dumping_ground(component_name, component_spec):
        passed += 1
    if not sql_rows or (len(data_ownership) >= 1 and _sql_tables_covered(sql_rows, data_entities, data_ownership)):
        passed += 1
    if not is_brownfield or len(anchors) >= 1:
        passed += 1
    if not soft_blockers:
        passed += 1
    completeness_score = round(passed / checks, 2)
    minimum_dispatch_score = 0.75 if is_brownfield else 0.65

    if completeness_score < minimum_dispatch_score:
        hard_blockers.append(
            {
                "gap_id": "GAP-COMPLETENESS-001",
                "category": "completeness",
                "description": (
                    f"Component handoff completeness score {completeness_score:.2f} is below the minimum "
                    f"dispatch threshold of {minimum_dispatch_score:.2f}."
                ),
                "resolution": "Architect stage must enrich the handoff package before Developer dispatch.",
                "blocks": ["all downstream implementation work"],
            }
        )

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
        "minimum_dispatch_score": minimum_dispatch_score,
        "estimated_rework_risk": estimated_rework_risk,
    }


def is_component_blocked(gap_report: dict[str, Any]) -> bool:
    report = _as_dict(gap_report)
    return bool(_as_list(report.get("hard_blockers")) or _as_list(report.get("soft_blockers")))
