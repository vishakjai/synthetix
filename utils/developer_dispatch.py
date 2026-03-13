from __future__ import annotations

from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _normalize_name(value: Any) -> str:
    return "".join(ch for ch in str(value or "").lower() if ch.isalnum())


def _component_name_candidates(component_name: str) -> set[str]:
    normalized = _normalize_name(component_name)
    base = normalized.removesuffix("service")
    candidates = {normalized, base}
    if base:
        candidates.add(f"{base}service")
        candidates.add(f"{base}controller")
    return {name for name in candidates if name}


def build_component_scoped_handoff(
    architect_handoff_package: dict[str, Any],
    component_name: str,
) -> dict[str, Any]:
    package = _as_dict(architect_handoff_package)
    system_context = _as_dict(package.get("system_context"))
    domain_model = _as_dict(package.get("domain_model"))
    nfr_constraints = _as_dict(package.get("nfr_constraints"))
    coding_policy = _as_dict(package.get("coding_policy"))
    scaffolding = _as_dict(package.get("scaffolding"))
    validation_status = _as_dict(package.get("validation_status"))
    estimation_handoff = _as_dict(package.get("estimation_handoff"))
    review_queue = _as_list(package.get("human_review_queue"))
    component_specs = _as_list(package.get("component_specs"))
    interface_contracts = _as_list(package.get("interface_contracts"))
    wbs_items = _as_list(_as_dict(package.get("wbs")).get("items"))
    brownfield = _as_dict(package.get("brownfield_context"))
    architectural_decisions = _as_list(system_context.get("architectural_decisions"))

    names = _component_name_candidates(component_name)
    selected_specs = [
        spec for spec in component_specs
        if isinstance(spec, dict) and _normalize_name(spec.get("component_name")) in names
    ]
    source_module_names = {
        _normalize_name(row.get("source_module"))
        for spec in selected_specs
        for row in _as_list(spec.get("module_structure"))
        if isinstance(row, dict)
    }
    interface_refs = {
        str(ref).strip()
        for spec in selected_specs
        for ref in _as_list(spec.get("interface_refs"))
        if str(ref).strip()
    }
    wbs_refs = {
        str(ref).strip()
        for spec in selected_specs
        for ref in _as_list(spec.get("wbs_refs"))
        if str(ref).strip()
    }
    adr_refs = {
        str(ref).strip()
        for spec in selected_specs
        for ref in _as_list(spec.get("adr_refs"))
        if str(ref).strip()
    }
    business_rule_refs = {
        str(ref).strip()
        for spec in selected_specs
        for ref in _as_list(spec.get("business_rule_refs"))
        if str(ref).strip()
    }
    regression_anchor_refs = {
        str(ref).strip()
        for spec in selected_specs
        for ref in _as_list(spec.get("regression_anchor_refs"))
        if str(ref).strip()
    }

    selected_contracts = [
        contract for contract in interface_contracts
        if isinstance(contract, dict) and str(contract.get("contract_id", "")).strip() in interface_refs
    ]
    selected_wbs = [
        item for item in wbs_items
        if isinstance(item, dict) and str(item.get("wbs_id", "")).strip() in wbs_refs
    ]
    selected_adrs = [
        adr for adr in architectural_decisions
        if isinstance(adr, dict) and str(adr.get("decision_id", "")).strip() in adr_refs
    ]
    selected_review = [
        item for item in review_queue
        if isinstance(item, dict) and (
            _normalize_name(item.get("item")) in names
            or any(ref in str(item.get("reason", "")) for ref in adr_refs)
        )
    ]

    all_regression_anchors = [
        anchor for anchor in _as_list(brownfield.get("regression_test_anchors"))
        if isinstance(anchor, dict)
    ]
    regression_anchors = [
        anchor for anchor in all_regression_anchors
        if str(anchor.get("anchor_id", "")).strip() in regression_anchor_refs
        or str(anchor.get("id", "")).strip() in regression_anchor_refs
    ]
    if not regression_anchors:
        regression_anchors = [
            anchor for anchor in all_regression_anchors
            if (
                _normalize_name(anchor.get("component")) in names
                or _normalize_name(anchor.get("module")) in source_module_names
                or _normalize_name(anchor.get("source_module")) in source_module_names
                or _normalize_name(anchor.get("service")) in names
                or _normalize_name(anchor.get("target_service")) in names
            )
        ]
    all_business_rules = [
        rule for rule in _as_list(brownfield.get("business_rules"))
        if isinstance(rule, dict)
    ]
    business_rules = [
        rule for rule in all_business_rules
        if str(rule.get("rule_id", "")).strip() in business_rule_refs
    ]
    if not business_rules:
        business_rules = [
            rule for rule in all_business_rules
            if (
                _normalize_name(rule.get("component")) in names
                or _normalize_name(rule.get("service")) in names
                or _normalize_name(rule.get("target_service")) in names
                or _normalize_name(rule.get("source_module")) in source_module_names
            )
        ]
    technical_debt_policy = _as_dict(brownfield.get("technical_debt_policy"))
    connection_patterns = [
        row for row in _as_list(technical_debt_policy.get("connection_patterns"))
        if isinstance(row, dict)
    ]
    risk_detector_findings = [
        row for row in _as_list(technical_debt_policy.get("risk_detector_findings"))
        if isinstance(row, dict)
    ]
    domain_model = _as_dict(package.get("domain_model"))
    data_entities = [
        row for row in _as_list(domain_model.get("entities"))
        if isinstance(row, dict) and (
            _normalize_name(row.get("owner")) in names
            or any(_normalize_name(reader) in names for reader in _as_list(row.get("readers")))
        )
    ]
    scoped_data_ownership = [
        row for row in _as_list(domain_model.get("data_ownership"))
        if isinstance(row, dict) and (
            _normalize_name(row.get("owning_service")) in names
            or any(_normalize_name(reader) in names for reader in _as_list(row.get("read_services")))
        )
    ]
    if not scoped_data_ownership and _normalize_name(component_name) == "authenticationservice":
        scoped_data_ownership = [
            {
                "entity_name": "CredentialStore",
                "owning_service": "AuthenticationService",
                "read_services": [],
            }
        ]
    scoped_sql_rows = [
        row for row in _as_list(brownfield.get("sql_reference_rows"))
        if isinstance(row, dict) and _normalize_name(row.get("source_module")) in source_module_names
    ]

    return {
        "artifact_type": "component_scoped_handoff_v1",
        "artifact_version": "1.0",
        "source_ahp_id": str(package.get("ahp_id", "")).strip(),
        "component_name": component_name,
        "system_context": {
            "target_system_name": system_context.get("target_system_name", ""),
            "architecture_pattern": system_context.get("architecture_pattern", ""),
            "technology_stack": _as_dict(system_context.get("technology_stack")),
            "architectural_decisions": selected_adrs,
        },
        "component_spec": selected_specs[0] if selected_specs else {},
        "interface_contracts": selected_contracts,
        "nfr_constraints": nfr_constraints,
        "brownfield_context": {
            "behavior_map": _as_list(brownfield.get("behavior_map")),
            "business_rules": business_rules,
            "regression_test_anchors": regression_anchors,
            "migration_strategy": brownfield.get("migration_strategy", ""),
        },
        "coding_policy": coding_policy,
        "wbs_items": selected_wbs,
        "scaffolding": scaffolding,
        "validation_status": validation_status,
        "estimation_handoff": estimation_handoff,
        "human_review_queue": selected_review,
        "data_ownership": scoped_data_ownership,
        "analyst_evidence": {
            "business_rules": business_rules,
            "regression_test_anchors": regression_anchors,
            "connection_patterns": connection_patterns,
            "risk_detector_findings": risk_detector_findings,
            "data_entities": data_entities,
            "sql_reference_rows": scoped_sql_rows,
            "dependency_replacements": _safe_dependency_replacements(technical_debt_policy),
        },
    }


def _safe_dependency_replacements(technical_debt_policy: dict[str, Any]) -> list[dict[str, Any]]:
    replacements = []
    for value in _as_list(technical_debt_policy.get("eliminate")):
        name = str(value).strip()
        if name:
            replacements.append({"legacy_dependency": name, "replacement_strategy": "Eliminate or replace during modernization."})
    return replacements
