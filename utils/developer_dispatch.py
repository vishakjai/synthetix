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

    regression_anchors = [
        anchor for anchor in _as_list(brownfield.get("regression_test_anchors"))
        if isinstance(anchor, dict) and (
            _normalize_name(anchor.get("component")) in names
            or _normalize_name(anchor.get("module")) in names
            or _normalize_name(anchor.get("service")) in names
        )
    ]
    if not regression_anchors:
        regression_anchors = [
            anchor for anchor in _as_list(brownfield.get("regression_test_anchors"))
            if isinstance(anchor, dict) and str(anchor.get("type", "")).strip() in {"legacy_flow", "module_parity"}
        ]
    business_rules = [
        rule for rule in _as_list(brownfield.get("business_rules"))
        if isinstance(rule, dict) and (
            _normalize_name(rule.get("component")) in names
            or _normalize_name(rule.get("service")) in names
            or not str(rule.get("component", "")).strip()
        )
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
        "data_ownership": _as_list(domain_model.get("data_ownership")),
    }
