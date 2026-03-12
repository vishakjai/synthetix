from __future__ import annotations

from datetime import datetime, timezone
import json
from pathlib import Path
import re
from typing import Any

import jsonschema


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"
SCHEMA_FILE = SCHEMA_DIR / "architect_handoff_package_v1.schema.json"


def _load_json(path: Path) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8"))


def _schema_store() -> dict[str, dict[str, Any]]:
    store: dict[str, dict[str, Any]] = {}
    for schema_file in SCHEMA_DIR.glob("*.json"):
        payload = _load_json(schema_file)
        store[schema_file.resolve().as_uri()] = payload
        schema_id = payload.get("$id")
        if schema_id:
            store[str(schema_id)] = payload
    return store


def validate_architect_handoff_json(payload: dict[str, Any]) -> None:
    schema = _load_json(SCHEMA_FILE)
    resolver = jsonschema.RefResolver(
        base_uri=SCHEMA_FILE.resolve().as_uri(),
        referrer=schema,
        store=_schema_store(),
    )
    jsonschema.validate(instance=payload, schema=schema, resolver=resolver)


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _iso_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def _slug(value: str) -> str:
    text = re.sub(r"[^A-Za-z0-9]+", "-", str(value or "").strip()).strip("-").lower()
    return text or "artifact"


def _normalize_name(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())


def _service_path_slug(service_name: str) -> str:
    return re.sub(r"service$", "", str(service_name or ""), flags=re.IGNORECASE).strip() or "service"


def _safe_list_text(values: list[Any]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def build_architect_handoff_package(
    state: dict[str, Any],
    parsed: dict[str, Any],
    architect_package: dict[str, Any],
) -> dict[str, Any]:
    analyst = state.get("analyst_output", {}) if isinstance(state.get("analyst_output", {}), dict) else {}
    raw = analyst.get("raw_artifacts", {}) if isinstance(analyst.get("raw_artifacts", {}), dict) else {}
    legacy = analyst.get("legacy_code_inventory", {}) if isinstance(analyst.get("legacy_code_inventory", {}), dict) else {}
    package_meta = _as_dict(architect_package.get("package_meta"))
    artifacts = _as_dict(architect_package.get("artifacts"))
    ownership = _as_dict(artifacts.get("data_ownership_matrix"))
    traceability = _as_dict(artifacts.get("traceability_matrix"))
    api_contracts = _as_dict(artifacts.get("api_contract_sketches"))
    migration_plan = _as_dict(artifacts.get("strangler_migration_plan"))
    risk_register = _as_dict(artifacts.get("component_risk_register"))
    adrs = _as_list(artifacts.get("architecture_decision_records"))
    review_queue = _as_list(architect_package.get("human_review_queue"))
    generated_at = _iso_now()
    project_name = str(analyst.get("project_name", "")).strip() or "Legacy Modernization"
    engagement_id = str(state.get("engagement_id", "")).strip() or str(state.get("run_id", "")).strip() or _slug(project_name)
    artifact_id = f"ahp_{_slug(engagement_id)}"
    services = parsed.get("services", []) if isinstance(parsed.get("services"), list) else []
    service_lookup = {str(svc.get("name", "")).strip(): svc for svc in services if isinstance(svc, dict) and str(svc.get("name", "")).strip()}
    component_rows = _build_component_specs(traceability, api_contracts, migration_plan, risk_register, adrs)
    payload = {
        "artifact_type": "architect_handoff_package_v1",
        "artifact_version": "1.0",
        "artifact_id": artifact_id,
        "generated_at": generated_at,
        "context": {
            "stage": "Plan",
            "source_mode": "repo" if str(state.get("use_case", "")).strip().lower() == "code_modernization" else "greenfield",
            "source_ref": str(state.get("repo_url", "")).strip() or str(state.get("project_repo_url", "")).strip(),
            "project_id": _slug(project_name),
            "engagement_id": engagement_id,
        },
        "ahp_id": artifact_id,
        "schema_version": "1.0",
        "engagement_id": engagement_id,
        "created_at": generated_at,
        "architect_agent_id": "synthetix.architect_agent",
        "system_context": {
            "target_system_name": str(parsed.get("architecture_name", "")).strip() or f"{project_name} Target Architecture",
            "source_type": "brownfield-modernization" if str(state.get("use_case", "")).strip().lower() == "code_modernization" else "greenfield",
            "architecture_pattern": str(parsed.get("pattern", "")).strip() or "modular-monolith",
            "technology_stack": {
                "primary_language": str(state.get("modernization_language", "")).strip() or "C#",
                "frameworks": sorted({str(svc.get("framework", "")).strip() for svc in services if str(svc.get("framework", "")).strip()}),
                "api_style": sorted({str(svc.get("api_type", "")).strip() for svc in services if str(svc.get("api_type", "")).strip()}),
                "database_target": str(state.get("database_target", "")).strip() or next((str(svc.get("database", "")).strip() for svc in services if str(svc.get("database", "")).strip()), ""),
                "runtime": "docker-portable",
            },
            "infrastructure": _as_dict(parsed.get("infrastructure")),
            "delivery_goals": [
                str(parsed.get("overview", "")).strip() or "Modernize the legacy application into explicit bounded components.",
                "Preserve functional parity with traceable migration sequencing.",
                "Enable downstream estimation and developer dispatch from component-scoped contracts.",
            ],
            "out_of_scope": [
                "Unreviewed low-confidence mappings pending human architect review.",
                "Direct one-to-one form/service translations without boundary rationale.",
            ],
            "architectural_decisions": [
                {
                    "decision_id": str(adr.get("id", "")).strip(),
                    "title": str(adr.get("title", "")).strip(),
                    "status": str(adr.get("status", "")).strip() or "Proposed",
                    "rationale": str(_as_dict(adr.get("context")).get("narrative", "")).strip() or str(adr.get("decision", "")).strip(),
                    "source_modules": _safe_list_text(_as_dict(adr.get("traceability")).get("source_modules", [])),
                    "target_services": _safe_list_text(_as_dict(adr.get("traceability")).get("target_services", [])),
                    "alternatives": _as_list(adr.get("alternatives_considered")),
                }
                for adr in adrs
                if isinstance(adr, dict)
            ],
        },
        "domain_model": _build_domain_model(ownership, services, legacy),
        "interface_contracts": _build_interface_contracts(api_contracts, service_lookup),
        "component_specs": component_rows,
        "nfr_constraints": _build_nfr_constraints(parsed),
        "brownfield_context": _build_brownfield_context(parsed, raw, traceability),
        "coding_policy": _build_coding_policy(state),
        "wbs": _build_wbs(migration_plan, traceability),
        "scaffolding": _build_scaffolding(parsed),
        "validation_status": {
            "status": str(package_meta.get("status", "")).strip() or "WARN",
            "warnings": _safe_list_text(package_meta.get("warnings", [])),
            "human_review_queue_count": len(review_queue),
            "blocking_review_item_count": len([item for item in review_queue if isinstance(item, dict) and bool(item.get("blocking"))]),
            "component_coverage": {
                "total_source_modules": int(_as_dict(traceability.get("coverage")).get("total_source_modules", 0) or 0),
                "mapped_confident": int(_as_dict(traceability.get("coverage")).get("mapped_confident", 0) or 0),
                "mapped_review": int(_as_dict(traceability.get("coverage")).get("mapped_review", 0) or 0),
                "mapped_unmapped": int(_as_dict(traceability.get("coverage")).get("mapped_unmapped", 0) or 0),
            },
            "contract_coverage": {
                "components": len(component_rows),
                "contracts": len(_build_interface_contracts(api_contracts, service_lookup)),
                "review_items": len(review_queue),
            },
        },
        "estimation_handoff": _as_dict(architect_package.get("estimation_handoff")),
        "human_review_queue": [
            {
                "priority": str(item.get("priority", "")).strip() or "MEDIUM",
                "artifact": str(item.get("artifact", "")).strip() or "architect_package",
                "item": str(item.get("item", "")).strip() or "review-item",
                "reason": str(item.get("reason", "")).strip() or "Architect review required.",
                "blocking": bool(item.get("blocking")),
            }
            for item in review_queue
            if isinstance(item, dict)
        ],
    }
    validate_architect_handoff_json(payload)
    return payload


def _build_domain_model(ownership: dict[str, Any], services: list[dict[str, Any]], legacy: dict[str, Any]) -> dict[str, Any]:
    entities = []
    data_ownership = []
    relationships = []
    bounded_contexts = []
    for entity in _as_list(ownership.get("entities")):
        if not isinstance(entity, dict):
            continue
        name = str(entity.get("name", "")).strip()
        owner = str(entity.get("owning_service", "")).strip()
        readers = _safe_list_text(entity.get("read_services", []))
        entities.append({
            "entity_name": name,
            "legacy_tables": _safe_list_text(entity.get("legacy_tables", [])),
            "owner": owner,
            "readers": readers,
            "migration_notes": str(entity.get("migration_notes", "")).strip(),
        })
        data_ownership.append({
            "entity_name": name,
            "owning_service": owner,
            "read_services": readers,
        })
        for reader in readers:
            relationships.append({
                "type": "shared-read",
                "entity": name,
                "from": owner,
                "to": reader,
            })
    for service in services:
        if not isinstance(service, dict):
            continue
        service_name = str(service.get("name", "")).strip()
        if not service_name:
            continue
        bounded_contexts.append({
            "context_id": _slug(service_name),
            "name": service_name,
            "responsibility": str(service.get("responsibility", "")).strip(),
            "technology": str(service.get("framework", "")).strip() or str(service.get("technology", "")).strip(),
        })
    schema_artifacts = []
    if str(legacy.get("database_schema", "")).strip():
        schema_artifacts.append({
            "type": "schema_excerpt",
            "ref": "legacy_code_inventory.database_schema",
        })
    return {
        "entities": entities,
        "relationships": relationships,
        "bounded_contexts": bounded_contexts,
        "data_ownership": data_ownership,
        "schema_artifacts": schema_artifacts,
        "migration_delta": {
            "summary": "Legacy data ownership is being re-bound to explicit target services to reduce form-to-form coupling during modernization.",
        },
    }


def _build_interface_contracts(api_contracts: dict[str, Any], service_lookup: dict[str, dict[str, Any]]) -> list[dict[str, Any]]:
    contracts = []
    for service in _as_list(api_contracts.get("services")):
        if not isinstance(service, dict):
            continue
        service_name = str(service.get("service", "")).strip()
        if not service_name:
            continue
        for idx, operation in enumerate(_as_list(service.get("operations")), start=1):
            if not isinstance(operation, dict):
                continue
            contract_id = f"contract_{_slug(service_name)}_{idx:02d}"
            contracts.append({
                "contract_id": contract_id,
                "contract_type": "internal_api",
                "owner_component": service_name,
                "consumers": _infer_consumers(service_name, service_lookup),
                "spec_format": "draft-json",
                "spec_content": {
                    "name": str(operation.get("name", "")).strip(),
                    "method": str(operation.get("method", "")).strip() or "GET",
                    "path": str(operation.get("path", "")).strip() or f"/{_service_path_slug(service_name)}",
                    "replaces": _safe_list_text(operation.get("replaces", [])),
                    "notes": str(operation.get("notes", "")).strip(),
                },
                "auth_model": "RBAC with centralized authentication" if service_name != "AuthenticationService" else "Credential validation and session bootstrap",
                "error_contract": {
                    "shape": "problem-details",
                    "retriable": False,
                },
                "versioning_strategy": "path-versioned-draft",
            })
    return contracts


def _infer_consumers(service_name: str, service_lookup: dict[str, dict[str, Any]]) -> list[str]:
    consumers = ["ExperienceShell"]
    if service_name == "AuthenticationService":
        consumers = sorted([name for name in service_lookup.keys() if name and name != "AuthenticationService"])
    return consumers


def _build_component_specs(
    traceability: dict[str, Any],
    api_contracts: dict[str, Any],
    migration_plan: dict[str, Any],
    risk_register: dict[str, Any],
    adrs: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    mappings = _as_list(traceability.get("mappings"))
    phases = {int(phase.get("phase", 0) or 0): phase for phase in _as_list(migration_plan.get("phases")) if isinstance(phase, dict)}
    modifiers = {
        str(row.get("service", "")).strip(): float(row.get("estimation_modifier", 1.0) or 1.0)
        for row in _as_list(risk_register.get("services"))
        if isinstance(row, dict) and str(row.get("service", "")).strip()
    }
    contract_refs_by_service: dict[str, list[str]] = {}
    for contract in _build_interface_contracts(api_contracts, {}):
        contract_refs_by_service.setdefault(str(contract.get("owner_component", "")), []).append(str(contract.get("contract_id", "")))
    adr_refs_by_service: dict[str, list[str]] = {}
    for adr in adrs:
        if not isinstance(adr, dict):
            continue
        adr_id = str(adr.get("id", "")).strip()
        targets = _safe_list_text(_as_dict(adr.get("traceability")).get("target_services", []))
        for service_name in targets:
            if adr_id:
                adr_refs_by_service.setdefault(service_name, []).append(adr_id)
    by_service: dict[str, list[dict[str, Any]]] = {}
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        service_name = str(_as_dict(mapping.get("target")).get("service", "")).strip()
        if service_name:
            by_service.setdefault(service_name, []).append(mapping)
    component_specs = []
    for service_name, rows in sorted(by_service.items()):
        phase_no = min(int(_as_dict(row.get("target")).get("phase", 4) or 4) for row in rows)
        phase = phases.get(phase_no, {})
        component_specs.append({
            "component_id": f"cmp_{_slug(service_name)}",
            "component_name": service_name,
            "responsibility": _component_responsibility(service_name, rows),
            "module_structure": [
                {
                    "source_module": str(_as_dict(row.get("source")).get("module", "")).strip(),
                    "suggested_component": str(_as_dict(row.get("target")).get("component", "")).strip(),
                    "migration_strategy": str(_as_dict(row.get("target")).get("migration_strategy", "")).strip(),
                }
                for row in rows
            ],
            "dependency_graph": {
                "phase": phase_no,
                "depends_on_phase": max(phase_no - 1, 0),
                "risk_modifier": modifiers.get(service_name, 1.0),
            },
            "design_patterns": _component_patterns(service_name),
            "state_management": "Stateless service boundary with explicit contract-driven state exchange.",
            "test_strategy": [
                "Contract tests for published operations",
                "Parity regression tests for mapped legacy modules",
                "Integration tests for owned data entities",
            ],
            "adr_refs": sorted(set(adr_refs_by_service.get(service_name, []))),
            "interface_refs": contract_refs_by_service.get(service_name, []),
            "wbs_refs": [f"WBS-PHASE-{phase_no:02d}"],
            "traceability_refs": [str(_as_dict(row.get("source")).get("analyst_ref", "")).strip() for row in rows if str(_as_dict(row.get("source")).get("analyst_ref", "")).strip()],
        })
    return component_specs


def _component_responsibility(service_name: str, rows: list[dict[str, Any]]) -> str:
    modules = [str(_as_dict(row.get("source")).get("module", "")).strip() for row in rows if str(_as_dict(row.get("source")).get("module", "")).strip()]
    preview = ", ".join(modules[:3])
    if len(modules) > 3:
        preview = f"{preview}, and related workflows"
    return f"{service_name} owns the behavior currently implemented by {preview}."


def _component_patterns(service_name: str) -> list[str]:
    patterns = ["Repository", "Service Layer"]
    if service_name == "ReportingService":
        patterns.append("Query Object")
    if service_name == "AuthenticationService":
        patterns.append("Policy Enforcement")
    return patterns


def _build_nfr_constraints(parsed: dict[str, Any]) -> dict[str, Any]:
    security = _as_dict(parsed.get("security"))
    scalability = _as_dict(parsed.get("scalability"))
    infrastructure = _as_dict(parsed.get("infrastructure"))
    return {
        "performance": [
            "Preserve acceptable interactive latency for modernized user workflows.",
            "Move reporting and reconciliation workloads off the interactive request path.",
            *[str(item).strip() for item in _as_list(scalability.get("auto_scaling_rules")) if str(item).strip()],
        ],
        "security": [
            str(security.get("authentication", "")).strip(),
            str(security.get("authorization", "")).strip(),
            str(security.get("api_security", "")).strip(),
            str(security.get("secrets_management", "")).strip(),
        ],
        "resilience": [
            "Support staged strangler delivery with rollback-aware phase exits.",
            str(scalability.get("strategy", "")).strip(),
            "Protect user-facing traffic from background migration workloads.",
        ],
        "observability": [
            str(infrastructure.get("monitoring", "")).strip(),
            str(infrastructure.get("logging", "")).strip(),
            "Emit structured telemetry for migration phases and cross-service calls.",
        ],
    }


def _build_brownfield_context(parsed: dict[str, Any], raw: dict[str, Any], traceability: dict[str, Any]) -> dict[str, Any]:
    legacy = _as_dict(parsed.get("legacy_system"))
    regression_anchors = []
    for idx, step in enumerate(_safe_list_text(legacy.get("key_logic_steps", [])), start=1):
        regression_anchors.append({
            "anchor_id": f"anchor_{idx:02d}",
            "type": "legacy_flow",
            "description": step,
        })
    for mapping in _as_list(traceability.get("mappings"))[:5]:
        if not isinstance(mapping, dict):
            continue
        source = _as_dict(mapping.get("source"))
        module_name = str(source.get("module", "")).strip()
        if module_name:
            regression_anchors.append({
                "anchor_id": f"anchor_{_slug(module_name)}",
                "type": "module_parity",
                "description": f"Preserve behavior currently implemented by {module_name}.",
            })
    dependencies = _safe_list_text(dep.get("name") for dep in _as_list(_as_dict(raw.get("dependency_inventory")).get("dependencies")) if isinstance(dep, dict))
    risk_ids = _safe_list_text(row.get("risk_id") for row in _as_list(_as_dict(raw.get("risk_register")).get("risks")) if isinstance(row, dict))
    return {
        "legacy_behavior_map": {
            "summary": str(legacy.get("current_logic_summary", "")).strip(),
            "key_logic_steps": _safe_list_text(legacy.get("key_logic_steps", [])),
        },
        "business_rules": [
            {
                "rule_id": str(rule.get("rule_id", "")).strip(),
                "scope": str(rule.get("form", "")).strip() or "legacy",
            }
            for rule in _as_list(_as_dict(raw.get("business_rule_catalog")).get("rules"))
            if isinstance(rule, dict) and str(rule.get("rule_id", "")).strip()
        ],
        "technical_debt_policy": {
            "preserve": [],
            "eliminate": dependencies,
            "notes": f"Analyst surfaced {len(risk_ids)} explicit risk signal(s) to address during migration.",
        },
        "data_migration_strategy": {
            "approach": "Strangler-compatible database transition with explicit ownership and parity checkpoints.",
            "target": "Operational database owned by target services.",
        },
        "regression_test_anchors": regression_anchors,
    }


def _build_coding_policy(state: dict[str, Any]) -> dict[str, Any]:
    language = str(state.get("modernization_language", "")).strip() or "C#"
    banned = ["Hardcoded secrets", "Direct legacy dependency wrappers without ADR coverage"]
    approved = ["ASP.NET Core", "xUnit", "FluentValidation", "OpenTelemetry"] if language.lower() == "c#" else []
    return {
        "language": language,
        "style_conventions": [
            "Enforce repository linting and naming standards.",
            "Use explicit dependency injection for service boundaries.",
            "Keep transport contracts separate from domain services.",
        ],
        "approved_libraries": approved,
        "banned_dependencies": banned,
        "security_rules": [
            "No hardcoded secrets or credentials.",
            "Validate all external inputs before persistence or orchestration.",
            "Use parameterized data access patterns for all database calls.",
        ],
        "coverage_requirements": {
            "unit_test_minimum": 0.8,
            "integration_contract_tests_required": True,
            "parity_regression_required": True,
        },
    }


def _build_wbs(migration_plan: dict[str, Any], traceability: dict[str, Any]) -> dict[str, Any]:
    phases = []
    items = []
    mappings = _as_list(traceability.get("mappings"))
    modules_by_phase: dict[int, list[str]] = {}
    for mapping in mappings:
        if not isinstance(mapping, dict):
            continue
        target = _as_dict(mapping.get("target"))
        source = _as_dict(mapping.get("source"))
        phase_no = int(target.get("phase", 0) or 0)
        module_name = str(source.get("module", "")).strip()
        if phase_no and module_name:
            modules_by_phase.setdefault(phase_no, []).append(module_name)
    for phase in _as_list(migration_plan.get("phases")):
        if not isinstance(phase, dict):
            continue
        phase_no = int(phase.get("phase", 0) or 0)
        phase_id = f"WBS-PHASE-{phase_no:02d}"
        phases.append({
            "phase_id": phase_id,
            "phase_number": phase_no,
            "name": str(phase.get("name", "")).strip(),
            "description": str(phase.get("description", "")).strip(),
            "exit_criteria": str(phase.get("exit_criteria", "")).strip(),
        })
        items.append({
            "wbs_id": phase_id,
            "phase_id": phase_id,
            "service": str(phase.get("target_service", "")).strip(),
            "strategy": str(phase.get("strategy", "")).strip(),
            "source_modules": sorted(set(modules_by_phase.get(phase_no, []))),
        })
    return {
        "phases": phases,
        "items": items,
    }


def _build_scaffolding(parsed: dict[str, Any]) -> dict[str, Any]:
    infrastructure = _as_dict(parsed.get("infrastructure"))
    return {
        "project_structure": [
            "src/Application",
            "src/Domain",
            "src/Infrastructure",
            "src/Contracts",
            "tests/Unit",
            "tests/Integration",
        ],
        "ci_cd": str(infrastructure.get("ci_cd", "")).strip() or "GitHub Actions",
        "environment_templates": [
            ".env.example",
            "docker-compose.yml",
        ],
        "reference_patterns": [
            "Repository pattern",
            "API contract tests",
            "Parity regression harness",
        ],
    }
