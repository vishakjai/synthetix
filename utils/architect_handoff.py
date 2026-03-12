from __future__ import annotations

from datetime import datetime, timezone
from difflib import SequenceMatcher
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
    _semantic_validate_architect_handoff(payload)


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


def _camel_split(value: str) -> str:
    return re.sub(r"(?<!^)(?=[A-Z])", " ", str(value or "")).strip()


def _service_path_slug(service_name: str) -> str:
    return re.sub(r"service$", "", str(service_name or ""), flags=re.IGNORECASE).strip() or "service"


def _safe_list_text(values: list[Any]) -> list[str]:
    return [str(value).strip() for value in values if str(value).strip()]


def _raw_bucket(raw: dict[str, Any], *keys: str) -> dict[str, Any]:
    for key in keys:
        value = raw.get(key)
        if isinstance(value, dict):
            return value
    return {}


def _raw_rows(bucket: dict[str, Any], *keys: str) -> list[dict[str, Any]]:
    for key in keys:
        rows = bucket.get(key)
        if isinstance(rows, list):
            return [row for row in rows if isinstance(row, dict)]
    return []


def _risk_detector_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(
        _raw_bucket(raw, "static_risk_detectors", "static_risk_detector_findings", "risk_detector_findings"),
        "findings",
        "rows",
        "detectors",
    )


def _golden_flow_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(
        _raw_bucket(raw, "golden_flows", "golden_flow_inventory", "golden_flow_catalog"),
        "flows",
        "rows",
        "anchors",
    )


def _dead_ref_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(
        _raw_bucket(raw, "dead_form_references", "dead_references", "orphaned_references"),
        "references",
        "rows",
        "items",
    )


def _connection_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(
        _raw_bucket(raw, "connection_string_variants", "connection_variants"),
        "variants",
        "rows",
        "items",
    )


def _global_state_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(
        _raw_bucket(raw, "global_module_inventory", "module_global_inventory", "global_state_inventory"),
        "variables",
        "rows",
        "globals",
        "items",
    )


def _business_rule_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(_raw_bucket(raw, "business_rule_catalog", "brd_business_rules"), "rules", "rows", "items")


def _sql_rows(raw: dict[str, Any]) -> list[dict[str, Any]]:
    return _raw_rows(_raw_bucket(raw, "sql_catalog", "php_sql_catalog", "php_sql_catalog_v1"), "statements", "rows", "items")


def _entity_name_for_table(table_name: str) -> str:
    name = str(table_name or "").strip()
    if not name:
        return ""
    name = re.sub(r"^tbl", "", name, flags=re.IGNORECASE)
    return _camel_split("".join(part.capitalize() for part in re.split(r"[^A-Za-z0-9]+", name) if part))


def _is_mutating_operation(operation_name: str, path: str, replaces: list[str]) -> bool:
    text = " ".join([operation_name or "", path or "", " ".join(replaces)]).lower()
    return any(
        token in text
        for token in (
            "create",
            "add",
            "save",
            "update",
            "close",
            "delete",
            "remove",
            "deposit",
            "withdraw",
            "expire",
            "post",
        )
    )


def _infer_http_method(operation_name: str, path: str, replaces: list[str], existing: str) -> str:
    current = str(existing or "").strip().upper()
    if current and current != "GET":
        return current
    if not _is_mutating_operation(operation_name, path, replaces):
        return current or "GET"
    lowered = " ".join([operation_name or "", path or "", " ".join(replaces)]).lower()
    if any(token in lowered for token in ("delete", "remove")):
        return "DELETE"
    if any(token in lowered for token in ("update", "close")):
        return "PUT"
    return "POST"


def _group_rules_by_scope(rules: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for rule in rules:
        scope = str(rule.get("form", "")).strip() or str(rule.get("module", "")).strip() or str(rule.get("service", "")).strip()
        grouped.setdefault(_normalize_name(scope), []).append(rule)
    return grouped


def _similar_name_pairs(values: list[str]) -> list[tuple[str, str]]:
    pairs: list[tuple[str, str]] = []
    for idx, left in enumerate(values):
        for right in values[idx + 1 :]:
            if left == right:
                continue
            if SequenceMatcher(None, _normalize_name(left), _normalize_name(right)).ratio() >= 0.92:
                pairs.append((left, right))
    return pairs


def _semantic_validate_architect_handoff(payload: dict[str, Any]) -> None:
    domain_model = _as_dict(payload.get("domain_model"))
    brownfield = _as_dict(payload.get("brownfield_context"))
    summary = _as_dict(brownfield.get("source_evidence_summary"))
    contracts = _as_list(payload.get("interface_contracts"))
    components = _as_list(payload.get("component_specs"))

    sql_count = int(summary.get("sql_statement_count", 0) or 0)
    business_rule_count = int(summary.get("business_rule_count", 0) or 0)
    golden_flow_count = int(summary.get("golden_flow_count", 0) or 0)
    entities = _as_list(domain_model.get("entities"))
    ownership = _as_list(domain_model.get("data_ownership"))
    rules = _as_list(brownfield.get("business_rules"))
    anchors = _as_list(brownfield.get("regression_test_anchors"))

    if sql_count > 0 and not entities:
        raise jsonschema.ValidationError("architect handoff invalid: SQL evidence exists but domain_model.entities is empty")
    if sql_count > 0 and not ownership:
        raise jsonschema.ValidationError("architect handoff invalid: SQL evidence exists but data ownership is empty")
    if business_rule_count > 0 and not rules:
        raise jsonschema.ValidationError("architect handoff invalid: upstream business rules exist but brownfield_context.business_rules is empty")
    if golden_flow_count > 0 and not anchors:
        raise jsonschema.ValidationError("architect handoff invalid: upstream golden flows exist but regression_test_anchors is empty")

    for contract in contracts:
        if not isinstance(contract, dict):
            continue
        spec = _as_dict(contract.get("spec_content"))
        method = str(spec.get("method", "")).strip().upper()
        if _is_mutating_operation(str(spec.get("name", "")), str(spec.get("path", "")), _safe_list_text(spec.get("replaces", []))) and method == "GET":
            raise jsonschema.ValidationError(
                f"architect handoff invalid: mutating operation emitted as GET for contract {contract.get('contract_id', '')}"
            )

    if any(
        _normalize_name(str(component.get("component_name", "")).strip()) == "legacycoreservice"
        and len(_as_list(component.get("module_structure"))) >= 10
        for component in components
        if isinstance(component, dict)
    ):
        raise jsonschema.ValidationError("architect handoff invalid: LegacyCoreService remains an unresolved dumping ground")


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
    source_evidence_summary = {
        "business_rule_count": len(_business_rule_rows(raw)),
        "sql_statement_count": len(_sql_rows(raw)),
        "golden_flow_count": len(_golden_flow_rows(raw)),
        "risk_detector_count": len(_risk_detector_rows(raw)),
        "dead_reference_count": len(_dead_ref_rows(raw)),
        "global_state_count": len(_global_state_rows(raw)),
        "connection_variant_count": len(_connection_rows(raw)),
    }
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
        "domain_model": _build_domain_model(ownership, services, legacy, raw, traceability),
        "interface_contracts": _build_interface_contracts(api_contracts, service_lookup, raw, component_rows),
        "component_specs": component_rows,
        "nfr_constraints": _build_nfr_constraints(parsed, raw),
        "brownfield_context": _build_brownfield_context(parsed, raw, traceability, source_evidence_summary),
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
                "contracts": len(_build_interface_contracts(api_contracts, service_lookup, raw, component_rows)),
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


def _build_domain_model(
    ownership: dict[str, Any],
    services: list[dict[str, Any]],
    legacy: dict[str, Any],
    raw: dict[str, Any],
    traceability: dict[str, Any],
) -> dict[str, Any]:
    entities = []
    data_ownership = []
    relationships = []
    bounded_contexts = []
    sql_rows = _sql_rows(raw)
    form_dossiers = _raw_rows(_raw_bucket(raw, "form_dossier"), "dossiers", "forms", "rows")
    dossier_tables = {
        _normalize_name(str(row.get("form_name", "")).strip() or str(row.get("base_form_name", "")).strip()): _safe_list_text(row.get("db_tables", []))
        for row in form_dossiers
        if isinstance(row, dict)
    }
    service_by_source = {
        _normalize_name(str(_as_dict(mapping.get("source")).get("module", "")).strip()): str(_as_dict(mapping.get("target")).get("service", "")).strip()
        for mapping in _as_list(traceability.get("mappings"))
        if isinstance(mapping, dict) and str(_as_dict(mapping.get("target")).get("service", "")).strip()
    }
    entity_seen: set[str] = set()
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
        if name:
            entity_seen.add(_normalize_name(name))
    if not entities:
        table_to_services: dict[str, list[str]] = {}
        for row in sql_rows:
            source_name = _normalize_name(str(row.get("form", "")).strip() or str(row.get("module", "")).strip())
            service_name = service_by_source.get(source_name)
            tables = _safe_list_text(row.get("tables", [])) or dossier_tables.get(source_name, [])
            for table in tables:
                if service_name:
                    table_to_services.setdefault(table, []).append(service_name)
        for table_name, services_for_table in sorted(table_to_services.items()):
            owner = ""
            if services_for_table:
                owner = max(set(services_for_table), key=services_for_table.count)
            readers = sorted({service for service in services_for_table if service and service != owner})
            entity_name = _entity_name_for_table(table_name) or table_name
            entity_key = _normalize_name(entity_name)
            if entity_key in entity_seen:
                continue
            entity_seen.add(entity_key)
            entities.append({
                "entity_name": entity_name,
                "legacy_tables": [table_name],
                "owner": owner,
                "readers": readers,
                "migration_notes": f"Derived from SQL catalog references to {table_name}.",
            })
            if owner:
                data_ownership.append({
                    "entity_name": entity_name,
                    "owning_service": owner,
                    "read_services": readers,
                })
            for reader in readers:
                relationships.append({
                    "type": "shared-read",
                    "entity": entity_name,
                    "from": owner,
                    "to": reader,
                })
    for variable in _global_state_rows(raw):
        name = str(variable.get("name", "")).strip() or str(variable.get("variable", "")).strip()
        owning_service = str(variable.get("owning_service", "")).strip()
        used_in = _safe_list_text(variable.get("used_in_modules", []))
        if not name:
            continue
        relationships.append({
            "type": "shared-state",
            "entity": name,
            "from": owning_service or "legacy-global-state",
            "to": ", ".join(used_in) if used_in else "multiple-modules",
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


def _build_interface_contracts(
    api_contracts: dict[str, Any],
    service_lookup: dict[str, dict[str, Any]],
    raw: dict[str, Any],
    component_rows: list[dict[str, Any]],
) -> list[dict[str, Any]]:
    contracts = []
    component_by_service = {
        str(component.get("component_name", "")).strip(): component
        for component in component_rows
        if isinstance(component, dict) and str(component.get("component_name", "")).strip()
    }
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
            replaces = _safe_list_text(operation.get("replaces", []))
            op_name = str(operation.get("name", "")).strip()
            path = str(operation.get("path", "")).strip() or f"/{_service_path_slug(service_name)}"
            method = _infer_http_method(op_name, path, replaces, str(operation.get("method", "")).strip())
            mutating = method in {"POST", "PUT", "PATCH", "DELETE"}
            component = component_by_service.get(service_name, {})
            contracts.append({
                "contract_id": contract_id,
                "contract_type": "internal_api",
                "owner_component": service_name,
                "consumers": _infer_consumers(service_name, service_lookup),
                "spec_format": "draft-json",
                "spec_content": {
                    "name": op_name,
                    "method": method,
                    "path": path,
                    "replaces": replaces,
                    "notes": str(operation.get("notes", "")).strip(),
                    "request_body": {
                        "required": mutating,
                        "shape": f"{service_name}.{op_name or 'operation'}Request",
                    },
                    "response_body": {
                        "shape": f"{service_name}.{op_name or 'operation'}Response",
                    },
                    "error_contract": {
                        "shape": "problem-details",
                        "codes": ["VALIDATION_ERROR", "NOT_FOUND", "CONFLICT"] if mutating else ["NOT_FOUND", "UNAUTHORIZED"],
                    },
                    "auth": {
                        "required": service_name != "AuthenticationService",
                        "policy": "rbac-authenticated" if service_name != "AuthenticationService" else "anonymous-login-bootstrap",
                    },
                    "traceability_refs": _safe_list_text(component.get("traceability_refs", [])),
                },
                "auth_model": "RBAC with centralized authentication" if service_name != "AuthenticationService" else "Credential validation and session bootstrap",
                "error_contract": {
                    "shape": "problem-details",
                    "retriable": False if mutating else True,
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
    for service in _as_list(api_contracts.get("services")):
        if not isinstance(service, dict):
            continue
        service_name = str(service.get("service", "")).strip()
        if not service_name:
            continue
        for idx, _operation in enumerate(_as_list(service.get("operations")), start=1):
            contract_refs_by_service.setdefault(service_name, []).append(f"contract_{_slug(service_name)}_{idx:02d}")
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


def _build_nfr_constraints(parsed: dict[str, Any], raw: dict[str, Any]) -> dict[str, Any]:
    security = _as_dict(parsed.get("security"))
    scalability = _as_dict(parsed.get("scalability"))
    infrastructure = _as_dict(parsed.get("infrastructure"))
    risk_rows = _risk_detector_rows(raw)
    transaction_risks = [
        row for row in risk_rows
        if any(token in str(row).lower() for token in ("rollback", "transaction", "select max", "concurrency", "sql injection"))
    ]
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
            *[
                f"Mitigate legacy detector finding: {str(row.get('title', '')).strip() or str(row.get('signal', '')).strip() or str(row.get('message', '')).strip()}"
                for row in transaction_risks[:3]
                if isinstance(row, dict)
            ],
        ],
        "resilience": [
            "Support staged strangler delivery with rollback-aware phase exits.",
            str(scalability.get("strategy", "")).strip(),
            "Protect user-facing traffic from background migration workloads.",
            "Explicit transaction boundaries and retry-safe ID generation are required for transactional services." if transaction_risks else "",
        ],
        "observability": [
            str(infrastructure.get("monitoring", "")).strip(),
            str(infrastructure.get("logging", "")).strip(),
            "Emit structured telemetry for migration phases and cross-service calls.",
        ],
    }


def _build_brownfield_context(
    parsed: dict[str, Any],
    raw: dict[str, Any],
    traceability: dict[str, Any],
    source_evidence_summary: dict[str, int],
) -> dict[str, Any]:
    legacy = _as_dict(parsed.get("legacy_system"))
    regression_anchors = []
    golden_flows = _golden_flow_rows(raw)
    for idx, flow in enumerate(golden_flows, start=1):
        description = str(flow.get("description", "")).strip() or str(flow.get("handler", "")).strip() or str(flow.get("entry_point", "")).strip()
        if description:
            regression_anchors.append({
                "anchor_id": f"anchor_flow_{idx:02d}",
                "type": "golden_flow",
                "description": description,
            })
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
    business_rules = [
        {
            "rule_id": str(rule.get("rule_id", "")).strip(),
            "scope": str(rule.get("form", "")).strip() or str(rule.get("module", "")).strip() or "legacy",
            "statement": str(rule.get("statement", "")).strip() or str(rule.get("rule_text", "")).strip(),
            "error_message": str(rule.get("error_message", "")).strip(),
        }
        for rule in _business_rule_rows(raw)
        if isinstance(rule, dict) and str(rule.get("rule_id", "")).strip()
    ]
    dead_refs = [
        {
            "reference": str(row.get("reference", "")).strip() or str(row.get("name", "")).strip(),
            "reason": str(row.get("reason", "")).strip() or "Legacy reference unresolved.",
        }
        for row in _dead_ref_rows(raw)
        if isinstance(row, dict)
    ]
    connection_patterns = [
        {
            "variant": str(row.get("name", "")).strip() or str(row.get("connection_name", "")).strip() or f"variant_{idx+1}",
            "provider": str(row.get("provider", "")).strip(),
            "notes": str(row.get("notes", "")).strip() or str(row.get("connection_string", "")).strip(),
        }
        for idx, row in enumerate(_connection_rows(raw))
        if isinstance(row, dict)
    ]
    risk_detector_findings = [
        {
            "signal": str(row.get("signal", "")).strip() or str(row.get("title", "")).strip(),
            "severity": str(row.get("severity", "")).strip() or "medium",
            "detail": str(row.get("message", "")).strip() or str(row.get("detail", "")).strip(),
        }
        for row in _risk_detector_rows(raw)
        if isinstance(row, dict)
    ]
    return {
        "legacy_behavior_map": {
            "summary": str(legacy.get("current_logic_summary", "")).strip(),
            "key_logic_steps": _safe_list_text(legacy.get("key_logic_steps", [])),
        },
        "business_rules": business_rules,
        "sql_reference_rows": [
            {
                "source_module": str(row.get("form", "")).strip() or str(row.get("module", "")).strip(),
                "tables": _safe_list_text(row.get("tables", [])),
                "sql_id": str(row.get("sql_id", "")).strip(),
            }
            for row in _sql_rows(raw)
            if isinstance(row, dict)
        ],
        "technical_debt_policy": {
            "preserve": [row["reference"] for row in dead_refs[:3]],
            "eliminate": dependencies,
            "notes": f"Analyst surfaced {len(risk_ids)} explicit risk signal(s), {len(risk_detector_findings)} static detector finding(s), and {len(dead_refs)} unresolved legacy reference(s).",
            "dead_references": dead_refs,
            "connection_patterns": connection_patterns,
            "risk_detector_findings": risk_detector_findings,
        },
        "data_migration_strategy": {
            "approach": "Strangler-compatible database transition with explicit ownership and parity checkpoints.",
            "target": "Operational database owned by target services.",
        },
        "regression_test_anchors": regression_anchors,
        "source_evidence_summary": source_evidence_summary,
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
