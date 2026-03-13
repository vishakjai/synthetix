from __future__ import annotations

import hashlib
import json
import re
from pathlib import Path
from typing import Any


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _slug(value: Any) -> str:
    text = re.sub(r"[^a-z0-9]+", "-", _clean(value).lower())
    return text.strip("-") or "artifact"


def _stable_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, ensure_ascii=True, default=str, separators=(",", ":"))


def _sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def _artifact_hashes(architect_package: dict[str, Any]) -> dict[str, str]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    hashes: dict[str, str] = {}
    for key, value in sorted(artifacts.items()):
        hashes[key] = _sha256_text(_stable_json(value))
    return hashes


def _codebase_id(state: dict[str, Any], parsed: dict[str, Any]) -> str:
    integration_context = _as_dict(state.get("integration_context"))
    brownfield = _as_dict(integration_context.get("brownfield"))
    repo_url = _clean(state.get("repo_url")) or _clean(brownfield.get("repo_url"))
    if repo_url:
        name = repo_url.rstrip("/").rsplit("/", 1)[-1]
        if name.endswith(".git"):
            name = name[:-4]
        return _slug(name)
    analyst = _as_dict(state.get("analyst_output"))
    project_name = _clean(analyst.get("project_name")) or _clean(parsed.get("architecture_name")) or "legacy-modernization"
    return _slug(project_name)


def _service_replacements(traceability: dict[str, Any]) -> dict[str, list[str]]:
    rows: dict[str, list[str]] = {}
    for mapping in _as_list(traceability.get("mappings")):
        if not isinstance(mapping, dict):
            continue
        source = _as_dict(mapping.get("source"))
        target = _as_dict(mapping.get("target"))
        service_name = _clean(target.get("service"))
        module_name = _clean(source.get("module"))
        if service_name and module_name:
            rows.setdefault(service_name, []).append(module_name)
    return {key: sorted(set(value)) for key, value in rows.items()}


def _module_loc_map(state: dict[str, Any]) -> dict[str, int]:
    analyst = _as_dict(state.get("analyst_output"))
    legacy = _as_dict(analyst.get("legacy_code_inventory"))
    loc_rows = _as_list(legacy.get("source_loc_by_file"))
    loc_map: dict[str, int] = {}
    for row in loc_rows:
        if not isinstance(row, dict):
            continue
        path = _clean(row.get("path"))
        if not path:
            continue
        module_name = Path(path.replace("\\", "/")).stem
        loc_map[module_name] = int(row.get("loc", 0) or 0)
    raw = _as_dict(analyst.get("raw_artifacts"))
    dossiers = _as_list(_as_dict(raw.get("form_dossier")).get("dossiers"))
    for row in dossiers:
        if not isinstance(row, dict):
            continue
        module_name = _clean(row.get("form_name") or row.get("base_form_name"))
        if module_name:
            loc_map[module_name] = int(row.get("source_loc", 0) or loc_map.get(module_name, 0))
    return loc_map


def _module_purpose_map(state: dict[str, Any]) -> dict[str, str]:
    analyst = _as_dict(state.get("analyst_output"))
    raw = _as_dict(analyst.get("raw_artifacts"))
    dossiers = _as_list(_as_dict(raw.get("form_dossier")).get("dossiers"))
    purposes: dict[str, str] = {}
    for row in dossiers:
        if not isinstance(row, dict):
            continue
        module_name = _clean(row.get("form_name") or row.get("base_form_name"))
        purpose = _clean(row.get("purpose") or row.get("business_use"))
        if module_name and purpose:
            purposes[module_name] = purpose
    return purposes


def _global_state_rows(state: dict[str, Any]) -> list[dict[str, Any]]:
    analyst = _as_dict(state.get("analyst_output"))
    raw = _as_dict(analyst.get("raw_artifacts"))
    variables = _as_list(_as_dict(raw.get("global_module_inventory")).get("variables"))
    rows: list[dict[str, Any]] = []
    for row in variables:
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "name": _clean(row.get("name")),
                "used_in_modules": ", ".join(str(value).strip() for value in _as_list(row.get("used_in_modules")) if str(value).strip()),
                "owning_service": _clean(row.get("owning_service")) or "TBD",
                "migration_blocker": "Yes" if len(_as_list(row.get("used_in_modules"))) >= 2 else "No",
            }
        )
    return rows


def _known_unknowns(state: dict[str, Any], architect_handoff: dict[str, Any]) -> list[dict[str, str]]:
    technical_debt = _as_dict(_as_dict(architect_handoff.get("brownfield_context")).get("technical_debt_policy"))
    connection_patterns = _as_list(technical_debt.get("connection_patterns"))
    dependencies = [str(value).strip() for value in _as_list(technical_debt.get("eliminate")) if str(value).strip()]
    unknowns: list[dict[str, str]] = []
    if dependencies:
        unknowns.append(
            {
                "item": "Runtime dependency registration outside the repository",
                "detail": f"Static analysis found external dependencies such as {', '.join(dependencies[:3])}, but deployment-time registration and version pinning cannot be confirmed from source alone.",
                "blocks_phase": "Phase 1",
                "resolution_path": "Validate runtime images / installer manifests before finalizing shell and authentication cutover.",
            }
        )
    if connection_patterns:
        unknowns.append(
            {
                "item": "Connection string secret resolution",
                "detail": "Connection variants were extracted from code, but secret material and environment-specific overrides cannot be confirmed from static analysis.",
                "blocks_phase": "Phase 2",
                "resolution_path": "Confirm production secret source and runtime configuration path with client operations.",
            }
        )
    unknowns.append(
        {
            "item": "External batch or scheduler orchestration",
            "detail": "The repository does not prove whether reporting, reconciliation, or end-of-day workflows are triggered by external schedulers outside the codebase.",
            "blocks_phase": "Phase 4",
            "resolution_path": "Confirm production job schedule and operator runbooks before finalizing migration constraints.",
        }
    )
    unknowns.append(
        {
            "item": "Production data volume and concurrency baseline",
            "detail": "Static analysis identifies write paths and concurrency risks, but not live transaction volume, peak user load, or data retention size.",
            "blocks_phase": "Phase 3",
            "resolution_path": "Collect production telemetry / DBA baselines before setting final performance and scaling targets.",
        }
    )
    return unknowns[:4]


def _legacy_component_inventory(state: dict[str, Any], architect_package: dict[str, Any]) -> list[dict[str, Any]]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    traceability = _as_dict(artifacts.get("traceability_matrix"))
    coupling = _as_dict(artifacts.get("coupling_heatmap"))
    loc_by_module = _module_loc_map(state)
    purpose_by_module = _module_purpose_map(state)
    coupling_by_module = {
        _clean(row.get("name")): row
        for row in _as_list(coupling.get("modules"))
        if isinstance(row, dict) and _clean(row.get("name"))
    }
    rows: list[dict[str, Any]] = []
    seen: set[str] = set()
    for mapping in _as_list(traceability.get("mappings")):
        if not isinstance(mapping, dict):
            continue
        source = _as_dict(mapping.get("source"))
        target = _as_dict(mapping.get("target"))
        module_name = _clean(source.get("module"))
        if not module_name or module_name in seen:
            continue
        seen.add(module_name)
        coupling_row = _as_dict(coupling_by_module.get(module_name))
        rows.append(
            {
                "module": module_name,
                "purpose": purpose_by_module.get(module_name) or "Legacy workflow module.",
                "loc": str(loc_by_module.get(module_name, 0)),
                "service": _clean(target.get("service")),
                "instability": str(coupling_row.get("instability", "")),
                "risk_tier": _clean(coupling_row.get("risk_tier")) or "Unknown",
            }
        )
    return rows


def _legacy_technical_debt(architect_package: dict[str, Any], architect_handoff: dict[str, Any]) -> list[dict[str, str]]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    risk_register = _as_dict(artifacts.get("component_risk_register"))
    handoff_debt = _as_dict(_as_dict(architect_handoff.get("brownfield_context")).get("technical_debt_policy"))
    detector_rows = [row for row in _as_list(handoff_debt.get("risk_detector_findings")) if isinstance(row, dict)]
    debt_rows: list[dict[str, str]] = []
    for row in _as_list(risk_register.get("services")):
        if not isinstance(row, dict):
            continue
        tier = _clean(row.get("risk_tier"))
        if tier not in {"High", "Critical"}:
            continue
        mitigations = _as_list(row.get("mitigation_recommendations"))
        debt_rows.append(
            {
                "item": f"{_clean(row.get('service'))} migration risk",
                "severity": tier,
                "migration_impact": f"Affects {(_clean(row.get('service')) or 'service')} delivery sequencing and review gates.",
                "affected_phase": "See migration plan",
                "evidence": "; ".join(str(value).strip() for value in mitigations[:2] if str(value).strip()) or "Risk register mitigation required.",
            }
        )
    for row in detector_rows[:4]:
        debt_rows.append(
            {
                "item": _clean(row.get("signal")) or "legacy-detector-finding",
                "severity": _clean(row.get("severity")).upper() or "MEDIUM",
                "migration_impact": _clean(row.get("detail")) or "Static detector finding requires mitigation during migration.",
                "affected_phase": "Phase 3" if "transaction" in _clean(row.get("detail")).lower() else "Phase 2",
                "evidence": _clean(row.get("detail")) or "Detector output",
            }
        )
    return debt_rows[:8]


def _service_inventory(architect_package: dict[str, Any], services: list[dict[str, Any]]) -> list[dict[str, Any]]:
    traceability = _as_dict(_as_dict(architect_package.get("artifacts")).get("traceability_matrix"))
    replacements = _service_replacements(traceability)
    rows: list[dict[str, Any]] = []
    for service in services:
        if not isinstance(service, dict):
            continue
        service_name = _clean(service.get("name"))
        if not service_name:
            continue
        rows.append(
            {
                "service": service_name,
                "responsibility": _clean(service.get("responsibility")),
                "replaces": ", ".join(replacements.get(service_name, [])),
                "database": _clean(service.get("database")),
                "api_type": _clean(service.get("api_type")) or "REST",
            }
        )
    return rows


def _api_topology_rows(architect_handoff: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for contract in _as_list(architect_handoff.get("interface_contracts")):
        if not isinstance(contract, dict):
            continue
        spec_content = _as_dict(contract.get("spec_content"))
        rows.append(
            {
                "service": _clean(contract.get("owner_component")),
                "method": _clean(spec_content.get("method") or contract.get("method")),
                "path": _clean(spec_content.get("path") or contract.get("path")),
                "source_trace": ", ".join(str(value).strip() for value in _as_list(spec_content.get("replaces")) if str(value).strip()),
                "status": "DRAFT",
            }
        )
    return rows


def _target_data_rows(architect_package: dict[str, Any]) -> list[dict[str, str]]:
    ownership = _as_dict(_as_dict(architect_package.get("artifacts")).get("data_ownership_matrix"))
    rows: list[dict[str, str]] = []
    for row in _as_list(ownership.get("entities")):
        if not isinstance(row, dict):
            continue
        rows.append(
            {
                "entity": _clean(row.get("name")),
                "owning_service": _clean(row.get("owning_service")),
                "read_services": ", ".join(str(value).strip() for value in _as_list(row.get("read_services")) if str(value).strip()),
                "migration_plan": _clean(row.get("migration_notes")) or "See data ownership matrix.",
            }
        )
    return rows


def _global_state_resolution_rows(state: dict[str, Any]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    for row in _global_state_rows(state):
        rows.append(
            {
                "variable": _clean(row.get("name")),
                "legacy_usage": _clean(row.get("used_in_modules")),
                "target_strategy": f"Resolve into {_clean(row.get('owning_service'))} owned state or typed contract payload.",
                "blocker": _clean(row.get("migration_blocker")),
            }
        )
    return rows


def _delta_map_rows(architect_package: dict[str, Any]) -> list[dict[str, str]]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    traceability = _as_dict(artifacts.get("traceability_matrix"))
    adrs = _as_list(artifacts.get("architecture_decision_records"))
    adr_by_service: dict[str, str] = {}
    for adr in adrs:
        if not isinstance(adr, dict):
            continue
        adr_id = _clean(adr.get("id"))
        targets = _as_list(_as_dict(adr.get("traceability")).get("target_services"))
        for service_name in targets:
            if _clean(service_name) and adr_id:
                adr_by_service.setdefault(_clean(service_name), adr_id)
    rows: list[dict[str, str]] = []
    for mapping in _as_list(traceability.get("mappings")):
        if not isinstance(mapping, dict):
            continue
        source = _as_dict(mapping.get("source"))
        target = _as_dict(mapping.get("target"))
        service_name = _clean(target.get("service"))
        rows.append(
            {
                "legacy_component": _clean(source.get("module")),
                "target_service": service_name,
                "strategy": _clean(target.get("migration_strategy")) or "Rewrite",
                "target_component": _clean(target.get("component")),
                "adr_ref": adr_by_service.get(service_name) or "ADR-TBD",
            }
        )
    return rows


def _hld_generation_gate(architect_package: dict[str, Any], services: list[dict[str, Any]]) -> tuple[bool, list[str]]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    package_meta = _as_dict(architect_package.get("package_meta"))
    traceability = _as_dict(artifacts.get("traceability_matrix"))
    coverage = _as_dict(traceability.get("coverage"))
    status = _clean(package_meta.get("status")).upper()
    adrs = _as_list(artifacts.get("architecture_decision_records"))
    traceability_services = {
        _clean(_as_dict(row.get("target")).get("service"))
        for row in _as_list(traceability.get("mappings"))
        if isinstance(row, dict) and _clean(_as_dict(row.get("target")).get("service"))
    }
    reasons: list[str] = []
    if status not in {"COMPLETE", "WARN"}:
        reasons.append("architect_package.status does not permit HLD generation")
    if int(coverage.get("mapped_unmapped", 0) or 0) != 0:
        reasons.append("traceability_matrix has unmapped modules")
    if len(adrs) < len(traceability_services):
        reasons.append("ADR coverage is incomplete for the proposed service boundaries")
    if len(_as_dict(architect_package.get("artifacts"))) < 7:
        reasons.append("not all structured architect artifacts are present")
    if not services:
        reasons.append("no target services are available for HLD synthesis")
    return not reasons, reasons


def _placeholder(text: str) -> str:
    return f"[ {text} ]"


def _legacy_hld_payload(
    state: dict[str, Any],
    parsed: dict[str, Any],
    architect_package: dict[str, Any],
    architect_handoff: dict[str, Any],
    artifact_hashes: dict[str, str],
    codebase_id: str,
) -> dict[str, Any]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    traceability = _as_dict(artifacts.get("traceability_matrix"))
    coupling = _as_dict(artifacts.get("coupling_heatmap"))
    inventory_rows = _legacy_component_inventory(state, architect_package)
    dependencies = [
        _clean(row.get("name"))
        for row in _as_list(_as_dict(_as_dict(state.get("analyst_output")).get("raw_artifacts")).get("dependency_inventory", {}).get("dependencies", []))
        if isinstance(row, dict) and _clean(row.get("name"))
    ]
    if not dependencies:
        dependencies = [value for value in _as_list(_as_dict(_as_dict(architect_handoff.get("brownfield_context")).get("technical_debt_policy")).get("eliminate")) if _clean(value)]
    module_count = len(inventory_rows)
    loc_total = sum(int(row.get("loc", "0") or 0) for row in inventory_rows)
    known_unknowns = _known_unknowns(state, architect_handoff)
    integration_rows = []
    for row in _as_list(_as_dict(architect_handoff.get("domain_model")).get("data_ownership")):
        if not isinstance(row, dict):
            continue
        integration_rows.append(
            {
                "entity": _clean(row.get("entity_name")),
                "owner": _clean(row.get("owning_service")),
                "readers": ", ".join(str(value).strip() for value in _as_list(row.get("read_services")) if str(value).strip()),
            }
        )
    return {
        "document_type": "legacy",
        "title": "Legacy High Level Design",
        "codebase_id": codebase_id,
        "version": "0.1",
        "status": "DRAFT",
        "classification": "CONFIDENTIAL",
        "brand_color": "#1B3A5C",
        "accent_color": "#E8511A",
        "critical_color": "#C0392B",
        "docx_path": f"outputs/Legacy-HLD-{codebase_id}-v0.1.docx",
        "metadata_rows": [
            ("Document Title", "Legacy High Level Design"),
            ("Codebase ID", codebase_id),
            ("Version", "0.1"),
            ("Status", "DRAFT"),
            ("Classification", "CONFIDENTIAL"),
        ],
        "cover_warnings": [
            "Sections with sourced placeholders require human architect review before distribution.",
            "Known Unknowns in Section 7 must be resolved or explicitly accepted before baseline sign-off.",
        ],
        "diagram_mermaid": _clean(_as_dict(parsed.get("legacy_system")).get("current_system_diagram_mermaid")),
        "diagram_title": "Current System Diagram",
        "sections": [
            {
                "title": "1. Executive Summary",
                "paragraphs": [
                    f"The legacy estate currently maps {module_count} source modules across {len(_as_list(coupling.get('modules')))} analyzed coupling rows.",
                    f"Static inventory covers {loc_total} lines of code across the mapped legacy modules and {len(dependencies)} external dependency reference(s).",
                ],
                "bullets": [
                    f"Traceability covers {_as_dict(traceability.get('coverage')).get('total_source_modules', module_count)} source modules.",
                    f"Coupling analysis produced {len(_as_list(coupling.get('modules')))} module-level instability scores.",
                    f"Data ownership matrix contains {len(_as_list(_as_dict(artifacts.get('data_ownership_matrix')).get('entities')))} entity rows.",
                ],
            },
            {
                "title": "2. System Context",
                "paragraphs": [
                    _clean(_as_dict(parsed.get("legacy_system")).get("current_logic_summary")) or "Legacy system context summary is not yet available in the parsed architecture output.",
                    f"External integrations extracted from source evidence: {', '.join(dependencies) if dependencies else _placeholder('Populate from symbol_index.external_deps - no dependency rows were available in the current run.')}",
                ],
            },
            {
                "title": "3. Component Architecture",
                "figure": True,
                "tables": [
                    {
                        "title": "Module Inventory",
                        "columns": ["Module", "Purpose", "LOC", "Mapped Service", "Instability", "Risk Tier"],
                        "rows": inventory_rows,
                    }
                ],
            },
            {
                "title": "4. Integration Catalogue",
                "tables": [
                    {
                        "title": "Legacy Data and Integration Touchpoints",
                        "columns": ["Entity", "Owner", "Readers"],
                        "rows": integration_rows or [
                            {
                                "entity": _placeholder("Populate from data_ownership_matrix - integration touchpoints were unavailable in this run."),
                                "owner": "Source: data_ownership_matrix",
                                "readers": "TBD",
                            }
                        ],
                    }
                ],
            },
            {
                "title": "5. Data Architecture",
                "tables": [
                    {
                        "title": "Global State Inventory",
                        "columns": ["Variable", "Used In Modules", "Owning Service", "Migration Blocker"],
                        "rows": [
                            {
                                "variable": row.get("name"),
                                "used_in_modules": row.get("used_in_modules"),
                                "owning_service": row.get("owning_service"),
                                "migration_blocker": row.get("migration_blocker"),
                            }
                            for row in _global_state_rows(state)
                        ] or [
                            {
                                "variable": _placeholder("Populate from symbol_index.globals - no global variable inventory was available."),
                                "used_in_modules": "Source: symbol_index.globals",
                                "owning_service": "TBD",
                                "migration_blocker": "TBD",
                            }
                        ],
                    }
                ],
            },
            {
                "title": "6. Technical Debt Catalogue",
                "tables": [
                    {
                        "title": "Debt Items",
                        "columns": ["Item", "Severity", "Migration Impact", "Affected Phase", "Evidence"],
                        "rows": _legacy_technical_debt(architect_package, architect_handoff),
                    }
                ],
            },
            {
                "title": "7. Known Unknowns",
                "tables": [
                    {
                        "title": "Known Unknowns",
                        "columns": ["Item", "Detail", "Blocks Phase", "Resolution Path"],
                        "rows": known_unknowns,
                    }
                ],
            },
            {
                "title": "8. Operational Characteristics",
                "paragraphs": [
                    "Execution remains form-driven and stateful, with modernization risk concentrated in write-heavy transaction and reporting workflows.",
                    "Error handling and runtime behaviors are only partially visible through static analysis; validate production scheduler, dependency registration, and throughput baselines before final baseline sign-off.",
                ],
            },
        ],
        "appendix_hashes": artifact_hashes,
    }


def _target_hld_payload(
    state: dict[str, Any],
    parsed: dict[str, Any],
    architect_package: dict[str, Any],
    architect_handoff: dict[str, Any],
    services: list[dict[str, Any]],
    artifact_hashes: dict[str, str],
    codebase_id: str,
) -> dict[str, Any]:
    artifacts = _as_dict(architect_package.get("artifacts"))
    traceability = _as_dict(artifacts.get("traceability_matrix"))
    migration_plan = _as_dict(artifacts.get("strangler_migration_plan"))
    adrs = _as_list(artifacts.get("architecture_decision_records"))
    delta_rows = _delta_map_rows(architect_package)
    principles = []
    for idx, adr in enumerate(adrs, start=1):
        if not isinstance(adr, dict):
            continue
        principles.append(
            {
                "principle": f"Principle {idx}",
                "decision": _clean(adr.get("title")) or f"ADR {idx}",
                "rationale": _clean(_as_dict(adr.get("context")).get("narrative")) or _clean(adr.get("decision")),
                "adr_refs": _clean(adr.get("id")),
            }
        )
    security_rows = [value for value in _as_list(_as_dict(architect_handoff.get("nfr_constraints")).get("security")) if _clean(value)]
    observability_rows = [value for value in _as_list(_as_dict(architect_handoff.get("nfr_constraints")).get("observability")) if _clean(value)]
    performance_rows = [value for value in _as_list(_as_dict(architect_handoff.get("nfr_constraints")).get("performance")) if _clean(value)]
    infra = _as_dict(_as_dict(architect_handoff.get("system_context")).get("infrastructure"))
    return {
        "document_type": "target",
        "title": "Target High Level Design",
        "codebase_id": codebase_id,
        "version": "0.1",
        "status": "DRAFT",
        "classification": "CONFIDENTIAL",
        "brand_color": "#0D3B2E",
        "accent_color": "#E8511A",
        "critical_color": "#C0392B",
        "docx_path": f"outputs/Target-HLD-{codebase_id}-v0.1.docx",
        "metadata_rows": [
            ("Document Title", "Target High Level Design"),
            ("Codebase ID", codebase_id),
            ("Version", "0.1"),
            ("Status", "DRAFT"),
            ("Classification", "CONFIDENTIAL"),
        ],
        "cover_warnings": [
            "All API contracts are DRAFT and require human architect review before client baseline.",
            "Any placeholder marked with a source instruction requires client or architect confirmation before distribution.",
        ],
        "diagram_mermaid": _clean(parsed.get("target_system_diagram_mermaid")),
        "diagram_title": "Target Architecture Diagram",
        "sections": [
            {
                "title": "1. Executive Summary",
                "paragraphs": [
                    f"The target architecture decomposes the estate into {len(services)} services across {len(_as_list(migration_plan.get('phases')))} migration phases.",
                    f"The design is governed by {len(adrs)} architecture decision record(s) and a {len(delta_rows)}-row legacy-to-target delta map.",
                ],
            },
            {
                "title": "2. Architectural Principles",
                "tables": [
                    {
                        "title": "Architectural Principles",
                        "columns": ["Principle", "Decision", "Rationale", "ADR Refs"],
                        "rows": principles or [
                            {
                                "principle": _placeholder("Populate from architecture_decision_records - no ADR payload was available."),
                                "decision": "Source: architecture_decision_records",
                                "rationale": "TBD",
                                "adr_refs": "TBD",
                            }
                        ],
                    }
                ],
            },
            {
                "title": "3. Service Architecture",
                "figure": True,
                "tables": [
                    {
                        "title": "Service Inventory",
                        "columns": ["Service", "Responsibility", "Replaces", "Database", "API Type"],
                        "rows": _service_inventory(architect_package, services),
                    }
                ],
            },
            {
                "title": "4. API Topology",
                "tables": [
                    {
                        "title": "API Contract Summary",
                        "columns": ["Service", "Method", "Path", "Source Trace", "Status"],
                        "rows": _api_topology_rows(architect_handoff),
                    }
                ],
            },
            {
                "title": "5. Data Architecture",
                "tables": [
                    {
                        "title": "Entity Ownership",
                        "columns": ["Entity", "Owning Service", "Read Services", "Migration Plan"],
                        "rows": _target_data_rows(architect_package),
                    },
                    {
                        "title": "Global State Resolution",
                        "columns": ["Variable", "Legacy Usage", "Target Strategy", "Blocker"],
                        "rows": _global_state_resolution_rows(state) or [
                            {
                                "variable": _placeholder("Populate from symbol_index.globals - no global state inventory was available."),
                                "legacy_usage": "Source: symbol_index.globals",
                                "target_strategy": "TBD",
                                "blocker": "TBD",
                            }
                        ],
                    },
                ],
            },
            {
                "title": "6. Security Architecture",
                "bullets": security_rows or [
                    _placeholder("Populate from target_stack_constraints / client_constraints - confirm auth, authorization, and data classification model before baseline."),
                ],
            },
            {
                "title": "7. Deployment Architecture",
                "bullets": [
                    f"Runtime topology: {_clean(infra.get('container_orchestration')) or _placeholder('Populate from target_stack_constraints - confirm runtime orchestration model.')}",
                    f"CI/CD: {_clean(infra.get('ci_cd')) or _placeholder('Populate from target_stack_constraints - confirm delivery pipeline.')}",
                    "Migration-period dual-run topology must preserve legacy coexistence until parity sign-off is complete.",
                ],
            },
            {
                "title": "8. Non-Functional Requirements",
                "tables": [
                    {
                        "title": "NFR Targets",
                        "columns": ["Category", "Legacy Baseline / Notes"],
                        "rows": (
                            [{"category": "Performance", "legacy_baseline___notes": value} for value in performance_rows]
                            + [{"category": "Observability", "legacy_baseline___notes": value} for value in observability_rows]
                            + [{"category": "Security", "legacy_baseline___notes": value} for value in security_rows]
                        ) or [
                            {
                                "category": "Performance",
                                "legacy_baseline___notes": _placeholder("Populate from client_constraints - final performance target requires client confirmation."),
                            }
                        ],
                    }
                ],
            },
            {
                "title": "9. Delta Map",
                "tables": [
                    {
                        "title": "Legacy to Target Mapping",
                        "columns": ["Legacy Component", "Target Service", "Strategy", "Target Component", "ADR Ref"],
                        "rows": delta_rows,
                    }
                ],
            },
            {
                "title": "10. Migration Constraints",
                "bullets": [
                    f"Critical path: {', '.join(str(value).strip() for value in _as_list(migration_plan.get('critical_path')) if str(value).strip()) or _placeholder('Populate from strangler_migration_plan.critical_path.')}",
                    "All global state blockers in Section 5 must be resolved before target-state cutover for the affected service.",
                    "No contract in Section 4 is final without human architect review and client sign-off.",
                ],
            },
        ],
        "appendix_hashes": artifact_hashes,
    }


def _count_placeholders(payload: dict[str, Any]) -> int:
    count = 0
    for row in _as_list(payload.get("cover_warnings")):
        text = _clean(row)
        if text.startswith("["):
            count += 1
    for section in _as_list(payload.get("sections")):
        if not isinstance(section, dict):
            continue
        for key in ("paragraphs", "bullets"):
            for value in _as_list(section.get(key)):
                if _clean(value).startswith("["):
                    count += 1
        for table in _as_list(section.get("tables")):
            if not isinstance(table, dict):
                continue
            for row in _as_list(table.get("rows")):
                if isinstance(row, dict):
                    for value in row.values():
                        if _clean(value).startswith("["):
                            count += 1
    return count


def build_hld_documents(
    state: dict[str, Any],
    parsed: dict[str, Any],
    architect_package: dict[str, Any],
    architect_handoff: dict[str, Any],
    services: list[dict[str, Any]],
) -> dict[str, Any]:
    artifact_hashes = _artifact_hashes(architect_package)
    codebase_id = _codebase_id(state, parsed)
    allowed, reasons = _hld_generation_gate(architect_package, services)
    if not allowed:
        return {
            "generation_status": "blocked",
            "gate_failures": reasons,
            "legacy_hld": {},
            "target_hld": {},
        }
    legacy_payload = _legacy_hld_payload(state, parsed, architect_package, architect_handoff, artifact_hashes, codebase_id)
    target_payload = _target_hld_payload(state, parsed, architect_package, architect_handoff, services, artifact_hashes, codebase_id)
    known_unknown_section = next(
        (
            section
            for section in _as_list(legacy_payload.get("sections"))
            if isinstance(section, dict) and _clean(section.get("title")) == "7. Known Unknowns"
        ),
        {},
    )
    known_unknown_tables = _as_list(_as_dict(known_unknown_section).get("tables"))
    known_unknown_rows = _as_list(_as_dict(known_unknown_tables[0] if known_unknown_tables else {}).get("rows"))
    return {
        "generation_status": "generated",
        "gate_failures": [],
        "legacy_hld": {
            "docx_path": legacy_payload.get("docx_path"),
            "section_count": len(_as_list(legacy_payload.get("sections"))),
            "placeholder_count": _count_placeholders(legacy_payload),
            "known_unknown_count": len(known_unknown_rows),
            "source_artefact_hashes": artifact_hashes,
            "render_payload": legacy_payload,
        },
        "target_hld": {
            "docx_path": target_payload.get("docx_path"),
            "section_count": len(_as_list(target_payload.get("sections"))),
            "placeholder_count": _count_placeholders(target_payload),
            "adr_references": len(_as_list(_as_dict(architect_package.get("artifacts")).get("architecture_decision_records"))),
            "delta_map_row_count": len(_delta_map_rows(architect_package)),
            "source_artefact_hashes": artifact_hashes,
            "render_payload": target_payload,
        },
    }
