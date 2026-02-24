"""
Analyst Agent-as-a-Service (AAS) orchestration.

Implements a deterministic LangGraph DAG:
1) ingest requirement and load persona/domain context
2) query knowledge graph dependencies
3) query vector context
4) query compliance constraints
5) synthesize requirements pack
6) validate contract sections
"""

from __future__ import annotations

import copy
import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, TypedDict

from langgraph.graph import END, StateGraph

from agents.analyst import AnalystAgent
from config import LLMProvider, PipelineConfig
from utils.domain_packs import (
    build_open_questions,
    get_domain_pack,
    infer_data_classification,
    infer_domain_pack_id,
    infer_jurisdiction,
    map_to_capabilities,
    normalize_requirement,
    retrieve_gold_patterns,
)
from utils.knowledge_gateway import KnowledgeGateway
from utils.llm import LLMClient
from utils.persona_registry import PersonaRegistry
from utils.settings_store import SettingsStore
from utils.tenant_memory import TenantMemoryStore


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


class AnalystServiceState(TypedDict, total=False):
    payload: dict[str, Any]
    actor: str
    scope: dict[str, Any]
    requirement: str
    thread_id: str
    persona: dict[str, Any]
    domain_pack: dict[str, Any]
    normalized_requirement: dict[str, Any]
    jurisdiction: str
    data_classification: list[str]
    memory_constraints: list[dict[str, Any]]
    memory_thread: list[dict[str, Any]]
    capability_mapping: dict[str, Any]
    graph_edges: list[dict[str, Any]]
    vector_context: list[dict[str, Any]]
    compliance_constraints: list[dict[str, Any]]
    gold_patterns: list[dict[str, Any]]
    standards_guidance: list[dict[str, Any]]
    quality_gates: list[dict[str, Any]]
    requirements_pack: dict[str, Any]
    assistant_summary: str
    warnings: list[str]
    errors: list[str]
    trace: list[dict[str, Any]]


def _extract_capability_ids(capability_map: dict[str, Any]) -> list[str]:
    primary = capability_map.get("primary_capabilities", [])
    ids: list[str] = []
    for row in primary if isinstance(primary, list) else []:
        if not isinstance(row, dict):
            continue
        cid = str(row.get("id", "")).strip()
        if cid:
            ids.append(cid)
    return ids


def _select_standards(domain_pack: dict[str, Any], capability_ids: list[str]) -> list[dict[str, Any]]:
    cap_set = {str(x).strip() for x in capability_ids if str(x).strip()}
    rows: list[dict[str, Any]] = []
    standards = domain_pack.get("standards", [])
    for item in standards if isinstance(standards, list) else []:
        if not isinstance(item, dict):
            continue
        applies = {str(x).strip() for x in (item.get("applies_to", []) if isinstance(item.get("applies_to", []), list) else [])}
        if applies and cap_set and not applies.intersection(cap_set):
            continue
        rows.append(
            {
                "id": str(item.get("id", "")).strip(),
                "name": str(item.get("name", "")).strip(),
                "engineering_actions": list(item.get("engineering_actions", [])) if isinstance(item.get("engineering_actions", []), list) else [],
            }
        )
    return rows[:10]


def _deterministic_pack(state: AnalystServiceState) -> dict[str, Any]:
    requirement = str(state.get("requirement", "")).strip()
    normalized = state.get("normalized_requirement", {})
    capability_map = state.get("capability_mapping", {})
    compliance = state.get("compliance_constraints", [])
    gold_patterns = state.get("gold_patterns", [])
    standards = state.get("standards_guidance", [])
    domain_pack = state.get("domain_pack", {})
    persona = state.get("persona", {})
    open_questions = build_open_questions(normalized, capability_map, compliance)

    functional_requirements: list[dict[str, Any]] = []
    primary = capability_map.get("primary_capabilities", [])
    for idx, cap in enumerate(primary if isinstance(primary, list) else [], start=1):
        if not isinstance(cap, dict):
            continue
        cap_name = str(cap.get("service_domain", "Capability")).strip() or "Capability"
        fr_id = f"FR-{idx:03d}"
        functional_requirements.append(
            {
                "id": fr_id,
                "title": f"{cap_name} implementation",
                "description": (
                    f"Implement {cap_name} behavior for: {requirement[:220]}"
                ),
                "priority": "P0" if idx <= 2 else "P1",
                "acceptance_criteria": [
                    f"{cap_name} flow is documented with explicit inputs and outputs.",
                    f"{cap_name} behavior is traceable to test scenarios.",
                    "Error handling and audit visibility are defined for this flow.",
                ],
            }
        )

    if not functional_requirements:
        functional_requirements.append(
            {
                "id": "FR-001",
                "title": "Core business flow",
                "description": requirement[:260] or "Implement the requested business objective.",
                "priority": "P0",
                "acceptance_criteria": [
                    "Core business flow is captured as executable scenarios.",
                    "Inputs, outputs, and side effects are explicitly documented.",
                    "Failure modes and validation behavior are defined.",
                ],
            }
        )

    non_functional_requirements = [
        {
            "id": "NFR-001",
            "title": "Auditability",
            "description": "All critical state changes are traceable to actor, action, and outcome.",
            "category": "reliability",
            "metric": "100% auditable critical events",
            "acceptance_criteria": [
                "Audit event schema is defined.",
                "Critical actions emit audit events.",
                "Evidence links are available for release verification.",
            ],
        },
        {
            "id": "NFR-002",
            "title": "Security controls",
            "description": "Sensitive data handling follows masking and least-privilege principles.",
            "category": "security",
            "metric": "No unmasked sensitive identifiers in logs",
            "acceptance_criteria": [
                "Sensitive fields are redacted in logs.",
                "Authorization controls are explicitly defined.",
                "Security test coverage includes critical paths.",
            ],
        },
    ]

    bdd_features: list[dict[str, Any]] = []
    for idx, fr in enumerate(functional_requirements[:4], start=1):
        fr_id = str(fr.get("id", f"FR-{idx:03d}"))
        title = str(fr.get("title", "Feature"))
        scenario_id = f"BDD-SC-{idx:03d}"
        bdd_features.append(
            {
                "id": f"BDD-FEAT-{idx:03d}",
                "title": title,
                "source_requirement_ids": [fr_id],
                "gherkin": (
                    f"Feature: {title}\n"
                    "Scenario: Core behavior\n"
                    "Given the system is configured for this capability\n"
                    "When the user submits a valid request\n"
                    "Then the expected business response is returned"
                ),
                "scenario_ids": [scenario_id],
            }
        )

    gates = [
        {
            "name": "requirements_completeness",
            "status": "PASS" if len(functional_requirements) >= 1 else "FAIL",
            "message": "At least one functional requirement exists.",
        },
        {
            "name": "bdd_presence",
            "status": "PASS" if len(bdd_features) >= 1 else "FAIL",
            "message": "BDD scenarios are present.",
        },
    ]

    return {
        "artifact_type": "requirements_pack",
        "artifact_version": "2.0",
        "artifact_id": f"reqpack-{uuid.uuid4().hex[:12]}",
        "generated_at": _utc_now(),
        "generated_by": {
            "agent_name": "Analyst Agent",
            "persona": str(persona.get("name", "Analyst Persona")),
            "persona_version": str(persona.get("version", "1.0.0")),
            "domain_pack": {
                "id": str(domain_pack.get("id", "")),
                "version": str(domain_pack.get("version", "")),
                "ontologies": [str(domain_pack.get("ontology", {}).get("framework", ""))] if isinstance(domain_pack.get("ontology", {}), dict) else [],
                "standards": [str(item.get("id", "")) for item in standards if isinstance(item, dict)],
                "internal_playbooks": [str(item.get("id", "")) for item in gold_patterns if isinstance(item, dict)],
            },
        },
        "project": {
            "name": "Analyzed Requirement",
            "jurisdictions": [str(state.get("jurisdiction", "GLOBAL"))],
            "regulatory_profile": [str(item.get("id", "")) for item in compliance if isinstance(item, dict)],
        },
        "intake": {
            "business_request_raw": requirement,
            "actors": list(normalized.get("actors", [])) if isinstance(normalized.get("actors", []), list) else [],
            "channels": [],
            "data_elements": [],
            "constraints_from_user": [str(row.get("text", "")) for row in state.get("memory_constraints", []) if isinstance(row, dict)],
        },
        "domain_mapping": {
            "capabilities": copy.deepcopy(capability_map.get("primary_capabilities", [])),
            "domain_invariants": [
                {"id": "INV-AUD-001", "statement": "Critical business actions must be auditable."},
                {"id": "INV-SEC-001", "statement": "Sensitive data must be masked in logs and responses."},
            ],
            "glossary": [],
        },
        "compliance": {
            "controls_triggered": copy.deepcopy(compliance),
            "privacy": {
                "pii_masking_rules": ["mask sensitive identifiers in logs"],
                "retention_days": 365,
                "access_model": "least_privilege",
            },
        },
        "requirements": {
            "functional": functional_requirements,
            "non_functional": non_functional_requirements,
        },
        "bdd": {
            "features": bdd_features,
            "lint": {"gherkin_valid": True, "missing_steps": []},
        },
        "open_questions": [{"id": f"Q-{idx+1:03d}", "question": q, "owner": "Client", "severity": "high"} for idx, q in enumerate(open_questions)],
        "risks": [],
        "out_of_scope": [],
        "traceability": {"links": []},
        "quality_gates": gates,
        "vector_context_used": copy.deepcopy(state.get("vector_context", [])),
        "graph_dependencies": copy.deepcopy(state.get("graph_edges", [])),
    }


def _deterministic_assistant_summary(requirement: str, requirements_pack: dict[str, Any]) -> str:
    req = str(requirement or "").strip()
    pack = requirements_pack if isinstance(requirements_pack, dict) else {}

    capabilities = []
    domain_mapping = pack.get("domain_mapping", {}) if isinstance(pack.get("domain_mapping", {}), dict) else {}
    for row in domain_mapping.get("capabilities", []) if isinstance(domain_mapping.get("capabilities", []), list) else []:
        if not isinstance(row, dict):
            continue
        cap = str(row.get("service_domain", "")).strip() or str(row.get("business_capability", "")).strip() or str(row.get("id", "")).strip()
        if cap:
            capabilities.append(cap)
    capabilities = capabilities[:4]

    requirements = pack.get("requirements", {}) if isinstance(pack.get("requirements", {}), dict) else {}
    functional = requirements.get("functional", []) if isinstance(requirements.get("functional", []), list) else []
    top_fr = []
    for row in functional[:3]:
        if not isinstance(row, dict):
            continue
        title = str(row.get("title", "")).strip() or str(row.get("id", "")).strip()
        if title:
            top_fr.append(title)

    compliance = pack.get("compliance", {}) if isinstance(pack.get("compliance", {}), dict) else {}
    controls = compliance.get("controls_triggered", []) if isinstance(compliance.get("controls_triggered", []), list) else []
    open_questions = pack.get("open_questions", []) if isinstance(pack.get("open_questions", []), list) else []
    bdd = pack.get("bdd", {}) if isinstance(pack.get("bdd", {}), dict) else {}
    features = bdd.get("features", []) if isinstance(bdd.get("features", []), list) else []

    lines: list[str] = []
    if req:
        lines.append(f"Objective understood: {req[:260]}")
    if capabilities:
        lines.append(f"Likely business capabilities: {', '.join(capabilities)}.")
    if top_fr:
        lines.append(f"Primary functional requirements drafted: {', '.join(top_fr)}.")
    lines.append(
        f"Generated requirements pack with {len(functional)} functional requirements, "
        f"{len(features)} BDD feature(s), {len(controls)} compliance control(s), "
        f"and {len(open_questions)} open question(s)."
    )
    lines.append("Ask me to expand inputs/outputs, acceptance criteria, or compliance rationale.")
    return " ".join(lines)


@dataclass
class AnalystAASService:
    persona_registry: PersonaRegistry
    knowledge_gateway: KnowledgeGateway
    memory_store: TenantMemoryStore
    settings_store: SettingsStore

    def __post_init__(self) -> None:
        self._graph = self._build_graph()

    def _trace(self, state: AnalystServiceState, step: str, detail: str) -> None:
        trace = state.get("trace", [])
        if not isinstance(trace, list):
            trace = []
        trace.append({"step": step, "detail": detail, "timestamp": _utc_now()})
        state["trace"] = trace

    def _node_ingest(self, state: AnalystServiceState) -> AnalystServiceState:
        payload = state.get("payload", {})
        requirement = str(payload.get("requirement") or payload.get("business_objective") or "").strip()
        if not requirement:
            state.setdefault("errors", []).append("requirement is required")
            return state

        scope = state.get("scope", {})
        persona_id = str(payload.get("persona_id", "")).strip().lower()
        persona_version = str(payload.get("persona_version", "")).strip()
        explicit_pack = str(payload.get("domain_pack_id", "")).strip().lower()
        custom_pack = payload.get("domain_pack")

        inferred_pack = infer_domain_pack_id(requirement, explicit_pack_id=explicit_pack)
        domain_pack = custom_pack if isinstance(custom_pack, dict) and custom_pack else get_domain_pack(inferred_pack)
        use_case = str(payload.get("use_case", "business_objectives")).strip().lower()
        normalized = normalize_requirement(requirement, use_case=use_case)
        jurisdiction = str(payload.get("jurisdiction", "")).strip().upper() or infer_jurisdiction(requirement, payload)
        data_classification = payload.get("data_classification", [])
        if not isinstance(data_classification, list) or not data_classification:
            data_classification = infer_data_classification(requirement, payload)
        data_classification = [str(x).strip().upper() for x in data_classification if str(x).strip()]

        default_persona_id = "senior-banking-analyst" if str(domain_pack.get("id", "")).startswith("banking") else "software-analyst"
        persona = self.persona_registry.get_persona(persona_id or default_persona_id, version=persona_version)
        if not persona:
            persona = {
                "id": default_persona_id,
                "version": "1.0.0",
                "name": "Analyst Persona",
                "role": "analyst",
                "system_prompt": "",
                "output_contract": {},
                "defaults": {},
            }

        thread_id = str(payload.get("thread_id", "")).strip() or "default-thread"
        memory_constraints = self.memory_store.search_constraints(scope, requirement, limit=10)
        memory_thread = self.memory_store.get_thread(scope, thread_id=thread_id, limit=24)

        state["requirement"] = requirement
        state["thread_id"] = thread_id
        state["persona"] = persona
        state["domain_pack"] = domain_pack
        state["normalized_requirement"] = normalized
        state["jurisdiction"] = jurisdiction or "GLOBAL"
        state["data_classification"] = data_classification or ["INTERNAL"]
        state["memory_constraints"] = memory_constraints
        state["memory_thread"] = memory_thread
        self._trace(
            state,
            "ingest",
            (
                f"domain_pack={domain_pack.get('id', '')}, persona={persona.get('id', '')}, "
                f"memory_constraints={len(memory_constraints)}"
            ),
        )
        return state

    def _node_query_graph(self, state: AnalystServiceState) -> AnalystServiceState:
        if state.get("errors"):
            return state
        capability_map = map_to_capabilities(
            state.get("domain_pack", {}),
            state.get("normalized_requirement", {}),
        )
        capability_ids = _extract_capability_ids(capability_map)
        edges = self.knowledge_gateway.query_capability_dependencies(
            state.get("domain_pack", {}),
            capability_ids,
        )
        state["capability_mapping"] = capability_map
        state["graph_edges"] = edges
        self._trace(state, "query_graph", f"capabilities={len(capability_ids)}, edges={len(edges)}")
        return state

    def _node_query_vector(self, state: AnalystServiceState) -> AnalystServiceState:
        if state.get("errors"):
            return state
        query = " ".join(
            [
                str(state.get("requirement", "")),
                " ".join([str(row.get("text", "")) for row in state.get("memory_constraints", []) if isinstance(row, dict)]),
            ]
        ).strip()
        vector_hits = self.knowledge_gateway.query_vector_context(
            query=query,
            domain_pack=state.get("domain_pack", {}),
            top_k=8,
        )
        state["vector_context"] = vector_hits
        self._trace(state, "query_vector", f"hits={len(vector_hits)}")
        return state

    def _node_query_compliance(self, state: AnalystServiceState) -> AnalystServiceState:
        if state.get("errors"):
            return state
        capability_ids = _extract_capability_ids(state.get("capability_mapping", {}))
        compliance = self.knowledge_gateway.query_regulatory_constraints(
            domain_pack=state.get("domain_pack", {}),
            capability_ids=capability_ids,
            jurisdiction=str(state.get("jurisdiction", "GLOBAL")),
            data_classes=state.get("data_classification", []),
        )
        state["compliance_constraints"] = compliance
        state["gold_patterns"] = retrieve_gold_patterns(state.get("domain_pack", {}), capability_ids=capability_ids)
        state["standards_guidance"] = _select_standards(state.get("domain_pack", {}), capability_ids=capability_ids)
        self._trace(state, "query_compliance", f"controls={len(compliance)}")
        return state

    def _synthesize_with_llm(self, state: AnalystServiceState) -> tuple[dict[str, Any], str, list[str]]:
        payload = state.get("payload", {})
        warnings: list[str] = []
        provider = str(payload.get("provider", "")).strip().lower()
        requested_model = str(payload.get("model", "")).strip()
        if provider not in {"anthropic", "openai"}:
            provider = str(
                self.settings_store.get_settings().get("llm", {}).get("default_provider", "anthropic")
            ).strip().lower()
            if provider not in {"anthropic", "openai"}:
                provider = "anthropic"

        creds = self.settings_store.resolve_llm_credentials(provider, requested_model=requested_model)
        api_key = str(creds.get("api_key", "")).strip()
        if not api_key:
            warnings.append(f"llm credentials unavailable for provider={provider}; using deterministic synthesis")
            return {}, "", warnings

        model = str(creds.get("model", "")).strip() or ("gpt-4o" if provider == "openai" else "claude-sonnet-4-20250514")
        config = PipelineConfig(
            provider=LLMProvider.OPENAI if provider == "openai" else LLMProvider.ANTHROPIC,
            anthropic_api_key=api_key if provider == "anthropic" else "",
            openai_api_key=api_key if provider == "openai" else "",
            anthropic_model=model if provider == "anthropic" else "claude-sonnet-4-20250514",
            openai_model=model if provider == "openai" else "gpt-4o",
            temperature=float(payload.get("temperature", 0.2) or 0.2),
        )

        agent = AnalystAgent(LLMClient(config))
        integration_context = payload.get("integration_context", {})
        if not isinstance(integration_context, dict):
            integration_context = {}
        integration_context = dict(integration_context)
        integration_context["domain_pack_id"] = str(state.get("domain_pack", {}).get("id", "")).strip()
        integration_context["jurisdiction"] = str(state.get("jurisdiction", "GLOBAL"))
        integration_context["data_classification"] = state.get("data_classification", [])
        if isinstance(state.get("domain_pack"), dict):
            integration_context["custom_domain_pack"] = state.get("domain_pack")

        agent_state: dict[str, Any] = {
            "business_objectives": str(state.get("requirement", "")),
            "use_case": str(payload.get("use_case", "business_objectives")),
            "integration_context": integration_context,
            "global_directives": [
                {
                    "id": row.get("id", ""),
                    "text": row.get("text", ""),
                    "priority": row.get("priority", "medium"),
                }
                for row in state.get("memory_constraints", [])
                if isinstance(row, dict)
            ],
            "context_bundle": payload.get("context_bundle", {}),
            "system_context_model": payload.get("system_context_model", {}),
            "convention_profile": payload.get("convention_profile", {}),
            "health_assessment": payload.get("health_assessment_bundle", {}),
            "agent_personas": {
                "1": {
                    "agent_id": str(state.get("persona", {}).get("id", "analyst")),
                    "display_name": str(state.get("persona", {}).get("name", "Analyst Persona")),
                    "persona": str(state.get("persona", {}).get("system_prompt", "")),
                    "requirements_pack_profile": str(
                        state.get("persona", {}).get("defaults", {}).get("requirements_pack_profile", "requirements-pack-v2-general")
                    ),
                }
            },
        }

        result = agent.run(agent_state)
        if result.status != "success":
            warnings.append(f"analyst llm synthesis returned status={result.status}; using deterministic synthesis")
            return {}, "", warnings
        output = result.output if isinstance(result.output, dict) else {}
        requirements_pack = output.get("requirements_pack", {}) if isinstance(output.get("requirements_pack", {}), dict) else {}
        summary = str(result.summary or "").strip()
        if not requirements_pack:
            warnings.append("analyst llm synthesis did not produce requirements_pack; using deterministic synthesis")
        return requirements_pack, summary, warnings

    def _node_synthesize(self, state: AnalystServiceState) -> AnalystServiceState:
        if state.get("errors"):
            return state
        warnings = list(state.get("warnings", [])) if isinstance(state.get("warnings", []), list) else []
        req_pack, summary, llm_warnings = self._synthesize_with_llm(state)
        warnings.extend(llm_warnings)
        if not isinstance(req_pack, dict) or not req_pack:
            req_pack = _deterministic_pack(state)
            summary = _deterministic_assistant_summary(str(state.get("requirement", "")), req_pack)
        elif not str(summary or "").strip():
            summary = _deterministic_assistant_summary(str(state.get("requirement", "")), req_pack)
        state["warnings"] = warnings
        state["requirements_pack"] = req_pack
        state["assistant_summary"] = summary
        self._trace(
            state,
            "synthesize",
            f"artifact_id={req_pack.get('artifact_id', '')}, warnings={len(warnings)}",
        )
        return state

    def _node_validate(self, state: AnalystServiceState) -> AnalystServiceState:
        if state.get("errors"):
            return state
        req_pack = state.get("requirements_pack", {})
        gates: list[dict[str, Any]] = []
        required_sections = [
            "project",
            "intake",
            "domain_mapping",
            "compliance",
            "requirements",
            "bdd",
            "open_questions",
            "traceability",
        ]
        missing = [key for key in required_sections if key not in req_pack]
        gates.append(
            {
                "name": "requirements_pack_sections",
                "status": "PASS" if not missing else "FAIL",
                "message": "Required sections are present." if not missing else f"Missing sections: {', '.join(missing)}",
            }
        )
        bdd_features = req_pack.get("bdd", {}).get("features", []) if isinstance(req_pack.get("bdd", {}), dict) else []
        gates.append(
            {
                "name": "bdd_presence",
                "status": "PASS" if isinstance(bdd_features, list) and len(bdd_features) > 0 else "FAIL",
                "message": "BDD features exist for downstream testing.",
            }
        )
        state["quality_gates"] = gates
        self._trace(
            state,
            "validate",
            f"failed={len([g for g in gates if str(g.get('status', '')).upper() == 'FAIL'])}",
        )
        return state

    def _build_graph(self):
        graph = StateGraph(AnalystServiceState)
        graph.add_node("ingest", self._node_ingest)
        graph.add_node("query_graph", self._node_query_graph)
        graph.add_node("query_vector", self._node_query_vector)
        graph.add_node("query_compliance", self._node_query_compliance)
        graph.add_node("synthesize", self._node_synthesize)
        graph.add_node("validate", self._node_validate)

        graph.set_entry_point("ingest")
        graph.add_edge("ingest", "query_graph")
        graph.add_edge("query_graph", "query_vector")
        graph.add_edge("query_vector", "query_compliance")
        graph.add_edge("query_compliance", "synthesize")
        graph.add_edge("synthesize", "validate")
        graph.add_edge("validate", END)
        return graph.compile()

    def analyze(self, payload: dict[str, Any], actor: str) -> dict[str, Any]:
        scope = {
            "workspace_id": str(payload.get("workspace_id", "default-workspace")),
            "client_id": str(payload.get("client_id", "default-client")),
            "project_id": str(payload.get("project_id", "default-project")),
        }
        thread_id = str(payload.get("thread_id", "")).strip() or "default-thread"
        requirement = str(payload.get("requirement") or payload.get("business_objective") or "").strip()
        initial: AnalystServiceState = {
            "payload": payload if isinstance(payload, dict) else {},
            "actor": str(actor or "local-user"),
            "scope": scope,
            "thread_id": thread_id,
            "requirement": requirement,
            "warnings": [],
            "errors": [],
            "trace": [],
        }
        final: AnalystServiceState = self._graph.invoke(initial)

        if requirement:
            self.memory_store.append_thread_message(
                scope,
                thread_id=thread_id,
                agent_role="analyst",
                role="user",
                message=requirement,
                metadata={"source": "api"},
            )

        assistant_summary = str(final.get("assistant_summary", "")).strip() or "Analysis completed."
        self.memory_store.append_thread_message(
            scope,
            thread_id=thread_id,
            agent_role="analyst",
            role="assistant",
            message=assistant_summary,
            metadata={"source": "api"},
        )

        constraints_to_save = payload.get("save_constraints", [])
        if isinstance(constraints_to_save, list):
            for row in constraints_to_save:
                text = str(row.get("text", "")) if isinstance(row, dict) else str(row)
                if not text.strip():
                    continue
                priority = str(row.get("priority", "medium")) if isinstance(row, dict) else "medium"
                self.memory_store.add_constraint(
                    scope,
                    text=text,
                    source="analyst_chat",
                    created_by=str(actor or "local-user"),
                    priority=priority,
                    applies_to=str(row.get("applies_to", "all")) if isinstance(row, dict) else "all",
                )

        return {
            "ok": len(final.get("errors", [])) == 0,
            "service": "analyst-aas",
            "service_version": "1.0.0",
            "run_id": f"analyst-aas-{uuid.uuid4().hex[:10]}",
            "generated_at": _utc_now(),
            "scope": scope,
            "thread_id": thread_id,
            "persona": final.get("persona", {}),
            "domain_pack": {
                "id": str(final.get("domain_pack", {}).get("id", "")),
                "name": str(final.get("domain_pack", {}).get("name", "")),
                "version": str(final.get("domain_pack", {}).get("version", "")),
            },
            "normalized_requirement": final.get("normalized_requirement", {}),
            "memory_constraints": final.get("memory_constraints", []),
            "graph_edges": final.get("graph_edges", []),
            "vector_context": final.get("vector_context", []),
            "compliance_constraints": final.get("compliance_constraints", []),
            "quality_gates": final.get("quality_gates", []),
            "assistant_summary": assistant_summary,
            "requirements_pack": final.get("requirements_pack", {}),
            "warnings": final.get("warnings", []),
            "errors": final.get("errors", []),
            "trace": final.get("trace", []),
        }
