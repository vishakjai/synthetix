"""
System Intelligence Layer (SIL) agent.

Produces canonical context artifacts:
  1. System Context Model (SCM)
  2. Convention Profile (CP)
  3. Health Assessment + Remediation Backlog (HA/RB)
"""

from __future__ import annotations

import json
from typing import Any

from .base import AgentResult, BaseAgent
from utils.context_vault import normalize_sil_output


class SystemIntelligenceAgent(BaseAgent):
    @property
    def name(self) -> str:
        return "System Intelligence Layer"

    @property
    def stage(self) -> int:
        return 0

    @property
    def system_prompt(self) -> str:
        return """You are the System Intelligence Layer (SIL) agent for enterprise software modernization.
Your output is a contract layer that downstream agents must depend on.

Return valid JSON in this exact shape:
{
  "system_context_model": {
    "version": "string",
    "summary": "string",
    "nodes": [
      {
        "id": "string",
        "type": "Service|Container|Component|Module|Package|Endpoint|Route|MessageTopic|ConsumerGroup|Database|Table|Column|InfraResource|ExternalDependency",
        "name": "string",
        "metadata": {},
        "confidence": 0.0,
        "provenance": [{"file": "string", "line": 1, "evidence": "string"}]
      }
    ],
    "edges": [
      {
        "type": "IMPORTS|CALLS_HTTP|PUBLISHES|CONSUMES|READS_TABLE|WRITES_TABLE|DEPLOYS_TO|OWNS_RESOURCE|RUNS_ON|DEPENDS_ON|USES_LIBRARY",
        "from": "node-id",
        "to": "node-id",
        "directionality": "directed|undirected",
        "protocol_metadata": {},
        "confidence": 0.0,
        "evidence": [{"file": "string", "line": 1, "evidence": "string"}]
      }
    ],
    "unknowns": ["string"]
  },
  "convention_profile": {
    "version": "string",
    "summary": "string",
    "rules": [
      {
        "id": "string",
        "category": "naming|error_handling|testing|logging|auth|api|security",
        "rule": "string",
        "confidence": 0.0,
        "examples": ["string"],
        "counterexamples": ["string"],
        "provenance": [{"file": "string", "line": 1, "evidence": "string"}]
      }
    ],
    "lint_recommendations": ["string"],
    "scaffold_templates": ["string"]
  },
  "health_assessment": {
    "version": "string",
    "summary": "string",
    "scores": {
      "maintainability": 0,
      "security": 0,
      "reliability": 0,
      "testability": 0
    },
    "hotspots": [
      {
        "scope": "string",
        "reason": "string",
        "severity": "high|medium|low",
        "confidence": 0.0,
        "provenance": [{"file": "string", "line": 1, "evidence": "string"}]
      }
    ],
    "risks": [
      {
        "title": "string",
        "description": "string",
        "severity": "critical|high|medium|low",
        "confidence": 0.0
      }
    ]
  },
  "remediation_backlog": [
    {
      "id": "string",
      "title": "string",
      "problem_statement": "string",
      "scope": ["string"],
      "risk_if_unaddressed": "string",
      "effort": "XS|S|M|L|XL",
      "severity": "critical|high|medium|low",
      "dependencies": ["string"],
      "suggested_approach": "string",
      "success_criteria": ["string"],
      "confidence": 0.0,
      "provenance": [{"file": "string", "line": 1, "evidence": "string"}]
    }
  ]
}

Rules:
- Provide confidence for every node/edge/rule/backlog item.
- Provide explicit provenance/evidence pointers where possible.
- Prefer precise, auditable, deterministic statements over generic advice.
- Respond ONLY with JSON."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        discovery = state.get("sil_discovery", {})
        objectives = state.get("business_objectives", "")
        use_case = state.get("use_case", "business_objectives")
        legacy = state.get("legacy_code", "")
        target_lang = state.get("modernization_language", "")
        db_source = state.get("database_source", "")
        db_target = state.get("database_target", "")
        db_schema = state.get("database_schema", "")
        discovery_prompt = self._compact_discovery_for_prompt(discovery if isinstance(discovery, dict) else {})

        return f"""Generate SIL contract artifacts from this repository/context.

BUSINESS OBJECTIVES:
{objectives}

USE CASE:
{use_case}

LEGACY CODE (if provided):
```text
{legacy[:12000]}
```

TARGET MODERNIZATION LANGUAGE:
{target_lang or "not specified"}

DATABASE CONVERSION CONTEXT:
- source: {db_source or "not specified"}
- target: {db_target or "not specified"}
```sql
{str(db_schema)[:12000]}
```

REPOSITORY DISCOVERY SNAPSHOT:
{json.dumps(discovery_prompt, indent=2)}

Produce SCM, CP, and HA/RB with actionable enterprise-grade detail and auditable provenance."""

    @staticmethod
    def _compact_discovery_for_prompt(discovery: dict[str, Any]) -> dict[str, Any]:
        static = discovery.get("static_analysis", {}) if isinstance(discovery.get("static_analysis", {}), dict) else {}
        import_graph = static.get("import_graph", {}) if isinstance(static.get("import_graph", {}), dict) else {}
        return {
            "repo_root": str(discovery.get("repo_root", "")),
            "scanned_files": int(discovery.get("scanned_files", 0) or 0),
            "content_fingerprint": str(discovery.get("content_fingerprint", "")),
            "language_counts": discovery.get("language_counts", {}) if isinstance(discovery.get("language_counts", {}), dict) else {},
            "file_samples": list(discovery.get("file_samples", []))[:80] if isinstance(discovery.get("file_samples", []), list) else [],
            "endpoint_hints": list(discovery.get("endpoint_hints", []))[:40] if isinstance(discovery.get("endpoint_hints", []), list) else [],
            "static_analysis": {
                "version": str(static.get("version", "sa-v1")),
                "adapters": list(static.get("adapters", []))[:10] if isinstance(static.get("adapters", []), list) else [],
                "stats": static.get("stats", {}) if isinstance(static.get("stats", {}), dict) else {},
                "modules_sample": list(static.get("modules", []))[:60] if isinstance(static.get("modules", []), list) else [],
                "import_edges_sample": list(import_graph.get("edges", []))[:120] if isinstance(import_graph.get("edges", []), list) else [],
                "route_surface_sample": list(static.get("route_surface", []))[:80] if isinstance(static.get("route_surface", []), list) else [],
                "config_artifacts_sample": list(static.get("config_artifacts", []))[:60] if isinstance(static.get("config_artifacts", []), list) else [],
                "infra_resources_sample": list(static.get("infra_resources", []))[:60] if isinstance(static.get("infra_resources", []), list) else [],
                "parse_errors_sample": list(static.get("parse_errors", []))[:20] if isinstance(static.get("parse_errors", []), list) else [],
            },
        }

    def parse_output(self, raw: str) -> dict[str, Any]:
        parsed = self.extract_json(raw)
        return normalize_sil_output(parsed, {})

    def run(self, state: dict[str, Any]) -> AgentResult:
        self._logs = []
        self.log(f"[{self.name}] Starting SIL generation...")
        discovery = state.get("sil_discovery", {})
        try:
            response = self.llm.invoke(self.effective_system_prompt(state), self.build_user_message(state))
            parsed = self.extract_json(response.content)
            normalized = normalize_sil_output(parsed, discovery if isinstance(discovery, dict) else {})
            self.log(f"[{self.name}] SIL artifacts generated from LLM response")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="success",
                summary=self._build_summary(normalized),
                output=normalized,
                raw_response=response.content,
                tokens_used=response.input_tokens + response.output_tokens,
                latency_ms=response.latency_ms,
                logs=self._logs.copy(),
            )
        except Exception as exc:
            self.log(f"[{self.name}] LLM parse failed, building deterministic SIL fallback: {exc}")
            fallback = normalize_sil_output({}, discovery if isinstance(discovery, dict) else {})
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="warning",
                summary=f"{self._build_summary(fallback)} (fallback)",
                output=fallback,
                raw_response="",
                tokens_used=0,
                latency_ms=0.0,
                logs=self._logs.copy(),
            )

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        scm = parsed.get("system_context_model", {})
        cp = parsed.get("convention_profile", {})
        rb = parsed.get("remediation_backlog", [])
        return (
            f"SIL ready: {len(scm.get('nodes', []))} nodes, "
            f"{len(scm.get('edges', []))} edges, "
            f"{len(cp.get('rules', []))} conventions, "
            f"{len(rb)} remediation items"
        )
