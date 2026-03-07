"""
Agent 2: Architect Agent
Designs system architecture optimized for latency, security, and scalability.
"""

from __future__ import annotations

from typing import Any
import json
import re

from .base import AgentResult, BaseAgent


class ArchitectAgent(BaseAgent):

    @property
    def name(self) -> str:
        return "Architect Agent"

    @property
    def stage(self) -> int:
        return 2

    @property
    def system_prompt(self) -> str:
        return """You are a Principal Software Architect Agent in a development pipeline.
You receive structured requirements from the Analyst Agent and design a complete
system architecture optimized for latency, security, and scalability.

You MUST respond with valid JSON in this exact structure:
{
  "architecture_name": "string",
  "pattern": "microservices|modular-monolith|monolith|serverless|event-driven|hybrid",
  "overview": "2-3 sentence architecture overview",
  "services": [
    {
      "name": "string",
      "responsibility": "string",
      "technology": "string",
      "language": "string",
      "framework": "string",
      "api_type": "REST|GraphQL|gRPC|WebSocket",
      "database": "string or null",
      "cache": "string or null"
    }
  ],
  "infrastructure": {
    "cloud_provider": "string",
    "container_orchestration": "string",
    "ci_cd": "string",
    "monitoring": "string",
    "logging": "string"
  },
  "security": {
    "authentication": "string",
    "authorization": "string",
    "encryption": "string",
    "api_security": "string",
    "secrets_management": "string"
  },
  "scalability": {
    "strategy": "string",
    "auto_scaling_rules": ["string", ...],
    "caching_strategy": "string",
    "cdn": "string or null"
  },
  "data_flow": [
    {
      "from": "string",
      "to": "string",
      "protocol": "string",
      "description": "string"
    }
  ],
  "latency_optimizations": ["string", ...],
  "trade_offs": [
    {
      "decision": "string",
      "rationale": "string",
      "alternatives_considered": ["string", ...]
    }
  ],
  "legacy_system": {
    "current_logic_summary": "string",
    "key_logic_steps": ["string", ...],
    "current_system_diagram_mermaid": "string (mermaid graph)"
  },
  "target_system_diagram_mermaid": "string (mermaid graph of modernized architecture)"
}

Design for production-grade systems. Be specific about technology choices.
Do NOT default to AWS or microservices unless requirements explicitly justify it.
Prefer local Docker-compatible architecture for MVP unless the user requests cloud.
Use-case behavior:
- For `code_modernization`, include a detailed `legacy_system` section and both current + target diagrams.
- For `business_objectives`, `legacy_system` is optional and target architecture is primary.
Depth requirements:
- Include at least 4 data_flow entries.
- Include at least 4 latency_optimizations.
- Include at least 3 trade_offs with explicit alternatives considered.
- If `legacy_system` is present, include at least 5 key_logic_steps.
Respond ONLY with the JSON, no other text."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        requirements = state.get("analyst_output", {})
        use_case = str(state.get("use_case", "business_objectives")).strip().lower()
        legacy_code = state.get("legacy_code", "")
        target_lang = state.get("modernization_language", "")
        db_source = str(state.get("database_source", "")).strip()
        db_target = str(state.get("database_target", "")).strip()
        db_schema = str(state.get("database_schema", "")).strip()
        deployment_target = state.get("deployment_target", "local")
        legacy_mode = use_case == "code_modernization"
        diagram_instructions = (
            "Include both a current-system (legacy) Mermaid diagram and a target-system Mermaid diagram."
            if legacy_mode
            else "Focus on the target architecture Mermaid diagram; omit legacy/current diagram unless truly needed for context."
        )
        requirements_compact = self._json_for_prompt(
            requirements,
            max_chars=7000,
            max_depth=4,
            max_items=12,
            max_str=420,
        )
        legacy_chunks = self._chunk_text_for_prompt(
            legacy_code,
            chunk_chars=1800,
            max_chunks=4,
        )
        legacy_sections = (
            "\n\n".join(
                [
                    f"LEGACY CODE CHUNK {idx + 1}/{len(legacy_chunks)}:\n```text\n{chunk}\n```"
                    for idx, chunk in enumerate(legacy_chunks)
                ]
            )
            if legacy_chunks
            else "No inline legacy code provided."
        )
        db_schema_excerpt = self._truncate_text(db_schema, max_chars=1800)
        return f"""Design a system architecture for the following requirements.
Optimize for latency, security, and scalability.

USE CASE:
{use_case}

REQUIREMENTS DOCUMENT:
{requirements_compact}

LEGACY CODE CONTEXT:
{legacy_sections}

MODERNIZATION TARGET LANGUAGE:
{target_lang or "Not specified"}

DATABASE CONVERSION CONTEXT:
- Source engine: {db_source or "Not specified"}
- Target engine: {db_target or "Not specified"}
```sql
{db_schema_excerpt}
```

DEPLOYMENT TARGET PREFERENCE:
{deployment_target}

Include:
- The target architecture and a NON-EMPTY Mermaid diagram.
- {diagram_instructions}
- Any Mermaid diagram provided MUST be valid syntax starting with "graph TD;".
- If deployment target is local, prefer Docker-local compatible services over cloud-managed dependencies."""

    def parse_output(self, raw: str) -> dict[str, Any]:
        parsed = self.extract_json(raw)
        return self._ensure_required_diagrams(parsed)

    def _deterministic_fallback(self, state: dict[str, Any], raw_response: str, parse_error: Exception) -> dict[str, Any]:
        analyst = state.get("analyst_output", {}) if isinstance(state.get("analyst_output", {}), dict) else {}
        analyst_inventory = analyst.get("legacy_code_inventory", {}) if isinstance(analyst.get("legacy_code_inventory", {}), dict) else {}
        project_candidates = analyst_inventory.get("vb6_projects", []) if isinstance(analyst_inventory.get("vb6_projects", []), list) else []
        project_name = next(
            (
                str(item.get("name", "")).strip()
                for item in project_candidates
                if isinstance(item, dict) and str(item.get("name", "")).strip()
            ),
            "",
        ) or str(analyst.get("project_name", "")).strip() or "Legacy Modernization Architecture"
        services = [
            {
                "name": "modernization-app",
                "responsibility": "Delivers the modernized business workflows and parity behavior for the legacy application.",
                "technology": "Containerized application service",
                "language": str(state.get("modernization_language", "")).strip() or "C#",
                "framework": "ASP.NET Core",
                "api_type": "REST",
                "database": str(state.get("database_target", "")).strip() or "PostgreSQL",
                "cache": "Redis",
            },
            {
                "name": "migration-worker",
                "responsibility": "Executes background migration, reconciliation, and batch modernization tasks.",
                "technology": "Containerized worker",
                "language": str(state.get("modernization_language", "")).strip() or "C#",
                "framework": ".NET Worker",
                "api_type": "REST",
                "database": str(state.get("database_target", "")).strip() or "PostgreSQL",
                "cache": None,
            },
        ]
        parsed = {
            "architecture_name": f"{project_name} Target Architecture",
            "pattern": "modular-monolith",
            "overview": (
                "Deterministic architecture fallback compiled from Analyst evidence because the primary Architect model response was not machine-readable. "
                "The design favors a Docker-compatible modular application with explicit data and worker boundaries."
            ),
            "services": services,
            "infrastructure": {
                "cloud_provider": "Docker-local / cloud-portable",
                "container_orchestration": "Docker Compose (initial) / Kubernetes-ready",
                "ci_cd": "GitHub Actions",
                "monitoring": "OpenTelemetry + Prometheus",
                "logging": "Structured JSON logs",
            },
            "security": {
                "authentication": "Application login backed by centralized identity model to be confirmed during design review",
                "authorization": "Role-based access control",
                "encryption": "TLS in transit and encrypted database/storage at rest",
                "api_security": "Parameterized queries, validated inputs, and authenticated API access",
                "secrets_management": "Environment-injected secrets with managed secret store support",
            },
            "scalability": {
                "strategy": "Scale stateless application and worker services independently",
                "auto_scaling_rules": [
                    "Scale application instances on request concurrency and response latency.",
                    "Scale worker instances on queue depth and reconciliation backlog.",
                    "Keep database capacity aligned to transaction workload and reporting windows.",
                    "Enable separate scaling for reporting/export workloads if retained in scope.",
                ],
                "caching_strategy": "Use short-lived cache for reference/master data and read-heavy lookup flows",
                "cdn": None,
            },
            "data_flow": [
                {
                    "from": "Client",
                    "to": "modernization-app",
                    "protocol": "HTTPS",
                    "description": "Users access modernized application workflows over authenticated HTTP requests.",
                },
                {
                    "from": "modernization-app",
                    "to": str(state.get("database_target", "")).strip() or "PostgreSQL",
                    "protocol": "SQL",
                    "description": "Application service persists transactional and reference data through parameterized data access.",
                },
                {
                    "from": "modernization-app",
                    "to": "migration-worker",
                    "protocol": "Queue/API",
                    "description": "Long-running migration, reconciliation, and reporting preparation tasks are delegated asynchronously.",
                },
                {
                    "from": "migration-worker",
                    "to": str(state.get("database_target", "")).strip() or "PostgreSQL",
                    "protocol": "SQL",
                    "description": "Worker processes backfill, reconciliation, and parity-validation workloads against the target datastore.",
                },
            ],
            "latency_optimizations": [
                "Keep UI/API services stateless to allow horizontal scaling.",
                "Cache stable reference data and account-type lookups.",
                "Move heavy reporting or reconciliation work to background workers.",
                "Use explicit database indexes for transaction, customer, and account lookups.",
            ],
            "trade_offs": [
                {
                    "decision": "Use a modular monolith first",
                    "rationale": "The legacy estate is tightly coupled and needs parity-first modernization before service decomposition.",
                    "alternatives_considered": ["Microservices", "Lift-and-shift monolith"],
                },
                {
                    "decision": "Prefer Docker-local compatible infrastructure",
                    "rationale": "The platform currently emphasizes local validation and controlled promotion to cloud environments.",
                    "alternatives_considered": ["Cloud-managed PaaS only", "Bare metal deployment"],
                },
                {
                    "decision": "Separate worker processing from interactive application traffic",
                    "rationale": "Background migration and reporting tasks should not impact user-facing latency.",
                    "alternatives_considered": ["Single process with internal scheduler", "Synchronous processing"],
                },
            ],
            "legacy_system": {
                "current_logic_summary": "Legacy VB6 project variants implement tightly coupled UI, reporting, and data-access behavior that must be modernized with controlled parity checks.",
                "key_logic_steps": [
                    "User authenticates into the legacy desktop workflow.",
                    "Menu or startup forms route the user to business transaction screens.",
                    "Business actions trigger form-bound validation and database access patterns.",
                    "Reporting and shared module logic execute alongside interactive workflows.",
                    "Operational state is maintained across multiple VB6 project variants and shared dependencies.",
                ],
                "current_system_diagram_mermaid": self._legacy_default_diagram(),
            },
            "target_system_diagram_mermaid": self._target_default_diagram(),
            "fallback_reason": str(parse_error),
            "raw_response_excerpt": str(raw_response or "")[:2000],
        }
        return self._ensure_required_diagrams(parsed)

    def run(self, state: dict[str, Any]) -> AgentResult:
        self._logs = []
        self.log(f"[{self.name}] Starting execution...")

        user_msg = self.build_user_message(state)
        self.log(f"[{self.name}] Sending request to LLM ({self.llm.config.get_model()})...")
        raw_response = ""

        try:
            response = self.llm.invoke(self.effective_system_prompt(state), user_msg)
            raw_response = str(response.content or "")
            self.log(f"[{self.name}] Received response ({response.output_tokens} tokens, {response.latency_ms:.0f}ms)")
            try:
                parsed = self.parse_output(raw_response)
            except Exception as parse_exc:
                self.log(f"[{self.name}] Structured parse failed; compiling deterministic fallback: {parse_exc}")
                parsed = self._deterministic_fallback(state, raw_response, parse_exc)
            self.log(f"[{self.name}] Output parsed successfully")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="success",
                summary=self._build_summary(parsed),
                output=parsed,
                raw_response=raw_response,
                tokens_used=response.input_tokens + response.output_tokens,
                latency_ms=response.latency_ms,
                logs=self._logs.copy(),
            )
        except Exception as e:
            self.log(f"[{self.name}] ERROR: {e}")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary=f"Agent failed: {e}",
                output={"error": str(e)},
                raw_response=raw_response,
                logs=self._logs.copy(),
            )

    @staticmethod
    def _first_non_empty(*values: Any) -> str:
        for value in values:
            text = str(value or "").strip()
            if text:
                return text
        return ""

    @staticmethod
    def _safe_node_id(label: str, idx: int) -> str:
        base = re.sub(r"[^a-zA-Z0-9]+", "_", str(label or "").strip()).strip("_").lower()
        if not base:
            base = "node"
        return f"{base}_{idx}"

    @staticmethod
    def _normalize_mermaid(diagram: str, default_diagram: str) -> str:
        text = str(diagram or "").strip()
        if not text:
            return default_diagram
        lower = text.lower()
        if not (lower.startswith("graph ") or lower.startswith("flowchart ")):
            return default_diagram
        return text

    @staticmethod
    def _legacy_default_diagram() -> str:
        return (
            "graph TD;\n"
            '    A["Legacy Input"] --> B["Legacy Processing"];\n'
            '    B --> C["Legacy Output"];'
        )

    @staticmethod
    def _target_default_diagram() -> str:
        return (
            "graph TD;\n"
            '    U["Client"] --> A["Application Service"];\n'
            '    A --> D["Database"];'
        )

    def _build_legacy_diagram(self, parsed: dict[str, Any], legacy: dict[str, Any]) -> str:
        steps_raw = legacy.get("key_logic_steps", [])
        steps = [str(s).strip() for s in steps_raw if str(s).strip()] if isinstance(steps_raw, list) else []
        if not steps:
            summary = str(legacy.get("current_logic_summary", "")).strip()
            if summary:
                steps = [summary]
        if not steps:
            return self._legacy_default_diagram()

        lines = ["graph TD;"]
        prev_id = "start_0"
        lines.append('    start_0["Legacy Entry"] --> step_1;')
        for idx, step in enumerate(steps, start=1):
            node_id = f"step_{idx}"
            step_text = step.replace('"', "'")
            lines.append(f'    {node_id}["{step_text}"];')
            if idx > 1:
                lines.append(f"    step_{idx - 1} --> {node_id};")
            prev_id = node_id
        lines.append(f'    {prev_id} --> end_0["Legacy Response"];')
        return "\n".join(lines)

    def _build_target_diagram(self, parsed: dict[str, Any]) -> str:
        services = parsed.get("services", [])
        data_flow = parsed.get("data_flow", [])

        lines = ["graph TD;"]
        node_ids: dict[str, str] = {}

        def ensure_node(label: str) -> str:
            key = str(label or "").strip() or "Unknown"
            existing = node_ids.get(key)
            if existing:
                return existing
            node_id = self._safe_node_id(key, len(node_ids) + 1)
            safe_label = key.replace('"', "'")
            lines.append(f'    {node_id}["{safe_label}"];')
            node_ids[key] = node_id
            return node_id

        user_node = ensure_node("Client")
        service_names: list[str] = []
        if isinstance(services, list):
            for svc in services:
                if not isinstance(svc, dict):
                    continue
                name = str(svc.get("name", "")).strip()
                if name:
                    service_names.append(name)
                    service_node = ensure_node(name)
                    lines.append(f"    {user_node} --> {service_node};")
                    db = str(svc.get("database", "")).strip()
                    cache = str(svc.get("cache", "")).strip()
                    if db and db.lower() != "null":
                        lines.append(f"    {service_node} --> {ensure_node(db)};")
                    if cache and cache.lower() != "null":
                        lines.append(f"    {service_node} --> {ensure_node(cache)};")

        if isinstance(data_flow, list):
            for flow in data_flow:
                if not isinstance(flow, dict):
                    continue
                src = str(flow.get("from", "")).strip()
                dst = str(flow.get("to", "")).strip()
                if not src or not dst:
                    continue
                src_node = ensure_node(src)
                dst_node = ensure_node(dst)
                lines.append(f"    {src_node} --> {dst_node};")

        if len(lines) <= 1:
            if service_names:
                app_node = ensure_node(service_names[0])
            else:
                app_node = ensure_node("Application Service")
            lines.append(f"    {user_node} --> {app_node};")
            lines.append(f"    {app_node} --> {ensure_node('Database')};")

        return "\n".join(lines)

    def _ensure_required_diagrams(self, parsed: dict[str, Any]) -> dict[str, Any]:
        legacy = parsed.get("legacy_system", {})
        if not isinstance(legacy, dict):
            legacy = {}

        legacy_steps = legacy.get("key_logic_steps", []) if isinstance(legacy.get("key_logic_steps", []), list) else []
        has_legacy_signal = any(str(s).strip() for s in legacy_steps) or any(
            str(v).strip()
            for v in [
                legacy.get("current_logic_summary", ""),
                legacy.get("current_system_diagram_mermaid", ""),
                legacy.get("legacy_diagram_mermaid", ""),
                legacy.get("diagram_mermaid", ""),
                parsed.get("legacy_system_diagram_mermaid", ""),
                parsed.get("current_system_diagram_mermaid", ""),
            ]
        )
        if has_legacy_signal:
            legacy_diagram = self._first_non_empty(
                legacy.get("current_system_diagram_mermaid"),
                legacy.get("legacy_diagram_mermaid"),
                legacy.get("diagram_mermaid"),
                parsed.get("legacy_system_diagram_mermaid"),
                parsed.get("current_system_diagram_mermaid"),
            )
            if not legacy_diagram:
                legacy_diagram = self._build_legacy_diagram(parsed, legacy)
                self.log(f"[{self.name}] Missing legacy diagram from LLM output; generated fallback diagram.")
            legacy["current_system_diagram_mermaid"] = self._normalize_mermaid(
                legacy_diagram,
                self._legacy_default_diagram(),
            )
            parsed["legacy_system"] = legacy
        else:
            parsed.pop("legacy_system", None)

        target_diagram = self._first_non_empty(
            parsed.get("target_system_diagram_mermaid"),
            parsed.get("target_architecture_diagram_mermaid"),
            parsed.get("target_diagram_mermaid"),
            parsed.get("architecture_diagram_mermaid"),
        )
        if not target_diagram:
            target_diagram = self._build_target_diagram(parsed)
            self.log(f"[{self.name}] Missing target diagram from LLM output; generated fallback diagram.")
        parsed["target_system_diagram_mermaid"] = self._normalize_mermaid(
            target_diagram,
            self._target_default_diagram(),
        )
        return parsed

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        pattern = parsed.get("pattern", "unknown")
        svc_count = len(parsed.get("services", []))
        infra = parsed.get("infrastructure", {})
        cloud = infra.get("cloud_provider", "N/A")
        return (
            f"{pattern.title()} architecture with {svc_count} services "
            f"on {cloud}"
        )
