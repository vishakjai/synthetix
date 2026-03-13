"""
Agent 2: Architect Agent
Designs system architecture optimized for latency, security, and scalability.
"""

from __future__ import annotations

from typing import Any
from collections import defaultdict
import json
import re

from .base import AgentResult, BaseAgent
from utils.architect_handoff import build_architect_handoff_package


class ArchitectAgent(BaseAgent):

    @staticmethod
    def _safe_mermaid_text(value: Any) -> str:
        return str(value or "").replace('"', "'").strip()

    @property
    def name(self) -> str:
        return "Architect Agent"

    @property
    def stage(self) -> int:
        return 2

    @property
    def system_prompt(self) -> str:
        return """You are a Principal Software Architect Agent in a development pipeline.
You receive structured requirements from the Analyst Agent and produce an Architect Package:
decision-grounded, traceability-linked, migration-aware architectural artifacts for brownfield modernization.

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
  "target_system_diagram_mermaid": "string (mermaid graph of modernized architecture)",
  "architect_package": {
    "package_meta": {
      "schema_version": "1.0",
      "status": "COMPLETE|WARN|HALTED",
      "warnings": ["string", "..."],
      "artifact_count": 7
    },
    "artifacts": {
      "data_ownership_matrix": {},
      "coupling_heatmap": {},
      "architecture_decision_records": [],
      "traceability_matrix": {},
      "api_contract_sketches": {},
      "strangler_migration_plan": {},
      "component_risk_register": {}
    },
    "estimation_handoff": {},
    "human_review_queue": []
  }
}

Architect behavior rules:
- Preserve the existing top-level compatibility fields because downstream stages consume them.
- The real architectural reasoning belongs in architect_package.
- Do NOT produce a 1:1 form-to-service rename without justification.
- Every major boundary needs rationale or an ADR.
- Every source module must appear in the traceability matrix; never silently omit a module.
- Before producing the architect_package, explicitly consume Analyst evidence from:
  - SQL catalog for entity discovery and data ownership
  - global module/shared-state inventory for boundary and ownership conflicts
  - static risk detectors for NFR and transaction-management constraints
  - dead form/reference findings for scope adjudication and duplicate cleanup
  - business rule catalogs/BRD rules for extracted_business_rules
  - golden flows and flow traces for regression anchors
- Include at least one rejected alternative per ADR.
- Emit explicit warnings/human review items for low-confidence mappings, shared state conflicts, or ownership conflicts.
- Use data-grounded confidence and risk signals. If evidence is missing, say so explicitly instead of inventing confidence.
- If a service owns stateful workflows but no data entities can be assigned, mark the package WARN or HALTED rather than emitting an empty data ownership section.
- Do not emit mutating contracts as GET. Include request/response shapes, auth expectations, and error contracts for every API contract sketch.
- For `code_modernization`, include a detailed `legacy_system` section and both current + target diagrams.
- For `business_objectives`, `legacy_system` is optional and target architecture is primary.
- Include at least 4 data_flow entries.
- Include at least 4 latency_optimizations.
- Include at least 3 trade_offs with explicit alternatives considered.
- If `legacy_system` is present, include at least 5 key_logic_steps.
- Preserve distinct legacy workflow boundaries. Do not describe deposit, withdrawal, reporting, customer management, navigation, authentication, or startup flows with the same generic wording.
- If the Analyst evidence identifies named workflows or forms, reflect those as separate responsibilities or legacy logic steps instead of collapsing them into a generic hub.
- Do not invent a navigation-hub description for non-navigation modules.
- When evidence is incomplete, say that the legacy behavior requires validation rather than masking the gap with boilerplate.
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
- If deployment target is local, prefer Docker-local compatible services over cloud-managed dependencies.
- Keep the architecture narrative aligned to the concrete legacy workflows surfaced by the Analyst output; avoid repeating the same generalized workflow text across different legacy modules."""

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
        return self._normalize_output(self._ensure_required_diagrams(parsed), state)

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
                parsed = self._normalize_output(parsed, state)
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

    def _normalize_output(self, parsed: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        package = self._build_architect_package(state, parsed)
        parsed["architect_package"] = package

        services = self._build_top_level_services(package, state)
        if services:
            parsed["services"] = services

        parsed["architect_handoff_package"] = build_architect_handoff_package(state, parsed, package)

        if not str(parsed.get("pattern", "")).strip():
            parsed["pattern"] = "modular-monolith"

        if not str(parsed.get("architecture_name", "")).strip():
            analyst = state.get("analyst_output", {}) if isinstance(state.get("analyst_output", {}), dict) else {}
            project_name = str(analyst.get("project_name", "")).strip() or "Legacy Modernization"
            parsed["architecture_name"] = f"{project_name} Target Architecture"

        generated_overview = self._build_architecture_overview(package)
        if not str(parsed.get("overview", "")).strip():
            parsed["overview"] = generated_overview

        parsed["trade_offs"] = self._normalize_tradeoffs(parsed.get("trade_offs"), package)
        parsed["data_flow"] = self._normalize_data_flow(parsed.get("data_flow"), package, services)
        parsed["latency_optimizations"] = self._normalize_latency_optimizations(parsed.get("latency_optimizations"), package)
        parsed["infrastructure"] = self._normalize_infrastructure(parsed.get("infrastructure"), state)
        parsed["security"] = self._normalize_security(parsed.get("security"), state)
        parsed["scalability"] = self._normalize_scalability(parsed.get("scalability"))

        target_diagram = self._build_target_diagram_from_package(package, services)
        parsed["target_system_diagram_mermaid"] = self._normalize_mermaid(
            target_diagram,
            self._target_default_diagram(),
        )
        if isinstance(parsed.get("legacy_system"), dict):
            parsed["legacy_system"]["current_system_diagram_mermaid"] = self._normalize_mermaid(
                str(parsed.get("legacy_system", {}).get("current_system_diagram_mermaid", "")).strip() or self._build_legacy_diagram_from_package(package),
                self._legacy_default_diagram(),
            )
        return parsed

    def _raw_analyst_evidence(self, state: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
        analyst = state.get("analyst_output", {}) if isinstance(state.get("analyst_output", {}), dict) else {}
        raw = analyst.get("raw_artifacts", {}) if isinstance(analyst.get("raw_artifacts", {}), dict) else {}
        pack = analyst.get("requirements_pack", {}) if isinstance(analyst.get("requirements_pack", {}), dict) else {}
        legacy = analyst.get("legacy_code_inventory", {}) if isinstance(analyst.get("legacy_code_inventory", {}), dict) else {}
        if not legacy and isinstance(pack.get("legacy_code_inventory", {}), dict):
            legacy = pack.get("legacy_code_inventory", {})
        return analyst, raw, legacy

    @staticmethod
    def _as_list(value: Any) -> list[Any]:
        return value if isinstance(value, list) else []

    @staticmethod
    def _as_dict(value: Any) -> dict[str, Any]:
        return value if isinstance(value, dict) else {}

    @staticmethod
    def _normalize_name(value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "", str(value or "").lower())

    @staticmethod
    def _titleize_identifier(value: str) -> str:
        text = re.sub(r"^(frm|mod|mdi)", "", str(value or ""), flags=re.IGNORECASE)
        text = re.sub(r"([a-z0-9])([A-Z])", r"\1 \2", text)
        text = re.sub(r"[_\-]+", " ", text)
        return " ".join(token.capitalize() for token in text.split()) or str(value or "Module")

    def _classify_service(self, module_name: str, purpose: str, tables: list[str]) -> tuple[str, str]:
        corpus = " ".join([module_name, purpose, " ".join(tables)]).lower()
        if any(token in corpus for token in ["login", "auth", "password", "user", "session"]):
            return "AuthenticationService", "Owns authentication, session bootstrap, and identity validation flows."
        if any(token in corpus for token in ["mdi", "menu", "startup", "splash", "shell", "navigation"]):
            return "ExperienceShell", "Owns startup orchestration and navigation routing for the modernized client experience."
        if any(token in corpus for token in ["report", "statement", "export", "ledger"]):
            return "ReportingService", "Owns statements, reports, exports, and read-optimized reporting queries."
        if any(token in corpus for token in ["deposit", "withdraw", "transaction", "balance", "payment", "loan"]):
            return "TransactionService", "Owns transactional workflows, balance movements, and write-heavy financial operations."
        if any(token in corpus for token in ["account type", "settings", "reference", "lookup", "config", "master data"]):
            return "ReferenceDataService", "Owns reference data, settings, lookup configuration, and operational parameters."
        if any(token in corpus for token in ["customer", "account", "profile", "closeaccount", "close account"]):
            return "CustomerService", "Owns customer lifecycle, account profile data, and customer-facing account maintenance."
        return "LegacyCoreService", "Owns residual shared legacy logic that still needs explicit decomposition decisions."

    def _module_match_tokens(self, module: dict[str, Any]) -> set[str]:
        tokens = {
            self._normalize_name(module.get("source_module", "")),
            self._normalize_name(module.get("base_name", "")),
            self._normalize_name(module.get("source_file", "")),
        }
        return {token for token in tokens if token}

    def _row_matches_module(self, row: dict[str, Any], module: dict[str, Any]) -> bool:
        row_tokens = {
            self._normalize_name(row.get("form") or row.get("form_name") or row.get("module") or row.get("module_name") or row.get("source_file") or row.get("path") or "")
        }
        row_tokens = {token for token in row_tokens if token}
        return bool(self._module_match_tokens(module) & row_tokens)

    def _collect_source_modules(self, state: dict[str, Any]) -> list[dict[str, Any]]:
        _, raw, legacy = self._raw_analyst_evidence(state)
        dossiers = self._as_list(self._as_dict(raw.get("form_dossier")).get("dossiers"))
        risk_rows = self._as_list(self._as_dict(raw.get("risk_register")).get("risks"))
        rule_rows = self._as_list(self._as_dict(raw.get("business_rule_catalog")).get("rules"))
        sql_rows = self._as_list(self._as_dict(raw.get("sql_catalog")).get("statements"))
        loc_rows = self._as_list(legacy.get("source_loc_by_file"))
        loc_by_path = {
            str(row.get("path", "")).replace("\\", "/").split("/")[-1].strip().lower(): int(row.get("loc", 0) or 0)
            for row in loc_rows if isinstance(row, dict) and str(row.get("path", "")).strip()
        }
        modules: list[dict[str, Any]] = []
        seen: set[str] = set()

        for row in dossiers:
            if not isinstance(row, dict):
                continue
            source_module = str(row.get("form_name") or row.get("base_form_name") or "").strip()
            if not source_module:
                continue
            base_name = source_module.split("::")[-1]
            source_file = str(row.get("source_file", "")).strip() or f"{base_name}.frm"
            tables: set[str] = set(str(t).strip() for t in self._as_list(row.get("db_tables")) if str(t).strip())
            matching_sql = [sql for sql in sql_rows if isinstance(sql, dict) and self._row_matches_module(sql, {"source_module": source_module, "base_name": base_name, "source_file": source_file})]
            for sql in matching_sql:
                for table in self._as_list(sql.get("tables")):
                    txt = str(table).strip()
                    if txt:
                        tables.add(txt)
            matching_risks = [risk for risk in risk_rows if isinstance(risk, dict) and self._row_matches_module(risk, {"source_module": source_module, "base_name": base_name, "source_file": source_file})]
            matching_rules = [rule for rule in rule_rows if isinstance(rule, dict) and self._row_matches_module(rule, {"source_module": source_module, "base_name": base_name, "source_file": source_file})]
            service_name, service_desc = self._classify_service(base_name, str(row.get("purpose") or row.get("business_use") or ""), sorted(tables))
            normalized_key = self._normalize_name(source_module) or self._normalize_name(source_file)
            if not normalized_key or normalized_key in seen:
                continue
            seen.add(normalized_key)
            loc_value = int(row.get("source_loc", 0) or 0) or int(loc_by_path.get(source_file.split("/")[-1].lower(), 0) or 0)
            modules.append(
                {
                    "source_module": source_module,
                    "base_name": base_name,
                    "display_name": self._titleize_identifier(base_name),
                    "source_file": source_file,
                    "project_name": str(row.get("project_name", "")).strip(),
                    "module_type": str(row.get("form_type") or row.get("type") or "VB6Form").strip() or "VB6Form",
                    "purpose": str(row.get("purpose") or row.get("business_use") or f"Legacy workflow handled by {base_name}.").strip(),
                    "loc": loc_value,
                    "coverage_score": float(row.get("coverage_score", 0.0) or 0.0),
                    "confidence_score": float(row.get("confidence_score", 0.0) or 0.0),
                    "tables": sorted(tables),
                    "risk_ids": [str(r.get("risk_id") or r.get("id") or "").strip() for r in matching_risks if str(r.get("risk_id") or r.get("id") or "").strip()],
                    "rule_ids": [str(r.get("rule_id") or r.get("id") or "").strip() for r in matching_rules if str(r.get("rule_id") or r.get("id") or "").strip()],
                    "service_name": service_name,
                    "service_responsibility": service_desc,
                    "shared_state_dependencies": [],
                }
            )

        bas_modules = self._as_list(self._as_dict(legacy.get("bas_module_summary")).get("modules"))
        for entry in bas_modules:
            path = str(entry or "").strip()
            if not path:
                continue
            base = path.replace("\\", "/").split("/")[-1].rsplit(".", 1)[0]
            normalized_key = self._normalize_name(base)
            if not normalized_key or normalized_key in seen:
                continue
            seen.add(normalized_key)
            service_name, service_desc = self._classify_service(base, base, [])
            modules.append(
                {
                    "source_module": base,
                    "base_name": base,
                    "display_name": self._titleize_identifier(base),
                    "source_file": path,
                    "project_name": "",
                    "module_type": "Module",
                    "purpose": f"Shared legacy module {base} supporting {service_name}.",
                    "loc": int(loc_by_path.get(path.replace('\\', '/').split('/')[-1].lower(), 0) or 0),
                    "coverage_score": 0.5,
                    "confidence_score": 0.55,
                    "tables": [],
                    "risk_ids": [],
                    "rule_ids": [],
                    "service_name": service_name,
                    "service_responsibility": service_desc,
                    "shared_state_dependencies": [],
                }
            )
        return modules

    def _build_coupling_heatmap(self, modules: list[dict[str, Any]], legacy: dict[str, Any]) -> dict[str, Any]:
        chunk_manifest = self._as_dict(legacy.get("chunk_manifest_v1"))
        chunks = self._as_list(chunk_manifest.get("chunks"))
        file_to_chunk: dict[str, str] = {}
        chunk_depends: dict[str, set[str]] = defaultdict(set)
        reverse_depends: dict[str, set[str]] = defaultdict(set)
        for chunk in chunks:
            if not isinstance(chunk, dict):
                continue
            chunk_id = str(chunk.get("chunk_id") or chunk.get("name") or "").strip()
            if not chunk_id:
                continue
            for file_path in self._as_list(chunk.get("files")):
                file_to_chunk[str(file_path).replace("\\", "/").split("/")[-1].lower()] = chunk_id
            for dep in self._as_list(chunk.get("depends_on_chunks")):
                dep_id = str(dep).strip()
                if dep_id and dep_id != chunk_id:
                    chunk_depends[chunk_id].add(dep_id)
                    reverse_depends[dep_id].add(chunk_id)

        module_rows: list[dict[str, Any]] = []
        for module in modules:
            source_file = str(module.get("source_file", "")).replace("\\", "/").split("/")[-1].lower()
            chunk_id = file_to_chunk.get(source_file, "")
            ca = len(reverse_depends.get(chunk_id, set())) if chunk_id else 0
            ce = len(chunk_depends.get(chunk_id, set())) if chunk_id else 0
            instability = round((ce / (ca + ce)) if (ca + ce) else 0.5, 2)
            risk_tier = "Low"
            if instability >= 0.8:
                risk_tier = "Critical"
            elif instability >= 0.6:
                risk_tier = "High"
            elif instability >= 0.3:
                risk_tier = "Medium"
            recommendation = "Decompose first" if instability < 0.3 else "Decompose after dependency resolution" if instability >= 0.6 else "Decompose in mid phases"
            module_rows.append(
                {
                    "name": module.get("source_module"),
                    "service": module.get("service_name"),
                    "chunk_id": chunk_id,
                    "afferent_coupling": ca,
                    "efferent_coupling": ce,
                    "instability": instability,
                    "risk_tier": risk_tier,
                    "shared_state_dependencies": self._as_list(module.get("shared_state_dependencies")),
                    "recommended_action": recommendation,
                }
            )
        sequence = [row.get("name") for row in sorted(module_rows, key=lambda row: (float(row.get("instability", 0.5) or 0.5), str(row.get("name") or "")))]
        return {
            "artifact_type": "coupling_heatmap_v1",
            "modules": module_rows,
            "decomposition_sequence": sequence,
        }

    def _build_data_ownership_matrix(self, modules: list[dict[str, Any]], legacy: dict[str, Any], raw: dict[str, Any] | None = None) -> dict[str, Any]:
        def _preferred_owner(table_name: str, service_names: list[str], fallback: str) -> str:
            lowered = table_name.lower()
            preferences: list[str] = []
            if "customer" in lowered:
                preferences = ["CustomerService"]
            elif "transaction" in lowered or "deposit" in lowered or "withdraw" in lowered:
                preferences = ["TransactionService"]
            elif "account" in lowered:
                preferences = ["AccountService", "CustomerService", "TransactionService"]
            elif any(token in lowered for token in ("type", "reference", "setting")):
                preferences = ["ReferenceDataService"]
            elif any(token in lowered for token in ("report", "statement")):
                preferences = ["ReportingService"]
            for candidate in preferences:
                if candidate in service_names:
                    return candidate
            if fallback == "ReportingService":
                for candidate in ("CustomerService", "TransactionService", "AccountService", "ReferenceDataService"):
                    if candidate in service_names:
                        return candidate
            return fallback

        entities: list[dict[str, Any]] = []
        table_to_services: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        table_to_write_services: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
        table_to_modules: dict[str, set[str]] = defaultdict(set)
        for module in modules:
            for table in self._as_list(module.get("tables")):
                table_to_services[str(table)][str(module.get("service_name"))] += 1
                table_to_modules[str(table)].add(str(module.get("source_module")))
        raw_map = raw if isinstance(raw, dict) else {}
        sql_rows = self._as_list(self._as_dict(raw_map.get("sql_catalog")).get("statements"))
        source_service_by_module = {
            str(module.get("source_module", "")).strip().lower(): str(module.get("service_name", "")).strip()
            for module in modules
            if str(module.get("source_module", "")).strip() and str(module.get("service_name", "")).strip()
        }
        for row in sql_rows:
            if not isinstance(row, dict):
                continue
            kind = str(row.get("kind", "")).strip().lower() or str(row.get("operation", "")).strip().lower()
            write_like = kind in {"insert", "update", "delete", "merge", "replace", "ddl", "upsert"}
            source_name = str(row.get("form", "")).strip() or str(row.get("module", "")).strip()
            if not source_name:
                usage_sites = self._as_list(row.get("usage_sites"))
                for usage in usage_sites:
                    if not isinstance(usage, dict):
                        continue
                    external_ref = self._as_dict(usage.get("external_ref"))
                    ref = str(external_ref.get("ref", "")).strip()
                    if not ref:
                        continue
                    parts = [part.strip() for part in ref.split("::") if part.strip()]
                    if len(parts) >= 2:
                        source_name = parts[-2]
                        break
            service_name = source_service_by_module.get(source_name.lower(), "")
            if not service_name:
                continue
            write_tables = [str(value).strip() for value in self._as_list(row.get("data_mutations")) if str(value).strip()]
            touched_tables = [str(value).strip() for value in self._as_list(row.get("tables")) if str(value).strip()]
            for table in touched_tables:
                table_to_services[table][service_name] += 1
            if write_like and not write_tables:
                write_tables = touched_tables
            for table in write_tables:
                table_to_write_services[table][service_name] += 1
        for table, service_counts in sorted(table_to_services.items()):
            write_service_counts = table_to_write_services.get(table, {})
            if write_service_counts:
                fallback_owner = max(write_service_counts.items(), key=lambda item: item[1])[0]
                owner = _preferred_owner(table, list(write_service_counts.keys()), fallback_owner)
            else:
                fallback_owner = max(service_counts.items(), key=lambda item: item[1])[0] if service_counts else "LegacyCoreService"
                owner = _preferred_owner(table, list(service_counts.keys()), fallback_owner)
            readers = sorted([svc for svc in service_counts.keys() if svc != owner])
            entities.append(
                {
                    "name": table,
                    "legacy_tables": [table],
                    "legacy_global_vars": [],
                    "owning_service": owner,
                    "read_services": readers,
                    "migration_notes": (
                        f"{len(table_to_modules[table])} legacy module(s) currently touch {table}; ownership shifts to {owner}"
                        f"{' based on observed write paths.' if write_service_counts else '.'}"
                    ),
                }
            )
        return {
            "artifact_type": "data_ownership_matrix_v1",
            "entities": entities,
            "global_state_resolution_plan": [],
        }

    def _confidence_for_module(self, module: dict[str, Any], ownership: dict[str, Any], adr_ids: list[str]) -> tuple[float, str, list[str]]:
        score = 0.35
        rationale: list[str] = []
        flags: list[str] = []
        if str(module.get("purpose", "")).strip():
            score += 0.2
            rationale.append("module has explicit business purpose")
        if self._as_list(module.get("tables")):
            score += 0.15
            rationale.append("module maps to owned data entities")
        if self._as_list(module.get("rule_ids")):
            score += 0.1
            rationale.append("business rules are linked")
        if self._as_list(module.get("risk_ids")):
            score += 0.05
            rationale.append("risk signals are explicit")
        score += min(0.15, float(module.get("coverage_score", 0.0) or 0.0) * 0.15)
        if adr_ids:
            score += 0.1
            rationale.append("service boundary is covered by ADR")
        if not self._as_list(module.get("tables")):
            flags.append("no_db_ownership")
        if score < 0.7:
            flags.append("requires_adr_review")
        elif score < 0.85:
            flags.append("review")
        return round(min(score, 0.98), 2), "; ".join(rationale) or "heuristic service alignment only", flags

    def _migration_strategy_for_module(self, module: dict[str, Any], confidence: float) -> str:
        service = str(module.get("service_name", ""))
        if confidence < 0.55:
            return "UNMAPPED"
        if service in {"AuthenticationService", "ExperienceShell", "LegacyCoreService"}:
            return "Wrap"
        if service in {"TransactionService", "ReportingService"}:
            return "Strangle"
        return "Rewrite"

    def _phase_for_service(self, service: str) -> int:
        if service in {"AuthenticationService", "ExperienceShell"}:
            return 1
        if service in {"CustomerService", "ReferenceDataService"}:
            return 2
        if service == "TransactionService":
            return 3
        if service == "ReportingService":
            return 4
        return 4

    def _build_adrs(self, modules: list[dict[str, Any]], coupling: dict[str, Any]) -> list[dict[str, Any]]:
        module_rows = self._as_list(coupling.get("modules"))
        instability_by_name = {str(row.get("name")): float(row.get("instability", 0.5) or 0.5) for row in module_rows if isinstance(row, dict)}
        by_service: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for module in modules:
            by_service[str(module.get("service_name", "LegacyCoreService"))].append(module)
        adrs: list[dict[str, Any]] = []
        for idx, (service_name, service_modules) in enumerate(sorted(by_service.items()), start=1):
            loc_total = sum(int(m.get("loc", 0) or 0) for m in service_modules)
            avg_instability = round(sum(instability_by_name.get(str(m.get("source_module")), 0.5) for m in service_modules) / max(len(service_modules), 1), 2)
            module_names = [str(m.get("source_module")) for m in service_modules[:10]]
            business_area = str(service_modules[0].get("service_responsibility", "")).strip() if service_modules else service_name
            adrs.append(
                {
                    "id": f"ADR-{idx:03d}",
                    "title": f"Bound {service_name} around {business_area.split('.')[0].lower()}",
                    "status": "Proposed",
                    "date": "",
                    "context": {
                        "narrative": f"{service_name} groups {len(service_modules)} legacy module(s) so the modernization can enforce a coherent boundary instead of carrying forward the VB6 UI structure.",
                        "data_signals": {
                            "source_modules": module_names,
                            "loc_total": loc_total,
                            "coupling_instability": avg_instability,
                            "shared_global_vars": [],
                            "risk_tier": "High" if avg_instability >= 0.6 else "Medium" if avg_instability >= 0.3 else "Low",
                            "analyst_refs": [f"MODULE-{self._normalize_name(name)[:12].upper()}" for name in module_names[:3]],
                        },
                    },
                    "decision": f"Establish {service_name} as the primary bounded context for {business_area.lower()}",
                    "alternatives_considered": [
                        {
                            "option": f"Keep {', '.join(module_names[:2]) or service_name} inside one shared legacy service",
                            "rejected_because": "It would preserve the legacy coupling and obscure data ownership boundaries.",
                        }
                    ],
                    "consequences": {
                        "positive": [
                            f"{service_name} receives a dedicated ownership boundary.",
                            "Delivery sequencing can align with explicit migration phases.",
                        ],
                        "negative": [
                            "Cross-service contracts must be introduced where the legacy client used direct form-to-form calls.",
                        ],
                        "risks": [
                            f"Average instability {avg_instability} may require staged strangler delivery if dependencies remain unresolved.",
                        ],
                        "migration_prerequisites": [
                            "Confirm service boundary with a human architect if confidence is below the threshold.",
                        ],
                    },
                    "traceability": {
                        "source_modules": module_names,
                        "target_services": [service_name],
                        "symbol_refs": [str(m.get("source_file")) for m in service_modules[:5]],
                        "downstream_artifacts": ["traceability_matrix", "strangler_migration_plan", "api_contract_sketches"],
                    },
                }
            )
        return adrs

    def _build_traceability_matrix(self, modules: list[dict[str, Any]], adrs: list[dict[str, Any]], ownership: dict[str, Any]) -> dict[str, Any]:
        adr_ids_by_service: dict[str, list[str]] = defaultdict(list)
        for adr in adrs:
            if not isinstance(adr, dict):
                continue
            for service in self._as_list(self._as_dict(adr.get("traceability")).get("target_services")):
                adr_ids_by_service[str(service)].append(str(adr.get("id")))
        mappings: list[dict[str, Any]] = []
        flagged = 0
        confident = 0
        avg_conf = 0.0
        for module in modules:
            service_name = str(module.get("service_name", "LegacyCoreService"))
            confidence, rationale, flags = self._confidence_for_module(module, ownership, adr_ids_by_service.get(service_name, []))
            strategy = self._migration_strategy_for_module(module, confidence)
            if strategy == "UNMAPPED":
                flags.append("requires_human_review")
            if confidence >= 0.85:
                confident += 1
            if flags:
                flagged += 1
            avg_conf += confidence
            component_name = f"{self._titleize_identifier(module.get('base_name', service_name)).replace(' ', '')}Controller"
            mappings.append(
                {
                    "source": {
                        "module": module.get("source_module"),
                        "type": module.get("module_type") or "VB6Form",
                        "loc": int(module.get("loc", 0) or 0),
                        "risk_tier": "High" if self._as_list(module.get("risk_ids")) else "Medium" if float(module.get("coverage_score", 0.0) or 0.0) < 0.6 else "Low",
                        "analyst_ref": f"MODULE-{self._normalize_name(module.get('source_module', ''))[:12].upper()}",
                    },
                    "target": {
                        "service": service_name,
                        "component": component_name,
                        "migration_strategy": strategy,
                        "phase": self._phase_for_service(service_name),
                    },
                    "confidence": confidence,
                    "confidence_rationale": rationale,
                    "flags": sorted(set(flags)),
                    "adr_refs": adr_ids_by_service.get(service_name, []),
                }
            )
        total = len(mappings)
        return {
            "artifact_type": "traceability_matrix_v1",
            "mappings": mappings,
            "coverage": {
                "total_source_modules": total,
                "mapped_confident": confident,
                "mapped_review": flagged,
                "mapped_unmapped": len([m for m in mappings if m.get("target", {}).get("migration_strategy") == "UNMAPPED"]),
                "average_confidence": round(avg_conf / max(total, 1), 2),
                "flags_summary": {
                    "requires_human_review": len([m for m in mappings if "requires_human_review" in self._as_list(m.get("flags"))]),
                    "no_db_ownership": len([m for m in mappings if "no_db_ownership" in self._as_list(m.get("flags"))]),
                },
            },
        }

    def _build_api_contract_sketches(self, modules: list[dict[str, Any]], traceability: dict[str, Any]) -> dict[str, Any]:
        def _contract_for_module(service_name: str, module: dict[str, Any]) -> dict[str, Any]:
            source_module = str(module.get("source_module", "")).strip()
            purpose = str(module.get("purpose", "")).strip()
            source_lower = source_module.lower()
            purpose_lower = purpose.lower()
            service_slug = re.sub(r"service$", "", service_name, flags=re.IGNORECASE).lower()
            base_name = self._titleize_identifier(module.get("base_name", source_module or service_name)).replace(" ", "")
            action_slug = self._normalize_name(base_name) or service_slug
            method = "GET"
            path = f"/{service_slug}/{action_slug}"
            auth = {"required": True, "policy": "jwt"}
            request_fields: list[dict[str, Any]] = []
            response_fields: list[dict[str, Any]] = [{"name": "result", "type": "object", "required": True}]
            notes = purpose or f"Draft contract for {source_module or service_name}"

            if "login" in source_lower or "login" in purpose_lower or service_name == "AuthenticationService":
                method = "POST"
                path = "/auth/login"
                auth = {"required": False, "policy": "anonymous-login-bootstrap"}
                request_fields = [
                    {"name": "username", "type": "string", "required": True},
                    {"name": "password", "type": "string", "required": True},
                ]
                response_fields = [
                    {"name": "token", "type": "string", "required": True},
                    {"name": "expires_at", "type": "datetime", "required": True},
                ]
                notes = "Authenticate credentials, issue session/token, and apply lockout policy before entering customer workflows."
            elif "logout" in source_lower or "logout" in purpose_lower:
                method = "POST"
                path = "/auth/logout"
                response_fields = [{"name": "logged_out", "type": "boolean", "required": True}]
            elif any(token in source_lower or token in purpose_lower for token in ("deposit", "withdraw")):
                method = "POST"
                path = f"/transactions/{'deposit' if 'deposit' in source_lower or 'deposit' in purpose_lower else 'withdraw'}"
                request_fields = [
                    {"name": "accountNo", "type": "string", "required": True},
                    {"name": "amount", "type": "decimal", "required": True},
                    {"name": "transactionDate", "type": "date", "required": False},
                ]
                response_fields = [
                    {"name": "transactionId", "type": "string", "required": True},
                    {"name": "balance", "type": "decimal", "required": True},
                ]
                notes = "Submit a transactional write with balance validation and ledger persistence."
            elif any(token in source_lower or token in purpose_lower for token in ("closeaccount", "close account")):
                method = "PUT"
                path = "/accounts/close"
                request_fields = [
                    {"name": "accountNo", "type": "string", "required": True},
                    {"name": "closureReason", "type": "string", "required": False},
                ]
                response_fields = [{"name": "closed", "type": "boolean", "required": True}]
                notes = "Close an account after eligibility and balance checks succeed."
            elif "customer" in source_lower or "customer" in purpose_lower:
                if any(token in source_lower or token in purpose_lower for token in ("add", "new", "create", "save", "update")):
                    method = "POST" if any(token in source_lower or token in purpose_lower for token in ("add", "new", "create")) else "PUT"
                    path = "/customers"
                    request_fields = [
                        {"name": "customerId", "type": "string", "required": False},
                        {"name": "accountNo", "type": "string", "required": True},
                        {"name": "firstName", "type": "string", "required": True},
                        {"name": "lastName", "type": "string", "required": False},
                    ]
                    response_fields = [{"name": "customer", "type": "object", "required": True}]
                    notes = "Create or update customer/account-holder records and enforce uniqueness validation."
                else:
                    method = "GET"
                    path = "/customers/{customerId}"
                    response_fields = [
                        {"name": "customer", "type": "object", "required": True},
                        {"name": "accounts", "type": "array", "required": False},
                    ]
                    notes = "Read customer profile and account summary details."
            elif any(token in source_lower or token in purpose_lower for token in ("statement", "report", "monthly")):
                method = "GET"
                path = f"/reports/{action_slug}"
                response_fields = [{"name": "report", "type": "object", "required": True}]
                notes = "Generate or retrieve reporting output for statements and monthly summaries."
            elif any(token in source_lower or token in purpose_lower for token in ("settings", "accounttype", "reference")):
                method = "GET"
                path = f"/reference/{action_slug}"
                response_fields = [{"name": "items", "type": "array", "required": True}]
                notes = "Retrieve or maintain reference data used by operational workflows."

            operation_name = base_name if method == "GET" else f"{method.title()}{base_name}"
            return {
                "name": operation_name,
                "method": method,
                "path": path,
                "replaces": [source_module] if source_module else [],
                "notes": notes,
                "request_body": {"fields": request_fields},
                "response_body": {"fields": response_fields},
                "error_contract": {
                    "shape": [
                        {"name": "code", "type": "string", "required": True},
                        {"name": "message", "type": "string", "required": True},
                    ]
                },
                "auth": auth,
            }

        by_service: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for module in modules:
            by_service[str(module.get("service_name", "LegacyCoreService"))].append(module)
        services_payload: list[dict[str, Any]] = []
        for service_name, service_modules in sorted(by_service.items()):
            operations: list[dict[str, Any]] = []
            for module in service_modules[:4]:
                operations.append(_contract_for_module(service_name, module))
            services_payload.append({
                "service": service_name,
                "operations": operations,
            })
        return {"artifact_type": "api_contract_sketches_v1", "services": services_payload}

    def _build_migration_plan(self, modules: list[dict[str, Any]], coupling: dict[str, Any], adrs: list[dict[str, Any]]) -> dict[str, Any]:
        by_phase: dict[int, list[dict[str, Any]]] = defaultdict(list)
        for module in modules:
            by_phase[self._phase_for_service(str(module.get("service_name", "LegacyCoreService")))].append(module)
        phase_names = {
            1: "Isolation Layer",
            2: "Customer and Reference Domains",
            3: "Transactional Core",
            4: "Reporting and Residual Shared Logic",
        }
        phases: list[dict[str, Any]] = []
        for phase_no in sorted(by_phase.keys()):
            phase_modules = by_phase[phase_no]
            service_name = str(phase_modules[0].get("service_name", "LegacyCoreService")) if phase_modules else "LegacyCoreService"
            phases.append(
                {
                    "phase": phase_no,
                    "name": phase_names.get(phase_no, f"Phase {phase_no}"),
                    "description": f"Modernize {service_name} related workflows and stabilize their contracts.",
                    "modules": [m.get("source_module") for m in phase_modules],
                    "strategy": self._migration_strategy_for_module(phase_modules[0], 0.85) if phase_modules else "Rewrite",
                    "target_service": service_name,
                    "prerequisite_adr": [adr.get("id") for adr in adrs if service_name in self._as_list(self._as_dict(adr.get("traceability")).get("target_services"))][:3],
                    "risk": "High" if any(self._as_list(m.get("risk_ids")) for m in phase_modules) else "Medium",
                    "estimated_sprints": max(1, round(sum(int(m.get("loc", 0) or 0) for m in phase_modules) / 2500)),
                    "exit_criteria": f"{service_name} has explicit traceability mappings, approved ADR coverage, and no unresolved critical review items.",
                }
            )
        sequence = self._as_list(coupling.get("decomposition_sequence"))
        return {
            "artifact_type": "strangler_migration_plan_v1",
            "phases": phases,
            "critical_path": sequence[:6],
        }

    def _build_component_risk_register(self, modules: list[dict[str, Any]], coupling: dict[str, Any], ownership: dict[str, Any], dependencies: list[dict[str, Any]]) -> dict[str, Any]:
        module_rows = {str(row.get("name")): row for row in self._as_list(coupling.get("modules")) if isinstance(row, dict)}
        by_service: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for module in modules:
            by_service[str(module.get("service_name", "LegacyCoreService"))].append(module)
        services_payload: list[dict[str, Any]] = []
        estimation_services: list[dict[str, Any]] = []
        for service_name, service_modules in sorted(by_service.items()):
            loc_total = sum(int(m.get("loc", 0) or 0) for m in service_modules)
            avg_instability = sum(float(self._as_dict(module_rows.get(str(m.get("source_module")))).get("instability", 0.5) or 0.5) for m in service_modules) / max(len(service_modules), 1)
            shared_state_count = sum(len(self._as_list(m.get("shared_state_dependencies"))) for m in service_modules)
            risk_signal_count = sum(len(self._as_list(m.get("risk_ids"))) for m in service_modules)
            external_dep_count = len(dependencies)
            risk_score = min(0.95, round((avg_instability * 0.30) + (min(shared_state_count / 10.0, 1.0) * 0.25) + (min(risk_signal_count / 5.0, 1.0) * 0.25) + (min(external_dep_count / 10.0, 1.0) * 0.20), 2))
            if risk_score < 0.30:
                modifier = 1.0
                tier = "Low"
            elif risk_score < 0.50:
                modifier = 1.2
                tier = "Medium"
            elif risk_score < 0.70:
                modifier = 1.4
                tier = "High"
            elif risk_score < 0.85:
                modifier = 1.6
                tier = "Critical"
            else:
                modifier = 1.8
                tier = "Critical"
            mitigations = [
                f"Sequence {service_name} after ADR review to validate boundary assumptions.",
                f"Add parity tests for {service_name} before strangling legacy modules.",
            ]
            if shared_state_count:
                mitigations.insert(0, f"Resolve shared state dependencies before decomposing {service_name}.")
            risk_drivers = [
                {"factor": "Source LOC", "value": loc_total, "signal": "Rewrite effort baseline"},
                {"factor": "Coupling instability", "value": round(avg_instability, 2), "signal": "Cross-module coordination risk"},
                {"factor": "Risk signals", "value": risk_signal_count, "signal": "Analyst-stage remediation load"},
            ]
            services_payload.append(
                {
                    "service": service_name,
                    "risk_tier": tier,
                    "risk_drivers": risk_drivers,
                    "composite_risk_score": risk_score,
                    "mitigation_recommendations": mitigations[:5],
                    "estimation_modifier": modifier,
                    "risk_escalation_note": f"{service_name} requires explicit architecture review before delivery planning." if modifier >= 1.6 else "",
                }
            )
            estimation_services.append({"service": service_name, "estimation_modifier": modifier, "primary_risk": risk_drivers[1]["signal"]})
        return {
            "artifact_type": "component_risk_register_v1",
            "services": services_payload,
            "estimation_handoff": {
                "services": estimation_services,
                "global_modifier_note": "Higher-risk services should inflate schedule estimates and require human review before phase commitment.",
                "schema_version": "1.0",
            },
        }

    def _build_human_review_queue(self, traceability: dict[str, Any], risk_register: dict[str, Any], adrs: list[dict[str, Any]], ownership: dict[str, Any]) -> list[dict[str, Any]]:
        queue: list[dict[str, Any]] = []
        for mapping in self._as_list(traceability.get("mappings")):
            if not isinstance(mapping, dict):
                continue
            confidence = float(mapping.get("confidence", 1.0) or 1.0)
            if confidence < 0.70:
                queue.append({
                    "priority": "HIGH",
                    "artifact": "traceability_matrix",
                    "item": str(self._as_dict(mapping.get("source")).get("module", "unknown")),
                    "reason": str(mapping.get("confidence_rationale") or "low confidence mapping"),
                    "blocking": True,
                })
        for entity in self._as_list(ownership.get("entities")):
            if not isinstance(entity, dict):
                continue
            if len(self._as_list(entity.get("read_services"))) >= 3:
                queue.append({
                    "priority": "MEDIUM",
                    "artifact": "data_ownership_matrix",
                    "item": str(entity.get("name", "entity")),
                    "reason": "Shared kernel candidate requires architect review.",
                    "blocking": False,
                })
        for adr in adrs:
            if not isinstance(adr, dict):
                continue
            queue.append({
                "priority": "MEDIUM",
                "artifact": "architecture_decision_records",
                "item": str(adr.get("id", "ADR")),
                "reason": "Boundary decision is still proposed and should be reviewed by a human architect.",
                "blocking": False,
            })
        for row in self._as_list(risk_register.get("services")):
            if not isinstance(row, dict):
                continue
            if float(row.get("estimation_modifier", 1.0) or 1.0) >= 1.6:
                queue.append({
                    "priority": "HIGH",
                    "artifact": "component_risk_register",
                    "item": str(row.get("service", "service")),
                    "reason": "High estimation modifier requires delivery risk review.",
                    "blocking": True,
                })
        return queue or [{
            "priority": "MEDIUM",
            "artifact": "architecture_decision_records",
            "item": "architect-package",
            "reason": "Human architect review is required before delivery execution.",
            "blocking": False,
        }]

    def _build_architect_package(self, state: dict[str, Any], parsed: dict[str, Any]) -> dict[str, Any]:
        _, raw, legacy = self._raw_analyst_evidence(state)
        modules = self._collect_source_modules(state)
        dependencies = self._as_list(self._as_dict(raw.get("dependency_inventory")).get("dependencies"))
        parsed_ownership = self._as_dict(parsed.get("data_ownership_matrix"))
        ownership = (
            parsed_ownership
            if self._as_list(parsed_ownership.get("entities"))
            else self._build_data_ownership_matrix(modules, legacy, raw)
        )
        coupling = self._build_coupling_heatmap(modules, legacy)
        adrs = self._build_adrs(modules, coupling)
        traceability = self._build_traceability_matrix(modules, adrs, ownership)
        api_contracts = self._build_api_contract_sketches(modules, traceability)
        migration_plan = self._build_migration_plan(modules, coupling, adrs)
        risk_register = self._build_component_risk_register(modules, coupling, ownership, dependencies)
        review_queue = self._build_human_review_queue(traceability, risk_register, adrs, ownership)
        warnings: list[str] = []
        if self._as_dict(traceability.get("coverage")).get("mapped_unmapped", 0):
            warnings.append("One or more source modules could not be mapped confidently and require review.")
        if review_queue:
            warnings.append(f"{len(review_queue)} architect review item(s) remain open.")
        return {
            "package_meta": {
                "schema_version": "1.0",
                "status": "WARN" if warnings else "COMPLETE",
                "warnings": warnings,
                "artifact_count": 7,
            },
            "artifacts": {
                "data_ownership_matrix": ownership,
                "coupling_heatmap": coupling,
                "architecture_decision_records": adrs,
                "traceability_matrix": traceability,
                "api_contract_sketches": api_contracts,
                "strangler_migration_plan": migration_plan,
                "component_risk_register": risk_register,
            },
            "estimation_handoff": self._as_dict(risk_register.get("estimation_handoff")),
            "human_review_queue": review_queue,
        }

    def _build_top_level_services(self, package: dict[str, Any], state: dict[str, Any]) -> list[dict[str, Any]]:
        traceability = self._as_dict(self._as_dict(package.get("artifacts")).get("traceability_matrix"))
        mappings = self._as_list(traceability.get("mappings"))
        by_service: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for mapping in mappings:
            if not isinstance(mapping, dict):
                continue
            target = self._as_dict(mapping.get("target"))
            source = self._as_dict(mapping.get("source"))
            service = str(target.get("service", "")).strip()
            if service:
                by_service[service].append({"source": source, "target": target})
        language = str(state.get("modernization_language", "")).strip() or "C#"
        framework = "ASP.NET Core"
        database_default = str(state.get("database_target", "")).strip() or "PostgreSQL"
        services: list[dict[str, Any]] = []
        for service_name, rows in sorted(by_service.items()):
            responsibilities = sorted({str(r.get("source", {}).get("module", "")).strip() for r in rows if str(r.get("source", {}).get("module", "")).strip()})
            services.append(
                {
                    "name": service_name,
                    "responsibility": f"Owns {', '.join(responsibilities[:4])}{' and other related workflows' if len(responsibilities) > 4 else ''}.",
                    "technology": "Containerized application service",
                    "language": language,
                    "framework": framework,
                    "api_type": "REST",
                    "database": None if service_name == "AuthenticationService" else database_default,
                    "cache": "Redis" if service_name in {"AuthenticationService", "ReferenceDataService"} else None,
                }
            )
        return services

    def _build_architecture_overview(self, package: dict[str, Any]) -> str:
        traceability = self._as_dict(self._as_dict(package.get("artifacts")).get("traceability_matrix"))
        coverage = self._as_dict(traceability.get("coverage"))
        risk_register = self._as_dict(self._as_dict(package.get("artifacts")).get("component_risk_register"))
        high_risk = len([row for row in self._as_list(risk_register.get("services")) if isinstance(row, dict) and float(row.get("estimation_modifier", 1.0) or 1.0) >= 1.6])
        return (
            f"This target architecture groups the legacy estate into explicit bounded services with traceable source-module mappings. "
            f"The package covers {int(coverage.get('total_source_modules', 0) or 0)} source modules and carries {high_risk} high-risk service boundary(ies) into the migration plan and estimation handoff."
        )

    def _normalize_tradeoffs(self, existing: Any, package: dict[str, Any]) -> list[dict[str, Any]]:
        rows = self._as_list(existing)
        if len(rows) >= 3:
            return rows
        adrs = self._as_list(self._as_dict(package.get("artifacts")).get("architecture_decision_records"))
        generated: list[dict[str, Any]] = []
        for adr in adrs[:3]:
            if not isinstance(adr, dict):
                continue
            generated.append(
                {
                    "decision": str(adr.get("title", "")).strip(),
                    "rationale": str(self._as_dict(adr.get("context")).get("narrative", "")).strip(),
                    "alternatives_considered": [str(row.get("option", "")).strip() for row in self._as_list(adr.get("alternatives_considered")) if isinstance(row, dict) and str(row.get("option", "")).strip()],
                }
            )
        return generated[:3]

    def _normalize_data_flow(self, existing: Any, package: dict[str, Any], services: list[dict[str, Any]]) -> list[dict[str, Any]]:
        rows = self._as_list(existing)
        if len(rows) >= 4:
            return rows
        service_names = [str(s.get("name", "")).strip() for s in services if isinstance(s, dict) and str(s.get("name", "")).strip()]
        database = next((str(s.get("database", "")).strip() for s in services if isinstance(s, dict) and str(s.get("database", "")).strip()), "PostgreSQL")
        flow = [
            {"from": "Client", "to": "ExperienceShell" if "ExperienceShell" in service_names else service_names[0] if service_names else "Application Service", "protocol": "HTTPS", "description": "Client traffic enters the modernized UI/API boundary."},
            {"from": "ExperienceShell" if "ExperienceShell" in service_names else "Application Service", "to": "AuthenticationService" if "AuthenticationService" in service_names else (service_names[0] if service_names else "Application Service"), "protocol": "REST", "description": "Authentication and session establishment are validated before business workflows continue."},
            {"from": "AuthenticationService" if "AuthenticationService" in service_names else (service_names[0] if service_names else "Application Service"), "to": "CustomerService" if "CustomerService" in service_names else (service_names[1] if len(service_names) > 1 else database), "protocol": "REST", "description": "Validated user context is forwarded into business-domain services."},
            {"from": "CustomerService" if "CustomerService" in service_names else (service_names[0] if service_names else "Application Service"), "to": database, "protocol": "SQL", "description": "Service boundaries own write access to target data stores using explicit contracts."},
        ]
        return flow[:4]

    def _normalize_latency_optimizations(self, existing: Any, package: dict[str, Any]) -> list[str]:
        rows = [str(x).strip() for x in self._as_list(existing) if str(x).strip()]
        if len(rows) >= 4:
            return rows[:6]
        return [
            "Keep authentication, reference data, and read-heavy workflows stateless so they can scale independently.",
            "Separate reporting and transaction processing so reconciliation and exports do not block interactive flows.",
            "Introduce caching for lookup/reference data rather than re-reading stable tables on every request.",
            "Prefer explicit APIs and background workers over synchronous cross-module calls inherited from the VB6 UI.",
        ]

    def _normalize_infrastructure(self, existing: Any, state: dict[str, Any]) -> dict[str, Any]:
        infra = self._as_dict(existing)
        if infra:
            return infra
        return {
            "cloud_provider": "Docker-local / cloud-portable",
            "container_orchestration": "Docker Compose (initial) / Kubernetes-ready",
            "ci_cd": "GitHub Actions",
            "monitoring": "OpenTelemetry + Prometheus",
            "logging": "Structured JSON logs",
        }

    def _normalize_security(self, existing: Any, state: dict[str, Any]) -> dict[str, Any]:
        security = self._as_dict(existing)
        if security:
            return security
        return {
            "authentication": "Centralized identity and session enforcement with explicit service boundary checks",
            "authorization": "Role-based access control enforced at the API boundary",
            "encryption": "TLS in transit and encrypted database/storage at rest",
            "api_security": "Parameterized data access, validated inputs, and authenticated service calls",
            "secrets_management": "Environment-injected secrets with managed secret store support",
        }

    def _normalize_scalability(self, existing: Any) -> dict[str, Any]:
        scalability = self._as_dict(existing)
        if scalability:
            return scalability
        return {
            "strategy": "Scale stateless application services independently from reporting and background workloads.",
            "auto_scaling_rules": [
                "Scale interactive services on request concurrency and latency.",
                "Scale reporting/background workers on queue depth and scheduled workload windows.",
                "Keep database capacity aligned to transaction workload and reporting peaks.",
            ],
            "caching_strategy": "Use short-lived caches for reference data and read-heavy lookup flows.",
            "cdn": None,
        }

    def _build_target_diagram_from_package(self, package: dict[str, Any], services: list[dict[str, Any]]) -> str:
        service_names = [str(s.get("name", "")).strip() for s in services if isinstance(s, dict) and str(s.get("name", "")).strip()]
        lines = ["graph TD;"]
        lines.append('    user_0["User"] --> shell_0["ExperienceShell / UI"];')
        lines.append('    shell_0 --> api_0["Application API Boundary"];')
        if "AuthenticationService" in service_names:
            lines.append('    api_0 --> auth_0["AuthenticationService"];')
        service_nodes = {
            "CustomerService": 'cust_0["CustomerService"]',
            "ReferenceDataService": 'ref_0["ReferenceDataService"]',
            "TransactionService": 'txn_0["TransactionService"]',
            "ReportingService": 'rep_0["ReportingService"]',
            "LegacyCoreService": 'core_0["LegacyCoreService"]',
        }
        for service, node in service_nodes.items():
            if service in service_names:
                node_id = node.split("[", 1)[0]
                lines.append(f"    {node};")
                lines.append(f"    api_0 --> {node_id};")
                if "AuthenticationService" in service_names and node_id != "auth_0":
                    lines.append(f"    auth_0 --> {node_id};")
        if "CustomerService" in service_names and "TransactionService" in service_names:
            lines.append("    cust_0 --> txn_0;")
        if "TransactionService" in service_names and "ReportingService" in service_names:
            lines.append("    txn_0 --> rep_0;")
        lines.append('    data_0["Target Operational Database"];')
        lines.append('    queue_0["Background Workers / Migration Jobs"];')
        for node_id in ["cust_0", "ref_0", "txn_0", "rep_0", "core_0"]:
            if any(line.startswith(f"    {node_id}[") for line in lines):
                lines.append(f"    {node_id} --> data_0;")
        if "ReportingService" in service_names:
            lines.append("    rep_0 --> queue_0;")
        return "\n".join(lines)

    def _build_legacy_diagram_from_package(self, package: dict[str, Any]) -> str:
        traceability = self._as_dict(self._as_dict(package.get("artifacts")).get("traceability_matrix"))
        mappings = self._as_list(traceability.get("mappings"))
        shell = next((m for m in mappings if isinstance(m, dict) and str(self._as_dict(m.get("target")).get("service", "")) == "ExperienceShell"), None)
        auth = next((m for m in mappings if isinstance(m, dict) and str(self._as_dict(m.get("target")).get("service", "")) == "AuthenticationService"), None)
        shell_name = str(self._as_dict(self._as_dict(shell or {}).get("source")).get("module", "Legacy Shell"))
        auth_name = str(self._as_dict(self._as_dict(auth or {}).get("source")).get("module", "Legacy Login"))
        lines = [
            "graph TD;",
            f'    user_0["User"] --> auth_0["{self._safe_mermaid_text(auth_name)}"];',
            f'    auth_0 --> shell_0["{self._safe_mermaid_text(shell_name)}"];',
        ]
        for mapping in mappings[:8]:
            if not isinstance(mapping, dict):
                continue
            target_service = str(self._as_dict(mapping.get("target")).get("service", ""))
            if target_service in {"AuthenticationService", "ExperienceShell"}:
                continue
            source_name = str(self._as_dict(mapping.get("source")).get("module", "")).strip()
            if source_name:
                node_id = self._safe_node_id(source_name, len(lines) + 1)
                lines.append(f'    {node_id}["{self._safe_mermaid_text(source_name)}"];')
                lines.append(f"    shell_0 --> {node_id};")
        return "\n".join(lines)

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        pattern = parsed.get("pattern", "unknown")
        svc_count = len(parsed.get("services", []))
        infra = parsed.get("infrastructure", {})
        cloud = infra.get("cloud_provider", "N/A")
        return (
            f"{pattern.title()} architecture with {svc_count} services "
            f"on {cloud}"
        )
