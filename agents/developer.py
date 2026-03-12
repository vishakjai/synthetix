"""
Agent 3: Developer Agent
Decomposes architecture into components and spawns parallel sub-agents for implementation.
Each sub-agent generates code for a specific service/component.
"""

from __future__ import annotations

import json
import concurrent.futures
from datetime import datetime
from typing import Any

from .base import BaseAgent
from utils.developer_dispatch import build_component_scoped_handoff
from utils.developer_prereqs import evaluate_component_prerequisites, is_component_blocked
from utils.llm import LLMClient

# Re-use the robust JSON extractor from BaseAgent
_extract_json = BaseAgent.extract_json


class DeveloperSubAgent:
    """A lightweight sub-agent that generates code for a single component."""

    SYSTEM_PROMPT = """You are a Senior Software Developer sub-agent.
You are responsible for generating COMPLETE, BUILDABLE, RUNNABLE code for a single service component.

The code you generate will be built into a Docker container and deployed to Kubernetes,
so it MUST actually work — not just be scaffolding.

You MUST respond with valid JSON in this exact structure:
{
  "component_name": "string",
  "language": "string",
  "framework": "string",
  "files": [
    {
      "path": "string",
      "description": "string",
      "code": "string (the actual code)",
      "lines_of_code": number
    }
  ],
  "dependencies": ["string", ...],
  "environment_variables": ["string", ...],
  "docker_support": true,
  "total_loc": number,
  "notes": "string"
}

CRITICAL REQUIREMENTS for the generated code:
1. ALWAYS include a dependency/package file as one of the files:
   - Python: requirements.txt with pinned versions
   - Node.js: package.json with dependencies and a "start" script
   - Go: go.mod
2. The main entrypoint MUST start an HTTP server that listens on a configurable port
   (default 8080, read from PORT env var).
3. The HTTP server MUST expose these health endpoints:
   - GET /health → returns {"status": "healthy"} with 200
   - GET /ready  → returns {"status": "ready"} with 200
4. ALWAYS set docker_support to true.
5. Include a working Dockerfile as one of the files (path: "Dockerfile").
6. Keep dependencies minimal — don't import packages you don't use.
7. The code should be functional, not placeholder stubs.
8. Generate implementation artifacts with practical depth:
   - Include at least one executable test file (pytest/jest/go test) when feasible.
   - Include a short component README.md with run/build/test commands.
   - Prefer 5+ files per component for non-trivial services (entrypoint, logic module, model/schema, tests, Docker/deps).

Write clean, well-documented, production-quality code that actually runs.
Respond ONLY with the JSON, no other text."""

    def __init__(
        self,
        llm: LLMClient,
        component: dict[str, Any],
        requirements: dict[str, Any],
        component_handoff: dict[str, Any] | None = None,
        modernization_language: str = "",
        legacy_code_excerpt: str = "",
        remediation_notes: list[dict[str, Any]] | None = None,
        previous_code_context: list[dict[str, str]] | None = None,
    ):
        self.llm = llm
        self.component = component
        self.requirements = requirements
        self.component_handoff = component_handoff or {}
        self.modernization_language = modernization_language
        self.legacy_code_excerpt = legacy_code_excerpt
        self.remediation_notes = remediation_notes or []
        self.previous_code_context = previous_code_context or []

    def run(self) -> dict[str, Any]:
        component_spec = BaseAgent._json_for_prompt(
            self.component,
            max_chars=2600,
            max_depth=4,
            max_items=12,
            max_str=420,
        )
        handoff_compact = BaseAgent._json_for_prompt(
            self.component_handoff,
            max_chars=3800,
            max_depth=5,
            max_items=16,
            max_str=360,
        )
        requirements_summary = BaseAgent._truncate_text(
            self.requirements.get("executive_summary", ""),
            max_chars=1200,
        )
        legacy_chunks = BaseAgent._chunk_text_for_prompt(
            self.legacy_code_excerpt,
            chunk_chars=1600,
            max_chunks=2,
        )
        legacy_context = "\n\n".join(
            [
                f"LEGACY EXCERPT {idx + 1}/{len(legacy_chunks)}:\n```text\n{chunk}\n```"
                for idx, chunk in enumerate(legacy_chunks)
            ]
        ) if legacy_chunks else "No legacy excerpt provided."
        remediation_compact = BaseAgent._json_for_prompt(
            self.remediation_notes,
            max_chars=2000,
            max_depth=3,
            max_items=10,
            max_str=320,
        )
        previous_context_compact = BaseAgent._json_for_prompt(
            self.previous_code_context,
            max_chars=2400,
            max_depth=4,
            max_items=8,
            max_str=300,
        )
        user_msg = f"""Generate COMPLETE, RUNNABLE code for this component.
The code will be built into a Docker image and deployed to Kubernetes.

COMPONENT SPECIFICATION:
{component_spec}

COMPONENT HANDOFF PACKAGE:
{handoff_compact}

CONTEXT (requirements summary):
{requirements_summary}

LEGACY CODE EXCERPT:
{legacy_context}

TARGET MODERNIZATION LANGUAGE:
{self.modernization_language or self.component.get("language", "Not specified")}

COMPONENT-SPECIFIC QA FAILURES TO FIX:
{remediation_compact}

PREVIOUS COMPONENT CODE CONTEXT:
{previous_context_compact}

REMEMBER:
- Include a requirements.txt (or package.json) with exact dependency versions
- Include a working Dockerfile
- The app MUST listen on port 8080 (or $PORT) and serve /health and /ready endpoints
- Generate ALL necessary files — this must build and run as-is with zero manual edits
- Preserve functional parity with the legacy behavior"""

        response = self.llm.invoke(self.SYSTEM_PROMPT, user_msg)
        try:
            parsed = _extract_json(response.content)
            parsed["_llm_metrics"] = {
                "tokens_used": response.input_tokens + response.output_tokens,
                "latency_ms": response.latency_ms,
            }
            return parsed
        except (json.JSONDecodeError, AttributeError):
            return {
                "component_name": self.component.get("name", "unknown"),
                "error": "Failed to parse sub-agent output",
                "raw": response.content[:500],
                "total_loc": 0,
                "files": [],
                "_llm_metrics": {
                    "tokens_used": response.input_tokens + response.output_tokens,
                    "latency_ms": response.latency_ms,
                },
            }


def _handoff_dispatch_record(component_handoff: dict[str, Any]) -> dict[str, Any]:
    handoff = component_handoff if isinstance(component_handoff, dict) else {}
    component_spec = handoff.get("component_spec", {}) if isinstance(handoff.get("component_spec", {}), dict) else {}
    system_context = handoff.get("system_context", {}) if isinstance(handoff.get("system_context", {}), dict) else {}
    brownfield_context = handoff.get("brownfield_context", {}) if isinstance(handoff.get("brownfield_context", {}), dict) else {}
    interface_contracts = handoff.get("interface_contracts", []) if isinstance(handoff.get("interface_contracts", []), list) else []
    wbs_items = handoff.get("wbs_items", []) if isinstance(handoff.get("wbs_items", []), list) else []
    review_queue = handoff.get("human_review_queue", []) if isinstance(handoff.get("human_review_queue", []), list) else []
    adrs = system_context.get("architectural_decisions", []) if isinstance(system_context.get("architectural_decisions", []), list) else []
    business_rules = brownfield_context.get("business_rules", []) if isinstance(brownfield_context.get("business_rules", []), list) else []
    anchors = brownfield_context.get("regression_test_anchors", []) if isinstance(brownfield_context.get("regression_test_anchors", []), list) else []
    return {
        "component_name": str(component_spec.get("component_name") or handoff.get("component_name") or "").strip(),
        "artifact_type": str(handoff.get("artifact_type") or "").strip(),
        "responsibility": str(component_spec.get("responsibility") or "").strip(),
        "interface_contract_ids": [
            str(contract.get("contract_id", "")).strip()
            for contract in interface_contracts
            if isinstance(contract, dict) and str(contract.get("contract_id", "")).strip()
        ],
        "wbs_ids": [
            str(item.get("wbs_id", "")).strip()
            for item in wbs_items
            if isinstance(item, dict) and str(item.get("wbs_id", "")).strip()
        ],
        "adr_ids": [
            str(adr.get("decision_id") or adr.get("id") or "").strip()
            for adr in adrs
            if isinstance(adr, dict) and str(adr.get("decision_id") or adr.get("id") or "").strip()
        ],
        "review_items": [
            {
                "priority": str(item.get("priority", "")).strip(),
                "item": str(item.get("item", "")).strip(),
                "blocking": bool(item.get("blocking")),
            }
            for item in review_queue
            if isinstance(item, dict)
        ],
        "business_rule_count": len(business_rules),
        "regression_anchor_count": len(anchors),
    }


class DeveloperAgent(BaseAgent):

    TOOL_PLANNER_PROMPT = """You are the orchestration planner for a Developer Agent.
You must decide which components should be implemented by sub-agents.

Use the available tool `spawn_sub_agent` one time for each component that should be implemented now.
Prefer high-priority and dependency-ready components first.

Rules:
1. Call the tool at least once and at most 5 times.
2. Do not call the same component twice.
3. Focus on components that maximize end-to-end runnable delivery.
4. If unsure, still call the tool with your best choices.
"""

    RETRY_DIAGNOSIS_PROMPT = """You are a Staff Engineer diagnosing failed QA results before a code regeneration retry.
Analyze tester failures and produce a precise remediation plan.

You MUST respond with valid JSON in this exact structure:
{
  "diagnosis_summary": "string",
  "confidence": 0.0,
  "root_causes": [
    {
      "component": "string",
      "issue": "string",
      "severity": "critical|warning",
      "evidence": "string",
      "suggested_fix": "string"
    }
  ],
  "retry_strategy": {
    "components_to_regenerate": ["string", ...],
    "component_exclusions": ["string", ...],
    "language_constraints": ["string", ...],
    "environment_actions": ["string", ...]
  },
  "prompt_addendum": ["string", ...]
}

Rules:
- Include only actionable, concrete items.
- Prefer minimal-scope remediation targeting failing components.
- Separate code defects from environment/toolchain issues.
- Respond ONLY with JSON, no extra text."""

    @property
    def name(self) -> str:
        return "Developer Agent"

    @property
    def stage(self) -> int:
        return 3

    @property
    def system_prompt(self) -> str:
        return """You are a Lead Developer Agent in a software development pipeline.
You receive architecture specifications and decompose them into implementable components.

You MUST respond with valid JSON in this exact structure:
{
  "decomposition_strategy": "string describing your approach",
  "components": [
    {
      "name": "string",
      "service": "which architecture service this belongs to",
      "type": "api|worker|frontend|database|middleware|shared-lib",
      "language": "string",
      "framework": "string",
      "description": "what this component does",
      "estimated_loc": number,
      "dependencies": ["names of other components it depends on"],
      "priority": "critical|high|medium"
    }
  ],
  "shared_libraries": ["string", ...],
  "development_order": ["component names in suggested build order"],
  "parallel_groups": [
    ["components that can be built simultaneously"],
    ["next batch after dependencies are met"]
  ]
}

Identify 1-8 distinct components based on system complexity.
Do not force a microservices split when a monolith/modular monolith is more appropriate.
If retry/remediation context is provided, prioritize fixing only failing components
and explicitly address each remediation item from QA output.
Respond ONLY with the JSON, no other text."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        architecture = state.get("architect_output", {})
        requirements = state.get("analyst_output", {})
        legacy_code = state.get("legacy_code", "")
        target_lang = state.get("modernization_language", "")
        db_source = str(state.get("database_source", "")).strip()
        db_target = str(state.get("database_target", "")).strip()
        db_schema = str(state.get("database_schema", "")).strip()

        architecture_compact = self._json_for_prompt(
            architecture,
            max_chars=7000,
            max_depth=4,
            max_items=12,
            max_str=420,
        )
        requirements_summary = self._truncate_text(
            requirements.get("executive_summary", ""),
            max_chars=1500,
        )
        legacy_chunks = self._chunk_text_for_prompt(
            legacy_code,
            chunk_chars=1700,
            max_chunks=3,
        )
        legacy_context = "\n\n".join(
            [
                f"LEGACY CHUNK {idx + 1}/{len(legacy_chunks)}:\n```text\n{chunk}\n```"
                for idx, chunk in enumerate(legacy_chunks)
            ]
        ) if legacy_chunks else "No legacy source code attached."
        schema_excerpt = self._truncate_text(db_schema, max_chars=1600)

        base_msg = f"""Decompose this architecture into implementable components
for parallel development by sub-agents.

ARCHITECTURE:
{architecture_compact}

REQUIREMENTS CONTEXT:
{requirements_summary}

LEGACY SOURCE CODE TO MODERNIZE:
{legacy_context}

TARGET MODERNIZATION LANGUAGE:
{target_lang or "Not specified"}

DATABASE CONVERSION CONTEXT:
- Source engine: {db_source or "Not specified"}
- Target engine: {db_target or "Not specified"}
```sql
{schema_excerpt}
```

Ensure decomposition and generated code prioritize migration fidelity from legacy logic."""

        # If this is a retry after tester feedback, append the failures
        tester_feedback = state.get("tester_feedback")
        if tester_feedback:
            blocking = tester_feedback.get("overall_results", {}).get("blocking_issues", [])
            failed_tests = []
            failed_checks = tester_feedback.get("failed_checks", [])
            retry_targets = self._extract_retry_target_components(tester_feedback)
            retry_diag = state.get("retry_plan", {}).get("pre_retry_diagnosis", {}) if isinstance(state.get("retry_plan", {}), dict) else {}
            for suite_name, suite_data in tester_feedback.get("test_suites", {}).items():
                tests = suite_data.get("tests", suite_data.get("checks", suite_data.get("scenarios", [])))
                for t in tests:
                    status = t.get("status", "") if isinstance(t, dict) else ""
                    result_status = t.get("result", {}).get("status", "") if isinstance(t, dict) else ""
                    if status == "fail" or result_status == "fail":
                        failed_tests.append({
                            "suite": suite_name,
                            "name": t.get("name", "?"),
                            "description": t.get("description", ""),
                            "remediation": t.get("remediation", ""),
                        })
            previous_context = self._collect_retry_component_context(state, retry_targets)

            base_msg += f"""

⚠️  THIS IS A RETRY — THE PREVIOUS CODE FAILED TESTING.
Fix the issues below. Regenerate ONLY the components that need changes.
Do not redesign unrelated services.

BLOCKING ISSUES:
{self._json_for_prompt(blocking, max_chars=1800, max_depth=3, max_items=10, max_str=280)}

FAILED TESTS:
{self._json_for_prompt(failed_tests, max_chars=2600, max_depth=3, max_items=12, max_str=280)}

FAILED CHECK ANALYSIS:
{self._json_for_prompt(failed_checks, max_chars=2000, max_depth=3, max_items=10, max_str=260)}

RETRY TARGET COMPONENTS:
{self._json_for_prompt(sorted(retry_targets), max_chars=700, max_depth=2, max_items=12, max_str=120)}

PRE-RETRY DIAGNOSIS:
{self._json_for_prompt(retry_diag, max_chars=1800, max_depth=3, max_items=12, max_str=260)}

PREVIOUS IMPLEMENTATION SNIPPETS FOR RETRY TARGETS:
{self._json_for_prompt(previous_context, max_chars=2600, max_depth=4, max_items=8, max_str=260)}

Previous code output is available in the state — fix the specific problems."""

        return base_msg

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

    @staticmethod
    def _build_dev_plan(
        decomposition: dict[str, Any],
        target_language: str,
        target_platform: str,
    ) -> dict[str, Any]:
        components = decomposition.get("components", [])
        options = {
            "microservices_count_options": [
                {"value": 1, "label": "Monolith / single deployable"},
                {"value": 2, "label": "Core + edge split"},
                {"value": 3, "label": "Balanced service split"},
                {"value": 5, "label": "Fine-grained microservices"},
            ],
            "split_strategy_options": [
                "domain-driven",
                "layered",
                "hybrid",
            ],
            "target_language_options": [
                target_language or "python",
                "python",
                "nodejs",
                "go",
                "java",
                "csharp",
            ],
            "target_platform_options": [
                target_platform or "docker-local",
                "docker-local",
                "kubernetes",
                "aws",
                "azure",
                "gcp",
            ],
        }
        return {
            "plan_summary": decomposition.get("decomposition_strategy", ""),
            "proposed_components": components,
            "default_microservices_count": max(1, min(5, len(components) or 1)),
            "default_split_strategy": "domain-driven",
            "default_target_language": target_language or "python",
            "default_target_platform": target_platform or "docker-local",
            "options": options,
            "estimated_total_loc": sum(int(c.get("estimated_loc", 0)) for c in components),
        }

    @staticmethod
    def _select_components_with_choices(
        decomposition: dict[str, Any],
        choices: dict[str, Any],
        fallback_parallel: int,
    ) -> list[dict[str, Any]]:
        components = list(decomposition.get("components", []))
        if not components:
            return []

        requested_count = choices.get("microservices_count")
        try:
            requested_count_int = int(requested_count)
        except (TypeError, ValueError):
            requested_count_int = fallback_parallel

        requested_count_int = max(1, min(requested_count_int, len(components)))
        strategy = str(choices.get("split_strategy", "")).strip().lower()

        # Reorder based on requested split strategy (best effort heuristic).
        if strategy == "layered":
            order = {"shared-lib": 0, "middleware": 1, "database": 2, "api": 3, "frontend": 4, "worker": 5}
            components.sort(key=lambda c: order.get(str(c.get("type", "")).lower(), 99))
        elif strategy == "domain-driven":
            prio = {"critical": 0, "high": 1, "medium": 2, "low": 3}
            components.sort(key=lambda c: (prio.get(str(c.get("priority", "")).lower(), 9), str(c.get("name", ""))))

        return components[:requested_count_int]

    @staticmethod
    def _normalize_language(value: str) -> str:
        raw = str(value or "").strip().lower()
        aliases = {
            "python": "python",
            "py": "python",
            "node": "nodejs",
            "nodejs": "nodejs",
            "node.js": "nodejs",
            "javascript": "nodejs",
            "typescript": "typescript",
            "ts": "typescript",
            "go": "go",
            "golang": "go",
            "java": "java",
            "c#": "csharp",
            "csharp": "csharp",
            "dotnet": "csharp",
            "rust": "rust",
        }
        return aliases.get(raw, raw)

    @staticmethod
    def _extract_retry_target_components(tester_feedback: dict[str, Any]) -> set[str]:
        targets: set[str] = set()
        failed_checks = tester_feedback.get("failed_checks", []) if isinstance(tester_feedback, dict) else []
        if isinstance(failed_checks, list):
            for item in failed_checks:
                if not isinstance(item, dict):
                    continue
                severity = str(item.get("severity", "")).lower()
                component = str(item.get("component", "")).strip()
                if severity == "critical" and component:
                    targets.add(component.lower())
        return targets

    @staticmethod
    def _extract_retry_targets_from_diagnosis(diagnosis: dict[str, Any]) -> set[str]:
        targets: set[str] = set()
        if not isinstance(diagnosis, dict):
            return targets
        strategy = diagnosis.get("retry_strategy", {})
        regen = strategy.get("components_to_regenerate", []) if isinstance(strategy, dict) else []
        if isinstance(regen, list):
            for item in regen:
                name = str(item).strip()
                if name:
                    targets.add(name.lower())
        return targets

    @staticmethod
    def _collect_retry_component_context(state: dict[str, Any], retry_targets: set[str]) -> list[dict[str, Any]]:
        if not retry_targets:
            return []
        previous_impl = (
            state.get("developer_output", {}).get("implementations", [])
            if isinstance(state.get("developer_output", {}), dict)
            else []
        )
        if not isinstance(previous_impl, list):
            return []
        context_items: list[dict[str, Any]] = []
        for impl in previous_impl:
            if not isinstance(impl, dict):
                continue
            name = str(impl.get("component_name", "")).strip()
            if not name or name.lower() not in retry_targets:
                continue
            files = impl.get("files", []) if isinstance(impl.get("files", []), list) else []
            file_snippets: list[dict[str, str]] = []
            for file_spec in files[:4]:
                if not isinstance(file_spec, dict):
                    continue
                file_snippets.append(
                    {
                        "path": str(file_spec.get("path", "")),
                        "code_snippet": str(file_spec.get("code", ""))[:1200],
                    }
                )
            context_items.append(
                {
                    "component_name": name,
                    "language": str(impl.get("language", "")),
                    "files": file_snippets,
                }
            )
        return context_items[:6]

    def generate_plan(self, state: dict[str, Any]) -> tuple[dict[str, Any], int, float, str]:
        """
        Generate a developer plan (decomposition + configurable options) without coding.
        """
        user_msg = self.build_user_message(state)
        response = self.llm.invoke(self.effective_system_prompt(state), user_msg)
        decomposition = self.parse_output(response.content)

        target_language = str(state.get("modernization_language", "")).strip().lower()
        target_platform = str(state.get("deployment_target", "docker-local")).strip().lower()
        plan = self._build_dev_plan(
            decomposition=decomposition,
            target_language=target_language,
            target_platform=target_platform,
        )
        return (
            plan,
            response.input_tokens + response.output_tokens,
            response.latency_ms,
            response.content,
        )

    def generate_retry_diagnosis(
        self,
        state: dict[str, Any],
        tester_output: dict[str, Any],
    ) -> tuple[dict[str, Any], int, float, str]:
        """
        Generate a focused diagnosis artifact before retrying developer generation.
        """
        developer_output = state.get("developer_output", {})
        architecture = state.get("architect_output", {})
        requirements = state.get("analyst_output", {})
        tester_output_compact = self._json_for_prompt(
            tester_output,
            max_chars=5000,
            max_depth=4,
            max_items=12,
            max_str=320,
        )
        dev_summary_compact = self._json_for_prompt(
            {
                "total_components": developer_output.get("total_components", 0) if isinstance(developer_output, dict) else 0,
                "total_files": developer_output.get("total_files", 0) if isinstance(developer_output, dict) else 0,
                "implementations": [
                    {
                        "component_name": i.get("component_name"),
                        "language": i.get("language"),
                        "file_paths": [f.get("path") for f in i.get("files", [])][:12],
                    }
                    for i in (developer_output.get("implementations", []) if isinstance(developer_output, dict) else [])
                ][:12],
            },
            max_chars=2600,
            max_depth=4,
            max_items=12,
            max_str=260,
        )
        architecture_compact = self._json_for_prompt(
            architecture,
            max_chars=2800,
            max_depth=3,
            max_items=10,
            max_str=260,
        )
        req_summary = self._truncate_text(requirements.get("executive_summary", ""), max_chars=900)
        user_msg = f"""Diagnose the QA failure and produce a deterministic retry plan.

TESTER OUTPUT:
{tester_output_compact}

CURRENT DEVELOPER OUTPUT SUMMARY:
{dev_summary_compact}

ARCHITECTURE CONTEXT:
{architecture_compact}

REQUIREMENTS SUMMARY:
{req_summary}
"""
        response = self.llm.invoke(self.effective_system_prompt(state, self.RETRY_DIAGNOSIS_PROMPT), user_msg)
        parsed = self.parse_output(response.content)
        return (
            parsed,
            response.input_tokens + response.output_tokens,
            response.latency_ms,
            response.content,
        )

    def _plan_sub_agent_spawns(
        self,
        state: dict[str, Any],
        components: list[dict[str, Any]],
        requirements: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], int, float]:
        """
        Use model tool-calling to choose which components to implement in parallel.

        Returns:
            selected_components, planner_tokens_used, planner_latency_ms
        """
        if not components:
            return [], 0, 0.0

        # Build lookup table by name to resolve tool arguments back to full component specs.
        component_by_name = {
            str(c.get("name", "")).strip(): c for c in components if c.get("name")
        }

        tool_schema = [
            {
                "type": "function",
                "function": {
                    "name": "spawn_sub_agent",
                    "description": "Select a component for parallel implementation by a developer sub-agent.",
                    "parameters": {
                        "type": "object",
                        "properties": {
                            "component_name": {
                                "type": "string",
                                "description": "Exact component name from the candidate list.",
                            },
                            "rationale": {
                                "type": "string",
                                "description": "Why this component should be implemented now.",
                            },
                        },
                        "required": ["component_name"],
                        "additionalProperties": False,
                    },
                },
            }
        ]

        planner_user_msg = f"""Select components for immediate implementation.
Call the spawn_sub_agent tool for each selected component.

Candidate components:
{self._json_for_prompt(components, max_chars=3200, max_depth=4, max_items=14, max_str=240)}

Requirements context:
{self._truncate_text(requirements.get("executive_summary", ""), max_chars=1100)}
"""

        try:
            planned = self.llm.invoke_with_tools(
                self.effective_system_prompt(state, self.TOOL_PLANNER_PROMPT),
                planner_user_msg,
                tools=tool_schema,
                tool_choice="auto",
            )
            calls = planned.tool_calls or []
            selected: list[dict[str, Any]] = []
            seen: set[str] = set()

            for call in calls:
                if call.get("name") != "spawn_sub_agent":
                    continue
                args = call.get("arguments", {}) or {}
                comp_name = str(args.get("component_name", "")).strip()
                if not comp_name or comp_name in seen:
                    continue
                component = component_by_name.get(comp_name)
                if component:
                    selected.append(component)
                    seen.add(comp_name)

            # Fallback if model skipped tool calls or returned invalid names.
            if not selected:
                selected = components[: self.llm.config.developer_parallel_agents]
                self.log(
                    f"[{self.name}] Tool planner returned no usable calls; using first "
                    f"{len(selected)} components as fallback."
                )
            else:
                self.log(
                    f"[{self.name}] Tool planner selected {len(selected)} components "
                    f"for parallel implementation."
                )

            return selected, planned.input_tokens + planned.output_tokens, planned.latency_ms
        except Exception as e:
            # Resilient fallback: keep pipeline moving even if tool-calling fails.
            fallback = components[: self.llm.config.developer_parallel_agents]
            self.log(f"[{self.name}] Tool planner unavailable ({e}); using fallback selection.")
            return fallback, 0, 0.0

    def run(self, state: dict[str, Any]) -> "AgentResult":
        """Override run to include parallel sub-agent execution."""
        from .base import AgentResult

        self._logs = []
        self.log(f"[{self.name}] Phase 1: Decomposing architecture into components...")

        # Phase 1: Decompose (reuse pre-approved plan when available)
        response_content = ""
        decomposition: dict[str, Any]
        tokens_used = 0
        latency_ms = 0.0

        if state.get("developer_plan_approved") and state.get("developer_plan"):
            plan = state.get("developer_plan", {})
            decomposition = {
                "decomposition_strategy": plan.get("plan_summary", "Approved developer plan"),
                "components": plan.get("proposed_components", []),
                "shared_libraries": [],
                "development_order": [c.get("name", "") for c in plan.get("proposed_components", [])],
                "parallel_groups": [],
            }
            self.log(
                f"[{self.name}] Reusing approved developer plan with "
                f"{len(decomposition.get('components', []))} proposed components"
            )
        else:
            user_msg = self.build_user_message(state)
            try:
                response = self.llm.invoke(self.effective_system_prompt(state), user_msg)
                decomposition = self.parse_output(response.content)
                response_content = response.content
                tokens_used += response.input_tokens + response.output_tokens
                latency_ms += response.latency_ms
                self.log(
                    f"[{self.name}] Identified {len(decomposition.get('components', []))} components"
                )
            except Exception as e:
                self.log(f"[{self.name}] ERROR in decomposition: {e}")
                return AgentResult(
                    agent_name=self.name,
                    stage=self.stage,
                    status="error",
                    summary=f"Decomposition failed: {e}",
                    output={"error": str(e)},
                    raw_response="",
                    logs=self._logs.copy(),
                )

        choices = state.get("developer_choices", {}) if isinstance(state.get("developer_choices"), dict) else {}
        if choices:
            target_lang = str(choices.get("target_language", "")).strip().lower()
            if target_lang:
                state = dict(state)
                state["modernization_language"] = target_lang

        try:
            selected_from_choices = self._select_components_with_choices(
                decomposition=decomposition,
                choices=choices,
                fallback_parallel=self.llm.config.developer_parallel_agents,
            )
        except Exception as e:
            self.log(f"[{self.name}] ERROR in decomposition: {e}")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary=f"Decomposition failed: {e}",
                output={"error": f"Invalid developer choices: {e}"},
                raw_response="",
                logs=self._logs.copy(),
            )

        # Phase 2: Parallel sub-agent execution
        candidate_components = list(selected_from_choices or decomposition.get("components", []))
        self_heal_applied: list[dict[str, Any]] = []

        tester_feedback = state.get("tester_feedback", {}) if isinstance(state.get("tester_feedback"), dict) else {}
        retry_targets = self._extract_retry_target_components(tester_feedback)
        retry_diagnosis = state.get("retry_plan", {}).get("pre_retry_diagnosis", {}) if isinstance(state.get("retry_plan", {}), dict) else {}
        retry_targets.update(self._extract_retry_targets_from_diagnosis(retry_diagnosis))
        failed_checks = tester_feedback.get("failed_checks", []) if isinstance(tester_feedback.get("failed_checks"), list) else []
        exclusions = {
            str(item.get("component", "")).strip().lower()
            for item in failed_checks
            if isinstance(item, dict)
            and str(item.get("name", "")).startswith("unsupported::")
            and str(item.get("component", "")).strip()
        }
        suggested_exclusions = tester_feedback.get("failure_analysis", {}).get("suggested_component_exclusions", [])
        if isinstance(suggested_exclusions, list):
            exclusions.update(str(x).strip().lower() for x in suggested_exclusions if str(x).strip())
        if exclusions:
            before = len(candidate_components)
            candidate_components = [
                c for c in candidate_components if str(c.get("name", "")).strip().lower() not in exclusions
            ]
            if len(candidate_components) != before:
                self_heal_applied.append(
                    {
                        "action": "exclude_non_executable_components",
                        "components": sorted(exclusions),
                    }
                )
                self.log(
                    f"[{self.name}] Self-heal: excluded non-executable components "
                    f"{sorted(exclusions)} from retry build set"
                )

        if retry_targets:
            focused = [
                c for c in candidate_components if str(c.get("name", "")).strip().lower() in retry_targets
            ]
            if focused:
                candidate_components = focused
                self_heal_applied.append(
                    {
                        "action": "focus_retry_targets",
                        "components": sorted(retry_targets),
                    }
                )
                self.log(
                    f"[{self.name}] Self-heal: focused retry build on critical components "
                    f"{sorted(retry_targets)}"
                )

        target_language_raw = str(state.get("modernization_language", "")).strip()
        normalized_target = self._normalize_language(target_language_raw)
        if normalized_target:
            for comp in candidate_components:
                comp_lang = self._normalize_language(str(comp.get("language", "")))
                if comp_lang and comp_lang != normalized_target:
                    from_lang = str(comp.get("language", ""))
                    self.log(
                        f"[{self.name}] Self-heal: aligning component `{comp.get('name', '')}` "
                        f"language `{from_lang}` -> `{target_language_raw}`"
                    )
                    comp["language"] = target_language_raw
                    self_heal_applied.append(
                        {
                            "action": "align_component_language",
                            "component": comp.get("name", ""),
                            "from": from_lang,
                            "to": target_language_raw,
                        }
                    )

        planner_used_tooling = True
        if retry_targets and candidate_components:
            planner_used_tooling = False
            planner_tokens = 0
            planner_latency_ms = 0.0
            components = candidate_components[: self.llm.config.developer_parallel_agents]
            self.log(
                f"[{self.name}] Retry remediation mode: deterministic component selection "
                f"({len(components)} components)."
            )
        else:
            components, planner_tokens, planner_latency_ms = self._plan_sub_agent_spawns(
                state,
                candidate_components,
                state.get("analyst_output", {}),
            )

        if not components:
            self.log(f"[{self.name}] No components available for implementation")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary="No components selected for implementation",
                output={"error": "No components selected for implementation"},
                raw_response=response_content,
                tokens_used=tokens_used + planner_tokens,
                latency_ms=latency_ms + planner_latency_ms,
                logs=self._logs.copy(),
            )

        # Enforce configured parallel cap after tool selection.
        components = components[: self.llm.config.developer_parallel_agents]
        self.log(
            f"[{self.name}] Phase 2: Spawning {len(components)} parallel sub-agents..."
        )

        sub_results = []
        total_loc = 0
        subagent_tokens = 0
        subagent_latency_ms = 0.0
        failures_by_component: dict[str, list[dict[str, Any]]] = {}
        previous_files_by_component: dict[str, list[dict[str, str]]] = {}
        component_dispatches: list[dict[str, Any]] = []
        prerequisite_gap_reports: list[dict[str, Any]] = []

        if isinstance(failed_checks, list):
            for failure in failed_checks:
                if not isinstance(failure, dict):
                    continue
                comp = str(failure.get("component", "")).strip().lower()
                if not comp:
                    continue
                failures_by_component.setdefault(comp, []).append({
                    "name": failure.get("name", ""),
                    "root_cause": failure.get("root_cause", ""),
                    "remediation": failure.get("remediation", ""),
                    "stderr_snippet": str(failure.get("stderr_snippet", ""))[:1200],
                })

        previous_impl = (
            state.get("developer_output", {}).get("implementations", [])
            if isinstance(state.get("developer_output", {}), dict)
            else []
        )
        architect_output = state.get("architect_output", {}) if isinstance(state.get("architect_output", {}), dict) else {}
        architect_handoff_package = (
            architect_output.get("architect_handoff_package", {})
            if isinstance(architect_output.get("architect_handoff_package", {}), dict)
            else state.get("architect_handoff_package", {})
            if isinstance(state.get("architect_handoff_package", {}), dict)
            else {}
        )
        if isinstance(previous_impl, list):
            for impl in previous_impl:
                if not isinstance(impl, dict):
                    continue
                comp = str(impl.get("component_name", "")).strip().lower()
                if not comp:
                    continue
                file_snips: list[dict[str, str]] = []
                files = impl.get("files", []) if isinstance(impl.get("files", []), list) else []
                for file_spec in files[:4]:
                    if not isinstance(file_spec, dict):
                        continue
                    file_snips.append(
                        {
                            "path": str(file_spec.get("path", "")),
                            "code_snippet": str(file_spec.get("code", ""))[:1200],
                        }
                    )
                previous_files_by_component[comp] = file_snips

        # Use ThreadPoolExecutor for parallel LLM calls
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(components))) as executor:
            futures = {}
            for comp in components:
                comp_name_norm = str(comp.get("name", "")).strip().lower()
                component_handoff = build_component_scoped_handoff(
                    architect_handoff_package,
                    str(comp.get("name", "")),
                )
                gap_report = evaluate_component_prerequisites(component_handoff)
                dispatch_record = _handoff_dispatch_record(component_handoff)
                if gap_report.get("status") == "BLOCKED":
                    dispatch_record["prerequisite_status"] = "BLOCKED"
                    dispatch_record["hard_blocker_count"] = len(gap_report.get("hard_blockers", []))
                    dispatch_record["soft_blocker_count"] = len(gap_report.get("soft_blockers", []))
                    dispatch_record["completeness_score"] = gap_report.get("completeness_score")
                    prerequisite_gap_reports.append(gap_report)
                    self.log(
                        f"[{self.name}]   ⛔ {comp.get('name', '')} blocked by prerequisites "
                        f"(hard={len(gap_report.get('hard_blockers', []))}, "
                        f"soft={len(gap_report.get('soft_blockers', []))})"
                    )
                else:
                    dispatch_record["prerequisite_status"] = "READY"
                    dispatch_record["hard_blocker_count"] = 0
                    dispatch_record["soft_blocker_count"] = 0
                    dispatch_record["completeness_score"] = gap_report.get("completeness_score")
                component_dispatches.append(dispatch_record)
                if is_component_blocked(gap_report):
                    continue
                sub = DeveloperSubAgent(
                    self.llm,
                    comp,
                    state.get("analyst_output", {}),
                    component_handoff=component_handoff,
                    modernization_language=state.get("modernization_language", ""),
                    legacy_code_excerpt=str(state.get("legacy_code", ""))[:8000],
                    remediation_notes=failures_by_component.get(comp_name_norm, []),
                    previous_code_context=previous_files_by_component.get(comp_name_norm, []),
                )
                future = executor.submit(sub.run)
                futures[future] = comp["name"]
                self.log(f"[{self.name}]   ↳ Spawned sub-agent: {comp['name']}")

            if prerequisite_gap_reports:
                self.log(
                    f"[{self.name}] Developer dispatch blocked: "
                    f"{len(prerequisite_gap_reports)} component(s) missing required handoff inputs"
                )
                output = {
                    "error": "Developer prerequisites not satisfied",
                    "prerequisite_gap_report": {
                        "status": "BLOCKED",
                        "generated_at": datetime.utcnow().isoformat() + "Z",
                        "component_reports": prerequisite_gap_reports,
                    },
                    "execution": {
                        "planner_used_tool_calling": planner_used_tooling,
                        "planner_selected_components": [c.get("name", "unknown") for c in components],
                        "developer_choices": choices,
                        "component_dispatches": component_dispatches,
                        "self_heal_applied": self_heal_applied,
                        "retry_target_components": sorted(retry_targets),
                        "generated_at": datetime.utcnow().isoformat() + "Z",
                    },
                    "implementations": [],
                    "total_loc": 0,
                    "total_files": 0,
                    "total_components": 0,
                }
                return AgentResult(
                    agent_name=self.name,
                    stage=self.stage,
                    status="error",
                    summary=(
                        f"Developer blocked by prerequisite gaps in "
                        f"{len(prerequisite_gap_reports)} component(s)"
                    ),
                    output=output,
                    raw_response=response_content,
                    tokens_used=tokens_used + planner_tokens,
                    latency_ms=latency_ms + planner_latency_ms,
                    logs=self._logs.copy(),
                )

            for future in concurrent.futures.as_completed(futures):
                comp_name = futures[future]
                try:
                    result = future.result()
                    loc = result.get("total_loc", 0)
                    total_loc += loc
                    metrics = result.get("_llm_metrics", {})
                    subagent_tokens += int(metrics.get("tokens_used", 0))
                    subagent_latency_ms += float(metrics.get("latency_ms", 0.0))

                    # Do not expose internal metrics in final artifact payload.
                    result.pop("_llm_metrics", None)
                    sub_results.append(result)
                    self.log(
                        f"[{self.name}]   ✓ {comp_name} complete ({loc} LOC, "
                        f"{len(result.get('files', []))} files)"
                    )
                except Exception as e:
                    self.log(f"[{self.name}]   ✗ {comp_name} failed: {e}")
                    sub_results.append({
                        "component_name": comp_name,
                        "error": str(e),
                        "total_loc": 0,
                        "files": [],
                    })

        self.log(f"[{self.name}] All sub-agents complete. Total: {total_loc} LOC")

        output = {
            "decomposition": decomposition,
            "implementations": sub_results,
            "total_loc": total_loc,
            "total_files": sum(len(r.get("files", [])) for r in sub_results),
            "total_components": len(sub_results),
            "execution": {
                "planner_used_tool_calling": planner_used_tooling,
                "planner_selected_components": [c.get("name", "unknown") for c in components],
                "developer_choices": choices,
                "component_dispatches": component_dispatches,
                "self_heal_applied": self_heal_applied,
                "retry_target_components": sorted(retry_targets),
                "generated_at": datetime.utcnow().isoformat() + "Z",
            },
        }

        return AgentResult(
            agent_name=self.name,
            stage=self.stage,
            status="success",
            summary=self._build_summary(output),
            output=output,
            raw_response=response_content,
            tokens_used=(
                tokens_used
                + planner_tokens
                + subagent_tokens
            ),
            latency_ms=latency_ms + planner_latency_ms + subagent_latency_ms,
            logs=self._logs.copy(),
        )

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        return (
            f"Generated {parsed.get('total_loc', 0)} LOC across "
            f"{parsed.get('total_files', 0)} files in "
            f"{parsed.get('total_components', 0)} components "
            f"(via {parsed.get('total_components', 0)} parallel sub-agents)"
        )
