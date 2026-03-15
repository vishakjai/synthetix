"""
Agent 3: Developer Agent
Decomposes architecture into components and spawns parallel sub-agents for implementation.
Each sub-agent generates code for a specific service/component.
"""

from __future__ import annotations

import json
import os
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

    REPAIR_PROMPT = """You repair Developer sub-agent responses into strict JSON.
Return exactly one valid JSON object. No markdown. No explanation.
Preserve the implementation intent and code content where possible.

Required top-level keys:
- component_name
- language
- framework
- files
- dependencies
- environment_variables
- docker_support
- total_loc
- notes

Rules:
- files must be a non-empty array
- every file entry must include path, description, code, and lines_of_code
- never leave files[].code empty
- keep code as plain string values; do not wrap code in markdown fences"""

    GENERATION_TOOLS = [
        {
            "type": "function",
            "function": {
                "name": "emit_component_artifact",
                "description": "Return the generated component implementation as structured files and metadata.",
                "parameters": {
                    "type": "object",
                    "properties": {
                        "component_name": {"type": "string"},
                        "language": {"type": "string"},
                        "framework": {"type": "string"},
                        "files": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "path": {"type": "string"},
                                    "description": {"type": "string"},
                                    "code": {"type": "string"},
                                    "lines_of_code": {"type": "number"},
                                },
                                "required": ["path", "description", "code", "lines_of_code"],
                            },
                        },
                        "dependencies": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "environment_variables": {
                            "type": "array",
                            "items": {"type": "string"},
                        },
                        "docker_support": {"type": "boolean"},
                        "total_loc": {"type": "number"},
                        "notes": {"type": "string"},
                    },
                    "required": [
                        "component_name",
                        "language",
                        "framework",
                        "files",
                        "dependencies",
                        "environment_variables",
                        "docker_support",
                        "total_loc",
                        "notes",
                    ],
                },
            },
        }
    ]

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

    @staticmethod
    def _validate_generated_files(parsed: dict[str, Any]) -> str | None:
        files = parsed.get("files", [])
        if not isinstance(files, list) or not files:
            return "No implementation files generated"
        for file_entry in files:
            if not isinstance(file_entry, dict):
                return "Generated file payload is invalid"
            path = str(file_entry.get("path", "")).strip()
            code = str(file_entry.get("code", "")).strip()
            if not path or not code:
                return "Generated file payload is invalid"
        return None

    def _invoke_generation(self, user_msg: str):
        fallback_model = str(os.getenv("DEVELOPER_SUBAGENT_OPENAI_FALLBACK_MODEL", "gpt-4o")).strip()
        current_model = str(getattr(self.llm.config, "get_model", lambda: "")() or "").strip()
        try:
            response = self.llm.invoke_with_tools(
                self.SYSTEM_PROMPT,
                user_msg,
                self.GENERATION_TOOLS,
                tool_choice="required",
            )
            tool_calls = getattr(response, "tool_calls", None)
            raw_content = getattr(response, "content", "")
            content = raw_content.strip() if isinstance(raw_content, str) else ""
            if (isinstance(tool_calls, list) and tool_calls) or content:
                return response
        except Exception:
            pass
        response = self.llm.invoke(self.SYSTEM_PROMPT, user_msg)
        raw_content = getattr(response, "content", "")
        content = raw_content.strip() if isinstance(raw_content, str) else ""
        if content:
            return response
        if (
            str(getattr(self.llm.config, "provider", "")).lower().endswith("openai")
            and fallback_model
            and fallback_model.lower() != current_model.lower()
        ):
            try:
                fallback_response = self.llm.invoke_with_tools(
                    self.SYSTEM_PROMPT,
                    user_msg,
                    self.GENERATION_TOOLS,
                    tool_choice="required",
                    model_override=fallback_model,
                )
                fallback_tool_calls = getattr(fallback_response, "tool_calls", None)
                fallback_raw_content = getattr(fallback_response, "content", "")
                fallback_content = fallback_raw_content.strip() if isinstance(fallback_raw_content, str) else ""
                if (isinstance(fallback_tool_calls, list) and fallback_tool_calls) or fallback_content:
                    return fallback_response
            except Exception:
                pass
            return self.llm.invoke(self.SYSTEM_PROMPT, user_msg, model_override=fallback_model)
        return response

    @staticmethod
    def _aggregate_metrics(responses: list[Any]) -> dict[str, Any]:
        def _safe_int(value: Any) -> int:
            try:
                return int(value or 0)
            except (TypeError, ValueError):
                return 0

        def _safe_float(value: Any) -> float:
            try:
                return float(value or 0.0)
            except (TypeError, ValueError):
                return 0.0

        return {
            "tokens_used": sum(
                _safe_int(getattr(r, "input_tokens", 0)) + _safe_int(getattr(r, "output_tokens", 0))
                for r in responses
            ),
            "latency_ms": sum(_safe_float(getattr(r, "latency_ms", 0.0)) for r in responses),
        }

    def _try_parse_tool_payload(self, tool_calls: list[dict[str, Any]] | None) -> tuple[dict[str, Any] | None, str | None]:
        if not isinstance(tool_calls, list) or not tool_calls:
            return None, "No tool payload returned"
        for call in tool_calls:
            if not isinstance(call, dict):
                continue
            arguments = call.get("arguments", {})
            if not isinstance(arguments, dict):
                continue
            file_error = self._validate_generated_files(arguments)
            if file_error:
                return None, file_error
            return arguments, None
        return None, "No tool payload returned"

    def _required_file_hints(self) -> list[str]:
        component_name = str(self.component.get("name", "Component")).strip() or "Component"
        component_type = str(self.component.get("type", "")).strip().lower()
        language = str(self.modernization_language or self.component.get("language", "")).strip().lower()
        if "c#" in language or "csharp" in language:
            project_file = f"{component_name}.csproj"
            if component_type == "frontend":
                return [
                    "Program.cs",
                    project_file,
                    "Pages/Index.cshtml",
                    "Pages/_ViewImports.cshtml",
                    "Dockerfile",
                    "README.md",
                    "Tests/SmokeTests.cs",
                ]
            return [
                "Program.cs",
                project_file,
                "Controllers/HealthController.cs",
                "Dockerfile",
                "README.md",
                "Tests/SmokeTests.cs",
            ]
        if "python" in language:
            return ["main.py", "requirements.txt", "Dockerfile", "README.md", "tests/test_smoke.py"]
        if "typescript" in language or "javascript" in language or "node" in language:
            return ["package.json", "src/index.ts", "Dockerfile", "README.md", "tests/smoke.test.ts"]
        return ["Dockerfile", "README.md"]

    def _build_user_message(self) -> str:
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
        required_file_hints = "\n".join(f"- {path}" for path in self._required_file_hints())
        return f"""Generate COMPLETE, RUNNABLE code for this component.
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

MINIMUM FILE CHECKLIST:
{required_file_hints}

OUTPUT RULES:
- Return a single JSON object only
- Do not use markdown fences
- files must be non-empty
- every files[] entry must include non-empty path, description, and code
- keep the implementation minimal but complete; prefer 5-8 files unless the component is tiny
- when unsure, choose the smallest compilable vertical slice that honors the handoff contracts

REMEMBER:
- Include a requirements.txt (or package.json / project file) with exact dependency versions
- Include a working Dockerfile
- The app MUST listen on port 8080 (or $PORT) and serve /health and /ready endpoints
- Generate ALL necessary files — this must build and run as-is with zero manual edits
- Preserve functional parity with the legacy behavior"""

    def _try_parse_payload(self, raw: str) -> tuple[dict[str, Any] | None, str | None]:
        try:
            parsed = _extract_json(raw)
        except (json.JSONDecodeError, AttributeError):
            return None, "Failed to parse sub-agent output"
        file_error = self._validate_generated_files(parsed)
        if file_error:
            return None, file_error
        return parsed, None

    def _repair_json_response(self, raw: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        text = str(raw or "").strip()
        if not text:
            return None, None
        repair_user = f"""The previous Developer sub-agent response was invalid.
Rewrite it into a strict JSON object that satisfies the required schema.

SOURCE RESPONSE:
```text
{text[:24000]}
```"""
        try:
            repaired = self.llm.invoke(self.REPAIR_PROMPT, repair_user)
            parsed, error = self._try_parse_payload(repaired.content)
            if parsed is None:
                return None, {
                    "response": repaired,
                    "error": error or "Failed to parse repaired sub-agent output",
                }
            return parsed, {
                "response": repaired,
                "error": "",
            }
        except Exception:
            return None, None

    def _retry_generation(self, failure_reason: str, previous_raw: str) -> tuple[dict[str, Any] | None, dict[str, Any] | None]:
        retry_user = f"""{self._build_user_message()}

PREVIOUS RESPONSE FAILURE:
- {failure_reason}

PREVIOUS INVALID RESPONSE EXCERPT:
```text
{str(previous_raw or '')[:4000]}
```

Retry now. Return only the corrected JSON object."""
        retry_response = self._invoke_generation(retry_user)
        parsed, error = self._try_parse_tool_payload(getattr(retry_response, "tool_calls", []))
        if parsed is None:
            parsed, error = self._try_parse_payload(retry_response.content)
        if parsed is not None:
            return parsed, {
                "response": retry_response,
                "error": "",
            }
        repaired, repair_meta = self._repair_json_response(retry_response.content)
        if repaired is not None:
            return repaired, repair_meta
        return None, {
            "response": retry_response,
            "error": error or "Failed to parse sub-agent retry output",
        }

    def run(self) -> dict[str, Any]:
        responses = []
        user_msg = self._build_user_message()
        response = self._invoke_generation(user_msg)
        responses.append(response)

        parsed, error = self._try_parse_tool_payload(getattr(response, "tool_calls", []))
        if parsed is None:
            parsed, error = self._try_parse_payload(response.content)
        if parsed is None:
            repaired, repair_meta = self._repair_json_response(response.content)
            if repair_meta and isinstance(repair_meta.get("response"), object):
                repaired_response = repair_meta.get("response")
                if repaired_response is not None:
                    responses.append(repaired_response)
            if repaired is not None:
                parsed = repaired
            else:
                retried, retry_meta = self._retry_generation(error or "Failed to parse sub-agent output", response.content)
                if retry_meta and isinstance(retry_meta.get("response"), object):
                    retry_response = retry_meta.get("response")
                    if retry_response is not None:
                        responses.append(retry_response)
                if retried is not None:
                    parsed = retried
                else:
                    final_error = ""
                    if retry_meta and str(retry_meta.get("error", "")).strip():
                        final_error = str(retry_meta.get("error", "")).strip()
                    elif repair_meta and str(repair_meta.get("error", "")).strip():
                        final_error = str(repair_meta.get("error", "")).strip()
                    else:
                        final_error = error or "Failed to parse sub-agent output"
                    return {
                        "component_name": self.component.get("name", "unknown"),
                        "error": final_error,
                        "raw": str((responses[-1].content if responses else response.content) or "")[:500],
                        "total_loc": 0,
                        "files": [],
                        "_llm_metrics": self._aggregate_metrics(responses),
                    }

        parsed["_llm_metrics"] = self._aggregate_metrics(responses)
        return parsed


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
        "phase_number": _dispatch_phase_number(handoff),
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


def _dispatch_phase_number(component_handoff: dict[str, Any]) -> int:
    handoff = component_handoff if isinstance(component_handoff, dict) else {}
    component_spec = handoff.get("component_spec", {}) if isinstance(handoff.get("component_spec", {}), dict) else {}
    dependency_graph = component_spec.get("dependency_graph", {}) if isinstance(component_spec.get("dependency_graph", {}), dict) else {}
    try:
        phase_number = int(dependency_graph.get("phase", 0) or 0)
    except (TypeError, ValueError):
        phase_number = 0
    if phase_number > 0:
        return phase_number
    wbs_items = handoff.get("wbs_items", []) if isinstance(handoff.get("wbs_items", []), list) else []
    for item in wbs_items:
        if not isinstance(item, dict):
            continue
        phase_id = str(item.get("phase_id", "") or item.get("wbs_id", "")).strip()
        if phase_id.startswith("WBS-PHASE-"):
            try:
                return int(phase_id.rsplit("-", 1)[-1])
            except (TypeError, ValueError):
                continue
    return 99


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
    def _normalize_component_key(value: Any) -> str:
        return "".join(ch for ch in str(value or "").lower() if ch.isalnum())

    @classmethod
    def _architect_component_catalog(
        cls,
        architect_handoff_package: dict[str, Any],
        target_language: str,
    ) -> list[dict[str, Any]]:
        package = architect_handoff_package if isinstance(architect_handoff_package, dict) else {}
        specs = package.get("component_specs", []) if isinstance(package.get("component_specs", []), list) else []
        if not specs:
            return []

        raw_language = str(target_language or "").strip() or "python"
        normalized_language = cls._normalize_language(raw_language)
        framework_by_language = {
            "csharp": "ASP.NET Core 8",
            "python": "FastAPI",
            "nodejs": "Express",
            "go": "Go net/http",
            "java": "Spring Boot",
            "typescript": "NestJS",
            "rust": "Axum",
        }

        def _phase(spec: dict[str, Any]) -> int:
            graph = spec.get("dependency_graph", {}) if isinstance(spec.get("dependency_graph", {}), dict) else {}
            try:
                return max(1, int(graph.get("phase", 99) or 99))
            except (TypeError, ValueError):
                return 99

        catalog: list[dict[str, Any]] = []
        valid_specs = [
            row for row in specs
            if isinstance(row, dict) and str(row.get("component_name", "")).strip()
        ]
        for spec in sorted(valid_specs, key=lambda row: (_phase(row), str(row.get("component_name", "")).strip())):
            name = str(spec.get("component_name", "")).strip()
            responsibility = str(spec.get("responsibility", "")).strip()
            module_structure = spec.get("module_structure", []) if isinstance(spec.get("module_structure", []), list) else []
            interface_refs = spec.get("interface_refs", []) if isinstance(spec.get("interface_refs", []), list) else []
            business_rule_refs = spec.get("business_rule_refs", []) if isinstance(spec.get("business_rule_refs", []), list) else []
            regression_anchor_refs = spec.get("regression_anchor_refs", []) if isinstance(spec.get("regression_anchor_refs", []), list) else []
            phase_number = _phase(spec)
            name_norm = cls._normalize_component_key(name)
            if "shell" in name_norm or "experience" in name_norm:
                component_type = "frontend"
            elif "legacycore" in name_norm:
                component_type = "shared-lib"
            else:
                component_type = "api"
            estimated_loc = max(
                400,
                (len(module_structure) * 180)
                + (len(interface_refs) * 70)
                + (len(business_rule_refs) * 35)
                + (len(regression_anchor_refs) * 25),
            )
            priority = "critical" if phase_number <= 1 else "high" if phase_number == 2 else "medium"
            catalog.append(
                {
                    "name": name,
                    "service": name,
                    "type": component_type,
                    "language": raw_language,
                    "framework": framework_by_language.get(normalized_language, raw_language),
                    "description": responsibility or f"Implements the {name} modernization boundary.",
                    "estimated_loc": estimated_loc,
                    "dependencies": [],
                    "priority": priority,
                    "_dispatch_phase": phase_number,
                }
            )
        return catalog

    @classmethod
    def _anchor_decomposition_to_handoff(
        cls,
        decomposition: dict[str, Any],
        architect_handoff_package: dict[str, Any],
        target_language: str,
    ) -> dict[str, Any]:
        catalog = cls._architect_component_catalog(architect_handoff_package, target_language)
        if not catalog:
            return decomposition

        catalog_by_name = {
            cls._normalize_component_key(component.get("name", "")): component
            for component in catalog
        }
        anchored: list[dict[str, Any]] = []
        seen: set[str] = set()
        candidate_components = decomposition.get("components", []) if isinstance(decomposition.get("components", []), list) else []

        for component in candidate_components:
            if not isinstance(component, dict):
                continue
            candidate_keys = [
                cls._normalize_component_key(component.get("service", "")),
                cls._normalize_component_key(component.get("name", "")),
            ]
            match = next((catalog_by_name.get(key) for key in candidate_keys if key and catalog_by_name.get(key)), None)
            if not match:
                continue
            canonical_name = str(match.get("name", "")).strip()
            if not canonical_name or canonical_name in seen:
                continue
            merged = dict(match)
            if str(component.get("description", "")).strip():
                merged["description"] = str(component.get("description", "")).strip()
            if int(component.get("estimated_loc", 0) or 0) > 0:
                merged["estimated_loc"] = int(component.get("estimated_loc", 0) or 0)
            anchored.append(merged)
            seen.add(canonical_name)

        if not anchored:
            anchored = [dict(component) for component in catalog]
        else:
            for component in catalog:
                canonical_name = str(component.get("name", "")).strip()
                if canonical_name and canonical_name not in seen:
                    anchored.append(dict(component))
                    seen.add(canonical_name)
        parallel_groups: list[list[str]] = []
        phases: dict[int, list[str]] = {}
        for component in anchored:
            phase_number = int(component.get("_dispatch_phase", 99) or 99)
            phases.setdefault(phase_number, []).append(str(component.get("name", "")).strip())
        for phase_number in sorted(phases):
            group = [name for name in phases[phase_number] if name]
            if group:
                parallel_groups.append(group)

        return {
            "decomposition_strategy": str(decomposition.get("decomposition_strategy", "")).strip() or "Architect-scoped component decomposition",
            "components": [{k: v for k, v in component.items() if not str(k).startswith("_")} for component in anchored],
            "shared_libraries": [
                str(component.get("name", "")).strip()
                for component in anchored
                if str(component.get("type", "")).strip().lower() == "shared-lib"
            ],
            "development_order": [str(component.get("name", "")).strip() for component in anchored if str(component.get("name", "")).strip()],
            "parallel_groups": parallel_groups,
        }

    @staticmethod
    def _architect_handoff_from_state(state: dict[str, Any]) -> dict[str, Any]:
        if isinstance(state.get("architect_handoff_package", {}), dict) and state.get("architect_handoff_package"):
            return state.get("architect_handoff_package", {})
        architect_output = state.get("architect_output", {}) if isinstance(state.get("architect_output", {}), dict) else {}
        if isinstance(architect_output.get("architect_handoff_package", {}), dict):
            return architect_output.get("architect_handoff_package", {})
        return {}

    @staticmethod
    def _build_dev_plan(
        decomposition: dict[str, Any],
        target_language: str,
        target_platform: str,
        architect_handoff_package: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        anchored = DeveloperAgent._anchor_decomposition_to_handoff(
            decomposition,
            architect_handoff_package or {},
            target_language,
        )
        components = anchored.get("components", [])
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
            "plan_summary": anchored.get("decomposition_strategy", ""),
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
        architect_handoff_package = self._architect_handoff_from_state(state)
        plan = self._build_dev_plan(
            decomposition=decomposition,
            target_language=target_language,
            target_platform=target_platform,
            architect_handoff_package=architect_handoff_package,
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

        architect_handoff_package = self._architect_handoff_from_state(state)
        decomposition = self._anchor_decomposition_to_handoff(
            decomposition,
            architect_handoff_package,
            str(state.get("modernization_language", "")).strip(),
        )
        if state.get("developer_plan_approved") and state.get("developer_plan"):
            self.log(
                f"[{self.name}] Reusing approved developer plan with "
                f"{len(decomposition.get('components', []))} architect-scoped components"
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
        architect_handoff_package = self._architect_handoff_from_state(state)
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

        component_dispatch_inputs: list[dict[str, Any]] = []
        for comp in components:
            component_handoff = build_component_scoped_handoff(
                architect_handoff_package,
                str(comp.get("name", "")),
            )
            component_phase = _dispatch_phase_number(component_handoff)
            component_dispatch_inputs.append(
                {
                    "component": comp,
                    "handoff": component_handoff,
                    "phase_number": component_phase,
                }
            )

        dispatch_phase = min(
            (row["phase_number"] for row in component_dispatch_inputs if int(row.get("phase_number", 99)) > 0),
            default=min((row["phase_number"] for row in component_dispatch_inputs), default=99),
        )
        deferred_components: list[dict[str, Any]] = []

        # Use ThreadPoolExecutor for parallel LLM calls
        with concurrent.futures.ThreadPoolExecutor(max_workers=max(1, len(components))) as executor:
            futures = {}
            for dispatch_input in component_dispatch_inputs:
                comp = dispatch_input["component"]
                component_handoff = dispatch_input["handoff"]
                component_phase = int(dispatch_input.get("phase_number", 99) or 99)
                comp_name_norm = str(comp.get("name", "")).strip().lower()
                dispatch_record = _handoff_dispatch_record(component_handoff)
                if component_phase != dispatch_phase:
                    dispatch_record["dispatch_decision"] = "DEFERRED"
                    dispatch_record["dispatch_reason"] = (
                        f"Deferred until phase {component_phase}; current developer dispatch phase is {dispatch_phase}."
                    )
                    component_dispatches.append(dispatch_record)
                    deferred_components.append(
                        {
                            "component_name": comp.get("name", ""),
                            "phase_number": component_phase,
                            "reason": dispatch_record["dispatch_reason"],
                        }
                    )
                    self.log(
                        f"[{self.name}]   ⏸ {comp.get('name', '')} deferred to phase {component_phase} "
                        f"(current dispatch phase: {dispatch_phase})"
                    )
                    continue
                gap_report = evaluate_component_prerequisites(component_handoff)
                if gap_report.get("status") == "BLOCKED":
                    dispatch_record["prerequisite_status"] = "BLOCKED"
                    dispatch_record["hard_blocker_count"] = len(gap_report.get("hard_blockers", []))
                    dispatch_record["soft_blocker_count"] = len(gap_report.get("soft_blockers", []))
                    dispatch_record["completeness_score"] = gap_report.get("completeness_score")
                    dispatch_record["minimum_dispatch_score"] = gap_report.get("minimum_dispatch_score")
                    dispatch_record["dispatch_decision"] = "BLOCKED"
                    dispatch_record["dispatch_reason"] = "Prerequisite gap report contains blockers."
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
                    dispatch_record["minimum_dispatch_score"] = gap_report.get("minimum_dispatch_score")
                    dispatch_record["dispatch_decision"] = "DISPATCHED"
                    dispatch_record["dispatch_reason"] = f"Eligible for current phase {dispatch_phase} dispatch."
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
                        "current_dispatch_phase": dispatch_phase,
                        "deferred_components": deferred_components,
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
                    if str(result.get("error", "")).strip():
                        self.log(
                            f"[{self.name}]   ✗ {comp_name} failed: "
                            f"{str(result.get('error', '')).strip()}"
                        )
                    else:
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

        generation_failures = [
            {
                "component_name": str(result.get("component_name", "")).strip(),
                "error": str(result.get("error", "")).strip() or "Unknown generation failure",
                "total_files": len(result.get("files", [])) if isinstance(result.get("files", []), list) else 0,
                "total_loc": int(result.get("total_loc", 0) or 0),
            }
            for result in sub_results
            if isinstance(result, dict) and (
                str(result.get("error", "")).strip()
                or not isinstance(result.get("files", []), list)
                or not result.get("files", [])
            )
        ]

        if generation_failures:
            self.log(
                f"[{self.name}] Developer generation failed for "
                f"{len(generation_failures)} component(s)"
            )
            output = {
                "error": "Developer sub-agent generation failed",
                "subagent_failure_report": {
                    "status": "BLOCKED",
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                    "component_failures": generation_failures,
                },
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
                    "current_dispatch_phase": dispatch_phase,
                    "deferred_components": deferred_components,
                    "self_heal_applied": self_heal_applied,
                    "retry_target_components": sorted(retry_targets),
                    "generated_at": datetime.utcnow().isoformat() + "Z",
                },
            }
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary=(
                    f"Developer generation failed for "
                    f"{len(generation_failures)} component(s)"
                ),
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
                "current_dispatch_phase": dispatch_phase,
                "deferred_components": deferred_components,
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
