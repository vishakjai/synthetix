"""
Agent 1: Analyst Agent
Parses business objectives into structured requirements with acceptance criteria.

Supports an optional interactive mode where the agent first generates clarifying
questions, presents them to the user, and incorporates the answers into a more
precise requirements document.
"""

from __future__ import annotations

import json
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .base import BaseAgent, AgentResult
from utils.llm import LLMClient
from utils.domain_packs import (
    build_open_questions,
    get_domain_pack,
    infer_data_classification,
    infer_domain_pack_id,
    infer_jurisdiction,
    map_to_capabilities,
    normalize_requirement,
    retrieve_gold_patterns,
    retrieve_regulatory_constraints,
)
from utils.legacy_skills import (
    infer_legacy_skill,
    extract_vb6_signals as legacy_extract_vb6_signals,
    build_vb6_readiness_assessment,
    build_source_target_modernization_profile,
    build_project_business_summaries,
    vb6_skill_pack_manifest,
)
from utils.analyst_report import build_analyst_report_v2, build_raw_artifact_set_v1


class AnalystAgent(BaseAgent):
    LEGACY_INLINE_MAX_CHARS = 12000
    LEGACY_CHUNK_MAX_CHARS = 4500
    LEGACY_CHUNK_MAX_COUNT = 12
    LEGACY_LLM_CHUNK_LIMIT = 3
    DB_SCHEMA_INLINE_MAX_CHARS = 9000
    LEGACY_MAX_FORMS = 80
    LEGACY_MAX_CONTROLS = 240
    LEGACY_MAX_DEPENDENCIES = 80
    LEGACY_MAX_PROJECTS = 24

    QUESTIONS_SYSTEM_PROMPT = """You are a Senior Business Analyst Agent.
You have been given raw business objectives for a new software project.
Before writing requirements, you need to ask clarifying questions to ensure
you fully understand the stakeholder's intent.

Generate 3-5 focused, high-impact clarifying questions that will help you
write better, more precise requirements. Focus on:
- Ambiguous scope (what's in vs. out)
- Target users and personas
- Key constraints (budget, timeline, tech stack preferences)
- Integration points with existing systems
- Performance/scale expectations if not specified
- Priority trade-offs (speed vs. features vs. quality)

You MUST respond with valid JSON in this exact structure:
{
  "questions": [
    {
      "id": "Q1",
      "question": "the clarifying question",
      "why": "brief reason this matters for requirements",
      "options": ["suggested answer 1", "suggested answer 2", "suggested answer 3"]
    }
  ]
}

Keep questions concise and actionable. Provide 2-3 suggested answers per question
so the user can quickly pick one or type their own.
Respond ONLY with the JSON, no other text."""

    @property
    def name(self) -> str:
        return "Analyst Agent"

    @property
    def stage(self) -> int:
        return 1

    @property
    def system_prompt(self) -> str:
        return """You are a Senior Business Analyst Agent in a software development pipeline.
Your job is to take raw business objectives and decompose them into precise, testable
functional and non-functional requirements with acceptance criteria.
You are a deterministic requirements compiler, not a free-form ideation assistant.

You MUST respond with valid JSON in this exact structure:
{
  "analysis_walkthrough": {
    "business_objective_summary": "plain-English summary of the objective",
    "requirements_understanding": ["key understanding bullet", "..."],
    "conversion_to_technical_requirements": ["how this maps to architecture/build choices", "..."],
    "clarifications_requested": ["explicit unresolved question", "..."]
  },
  "project_name": "string",
  "executive_summary": "1-2 sentence summary",
  "functional_requirements": [
    {
      "id": "FR-001",
      "title": "string",
      "description": "string",
      "priority": "P0|P1|P2",
      "acceptance_criteria": ["string", ...]
    }
  ],
  "non_functional_requirements": [
    {
      "id": "NFR-001",
      "title": "string",
      "description": "string",
      "category": "performance|security|scalability|reliability|usability",
      "metric": "measurable target string",
      "acceptance_criteria": ["string", ...]
    }
  ],
  "legacy_functional_contract": [
    {
      "function_name": "string",
      "inputs": ["string", ...],
      "outputs": ["string", ...],
      "side_effects": ["string", ...]
    }
  ],
  "legacy_code_inventory": {
    "summary": "string",
    "vb6_projects": [
      {
        "project_name": "string",
        "project_file": "string",
        "project_type": "string",
        "startup_object": "string",
        "member_count": 0,
        "member_files": ["string", ...],
        "forms": ["string", ...],
        "controls": ["string", ...],
        "activex_dependencies": ["string", ...],
        "event_handlers": ["string", ...],
        "business_objective_hypothesis": "string",
        "key_business_capabilities": ["string", ...],
        "primary_workflows": ["string", ...],
        "data_touchpoints": {
          "tables": ["string", ...],
          "procedures": ["string", ...],
          "input_signals": ["string", ...],
          "output_signals": ["string", ...]
        },
        "technical_components": {
          "notable_components": ["string", ...],
          "external_dependencies": ["string", ...],
          "integration_hints": ["string", ...]
        },
        "modernization_considerations": ["string", ...]
      }
    ],
    "forms": [
      {
        "form_name": "string",
        "form_type": "Form|MDIForm|UserControl",
        "business_use": "string",
        "controls": ["string", ...],
        "event_handlers": ["string", ...]
      }
    ],
    "activex_controls": ["string", ...],
    "dll_dependencies": ["string", ...],
    "ocx_dependencies": ["string", ...],
    "event_handlers": ["string", ...],
    "project_members": ["string", ...],
    "database_tables": ["string", ...],
    "procedures": ["string", ...],
    "input_signals": ["string", ...],
    "side_effect_patterns": ["string", ...],
    "ui_event_map": [
      {
        "event_handler": "string",
        "form": "string",
        "control": "string",
        "event": "string",
        "procedure_calls": ["string", ...],
        "sql_touches": ["string", ...],
        "side_effects": ["string", ...]
      }
    ],
    "sql_query_catalog": ["string", ...],
    "com_surface_map": {
      "late_bound_progids": ["string", ...],
      "call_by_name_sites": 0,
      "createobject_getobject_sites": 0,
      "references": ["string", ...]
    },
    "win32_declares": ["string", ...],
    "error_handling_profile": {
      "on_error_resume_next": 0,
      "on_error_goto": 0,
      "on_error_goto0": 0,
      "control_array_index_markers": 0,
      "late_bound_com_calls": 0,
      "variant_declarations": 0,
      "default_instance_references": 0,
      "doevents_calls": 0,
      "registry_operations": 0
    },
    "pitfall_detectors": [
      {
        "id": "VB6-ERR-001",
        "severity": "high|medium|low",
        "count": 0,
        "requires": ["string", ...],
        "evidence": "string"
      }
    ],
    "modernization_readiness": {
      "score": 0,
      "risk_tier": "low|medium|high",
      "recommended_strategy": {
        "id": "rehost_stabilize|strangler_wrap|phased_ui_migration|full_upgrade_translation",
        "name": "string",
        "rationale": "string"
      },
      "totals": {},
      "penalties": {},
      "required_actions": ["string", ...]
    },
    "business_rules_catalog": [
      {
        "id": "BR-001",
        "rule_type": "calculation_logic|threshold_rule|decision_branching|input_validation|workflow_orchestration|data_persistence",
        "statement": "string",
        "scope": "string",
        "evidence": "file-or-project reference",
        "confidence": 0.0
      }
    ]
  },
  "legacy_skill_profile": {
    "selected_skill_id": "string",
    "selected_skill_name": "string",
    "confidence": 0.0,
    "reasons": ["string", ...]
  },
  "assumptions": ["string", ...],
  "risks": [
    {
      "description": "string",
      "impact": "high|medium|low",
      "mitigation": "string"
    }
  ],
  "out_of_scope": ["string", ...],
  "requirements_pack": {
    "capability_mapping": {
      "framework": "string",
      "primary_capabilities": [{"id": "string", "service_domain": "string", "business_capability": "string", "confidence": 0.0}],
      "alternative_capabilities": [{"id": "string", "service_domain": "string", "business_capability": "string", "confidence": 0.0}]
    },
    "domain_model_excerpt": {
      "entities": ["string", ...],
      "lifecycle_states": ["string", ...],
      "relationships": ["string", ...]
    },
    "regulatory_constraints_applied": [
      {
        "id": "string",
        "name": "string",
        "control_objective": "string",
        "software_actions": ["string", ...],
        "evidence_required": ["string", ...]
      }
    ],
    "bdd_contract": {
      "features": [
        {
          "id": "string",
          "title": "string",
          "source_requirement_ids": ["FR-001"],
          "gherkin": "Feature: ...\\nScenario: ...\\nGiven ...\\nWhen ...\\nThen ..."
        }
      ]
    }
  }
}

Be thorough — generate at least 8 functional requirements and 5 non-functional requirements.
The `analysis_walkthrough` section must be concise, readable, and non-JSON-jargony.
Each requirement MUST have at least 3 acceptance criteria.
For modernization use cases, each requirement description should explicitly mention expected inputs/outputs or data contracts where relevant.
Include concrete acceptance criteria that can be objectively tested (not generic statements).
Use stable IDs for requirements and BDD features so downstream traceability is deterministic.
Respond ONLY with the JSON, no other text."""

    def _build_user_message_with_context(self, state: dict[str, Any], deterministic: dict[str, Any]) -> str:
        objectives = state.get("business_objectives", "")
        answers = state.get("analyst_answers")
        legacy_code = state.get("legacy_code", "")
        target_lang = state.get("modernization_language", "")
        db_source = str(state.get("database_source", "")).strip()
        db_target = str(state.get("database_target", "")).strip()
        db_schema = str(state.get("database_schema", "")).strip()
        use_case = str(state.get("use_case", "business_objectives")).strip().lower()
        legacy_compact = state.get("legacy_compact_context", {})
        legacy_compact = legacy_compact if isinstance(legacy_compact, dict) else {}
        db_compact = state.get("database_compact_context", {})
        db_compact = db_compact if isinstance(db_compact, dict) else {}

        modernization_context = ""
        if legacy_code:
            compact_summary = str(legacy_compact.get("summary", "")).strip()
            if compact_summary:
                contracts = legacy_compact.get("seed_legacy_contract", [])
                inventory = legacy_compact.get("inventory", {}) if isinstance(legacy_compact.get("inventory", {}), dict) else {}
                legacy_skill = (
                    legacy_compact.get("legacy_skill_profile", {})
                    if isinstance(legacy_compact.get("legacy_skill_profile", {}), dict)
                    else {}
                )
                contract_lines: list[str] = []
                if isinstance(contracts, list):
                    for row in contracts[:12]:
                        if not isinstance(row, dict):
                            continue
                        fn = str(row.get("function_name", "")).strip() or "Function"
                        ins = ", ".join(str(x).strip() for x in row.get("inputs", []) if str(x).strip()) if isinstance(row.get("inputs", []), list) else ""
                        outs = ", ".join(str(x).strip() for x in row.get("outputs", []) if str(x).strip()) if isinstance(row.get("outputs", []), list) else ""
                        side = ", ".join(str(x).strip() for x in row.get("side_effects", []) if str(x).strip()) if isinstance(row.get("side_effects", []), list) else ""
                        contract_lines.append(
                            f"- {fn} | inputs: {ins or 'unknown'} | outputs: {outs or 'unknown'} | side effects: {side or 'none'}"
                        )
                forms = inventory.get("forms", []) if isinstance(inventory.get("forms", []), list) else []
                activex = inventory.get("activex_controls", []) if isinstance(inventory.get("activex_controls", []), list) else []
                dlls = inventory.get("dll_dependencies", []) if isinstance(inventory.get("dll_dependencies", []), list) else []
                ocx = inventory.get("ocx_dependencies", []) if isinstance(inventory.get("ocx_dependencies", []), list) else []
                form_lines: list[str] = []
                for item in forms[:10]:
                    if not isinstance(item, dict):
                        continue
                    form_name = str(item.get("form_name", "")).strip() or "Form"
                    form_type = str(item.get("form_type", "")).strip() or "Form"
                    use = str(item.get("business_use", "")).strip() or "business flow"
                    controls = item.get("controls", []) if isinstance(item.get("controls", []), list) else []
                    form_lines.append(
                        f"- {form_type} {form_name} | use: {use} | controls: {len(controls)}"
                    )
                mode_label = "chunked"
                if bool(legacy_compact.get("inline", False)):
                    mode_label = "inline"
                chunk_count = int(legacy_compact.get("chunk_count", 1) or 1)
                modernization_context = f"""

LEGACY SOURCE ANALYSIS ({mode_label}, chunks={chunk_count}):
{compact_summary}

LEGACY SKILL PROFILE:
- Selected skill: {str(legacy_skill.get("selected_skill_name", "Generic Legacy Skill"))}
- Skill ID: {str(legacy_skill.get("selected_skill_id", "generic_legacy"))}
- Confidence: {str(legacy_skill.get("confidence", "n/a"))}
- Rationale: {", ".join([str(x) for x in legacy_skill.get("reasons", [])[:4]]) if isinstance(legacy_skill.get("reasons", []), list) else "n/a"}

LEGACY FUNCTIONAL CONTRACT CANDIDATES:
{chr(10).join(contract_lines) if contract_lines else "- No function-level contract candidates were extracted."}

LEGACY COMPONENT INVENTORY:
- Forms/UserControls: {len(forms)}
- ActiveX/COM dependencies: {len(activex)}
- DLL references: {len(dlls)}
- OCX references: {len(ocx)}
{chr(10).join(form_lines) if form_lines else "- No form-level inventory was extracted from this code sample."}

MODERNIZATION TARGET LANGUAGE:
{target_lang or "Not specified"}

IMPORTANT:
- Use this compact legacy analysis as the source of truth instead of requesting full raw code.
- Document explicit inputs, outputs, and side effects.
- Produce a granular legacy inventory in the output (forms, controls, ActiveX/DLL/OCX dependencies, project members, and mapped business use).
- Preserve backward-compatible behavior in requirements unless explicitly changed.
"""
            else:
                preview = str(legacy_code)[: self.LEGACY_INLINE_MAX_CHARS]
                modernization_context = f"""

LEGACY SOURCE CODE TO MODERNIZE (TRUNCATED):
```asp
{preview}
```

MODERNIZATION TARGET LANGUAGE:
{target_lang or "Not specified"}

IMPORTANT:
- Extract functional behavior from the legacy code.
- Document explicit inputs, outputs, and side effects.
- Preserve backward-compatible behavior in requirements unless explicitly changed.
"""

        database_context = ""
        if db_schema or use_case == "database_conversion":
            schema_text = db_schema
            if db_compact:
                schema_text = str(db_compact.get("schema_excerpt", db_schema)).strip() or db_schema
            if len(schema_text) > self.DB_SCHEMA_INLINE_MAX_CHARS:
                schema_text = schema_text[: self.DB_SCHEMA_INLINE_MAX_CHARS]
            db_summary = str(db_compact.get("summary", "")).strip()
            database_context = f"""

DATABASE CONVERSION CONTEXT:
- Source engine: {db_source or "Not specified"}
- Target engine: {db_target or "Not specified"}
{f"- Schema analysis: {db_summary}" if db_summary else ""}

LEGACY SCHEMA / SQL INPUT:
```sql
{schema_text}
```

IMPORTANT:
- Capture database migration requirements and acceptance criteria.
- Include data validation and rollback expectations.
"""

        deterministic_context = {
            "domain_pack": deterministic.get("domain_pack_ref", {}),
            "normalized_requirement": deterministic.get("normalized_requirement", {}),
            "jurisdiction": deterministic.get("jurisdiction", "GLOBAL"),
            "data_classification": deterministic.get("data_classification", []),
            "capability_mapping": deterministic.get("capability_mapping", {}),
            "regulatory_constraints": deterministic.get("regulatory_constraints", []),
            "standards_guidance": deterministic.get("standards_guidance", []),
            "gold_patterns": deterministic.get("gold_patterns", []),
            "non_negotiables": deterministic.get("non_negotiables", []),
            "evaluation_harness": deterministic.get("evaluation_harness", {}),
            "analyst_dag_required_steps": [
                "normalize_requirement",
                "map_to_capabilities",
                "retrieve_internal_patterns",
                "retrieve_compliance_constraints",
                "synthesize_requirements_pack",
                "run_quality_gates",
            ],
        }

        if answers:
            # Enhanced prompt incorporating user's answers
            answers_text = "\n".join(
                f"  Q: {a.get('question', '?')}\n  A: {a.get('answer', 'No answer')}"
                for a in answers
            )
            return f"""Analyze the following business objectives and produce a comprehensive
requirements document with acceptance criteria.

BUSINESS OBJECTIVES:
{objectives}

CLARIFICATION FROM THE STAKEHOLDER:
The following questions were asked and answered by the business stakeholder.
Incorporate these answers to make the requirements more precise and aligned
with the stakeholder's intent.

{answers_text}
{modernization_context}
{database_context}"""
        else:
            base = f"""Analyze the following business objectives and produce a comprehensive
requirements document with acceptance criteria.

USE CASE:
{use_case}

BUSINESS OBJECTIVES:
{objectives}
{modernization_context}
{database_context}"""
            return f"""{base}

DETERMINISTIC DOMAIN CONTEXT (MUST BE APPLIED, NOT IGNORED):
{self._json_for_prompt(deterministic_context, max_chars=7000, max_depth=4, max_items=12, max_str=320)}

OUTPUT REQUIREMENTS:
- Keep output human-readable and implementation-ready.
- Include `requirements_pack` with capability mapping, domain model excerpt, regulatory constraints, and BDD contract.
- Every critical regulatory/software constraint should appear in acceptance criteria or NFRs.
- Do not invent unsupported regulations; only use provided constraints and objective-derived assumptions."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        deterministic = self._build_deterministic_context(state)
        return self._build_user_message_with_context(state, deterministic)

    def parse_output(self, raw: str) -> dict[str, Any]:
        try:
            return self.extract_json(raw)
        except Exception as exc:
            repaired = self._repair_json_response(raw)
            if repaired is not None:
                self.log(f"[{self.name}] Output recovered via JSON repair pass")
                return repaired
            raise exc

    def _deterministic_parsed_fallback(
        self,
        state: dict[str, Any],
        deterministic: dict[str, Any],
        raw_response: str,
        parse_error: Exception,
    ) -> dict[str, Any]:
        legacy_compact = state.get("legacy_compact_context", {}) if isinstance(state.get("legacy_compact_context", {}), dict) else {}
        compact_inventory = legacy_compact.get("inventory", {}) if isinstance(legacy_compact.get("inventory", {}), dict) else {}
        if not compact_inventory:
            compact_inventory = self._inventory_from_discover_cache(state)
        project_candidates = compact_inventory.get("vb6_projects", []) if isinstance(compact_inventory.get("vb6_projects", []), list) else []
        project_name = next(
            (
                str(item.get("name", "")).strip()
                for item in project_candidates
                if isinstance(item, dict) and str(item.get("name", "")).strip()
            ),
            "",
        ) or str(state.get("project_name", "")).strip() or "Legacy Modernization Scope"
        fallback_contract = legacy_compact.get("seed_legacy_contract", []) if isinstance(legacy_compact.get("seed_legacy_contract", []), list) else []
        objective = str(deterministic.get("normalized_requirement", {}).get("raw_requirement", "")).strip()
        summary = (
            "Deterministic analyst fallback compiled from repository evidence because the primary model response was not machine-readable."
        )
        if objective:
            summary = f"{summary} Objective: {objective[:400]}"
        return {
            "analysis_walkthrough": {
                "business_objective_summary": objective or summary,
                "requirements_understanding": [
                    "Repository evidence was analyzed successfully, but the narrative LLM response could not be parsed into machine-readable JSON.",
                    "Functional and non-functional requirements were reconstructed from deterministic inventory, traceability, and legacy contract signals.",
                ],
                "conversion_to_technical_requirements": [
                    "Use deterministic inventory and traceability as the baseline for migration planning.",
                    "Review generated artifacts where coverage is incomplete or evidence confidence is reduced.",
                ],
                "clarifications_requested": [str(x) for x in deterministic.get("open_questions", []) if str(x).strip()],
            },
            "project_name": project_name,
            "executive_summary": summary,
            "functional_requirements": [],
            "non_functional_requirements": [],
            "legacy_functional_contract": fallback_contract[:20],
            "legacy_code_inventory": compact_inventory,
            "assumptions": [
                "Deterministic repository evidence is treated as authoritative for inventory, dependency, and traceability baseline details.",
                "Narrative refinement may still be needed for business-language polish where the model response was not machine-readable.",
            ],
            "risks": [
                {
                    "id": "RISK-PARSE-001",
                    "title": "Analyst output required deterministic fallback",
                    "severity": "medium",
                    "description": "The primary Analyst LLM response could not be parsed into valid JSON, so Synthetix compiled the requirements pack from deterministic evidence instead.",
                    "mitigation": "Review the generated artifacts and rerun with a stricter output contract if additional narrative depth is required.",
                    "evidence": str(parse_error),
                }
            ],
            "open_questions": [str(x) for x in deterministic.get("open_questions", []) if str(x).strip()],
            "raw_response_excerpt": str(raw_response or "")[:2000],
        }

    def _repair_json_response(self, raw: str) -> dict[str, Any] | None:
        text = str(raw or "").strip()
        if not text:
            return None
        self.log(f"[{self.name}] Attempting JSON repair pass for analyst output...")
        repair_system = """You repair Analyst Agent outputs into strict JSON.
Return a single valid JSON object only. No markdown. No explanation.
Preserve the original meaning. If some sections are missing, use empty arrays/objects.
Required top-level keys:
- analysis_walkthrough
- project_name
- executive_summary
- functional_requirements
- non_functional_requirements
- legacy_functional_contract
- legacy_code_inventory
- assumptions
- risks
- open_questions"""
        repair_user = f"""The previous model response was not valid JSON.
Rewrite it into valid JSON with the required top-level keys listed above.

SOURCE RESPONSE:
```text
{text[:24000]}
```"""
        try:
            repaired = self.llm.invoke(repair_system, repair_user)
            self.log(f"[{self.name}] JSON repair response received ({repaired.output_tokens} tokens, {repaired.latency_ms:.0f}ms)")
            parsed = self.extract_json(repaired.content)
            return parsed if isinstance(parsed, dict) else None
        except Exception as repair_exc:
            self.log(f"[{self.name}] JSON repair failed: {repair_exc}")
            return None

    def _split_legacy_chunks(self, legacy_code: str) -> list[str]:
        text = str(legacy_code or "").strip()
        if not text:
            return []
        # Prefer file-aware chunking if code bundle contains FILE markers.
        parts = re.split(r"(?im)(?=^(?:### FILE:\s+|===== FILE:\s+))", text)
        segments = [p.strip() for p in parts if p and p.strip()]
        if len(segments) <= 1:
            return [text[i : i + self.LEGACY_CHUNK_MAX_CHARS] for i in range(0, len(text), self.LEGACY_CHUNK_MAX_CHARS)][
                : self.LEGACY_CHUNK_MAX_COUNT
            ]

        chunks: list[str] = []
        current = ""
        for seg in segments:
            block = seg + "\n"
            if len(block) > self.LEGACY_CHUNK_MAX_CHARS:
                if current.strip():
                    chunks.append(current.strip())
                    current = ""
                for i in range(0, len(block), self.LEGACY_CHUNK_MAX_CHARS):
                    piece = block[i : i + self.LEGACY_CHUNK_MAX_CHARS].strip()
                    if piece:
                        chunks.append(piece)
                    if len(chunks) >= self.LEGACY_CHUNK_MAX_COUNT:
                        return chunks
                continue
            if len(current) + len(block) > self.LEGACY_CHUNK_MAX_CHARS:
                if current.strip():
                    chunks.append(current.strip())
                current = block
            else:
                current += block
            if len(chunks) >= self.LEGACY_CHUNK_MAX_COUNT:
                return chunks
        if current.strip() and len(chunks) < self.LEGACY_CHUNK_MAX_COUNT:
            chunks.append(current.strip())
        return chunks

    def _normalize_legacy_path(self, path: str) -> str:
        value = str(path or "").strip().replace("\\", "/")
        while value.startswith("./"):
            value = value[2:]
        return value

    def _parse_legacy_bundle_files(self, text: str) -> dict[str, str]:
        raw = str(text or "")
        files: dict[str, str] = {}
        markers = list(
            re.finditer(
                r"(?im)^(?:### FILE:\s+|===== FILE:\s+)(.+?)(?:\s*=====\s*)?$",
                raw,
            )
        )
        if not markers:
            return files
        for idx, marker in enumerate(markers):
            file_path = self._normalize_legacy_path(str(marker.group(1) or "").strip())
            if not file_path:
                continue
            start = marker.end()
            end = markers[idx + 1].start() if idx + 1 < len(markers) else len(raw)
            body = raw[start:end].strip("\n")
            if file_path in files and body:
                files[file_path] = (files[file_path] + "\n" + body).strip("\n")
            else:
                files[file_path] = body
        return files

    def _resolve_vb6_member_path(
        self,
        member_path: str,
        project_file: str,
        known_paths_by_lower: dict[str, str],
    ) -> str:
        def _canon(raw: str) -> str:
            value = self._normalize_legacy_path(raw)
            parts: list[str] = []
            for token in str(value).split("/"):
                part = str(token).strip()
                if not part or part == ".":
                    continue
                if part == "..":
                    if parts:
                        parts.pop()
                    continue
                parts.append(part)
            return "/".join(parts)

        member = _canon(member_path)
        project = _canon(project_file)
        if not member:
            return ""
        project_dir = ""
        if "/" in project:
            project_dir = project.rsplit("/", 1)[0]
        candidates = [member]
        if project_dir:
            candidates.insert(0, f"{project_dir}/{member}")
        candidates = [_canon(cand) for cand in candidates if _canon(cand)]
        # Keep order stable while removing duplicates.
        candidates = list(dict.fromkeys(candidates))
        for cand in candidates:
            hit = known_paths_by_lower.get(cand.lower())
            if hit:
                return hit
        member_leaf = member.rsplit("/", 1)[-1].strip().lower()
        if member_leaf:
            for lower_path, hit in known_paths_by_lower.items():
                if lower_path == member_leaf or lower_path.endswith("/" + member_leaf):
                    return hit
        return ""

    def _extract_project_text_signals(self, text: str) -> dict[str, list[str]]:
        raw = str(text or "")
        lower = raw.lower()
        procedures: list[str] = []
        tables: list[str] = []
        input_signals: list[str] = []
        output_signals: list[str] = []
        integrations: list[str] = []

        for match in re.findall(
            r"(?im)^\s*(?:Public|Private|Friend|Protected)?\s*(?:Sub|Function|Property Get|Property Let|Property Set)\s+([A-Za-z_][A-Za-z0-9_]*)",
            raw,
        ):
            name = str(match or "").strip()
            if name and name not in procedures:
                procedures.append(name)
            if len(procedures) >= 120:
                break

        for pattern in [r"(?i)\bfrom\s+([A-Za-z_][A-Za-z0-9_]*)", r"(?i)\bjoin\s+([A-Za-z_][A-Za-z0-9_]*)", r"(?i)\binto\s+([A-Za-z_][A-Za-z0-9_]*)"]:
            for match in re.findall(pattern, raw):
                table = str(match or "").strip()
                if table and table not in tables and table.lower() not in {"where", "select"}:
                    tables.append(table)
                if len(tables) >= 80:
                    break
            if len(tables) >= 80:
                break

        req_matches = re.findall(r'(?i)request\.(?:querystring|form)\(\s*"([^"]+)"\s*\)', raw)
        req_matches += re.findall(r"(?i)request\.(?:querystring|form)\(\s*'([^']+)'\s*\)", raw)
        for m in req_matches:
            key = str(m or "").strip()
            if key and key not in input_signals:
                input_signals.append(key)
        if ("request." in lower or "request(" in lower) and "HTTP request parameters" not in input_signals:
            input_signals.append("HTTP request parameters")

        if "response.write" in lower and "HTML/text response output" not in output_signals:
            output_signals.append("HTML/text response output")
        if "response.redirect" in lower and "HTTP redirect output" not in output_signals:
            output_signals.append("HTTP redirect output")
        if any(token in lower for token in ["adodc", "recordset", "dao.", "adodb"]):
            output_signals.append("Database result set rendering")

        integration_tokens = [
            ("createobject(", "COM object invocation"),
            ("msxml2.xmlhttp", "HTTP integration via MSXML"),
            ("winsock", "Socket/transport integration"),
            ("ado", "ADO data access"),
            ("dao", "DAO data access"),
            ("scripting.filesystemobject", "Filesystem automation"),
        ]
        for token, label in integration_tokens:
            if token in lower and label not in integrations:
                integrations.append(label)

        return {
            "procedures": procedures[:120],
            "tables": tables[:80],
            "input_signals": input_signals[:40],
            "output_signals": output_signals[:40],
            "integrations": integrations[:20],
        }

    def _infer_project_business_objective(
        self,
        project_name: str,
        forms: list[str],
        procedures: list[str],
        tables: list[str],
        controls: list[str],
        events: list[str],
    ) -> tuple[str, list[str]]:
        haystack = " ".join(
            [
                str(project_name or "").lower(),
                " ".join(str(x or "").lower() for x in forms[:40]),
                " ".join(str(x or "").lower() for x in procedures[:80]),
                " ".join(str(x or "").lower() for x in tables[:40]),
                " ".join(str(x or "").lower() for x in controls[:40]),
                " ".join(str(x or "").lower() for x in events[:80]),
            ]
        )
        mapping = [
            ("login", "Authentication and user access", "Authenticate users and initiate secure application sessions."),
            ("auth", "Authentication and user access", "Enforce access-control and identity verification workflows."),
            ("customer", "Customer profile operations", "Manage customer profile lookup and maintenance workflows."),
            ("account", "Account operations", "Handle account-level operations and account data maintenance."),
            ("payment", "Payment processing", "Capture and process payment-related transactions."),
            ("transfer", "Funds transfer", "Orchestrate funds movement and transfer lifecycle management."),
            ("transaction", "Transaction processing", "Record and process transaction lifecycle events."),
            ("invoice", "Billing and invoicing", "Support billing calculations and invoice generation workflows."),
            ("report", "Reporting and analytics", "Generate operational and management reports."),
            ("inventory", "Inventory operations", "Maintain inventory state and allocation workflows."),
            ("order", "Order processing", "Support order capture and fulfillment processing."),
            ("settlement", "Settlement and reconciliation", "Execute settlement/reconciliation processes."),
            ("audit", "Audit and compliance", "Track auditable actions and compliance-relevant workflows."),
            ("fraud", "Fraud/risk controls", "Evaluate anomalies and enforce fraud/risk checks."),
        ]
        capabilities: list[str] = []
        objective = ""
        for token, capability, desc in mapping:
            if token in haystack:
                if capability not in capabilities:
                    capabilities.append(capability)
                if not objective:
                    objective = desc
        if not objective:
            objective = (
                "Deliver event-driven desktop business workflows through VB6 forms, modules, and COM integrations."
            )
        if not capabilities:
            capabilities = ["Desktop workflow orchestration"]
        return objective, capabilities[:8]

    def _build_project_workflows(
        self,
        forms: list[str],
        events: list[str],
        procedures: list[str],
    ) -> list[str]:
        workflows: list[str] = []
        form_names = [str(x).split(":", 1)[-1].strip() for x in forms if str(x).strip()]
        for name in form_names[:8]:
            related = [ev for ev in events if str(ev).lower().startswith(name.lower() + "_")][:4]
            if related:
                workflows.append(f"{name}: handles {'; '.join(related)}")
            else:
                workflows.append(f"{name}: drives user-triggered workflow via control events")
        if not workflows and procedures:
            workflows.append("Core procedural workflow: " + ", ".join(procedures[:6]))
        return workflows[:12]

    def _parse_vbp_dependency_reference(self, reference_line: Any) -> dict[str, str] | None:
        text = str(reference_line or "").strip()
        if not text:
            return None
        name = ""
        reference = text
        pair_match = re.search(r'"([^"]+)"\s*;\s*"([^"]+)"', text)
        if pair_match:
            reference = str(pair_match.group(1) or "").strip()
            name = str(pair_match.group(2) or "").strip()
        if not name:
            ext_match = re.search(r"([A-Za-z0-9_.-]+\.(?:ocx|dll|dcx|dca))", text, flags=re.IGNORECASE)
            if ext_match:
                name = str(ext_match.group(1) or "").strip()
        if not name:
            return None
        guid_match = re.search(r"\{[0-9A-Fa-f-]{36}\}", reference or text)
        guid = str(guid_match.group(0) or "").strip() if guid_match else ""
        return {
            "name": name,
            "reference": reference,
            "guid": guid,
        }

    def _extract_vbp_dependency_references(self, project_defs: list[dict[str, Any]]) -> list[dict[str, str]]:
        rows: list[dict[str, str]] = []
        seen: set[tuple[str, str, str]] = set()
        for project in project_defs:
            if not isinstance(project, dict):
                continue
            project_name = str(project.get("project_name", "")).strip()
            project_file = str(project.get("project_file", "")).strip()
            refs = project.get("references", []) if isinstance(project.get("references", []), list) else []
            for raw_ref in refs:
                parsed = self._parse_vbp_dependency_reference(raw_ref)
                if not parsed:
                    continue
                name = str(parsed.get("name", "")).strip()
                reference = str(parsed.get("reference", "")).strip()
                key = (project_file.lower(), name.lower(), reference.lower())
                if key in seen:
                    continue
                seen.add(key)
                rows.append(
                    {
                        "name": name,
                        "reference": reference,
                        "guid": str(parsed.get("guid", "")).strip(),
                        "project_name": project_name,
                        "project_file": project_file,
                    }
                )
        return rows[: self.LEGACY_MAX_DEPENDENCIES * 4]

    def _extract_business_rules_catalog(
        self,
        *,
        bundle_file_map: dict[str, str],
        vb6_projects: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        rules: list[dict[str, Any]] = []
        seen: set[str] = set()

        def add_rule(rule_type: str, statement: str, evidence: str, scope: str, confidence: float = 0.75):
            key = f"{rule_type}|{statement}|{scope}"
            if key in seen:
                return
            seen.add(key)
            rules.append(
                {
                    "id": f"BR-{len(rules) + 1:03d}",
                    "rule_type": rule_type,
                    "statement": statement,
                    "scope": scope,
                    "evidence": evidence,
                    "confidence": round(float(confidence), 2),
                }
            )

        for project in vb6_projects[: self.LEGACY_MAX_PROJECTS]:
            if not isinstance(project, dict):
                continue
            pname = str(project.get("project_name", "VB6 Project")).strip() or "VB6 Project"
            touch = project.get("data_touchpoints", {}) if isinstance(project.get("data_touchpoints", {}), dict) else {}
            procedures = touch.get("procedures", []) if isinstance(touch.get("procedures", []), list) else []
            inputs = touch.get("input_signals", []) if isinstance(touch.get("input_signals", []), list) else []
            tables = touch.get("tables", []) if isinstance(touch.get("tables", []), list) else []
            objective = str(project.get("business_objective_hypothesis", "")).strip()
            if objective:
                add_rule(
                    "business_objective",
                    objective,
                    evidence=f"{pname} objective inference",
                    scope=pname,
                    confidence=0.68,
                )
            if inputs:
                add_rule(
                    "input_validation",
                    f"{pname} enforces business flow based on runtime/user inputs ({', '.join([str(x) for x in inputs[:4]])}).",
                    evidence=f"{pname} input signals",
                    scope=pname,
                    confidence=0.72,
                )
            if procedures:
                add_rule(
                    "workflow_orchestration",
                    f"{pname} executes transaction workflow through procedures such as {', '.join([str(x) for x in procedures[:4]])}.",
                    evidence=f"{pname} procedure map",
                    scope=pname,
                    confidence=0.76,
                )
            if tables:
                add_rule(
                    "data_persistence",
                    f"{pname} reads/writes persisted entities including {', '.join([str(x) for x in tables[:4]])}.",
                    evidence=f"{pname} SQL/table hints",
                    scope=pname,
                    confidence=0.7,
                )

        calc_rx = re.compile(
            r"(?im)^\s*(?:if\s+)?([A-Za-z_][A-Za-z0-9_\.]*)\s*=\s*([A-Za-z0-9_\.()\s\+\-\*/]+)\s*(?:then)?\s*$"
        )
        threshold_rx = re.compile(r"(?im)^\s*If\s+(.+?)\s*(=|<>|>=|<=|>|<)\s*([0-9][0-9\.\-]*)\s+Then\b")
        case_rx = re.compile(r"(?im)^\s*Select\s+Case\s+(.+)$")
        for path, text in list(bundle_file_map.items())[:220]:
            lines = str(text or "").splitlines()
            for idx, line in enumerate(lines[:1600], start=1):
                raw = str(line or "").strip()
                if not raw:
                    continue
                calc = calc_rx.match(raw)
                if calc and any(op in calc.group(2) for op in ["+", "-", "*", "/"]):
                    lhs = str(calc.group(1) or "").strip()
                    rhs = str(calc.group(2) or "").strip()
                    if len(rhs) > 120:
                        rhs = rhs[:120] + "..."
                    add_rule(
                        "calculation_logic",
                        f"Computed value rule: {lhs} = {rhs}",
                        evidence=f"{path}:{idx}",
                        scope=path,
                        confidence=0.84,
                    )
                thr = threshold_rx.match(raw)
                if thr:
                    cond = f"{thr.group(1)} {thr.group(2)} {thr.group(3)}"
                    add_rule(
                        "threshold_rule",
                        f"Threshold decision rule: IF {cond} THEN ...",
                        evidence=f"{path}:{idx}",
                        scope=path,
                        confidence=0.82,
                    )
                case = case_rx.match(raw)
                if case:
                    add_rule(
                        "decision_branching",
                        f"Branching logic based on CASE {str(case.group(1) or '').strip()}",
                        evidence=f"{path}:{idx}",
                        scope=path,
                        confidence=0.74,
                    )
                low = raw.lower()
                if "datediff(" in low or "dateadd(" in low:
                    add_rule(
                        "date_rule",
                        "Date-based business rule detected (DateDiff/DateAdd).",
                        evidence=f"{path}:{idx}",
                        scope=path,
                        confidence=0.78,
                    )
                if "isnumeric(" in low or "isdate(" in low or "len(" in low:
                    add_rule(
                        "input_validation",
                        "Input validation rule detected (IsNumeric/IsDate/Len).",
                        evidence=f"{path}:{idx}",
                        scope=path,
                        confidence=0.76,
                    )
                if len(rules) >= 180:
                    break
            if len(rules) >= 180:
                break
        return rules[:180]

    def _compute_bundle_loc_metrics(self, bundle_file_map: dict[str, str]) -> dict[str, Any]:
        file_line_counts: dict[str, int] = {}
        totals = {
            "total_loc": 0,
            "forms_loc": 0,
            "modules_loc": 0,
            "classes_loc": 0,
            "reports_loc": 0,
            "projects_loc": 0,
        }
        if not isinstance(bundle_file_map, dict):
            return {**totals, "files_scanned": 0, "by_file": file_line_counts}
        for raw_path, raw_text in bundle_file_map.items():
            path = self._normalize_legacy_path(str(raw_path))
            if not path:
                continue
            text = str(raw_text or "")
            loc = len(text.splitlines())
            file_line_counts[path] = loc
            totals["total_loc"] += loc
            low = path.lower()
            if low.endswith((".frm", ".ctl")):
                totals["forms_loc"] += loc
            elif low.endswith(".bas"):
                totals["modules_loc"] += loc
            elif low.endswith(".cls"):
                totals["classes_loc"] += loc
            elif low.endswith(".dsr"):
                totals["reports_loc"] += loc
            elif low.endswith(".vbp"):
                totals["projects_loc"] += loc
        return {
            **totals,
            "files_scanned": len(file_line_counts),
            "by_file": file_line_counts,
        }

    def _build_vb6_project_breakdown(
        self,
        project_defs: list[dict[str, Any]],
        vb6_by_path: dict[str, dict[str, Any]],
        bundle_file_map: dict[str, str] | None = None,
        file_line_counts: dict[str, int] | None = None,
    ) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        files = bundle_file_map if isinstance(bundle_file_map, dict) else {}
        loc_by_file = file_line_counts if isinstance(file_line_counts, dict) else {}
        known_paths: set[str] = {str(path) for path in vb6_by_path.keys()}
        known_paths.update(str(path) for path in files.keys())
        known_paths_by_lower = {str(path).lower(): path for path in known_paths}

        for project in project_defs:
            if not isinstance(project, dict):
                continue
            members = project.get("members", []) if isinstance(project.get("members", []), list) else []
            project_file = self._normalize_legacy_path(str(project.get("project_file", "")))
            member_files: list[str] = []
            member_type_counts: dict[str, int] = {}
            forms: set[str] = set()
            controls: set[str] = set()
            activex: set[str] = set()
            events: set[str] = set()
            event_keys: set[str] = set()
            procedures: set[str] = set()
            tables: set[str] = set()
            input_signals: set[str] = set()
            output_signals: set[str] = set()
            integrations: set[str] = set()
            for member in members:
                if not isinstance(member, dict):
                    continue
                member_type = str(member.get("member_type", "")).strip()
                if member_type:
                    member_type_counts[member_type] = int(member_type_counts.get(member_type, 0)) + 1
                resolved = self._resolve_vb6_member_path(
                    str(member.get("member_path", "")),
                    project_file,
                    known_paths_by_lower,
                )
                if not resolved:
                    continue
                member_files.append(resolved)
                sig = vb6_by_path.get(resolved, {})
                for value in sig.get("forms", []) if isinstance(sig.get("forms", []), list) else []:
                    forms.add(str(value))
                for value in sig.get("controls", []) if isinstance(sig.get("controls", []), list) else []:
                    controls.add(str(value))
                for value in sig.get("activex_dependencies", []) if isinstance(sig.get("activex_dependencies", []), list) else []:
                    activex.add(str(value))
                for value in sig.get("event_handlers", []) if isinstance(sig.get("event_handlers", []), list) else []:
                    events.add(str(value))
                for value in sig.get("event_handler_keys", []) if isinstance(sig.get("event_handler_keys", []), list) else []:
                    event_keys.add(str(value))
                text = str(files.get(resolved, ""))
                if text:
                    signals = self._extract_project_text_signals(text)
                    for v in signals.get("procedures", []):
                        procedures.add(str(v))
                    for v in signals.get("tables", []):
                        tables.add(str(v))
                    for v in signals.get("input_signals", []):
                        input_signals.add(str(v))
                    for v in signals.get("output_signals", []):
                        output_signals.add(str(v))
                    for v in signals.get("integrations", []):
                        integrations.add(str(v))
            project_name = str(project.get("project_name", "")).strip() or (
                project_file.rsplit("/", 1)[-1].rsplit(".", 1)[0] if project_file else "VB6 Project"
            )
            sorted_forms = sorted(forms)[: self.LEGACY_MAX_FORMS]
            sorted_controls = sorted(controls)[: self.LEGACY_MAX_CONTROLS]
            sorted_events = sorted(events)[: self.LEGACY_MAX_CONTROLS]
            sorted_procs = sorted(procedures)[:120]
            sorted_tables = sorted(tables)[:80]
            sorted_inputs = sorted(input_signals)[:40]
            sorted_outputs = sorted(output_signals)[:40]
            sorted_integrations = sorted(integrations)[:20]
            objective, capabilities = self._infer_project_business_objective(
                project_name=project_name,
                forms=sorted_forms,
                procedures=sorted_procs,
                tables=sorted_tables,
                controls=sorted_controls,
                events=sorted_events,
            )
            workflows = self._build_project_workflows(sorted_forms, sorted_events, sorted_procs)
            modernization_notes: list[str] = []
            if sorted_integrations:
                modernization_notes.append("Preserve COM/integration behavior while replacing deprecated runtime dependencies.")
            if any(str(dep).upper().endswith(".OCX") for dep in activex):
                modernization_notes.append("Map OCX controls to modern UI/widget equivalents with event parity.")
            if sorted_tables:
                modernization_notes.append("Validate SQL/table contracts and side effects during migration.")
            if not modernization_notes:
                modernization_notes.append("Preserve workflow behavior and event semantics during modernization.")
            member_files_unique = sorted({str(x) for x in member_files if str(x).strip()})
            project_source_loc = sum(int(loc_by_file.get(path, 0) or 0) for path in member_files_unique)
            project_forms_loc = sum(
                int(loc_by_file.get(path, 0) or 0)
                for path in member_files_unique
                if str(path).lower().endswith((".frm", ".ctl"))
            )
            project_modules_loc = sum(
                int(loc_by_file.get(path, 0) or 0)
                for path in member_files_unique
                if str(path).lower().endswith(".bas")
            )
            project_classes_loc = sum(
                int(loc_by_file.get(path, 0) or 0)
                for path in member_files_unique
                if str(path).lower().endswith(".cls")
            )
            bas_modules = [m for m in member_files_unique if str(m).lower().endswith(".bas")]
            binary_companions = [
                m for m in member_files_unique
                if str(m).lower().endswith((".frx", ".ctx", ".res"))
            ]
            rows.append(
                {
                    "project_name": project_name,
                    "project_file": project_file,
                    "project_type": str(project.get("project_type", "")).strip(),
                    "startup_object": str(project.get("startup_object", "")).strip(),
                    "member_count": len(members),
                    "member_files": member_files_unique[:160],
                    "source_loc_total": project_source_loc,
                    "source_loc_forms": project_forms_loc,
                    "source_loc_modules": project_modules_loc,
                    "source_loc_classes": project_classes_loc,
                    "member_type_counts": member_type_counts,
                    "forms": sorted_forms,
                    "controls": sorted_controls,
                    "activex_dependencies": sorted(activex)[: self.LEGACY_MAX_DEPENDENCIES],
                    "event_handlers": sorted_events,
                    "event_handler_keys": sorted(event_keys)[: self.LEGACY_MAX_CONTROLS * 3],
                    "event_handler_count_exact": max(len(event_keys), len(sorted_events)),
                    "business_objective_hypothesis": objective,
                    "key_business_capabilities": capabilities,
                    "primary_workflows": workflows,
                    "data_touchpoints": {
                        "tables": sorted_tables,
                        "procedures": sorted_procs,
                        "input_signals": sorted_inputs,
                        "output_signals": sorted_outputs,
                    },
                    "technical_components": {
                        "notable_components": sorted({str(x) for x in member_files if str(x).strip()})[:20],
                        "external_dependencies": sorted(activex)[: self.LEGACY_MAX_DEPENDENCIES],
                        "integration_hints": sorted_integrations,
                        "bas_modules": bas_modules[:80],
                        "binary_companion_files": binary_companions[:80],
                    },
                    "modernization_considerations": modernization_notes,
                }
            )

        if not rows:
            # Fallback grouping when VBP files are unavailable: group by top-level directory.
            grouped: dict[str, dict[str, Any]] = {}
            for path, sig in vb6_by_path.items():
                norm = self._normalize_legacy_path(path)
                root = norm.split("/", 1)[0] if "/" in norm else "(root)"
                bucket = grouped.setdefault(
                    root,
                    {
                        "project_name": f"Inferred:{root}",
                        "project_file": "",
                        "project_type": "inferred",
                        "startup_object": "",
                        "member_count": 0,
                        "member_files": [],
                        "member_type_counts": {},
                        "forms": set(),
                        "controls": set(),
                        "activex_dependencies": set(),
                        "event_handlers": set(),
                        "event_handler_keys": set(),
                        "procedures": set(),
                        "tables": set(),
                        "input_signals": set(),
                        "output_signals": set(),
                        "integration_hints": set(),
                    },
                )
                bucket["member_count"] += 1
                bucket["member_files"].append(norm)
                for value in sig.get("forms", []) if isinstance(sig.get("forms", []), list) else []:
                    bucket["forms"].add(str(value))
                for value in sig.get("controls", []) if isinstance(sig.get("controls", []), list) else []:
                    bucket["controls"].add(str(value))
                for value in sig.get("activex_dependencies", []) if isinstance(sig.get("activex_dependencies", []), list) else []:
                    bucket["activex_dependencies"].add(str(value))
                for value in sig.get("event_handlers", []) if isinstance(sig.get("event_handlers", []), list) else []:
                    bucket["event_handlers"].add(str(value))
                for value in sig.get("event_handler_keys", []) if isinstance(sig.get("event_handler_keys", []), list) else []:
                    bucket["event_handler_keys"].add(str(value))
                path_lower = norm.lower()
                if path_lower.endswith(".frm"):
                    bucket["member_type_counts"]["Form"] = int(bucket["member_type_counts"].get("Form", 0)) + 1
                elif path_lower.endswith(".bas"):
                    bucket["member_type_counts"]["Module"] = int(bucket["member_type_counts"].get("Module", 0)) + 1
                elif path_lower.endswith(".cls"):
                    bucket["member_type_counts"]["Class"] = int(bucket["member_type_counts"].get("Class", 0)) + 1
                elif path_lower.endswith(".ctl"):
                    bucket["member_type_counts"]["UserControl"] = int(bucket["member_type_counts"].get("UserControl", 0)) + 1
                signals = self._extract_project_text_signals(str(files.get(norm, "")))
                for v in signals.get("procedures", []):
                    bucket["procedures"].add(str(v))
                for v in signals.get("tables", []):
                    bucket["tables"].add(str(v))
                for v in signals.get("input_signals", []):
                    bucket["input_signals"].add(str(v))
                for v in signals.get("output_signals", []):
                    bucket["output_signals"].add(str(v))
                for v in signals.get("integrations", []):
                    bucket["integration_hints"].add(str(v))
            for _, bucket in grouped.items():
                member_files_unique = sorted({str(x) for x in bucket["member_files"] if str(x).strip()})
                project_source_loc = sum(int(loc_by_file.get(path, 0) or 0) for path in member_files_unique)
                project_forms_loc = sum(
                    int(loc_by_file.get(path, 0) or 0)
                    for path in member_files_unique
                    if str(path).lower().endswith((".frm", ".ctl"))
                )
                project_modules_loc = sum(
                    int(loc_by_file.get(path, 0) or 0)
                    for path in member_files_unique
                    if str(path).lower().endswith(".bas")
                )
                project_classes_loc = sum(
                    int(loc_by_file.get(path, 0) or 0)
                    for path in member_files_unique
                    if str(path).lower().endswith(".cls")
                )
                forms_sorted = sorted(bucket["forms"])[: self.LEGACY_MAX_FORMS]
                controls_sorted = sorted(bucket["controls"])[: self.LEGACY_MAX_CONTROLS]
                events_sorted = sorted(bucket["event_handlers"])[: self.LEGACY_MAX_CONTROLS]
                event_keys_sorted = sorted(bucket["event_handler_keys"])[: self.LEGACY_MAX_CONTROLS * 3]
                procs_sorted = sorted(bucket["procedures"])[:120]
                tables_sorted = sorted(bucket["tables"])[:80]
                inputs_sorted = sorted(bucket["input_signals"])[:40]
                outputs_sorted = sorted(bucket["output_signals"])[:40]
                integration_sorted = sorted(bucket["integration_hints"])[:20]
                objective, capabilities = self._infer_project_business_objective(
                    project_name=str(bucket["project_name"]),
                    forms=forms_sorted,
                    procedures=procs_sorted,
                    tables=tables_sorted,
                    controls=controls_sorted,
                    events=events_sorted,
                )
                rows.append(
                    {
                        "project_name": bucket["project_name"],
                        "project_file": bucket["project_file"],
                        "project_type": bucket["project_type"],
                        "startup_object": bucket["startup_object"],
                        "member_count": int(bucket["member_count"] or 0),
                        "member_files": member_files_unique[:160],
                        "source_loc_total": project_source_loc,
                        "source_loc_forms": project_forms_loc,
                        "source_loc_modules": project_modules_loc,
                        "source_loc_classes": project_classes_loc,
                        "member_type_counts": dict(bucket["member_type_counts"]),
                        "forms": forms_sorted,
                        "controls": controls_sorted,
                        "activex_dependencies": sorted(bucket["activex_dependencies"])[: self.LEGACY_MAX_DEPENDENCIES],
                        "event_handlers": events_sorted,
                        "event_handler_keys": event_keys_sorted,
                        "event_handler_count_exact": max(len(bucket["event_handler_keys"]), len(events_sorted)),
                        "business_objective_hypothesis": objective,
                        "key_business_capabilities": capabilities,
                        "primary_workflows": self._build_project_workflows(forms_sorted, events_sorted, procs_sorted),
                        "data_touchpoints": {
                            "tables": tables_sorted,
                            "procedures": procs_sorted,
                            "input_signals": inputs_sorted,
                            "output_signals": outputs_sorted,
                        },
                        "technical_components": {
                            "notable_components": sorted({str(x) for x in bucket["member_files"] if str(x).strip()})[:20],
                            "external_dependencies": sorted(bucket["activex_dependencies"])[: self.LEGACY_MAX_DEPENDENCIES],
                            "integration_hints": integration_sorted,
                            "bas_modules": [
                                m for m in sorted({str(x) for x in bucket["member_files"] if str(x).strip()})
                                if str(m).lower().endswith(".bas")
                            ][:80],
                            "binary_companion_files": [
                                m for m in sorted({str(x) for x in bucket["member_files"] if str(x).strip()})
                                if str(m).lower().endswith((".frx", ".ctx", ".res"))
                            ][:80],
                        },
                        "modernization_considerations": [
                            "Preserve inferred project boundaries during migration.",
                            "Validate UI event workflow parity and data contracts in converted implementation.",
                        ],
                    }
                )

        name_buckets: dict[str, list[dict[str, Any]]] = {}
        for row in rows:
            if not isinstance(row, dict):
                continue
            name_key = str(row.get("project_name", "")).strip().lower()
            if not name_key:
                continue
            name_buckets.setdefault(name_key, []).append(row)
        for bucket in name_buckets.values():
            if len(bucket) <= 1:
                continue
            used: set[str] = set()
            for idx, row in enumerate(bucket, start=1):
                base_name = str(row.get("project_name", "")).strip() or f"Project {idx}"
                project_file = self._normalize_legacy_path(str(row.get("project_file", "")))
                hint = project_file.replace("\\", "/").strip("/")
                if hint:
                    parts = [p for p in hint.split("/") if p]
                    hint = "/".join(parts[-2:]) if len(parts) >= 2 else parts[0]
                else:
                    hint = f"variant-{idx}"
                candidate = f"{base_name} ({hint})"
                dedup = candidate
                suffix = 2
                while dedup.lower() in used:
                    dedup = f"{candidate} #{suffix}"
                    suffix += 1
                row["project_name_original"] = base_name
                row["project_name"] = dedup
                used.add(dedup.lower())

        rows.sort(key=lambda row: str(row.get("project_name", "")).lower())
        return rows[: self.LEGACY_MAX_PROJECTS]

    def _infer_form_business_use(self, form_name: str, controls: list[str], handlers: list[str]) -> str:
        haystack = " ".join(
            [
                str(form_name or "").lower(),
                " ".join(str(x or "").lower() for x in controls[:20]),
                " ".join(str(x or "").lower() for x in handlers[:20]),
            ]
        )
        mapping = [
            ("login", "User authentication and sign-in flow"),
            ("auth", "Authentication and access-control workflow"),
            ("customer", "Customer profile lookup and maintenance"),
            ("account", "Account details and account operations"),
            ("payment", "Payment initiation and payment processing"),
            ("transfer", "Funds transfer workflow"),
            ("transaction", "Transaction capture/review workflow"),
            ("invoice", "Invoice/billing management"),
            ("report", "Reporting and operational analytics"),
            ("search", "Record search and retrieval workflow"),
            ("order", "Order processing workflow"),
            ("inventory", "Inventory update and stock management"),
            ("admin", "Administrative configuration and controls"),
            ("audit", "Audit and compliance review workflow"),
            ("settlement", "Settlement and reconciliation workflow"),
            ("risk", "Risk assessment and exception handling"),
            ("fraud", "Fraud monitoring and case triage"),
        ]
        for token, description in mapping:
            if token in haystack:
                return description
        return "Business workflow executed through event-driven UI controls."

    def _current_form_from_stack(self, stack: list[tuple[str, str]]) -> str:
        for ctype, cname in reversed(stack):
            lower_type = str(ctype or "").lower()
            if lower_type in {"vb.form", "vb.mdiform", "vb.usercontrol"}:
                kind = "Form"
                if lower_type == "vb.mdiform":
                    kind = "MDIForm"
                elif lower_type == "vb.usercontrol":
                    kind = "UserControl"
                return f"{kind}:{str(cname or '').strip()}"
        return ""

    def _extract_vb6_inventory_from_chunk(self, text: str) -> dict[str, Any]:
        forms: set[str] = set()
        controls: set[str] = set()
        activex: set[str] = set()
        event_handlers: set[str] = set()
        project_members: set[str] = set()
        dll_dependencies: set[str] = set()
        ocx_dependencies: set[str] = set()
        form_control_map: dict[str, set[str]] = {}
        form_event_map: dict[str, set[str]] = {}

        stack: list[tuple[str, str]] = []
        begin_rx = re.compile(r"(?im)^\s*Begin\s+([A-Za-z_][A-Za-z0-9_\.]*)\s+([A-Za-z_][A-Za-z0-9_]*)")
        object_rx = re.compile(r'(?im)^\s*Object\s*=\s*"([^"]+)"\s*;\s*"([^"]+)"')
        member_rx = re.compile(r"(?im)^\s*(Form|Module|Class|UserControl|Designer|PropertyPage|UserDocument)\s*=\s*(.+)$")
        event_rx = re.compile(
            r"(?im)^\s*(?:Public|Private|Friend|Protected)?\s*Sub\s+([A-Za-z_][A-Za-z0-9_]*)\s*\("
        )

        for raw_line in str(text or "").splitlines():
            line = str(raw_line or "").rstrip()
            if not line:
                continue
            begin_match = begin_rx.match(line)
            if begin_match:
                ctype = str(begin_match.group(1) or "").strip()
                cname = str(begin_match.group(2) or "").strip()
                stack.append((ctype, cname))
                ctype_l = ctype.lower()
                if ctype_l in {"vb.form", "vb.mdiform", "vb.usercontrol"}:
                    kind = "Form"
                    if ctype_l == "vb.mdiform":
                        kind = "MDIForm"
                    elif ctype_l == "vb.usercontrol":
                        kind = "UserControl"
                    form_key = f"{kind}:{cname}"
                    forms.add(form_key)
                    form_control_map.setdefault(form_key, set())
                    form_event_map.setdefault(form_key, set())
                else:
                    control_key = f"{ctype}:{cname}"
                    controls.add(control_key)
                    current_form = self._current_form_from_stack(stack)
                    if current_form:
                        form_control_map.setdefault(current_form, set()).add(control_key)
                    if not ctype.startswith("VB."):
                        activex.add(ctype)
                        if ctype.upper().endswith(".DLL"):
                            dll_dependencies.add(ctype)
                        if ctype.upper().endswith(".OCX"):
                            ocx_dependencies.add(ctype)
                continue

            if line.strip().lower() == "end":
                if stack:
                    stack.pop()
                continue

            obj_match = object_rx.match(line)
            if obj_match:
                obj_ref = str(obj_match.group(1) or "").strip()
                bin_ref = str(obj_match.group(2) or "").strip()
                dep = bin_ref or obj_ref
                if dep:
                    activex.add(dep)
                dep_upper = dep.upper()
                if dep_upper.endswith(".DLL"):
                    dll_dependencies.add(dep)
                if dep_upper.endswith(".OCX"):
                    ocx_dependencies.add(dep)
                continue

            member_match = member_rx.match(line)
            if member_match:
                member_type = str(member_match.group(1) or "").strip()
                member_value = str(member_match.group(2) or "").strip()
                member_path = member_value.split(";")[-1].strip().strip('"')
                member_key = f"{member_type}:{member_path or member_value}"
                if member_key:
                    project_members.add(member_key)
                continue

            event_match = event_rx.match(line)
            if event_match:
                event_name = str(event_match.group(1) or "").strip()
                if not event_name:
                    continue
                event_handlers.add(event_name)
                event_prefix = event_name.split("_", 1)[0].strip().lower()
                if event_prefix:
                    for form_key, control_set in form_control_map.items():
                        control_names = [
                            str(item).split(":", 1)[1].strip().lower()
                            for item in control_set
                            if ":" in str(item)
                        ]
                        if event_prefix in control_names:
                            form_event_map.setdefault(form_key, set()).add(event_name)
                            break

        for dep in re.findall(r"(?i)\b([A-Za-z0-9_.-]+\.(?:dll|ocx))\b", str(text or "")):
            d = str(dep).strip()
            if not d:
                continue
            activex.add(d)
            if d.upper().endswith(".DLL"):
                dll_dependencies.add(d)
            if d.upper().endswith(".OCX"):
                ocx_dependencies.add(d)

        return {
            "forms": sorted(forms)[: self.LEGACY_MAX_FORMS],
            "controls": sorted(controls)[: self.LEGACY_MAX_CONTROLS],
            "activex_dependencies": sorted(activex)[: self.LEGACY_MAX_DEPENDENCIES],
            "dll_dependencies": sorted(dll_dependencies)[: self.LEGACY_MAX_DEPENDENCIES],
            "ocx_dependencies": sorted(ocx_dependencies)[: self.LEGACY_MAX_DEPENDENCIES],
            "event_handlers": sorted(event_handlers)[: self.LEGACY_MAX_CONTROLS],
            "project_members": sorted(project_members)[: self.LEGACY_MAX_CONTROLS],
            "form_control_map": {
                key: sorted(value)[:80]
                for key, value in form_control_map.items()
                if key
            },
            "form_event_map": {
                key: sorted(value)[:80]
                for key, value in form_event_map.items()
                if key
            },
        }

    def _extract_chunk_deterministic(self, chunk: str) -> dict[str, Any]:
        text = str(chunk or "")
        funcs = []
        for match in re.findall(r"(?im)^\s*(?:Public|Private|Friend|Protected)?\s*(?:Function|Sub|Property Get|Property Let|Property Set)\s+([A-Za-z_][A-Za-z0-9_]*)", text):
            fn = str(match).strip()
            if fn and fn not in funcs:
                funcs.append(fn)
            if len(funcs) >= 30:
                break

        inputs = []
        for m in re.findall(r'(?i)request\.(?:querystring|form)\(\s*"([^"]+)"\s*\)', text):
            key = str(m).strip()
            if key and key not in inputs:
                inputs.append(key)
        for m in re.findall(r"(?i)request\.(?:querystring|form)\(\s*'([^']+)'\s*\)", text):
            key = str(m).strip()
            if key and key not in inputs:
                inputs.append(key)
        if "begin vb.form" in text.lower():
            inputs.append("VB6 form controls")

        tables = []
        for m in re.findall(r"(?i)\bfrom\s+([A-Za-z_][A-Za-z0-9_]*)", text):
            t = str(m).strip()
            if t and t.lower() not in {"where", "select"} and t not in tables:
                tables.append(t)
            if len(tables) >= 20:
                break

        side_effects = []
        side_markers = [
            ("insert ", "Insert into data store"),
            ("update ", "Update existing records"),
            ("delete ", "Delete records"),
            ("response.write", "Render response output"),
            ("response.redirect", "Redirect client flow"),
            ("createobject(", "Create COM/ActiveX object"),
        ]
        lower = text.lower()
        for token, label in side_markers:
            if token in lower and label not in side_effects:
                side_effects.append(label)

        contracts = []
        for fn in funcs[:10]:
            contracts.append(
                {
                    "function_name": fn,
                    "inputs": inputs[:8] if inputs else ["context parameters"],
                    "outputs": ["response payload or state mutation"],
                    "side_effects": side_effects[:6] if side_effects else ["business state update"],
                }
            )

        summary_bits = []
        if funcs:
            summary_bits.append(f"procedures={len(funcs)}")
        if inputs:
            summary_bits.append(f"inputs={len(inputs)}")
        if tables:
            summary_bits.append(f"tables={len(tables)}")
        if side_effects:
            summary_bits.append(f"side_effect_patterns={len(side_effects)}")
        summary = "Legacy chunk signals: " + (", ".join(summary_bits) if summary_bits else "limited code signals found")

        vb6_inventory = self._extract_vb6_inventory_from_chunk(text)
        if vb6_inventory.get("forms"):
            summary += (
                f", vb6_forms={len(vb6_inventory.get('forms', []))}, "
                f"activex={len(vb6_inventory.get('activex_dependencies', []))}"
            )

        return {
            "summary": summary,
            "contracts": contracts,
            "functions": funcs[:20],
            "tables": tables[:20],
            "inputs": inputs[:20],
            "side_effects": side_effects[:12],
            "forms": vb6_inventory.get("forms", []),
            "controls": vb6_inventory.get("controls", []),
            "activex_dependencies": vb6_inventory.get("activex_dependencies", []),
            "dll_dependencies": vb6_inventory.get("dll_dependencies", []),
            "ocx_dependencies": vb6_inventory.get("ocx_dependencies", []),
            "event_handlers": vb6_inventory.get("event_handlers", []),
            "project_members": vb6_inventory.get("project_members", []),
            "form_control_map": vb6_inventory.get("form_control_map", {}),
            "form_event_map": vb6_inventory.get("form_event_map", {}),
        }

    def _extract_chunk_with_llm(self, chunk: str, target_lang: str) -> dict[str, Any]:
        system = (
            "You analyze a single chunk of legacy code and extract migration-relevant signals. "
            "Return JSON only with keys: summary, contracts, entities, interfaces, side_effects. "
            "Each contracts item must include function_name, inputs, outputs, side_effects."
        )
        user = f"""TARGET MODERNIZATION LANGUAGE: {target_lang or "Not specified"}
Analyze this code chunk and extract behavior compactly.

```legacy
{chunk}
```"""
        response = self.llm.invoke(system, user)
        parsed = self.extract_json(response.content)
        if not isinstance(parsed, dict):
            raise ValueError("invalid chunk extraction payload")
        contracts = parsed.get("contracts", [])
        if not isinstance(contracts, list):
            contracts = []
        normalized_contracts = []
        for item in contracts[:20]:
            if not isinstance(item, dict):
                continue
            normalized_contracts.append(
                {
                    "function_name": str(item.get("function_name", "")).strip() or "UnknownFunction",
                    "inputs": [str(x).strip() for x in item.get("inputs", []) if str(x).strip()] if isinstance(item.get("inputs", []), list) else [],
                    "outputs": [str(x).strip() for x in item.get("outputs", []) if str(x).strip()] if isinstance(item.get("outputs", []), list) else [],
                    "side_effects": [str(x).strip() for x in item.get("side_effects", []) if str(x).strip()] if isinstance(item.get("side_effects", []), list) else [],
                }
            )
        return {
            "summary": str(parsed.get("summary", "")).strip() or "Chunk analyzed.",
            "contracts": normalized_contracts,
            "functions": [],
            "tables": [str(x).strip() for x in parsed.get("entities", []) if str(x).strip()] if isinstance(parsed.get("entities", []), list) else [],
            "inputs": [str(x).strip() for x in parsed.get("interfaces", []) if str(x).strip()] if isinstance(parsed.get("interfaces", []), list) else [],
            "side_effects": [str(x).strip() for x in parsed.get("side_effects", []) if str(x).strip()] if isinstance(parsed.get("side_effects", []), list) else [],
        }

    def _build_legacy_compact_context(
        self,
        legacy_code: str,
        target_lang: str,
        state: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        text = str(legacy_code or "").strip()
        if not text:
            return {}
        bundle_file_map = self._parse_legacy_bundle_files(text)
        if not bundle_file_map:
            bundle_file_map = {"inline_legacy.vb": text}
        source_loc_metrics = self._compute_bundle_loc_metrics(bundle_file_map)
        source_loc_by_file = (
            source_loc_metrics.get("by_file", {})
            if isinstance(source_loc_metrics.get("by_file", {}), dict)
            else {}
        )
        bundle_paths = list(bundle_file_map.keys())
        legacy_skill_profile = infer_legacy_skill(
            file_paths=bundle_paths,
            file_contents=(bundle_file_map if bundle_file_map else {"legacy_bundle": text[:50000]}),
        )
        inline = len(text) <= self.LEGACY_INLINE_MAX_CHARS
        chunks = [text] if inline else self._split_legacy_chunks(text)
        if not chunks:
            chunks = [text]
        summaries: list[str] = []
        contracts: list[dict[str, Any]] = []
        seen_fn: set[str] = set()
        chunk_count = len(chunks)
        llm_chunk_limit = 0 if inline else min(self.LEGACY_LLM_CHUNK_LIMIT, chunk_count)
        functions_set: set[str] = set()
        tables_set: set[str] = set()
        inputs_set: set[str] = set()
        side_effects_set: set[str] = set()
        forms_set: set[str] = set()
        controls_set: set[str] = set()
        activex_set: set[str] = set()
        dll_set: set[str] = set()
        ocx_set: set[str] = set()
        event_handlers_set: set[str] = set()
        event_handler_keys_set: set[str] = set()
        project_members_set: set[str] = set()
        sql_query_set: set[str] = set()
        connection_strings_set: set[str] = set()
        database_file_refs_set: set[str] = set()
        connection_string_rows: list[dict[str, Any]] = []
        database_file_reference_rows: list[dict[str, Any]] = []
        module_global_declarations: list[dict[str, Any]] = []
        win32_declares_set: set[str] = set()
        com_progids_set: set[str] = set()
        com_references_set: set[str] = set()
        file_type_coverage: dict[str, int] = {}
        bas_module_paths: set[str] = set()
        bas_procedure_count = 0
        binary_companions: list[dict[str, Any]] = []
        call_by_name_sites_total = 0
        createobject_sites_total = 0
        ui_event_map_index: dict[str, dict[str, Any]] = {}
        pitfall_detector_index: dict[str, dict[str, Any]] = {}
        error_profile_totals: dict[str, int] = {
            "on_error_resume_next": 0,
            "on_error_goto": 0,
            "on_error_goto0": 0,
            "control_array_index_markers": 0,
            "late_bound_com_calls": 0,
            "variant_declarations": 0,
            "default_instance_references": 0,
            "doevents_calls": 0,
            "registry_operations": 0,
        }
        form_control_map: dict[str, set[str]] = {}
        form_event_map: dict[str, set[str]] = {}
        vb6_by_path: dict[str, dict[str, Any]] = {}
        project_definitions: list[dict[str, Any]] = []

        for path, body in bundle_file_map.items():
            sig = legacy_extract_vb6_signals(path, body)
            if not isinstance(sig, dict) or not sig:
                continue
            vb6_by_path[self._normalize_legacy_path(path)] = sig
            sig_forms = [str(x).strip() for x in sig.get("forms", []) if str(x).strip()] if isinstance(sig.get("forms", []), list) else []
            sig_controls = [str(x).strip() for x in sig.get("controls", []) if str(x).strip()] if isinstance(sig.get("controls", []), list) else []
            sig_events = [str(x).strip() for x in sig.get("event_handlers", []) if str(x).strip()] if isinstance(sig.get("event_handlers", []), list) else []
            sig_dependencies = [str(x).strip() for x in sig.get("activex_dependencies", []) if str(x).strip()] if isinstance(sig.get("activex_dependencies", []), list) else []
            sig_members = [str(x).strip() for x in sig.get("project_members", []) if str(x).strip()] if isinstance(sig.get("project_members", []), list) else []
            for token in sig_forms:
                forms_set.add(token)
            for token in sig_controls:
                controls_set.add(token)
            for token in sig_events:
                event_handlers_set.add(token)
            for token in sig_dependencies:
                activex_set.add(token)
                upper = token.upper()
                if upper.endswith(".DLL"):
                    dll_set.add(token)
                if upper.endswith(".OCX"):
                    ocx_set.add(token)
                if ".DLL" in upper and upper.count(".DLL") == 1 and not upper.endswith(".DLL"):
                    dll_set.add(token)
                if ".OCX" in upper and upper.count(".OCX") == 1 and not upper.endswith(".OCX"):
                    ocx_set.add(token)
            for token in sig_members:
                project_members_set.add(token)
            if sig_forms:
                for form_token in sig_forms:
                    c_bucket = form_control_map.setdefault(form_token, set())
                    e_bucket = form_event_map.setdefault(form_token, set())
                    for ctl in sig_controls:
                        c_bucket.add(ctl)
                    for ev in sig_events:
                        e_bucket.add(ev)
            ftype = str(sig.get("vb6_file_type", "")).strip() or "unknown"
            file_type_coverage[ftype] = int(file_type_coverage.get(ftype, 0) or 0) + 1
            if bool(sig.get("is_binary_companion", False)):
                info = sig.get("binary_companion_info", {})
                if isinstance(info, dict):
                    binary_companions.append(
                        {
                            "path": str(info.get("path", path)).strip(),
                            "extension": str(info.get("extension", "")).strip(),
                            "note": str(info.get("note", "Binary companion file detected.")).strip(),
                        }
                    )
            if str(path).lower().endswith(".bas"):
                bas_module_paths.add(self._normalize_legacy_path(path))
                bas_procedure_count += len(sig.get("procedures", []) if isinstance(sig.get("procedures", []), list) else [])
            project_def = sig.get("project_definition", {})
            if isinstance(project_def, dict) and project_def:
                project_definitions.append(project_def)
            for query in sig.get("sql_queries", []) if isinstance(sig.get("sql_queries", []), list) else []:
                text_query = str(query).strip()
                if text_query:
                    sql_query_set.add(text_query)
            source_file_norm = self._normalize_legacy_path(path)
            form_hint = ""
            if sig_forms:
                form_hint = str(sig_forms[0]).split(":", 1)[-1].strip()
            module_hint = ""
            if str(path).lower().endswith(".bas"):
                module_hint = Path(str(path)).stem
            for conn in sig.get("connection_strings", []) if isinstance(sig.get("connection_strings", []), list) else []:
                text_conn = str(conn).strip()
                if text_conn:
                    connection_strings_set.add(text_conn)
                    connection_string_rows.append(
                        {
                            "connection_string": text_conn,
                            "source_file": source_file_norm,
                            "form": form_hint,
                            "module": module_hint,
                            "evidence": source_file_norm,
                        }
                    )
            for db_ref in sig.get("database_file_refs", []) if isinstance(sig.get("database_file_refs", []), list) else []:
                text_ref = str(db_ref).strip()
                if text_ref:
                    ref_norm = self._normalize_legacy_path(text_ref)
                    database_file_refs_set.add(ref_norm)
                    database_file_reference_rows.append(
                        {
                            "path": ref_norm,
                            "source_file": source_file_norm,
                            "form": form_hint,
                            "module": module_hint,
                            "evidence": source_file_norm,
                        }
                    )
            for gdecl in sig.get("module_global_declarations", []) if isinstance(sig.get("module_global_declarations", []), list) else []:
                if not isinstance(gdecl, dict):
                    continue
                symbol = str(gdecl.get("symbol", "")).strip()
                if not symbol:
                    continue
                module_global_declarations.append(
                    {
                        "symbol": symbol,
                        "declared_type": str(gdecl.get("declared_type", "Variant")).strip() or "Variant",
                        "scope": str(gdecl.get("scope", "dim")).strip() or "dim",
                        "source_file": self._normalize_legacy_path(str(gdecl.get("source_file", "")).strip() or source_file_norm),
                        "line": int(gdecl.get("line", 0) or 0),
                        "declaration": str(gdecl.get("declaration", "")).strip()[:320],
                    }
                )
            for entry in sig.get("ui_event_map", []) if isinstance(sig.get("ui_event_map", []), list) else []:
                if not isinstance(entry, dict):
                    continue
                key = str(entry.get("event_handler", "")).strip() or str(entry.get("control", "")).strip()
                if not key:
                    continue
                if key not in ui_event_map_index:
                    ui_event_map_index[key] = {
                        "event_handler": str(entry.get("event_handler", "")).strip(),
                        "form": str(entry.get("form", "")).strip(),
                        "control": str(entry.get("control", "")).strip(),
                        "event": str(entry.get("event", "")).strip(),
                        "source_file": self._normalize_legacy_path(
                            str(entry.get("source_file", "")).strip() or source_file_norm
                        ),
                        "line": int(entry.get("line", 0) or 0),
                        "procedure_calls": entry.get("procedure_calls", [])[:20]
                        if isinstance(entry.get("procedure_calls", []), list)
                        else [],
                        "sql_touches": entry.get("sql_touches", [])[:10]
                        if isinstance(entry.get("sql_touches", []), list)
                        else [],
                        "side_effects": entry.get("side_effects", [])[:12]
                        if isinstance(entry.get("side_effects", []), list)
                        else [],
                    }
            for decl in sig.get("win32_declares", []) if isinstance(sig.get("win32_declares", []), list) else []:
                text_decl = str(decl).strip()
                if text_decl:
                    win32_declares_set.add(text_decl)
            com_surface = sig.get("com_surface_map", {}) if isinstance(sig.get("com_surface_map", {}), dict) else {}
            for progid in com_surface.get("late_bound_progids", []) if isinstance(com_surface.get("late_bound_progids", []), list) else []:
                text_progid = str(progid).strip()
                if text_progid:
                    com_progids_set.add(text_progid)
            for ref in com_surface.get("references", []) if isinstance(com_surface.get("references", []), list) else []:
                text_ref = str(ref).strip()
                if text_ref:
                    com_references_set.add(text_ref)
            call_by_name_sites_total += int(com_surface.get("call_by_name_sites", 0) or 0)
            createobject_sites_total += int(com_surface.get("createobject_getobject_sites", 0) or 0)
            for value in sig.get("event_handler_keys", []) if isinstance(sig.get("event_handler_keys", []), list) else []:
                token = str(value).strip()
                if token:
                    event_handler_keys_set.add(token)
            err_profile = sig.get("error_handling_profile", {}) if isinstance(sig.get("error_handling_profile", {}), dict) else {}
            for key in error_profile_totals:
                error_profile_totals[key] = int(error_profile_totals.get(key, 0) or 0) + int(err_profile.get(key, 0) or 0)
            for detector in sig.get("pitfall_detectors", []) if isinstance(sig.get("pitfall_detectors", []), list) else []:
                if not isinstance(detector, dict):
                    continue
                did = str(detector.get("id", "")).strip()
                if not did:
                    continue
                existing = pitfall_detector_index.get(did)
                if not existing:
                    pitfall_detector_index[did] = {
                        "id": did,
                        "severity": str(detector.get("severity", "medium")).strip() or "medium",
                        "count": int(detector.get("count", 0) or 0),
                        "requires": detector.get("requires", [])[:8]
                        if isinstance(detector.get("requires", []), list)
                        else [],
                        "evidence": str(detector.get("evidence", "")).strip(),
                    }
                else:
                    existing["count"] = int(existing.get("count", 0) or 0) + int(detector.get("count", 0) or 0)
                    if not str(existing.get("evidence", "")).strip():
                        existing["evidence"] = str(detector.get("evidence", "")).strip()

        for idx, chunk in enumerate(chunks, start=1):
            use_llm = idx <= llm_chunk_limit
            extracted: dict[str, Any]
            if use_llm:
                try:
                    extracted = self._extract_chunk_with_llm(chunk, target_lang)
                except Exception as exc:
                    self.log(f"[{self.name}] chunk {idx}/{chunk_count} LLM extraction failed; falling back to deterministic: {exc}")
                    extracted = self._extract_chunk_deterministic(chunk)
            else:
                extracted = self._extract_chunk_deterministic(chunk)

            summary = str(extracted.get("summary", "")).strip()
            if summary:
                summaries.append(f"Chunk {idx}: {summary}")
            for fn in extracted.get("functions", []) if isinstance(extracted.get("functions", []), list) else []:
                fn_text = str(fn).strip()
                if fn_text:
                    functions_set.add(fn_text)
            for tbl in extracted.get("tables", []) if isinstance(extracted.get("tables", []), list) else []:
                tbl_text = str(tbl).strip()
                if tbl_text:
                    tables_set.add(tbl_text)
            for inp in extracted.get("inputs", []) if isinstance(extracted.get("inputs", []), list) else []:
                inp_text = str(inp).strip()
                if inp_text:
                    inputs_set.add(inp_text)
            for side in extracted.get("side_effects", []) if isinstance(extracted.get("side_effects", []), list) else []:
                side_text = str(side).strip()
                if side_text:
                    side_effects_set.add(side_text)
            for row in extracted.get("forms", []) if isinstance(extracted.get("forms", []), list) else []:
                form_text = str(row).strip()
                if form_text:
                    forms_set.add(form_text)
            for row in extracted.get("controls", []) if isinstance(extracted.get("controls", []), list) else []:
                control_text = str(row).strip()
                if control_text:
                    controls_set.add(control_text)
            for row in extracted.get("activex_dependencies", []) if isinstance(extracted.get("activex_dependencies", []), list) else []:
                dep_text = str(row).strip()
                if dep_text:
                    activex_set.add(dep_text)
            for row in extracted.get("dll_dependencies", []) if isinstance(extracted.get("dll_dependencies", []), list) else []:
                dep_text = str(row).strip()
                if dep_text:
                    dll_set.add(dep_text)
            for row in extracted.get("ocx_dependencies", []) if isinstance(extracted.get("ocx_dependencies", []), list) else []:
                dep_text = str(row).strip()
                if dep_text:
                    ocx_set.add(dep_text)
            for row in extracted.get("event_handlers", []) if isinstance(extracted.get("event_handlers", []), list) else []:
                event_text = str(row).strip()
                if event_text:
                    event_handlers_set.add(event_text)
                    event_handler_keys_set.add(event_text)
            for row in extracted.get("project_members", []) if isinstance(extracted.get("project_members", []), list) else []:
                member_text = str(row).strip()
                if member_text:
                    project_members_set.add(member_text)
            if isinstance(extracted.get("form_control_map", {}), dict):
                for form_key, entries in extracted.get("form_control_map", {}).items():
                    form_id = str(form_key).strip()
                    if not form_id:
                        continue
                    bucket = form_control_map.setdefault(form_id, set())
                    if isinstance(entries, list):
                        for item in entries:
                            item_text = str(item).strip()
                            if item_text:
                                bucket.add(item_text)
            if isinstance(extracted.get("form_event_map", {}), dict):
                for form_key, entries in extracted.get("form_event_map", {}).items():
                    form_id = str(form_key).strip()
                    if not form_id:
                        continue
                    bucket = form_event_map.setdefault(form_id, set())
                    if isinstance(entries, list):
                        for item in entries:
                            item_text = str(item).strip()
                            if item_text:
                                bucket.add(item_text)
            rows = extracted.get("contracts", [])
            if isinstance(rows, list):
                for row in rows:
                    if not isinstance(row, dict):
                        continue
                    fn = str(row.get("function_name", "")).strip() or "UnknownFunction"
                    if fn in seen_fn:
                        continue
                    seen_fn.add(fn)
                    contracts.append(
                        {
                            "function_name": fn,
                            "inputs": [str(x).strip() for x in row.get("inputs", []) if str(x).strip()] if isinstance(row.get("inputs", []), list) else [],
                            "outputs": [str(x).strip() for x in row.get("outputs", []) if str(x).strip()] if isinstance(row.get("outputs", []), list) else [],
                            "side_effects": [str(x).strip() for x in row.get("side_effects", []) if str(x).strip()] if isinstance(row.get("side_effects", []), list) else [],
                        }
                    )
                    if len(contracts) >= 40:
                        break
            if len(contracts) >= 40:
                break

        vb6_projects = self._build_vb6_project_breakdown(
            project_definitions,
            vb6_by_path,
            bundle_file_map,
            source_loc_by_file,
        )

        ui_event_rows_all = list(ui_event_map_index.values())
        ui_form_handler_map: dict[str, set[str]] = {}
        ui_form_sql_touches: dict[str, int] = {}
        for row in ui_event_rows_all:
            if not isinstance(row, dict):
                continue
            form_token = str(row.get("form", "")).strip()
            if not form_token:
                continue
            normalized_form = form_token.split("::", 1)[-1].strip().lower()
            if not normalized_form:
                continue
            bucket = ui_form_handler_map.setdefault(normalized_form, set())
            handler = str(row.get("event_handler", "")).strip()
            if handler:
                bucket.add(handler)
            sql_touches = row.get("sql_touches", []) if isinstance(row.get("sql_touches", []), list) else []
            ui_form_sql_touches[normalized_form] = int(ui_form_sql_touches.get(normalized_form, 0) or 0) + len(sql_touches)

        forms_details: list[dict[str, Any]] = []
        form_coverage: list[dict[str, Any]] = []
        project_scoped_forms: list[tuple[str, str]] = []
        project_scoped_form_bases: set[str] = set()
        form_member_path_map: dict[tuple[str, str], list[str]] = {}
        for project in vb6_projects:
            if not isinstance(project, dict):
                continue
            pname = str(project.get("project_name", "")).strip() or "VB6 Project"
            member_files = [
                self._normalize_legacy_path(str(path))
                for path in (project.get("member_files", []) if isinstance(project.get("member_files", []), list) else [])
                if str(path).strip()
            ]
            for form_value in project.get("forms", []) if isinstance(project.get("forms", []), list) else []:
                form_id = str(form_value).strip()
                if form_id:
                    project_scoped_forms.append((pname, form_id))
                    form_tail = form_id.split(":", 1)[-1].strip().lower()
                    if form_tail:
                        project_scoped_form_bases.add(form_tail)
                        matches = [
                            path for path in member_files
                            if str(path).lower().endswith(f"/{form_tail}.frm")
                            or str(path).lower().endswith(f"/{form_tail}.ctl")
                            or str(path).lower().endswith(f"/{form_tail}.cls")
                        ]
                        if matches:
                            form_member_path_map[(pname, form_id)] = matches
        discovered_unmapped_form_ids: list[str] = []
        form_file_loc_by_base: dict[str, int] = {}
        for path, loc in source_loc_by_file.items():
            low_path = str(path).lower()
            if not low_path.endswith((".frm", ".ctl", ".cls")):
                continue
            stem = str(path).replace("\\", "/").split("/")[-1].rsplit(".", 1)[0].strip().lower()
            if not stem:
                continue
            form_file_loc_by_base[stem] = int(form_file_loc_by_base.get(stem, 0) or 0) + int(loc or 0)
        if project_scoped_forms:
            for fid in sorted(forms_set):
                form_id = str(fid).strip()
                if not form_id:
                    continue
                form_tail = form_id.split(":", 1)[-1].strip().lower()
                if form_tail and form_tail not in project_scoped_form_bases:
                    discovered_unmapped_form_ids.append(form_id)
            form_iter = (
                project_scoped_forms
                + [("(unmapped)", fid) for fid in discovered_unmapped_form_ids]
            )[: self.LEGACY_MAX_FORMS]
        else:
            form_iter = [("", fid) for fid in sorted(forms_set)[: self.LEGACY_MAX_FORMS]]

        for project_name, form_id in form_iter:
            form_type = "Form"
            form_name = form_id
            if ":" in form_id:
                head, tail = form_id.split(":", 1)
                form_type = head or "Form"
                form_name = tail or form_id
            form_controls = sorted(form_control_map.get(form_id, set()))[:60]
            form_events = sorted(form_event_map.get(form_id, set()))[:60]
            if not form_events and form_controls:
                control_names = [row.split(":", 1)[1].strip().lower() for row in form_controls if ":" in row]
                inferred_events = [
                    ev for ev in sorted(event_handlers_set)
                    if str(ev).split("_", 1)[0].strip().lower() in control_names
                ]
                form_events = inferred_events[:60]
            normalized_form_name = str(form_name).strip().lower()
            expected_events: set[str] = set()
            for path in form_member_path_map.get((project_name, form_id), []):
                sig = vb6_by_path.get(self._normalize_legacy_path(path), {})
                if not isinstance(sig, dict):
                    continue
                for value in sig.get("event_handler_keys", []) if isinstance(sig.get("event_handler_keys", []), list) else []:
                    token = str(value).strip()
                    if token:
                        expected_events.add(token)
                for value in sig.get("event_handlers", []) if isinstance(sig.get("event_handlers", []), list) else []:
                    token = str(value).strip()
                    if token:
                        expected_events.add(f"{form_name}::{token}")
            ui_handlers = ui_form_handler_map.get(normalized_form_name, set())
            extracted_handlers_count = len(form_events)
            expected_handlers_count = max(len(expected_events), len(ui_handlers), extracted_handlers_count)
            explained_handlers_count = extracted_handlers_count
            sql_touched_count = int(ui_form_sql_touches.get(normalized_form_name, 0) or 0)
            member_loc = 0
            for member_path in form_member_path_map.get((project_name, form_id), []):
                member_loc += int(source_loc_by_file.get(self._normalize_legacy_path(member_path), 0) or 0)
            source_loc = int(member_loc or form_file_loc_by_base.get(normalized_form_name, 0) or 0)
            coverage_score = 1.0 if expected_handlers_count == 0 else min(1.0, extracted_handlers_count / float(expected_handlers_count))
            confidence_score = min(
                0.99,
                0.55
                + (0.28 * coverage_score)
                + (0.08 if sql_touched_count > 0 else 0.0)
                + (0.06 if bool(form_controls) else 0.0)
                + (0.03 if bool(project_name) else 0.0),
            )
            scoped_form_name = f"{project_name}::{form_name}" if project_name else form_name
            business_use = self._infer_form_business_use(form_name, form_controls, form_events)
            forms_details.append(
                {
                    "form_name": scoped_form_name,
                    "form_type": form_type,
                    "project_name": project_name,
                    "base_form_name": form_name,
                    "business_use": business_use,
                    "controls": form_controls,
                    "event_handlers": form_events,
                    "expected_handlers_count": expected_handlers_count,
                    "extracted_handlers_count": extracted_handlers_count,
                    "explained_handlers_count": explained_handlers_count,
                    "sql_touched_count": sql_touched_count,
                    "source_loc": source_loc,
                    "coverage_score": round(coverage_score, 4),
                    "confidence_score": round(confidence_score, 4),
                }
            )
            form_coverage.append(
                {
                    "form_name": scoped_form_name,
                    "project_name": project_name,
                    "coverage_score": round(coverage_score, 4),
                    "confidence_score": round(confidence_score, 4),
                    "expected_handlers_count": expected_handlers_count,
                    "extracted_handlers_count": extracted_handlers_count,
                    "explained_handlers_count": explained_handlers_count,
                    "sql_touched_count": sql_touched_count,
                    "source_loc": source_loc,
                    "risk_count": 0,
                }
            )

        all_forms_unique: list[str] = sorted(forms_set)[: self.LEGACY_MAX_FORMS]
        discovered_form_file_count = len(
            [
                p
                for p, sig in vb6_by_path.items()
                if isinstance(sig, dict)
                and str(p).lower().endswith((".frm", ".ctl"))
            ]
        )
        mapped_form_file_count = len(
            {
                self._normalize_legacy_path(path)
                for _, matches in form_member_path_map.items()
                for path in matches
                if str(path).strip()
            }
        )
        unmapped_form_file_count = max(0, discovered_form_file_count - mapped_form_file_count)
        referenced_form_count = sum(
            len(project.get("forms", []) if isinstance(project.get("forms", []), list) else [])
            for project in vb6_projects
            if isinstance(project, dict)
        )
        business_rules_catalog = self._extract_business_rules_catalog(
            bundle_file_map=bundle_file_map,
            vb6_projects=vb6_projects,
        )
        modernization_readiness = build_vb6_readiness_assessment(
            vb6_by_path=vb6_by_path,
            vb6_projects=vb6_projects,
        )
        readiness_score = int(modernization_readiness.get("score", 0) or 0)
        readiness_strategy = (
            modernization_readiness.get("recommended_strategy", {})
            if isinstance(modernization_readiness.get("recommended_strategy", {}), dict)
            else {}
        )
        ui_event_map = ui_event_rows_all[:240]
        pitfall_detectors = sorted(
            [row for row in pitfall_detector_index.values() if isinstance(row, dict)],
            key=lambda row: str(row.get("id", "")),
        )[:120]
        forms_count_reported = len(forms_details)
        event_handler_count_exact = max(len(event_handler_keys_set), len(ui_event_map), len(event_handlers_set))
        source_target_state = dict(state or {})
        if target_lang:
            source_target_state["modernization_language"] = target_lang
        source_target_profile = build_source_target_modernization_profile(
            legacy_skill_profile=legacy_skill_profile,
            legacy_inventory={
                "vb6_projects": vb6_projects,
                "modernization_readiness": modernization_readiness,
            },
            state=source_target_state,
        )
        project_business_summaries = build_project_business_summaries(
            vb6_projects=vb6_projects,
            source_target_profile=source_target_profile,
            global_readiness=modernization_readiness,
        )
        dependency_reference_rows = self._extract_vbp_dependency_references(project_definitions)

        legacy_inventory = {
            "summary": (
                f"Detected {len(vb6_projects)} VB6 project(s), {forms_count_reported} form entries "
                f"({referenced_form_count} referenced + {unmapped_form_file_count} unmapped discovered files), "
                f"{len(controls_set)} controls, {len(activex_set)} ActiveX/COM dependencies, "
                f"{event_handler_count_exact} event handlers, {len(bas_module_paths)} .bas modules "
                f"({bas_procedure_count} procedures). "
                f"Modernization readiness score={readiness_score}/100 "
                f"({str(readiness_strategy.get('name', 'strategy pending'))})."
            ),
            "form_count_referenced": referenced_form_count,
            "form_count_discovered_files": discovered_form_file_count,
            "form_count_unmapped_files": unmapped_form_file_count,
            "source_loc_total": int(source_loc_metrics.get("total_loc", 0) or 0),
            "source_loc_forms": int(source_loc_metrics.get("forms_loc", 0) or 0),
            "source_loc_modules": int(source_loc_metrics.get("modules_loc", 0) or 0),
            "source_loc_classes": int(source_loc_metrics.get("classes_loc", 0) or 0),
            "source_files_scanned": int(source_loc_metrics.get("files_scanned", 0) or 0),
            "source_loc_by_file": [
                {"path": path, "loc": int(loc or 0)}
                for path, loc in sorted(source_loc_by_file.items())
            ][:2000],
            "vb6_projects": vb6_projects,
            "forms": forms_details,
            "form_coverage": form_coverage[: self.LEGACY_MAX_FORMS],
            "activex_controls": sorted(activex_set)[: self.LEGACY_MAX_DEPENDENCIES],
            "dll_dependencies": sorted(dll_set)[: self.LEGACY_MAX_DEPENDENCIES],
            "ocx_dependencies": sorted(ocx_set)[: self.LEGACY_MAX_DEPENDENCIES],
            "dependency_references": dependency_reference_rows,
            "event_handlers": sorted(event_handlers_set)[: self.LEGACY_MAX_CONTROLS],
            "event_handler_keys": sorted(event_handler_keys_set)[: self.LEGACY_MAX_CONTROLS * 3],
            "event_handler_count_exact": event_handler_count_exact,
            "project_members": sorted(project_members_set)[: self.LEGACY_MAX_CONTROLS],
            "database_tables": sorted(tables_set)[:60],
            "procedures": sorted(functions_set)[:120],
            "input_signals": sorted(inputs_set)[:60],
            "side_effect_patterns": sorted(side_effects_set)[:40],
            "ui_event_map": ui_event_map,
            "sql_query_catalog": sorted(sql_query_set)[:160],
            "connection_strings": sorted(connection_strings_set)[:200],
            "database_file_references": sorted(database_file_refs_set)[:200],
            "connection_string_rows": connection_string_rows[:800],
            "database_file_reference_rows": database_file_reference_rows[:800],
            "module_global_declarations": module_global_declarations[:1000],
            "com_surface_map": {
                "late_bound_progids": sorted(com_progids_set)[:120],
                "call_by_name_sites": call_by_name_sites_total,
                "createobject_getobject_sites": createobject_sites_total,
                "references": sorted(com_references_set)[:120],
            },
            "win32_declares": sorted(win32_declares_set)[:120],
            "error_handling_profile": error_profile_totals,
            "pitfall_detectors": pitfall_detectors,
            "vb6_skill_pack_manifest": vb6_skill_pack_manifest(),
            "modernization_readiness": modernization_readiness,
            "migration_strategy_recommendation": readiness_strategy,
            "source_target_modernization_profile": source_target_profile,
            "project_business_summaries": project_business_summaries,
            "vb6_file_type_coverage": file_type_coverage,
            "bas_module_summary": {
                "module_count": len(bas_module_paths),
                "modules": sorted(bas_module_paths)[:120],
                "procedure_count": bas_procedure_count,
                "note": "Standard module (.bas) files are treated as primary business-logic sources.",
            },
            "binary_companion_files": binary_companions[:120],
            "business_rules_catalog": business_rules_catalog,
            "vb6_analysis": {
                "project_count": len(vb6_projects),
                "projects": [
                    {
                        "project_name": str(row.get("project_name", "")),
                        "project_file": str(row.get("project_file", "")),
                        "project_type": str(row.get("project_type", "")),
                        "startup_object": str(row.get("startup_object", "")),
                        "member_count": int(row.get("member_count", 0) or 0),
                        "forms_count": len(row.get("forms", []) if isinstance(row.get("forms", []), list) else []),
                        "controls_count": len(row.get("controls", []) if isinstance(row.get("controls", []), list) else []),
                        "activex_dependency_count": len(
                            row.get("activex_dependencies", [])
                            if isinstance(row.get("activex_dependencies", []), list)
                            else []
                        ),
                        "event_handler_count": len(
                            row.get("event_handlers", [])
                            if isinstance(row.get("event_handlers", []), list)
                            else []
                        ),
                        "event_handler_count_exact": int(
                            row.get("event_handler_count_exact", 0)
                            or len(
                                row.get("event_handler_keys", [])
                                if isinstance(row.get("event_handler_keys", []), list)
                                else []
                            )
                            or len(
                                row.get("event_handlers", [])
                                if isinstance(row.get("event_handlers", []), list)
                                else []
                            )
                        ),
                        "member_files": row.get("member_files", [])[:30]
                        if isinstance(row.get("member_files", []), list)
                        else [],
                        "business_objective_hypothesis": str(row.get("business_objective_hypothesis", "")),
                        "key_business_capabilities": row.get("key_business_capabilities", [])[:12]
                        if isinstance(row.get("key_business_capabilities", []), list)
                        else [],
                        "primary_workflows": row.get("primary_workflows", [])[:12]
                        if isinstance(row.get("primary_workflows", []), list)
                        else [],
                        "data_touchpoints": row.get("data_touchpoints", {})
                        if isinstance(row.get("data_touchpoints", {}), dict)
                        else {},
                        "technical_components": row.get("technical_components", {})
                        if isinstance(row.get("technical_components", {}), dict)
                        else {},
                        "modernization_considerations": row.get("modernization_considerations", [])[:12]
                        if isinstance(row.get("modernization_considerations", []), list)
                        else [],
                    }
                    for row in vb6_projects[: self.LEGACY_MAX_PROJECTS]
                    if isinstance(row, dict)
                ],
                "forms": [f"{pname}::{fid}" if pname else fid for pname, fid in form_iter][: self.LEGACY_MAX_FORMS],
                "forms_unique": all_forms_unique,
                "form_count_referenced": referenced_form_count,
                "form_count_discovered_files": discovered_form_file_count,
                "form_count_unmapped_files": unmapped_form_file_count,
                "source_loc_total": int(source_loc_metrics.get("total_loc", 0) or 0),
                "source_loc_forms": int(source_loc_metrics.get("forms_loc", 0) or 0),
                "source_loc_modules": int(source_loc_metrics.get("modules_loc", 0) or 0),
                "source_loc_classes": int(source_loc_metrics.get("classes_loc", 0) or 0),
                "source_files_scanned": int(source_loc_metrics.get("files_scanned", 0) or 0),
                "source_loc_by_file": [
                    {"path": path, "loc": int(loc or 0)}
                    for path, loc in sorted(source_loc_by_file.items())
                ][:2000],
                "controls": sorted(controls_set)[: self.LEGACY_MAX_CONTROLS],
                "activex_dependencies": sorted(activex_set)[: self.LEGACY_MAX_DEPENDENCIES],
                "dependency_references": dependency_reference_rows,
                "event_handlers": sorted(event_handlers_set)[: self.LEGACY_MAX_CONTROLS],
                "event_handler_keys": sorted(event_handler_keys_set)[: self.LEGACY_MAX_CONTROLS * 3],
                "event_handler_count_exact": event_handler_count_exact,
                "project_members": sorted(project_members_set)[: self.LEGACY_MAX_CONTROLS],
                "ui_event_map": ui_event_map[:160],
                "sql_query_catalog": sorted(sql_query_set)[:120],
                "connection_strings": sorted(connection_strings_set)[:200],
                "database_file_references": sorted(database_file_refs_set)[:200],
                "connection_string_rows": connection_string_rows[:800],
                "database_file_reference_rows": database_file_reference_rows[:800],
                "module_global_declarations": module_global_declarations[:1000],
                "com_surface_map": {
                    "late_bound_progids": sorted(com_progids_set)[:120],
                    "call_by_name_sites": call_by_name_sites_total,
                    "createobject_getobject_sites": createobject_sites_total,
                    "references": sorted(com_references_set)[:120],
                },
                "win32_declares": sorted(win32_declares_set)[:120],
                "error_handling_profile": error_profile_totals,
                "pitfall_detectors": pitfall_detectors[:80],
                "modernization_readiness": modernization_readiness,
                "source_target_modernization_profile": source_target_profile,
                "project_business_summaries": project_business_summaries,
                "vb6_file_type_coverage": file_type_coverage,
                "bas_module_summary": {
                    "module_count": len(bas_module_paths),
                    "modules": sorted(bas_module_paths)[:120],
                    "procedure_count": bas_procedure_count,
                },
                "binary_companion_files": binary_companions[:120],
                "business_rules_catalog": business_rules_catalog[:120],
                "form_coverage": form_coverage[: self.LEGACY_MAX_FORMS],
            },
        }

        compact_summary = (
            ("Inline" if inline else "Chunked")
            + " legacy analysis completed. "
            + f"chunks={chunk_count}, llm_chunks={llm_chunk_limit}, extracted_contracts={len(contracts)}, "
            + f"projects={len(vb6_projects)}, forms={forms_count_reported}, activex={len(activex_set)}, "
            + f"event_handlers={event_handler_count_exact}, dlls={len(dll_set)}, ocx={len(ocx_set)}, business_rules={len(business_rules_catalog)}, "
            + f"sql={len(sql_query_set)}, win32_declares={len(win32_declares_set)}, loc={int(source_loc_metrics.get('total_loc', 0) or 0)}, readiness={readiness_score}.\n"
            + f"source_target={str(source_target_profile.get('source', {}).get('language', 'legacy'))}"
            + f"->{str(source_target_profile.get('target', {}).get('language', 'unspecified'))}.\n"
            + "\n".join(summaries[:10])
        )
        return {
            "inline": inline,
            "chunk_count": chunk_count,
            "summary": compact_summary[:4000],
            "seed_legacy_contract": contracts[:30],
            "inventory": legacy_inventory,
            "legacy_skill_profile": legacy_skill_profile,
        }

    def _inventory_from_discover_cache(self, state: dict[str, Any]) -> dict[str, Any]:
        integration_ctx = state.get("integration_context", {}) if isinstance(state.get("integration_context", {}), dict) else {}
        discover_cache = (
            integration_ctx.get("discover_cache", {})
            if isinstance(integration_ctx.get("discover_cache", {}), dict)
            else {}
        )
        analyst_summary = (
            discover_cache.get("analyst_summary", {})
            if isinstance(discover_cache.get("analyst_summary", {}), dict)
            else {}
        )
        vb6 = analyst_summary.get("vb6_analysis", {}) if isinstance(analyst_summary.get("vb6_analysis", {}), dict) else {}
        if not vb6:
            return {}
        forms_raw = vb6.get("forms", []) if isinstance(vb6.get("forms", []), list) else []
        projects_raw = vb6.get("projects", []) if isinstance(vb6.get("projects", []), list) else []
        business_rules = vb6.get("business_rules_catalog", []) if isinstance(vb6.get("business_rules_catalog", []), list) else []
        ui_event_map = vb6.get("ui_event_map", []) if isinstance(vb6.get("ui_event_map", []), list) else []
        sql_catalog = vb6.get("sql_query_catalog", []) if isinstance(vb6.get("sql_query_catalog", []), list) else []
        com_surface = vb6.get("com_surface_map", {}) if isinstance(vb6.get("com_surface_map", {}), dict) else {}
        win32_declares = vb6.get("win32_declares", []) if isinstance(vb6.get("win32_declares", []), list) else []
        error_profile = vb6.get("error_handling_profile", {}) if isinstance(vb6.get("error_handling_profile", {}), dict) else {}
        pitfall_detectors = vb6.get("pitfall_detectors", []) if isinstance(vb6.get("pitfall_detectors", []), list) else []
        connection_strings = vb6.get("connection_strings", []) if isinstance(vb6.get("connection_strings", []), list) else []
        connection_string_rows = (
            vb6.get("connection_string_rows", [])
            if isinstance(vb6.get("connection_string_rows", []), list)
            else []
        )
        database_file_references = (
            vb6.get("database_file_references", [])
            if isinstance(vb6.get("database_file_references", []), list)
            else (
                vb6.get("database_file_refs", [])
                if isinstance(vb6.get("database_file_refs", []), list)
                else []
            )
        )
        normalized_db_refs: list[str] = []
        for value in database_file_references[:200]:
            if isinstance(value, dict):
                token = str(value.get("path", "") or value.get("db_path", "") or value.get("database", "")).strip()
            else:
                token = str(value).strip()
            if not token:
                continue
            normalized = self._normalize_legacy_path(token)
            if normalized and normalized not in normalized_db_refs:
                normalized_db_refs.append(normalized)
        database_file_reference_rows = (
            vb6.get("database_file_reference_rows", [])
            if isinstance(vb6.get("database_file_reference_rows", []), list)
            else []
        )
        module_global_declarations = (
            vb6.get("module_global_declarations", [])
            if isinstance(vb6.get("module_global_declarations", []), list)
            else []
        )
        readiness = vb6.get("modernization_readiness", {}) if isinstance(vb6.get("modernization_readiness", {}), dict) else {}
        source_target_profile = (
            vb6.get("source_target_modernization_profile", {})
            if isinstance(vb6.get("source_target_modernization_profile", {}), dict)
            else {}
        )
        if not source_target_profile:
            source_target_profile = (
                analyst_summary.get("source_target_modernization_profile", {})
                if isinstance(analyst_summary.get("source_target_modernization_profile", {}), dict)
                else {}
            )
        project_business_summaries = (
            vb6.get("project_business_summaries", [])
            if isinstance(vb6.get("project_business_summaries", []), list)
            else []
        )
        if not project_business_summaries:
            project_business_summaries = (
                analyst_summary.get("project_business_summaries", [])
                if isinstance(analyst_summary.get("project_business_summaries", []), list)
                else []
            )
        file_type_coverage = (
            vb6.get("vb6_file_type_coverage", {})
            if isinstance(vb6.get("vb6_file_type_coverage", {}), dict)
            else {}
        )
        bas_module_summary = (
            vb6.get("bas_module_summary", {})
            if isinstance(vb6.get("bas_module_summary", {}), dict)
            else {}
        )
        binary_companions = (
            vb6.get("binary_companion_files", [])
            if isinstance(vb6.get("binary_companion_files", []), list)
            else []
        )
        source_loc_by_file_rows = (
            vb6.get("source_loc_by_file", [])
            if isinstance(vb6.get("source_loc_by_file", []), list)
            else []
        )
        source_loc_by_file: dict[str, int] = {}
        for row in source_loc_by_file_rows[:5000]:
            if not isinstance(row, dict):
                continue
            path = self._normalize_legacy_path(str(row.get("path", "")))
            if not path:
                continue
            source_loc_by_file[path] = int(row.get("loc", 0) or 0)
        source_loc_total = int(vb6.get("source_loc_total", 0) or sum(source_loc_by_file.values()))
        source_loc_forms = int(
            vb6.get("source_loc_forms", 0)
            or sum(loc for path, loc in source_loc_by_file.items() if str(path).lower().endswith((".frm", ".ctl")))
        )
        source_loc_modules = int(
            vb6.get("source_loc_modules", 0)
            or sum(loc for path, loc in source_loc_by_file.items() if str(path).lower().endswith(".bas"))
        )
        source_loc_classes = int(
            vb6.get("source_loc_classes", 0)
            or sum(loc for path, loc in source_loc_by_file.items() if str(path).lower().endswith(".cls"))
        )
        source_files_scanned = int(vb6.get("source_files_scanned", 0) or len(source_loc_by_file))
        vb6_projects: list[dict[str, Any]] = []
        for row in projects_raw[: self.LEGACY_MAX_PROJECTS]:
            if not isinstance(row, dict):
                continue
            vb6_projects.append(
                {
                    "project_name": str(row.get("project_name", "")).strip() or "VB6 Project",
                    "project_file": str(row.get("project_file", "")).strip(),
                    "project_type": str(row.get("project_type", "")).strip(),
                    "startup_object": str(row.get("startup_object", "")).strip(),
                    "member_count": int(row.get("member_count", 0) or 0),
                    "member_files": row.get("member_files", [])[:80] if isinstance(row.get("member_files", []), list) else [],
                    "source_loc_total": int(row.get("source_loc_total", 0) or 0),
                    "source_loc_forms": int(row.get("source_loc_forms", 0) or 0),
                    "source_loc_modules": int(row.get("source_loc_modules", 0) or 0),
                    "source_loc_classes": int(row.get("source_loc_classes", 0) or 0),
                    "forms": row.get("forms", [])[: self.LEGACY_MAX_FORMS] if isinstance(row.get("forms", []), list) else [],
                    "controls": row.get("controls", [])[: self.LEGACY_MAX_CONTROLS] if isinstance(row.get("controls", []), list) else [],
                    "activex_dependencies": row.get("activex_dependencies", [])[: self.LEGACY_MAX_DEPENDENCIES]
                    if isinstance(row.get("activex_dependencies", []), list)
                    else [],
                    "event_handlers": row.get("event_handlers", [])[: self.LEGACY_MAX_CONTROLS]
                    if isinstance(row.get("event_handlers", []), list)
                    else [],
                    "business_objective_hypothesis": str(row.get("business_objective_hypothesis", "")).strip(),
                    "key_business_capabilities": row.get("key_business_capabilities", [])[:12]
                    if isinstance(row.get("key_business_capabilities", []), list)
                    else [],
                    "primary_workflows": row.get("primary_workflows", [])[:20]
                    if isinstance(row.get("primary_workflows", []), list)
                    else [],
                    "data_touchpoints": row.get("data_touchpoints", {}) if isinstance(row.get("data_touchpoints", {}), dict) else {},
                    "technical_components": row.get("technical_components", {}) if isinstance(row.get("technical_components", {}), dict) else {},
                    "modernization_considerations": row.get("modernization_considerations", [])[:20]
                    if isinstance(row.get("modernization_considerations", []), list)
                    else [],
                }
            )
        ui_form_control_map: dict[str, set[str]] = {}
        ui_form_event_map: dict[str, set[str]] = {}
        ui_form_sql_count: dict[str, int] = {}
        form_file_loc_by_base: dict[str, int] = {}
        for path, loc in source_loc_by_file.items():
            low_path = str(path).lower()
            if not low_path.endswith((".frm", ".ctl", ".cls")):
                continue
            stem = str(path).replace("\\", "/").split("/")[-1].rsplit(".", 1)[0].strip().lower()
            if not stem:
                continue
            form_file_loc_by_base[stem] = int(form_file_loc_by_base.get(stem, 0) or 0) + int(loc or 0)
        for row in ui_event_map[:400]:
            if not isinstance(row, dict):
                continue
            form_token = str(row.get("form", "")).strip()
            if not form_token:
                continue
            normalized = form_token.split("::", 1)[-1].strip().lower()
            if not normalized:
                continue
            control = str(row.get("control", "")).strip()
            event_handler = str(row.get("event_handler", "")).strip()
            if control:
                ui_form_control_map.setdefault(normalized, set()).add(control)
            if event_handler:
                ui_form_event_map.setdefault(normalized, set()).add(event_handler)
            sql_touches = row.get("sql_touches", []) if isinstance(row.get("sql_touches", []), list) else []
            ui_form_sql_count[normalized] = int(ui_form_sql_count.get(normalized, 0) or 0) + len(sql_touches)

        forms_details: list[dict[str, Any]] = []
        if vb6_projects:
            for project in vb6_projects[: self.LEGACY_MAX_PROJECTS]:
                pname = str(project.get("project_name", "")).strip() or "VB6 Project"
                for form_value in project.get("forms", []) if isinstance(project.get("forms", []), list) else []:
                    raw = str(form_value or "").strip()
                    if not raw:
                        continue
                    form_type = "Form"
                    form_name = raw
                    if ":" in raw:
                        head, tail = raw.split(":", 1)
                        form_type = head or "Form"
                        form_name = tail or raw
                    normalized = str(form_name).strip().lower()
                    controls = sorted(ui_form_control_map.get(normalized, set()))[:60]
                    event_handlers = sorted(ui_form_event_map.get(normalized, set()))[:120]
                    extracted = len(event_handlers)
                    expected = max(
                        extracted,
                        int(project.get("event_handler_count_exact", 0) or 0) // max(1, int(project.get("forms_count", 0) or len(project.get("forms", [])) or 1)),
                    )
                    member_files = project.get("member_files", []) if isinstance(project.get("member_files", []), list) else []
                    member_loc = 0
                    for member_path in member_files:
                        path_norm = self._normalize_legacy_path(str(member_path))
                        low_path = path_norm.lower()
                        if low_path.endswith(f"/{normalized}.frm") or low_path.endswith(f"/{normalized}.ctl") or low_path.endswith(f"/{normalized}.cls"):
                            member_loc += int(source_loc_by_file.get(path_norm, 0) or 0)
                    source_loc = int(member_loc or form_file_loc_by_base.get(normalized, 0) or 0)
                    coverage = 1.0 if expected <= 0 else min(1.0, extracted / float(expected))
                    confidence = min(
                        0.99,
                        0.55 + (0.25 * coverage) + (0.1 if controls else 0.0) + (0.08 if ui_form_sql_count.get(normalized, 0) > 0 else 0.0),
                    )
                    forms_details.append(
                        {
                            "form_name": f"{pname}::{form_name}",
                            "form_type": form_type,
                            "project_name": pname,
                            "base_form_name": form_name,
                            "business_use": self._infer_form_business_use(form_name, controls, event_handlers),
                            "controls": controls,
                            "event_handlers": event_handlers,
                            "expected_handlers_count": expected,
                            "extracted_handlers_count": extracted,
                            "explained_handlers_count": extracted,
                            "sql_touched_count": int(ui_form_sql_count.get(normalized, 0) or 0),
                            "source_loc": source_loc,
                            "coverage_score": round(coverage, 4),
                            "confidence_score": round(confidence, 4),
                        }
                    )
        existing_norm_forms = {
            str(row.get("base_form_name", "") or row.get("form_name", "")).split("::", 1)[-1].strip().lower()
            for row in forms_details
            if isinstance(row, dict)
        }
        for form_id in forms_raw[: self.LEGACY_MAX_FORMS * 2]:
            form_text = str(form_id).strip()
            if not form_text:
                continue
            form_type = "Form"
            form_name = form_text
            if ":" in form_text:
                head, tail = form_text.split(":", 1)
                form_type = head or "Form"
                form_name = tail or form_text
            normalized = str(form_name).strip().lower()
            if vb6_projects and normalized in existing_norm_forms:
                continue
            controls = sorted(ui_form_control_map.get(normalized, set()))[:60]
            event_handlers = sorted(ui_form_event_map.get(normalized, set()))[:120]
            extracted = len(event_handlers)
            expected = max(extracted, len(event_handlers))
            source_loc = int(form_file_loc_by_base.get(normalized, 0) or 0)
            coverage = 1.0 if expected <= 0 else min(1.0, extracted / float(expected))
            confidence = min(0.99, 0.55 + (0.25 * coverage) + (0.1 if controls else 0.0))
            forms_details.append(
                {
                    "form_name": form_name,
                    "form_type": form_type,
                    "project_name": "",
                    "base_form_name": form_name,
                    "business_use": self._infer_form_business_use(form_name, controls, event_handlers),
                    "controls": controls,
                    "event_handlers": event_handlers,
                    "expected_handlers_count": expected,
                    "extracted_handlers_count": extracted,
                    "explained_handlers_count": extracted,
                    "sql_touched_count": int(ui_form_sql_count.get(normalized, 0) or 0),
                    "source_loc": source_loc,
                    "coverage_score": round(coverage, 4),
                    "confidence_score": round(confidence, 4),
                }
            )
        if forms_details:
            dedupe_map: dict[str, dict[str, Any]] = {}
            for row in forms_details:
                key = f"{row.get('form_type','')}|{row.get('form_name','')}"
                existing = dedupe_map.get(key)
                if not existing:
                    dedupe_map[key] = row
                    continue
                existing_controls = existing.get("controls", []) if isinstance(existing.get("controls", []), list) else []
                existing_events = existing.get("event_handlers", []) if isinstance(existing.get("event_handlers", []), list) else []
                row_controls = row.get("controls", []) if isinstance(row.get("controls", []), list) else []
                row_events = row.get("event_handlers", []) if isinstance(row.get("event_handlers", []), list) else []
                merged_controls = sorted({str(x) for x in [*existing_controls, *row_controls] if str(x).strip()})[:60]
                merged_events = sorted({str(x) for x in [*existing_events, *row_events] if str(x).strip()})[:120]
                existing["controls"] = merged_controls
                existing["event_handlers"] = merged_events
                existing["extracted_handlers_count"] = len(merged_events)
                existing["explained_handlers_count"] = len(merged_events)
                existing["expected_handlers_count"] = max(
                    int(existing.get("expected_handlers_count", 0) or 0),
                    int(row.get("expected_handlers_count", 0) or 0),
                    len(merged_events),
                )
                expected = int(existing.get("expected_handlers_count", 0) or 0)
                coverage = 1.0 if expected <= 0 else min(1.0, len(merged_events) / float(expected))
                existing["coverage_score"] = round(coverage, 4)
                existing["source_loc"] = max(
                    int(existing.get("source_loc", 0) or 0),
                    int(row.get("source_loc", 0) or 0),
                )
                existing["confidence_score"] = round(
                    min(0.99, 0.55 + (0.25 * coverage) + (0.1 if merged_controls else 0.0)),
                    4,
                )
            forms_details = list(dedupe_map.values())[: self.LEGACY_MAX_FORMS * 2]
        form_coverage = [
            {
                "form_name": str(row.get("form_name", "")).strip(),
                "project_name": str(row.get("project_name", "")).strip(),
                "coverage_score": float(row.get("coverage_score", 0.0) or 0.0),
                "confidence_score": float(row.get("confidence_score", 0.0) or 0.0),
                "expected_handlers_count": int(row.get("expected_handlers_count", 0) or 0),
                "extracted_handlers_count": int(row.get("extracted_handlers_count", 0) or 0),
                "explained_handlers_count": int(row.get("explained_handlers_count", 0) or 0),
                "sql_touched_count": int(row.get("sql_touched_count", 0) or 0),
                "source_loc": int(row.get("source_loc", 0) or 0),
            }
            for row in forms_details[: self.LEGACY_MAX_FORMS]
            if isinstance(row, dict)
        ]
        event_handler_count_exact = int(
            vb6.get("event_handler_count_exact", 0)
            or len(vb6.get("event_handler_keys", []) if isinstance(vb6.get("event_handler_keys", []), list) else [])
            or len(ui_event_map)
            or len(vb6.get("event_handlers", []) if isinstance(vb6.get("event_handlers", []), list) else [])
        )
        referenced_form_count = int(
            vb6.get("form_count_referenced", 0)
            or sum(
                len(row.get("forms", []) if isinstance(row.get("forms", []), list) else [])
                for row in vb6_projects
                if isinstance(row, dict)
            )
        )
        discovered_form_file_count = int(vb6.get("form_count_discovered_files", 0) or len(forms_details))
        unmapped_form_file_count = int(
            vb6.get("form_count_unmapped_files", 0)
            or max(0, discovered_form_file_count - referenced_form_count)
        )
        return {
            "summary": (
                f"Discover cache indicates {len(vb6_projects)} VB6 project(s), {len(forms_details)} form entries "
                f"({referenced_form_count} referenced + {unmapped_form_file_count} unmapped discovered files), "
                f"{len(vb6.get('controls', []) if isinstance(vb6.get('controls', []), list) else [])} controls and "
                f"{len(vb6.get('activex_dependencies', []) if isinstance(vb6.get('activex_dependencies', []), list) else [])} ActiveX/COM dependencies, "
                f"{event_handler_count_exact} event handlers."
            ),
            "form_count_referenced": referenced_form_count,
            "form_count_discovered_files": discovered_form_file_count,
            "form_count_unmapped_files": unmapped_form_file_count,
            "source_loc_total": source_loc_total,
            "source_loc_forms": source_loc_forms,
            "source_loc_modules": source_loc_modules,
            "source_loc_classes": source_loc_classes,
            "source_files_scanned": source_files_scanned,
            "source_loc_by_file": [{"path": path, "loc": int(loc or 0)} for path, loc in sorted(source_loc_by_file.items())][:2000],
            "vb6_projects": vb6_projects,
            "forms": forms_details,
            "form_coverage": form_coverage,
            "activex_controls": vb6.get("activex_dependencies", []) if isinstance(vb6.get("activex_dependencies", []), list) else [],
            "dependency_references": vb6.get("dependency_references", []) if isinstance(vb6.get("dependency_references", []), list) else [],
            "dll_dependencies": [str(x) for x in vb6.get("activex_dependencies", []) if str(x).upper().endswith(".DLL")] if isinstance(vb6.get("activex_dependencies", []), list) else [],
            "ocx_dependencies": [str(x) for x in vb6.get("activex_dependencies", []) if str(x).upper().endswith(".OCX")] if isinstance(vb6.get("activex_dependencies", []), list) else [],
            "event_handlers": vb6.get("event_handlers", []) if isinstance(vb6.get("event_handlers", []), list) else [],
            "event_handler_keys": vb6.get("event_handler_keys", []) if isinstance(vb6.get("event_handler_keys", []), list) else [],
            "event_handler_count_exact": event_handler_count_exact,
            "project_members": vb6.get("project_members", []) if isinstance(vb6.get("project_members", []), list) else [],
            "database_tables": [],
            "procedures": [],
            "input_signals": [],
            "side_effect_patterns": [],
            "ui_event_map": ui_event_map[:160],
            "sql_query_catalog": sql_catalog[:120],
            "connection_strings": connection_strings[:200],
            "database_file_references": normalized_db_refs,
            "connection_string_rows": connection_string_rows[:800],
            "database_file_reference_rows": database_file_reference_rows[:800],
            "module_global_declarations": module_global_declarations[:1000],
            "com_surface_map": com_surface,
            "win32_declares": win32_declares[:120],
            "error_handling_profile": error_profile,
            "pitfall_detectors": pitfall_detectors[:80],
            "modernization_readiness": readiness,
            "source_target_modernization_profile": source_target_profile,
            "project_business_summaries": project_business_summaries[:32],
            "vb6_file_type_coverage": file_type_coverage,
            "bas_module_summary": bas_module_summary,
            "binary_companion_files": binary_companions[:120],
            "business_rules_catalog": business_rules[:120],
            "vb6_analysis": {
                "project_count": len(vb6_projects),
                "projects": vb6.get("projects", []) if isinstance(vb6.get("projects", []), list) else [],
                "forms": [str(row.get("form_name", "")) for row in forms_details if isinstance(row, dict)],
                "form_count_referenced": referenced_form_count,
                "form_count_discovered_files": discovered_form_file_count,
                "form_count_unmapped_files": unmapped_form_file_count,
                "source_loc_total": source_loc_total,
                "source_loc_forms": source_loc_forms,
                "source_loc_modules": source_loc_modules,
                "source_loc_classes": source_loc_classes,
                "source_files_scanned": source_files_scanned,
                "source_loc_by_file": [{"path": path, "loc": int(loc or 0)} for path, loc in sorted(source_loc_by_file.items())][:2000],
                "controls": vb6.get("controls", []) if isinstance(vb6.get("controls", []), list) else [],
                "activex_dependencies": vb6.get("activex_dependencies", []) if isinstance(vb6.get("activex_dependencies", []), list) else [],
                "dependency_references": vb6.get("dependency_references", []) if isinstance(vb6.get("dependency_references", []), list) else [],
                "event_handlers": vb6.get("event_handlers", []) if isinstance(vb6.get("event_handlers", []), list) else [],
                "event_handler_keys": vb6.get("event_handler_keys", []) if isinstance(vb6.get("event_handler_keys", []), list) else [],
                "event_handler_count_exact": event_handler_count_exact,
                "project_members": vb6.get("project_members", []) if isinstance(vb6.get("project_members", []), list) else [],
                "ui_event_map": ui_event_map[:160],
                "sql_query_catalog": sql_catalog[:120],
                "connection_strings": connection_strings[:200],
                "database_file_references": normalized_db_refs,
                "connection_string_rows": connection_string_rows[:800],
                "database_file_reference_rows": database_file_reference_rows[:800],
                "module_global_declarations": module_global_declarations[:1000],
                "com_surface_map": com_surface,
                "win32_declares": win32_declares[:120],
                "error_handling_profile": error_profile,
                "pitfall_detectors": pitfall_detectors[:80],
                "modernization_readiness": readiness,
                "source_target_modernization_profile": source_target_profile,
                "project_business_summaries": project_business_summaries[:32],
                "vb6_file_type_coverage": file_type_coverage,
                "bas_module_summary": bas_module_summary,
                "binary_companion_files": binary_companions[:120],
                "business_rules_catalog": business_rules[:120],
                "form_coverage": form_coverage,
            },
        }

    def _skill_from_discover_cache(self, state: dict[str, Any]) -> dict[str, Any]:
        integration_ctx = state.get("integration_context", {}) if isinstance(state.get("integration_context", {}), dict) else {}
        discover_cache = (
            integration_ctx.get("discover_cache", {})
            if isinstance(integration_ctx.get("discover_cache", {}), dict)
            else {}
        )
        analyst_summary = (
            discover_cache.get("analyst_summary", {})
            if isinstance(discover_cache.get("analyst_summary", {}), dict)
            else {}
        )
        skill = analyst_summary.get("legacy_skill_profile", {}) if isinstance(analyst_summary.get("legacy_skill_profile", {}), dict) else {}
        if not skill:
            return {}
        return {
            "selected_skill_id": str(skill.get("selected_skill_id", "generic_legacy")).strip() or "generic_legacy",
            "selected_skill_name": str(skill.get("selected_skill_name", "Generic Legacy Skill")).strip() or "Generic Legacy Skill",
            "confidence": skill.get("confidence", "n/a"),
            "reasons": [str(x).strip() for x in skill.get("reasons", []) if str(x).strip()] if isinstance(skill.get("reasons", []), list) else [],
        }

    def _build_database_compact_context(self, db_schema: str, db_source: str, db_target: str) -> dict[str, Any]:
        text = str(db_schema or "").strip()
        if not text:
            return {}
        tables = []
        for m in re.findall(r"(?im)^\s*create\s+table\s+([A-Za-z_][A-Za-z0-9_\.\[\]\"`]*)", text):
            t = str(m).strip().strip("[]`\"")
            if t and t not in tables:
                tables.append(t)
            if len(tables) >= 40:
                break
        summary = (
            f"Schema context: source={db_source or 'unknown'}, target={db_target or 'unknown'}, "
            f"table_hints={len(tables)}."
        )
        excerpt = text[: self.DB_SCHEMA_INLINE_MAX_CHARS]
        return {"summary": summary, "tables": tables[:30], "schema_excerpt": excerpt}

    def generate_questions(self, business_objectives: str) -> dict[str, Any]:
        """
        Generate clarifying questions before producing requirements.

        Returns a dict with a "questions" key containing the list of questions.
        """
        self.log(f"[{self.name}] Generating clarifying questions...")

        user_msg = f"""Review these business objectives and generate clarifying questions
that will help you write better requirements.

BUSINESS OBJECTIVES:
{business_objectives}"""

        response = self.llm.invoke(self.QUESTIONS_SYSTEM_PROMPT, user_msg)
        self.log(f"[{self.name}] Questions generated ({response.output_tokens} tokens)")

        return self.extract_json(response.content)

    def _integration_context(self, state: dict[str, Any]) -> dict[str, Any]:
        ctx = state.get("integration_context", {})
        return ctx if isinstance(ctx, dict) else {}

    def _analyst_persona_context(self, state: dict[str, Any]) -> dict[str, Any]:
        personas = state.get("agent_personas", {})
        if not isinstance(personas, dict):
            return {}
        stage_payload = personas.get("1", {})
        return stage_payload if isinstance(stage_payload, dict) else {}

    def _select_standards_guidance(self, domain_pack: dict[str, Any], capability_ids: list[str]) -> list[dict[str, Any]]:
        standards = domain_pack.get("standards", [])
        selected: list[dict[str, Any]] = []
        cap_set = {str(x or "").strip() for x in capability_ids if str(x or "").strip()}
        for item in standards if isinstance(standards, list) else []:
            if not isinstance(item, dict):
                continue
            applies_to = {
                str(x or "").strip()
                for x in item.get("applies_to", [])
                if str(x or "").strip()
            }
            if cap_set and applies_to and not cap_set.intersection(applies_to):
                continue
            selected.append(
                {
                    "id": str(item.get("id", "")),
                    "name": str(item.get("name", "")),
                    "engineering_actions": list(item.get("engineering_actions", []))
                    if isinstance(item.get("engineering_actions", []), list)
                    else [],
                }
            )
        return selected[:8]

    def _build_domain_model_excerpt(self, capability_ids: list[str], use_case: str) -> dict[str, Any]:
        entities: set[str] = set()
        states: set[str] = set()
        relationships: set[str] = set()

        if "payments_execution" in capability_ids:
            entities.update(["PaymentInstruction", "Account", "LedgerEntry", "SettlementBatch"])
            states.update(["initiated", "authorized", "pending_settlement", "settled", "reversed", "failed"])
            relationships.update(
                [
                    "Customer initiates PaymentInstruction",
                    "PaymentInstruction posts LedgerEntry",
                    "SettlementBatch reconciles PaymentInstruction outcomes",
                ]
            )
        if "ledger_management" in capability_ids:
            entities.update(["Ledger", "JournalEntry", "Transaction"])
            states.update(["recorded", "posted", "reconciled", "reversed"])
            relationships.add("Transaction updates Ledger through JournalEntry")
        if "customer_profile" in capability_ids:
            entities.update(["Customer", "KYCProfile", "ConsentRecord"])
            states.update(["created", "verified", "suspended", "closed"])
            relationships.add("Customer owns KYCProfile and ConsentRecord")
        if "loan_servicing" in capability_ids:
            entities.update(["LoanAccount", "RepaymentSchedule", "Disbursement", "Collateral"])
            states.update(["application_received", "approved", "disbursed", "active", "delinquent", "closed"])
            relationships.add("LoanAccount references RepaymentSchedule and Disbursement events")
        if "risk_assessment" in capability_ids:
            entities.update(["RiskAssessment", "RiskDecision", "RiskRule"])
            states.update(["evaluated", "approved", "declined", "escalated"])
            relationships.add("RiskAssessment produces RiskDecision via RiskRule set")
        if "fraud_detection" in capability_ids:
            entities.update(["FraudSignal", "Alert", "Case"])
            states.update(["detected", "triaged", "investigating", "resolved"])
            relationships.add("FraudSignal triggers Alert and investigation Case")

        if not entities:
            entities.update(["User", "ServiceRequest", "DomainEvent", "AuditEvent"])
            states.update(["requested", "validated", "processed", "completed", "failed"])
            relationships.add("ServiceRequest emits DomainEvent and AuditEvent")

        if str(use_case or "").strip().lower() == "code_modernization":
            relationships.add("LegacyInput contract must map deterministically to ModernizedOutput contract")
        if str(use_case or "").strip().lower() == "database_conversion":
            relationships.add("SourceSchema object maps to TargetSchema migration artifacts")

        return {
            "entities": sorted(entities),
            "lifecycle_states": sorted(states),
            "relationships": sorted(relationships),
        }

    def _resolve_custom_domain_pack(
        self,
        state: dict[str, Any],
        integration_context: dict[str, Any],
        analyst_persona: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        """
        Build a safe custom domain pack from state/integration context.
        Falls back to a generalized template for any missing fields.
        """
        raw_custom = {}
        state_custom = state.get("custom_domain_pack")
        if isinstance(state_custom, dict) and state_custom:
            raw_custom = state_custom
        else:
            integration_custom = integration_context.get("custom_domain_pack")
            if isinstance(integration_custom, dict) and integration_custom:
                raw_custom = integration_custom
        if (not isinstance(raw_custom, dict) or not raw_custom) and isinstance(analyst_persona, dict):
            persona_template = analyst_persona.get("requirements_pack_template", {})
            if isinstance(persona_template, dict) and persona_template:
                if isinstance(persona_template.get("project", {}), dict):
                    project = persona_template.get("project", {})
                    dp = project.get("domain_pack", {}) if isinstance(project.get("domain_pack", {}), dict) else {}
                    if isinstance(dp, dict) and dp:
                        requested_id = str(dp.get("id", "")).strip()
                        resolved = get_domain_pack(requested_id) if requested_id else {}
                        resolved_id = str(resolved.get("id", "")).strip() if isinstance(resolved, dict) else ""
                        if requested_id and resolved_id == requested_id:
                            # Built-in domain pack reference from template: use normal pack resolution path.
                            return {}
                        raw_custom = {
                            "id": requested_id or "custom-domain-pack-v1",
                            "name": str(dp.get("name", "")).strip() or "Custom Domain Pack",
                            "version": str(dp.get("version", "")).strip() or "1.0.0",
                            "ontology": {"framework": "Capability Taxonomy", "capabilities": []},
                            "standards": [],
                            "regulations": [],
                            "gold_patterns": [],
                            "rules": {"non_negotiables": [], "completeness_checklist": []},
                            "evaluation_harness": {},
                        }
        if not isinstance(raw_custom, dict) or not raw_custom:
            return {}

        base = get_domain_pack("software-general-v1")
        custom = dict(raw_custom)
        explicit_id = str(integration_context.get("domain_pack_id", "")).strip()
        pack_id = str(custom.get("id", "")).strip() or explicit_id or "custom-domain-pack-v1"
        name = str(custom.get("name", "")).strip() or "Custom Domain Pack"
        version = str(custom.get("version", "")).strip() or "1.0.0"

        ontology = custom.get("ontology", {})
        if not isinstance(ontology, dict):
            ontology = {}
        capabilities = ontology.get("capabilities", [])
        if not isinstance(capabilities, list):
            capabilities = []

        rules = custom.get("rules", {})
        if not isinstance(rules, dict):
            rules = {}
        evaluation_harness = custom.get("evaluation_harness", {})
        if not isinstance(evaluation_harness, dict):
            evaluation_harness = {}

        merged = dict(base)
        merged.update(
            {
                "id": pack_id,
                "name": name,
                "version": version,
                "ontology": {
                    "framework": str(ontology.get("framework", "Capability Taxonomy") or "Capability Taxonomy"),
                    "capabilities": [item for item in capabilities if isinstance(item, dict)],
                },
                "standards": [item for item in custom.get("standards", []) if isinstance(item, dict)]
                if isinstance(custom.get("standards", []), list)
                else [],
                "regulations": [item for item in custom.get("regulations", []) if isinstance(item, dict)]
                if isinstance(custom.get("regulations", []), list)
                else [],
                "gold_patterns": [item for item in custom.get("gold_patterns", []) if isinstance(item, dict)]
                if isinstance(custom.get("gold_patterns", []), list)
                else [],
                "rules": {
                    "non_negotiables": [str(x) for x in rules.get("non_negotiables", []) if str(x).strip()]
                    if isinstance(rules.get("non_negotiables", []), list)
                    else [],
                    "completeness_checklist": [str(x) for x in rules.get("completeness_checklist", []) if str(x).strip()]
                    if isinstance(rules.get("completeness_checklist", []), list)
                    else [],
                },
                "evaluation_harness": {
                    "minimum_functional_requirements": int(
                        evaluation_harness.get(
                            "minimum_functional_requirements",
                            base.get("evaluation_harness", {}).get("minimum_functional_requirements", 6),
                        )
                        or 6
                    ),
                    "minimum_non_functional_requirements": int(
                        evaluation_harness.get(
                            "minimum_non_functional_requirements",
                            base.get("evaluation_harness", {}).get("minimum_non_functional_requirements", 4),
                        )
                        or 4
                    ),
                    "minimum_bdd_scenarios": int(
                        evaluation_harness.get(
                            "minimum_bdd_scenarios",
                            base.get("evaluation_harness", {}).get("minimum_bdd_scenarios", 4),
                        )
                        or 4
                    ),
                    "required_quality_gates": [
                        str(x)
                        for x in evaluation_harness.get(
                            "required_quality_gates",
                            base.get("evaluation_harness", {}).get("required_quality_gates", []),
                        )
                        if str(x).strip()
                    ],
                },
            }
        )
        return merged

    def _build_deterministic_context(self, state: dict[str, Any]) -> dict[str, Any]:
        objectives = str(state.get("business_objectives", "")).strip()
        use_case = str(state.get("use_case", "business_objectives")).strip().lower()
        integration_context = self._integration_context(state)
        analyst_persona = self._analyst_persona_context(state)
        selection = str(integration_context.get("domain_pack_selection", "")).strip().lower()
        persona_profile = str(analyst_persona.get("requirements_pack_profile", "")).strip().lower()
        persona_pack_hint = ""
        if persona_profile == "requirements-pack-v2-banking":
            persona_pack_hint = "banking-core-v1"
        elif persona_profile in {"requirements-pack-v2-general", "requirements-pack-v2-custom"}:
            persona_pack_hint = "software-general-v1"

        explicit_pack = str(state.get("domain_pack_id", "")).strip() or str(
            integration_context.get("domain_pack_id", "")
        ).strip()
        if not explicit_pack and selection == "auto":
            explicit_pack = persona_pack_hint
        custom_domain_pack = self._resolve_custom_domain_pack(state, integration_context, analyst_persona=analyst_persona)
        if custom_domain_pack:
            domain_pack = custom_domain_pack
            domain_pack_id = str(custom_domain_pack.get("id", "")).strip() or "custom-domain-pack-v1"
        else:
            domain_pack_id = infer_domain_pack_id(objectives, explicit_pack)
            domain_pack = get_domain_pack(domain_pack_id)
        normalized = normalize_requirement(objectives, use_case=use_case)
        capability_map = map_to_capabilities(domain_pack, normalized)
        primary_capabilities = capability_map.get("primary_capabilities", [])
        capability_ids = [
            str(item.get("id", ""))
            for item in primary_capabilities
            if isinstance(item, dict) and str(item.get("id", "")).strip()
        ]
        jurisdiction = infer_jurisdiction(objectives, integration_context=integration_context)
        data_classification = infer_data_classification(objectives, integration_context=integration_context)
        regulatory_constraints = retrieve_regulatory_constraints(
            domain_pack,
            capability_ids=capability_ids,
            jurisdiction=jurisdiction,
            data_classes=data_classification,
        )
        gold_patterns = retrieve_gold_patterns(domain_pack, capability_ids=capability_ids)
        standards_guidance = self._select_standards_guidance(domain_pack, capability_ids=capability_ids)
        open_questions = build_open_questions(normalized, capability_map, regulatory_constraints)
        non_negotiables = list(domain_pack.get("rules", {}).get("non_negotiables", []))
        completeness = list(domain_pack.get("rules", {}).get("completeness_checklist", []))
        evaluation_harness = domain_pack.get("evaluation_harness", {})
        domain_model = self._build_domain_model_excerpt(capability_ids, use_case)
        dag_trace = [
            {
                "step": "normalize_requirement",
                "status": "completed",
                "details": f"actors={len(normalized.get('actors', []))}, actions={len(normalized.get('actions', []))}, objects={len(normalized.get('objects', []))}",
            },
            {
                "step": "map_to_capabilities",
                "status": "completed",
                "details": f"primary={len(primary_capabilities)}, alternatives={len(capability_map.get('alternative_capabilities', []))}",
            },
            {
                "step": "retrieve_internal_patterns",
                "status": "completed",
                "details": f"patterns={len(gold_patterns)}, standards={len(standards_guidance)}",
            },
            {
                "step": "retrieve_compliance_constraints",
                "status": "completed",
                "details": f"jurisdiction={jurisdiction}, data_classification={','.join(data_classification)}, constraints={len(regulatory_constraints)}",
            },
            {
                "step": "synthesize_requirements_pack",
                "status": "pending",
                "details": "awaiting final synthesis",
            },
            {
                "step": "run_quality_gates",
                "status": "pending",
                "details": "awaiting quality checks",
            },
        ]
        return {
            "domain_pack_ref": {
                "id": str(domain_pack.get("id", domain_pack_id)),
                "name": str(domain_pack.get("name", "Domain Pack")),
                "version": str(domain_pack.get("version", "1.0.0")),
            },
            "requirements_pack_profile": persona_profile or "requirements-pack-v2-general",
            "requirements_pack_template": (
                analyst_persona.get("requirements_pack_template", {})
                if isinstance(analyst_persona.get("requirements_pack_template", {}), dict)
                else {}
            ),
            "normalized_requirement": normalized,
            "capability_mapping": capability_map,
            "jurisdiction": jurisdiction,
            "data_classification": data_classification,
            "regulatory_constraints": regulatory_constraints,
            "gold_patterns": gold_patterns,
            "standards_guidance": standards_guidance,
            "domain_model_excerpt": domain_model,
            "open_questions": open_questions,
            "non_negotiables": non_negotiables,
            "completeness_checklist": completeness,
            "evaluation_harness": evaluation_harness if isinstance(evaluation_harness, dict) else {},
            "analyst_dag_trace": dag_trace,
        }

    def _normalize_functional_requirements(self, raw: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        items = raw if isinstance(raw, list) else []
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            rid = str(item.get("id", "")).strip() or f"FR-{idx:03d}"
            title = str(item.get("title", "")).strip() or f"Functional Requirement {idx}"
            desc = str(item.get("description", "")).strip() or "Requirement details not provided."
            priority = str(item.get("priority", "P1")).strip().upper()
            if priority not in {"P0", "P1", "P2"}:
                priority = "P1"
            ac = [str(x).strip() for x in item.get("acceptance_criteria", []) if str(x).strip()] if isinstance(item.get("acceptance_criteria", []), list) else []
            while len(ac) < 3:
                ac.append(f"{rid} acceptance criterion {len(ac) + 1} is measurable and testable.")
            out.append(
                {
                    "id": rid,
                    "title": title,
                    "description": desc,
                    "priority": priority,
                    "acceptance_criteria": ac,
                }
            )
        return out

    def _normalize_non_functional_requirements(self, raw: Any) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        items = raw if isinstance(raw, list) else []
        valid_categories = {"performance", "security", "scalability", "reliability", "usability"}
        for idx, item in enumerate(items, start=1):
            if not isinstance(item, dict):
                continue
            rid = str(item.get("id", "")).strip() or f"NFR-{idx:03d}"
            title = str(item.get("title", "")).strip() or f"Non-Functional Requirement {idx}"
            desc = str(item.get("description", "")).strip() or "NFR details not provided."
            category = str(item.get("category", "reliability")).strip().lower()
            if category not in valid_categories:
                category = "reliability"
            metric = str(item.get("metric", "")).strip() or "Define measurable target before implementation."
            ac = [str(x).strip() for x in item.get("acceptance_criteria", []) if str(x).strip()] if isinstance(item.get("acceptance_criteria", []), list) else []
            while len(ac) < 3:
                ac.append(f"{rid} criterion {len(ac) + 1} is measurable and testable.")
            out.append(
                {
                    "id": rid,
                    "title": title,
                    "description": desc,
                    "category": category,
                    "metric": metric,
                    "acceptance_criteria": ac,
                }
            )
        return out

    def _augment_functional_requirements(
        self,
        functional: list[dict[str, Any]],
        deterministic: dict[str, Any],
    ) -> list[dict[str, Any]]:
        minimum = int(deterministic.get("evaluation_harness", {}).get("minimum_functional_requirements", 8) or 8)
        capability_items = deterministic.get("capability_mapping", {}).get("primary_capabilities", [])
        capability_titles = [
            str(item.get("service_domain", "")).strip() or str(item.get("business_capability", "")).strip()
            for item in capability_items
            if isinstance(item, dict)
        ]
        if not capability_titles:
            capability_titles = ["Core Platform Capability"]

        generated_idx = 1
        while len(functional) < minimum:
            cap = capability_titles[(generated_idx - 1) % len(capability_titles)]
            rid = f"FR-AUTO-{generated_idx:03d}"
            functional.append(
                {
                    "id": rid,
                    "title": f"Implement {cap} workflow contract",
                    "description": f"Implement deterministic workflow behavior for {cap} with explicit input/output and error contracts.",
                    "priority": "P1",
                    "acceptance_criteria": [
                        f"{rid}: Inputs are validated against contract before processing.",
                        f"{rid}: Outputs include deterministic status and correlation identifiers.",
                        f"{rid}: Error paths are captured with actionable diagnostics.",
                    ],
                }
            )
            generated_idx += 1
        return functional

    def _augment_non_functional_requirements(
        self,
        nfr: list[dict[str, Any]],
        deterministic: dict[str, Any],
    ) -> list[dict[str, Any]]:
        minimum = int(deterministic.get("evaluation_harness", {}).get("minimum_non_functional_requirements", 5) or 5)
        templates = [
            ("NFR-AUTO-001", "Auditability", "security", "100% critical state changes are logged with actor, action, and outcome."),
            ("NFR-AUTO-002", "Observability", "reliability", "Critical flows emit trace/correlation identifiers for diagnosis."),
            ("NFR-AUTO-003", "Performance parity", "performance", "Critical legacy flows do not regress by more than 20% versus baseline."),
            ("NFR-AUTO-004", "Operational reliability", "reliability", "Recovery and fallback procedures are documented and validated for critical failures."),
            ("NFR-AUTO-005", "Capacity planning", "scalability", "Target runtime sustains measured legacy peak load with agreed safety headroom."),
            ("NFR-AUTO-006", "Security posture", "security", "All sensitive data is encrypted at rest and in transit."),
        ]
        used_ids = {str(item.get("id", "")).strip() for item in nfr}
        for rid, title, category, metric in templates:
            if len(nfr) >= minimum:
                break
            if rid in used_ids:
                continue
            nfr.append(
                {
                    "id": rid,
                    "title": title,
                    "description": f"{title} requirements must be explicitly validated before release.",
                    "category": category,
                    "metric": metric,
                    "acceptance_criteria": [
                        f"{rid}: Metric is continuously measured in CI/CD or runtime dashboards.",
                        f"{rid}: Threshold breach triggers alerting and remediation workflow.",
                        f"{rid}: Compliance evidence is retained for release review.",
                    ],
                }
            )
            used_ids.add(rid)
        return nfr

    def _tokenize_grounding_text(self, text: str) -> set[str]:
        stop = {
            "the", "and", "for", "with", "that", "this", "from", "into", "must", "should", "will",
            "are", "was", "were", "have", "has", "had", "using", "through", "under", "without",
            "application", "system", "legacy", "modernized", "modernization", "workflow", "requirement",
            "data", "flow", "user", "users", "service", "services", "support", "supports",
        }
        tokens = {t for t in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", str(text or "").lower())}
        return {t for t in tokens if t not in stop}

    def _collect_legacy_grounding_terms(self, legacy_inventory: dict[str, Any]) -> set[str]:
        terms: set[str] = set()
        forms = legacy_inventory.get("forms", [])
        if isinstance(forms, list):
            for row in forms[:400]:
                if isinstance(row, dict):
                    terms.update(self._tokenize_grounding_text(str(row.get("base_form_name", ""))))
                    terms.update(self._tokenize_grounding_text(str(row.get("form_name", ""))))
                    terms.update(self._tokenize_grounding_text(str(row.get("business_use", ""))))
                else:
                    terms.update(self._tokenize_grounding_text(str(row)))
        tables = legacy_inventory.get("database_tables", [])
        if isinstance(tables, list):
            for row in tables[:300]:
                terms.update(self._tokenize_grounding_text(str(row)))
        procedures = legacy_inventory.get("procedures", [])
        if isinstance(procedures, list):
            for row in procedures[:500]:
                terms.update(self._tokenize_grounding_text(str(row)))
        rules = legacy_inventory.get("business_rules_catalog", [])
        if isinstance(rules, list):
            for rule in rules[:500]:
                if not isinstance(rule, dict):
                    continue
                terms.update(self._tokenize_grounding_text(str(rule.get("statement", ""))))
                terms.update(self._tokenize_grounding_text(str(rule.get("scope", ""))))
        projects = legacy_inventory.get("vb6_projects", [])
        if isinstance(projects, list):
            for project in projects[:64]:
                if not isinstance(project, dict):
                    continue
                terms.update(self._tokenize_grounding_text(str(project.get("project_name", ""))))
                for arr_key in ("forms", "key_business_capabilities", "primary_workflows"):
                    rows = project.get(arr_key, [])
                    if isinstance(rows, list):
                        for row in rows[:120]:
                            terms.update(self._tokenize_grounding_text(str(row)))
        return terms

    def _classify_requirement_grounding(
        self,
        item: dict[str, Any],
        legacy_terms: set[str],
        *,
        is_non_functional: bool,
    ) -> tuple[str, str]:
        title = str(item.get("title", "")).strip()
        description = str(item.get("description", "")).strip()
        acceptance = item.get("acceptance_criteria", [])
        acceptance_text = " ".join(str(x) for x in acceptance if str(x).strip()) if isinstance(acceptance, list) else ""
        text = " ".join([title, description, acceptance_text]).lower()
        extension_markers = [
            "payment gateway",
            "horizontal scaling",
            "kubernetes",
            "microservice",
            "10,000 concurrent",
            "1000 concurrent",
            "99.9%",
            "global rollout",
        ]
        if any(marker in text for marker in extension_markers):
            return ("proposed_extension", "contains capability/scale targets not evidenced in legacy scan")

        overlap = len(self._tokenize_grounding_text(text).intersection(legacy_terms))
        if overlap >= 2:
            return ("derived_from_legacy", f"matched {overlap} legacy terms")
        if is_non_functional and overlap == 0 and any(
            marker in text for marker in ("failover", "horizontal", "multi-region", "auto-scaling", "five nines")
        ):
            return ("proposed_extension", "non-functional target appears cloud-template based")
        if overlap >= 1:
            return ("derived_from_legacy", f"matched {overlap} legacy term")
        return ("proposed_extension", "no direct grounding evidence in extracted legacy artifacts")

    def _ground_requirements_for_legacy_parity(
        self,
        functional: list[dict[str, Any]],
        non_functional: list[dict[str, Any]],
        legacy_inventory: dict[str, Any],
    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, list[dict[str, Any]]]]:
        legacy_terms = self._collect_legacy_grounding_terms(legacy_inventory)
        proposed: dict[str, list[dict[str, Any]]] = {"functional": [], "non_functional": []}
        grounded_f: list[dict[str, Any]] = []
        grounded_n: list[dict[str, Any]] = []

        for row in functional:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            classification, reason = self._classify_requirement_grounding(item, legacy_terms, is_non_functional=False)
            item["grounding"] = {"classification": classification, "reason": reason}
            if classification == "derived_from_legacy":
                grounded_f.append(item)
            else:
                proposed["functional"].append(item)

        for row in non_functional:
            if not isinstance(row, dict):
                continue
            item = dict(row)
            classification, reason = self._classify_requirement_grounding(item, legacy_terms, is_non_functional=True)
            item["grounding"] = {"classification": classification, "reason": reason}
            if classification == "derived_from_legacy":
                grounded_n.append(item)
            else:
                proposed["non_functional"].append(item)

        # Do not return empty lists; preserve forward progress if grounding fails unexpectedly.
        if not grounded_f:
            grounded_f = [dict(row) for row in functional if isinstance(row, dict)]
        if not grounded_n:
            grounded_n = [dict(row) for row in non_functional if isinstance(row, dict)]

        return grounded_f, grounded_n, proposed

    def _build_grounded_bdd_features(
        self,
        parsed: dict[str, Any],
        functional_requirements: list[dict[str, Any]],
        *,
        limit: int,
        existing_ids: set[str],
    ) -> list[dict[str, Any]]:
        legacy_inventory = (
            parsed.get("legacy_code_inventory", {})
            if isinstance(parsed.get("legacy_code_inventory", {}), dict)
            else {}
        )
        vb6_analysis = (
            parsed.get("vb6_analysis", {})
            if isinstance(parsed.get("vb6_analysis", {}), dict)
            else {}
        )
        ui_rows = legacy_inventory.get("ui_event_map", [])
        if not isinstance(ui_rows, list) or not ui_rows:
            ui_rows = vb6_analysis.get("ui_event_map", [])
        if not isinstance(ui_rows, list):
            ui_rows = []

        def _normalize_form_label(raw: str) -> str:
            text = str(raw or "").strip()
            if not text:
                return ""
            if "::" in text:
                return text
            lowered = text.lower()
            if lowered.startswith(("form:", "usercontrol:", "control:", "module:")):
                return text.split(":", 1)[-1].strip()
            return text

        handler_to_form: dict[str, str] = {}
        for form_row in legacy_inventory.get("forms", []) if isinstance(legacy_inventory.get("forms", []), list) else []:
            if not isinstance(form_row, dict):
                continue
            form_name = _normalize_form_label(
                str(form_row.get("form_name", "")).strip()
                or str(form_row.get("base_form_name", "")).strip()
            )
            if not form_name:
                continue
            event_handlers = form_row.get("event_handlers", [])
            if not isinstance(event_handlers, list):
                continue
            for handler_name in event_handlers:
                token = str(handler_name or "").strip().lower()
                if token and token not in handler_to_form:
                    handler_to_form[token] = form_name

        def _tokens(text: str) -> set[str]:
            return {
                t
                for t in re.findall(r"[a-zA-Z][a-zA-Z0-9_]{2,}", str(text or "").lower())
                if t not in {"form", "event", "click", "load", "change", "handler", "legacy", "workflow"}
            }

        def _extract_tables(sql_touches: list[str]) -> list[str]:
            found: list[str] = []
            seen: set[str] = set()
            for sql in sql_touches[:20]:
                for m in re.findall(r"(?i)\b(?:from|join|into|update)\s+([A-Za-z_][A-Za-z0-9_]*)", str(sql or "")):
                    name = str(m or "").strip()
                    if not name:
                        continue
                    key = name.lower()
                    if key in seen:
                        continue
                    seen.add(key)
                    found.append(name)
            return found[:6]

        rows: list[dict[str, Any]] = []
        seen_keys: set[str] = set()
        for row in ui_rows[:800]:
            if not isinstance(row, dict):
                continue
            form = _normalize_form_label(str(row.get("form", "")).strip())
            handler = str(row.get("event_handler", "")).strip()
            event = str(row.get("event", "")).strip()
            control = str(row.get("control", "")).strip()
            if not handler and not control:
                continue
            if not form and handler:
                form = _normalize_form_label(handler_to_form.get(handler.lower(), ""))
            form_base = form
            key = f"{form_base.lower()}|{handler.lower()}|{event.lower()}|{control.lower()}"
            if key in seen_keys:
                continue
            seen_keys.add(key)
            sql_touches = [str(x).strip() for x in row.get("sql_touches", []) if str(x).strip()] if isinstance(row.get("sql_touches", []), list) else []
            tables = _extract_tables(sql_touches)
            rows.append(
                {
                    "form": form_base or form or "legacy_form",
                    "handler": handler or f"{control}_{event}" if control and event else (handler or "legacy_handler"),
                    "event": event,
                    "control": control,
                    "sql_touches": sql_touches[:6],
                    "tables": tables,
                }
            )
        rows.sort(key=lambda r: (len(r.get("sql_touches", [])), len(r.get("tables", []))), reverse=True)

        if not rows:
            return []

        req_index: list[tuple[str, set[str], str]] = []
        fallback_req = ""
        for fr in functional_requirements:
            if not isinstance(fr, dict):
                continue
            rid = str(fr.get("id", "")).strip()
            title = str(fr.get("title", "")).strip()
            desc = str(fr.get("description", "")).strip()
            priority = str(fr.get("priority", "P1")).strip().upper()
            if not rid:
                continue
            if not fallback_req or priority == "P0":
                fallback_req = rid
            req_index.append((rid, _tokens(f"{title} {desc}"), priority))
        if not fallback_req and req_index:
            fallback_req = req_index[0][0]

        def _select_requirement(flow: dict[str, Any]) -> str:
            flow_terms = _tokens(" ".join([flow.get("form", ""), flow.get("handler", ""), " ".join(flow.get("tables", []))]))
            best_id = fallback_req
            best_score = -1
            best_priority = "P3"
            for rid, terms, priority in req_index:
                score = len(flow_terms.intersection(terms))
                if score > best_score or (score == best_score and priority < best_priority):
                    best_id = rid
                    best_score = score
                    best_priority = priority
            return best_id

        features: list[dict[str, Any]] = []
        idx = 1
        for flow in rows:
            if len(features) >= max(1, int(limit or 1)):
                break
            feature_id = f"BDD-LEGACY-{idx:03d}"
            while feature_id in existing_ids:
                idx += 1
                feature_id = f"BDD-LEGACY-{idx:03d}"
            existing_ids.add(feature_id)
            form = str(flow.get("form", "")).strip() or "legacy_form"
            handler = str(flow.get("handler", "")).strip() or "legacy_handler"
            event = str(flow.get("event", "")).strip() or "event"
            control = str(flow.get("control", "")).strip()
            tables = [str(x).strip() for x in flow.get("tables", []) if str(x).strip()]
            sql_touches = [str(x).strip() for x in flow.get("sql_touches", []) if str(x).strip()]
            req_id = _select_requirement(flow)
            title = f"{form} {event} flow parity"
            given_line = (
                f'    Given legacy form "{form}" control "{control}" handles event "{event}"'
                if control
                else f'    Given legacy form "{form}" handles event "{event}"'
            )
            then_data_line = (
                f'    And data side effects preserve table contracts for {", ".join(tables[:3])}'
                if tables
                else "    And data side effects remain equivalent to legacy behavior"
            )
            gherkin = (
                f"Feature: {title}\n"
                f"  Scenario: {handler} preserves legacy business behavior\n"
                f"{given_line}\n"
                f'    And legacy handler "{handler}" is triggered\n'
                f'    When the modernization target executes "{handler}"\n'
                "    Then the user-visible outcome matches legacy behavior\n"
                f"{then_data_line}\n"
                f'    And traceability links include legacy form "{form}" and handler "{handler}"'
            )
            if sql_touches:
                gherkin += (
                    "\n"
                    f'    And SQL touchpoints are equivalent to legacy statement "{sql_touches[0][:120]}"'
                )
            features.append(
                {
                    "id": feature_id,
                    "title": title,
                    "source_requirement_ids": [req_id] if req_id else [],
                    "gherkin": gherkin,
                }
            )
            idx += 1
        return features

    def _normalize_bdd_features(
        self,
        parsed: dict[str, Any],
        functional_requirements: list[dict[str, Any]],
        deterministic: dict[str, Any],
    ) -> list[dict[str, Any]]:
        legacy_inventory = (
            parsed.get("legacy_code_inventory", {})
            if isinstance(parsed.get("legacy_code_inventory", {}), dict)
            else {}
        )
        ui_rows = legacy_inventory.get("ui_event_map", [])
        if not isinstance(ui_rows, list):
            ui_rows = []

        legacy_markers: set[str] = set()
        for row in ui_rows[:400]:
            if not isinstance(row, dict):
                continue
            for token in (
                str(row.get("form", "")).strip(),
                str(row.get("event_handler", "")).strip(),
                str(row.get("control", "")).strip(),
            ):
                if token and len(token) >= 3:
                    legacy_markers.add(token.lower())
            sql_touches = row.get("sql_touches", [])
            if isinstance(sql_touches, list):
                for sql in sql_touches[:4]:
                    for match in re.findall(
                        r"(?i)\b(?:from|join|into|update)\s+([A-Za-z_][A-Za-z0-9_]*)",
                        str(sql or ""),
                    ):
                        name = str(match or "").strip()
                        if name and len(name) >= 3:
                            legacy_markers.add(name.lower())

        def _is_generic_bdd_text(text: str) -> bool:
            lower = str(text or "").lower()
            if not lower.strip():
                return True
            direct_markers = (
                "given requirement",
                "when requirement",
                "then requirement",
                "given the vb6 application",
                "when analyzing",
                "identified and documented",
                "document all sql interactions",
                "extract and document business rules",
                "replacement strategy is documented",
            )
            if any(marker in lower for marker in direct_markers):
                return True
            if "legacy form" in lower and "legacy handler" in lower:
                return False
            if legacy_markers and any(marker in lower for marker in legacy_markers):
                return False
            vague_markers = ("document", "analyze", "identify", "catalog", "inventory")
            return any(marker in lower for marker in vague_markers)

        candidates: list[dict[str, Any]] = []
        top_level_bdd = parsed.get("bdd_contract", {})
        if isinstance(top_level_bdd, dict) and isinstance(top_level_bdd.get("features"), list):
            candidates.extend([x for x in top_level_bdd.get("features", []) if isinstance(x, dict)])
        req_pack = parsed.get("requirements_pack", {})
        if isinstance(req_pack, dict):
            nested_bdd = req_pack.get("bdd_contract", {})
            if isinstance(nested_bdd, dict) and isinstance(nested_bdd.get("features"), list):
                candidates.extend([x for x in nested_bdd.get("features", []) if isinstance(x, dict)])

        normalized: list[dict[str, Any]] = []
        existing_ids: set[str] = set()
        for idx, item in enumerate(candidates, start=1):
            rid_list = item.get("source_requirement_ids", [])
            req_ids = [str(x).strip() for x in rid_list if str(x).strip()] if isinstance(rid_list, list) else []
            feature_id = str(item.get("id", "")).strip() or f"BDD-{idx:03d}"
            if feature_id in existing_ids:
                feature_id = f"{feature_id}-{idx:02d}"
            existing_ids.add(feature_id)
            title = str(item.get("title", "")).strip() or f"Business Behavior {idx}"
            gherkin = str(item.get("gherkin", "")).strip()
            if not gherkin:
                gherkin = (
                    f"Feature: {title}\n"
                    f"  Scenario: {feature_id} primary flow\n"
                    "    Given a valid request context\n"
                    "    When the operation is executed\n"
                    "    Then the expected outcome is returned with traceable evidence"
                )
            normalized.append(
                {
                    "id": feature_id,
                    "title": title,
                    "source_requirement_ids": req_ids,
                    "gherkin": gherkin,
                }
            )

        min_scenarios = int(deterministic.get("evaluation_harness", {}).get("minimum_bdd_scenarios", 5) or 5)
        generic_count = sum(
            1
            for row in normalized
            if _is_generic_bdd_text(" ".join([str(row.get("title", "")), str(row.get("gherkin", ""))]))
        )
        grounded_target = max(min_scenarios, 5)
        grounded = self._build_grounded_bdd_features(
            parsed,
            functional_requirements,
            limit=grounded_target,
            existing_ids=existing_ids,
        )
        if grounded:
            retained = [
                row
                for row in normalized
                if not _is_generic_bdd_text(" ".join([str(row.get("title", "")), str(row.get("gherkin", ""))]))
            ]
            normalized = retained[: max(0, grounded_target // 2)]
            normalized.extend(grounded[:grounded_target])

        seed_requirements = functional_requirements if functional_requirements else []
        idx = 1
        while len(normalized) < min_scenarios and seed_requirements:
            fr = seed_requirements[(idx - 1) % len(seed_requirements)]
            fr_id = str(fr.get("id", f"FR-{idx:03d}"))
            title = str(fr.get("title", f"Requirement {idx}"))
            feature_id = f"{fr_id}-BDD-{idx:02d}"
            if feature_id in existing_ids:
                idx += 1
                continue
            existing_ids.add(feature_id)
            gherkin = (
                f"Feature: {title}\n"
                f"  Scenario: {feature_id} happy path\n"
                f"    Given legacy behavior mapped to requirement {fr_id} is identified\n"
                f"    When a valid modernization request is submitted for {title.lower()}\n"
                "    Then the system returns the expected result and preserves legacy side-effect contracts"
            )
            normalized.append(
                {
                    "id": feature_id,
                    "title": title,
                    "source_requirement_ids": [fr_id],
                    "gherkin": gherkin,
                }
            )
            idx += 1
            if idx > 30:
                break
        return normalized

    def _lint_bdd_features(self, features: list[dict[str, Any]]) -> dict[str, Any]:
        issues: list[dict[str, Any]] = []
        scenario_count = 0
        for item in features:
            feature_id = str(item.get("id", "")).strip() or "BDD"
            gherkin = str(item.get("gherkin", "")).strip()
            lines = [line.strip() for line in gherkin.splitlines() if line.strip()]
            has_feature = any(line.lower().startswith("feature:") for line in lines)
            has_scenario = any(line.lower().startswith("scenario:") for line in lines)
            has_given = any(line.lower().startswith("given ") for line in lines)
            has_when = any(line.lower().startswith("when ") for line in lines)
            has_then = any(line.lower().startswith("then ") for line in lines)
            scenario_count += sum(1 for line in lines if line.lower().startswith("scenario:"))
            if not has_feature:
                issues.append({"feature_id": feature_id, "issue": "Missing `Feature:` header"})
            if not has_scenario:
                issues.append({"feature_id": feature_id, "issue": "Missing `Scenario:` definition"})
            if not (has_given and has_when and has_then):
                issues.append({"feature_id": feature_id, "issue": "Scenario must include Given/When/Then"})
            if re.search(r"\b(and|but)\b", gherkin.lower()) and not has_given:
                issues.append({"feature_id": feature_id, "issue": "Uses connectors without a proper Given step"})
        return {
            "pass": len(issues) == 0,
            "issues": issues,
            "scenario_count": scenario_count,
        }

    def _build_acceptance_test_mapping(
        self,
        functional_requirements: list[dict[str, Any]],
        bdd_features: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        mapping: list[dict[str, Any]] = []
        feature_map: dict[str, list[str]] = {}
        for feature in bdd_features:
            sid = str(feature.get("id", "")).strip()
            for req_id in feature.get("source_requirement_ids", []):
                rid = str(req_id).strip()
                if not rid or not sid:
                    continue
                feature_map.setdefault(rid, []).append(sid)

        for fr in functional_requirements:
            rid = str(fr.get("id", "")).strip()
            title = str(fr.get("title", "")).strip()
            desc = f"{title} {fr.get('description', '')}".lower()
            test_types = ["unit", "integration"]
            if any(token in desc for token in ["api", "endpoint", "contract", "schema"]):
                test_types.append("contract")
            if any(token in desc for token in ["ui", "journey", "workflow", "checkout", "customer"]):
                test_types.append("e2e")
            if any(token in desc for token in ["auth", "security", "access", "pii", "pci"]):
                test_types.append("security")
            mapping.append(
                {
                    "requirement_id": rid,
                    "test_types": sorted(set(test_types)),
                    "bdd_scenarios": feature_map.get(rid, []),
                }
            )
        return mapping

    def _build_quality_gates(
        self,
        deterministic: dict[str, Any],
        functional_requirements: list[dict[str, Any]],
        non_functional_requirements: list[dict[str, Any]],
        bdd_lint: dict[str, Any],
    ) -> list[dict[str, Any]]:
        min_fr = int(deterministic.get("evaluation_harness", {}).get("minimum_functional_requirements", 8) or 8)
        min_nfr = int(deterministic.get("evaluation_harness", {}).get("minimum_non_functional_requirements", 5) or 5)
        min_scenarios = int(deterministic.get("evaluation_harness", {}).get("minimum_bdd_scenarios", 5) or 5)
        primary_caps = deterministic.get("capability_mapping", {}).get("primary_capabilities", [])
        regs = deterministic.get("regulatory_constraints", [])
        classes = {str(x).upper() for x in deterministic.get("data_classification", [])}
        objective_text = str(deterministic.get("normalized_requirement", {}).get("raw_requirement", "")).lower()
        banking_like = any(
            marker in objective_text
            for marker in ("bank", "banking", "payment", "ledger", "transaction", "account")
        )
        compliance_expected = bool(classes.intersection({"PII", "PCI", "PHI"})) or str(
            deterministic.get("domain_pack_ref", {}).get("id", "")
        ).startswith("banking") or banking_like
        gates: list[dict[str, Any]] = []

        gates.append(
            {
                "name": "gherkin_syntax",
                "status": "PASS" if bdd_lint.get("pass") else "FAIL",
                "message": "BDD syntax validation for Feature/Scenario/Given/When/Then.",
                "details": {
                    "scenario_count": int(bdd_lint.get("scenario_count", 0)),
                    "issues": bdd_lint.get("issues", []),
                },
            }
        )

        completeness_pass = (
            len(functional_requirements) >= min_fr
            and len(non_functional_requirements) >= min_nfr
            and int(bdd_lint.get("scenario_count", 0)) >= min_scenarios
            and len(primary_caps) > 0
        )
        gates.append(
            {
                "name": "requirements_completeness",
                "status": "PASS" if completeness_pass else "FAIL",
                "message": "Checks minimum requirement volume, scenario coverage, and capability mapping presence.",
                "details": {
                    "functional_count": len(functional_requirements),
                    "non_functional_count": len(non_functional_requirements),
                    "min_functional": min_fr,
                    "min_non_functional": min_nfr,
                    "scenario_count": int(bdd_lint.get("scenario_count", 0)),
                    "min_scenarios": min_scenarios,
                    "capability_count": len(primary_caps),
                },
            }
        )

        compliance_status = "PASS"
        if compliance_expected and len(regs) == 0:
            compliance_status = "FAIL"
        elif not compliance_expected and len(regs) == 0:
            compliance_status = "WARN"
        gates.append(
            {
                "name": "compliance_constraints_applied",
                "status": compliance_status,
                "message": "Verifies that regulatory/software controls are linked to requirements when applicable.",
                "details": {
                    "jurisdiction": deterministic.get("jurisdiction", "GLOBAL"),
                    "data_classification": sorted(classes),
                    "constraints_count": len(regs),
                },
            }
        )
        return gates

    def _infer_channels(self, text: str) -> list[str]:
        lower = str(text or "").lower()
        channels: list[str] = []
        if any(x in lower for x in ["mobile", "ios", "android", "app"]):
            channels.append("Mobile")
        if any(x in lower for x in ["web", "portal", "browser"]):
            channels.append("Web")
        if any(x in lower for x in ["api", "endpoint", "service"]):
            channels.append("API")
        if any(x in lower for x in ["ops", "operations", "dashboard", "internal"]):
            channels.append("Internal Ops Console")
        return channels or ["Web"]

    def _extract_user_constraints(self, objective: str) -> list[str]:
        constraints: list[str] = []
        parts = re.split(r"[.\n;]+", str(objective or ""))
        markers = ["must", "should", "required", "no ", "cannot", "can't", "without", "do not", "non-negotiable"]
        for part in parts:
            candidate = part.strip()
            if not candidate:
                continue
            lower = candidate.lower()
            if any(marker in lower for marker in markers):
                constraints.append(candidate)
        return constraints[:12]

    def _build_bdd_v2_features(self, bdd_features: list[dict[str, Any]]) -> tuple[list[dict[str, Any]], dict[str, list[str]]]:
        features_v2: list[dict[str, Any]] = []
        feature_to_scenarios: dict[str, list[str]] = {}
        for feature in bdd_features:
            feature_id = str(feature.get("id", "")).strip() or "BDD-FEAT-001"
            feature_title = str(feature.get("title", "")).strip() or feature_id
            source_reqs = [str(x).strip() for x in feature.get("source_requirement_ids", []) if str(x).strip()] if isinstance(feature.get("source_requirement_ids", []), list) else []
            raw = str(feature.get("gherkin", "")).strip()
            lines = [line.strip() for line in raw.splitlines() if line.strip()]

            scenarios: list[dict[str, Any]] = []
            scenario_name = ""
            scenario_steps: list[str] = []
            scenario_idx = 0

            def flush_scenario() -> None:
                nonlocal scenario_idx, scenario_name, scenario_steps
                if not scenario_name and not scenario_steps:
                    return
                scenario_idx += 1
                scenario_id = f"{feature_id}-SC-{scenario_idx:03d}"
                scenarios.append(
                    {
                        "id": scenario_id,
                        "name": scenario_name or f"{feature_title} scenario {scenario_idx}",
                        "gherkin": scenario_steps[:] if scenario_steps else [
                            "Given valid preconditions",
                            "When the operation is executed",
                            "Then the expected result is returned",
                        ],
                        "maps_to": source_reqs[:],
                    }
                )
                feature_to_scenarios.setdefault(feature_id, []).append(scenario_id)
                scenario_name = ""
                scenario_steps = []

            for line in lines:
                lower = line.lower()
                if lower.startswith("feature:"):
                    if not feature_title:
                        feature_title = line.split(":", 1)[1].strip() or feature_title
                    continue
                if lower.startswith("scenario:"):
                    flush_scenario()
                    scenario_name = line.split(":", 1)[1].strip() or f"{feature_title} scenario"
                    continue
                scenario_steps.append(line)
            flush_scenario()

            if not scenarios:
                scenario_id = f"{feature_id}-SC-001"
                scenario_lines = [line for line in lines if re.match(r"^(given|when|then|and|but)\b", line.lower())]
                if not scenario_lines:
                    scenario_lines = [
                        "Given valid preconditions",
                        "When the operation is executed",
                        "Then the expected result is returned",
                    ]
                scenarios = [
                    {
                        "id": scenario_id,
                        "name": f"{feature_title} primary scenario",
                        "gherkin": scenario_lines,
                        "maps_to": source_reqs[:],
                    }
                ]
                feature_to_scenarios.setdefault(feature_id, []).append(scenario_id)

            features_v2.append(
                {
                    "id": feature_id,
                    "title": feature_title,
                    "scenarios": scenarios,
                }
            )
        return features_v2, feature_to_scenarios

    def _build_requirements_pack(
        self,
        parsed: dict[str, Any],
        deterministic: dict[str, Any],
        functional_requirements: list[dict[str, Any]],
        non_functional_requirements: list[dict[str, Any]],
        state: dict[str, Any],
    ) -> dict[str, Any]:
        assumptions = [str(x).strip() for x in parsed.get("assumptions", []) if str(x).strip()] if isinstance(parsed.get("assumptions", []), list) else []
        open_questions = [
            str(x).strip()
            for x in deterministic.get("open_questions", [])
            if str(x).strip()
        ]

        bdd_features = self._normalize_bdd_features(parsed, functional_requirements, deterministic)
        bdd_lint = self._lint_bdd_features(bdd_features)
        acceptance_mapping = self._build_acceptance_test_mapping(functional_requirements, bdd_features)
        quality_gates = self._build_quality_gates(
            deterministic,
            functional_requirements,
            non_functional_requirements,
            bdd_lint,
        )
        for step in deterministic.get("analyst_dag_trace", []):
            if not isinstance(step, dict):
                continue
            step_name = str(step.get("step", ""))
            if step_name == "synthesize_requirements_pack":
                step["status"] = "completed"
                step["details"] = f"requirements_pack_ready=true, bdd_features={len(bdd_features)}"
            if step_name == "run_quality_gates":
                failing = [g for g in quality_gates if str(g.get("status", "")).upper() == "FAIL"]
                step["status"] = "completed"
                step["details"] = f"quality_gates={len(quality_gates)}, failed={len(failing)}"

        features_v2, feature_to_scenarios = self._build_bdd_v2_features(bdd_features)

        risk_items = parsed.get("risks", [])
        risks = [item for item in risk_items if isinstance(item, dict)] if isinstance(risk_items, list) else []
        out_of_scope_raw = parsed.get("out_of_scope", [])
        out_of_scope = [str(x).strip() for x in out_of_scope_raw if str(x).strip()] if isinstance(out_of_scope_raw, list) else []

        nfr_must = [
            item
            for item in non_functional_requirements
            if str(item.get("id", "")).upper().startswith("NFR-AUTO")
            or str(item.get("category", "")).lower() in {"security", "reliability"}
        ]
        nfr_should = [item for item in non_functional_requirements if item not in nfr_must]

        integration_ctx = state.get("integration_context", {}) if isinstance(state.get("integration_context", {}), dict) else {}
        persona_ctx = self._analyst_persona_context(state)
        req_pack_template = deterministic.get("requirements_pack_template", {})
        req_pack_template = req_pack_template if isinstance(req_pack_template, dict) else {}
        project_template = req_pack_template.get("project", {}) if isinstance(req_pack_template.get("project", {}), dict) else {}

        objective = str(deterministic.get("normalized_requirement", {}).get("raw_requirement", "")).strip()
        channels = self._infer_channels(objective)
        actor_tokens = deterministic.get("normalized_requirement", {}).get("actors", [])
        actors = [{"id": f"ACT-{idx:03d}", "name": str(name).title(), "type": "external" if str(name).lower() in {"customer", "merchant", "partner"} else "internal"} for idx, name in enumerate(actor_tokens, start=1)]
        if not actors:
            actors = [{"id": "ACT-001", "name": "Business User", "type": "external"}]

        data_classes = [str(x).upper() for x in deterministic.get("data_classification", []) if str(x).strip()]
        data_elements = []
        seen_fields: set[str] = set()
        for obj in deterministic.get("normalized_requirement", {}).get("objects", []):
            field = f"{str(obj).strip()}Id"
            if not field or field in seen_fields:
                continue
            seen_fields.add(field)
            cls = "PII" if "customer" in field.lower() else ("PCI" if "payment" in field.lower() else (data_classes[0] if data_classes else "INTERNAL"))
            data_elements.append({"name": field, "classification": cls})
        if not data_elements and data_classes:
            data_elements.append({"name": "domainEntityId", "classification": data_classes[0]})

        constraints_from_user = self._extract_user_constraints(objective)
        capability_primary = deterministic.get("capability_mapping", {}).get("primary_capabilities", [])
        capability_alts = deterministic.get("capability_mapping", {}).get("alternative_capabilities", [])
        combined_caps = [x for x in (capability_primary + capability_alts) if isinstance(x, dict)]
        capabilities_v2 = []
        for cap in combined_caps[:10]:
            capabilities_v2.append(
                {
                    "framework": str(deterministic.get("capability_mapping", {}).get("framework", "Capability Taxonomy")),
                    "service_domain": str(cap.get("service_domain", "")),
                    "capability_id": str(cap.get("id", "")),
                    "confidence": float(cap.get("confidence", 0.0)),
                    "rationale": f"Mapped from objective overlap for {str(cap.get('service_domain', cap.get('id', 'capability')))}.",
                }
            )

        domain_invariants = []
        for idx, inv in enumerate(deterministic.get("non_negotiables", []) if isinstance(deterministic.get("non_negotiables", []), list) else [], start=1):
            text = str(inv).strip()
            if text:
                domain_invariants.append({"id": f"INV-{idx:03d}", "statement": text})
        glossary = []
        for entity in deterministic.get("domain_model_excerpt", {}).get("entities", []) if isinstance(deterministic.get("domain_model_excerpt", {}).get("entities", []), list) else []:
            glossary.append({"term": str(entity), "definition": f"{entity} entity in current domain scope.", "synonyms": []})

        controls_triggered = []
        reg_constraints = deterministic.get("regulatory_constraints", [])
        for reg in reg_constraints if isinstance(reg_constraints, list) else []:
            if not isinstance(reg, dict):
                continue
            controls_triggered.append(
                {
                    "id": str(reg.get("id", "")),
                    "name": str(reg.get("name", "")),
                    "jurisdiction": [str(deterministic.get("jurisdiction", "GLOBAL"))],
                    "applies_because": [f"Matched capability tags in domain mapping for {str(reg.get('name', 'control'))}."],
                    "software_constraints": [str(x) for x in reg.get("software_actions", []) if str(x).strip()] if isinstance(reg.get("software_actions", []), list) else [],
                    "evidence_required": [str(x) for x in reg.get("evidence_required", []) if str(x).strip()] if isinstance(reg.get("evidence_required", []), list) else [],
                    "sources": [{"doc": str(reg.get("name", "")), "section": str(reg.get("id", "")), "url_or_ref": f"internal://compliance/{str(reg.get('id', '')).lower()}"}],
                }
            )

        functional_v2 = []
        for fr in functional_requirements:
            fr_id = str(fr.get("id", "")).strip()
            feature_ids = []
            scenario_ids: list[str] = []
            for feat in bdd_features:
                if not isinstance(feat, dict):
                    continue
                source_ids = feat.get("source_requirement_ids", [])
                if isinstance(source_ids, list) and fr_id in [str(x).strip() for x in source_ids]:
                    feat_id = str(feat.get("id", "")).strip()
                    if feat_id:
                        feature_ids.append(feat_id)
                        scenario_ids.extend(feature_to_scenarios.get(feat_id, []))
            functional_v2.append(
                {
                    "id": fr_id,
                    "title": str(fr.get("title", "")),
                    "priority": str(fr.get("priority", "P1")).upper(),
                    "acceptance": {
                        "gherkin_feature_id": feature_ids[0] if feature_ids else "",
                        "scenarios": scenario_ids,
                    },
                }
            )

        non_functional_v2 = []
        for nfr in non_functional_requirements:
            non_functional_v2.append(
                {
                    "id": str(nfr.get("id", "")),
                    "category": str(nfr.get("category", "")),
                    "metric": str(nfr.get("metric", "")),
                    "verification_method": [
                        x for x in ["unit", "integration", "e2e"] if x in [str(y).lower() for y in (next((m.get("test_types", []) for m in acceptance_mapping if isinstance(m, dict)), []))]
                    ] or ["integration"],
                }
            )

        trace_links: list[dict[str, Any]] = []
        for entry in acceptance_mapping:
            if not isinstance(entry, dict):
                continue
            rid = str(entry.get("requirement_id", "")).strip()
            for sid in entry.get("bdd_scenarios", []) if isinstance(entry.get("bdd_scenarios", []), list) else []:
                sid_text = str(sid).strip()
                if rid and sid_text:
                    trace_links.append({"from": rid, "to": sid_text, "type": "verified_by"})
        security_nfr = next(
            (str(item.get("id", "")) for item in non_functional_requirements if str(item.get("category", "")).lower() == "security"),
            "",
        )
        if security_nfr:
            for control in controls_triggered:
                cid = str(control.get("id", "")).strip()
                if cid:
                    trace_links.append({"from": cid, "to": security_nfr, "type": "enforced_by"})

        run_id = str(state.get("run_id", "")).strip()
        thread_id = f"THREAD-{run_id}" if run_id else f"THREAD-{datetime.now(timezone.utc).strftime('%Y%m%d%H%M%S')}"
        conversation_changes = state.get("analyst_conversation_changes", [])
        if not isinstance(conversation_changes, list):
            conversation_changes = []

        domain_pack_ref = deterministic.get("domain_pack_ref", {})
        domain_pack_ref = domain_pack_ref if isinstance(domain_pack_ref, dict) else {}
        standards_from_ref = [str(x.get("id", "")) for x in deterministic.get("standards_guidance", []) if isinstance(x, dict) and str(x.get("id", "")).strip()]
        domain_pack_project = project_template.get("domain_pack", {}) if isinstance(project_template.get("domain_pack", {}), dict) else {}
        regulatory_profile = []
        for control in controls_triggered:
            name = str(control.get("name", "")).strip()
            if name:
                regulatory_profile.append(name)
        if isinstance(project_template.get("regulatory_profile", []), list):
            regulatory_profile = [str(x).strip() for x in project_template.get("regulatory_profile", []) if str(x).strip()] or regulatory_profile
        if not regulatory_profile:
            regulatory_profile = [str(x.get("id", "")).upper() for x in controls_triggered if str(x.get("id", "")).strip()]

        generated_at = datetime.now(timezone.utc).isoformat()
        project_name = str(parsed.get("project_name", "")).strip() or "Untitled Project"
        objective_summary = str(parsed.get("executive_summary", "")).strip() or str(parsed.get("analysis_walkthrough", {}).get("business_objective_summary", "")).strip()
        legacy_compact = (
            state.get("legacy_compact_context", {})
            if isinstance(state.get("legacy_compact_context", {}), dict)
            else {}
        )
        legacy_skill_profile = (
            legacy_compact.get("legacy_skill_profile", {})
            if isinstance(legacy_compact.get("legacy_skill_profile", {}), dict)
            else {}
        )
        if not legacy_skill_profile:
            legacy_skill_profile = self._skill_from_discover_cache(state)
        legacy_inventory = (
            legacy_compact.get("inventory", {})
            if isinstance(legacy_compact.get("inventory", {}), dict)
            else {}
        )
        if not legacy_inventory:
            legacy_inventory = self._inventory_from_discover_cache(state)

        pack_v2 = {
            "artifact_type": "requirements_pack",
            "artifact_version": "2.0",
            "artifact_id": f"REQPACK-{(run_id or 'LOCAL').upper()}",
            "generated_at": generated_at,
            "generated_by": {
                "agent_name": "Analyst Agent",
                "persona": str(persona_ctx.get("display_name", "Senior Analyst")),
                "persona_version": "1.0",
                "requirements_pack_profile": deterministic.get("requirements_pack_profile", "requirements-pack-v2-general"),
                "domain_pack": {
                    "id": str(domain_pack_project.get("id", domain_pack_ref.get("id", "software-general-v1"))),
                    "version": str(domain_pack_project.get("version", domain_pack_ref.get("version", "1.0.0"))),
                    "ontologies": [str(deterministic.get("capability_mapping", {}).get("framework", "Capability Taxonomy"))],
                    "standards": standards_from_ref,
                    "internal_playbooks": [
                        str(x)
                        for x in (project_template.get("domain_pack", {}).get("internal_playbooks", []) if isinstance(project_template.get("domain_pack", {}).get("internal_playbooks", []), list) else [])
                        if str(x).strip()
                    ],
                },
            },
            "project": {
                "name": project_name,
                "work_item_id": str(project_template.get("work_item_id", run_id)),
                "jurisdiction": [str(deterministic.get("jurisdiction", "GLOBAL"))],
                "regulatory_profile": regulatory_profile,
                "domain_pack": {
                    "id": str(domain_pack_project.get("id", domain_pack_ref.get("id", "software-general-v1"))),
                    "version": str(domain_pack_project.get("version", domain_pack_ref.get("version", "1.0.0"))),
                    "ontologies": [str(deterministic.get("capability_mapping", {}).get("framework", "Capability Taxonomy"))],
                    "standards": standards_from_ref,
                    "internal_playbooks": [
                        str(x)
                        for x in (domain_pack_project.get("internal_playbooks", []) if isinstance(domain_pack_project.get("internal_playbooks", []), list) else [])
                        if str(x).strip()
                    ],
                },
            },
            "intake": {
                "business_request_raw": objective,
                "actors": [item.get("name", "") for item in actors],
                "channels": channels,
                "data_elements": data_elements,
                "constraints_from_user": constraints_from_user,
                "assumptions": assumptions,
            },
            "domain_mapping": {
                "capabilities": capabilities_v2,
                "domain_invariants": domain_invariants,
                "glossary": glossary,
            },
            "compliance": {
                "controls_triggered": controls_triggered,
                "privacy": {
                    "pii_masking_rules": [
                        "PAN => last4",
                        "account identifiers => masked",
                        "customer identifiers => tokenized in logs",
                    ] if "PII" in data_classes or "PCI" in data_classes else [],
                    "retention_days": 365,
                    "access_model": "least_privilege",
                },
            },
            "requirements": {
                "functional": functional_v2,
                "non_functional": non_functional_v2,
            },
            "bdd": {
                "features": features_v2,
                "lint": {
                    "gherkin_valid": bool(bdd_lint.get("pass", False)),
                    "missing_steps": [str(x.get("issue", "")) for x in bdd_lint.get("issues", []) if isinstance(x, dict)],
                },
            },
            "open_questions": [
                {
                    "id": f"Q-{idx:03d}",
                    "question": question,
                    "owner": "Client",
                    "severity": "high",
                }
                for idx, question in enumerate(open_questions, start=1)
            ],
            "risks": risks,
            "out_of_scope": out_of_scope,
            "traceability": {
                "links": trace_links,
            },
            "conversation_audit": {
                "thread_id": thread_id,
                "changes": conversation_changes,
            },
            "context_reference": {
                "version_id": str(state.get("context_vault_ref", {}).get("version_id", "")) if isinstance(state.get("context_vault_ref", {}), dict) else "",
                "repo": str(state.get("context_vault_ref", {}).get("repo", "")) if isinstance(state.get("context_vault_ref", {}), dict) else "",
                "branch": str(state.get("context_vault_ref", {}).get("branch", "")) if isinstance(state.get("context_vault_ref", {}), dict) else "",
                "commit_sha": str(state.get("context_vault_ref", {}).get("commit_sha", "")) if isinstance(state.get("context_vault_ref", {}), dict) else "",
                "scm_version": str(state.get("system_context_model", {}).get("version", "")) if isinstance(state.get("system_context_model", {}), dict) else "",
                "cp_version": str(state.get("convention_profile", {}).get("version", "")) if isinstance(state.get("convention_profile", {}), dict) else "",
                "ha_version": str(state.get("health_assessment", {}).get("version", "")) if isinstance(state.get("health_assessment", {}), dict) else "",
            },
            # Compatibility fields used by existing UI and downstream summary cards.
            "schema_version": "2.0.0",
            "domain_pack_ref": domain_pack_ref,
            "analyst_dag_trace": deterministic.get("analyst_dag_trace", []),
            "input_contract": {
                "business_objective": objective,
                "use_case": deterministic.get("normalized_requirement", {}).get("use_case", "business_objectives"),
                "jurisdiction": deterministic.get("jurisdiction", "GLOBAL"),
                "data_classification": deterministic.get("data_classification", []),
            },
            "capability_mapping": deterministic.get("capability_mapping", {}),
            "domain_model_excerpt": deterministic.get("domain_model_excerpt", {}),
            "regulatory_constraints_applied": deterministic.get("regulatory_constraints", []),
            "standards_guidance": deterministic.get("standards_guidance", []),
            "gold_patterns": deterministic.get("gold_patterns", []),
            "non_negotiables": deterministic.get("non_negotiables", []),
            "bdd_contract": {
                "features": bdd_features,
                "lint": bdd_lint,
            },
            "non_functional_contract": {
                "must": nfr_must,
                "should": nfr_should,
            },
            "acceptance_test_mapping": acceptance_mapping,
            "quality_gates": quality_gates,
            "executive_summary": objective_summary,
        }
        business_rules_catalog = (
            legacy_inventory.get("business_rules_catalog", [])
            if isinstance(legacy_inventory.get("business_rules_catalog", []), list)
            else []
        )

        if legacy_inventory:
            pack_v2["legacy_code_inventory"] = legacy_inventory
            if isinstance(legacy_inventory.get("vb6_analysis", {}), dict):
                pack_v2["vb6_analysis"] = legacy_inventory.get("vb6_analysis", {})
            if business_rules_catalog:
                pack_v2["business_rules_catalog"] = business_rules_catalog[:200]
            if isinstance(legacy_inventory.get("modernization_readiness", {}), dict):
                pack_v2["modernization_readiness"] = legacy_inventory.get("modernization_readiness", {})
            if isinstance(legacy_inventory.get("migration_strategy_recommendation", {}), dict):
                pack_v2["migration_strategy_recommendation"] = legacy_inventory.get("migration_strategy_recommendation", {})
            if isinstance(legacy_inventory.get("source_target_modernization_profile", {}), dict):
                pack_v2["source_target_modernization_profile"] = legacy_inventory.get("source_target_modernization_profile", {})
            if isinstance(legacy_inventory.get("project_business_summaries", []), list):
                pack_v2["project_business_summaries"] = legacy_inventory.get("project_business_summaries", [])[:32]
        if legacy_skill_profile:
            pack_v2["legacy_skill_profile"] = legacy_skill_profile

        if isinstance(req_pack_template, dict) and req_pack_template:
            # Keep template as reference while preserving deterministic computed fields.
            pack_v2["template_reference"] = req_pack_template

        return pack_v2

    def _finalize_output(self, parsed: dict[str, Any], deterministic: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
        out = parsed if isinstance(parsed, dict) else {}
        out = dict(out)

        # Preserve and normalize legacy output contract used by downstream stages.
        functional = self._normalize_functional_requirements(out.get("functional_requirements", []))
        non_functional = self._normalize_non_functional_requirements(out.get("non_functional_requirements", []))
        functional = self._augment_functional_requirements(functional, deterministic)
        non_functional = self._augment_non_functional_requirements(non_functional, deterministic)
        out["functional_requirements"] = functional
        out["non_functional_requirements"] = non_functional

        walkthrough = out.get("analysis_walkthrough", {})
        if not isinstance(walkthrough, dict):
            walkthrough = {}
        req_understanding = walkthrough.get("requirements_understanding", [])
        if not isinstance(req_understanding, list) or not req_understanding:
            caps = deterministic.get("capability_mapping", {}).get("primary_capabilities", [])
            req_understanding = [
                f"Primary capability: {str(cap.get('service_domain', cap.get('id', 'Unknown')))} (confidence {cap.get('confidence', 0)})"
                for cap in caps[:4]
                if isinstance(cap, dict)
            ]
            if not req_understanding:
                req_understanding = ["Objective mapped to a generalized software capability profile."]
        conversion = walkthrough.get("conversion_to_technical_requirements", [])
        if not isinstance(conversion, list) or not conversion:
            conversion = [
                "Translate requirements into enforceable API/data contracts and explicit acceptance tests.",
                "Apply non-negotiable controls (auditability, idempotency, PII safeguards) as implementation constraints.",
                "Preserve traceability from business requirement to BDD scenarios and test suites.",
            ]
        clarifications = walkthrough.get("clarifications_requested", [])
        if not isinstance(clarifications, list) or not clarifications:
            clarifications = deterministic.get("open_questions", [])
        walkthrough["requirements_understanding"] = [str(x) for x in req_understanding]
        walkthrough["conversion_to_technical_requirements"] = [str(x) for x in conversion]
        walkthrough["clarifications_requested"] = [str(x) for x in clarifications]
        if not str(walkthrough.get("business_objective_summary", "")).strip():
            walkthrough["business_objective_summary"] = str(deterministic.get("normalized_requirement", {}).get("raw_requirement", ""))[:600]
        out["analysis_walkthrough"] = walkthrough

        assumptions = out.get("assumptions", [])
        if not isinstance(assumptions, list):
            assumptions = []
        if not assumptions:
            assumptions = [
                "Existing integration contracts remain valid unless explicitly revised.",
                "Operational environments support observability instrumentation for release verification.",
            ]
        out["assumptions"] = [str(x) for x in assumptions if str(x).strip()]

        compact_inventory = {}
        legacy_compact = (
            state.get("legacy_compact_context", {})
            if isinstance(state.get("legacy_compact_context", {}), dict)
            else {}
        )
        if isinstance(legacy_compact.get("inventory", {}), dict):
            compact_inventory = legacy_compact.get("inventory", {})
        if not compact_inventory:
            compact_inventory = self._inventory_from_discover_cache(state)
        compact_skill = (
            legacy_compact.get("legacy_skill_profile", {})
            if isinstance(legacy_compact.get("legacy_skill_profile", {}), dict)
            else {}
        )
        if not compact_skill:
            compact_skill = self._skill_from_discover_cache(state)
        existing_inventory = out.get("legacy_code_inventory", {})
        if not isinstance(existing_inventory, dict):
            existing_inventory = {}
        if compact_inventory and existing_inventory:
            merged_inventory = {**existing_inventory, **compact_inventory}
        elif compact_inventory:
            merged_inventory = compact_inventory
        else:
            merged_inventory = existing_inventory
        if merged_inventory:
            out["legacy_code_inventory"] = merged_inventory
            if not isinstance(out.get("vb6_analysis", {}), dict) and isinstance(merged_inventory.get("vb6_analysis", {}), dict):
                out["vb6_analysis"] = merged_inventory.get("vb6_analysis", {})
            elif isinstance(out.get("vb6_analysis", {}), dict):
                vb6_existing = out.get("vb6_analysis", {})
                if isinstance(merged_inventory.get("vb6_analysis", {}), dict):
                    vb6_existing = {**vb6_existing, **merged_inventory.get("vb6_analysis", {})}
                out["vb6_analysis"] = vb6_existing
            functional, non_functional, proposed_additions = self._ground_requirements_for_legacy_parity(
                functional=functional,
                non_functional=non_functional,
                legacy_inventory=merged_inventory,
            )
            out["functional_requirements"] = functional
            out["non_functional_requirements"] = non_functional
            if proposed_additions.get("functional") or proposed_additions.get("non_functional"):
                out["proposed_additions"] = proposed_additions
        if compact_skill:
            out["legacy_skill_profile"] = compact_skill

        requirements_pack = self._build_requirements_pack(
            out,
            deterministic,
            functional_requirements=functional,
            non_functional_requirements=non_functional,
            state=state,
        )
        out["requirements_pack"] = requirements_pack
        out["domain_pack"] = requirements_pack.get("domain_pack_ref", {})
        out["capability_mapping"] = requirements_pack.get("capability_mapping", {})
        out["domain_model_excerpt"] = requirements_pack.get("domain_model_excerpt", {})
        out["regulatory_constraints"] = requirements_pack.get("regulatory_constraints_applied", [])
        out["bdd_contract"] = requirements_pack.get("bdd_contract", {})
        out["quality_gates"] = requirements_pack.get("quality_gates", [])
        out["acceptance_test_mapping"] = requirements_pack.get("acceptance_test_mapping", [])
        out["open_questions"] = requirements_pack.get("open_questions", [])
        out["standards_guidance"] = requirements_pack.get("standards_guidance", [])
        out["gold_patterns"] = requirements_pack.get("gold_patterns", [])
        out["conversation_audit"] = requirements_pack.get("conversation_audit", {})
        if isinstance(requirements_pack.get("modernization_readiness", {}), dict):
            out["modernization_readiness"] = requirements_pack.get("modernization_readiness", {})
        if isinstance(requirements_pack.get("migration_strategy_recommendation", {}), dict):
            out["migration_strategy_recommendation"] = requirements_pack.get("migration_strategy_recommendation", {})
        if isinstance(requirements_pack.get("source_target_modernization_profile", {}), dict):
            out["source_target_modernization_profile"] = requirements_pack.get("source_target_modernization_profile", {})
        if isinstance(requirements_pack.get("project_business_summaries", []), list):
            out["project_business_summaries"] = requirements_pack.get("project_business_summaries", [])[:32]
        if isinstance(requirements_pack.get("legacy_code_inventory", {}), dict):
            out["legacy_code_inventory"] = requirements_pack.get("legacy_code_inventory", {})
        if isinstance(requirements_pack.get("vb6_analysis", {}), dict):
            out["vb6_analysis"] = requirements_pack.get("vb6_analysis", {})
        if isinstance(requirements_pack.get("legacy_skill_profile", {}), dict):
            out["legacy_skill_profile"] = requirements_pack.get("legacy_skill_profile", {})
        if isinstance(out.get("legacy_code_inventory", {}), dict):
            rules = out.get("legacy_code_inventory", {}).get("business_rules_catalog", [])
            if isinstance(rules, list):
                out["business_rules_catalog"] = rules[:200]
        elif isinstance(requirements_pack.get("business_rules_catalog", []), list):
            out["business_rules_catalog"] = requirements_pack.get("business_rules_catalog", [])[:200]
        db_schema_input = str(state.get("database_schema", "")).strip()
        if db_schema_input:
            # Preserve schema input for deterministic DB archaeology artifacts.
            out["database_schema_input"] = db_schema_input[:900000]
            out["database_source"] = str(state.get("database_source", "")).strip()
            out["database_target"] = str(state.get("database_target", "")).strip()
        try:
            out["raw_artifacts"] = build_raw_artifact_set_v1(out)
        except Exception as exc:
            self.log(f"[{self.name}] WARN: failed to build raw_artifacts: {exc}")
        try:
            out["analyst_report_v2"] = build_analyst_report_v2(out)
            qa_report = out.get("analyst_report_v2", {}).get("qa_report_v1", {})
            if isinstance(qa_report, dict) and qa_report:
                out["qa_report_v1"] = qa_report
        except Exception as exc:
            self.log(f"[{self.name}] WARN: failed to build analyst_report_v2: {exc}")
        return out

    def run(self, state: dict[str, Any]) -> AgentResult:
        self._logs = []
        self.log(f"[{self.name}] Starting execution...")
        deterministic = self._build_deterministic_context(state)
        self.log(
            f"[{self.name}] Domain Pack selected: {deterministic.get('domain_pack_ref', {}).get('id', 'software-general-v1')}"
        )
        self.log(
            f"[{self.name}] Deterministic mapping: capabilities={len(deterministic.get('capability_mapping', {}).get('primary_capabilities', []))}, "
            f"constraints={len(deterministic.get('regulatory_constraints', []))}"
        )

        state_for_prompt = dict(state)
        legacy_code = str(state.get("legacy_code", "")).strip()
        if legacy_code:
            compact = self._build_legacy_compact_context(
                legacy_code,
                str(state.get("modernization_language", "")).strip(),
                state=state,
            )
            if compact:
                state_for_prompt["legacy_compact_context"] = compact
                if not bool(compact.get("inline", False)):
                    self.log(
                        f"[{self.name}] Legacy code chunked for analysis: chunks={int(compact.get('chunk_count', 0) or 0)}, "
                        "final prompt uses compact context."
                    )
        db_schema = str(state.get("database_schema", "")).strip()
        if db_schema:
            db_compact = self._build_database_compact_context(
                db_schema,
                str(state.get("database_source", "")).strip(),
                str(state.get("database_target", "")).strip(),
            )
            if db_compact:
                state_for_prompt["database_compact_context"] = db_compact

        user_msg = self._build_user_message_with_context(state_for_prompt, deterministic)
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
                parsed = self._deterministic_parsed_fallback(state_for_prompt, deterministic, raw_response, parse_exc)
            self.log(f"[{self.name}] LLM output parsed; compiling deterministic Requirements Pack...")
            finalized = self._finalize_output(parsed, deterministic, state_for_prompt)
            if (
                (not isinstance(finalized.get("legacy_functional_contract", []), list))
                or not finalized.get("legacy_functional_contract", [])
            ):
                fallback_contract = (
                    state_for_prompt.get("legacy_compact_context", {}).get("seed_legacy_contract", [])
                    if isinstance(state_for_prompt.get("legacy_compact_context", {}), dict)
                    else []
                )
                if isinstance(fallback_contract, list) and fallback_contract:
                    finalized["legacy_functional_contract"] = fallback_contract[:20]
                    self.log(f"[{self.name}] Applied chunk-derived legacy functional contract fallback ({len(finalized['legacy_functional_contract'])} items).")
            self.log(f"[{self.name}] Requirements Pack compiled with quality gates")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="success",
                summary=self._build_summary(finalized),
                output=finalized,
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

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        fr_count = len(parsed.get("functional_requirements", []))
        nfr_count = len(parsed.get("non_functional_requirements", []))
        risk_count = len(parsed.get("risks", []))
        clarifications = len(parsed.get("analysis_walkthrough", {}).get("clarifications_requested", []))
        gate_items = parsed.get("quality_gates", [])
        failed_gates = len([g for g in gate_items if str(g.get("status", "")).upper() == "FAIL"]) if isinstance(gate_items, list) else 0
        domain_pack = str(parsed.get("domain_pack", {}).get("id", "")).strip() if isinstance(parsed.get("domain_pack", {}), dict) else ""
        legacy_inventory = parsed.get("legacy_code_inventory", {}) if isinstance(parsed.get("legacy_code_inventory", {}), dict) else {}
        projects_count = len(legacy_inventory.get("vb6_projects", [])) if isinstance(legacy_inventory.get("vb6_projects", []), list) else 0
        forms_count = len(legacy_inventory.get("forms", [])) if isinstance(legacy_inventory.get("forms", []), list) else 0
        activex_count = len(legacy_inventory.get("activex_controls", [])) if isinstance(legacy_inventory.get("activex_controls", []), list) else 0
        legacy_skill = parsed.get("legacy_skill_profile", {}) if isinstance(parsed.get("legacy_skill_profile", {}), dict) else {}
        legacy_skill_id = str(legacy_skill.get("selected_skill_id", "")).strip()
        return (
            f"Extracted {fr_count} functional requirements, "
            f"{nfr_count} non-functional requirements, "
            f"{risk_count} risks, {clarifications} clarifications, "
            f"{failed_gates} failed quality gates"
            + (
                f", VB6 projects={projects_count}, legacy forms={forms_count}, ActiveX/COM={activex_count}"
                if projects_count or forms_count or activex_count
                else ""
            )
            + (f", skill={legacy_skill_id}" if legacy_skill_id else "")
            + (f" [{domain_pack}]" if domain_pack else "")
        )
