"""
Agent 1: Analyst Agent
Parses business objectives into structured requirements with acceptance criteria.

Supports an optional interactive mode where the agent first generates clarifying
questions, presents them to the user, and incorporates the answers into a more
precise requirements document.
"""

from __future__ import annotations

import json
from typing import Any

from .base import BaseAgent, AgentResult
from utils.llm import LLMClient


class AnalystAgent(BaseAgent):

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
  "assumptions": ["string", ...],
  "risks": [
    {
      "description": "string",
      "impact": "high|medium|low",
      "mitigation": "string"
    }
  ],
  "out_of_scope": ["string", ...]
}

Be thorough — generate at least 5 functional requirements and 3 non-functional requirements.
The `analysis_walkthrough` section must be concise, readable, and non-JSON-jargony.
Each requirement MUST have at least 2 acceptance criteria.
Respond ONLY with the JSON, no other text."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        objectives = state.get("business_objectives", "")
        answers = state.get("analyst_answers")
        legacy_code = state.get("legacy_code", "")
        target_lang = state.get("modernization_language", "")
        db_source = str(state.get("database_source", "")).strip()
        db_target = str(state.get("database_target", "")).strip()
        db_schema = str(state.get("database_schema", "")).strip()
        use_case = str(state.get("use_case", "business_objectives")).strip().lower()

        modernization_context = ""
        if legacy_code:
            modernization_context = f"""

LEGACY SOURCE CODE TO MODERNIZE:
```asp
{legacy_code}
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
            database_context = f"""

DATABASE CONVERSION CONTEXT:
- Source engine: {db_source or "Not specified"}
- Target engine: {db_target or "Not specified"}

LEGACY SCHEMA / SQL INPUT:
```sql
{db_schema}
```

IMPORTANT:
- Capture database migration requirements and acceptance criteria.
- Include data validation and rollback expectations.
"""

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
            return f"""Analyze the following business objectives and produce a comprehensive
requirements document with acceptance criteria.

USE CASE:
{use_case}

BUSINESS OBJECTIVES:
{objectives}
{modernization_context}
{database_context}"""

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

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

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        fr_count = len(parsed.get("functional_requirements", []))
        nfr_count = len(parsed.get("non_functional_requirements", []))
        risk_count = len(parsed.get("risks", []))
        clarifications = len(parsed.get("analysis_walkthrough", {}).get("clarifications_requested", []))
        return (
            f"Extracted {fr_count} functional requirements, "
            f"{nfr_count} non-functional requirements, "
            f"{risk_count} risks, and {clarifications} clarifications"
        )
