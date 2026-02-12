"""
Agent 4: Database Engineer Agent
Designs and generates migration artifacts for database modernization tasks.
"""

from __future__ import annotations

import json
from typing import Any

from .base import BaseAgent


class DatabaseEngineerAgent(BaseAgent):

    @property
    def name(self) -> str:
        return "Database Engineer Agent"

    @property
    def stage(self) -> int:
        return 4

    @property
    def system_prompt(self) -> str:
        return """You are a Senior Database Engineer Agent in a software modernization pipeline.
Your job is to produce concrete migration artifacts and schema recommendations.

You MUST respond with valid JSON in this exact structure:
{
  "migration_summary": "string",
  "source_engine": "string",
  "target_engine": "string",
  "schema_assessment": {
    "tables": number,
    "indexes": number,
    "constraints": number,
    "risk_notes": ["string", ...]
  },
  "migration_plan": [
    {
      "step": "string",
      "description": "string",
      "risk": "low|medium|high"
    }
  ],
  "generated_scripts": [
    {
      "name": "string",
      "type": "ddl|dml|validation|rollback",
      "sql": "string"
    }
  ],
  "data_validation_checks": ["string", ...],
  "rollback_strategy": "string"
}

Prefer executable SQL scripts and pragmatic migration sequencing.
Respond ONLY with JSON, no additional text."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        analyst = state.get("analyst_output", {})
        architect = state.get("architect_output", {})
        source_engine = str(state.get("database_source", "")).strip() or "legacy_db"
        target_engine = str(state.get("database_target", "")).strip() or "target_db"
        schema_input = str(state.get("database_schema", "")).strip()
        use_case = str(state.get("use_case", "business_objectives")).strip().lower()

        return f"""Generate database migration artifacts for this task.

USE CASE:
{use_case}

SOURCE ENGINE:
{source_engine}

TARGET ENGINE:
{target_engine}

LEGACY SCHEMA / SQL INPUT:
```sql
{schema_input}
```

ANALYST REQUIREMENTS:
{json.dumps(analyst, indent=2)}

ARCHITECTURE CONTEXT:
{json.dumps(architect, indent=2)}

If use_case is not database_conversion, still provide database hardening/migration guidance for persistence layers."""

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        scripts = len(parsed.get("generated_scripts", []))
        steps = len(parsed.get("migration_plan", []))
        source = parsed.get("source_engine", "source")
        target = parsed.get("target_engine", "target")
        return f"Prepared DB migration plan ({steps} steps, {scripts} scripts) {source} -> {target}"
