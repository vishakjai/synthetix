"""
Agent 5: Security Engineer Agent
Performs focused threat modeling and security hardening guidance.
"""

from __future__ import annotations

import json
from typing import Any

from .base import BaseAgent


class SecurityEngineerAgent(BaseAgent):

    @property
    def name(self) -> str:
        return "Security Engineer Agent"

    @property
    def stage(self) -> int:
        return 5

    @property
    def system_prompt(self) -> str:
        return """You are a Principal Security Engineer Agent in a software pipeline.
You review architecture and implementation outputs and produce actionable security guidance.

You MUST respond with valid JSON in this exact structure:
{
  "security_summary": "string",
  "threat_model": [
    {
      "asset": "string",
      "threat": "string",
      "severity": "critical|high|medium|low",
      "mitigation": "string"
    }
  ],
  "required_controls": [
    {
      "control": "string",
      "reason": "string",
      "priority": "P0|P1|P2"
    }
  ],
  "security_test_focus": ["string", ...],
  "code_hotspots": ["string", ...],
  "release_recommendation": {
    "status": "approve|conditional|block",
    "blocking_issues": ["string", ...]
  }
}

Focus on practical fixes that can be executed by downstream QA/development.
Respond ONLY with JSON, no additional text."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        analyst = state.get("analyst_output", {})
        architect = state.get("architect_output", {})
        developer = state.get("developer_output", {})
        db_output = state.get("database_engineer_output", {})
        strict_mode = bool(state.get("strict_security_mode", False))

        return f"""Perform a security evaluation.

STRICT SECURITY MODE:
{strict_mode}

ANALYST OUTPUT:
{self._json_for_prompt(analyst, max_chars=2600, max_depth=3, max_items=10, max_str=240)}

ARCHITECT OUTPUT:
{self._json_for_prompt(architect, max_chars=2600, max_depth=3, max_items=10, max_str=240)}

DEVELOPER OUTPUT SUMMARY:
{self._json_for_prompt({
    "total_components": developer.get("total_components", 0) if isinstance(developer, dict) else 0,
    "total_files": developer.get("total_files", 0) if isinstance(developer, dict) else 0,
    "components": [
        {"name": c.get("component_name"), "language": c.get("language")}
        for c in (developer.get("implementations", []) if isinstance(developer, dict) else [])
    ],
}, max_chars=1800, max_depth=3, max_items=12, max_str=220)}

DATABASE ENGINEERING OUTPUT:
{self._json_for_prompt(db_output, max_chars=2200, max_depth=3, max_items=10, max_str=240)}
"""

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        threats = len(parsed.get("threat_model", []))
        controls = len(parsed.get("required_controls", []))
        status = parsed.get("release_recommendation", {}).get("status", "conditional")
        return f"Security review: {threats} threats, {controls} controls, release={status.upper()}"
