"""
Agent 5: Validator Agent (Analyst re-engagement)
Re-engages the Analyst to verify functional requirements against acceptance criteria.
"""

from __future__ import annotations

import json
from typing import Any

from .base import BaseAgent


class ValidatorAgent(BaseAgent):

    @property
    def name(self) -> str:
        return "Analyst (Validation)"

    @property
    def stage(self) -> int:
        return 7

    @property
    def system_prompt(self) -> str:
        return """You are the Business Analyst Agent being re-engaged for VALIDATION.
You previously defined requirements and acceptance criteria. Now you must verify
whether the implemented and tested system meets those criteria.

You MUST respond with valid JSON in this exact structure:
{
  "validation_summary": "overall assessment string",
  "functional_validation": [
    {
      "requirement_id": "FR-001",
      "title": "string",
      "acceptance_criteria_results": [
        {
          "criterion": "string (the original acceptance criterion)",
          "verdict": "met|partially_met|not_met",
          "evidence": "string explaining how this was verified",
          "notes": "string or null"
        }
      ],
      "overall_status": "validated|partial|failed"
    }
  ],
  "non_functional_validation": [
    {
      "requirement_id": "NFR-001",
      "title": "string",
      "metric_target": "string",
      "actual_result": "string",
      "verdict": "met|partially_met|not_met",
      "evidence": "string"
    }
  ],
  "gap_analysis": [
    {
      "gap": "string describing what's missing",
      "severity": "critical|major|minor",
      "recommendation": "string"
    }
  ],
  "overall_verdict": {
    "status": "approved|conditionally_approved|rejected",
    "functional_coverage_percent": number,
    "nfr_compliance_percent": number,
    "blocking_gaps": ["string if any"],
    "sign_off_recommendation": "string"
  }
}

Be thorough and realistic. Not everything needs to pass perfectly.
Respond ONLY with the JSON, no other text."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        requirements = state.get("analyst_output", {})
        test_results = state.get("tester_output", {})
        developer_output = state.get("developer_output", {})
        db_output = state.get("database_engineer_output", {})
        security_output = state.get("security_engineer_output", {})
        legacy_contract = requirements.get("legacy_functional_contract", []) if isinstance(requirements, dict) else []

        return f"""Validate the implementation against the original requirements and acceptance criteria.

ORIGINAL REQUIREMENTS:
{self._json_for_prompt(requirements, max_chars=5000, max_depth=4, max_items=12, max_str=300)}

TEST RESULTS:
{self._json_for_prompt(test_results.get("overall_results", {}), max_chars=1800, max_depth=3, max_items=10, max_str=240)}

Test Suites Summary:
- Unit Tests: {test_results.get("test_suites", {}).get("unit_tests", {}).get("total_tests", 0)} tests, {test_results.get("test_suites", {}).get("unit_tests", {}).get("coverage_percent", 0)}% coverage
- Integration Tests: {test_results.get("test_suites", {}).get("integration_tests", {}).get("total_tests", 0)} tests
- Load Tests: {len(test_results.get("test_suites", {}).get("load_tests", {}).get("scenarios", []))} scenarios
- Security Tests: {len(test_results.get("test_suites", {}).get("security_tests", {}).get("checks", []))} checks
- E2E Tests: {test_results.get("test_suites", {}).get("e2e_tests", {}).get("total_tests", 0)} tests

IMPLEMENTATION STATS:
- Total LOC: {developer_output.get("total_loc", 0)}
- Total Files: {developer_output.get("total_files", 0)}
- Components: {developer_output.get("total_components", 0)}

DATABASE ENGINEER OUTPUT:
{self._json_for_prompt(db_output, max_chars=1800, max_depth=3, max_items=10, max_str=220)}

SECURITY ENGINEER OUTPUT:
{self._json_for_prompt(security_output, max_chars=1800, max_depth=3, max_items=10, max_str=220)}

LEGACY FUNCTIONAL CONTRACT (if available):
{self._json_for_prompt(legacy_contract, max_chars=1600, max_depth=3, max_items=8, max_str=220)}

Verify each requirement's acceptance criteria against the implementation and test evidence."""

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        verdict = parsed.get("overall_verdict", {})
        status = verdict.get("status", "unknown")
        fc = verdict.get("functional_coverage_percent", 0)
        nfr = verdict.get("nfr_compliance_percent", 0)
        gaps = len(parsed.get("gap_analysis", []))
        return (
            f"Verdict: {status.upper()} | "
            f"Functional: {fc}% | NFR: {nfr}% | "
            f"{gaps} gaps identified"
        )
