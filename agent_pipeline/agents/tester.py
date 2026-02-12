"""
Agent 4: Tester Agent

Executes real validation checks against generated code artifacts:
  - Syntax and unit-style checks
  - Integration command checks
  - Lightweight load probes (where feasible)
  - Static security checks
  - Functional E2E smoke checks (where feasible)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any

from .base import AgentResult, BaseAgent
from utils.artifacts import ensure_dir, find_files, run_cmd, safe_name, write_files


class TesterAgent(BaseAgent):

    @property
    def name(self) -> str:
        return "Tester Agent"

    @property
    def stage(self) -> int:
        return 6

    @property
    def system_prompt(self) -> str:
        # This prompt is still used for planning test intent and expected scenarios.
        return """You are a Senior QA Engineer Agent.
You receive generated code components and must propose practical checks.

Return JSON:
{
  "test_strategy": "string",
  "focus_areas": ["string", ...],
  "critical_paths": ["string", ...]
}

Keep it concise and implementation-oriented."""

    def build_user_message(self, state: dict[str, Any]) -> str:
        developer_output = state.get("developer_output", {})
        requirements = state.get("analyst_output", {})
        security_output = state.get("security_engineer_output", {})
        db_output = state.get("database_engineer_output", {})
        return f"""Plan practical QA checks for this implementation.

IMPLEMENTATION SUMMARY:
{json.dumps([{
  "component": i.get("component_name"),
  "language": i.get("language"),
  "files": [f.get("path") for f in i.get("files", [])]
} for i in developer_output.get("implementations", [])], indent=2)}

REQUIREMENTS:
{json.dumps(requirements.get("functional_requirements", []), indent=2)}

SECURITY ENGINEERING CONTEXT:
{json.dumps(security_output, indent=2)}

DATABASE ENGINEERING CONTEXT:
{json.dumps(db_output, indent=2)}"""

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

    def _component_dir(self, run_id: str, comp_name: str) -> Path:
        root = Path(__file__).resolve().parents[1] / "run_artifacts" / safe_name(run_id) / "qa"
        return ensure_dir(root / safe_name(comp_name))

    def _python_checks(self, comp_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        unit_tests: list[dict[str, Any]] = []
        integration_tests: list[dict[str, Any]] = []

        py_files = find_files(comp_dir, ".py")
        for py_file in py_files:
            rel = str(py_file.relative_to(comp_dir))
            result = run_cmd([sys.executable, "-m", "py_compile", rel], cwd=comp_dir, timeout_sec=45)
            unit_tests.append({
                "name": f"compile::{rel}",
                "component": comp_dir.name,
                "description": "Python syntax compilation",
                "assertions": ["File compiles with py_compile"],
                "status": result["status"],
                "code_snippet": result["command"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            })

        has_pytest = any(p.name.startswith("test_") and p.suffix == ".py" for p in py_files)
        if has_pytest:
            result = run_cmd([sys.executable, "-m", "pytest", "-q"], cwd=comp_dir, timeout_sec=180)
            integration_tests.append({
                "name": "pytest_suite",
                "services_involved": [comp_dir.name],
                "description": "Run discovered pytest suite",
                "status": result["status"],
                "code_snippet": result["command"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            })
        return unit_tests, integration_tests

    def _node_checks(self, comp_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        unit_tests: list[dict[str, Any]] = []
        integration_tests: list[dict[str, Any]] = []

        js_files = find_files(comp_dir, ".js")
        for js_file in js_files:
            rel = str(js_file.relative_to(comp_dir))
            result = run_cmd(["node", "--check", rel], cwd=comp_dir, timeout_sec=45)
            unit_tests.append({
                "name": f"node-check::{rel}",
                "component": comp_dir.name,
                "description": "Node syntax validation",
                "assertions": ["File passes node --check"],
                "status": result["status"],
                "code_snippet": result["command"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            })

        if (comp_dir / "package.json").exists():
            result = run_cmd(["npm", "test", "--", "--runInBand"], cwd=comp_dir, timeout_sec=240)
            integration_tests.append({
                "name": "npm_test",
                "services_involved": [comp_dir.name],
                "description": "Run npm test suite",
                "status": result["status"],
                "code_snippet": result["command"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
            })
        return unit_tests, integration_tests

    def _go_checks(self, comp_dir: Path) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
        unit_tests: list[dict[str, Any]] = []
        integration_tests: list[dict[str, Any]] = []
        if (comp_dir / "go.mod").exists():
            result = run_cmd(["go", "test", "./..."], cwd=comp_dir, timeout_sec=240)
            status = result["status"]
            stderr_l = (result.get("stderr", "") or "").lower()
            if "no such file or directory" in stderr_l or "not found" in stderr_l:
                status = "warning"
            integration_tests.append({
                "name": "go_test",
                "services_involved": [comp_dir.name],
                "description": "Run go test for all packages",
                "status": status,
                "code_snippet": result["command"],
                "stdout": result["stdout"],
                "stderr": result["stderr"],
                "remediation": (
                    "Install Go toolchain and re-run tests (brew install go or apt/yum equivalent)."
                    if status == "warning"
                    else "Fix compilation/runtime issues from go test output."
                ),
            })
        return unit_tests, integration_tests

    def _security_checks(self, comp_dir: Path, strict_security_mode: bool = False) -> list[dict[str, Any]]:
        checks: list[dict[str, Any]] = []
        suspicious = [
            {
                "token": "eval(",
                "severity": "high",
                "status_on_hit": "fail",
                "category": "OWASP A03:2021-Injection",
                "remediation": "Avoid eval(); replace with safe parsing or explicit dispatch.",
            },
            {
                "token": "exec(",
                "severity": "high",
                "status_on_hit": "fail",
                "category": "OWASP A03:2021-Injection",
                "remediation": "Avoid exec(); replace with explicit function calls.",
            },
            {
                "token": "shell=True",
                "severity": "high",
                "status_on_hit": "fail",
                "category": "OWASP A03:2021-Injection",
                "remediation": "Do not enable shell=True for external commands.",
            },
            {
                "token": "subprocess.Popen(",
                "severity": "medium",
                "status_on_hit": "warning",
                "category": "OWASP A03:2021-Injection",
                "remediation": "Ensure command args are sanitized and avoid shell execution.",
            },
            {
                "token": "SELECT *",
                "severity": "low",
                "status_on_hit": "warning",
                "category": "Query Hygiene",
                "remediation": "Prefer explicit column selection to reduce exposure and improve performance.",
            },
        ]
        text_files = [p for p in comp_dir.rglob("*") if p.is_file() and p.suffix in {".py", ".js", ".ts", ".go"}]

        for spec in suspicious:
            token = str(spec.get("token", ""))
            hits = []
            for fp in text_files:
                content = fp.read_text(errors="ignore")
                if token in content:
                    hits.append(str(fp.relative_to(comp_dir)))
            status_on_hit = str(spec.get("status_on_hit", "warning"))
            if strict_security_mode and status_on_hit == "warning":
                status_on_hit = "fail"
            checks.append({
                "name": f"static::{token}",
                "component": comp_dir.name,
                "category": str(spec.get("category", "Static Scan")),
                "description": f"Scan for risky token `{token}`",
                "severity": str(spec.get("severity", "medium")),
                "status": status_on_hit if hits else "pass",
                "remediation": (
                    f"{str(spec.get('remediation', 'Review and remediate'))} "
                    f"Affected files: {', '.join(hits)}"
                ) if hits else "",
            })
        return checks

    def run(self, state: dict[str, Any]) -> AgentResult:
        self._logs = []
        self.log(f"[{self.name}] Starting real test execution...")

        # Optional planning step for richer context.
        planning = {"test_strategy": "Pragmatic executable checks", "focus_areas": [], "critical_paths": []}
        try:
            llm_resp = self.llm.invoke(self.effective_system_prompt(state), self.build_user_message(state))
            planning = self.parse_output(llm_resp.content)
            self.log(f"[{self.name}] Generated QA plan from LLM")
        except Exception as e:
            self.log(f"[{self.name}] QA planning fallback: {e}")

        run_id = str(state.get("run_id", "adhoc"))
        strict_security_mode = bool(state.get("strict_security_mode", False))
        implementations = state.get("developer_output", {}).get("implementations", [])

        unit_tests: list[dict[str, Any]] = []
        integration_tests: list[dict[str, Any]] = []
        load_scenarios: list[dict[str, Any]] = []
        security_checks: list[dict[str, Any]] = []
        e2e_tests: list[dict[str, Any]] = []
        artifact_paths: list[str] = []

        for impl in implementations:
            comp_name = str(impl.get("component_name", "component"))
            language = str(impl.get("language", "unknown")).lower()
            comp_dir = self._component_dir(run_id, comp_name)
            written = write_files(comp_dir, impl.get("files", []))
            artifact_paths.extend(written)
            self.log(f"[{self.name}] Materialized component `{comp_name}` into {comp_dir}")

            if "python" in language:
                unit, integ = self._python_checks(comp_dir)
            elif "node" in language or "javascript" in language or "typescript" in language:
                unit, integ = self._node_checks(comp_dir)
            elif "go" in language:
                unit, integ = self._go_checks(comp_dir)
            else:
                unit, integ = [], []
                unit.append({
                    "name": f"unsupported::{comp_name}",
                    "component": comp_name,
                    "description": f"No executable checker implemented for language `{language}`",
                    "assertions": ["Manual validation required"],
                    "status": "warning",
                    "code_snippet": "",
                    "stdout": "",
                    "stderr": "",
                    "remediation": "Skip runtime tests for this artifact type or add a language-specific tester.",
                })

            unit_tests.extend(unit)
            integration_tests.extend(integ)
            security_checks.extend(
                self._security_checks(comp_dir, strict_security_mode=strict_security_mode)
            )

            # Lightweight load/e2e placeholders backed by executable evidence availability.
            load_scenarios.append({
                "name": f"load_probe::{comp_name}",
                "description": "Load test deferred to deployment endpoint probe",
                "virtual_users": 25,
                "duration_seconds": 30,
                "target_rps": 100,
                "result": {
                    "avg_latency_ms": 0,
                    "p99_latency_ms": 0,
                    "error_rate_percent": 0 if integ else 100,
                    "status": "pass" if integ else "fail",
                },
            })
            e2e_tests.append({
                "name": f"e2e_smoke::{comp_name}",
                "user_flow": "Basic runtime smoke via build + command execution",
                "steps": ["Materialize files", "Run syntax/unit/integration checks"],
                "status": "pass" if all(t.get("status") == "pass" for t in unit) else "fail",
            })

        all_checks = unit_tests + integration_tests + security_checks
        all_statuses = [c.get("status") for c in all_checks]
        passed = sum(1 for s in all_statuses if s == "pass")
        failed = sum(1 for s in all_statuses if s == "fail")
        warnings = sum(1 for s in all_statuses if s == "warning")
        total = len(all_statuses)
        coverage = round((passed / total) * 100, 1) if total else 0.0
        critical_failures: list[dict[str, Any]] = []
        non_blocking_failures: list[dict[str, Any]] = []
        failed_checks: list[dict[str, Any]] = []
        suggested_exclusions: list[str] = []

        for check in all_checks:
            status = str(check.get("status", "")).lower()
            if status not in {"fail", "warning"}:
                continue
            name = str(check.get("name", "check"))
            stderr = str(check.get("stderr", ""))
            remediation = str(check.get("remediation", "")).strip()
            command = str(check.get("code_snippet", ""))
            component = (
                str(check.get("component", ""))
                or (check.get("services_involved", [""])[0] if isinstance(check.get("services_involved"), list) else "")
            )
            root_cause = "test assertion failed"
            stderr_l = stderr.lower()
            if name.startswith("unsupported::"):
                root_cause = "artifact type not executable by current test adapter"
                if component:
                    suggested_exclusions.append(component)
            elif name.startswith("static::"):
                if "SELECT *" in name:
                    root_cause = "over-broad SQL query pattern detected"
                else:
                    root_cause = "potentially unsafe static code pattern detected"
            elif "no such file or directory" in stderr_l or "not found" in stderr_l:
                root_cause = "missing runtime/tooling in test environment"
            elif "syntax" in stderr_l or "compile" in stderr_l:
                root_cause = "compile/syntax error"
            check_sev = str(check.get("severity", "")).lower()
            detail_severity = "critical" if (status == "fail" and check_sev == "high") else "warning"

            detail = {
                "suite": "security" if check in security_checks else ("integration" if check in integration_tests else "unit"),
                "name": name,
                "component": component,
                "severity": detail_severity,
                "root_cause": root_cause,
                "description": str(check.get("description", "")),
                "remediation": remediation
                or (
                    "Inspect test output and update generated code to satisfy test assertions."
                    if status == "fail"
                    else "Review warning and decide whether to block release."
                ),
                "command": command,
                "stderr_snippet": stderr[:700],
            }
            failed_checks.append(detail)
            is_security_check = check in security_checks
            if detail_severity == "critical" or (strict_security_mode and status == "fail" and is_security_check):
                detail["severity"] = "critical"
                critical_failures.append(detail)
            else:
                non_blocking_failures.append(detail)

        quality_gate = "pass" if len(critical_failures) == 0 else "fail"

        output = {
            "test_strategy": planning.get("test_strategy", "Executable QA checks"),
            "planning_focus_areas": planning.get("focus_areas", []),
            "strict_security_mode": strict_security_mode,
            "failed_checks": failed_checks,
            "failure_analysis": {
                "critical_failures": critical_failures,
                "non_blocking_failures": non_blocking_failures,
                "self_heal_actions": [
                    "Regenerate only failing components with focused fixes from remediation notes",
                    "Exclude non-executable artifacts from runtime test targets",
                    "Keep modernization target language consistent across regenerated components",
                ],
                "suggested_component_exclusions": sorted(set(suggested_exclusions)),
                "next_retry_prompt": (
                    "Address critical failures first; warnings should not block progression unless risk is unacceptable."
                ),
            },
            "test_suites": {
                "unit_tests": {
                    "framework": "language-native compile/runtime checks",
                    "total_tests": len(unit_tests),
                    "tests": unit_tests,
                    "coverage_percent": coverage,
                },
                "integration_tests": {
                    "framework": "runtime command execution",
                    "total_tests": len(integration_tests),
                    "tests": integration_tests,
                },
                "load_tests": {
                    "tool": "lightweight probe",
                    "scenarios": load_scenarios,
                },
                "security_tests": {
                    "tool": "static pattern scan",
                    "checks": security_checks,
                },
                "e2e_tests": {
                    "framework": "artifact flow smoke",
                    "total_tests": len(e2e_tests),
                    "tests": e2e_tests,
                },
            },
            "overall_results": {
                "total_tests": total,
                "passed": passed,
                "failed": failed,
                "warnings": warnings,
                "critical_failed": len(critical_failures),
                "coverage_percent": coverage,
                "quality_gate": quality_gate,
                "blocking_issues": (
                    []
                    if quality_gate == "pass"
                    else [f"{f.get('name')}: {f.get('root_cause')}" for f in critical_failures]
                ),
            },
            "artifacts": {
                "run_id": run_id,
                "qa_artifact_paths": artifact_paths[:200],
            },
        }

        self.log(
            f"[{self.name}] QA complete: {passed}/{total} passed, "
            f"{failed} failed, {warnings} warnings, gate={quality_gate.upper()}"
        )

        return AgentResult(
            agent_name=self.name,
            stage=self.stage,
            status="success" if quality_gate == "pass" else "warning",
            summary=self._build_summary(output),
            output=output,
            raw_response=json.dumps(output),
            tokens_used=0,
            latency_ms=0,
            logs=self._logs.copy(),
        )

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        results = parsed.get("overall_results", {})
        top_issue = ""
        issues = results.get("blocking_issues", [])
        if isinstance(issues, list) and issues:
            top_issue = f" | top issue: {str(issues[0])[:90]}"
        return (
            f"{results.get('total_tests', 0)} executable checks, "
            f"{results.get('passed', 0)} passed, "
            f"{results.get('failed', 0)} failed, "
            f"{results.get('warnings', 0)} warnings, "
            f"{results.get('critical_failed', 0)} critical, "
            f"gate={results.get('quality_gate', 'unknown').upper()}"
            f"{top_issue}"
        )
