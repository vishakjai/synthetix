import json
import unittest
from unittest.mock import call
from unittest.mock import Mock

from agents.architect import ArchitectAgent
from agents.developer import DeveloperAgent, DeveloperSubAgent


class DeveloperPlanAlignmentTest(unittest.TestCase):
    def _llm(self):
        llm = Mock()
        llm.config = Mock()
        llm.config.developer_parallel_agents = 1
        return llm

    def _state(self):
        return {
            "run_id": "run_developer_plan",
            "use_case": "code_modernization",
            "modernization_language": "C#",
            "database_target": "PostgreSQL",
            "developer_plan_approved": True,
            "developer_plan": {
                "plan_summary": "Approved developer plan",
                "proposed_components": [
                    {
                        "name": "TransactionModule",
                        "service": "TransactionService",
                        "type": "shared-lib",
                        "language": "C#",
                        "framework": "ASP.NET Core",
                        "description": "Implements transaction workflows.",
                        "estimated_loc": 1200,
                        "dependencies": [],
                        "priority": "critical",
                    }
                ],
            },
            "analyst_output": {
                "project_name": "BANK_SYSTEM_Modernization",
                "executive_summary": "Modernize the VB6 bank system while preserving login, customer, and transaction behavior.",
                "legacy_code_inventory": {
                    "chunk_manifest_v1": {
                        "chunks": [
                            {"chunk_id": "chunk-auth", "files": ["frmLogin.frm"], "depends_on_chunks": []},
                            {"chunk_id": "chunk-customer", "files": ["frmcustomer.frm"], "depends_on_chunks": ["chunk-auth"]},
                            {"chunk_id": "chunk-txn", "files": ["frmdeposit.frm", "frmwithdraw.frm"], "depends_on_chunks": ["chunk-customer"]},
                        ]
                    }
                },
                "raw_artifacts": {
                    "form_dossier": {
                        "dossiers": [
                            {"form_name": "frmLogin", "purpose": "Authenticate users into the application.", "source_loc": 300, "db_tables": ["users"]},
                            {"form_name": "frmcustomer", "purpose": "Customer profile onboarding and maintenance workflow.", "source_loc": 1400, "db_tables": ["customers", "accounts"]},
                            {"form_name": "frmdeposit", "purpose": "Deposit capture and account balance update workflow.", "source_loc": 900, "db_tables": ["transactions", "accounts"]},
                            {"form_name": "frmwithdraw", "purpose": "Withdrawal processing and balance validation workflow.", "source_loc": 850, "db_tables": ["transactions", "accounts"]},
                        ]
                    },
                    "risk_register": {"risks": [{"risk_id": "RISK-001", "form": "frmdeposit"}]},
                    "business_rule_catalog": {
                        "rules": [
                            {"rule_id": "BR-001", "form": "frmcustomer", "statement": "Customer records require a unique account number."},
                            {"rule_id": "BR-002", "form": "frmdeposit", "statement": "Deposits must update balance and ledger atomically."},
                        ]
                    },
                    "sql_catalog": {
                        "statements": [
                            {"form": "frmdeposit", "tables": ["tbltransaction", "tblaccount"], "sql_id": "sql:01"},
                            {"form": "frmwithdraw", "tables": ["tbltransaction", "tblaccount"], "sql_id": "sql:02"},
                            {"form": "frmcustomer", "tables": ["tblcustomers", "tblaccount"], "sql_id": "sql:03"},
                        ]
                    },
                    "dependency_inventory": {"dependencies": [{"name": "MSCOMCT2.OCX"}]},
                    "golden_flows": {
                        "flows": [
                            {"description": "frmdeposit::cmdSave_Click records deposit and updates account balance."},
                        ]
                    },
                    "global_module_inventory": {
                        "variables": [
                            {"name": "gCurrentBalance", "used_in_modules": ["frmdeposit", "frmwithdraw"], "owning_service": "TransactionService"},
                        ]
                    },
                    "static_risk_detectors": {
                        "findings": [
                            {"signal": "missing_rollback_guard", "severity": "high", "message": "Deposit and withdrawal writes have no rollback guards."},
                        ]
                    },
                    "connection_string_variants": {
                        "variants": [
                            {"name": "bank-mdb", "provider": "OLEDB", "connection_string": "Provider=Microsoft.Jet.OLEDB.4.0;Data Source=bank.mdb;"},
                        ]
                    },
                },
            },
        }

    def _handoff(self):
        agent = ArchitectAgent(self._llm())
        normalized = agent._normalize_output(
            {
                "architecture_name": "",
                "pattern": "",
                "overview": "",
                "services": [],
                "legacy_system": {
                    "current_logic_summary": "Legacy VB6 workflow.",
                    "key_logic_steps": ["Login", "Customer menu", "Transaction processing"],
                },
            },
            self._state(),
        )
        handoff = normalized.get("architect_handoff_package", {})
        for decision in handoff.get("system_context", {}).get("architectural_decisions", []):
            if "TransactionService" in (decision.get("target_services") or []):
                decision["status"] = "Approved"
        return handoff

    def test_anchor_decomposition_reuses_architect_component_names(self):
        handoff = self._handoff()
        decomposition = {
            "decomposition_strategy": "LLM invented component names",
            "components": [
                {
                    "name": "AuthenticationModule",
                    "service": "AuthenticationService",
                    "type": "shared-lib",
                    "language": "C#",
                    "framework": "ASP.NET Core",
                    "description": "Handles login.",
                    "estimated_loc": 600,
                    "dependencies": [],
                    "priority": "critical",
                },
                {
                    "name": "CustomerModule",
                    "service": "CustomerService",
                    "type": "shared-lib",
                    "language": "C#",
                    "framework": "ASP.NET Core",
                    "description": "Handles customers.",
                    "estimated_loc": 900,
                    "dependencies": [],
                    "priority": "high",
                },
            ],
        }

        anchored = DeveloperAgent._anchor_decomposition_to_handoff(decomposition, handoff, "C#")
        component_names = [row.get("name") for row in anchored.get("components", [])]

        self.assertIn("AuthenticationService", component_names)
        self.assertIn("CustomerService", component_names)
        self.assertNotIn("AuthenticationModule", component_names)
        self.assertNotIn("CustomerModule", component_names)

    def test_run_aligns_approved_plan_to_architect_scoped_components(self):
        handoff = self._handoff()
        llm = self._llm()
        llm.invoke_with_tools.return_value = Mock(tool_calls=[], input_tokens=0, output_tokens=0, latency_ms=0.0)
        llm.invoke.return_value = Mock(
            content=json.dumps(
                {
                    "component_name": "TransactionService",
                    "language": "C#",
                    "framework": "ASP.NET Core",
                    "files": [
                        {
                            "path": "Program.cs",
                            "description": "App entrypoint",
                            "code": "var builder = WebApplication.CreateBuilder(args);",
                            "lines_of_code": 1,
                        },
                        {
                            "path": "Dockerfile",
                            "description": "Container image",
                            "code": "FROM mcr.microsoft.com/dotnet/aspnet:8.0",
                            "lines_of_code": 1,
                        },
                        {
                            "path": "README.md",
                            "description": "Run instructions",
                            "code": "# TransactionService",
                            "lines_of_code": 1,
                        },
                        {
                            "path": "TransactionService.csproj",
                            "description": "Project file",
                            "code": "<Project Sdk=\"Microsoft.NET.Sdk.Web\"></Project>",
                            "lines_of_code": 1,
                        },
                        {
                            "path": "TransactionService.Tests.cs",
                            "description": "Tests",
                            "code": "public class SmokeTest {}",
                            "lines_of_code": 1,
                        },
                    ],
                    "dependencies": [],
                    "environment_variables": ["PORT"],
                    "docker_support": True,
                    "total_loc": 5,
                    "notes": "Generated for parity testing.",
                }
            ),
            input_tokens=1,
            output_tokens=1,
            latency_ms=1.0,
        )
        state = self._state()
        state["architect_handoff_package"] = handoff

        result = DeveloperAgent(llm).run(state)

        self.assertEqual(result.status, "success")
        self.assertEqual(
            result.output.get("execution", {}).get("planner_selected_components"),
            ["TransactionService"],
        )
        self.assertEqual(
            result.output.get("execution", {}).get("component_dispatches", [])[0].get("component_name"),
            "TransactionService",
        )

    def test_run_fails_when_subagent_returns_no_files(self):
        handoff = self._handoff()
        llm = self._llm()
        llm.invoke_with_tools.return_value = Mock(tool_calls=[], input_tokens=0, output_tokens=0, latency_ms=0.0)
        llm.invoke.return_value = Mock(
            content=json.dumps(
                {
                    "component_name": "TransactionService",
                    "language": "C#",
                    "framework": "ASP.NET Core",
                    "files": [],
                    "dependencies": [],
                    "environment_variables": ["PORT"],
                    "docker_support": True,
                    "total_loc": 0,
                    "notes": "No files emitted.",
                }
            ),
            input_tokens=1,
            output_tokens=1,
            latency_ms=1.0,
        )
        state = self._state()
        state["architect_handoff_package"] = handoff

        result = DeveloperAgent(llm).run(state)

        self.assertEqual(result.status, "error")
        self.assertEqual(result.output.get("error"), "Developer sub-agent generation failed")
        self.assertEqual(
            result.output.get("subagent_failure_report", {}).get("component_failures", [])[0].get("error"),
            "No implementation files generated",
        )
        self.assertTrue(
            result.output.get("implementations", [])[0].get("generation_diagnostics", {}).get("attempt_trace")
        )

    def test_run_fails_when_subagent_returns_invalid_file_payload(self):
        handoff = self._handoff()
        llm = self._llm()
        llm.invoke_with_tools.return_value = Mock(tool_calls=[], input_tokens=0, output_tokens=0, latency_ms=0.0)
        llm.invoke.return_value = Mock(
            content=json.dumps(
                {
                    "component_name": "TransactionService",
                    "language": "C#",
                    "framework": "ASP.NET Core",
                    "files": [
                        {
                            "path": "Program.cs",
                            "description": "Broken payload",
                            "code": "",
                            "lines_of_code": 0,
                        }
                    ],
                    "dependencies": [],
                    "environment_variables": ["PORT"],
                    "docker_support": True,
                    "total_loc": 0,
                    "notes": "Malformed file entry.",
                }
            ),
            input_tokens=1,
            output_tokens=1,
            latency_ms=1.0,
        )
        state = self._state()
        state["architect_handoff_package"] = handoff

        result = DeveloperAgent(llm).run(state)

        self.assertEqual(result.status, "error")
        self.assertEqual(result.output.get("error"), "Developer sub-agent generation failed")
        self.assertEqual(
            result.output.get("subagent_failure_report", {}).get("component_failures", [])[0].get("error"),
            "Generated file payload is invalid",
        )
        self.assertTrue(
            result.output.get("implementations", [])[0].get("generation_diagnostics", {}).get("attempt_trace")
        )

    def test_subagent_retries_after_empty_response_and_succeeds(self):
        llm = self._llm()
        llm.invoke.side_effect = [
            Mock(content="", input_tokens=1, output_tokens=1, latency_ms=1.0),
            Mock(
                content=json.dumps(
                    {
                        "component_name": "AuthenticationService",
                        "language": "C#",
                        "framework": "ASP.NET Core 8",
                        "files": [
                            {
                                "path": "Program.cs",
                                "description": "Entrypoint",
                                "code": "var builder = WebApplication.CreateBuilder(args);",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "AuthenticationService.csproj",
                                "description": "Project file",
                                "code": "<Project Sdk=\"Microsoft.NET.Sdk.Web\"></Project>",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Controllers/HealthController.cs",
                                "description": "Health controller",
                                "code": "public class HealthController {}",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Dockerfile",
                                "description": "Container",
                                "code": "FROM mcr.microsoft.com/dotnet/aspnet:8.0",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "README.md",
                                "description": "Docs",
                                "code": "# AuthenticationService",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Tests/SmokeTests.cs",
                                "description": "Smoke test",
                                "code": "public class SmokeTests {}",
                                "lines_of_code": 1,
                            },
                        ],
                        "dependencies": [],
                        "environment_variables": ["PORT"],
                        "docker_support": True,
                        "total_loc": 6,
                        "notes": "Recovered on retry.",
                    }
                ),
                input_tokens=2,
                output_tokens=2,
                latency_ms=2.0,
            ),
        ]

        result = DeveloperSubAgent(
            llm=llm,
            component={"name": "AuthenticationService", "type": "api", "language": "C#"},
            requirements={"executive_summary": "Modernize auth."},
            component_handoff={"component_name": "AuthenticationService"},
            modernization_language="C#",
        ).run()

        self.assertNotIn("error", result)
        self.assertEqual(result.get("component_name"), "AuthenticationService")
        self.assertEqual(len(result.get("files", [])), 6)
        self.assertEqual(result.get("_llm_metrics", {}).get("tokens_used"), 6)

    def test_subagent_repairs_wrapped_json_response(self):
        llm = self._llm()
        llm.invoke_with_tools.side_effect = RuntimeError("tool path unavailable")
        llm.invoke.side_effect = [
            Mock(content="Here is the implementation you asked for.", input_tokens=1, output_tokens=1, latency_ms=1.0),
            Mock(
                content=json.dumps(
                    {
                        "component_name": "ExperienceShell",
                        "language": "C#",
                        "framework": "ASP.NET Core 8",
                        "files": [
                            {
                                "path": "Program.cs",
                                "description": "Entrypoint",
                                "code": "var builder = WebApplication.CreateBuilder(args);",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "ExperienceShell.csproj",
                                "description": "Project file",
                                "code": "<Project Sdk=\"Microsoft.NET.Sdk.Web\"></Project>",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Pages/Index.cshtml",
                                "description": "UI shell",
                                "code": "<h1>ExperienceShell</h1>",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Pages/_ViewImports.cshtml",
                                "description": "Razor imports",
                                "code": "@addTagHelper *, Microsoft.AspNetCore.Mvc.TagHelpers",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Dockerfile",
                                "description": "Container",
                                "code": "FROM mcr.microsoft.com/dotnet/aspnet:8.0",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "README.md",
                                "description": "Docs",
                                "code": "# ExperienceShell",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Tests/SmokeTests.cs",
                                "description": "Smoke test",
                                "code": "public class SmokeTests {}",
                                "lines_of_code": 1,
                            },
                        ],
                        "dependencies": [],
                        "environment_variables": ["PORT"],
                        "docker_support": True,
                        "total_loc": 7,
                        "notes": "Recovered via repair.",
                    }
                ),
                input_tokens=2,
                output_tokens=2,
                latency_ms=2.0,
            ),
        ]

        result = DeveloperSubAgent(
            llm=llm,
            component={"name": "ExperienceShell", "type": "frontend", "language": "C#"},
            requirements={"executive_summary": "Modernize shell."},
            component_handoff={"component_name": "ExperienceShell"},
            modernization_language="C#",
        ).run()

        self.assertNotIn("error", result)
        self.assertEqual(result.get("component_name"), "ExperienceShell")
        self.assertEqual(len(result.get("files", [])), 7)

    def test_subagent_accepts_tool_payload_with_empty_message_content(self):
        llm = self._llm()
        llm.invoke_with_tools.return_value = Mock(
            content="",
            tool_calls=[
                {
                    "name": "emit_component_artifact",
                    "arguments": {
                        "component_name": "AuthenticationService",
                        "language": "C#",
                        "framework": "ASP.NET Core 8",
                        "files": [
                            {
                                "path": "Program.cs",
                                "description": "Entrypoint",
                                "code": "var builder = WebApplication.CreateBuilder(args);",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "AuthenticationService.csproj",
                                "description": "Project file",
                                "code": "<Project Sdk=\"Microsoft.NET.Sdk.Web\"></Project>",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Controllers/HealthController.cs",
                                "description": "Health controller",
                                "code": "public class HealthController {}",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Dockerfile",
                                "description": "Container",
                                "code": "FROM mcr.microsoft.com/dotnet/aspnet:8.0",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "README.md",
                                "description": "Docs",
                                "code": "# AuthenticationService",
                                "lines_of_code": 1,
                            },
                            {
                                "path": "Tests/SmokeTests.cs",
                                "description": "Smoke test",
                                "code": "public class SmokeTests {}",
                                "lines_of_code": 1,
                            },
                        ],
                        "dependencies": [],
                        "environment_variables": ["PORT"],
                        "docker_support": True,
                        "total_loc": 6,
                        "notes": "Generated through tool call.",
                    },
                }
            ],
            input_tokens=2,
            output_tokens=2,
            latency_ms=2.0,
        )

        result = DeveloperSubAgent(
            llm=llm,
            component={"name": "AuthenticationService", "type": "api", "language": "C#"},
            requirements={"executive_summary": "Modernize auth."},
            component_handoff={"component_name": "AuthenticationService"},
            modernization_language="C#",
        ).run()

        self.assertNotIn("error", result)
        self.assertEqual(result.get("component_name"), "AuthenticationService")
        self.assertEqual(len(result.get("files", [])), 6)

    def test_subagent_falls_back_to_secondary_model_when_primary_is_empty(self):
        llm = self._llm()
        llm.config.provider = "openai"
        llm.config.get_model = Mock(return_value="gpt-5")
        llm.invoke_with_tools.side_effect = [
            Mock(content="", tool_calls=[], input_tokens=1, output_tokens=1, latency_ms=1.0),
            Mock(
                content="",
                tool_calls=[
                    {
                        "name": "emit_component_artifact",
                        "arguments": {
                            "component_name": "AuthenticationService",
                            "language": "C#",
                            "framework": "ASP.NET Core 8",
                            "files": [
                                {
                                    "path": "Program.cs",
                                    "description": "Entrypoint",
                                    "code": "var builder = WebApplication.CreateBuilder(args);",
                                    "lines_of_code": 1,
                                },
                                {
                                    "path": "AuthenticationService.csproj",
                                    "description": "Project file",
                                    "code": "<Project Sdk=\"Microsoft.NET.Sdk.Web\"></Project>",
                                    "lines_of_code": 1,
                                },
                                {
                                    "path": "Controllers/HealthController.cs",
                                    "description": "Health controller",
                                    "code": "public class HealthController {}",
                                    "lines_of_code": 1,
                                },
                                {
                                    "path": "Dockerfile",
                                    "description": "Container",
                                    "code": "FROM mcr.microsoft.com/dotnet/aspnet:8.0",
                                    "lines_of_code": 1,
                                },
                                {
                                    "path": "README.md",
                                    "description": "Docs",
                                    "code": "# AuthenticationService",
                                    "lines_of_code": 1,
                                },
                                {
                                    "path": "Tests/SmokeTests.cs",
                                    "description": "Smoke test",
                                    "code": "public class SmokeTests {}",
                                    "lines_of_code": 1,
                                },
                            ],
                            "dependencies": [],
                            "environment_variables": ["PORT"],
                            "docker_support": True,
                            "total_loc": 6,
                            "notes": "Fallback model output.",
                        },
                    }
                ],
                input_tokens=2,
                output_tokens=2,
                latency_ms=2.0,
            ),
        ]
        llm.invoke.return_value = Mock(content="", input_tokens=1, output_tokens=1, latency_ms=1.0)

        result = DeveloperSubAgent(
            llm=llm,
            component={"name": "AuthenticationService", "type": "api", "language": "C#"},
            requirements={"executive_summary": "Modernize auth."},
            component_handoff={"component_name": "AuthenticationService"},
            modernization_language="C#",
        ).run()

        self.assertNotIn("error", result)
        self.assertEqual(result.get("component_name"), "AuthenticationService")
        self.assertEqual(len(result.get("files", [])), 6)
        self.assertTrue(
            any(
                candidate.kwargs.get("model_override") == "gpt-4o"
                for candidate in llm.invoke_with_tools.call_args_list
            )
        )


if __name__ == "__main__":
    unittest.main()
