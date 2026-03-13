import unittest
from unittest.mock import Mock

from agents.architect import ArchitectAgent
from agents.developer import DeveloperAgent
from utils.developer_dispatch import build_component_scoped_handoff
from utils.developer_prereqs import evaluate_component_prerequisites


class DeveloperPrereqsTest(unittest.TestCase):
    def _llm(self):
        llm = Mock()
        llm.config = Mock()
        llm.config.developer_parallel_agents = 3
        return llm

    def _state(self):
        return {
            "run_id": "run_developer_prereqs",
            "use_case": "code_modernization",
            "modernization_language": "C#",
            "database_target": "PostgreSQL",
            "repo_url": "https://github.com/example/bank-vb6",
            "developer_plan_approved": True,
            "developer_plan": {
                "plan_summary": "Approved developer plan",
                "proposed_components": [
                    {
                        "name": "TransactionService",
                        "service": "TransactionService",
                        "type": "api",
                        "language": "C#",
                        "framework": "ASP.NET Core",
                        "description": "Handles deposits and withdrawals.",
                        "estimated_loc": 1200,
                        "dependencies": [],
                        "priority": "critical",
                    }
                ],
            },
            "analyst_output": {
                "project_name": "BANK_SYSTEM_Modernization",
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

    def _scoped(self):
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
        self._state()["architect_handoff_package"] = handoff
        return build_component_scoped_handoff(handoff, "TransactionService")

    def test_prereqs_ready_for_valid_scoped_handoff(self):
        scoped = self._scoped()
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "READY")
        self.assertFalse(report.get("hard_blockers"))
        self.assertFalse(report.get("soft_blockers"))

    def test_prereqs_block_when_domain_evidence_missing(self):
        scoped = self._scoped()
        scoped["data_ownership"] = []
        scoped["analyst_evidence"]["data_entities"] = []
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("category") == "domain_model" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_on_mutating_get_contract(self):
        scoped = self._scoped()
        scoped["interface_contracts"][0]["operation"] = "CloseAccount"
        scoped["interface_contracts"][0]["path"] = "/accounts/{id}/close"
        scoped["interface_contracts"][0]["spec_content"]["method"] = "GET"
        report = evaluate_component_prerequisites(scoped)
        self.assertTrue(any("interface_contracts" == row.get("category") for row in report.get("hard_blockers", [])))

    def test_prereqs_block_on_semantically_incomplete_contract(self):
        scoped = self._scoped()
        scoped["interface_contracts"][0]["operation"] = "CloseAccount"
        scoped["interface_contracts"][0]["path"] = "/accounts/{id}/close"
        scoped["interface_contracts"][0]["spec_content"]["method"] = "POST"
        scoped["interface_contracts"][0]["spec_content"]["request_body"]["shape"] = ""
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("category") == "interface_contracts" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_on_residual_legacy_core_component(self):
        scoped = self._scoped()
        scoped["component_name"] = "LegacyCoreService"
        scoped["component_spec"]["component_name"] = "LegacyCoreService"
        scoped["component_spec"]["module_structure"] = [
            {"source_module": f"mod{i}", "suggested_component": f"Comp{i}", "migration_strategy": "Wrap"}
            for i in range(6)
        ]
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("category") == "component_spec" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_no_approved_adrs_exist(self):
        scoped = self._scoped()
        for decision in scoped["system_context"]["architectural_decisions"]:
            decision["status"] = "Proposed"
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("category") == "architectural_decisions" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_business_rule_refs_missing(self):
        scoped = self._scoped()
        scoped["component_spec"]["business_rule_refs"] = []
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-004" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_regression_anchor_refs_missing(self):
        scoped = self._scoped()
        scoped["component_spec"]["regression_anchor_refs"] = []
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-006" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_business_rules_are_not_semantically_enriched(self):
        scoped = self._scoped()
        for rule in scoped["brownfield_context"]["business_rules"]:
            rule["target_service"] = ""
            rule["category"] = ""
            rule["source_module"] = ""
            rule["acceptance_criteria"] = ""
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-005" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_regression_anchors_are_not_semantically_enriched(self):
        scoped = self._scoped()
        for anchor in scoped["brownfield_context"]["regression_test_anchors"]:
            anchor["golden_flow_ref"] = ""
            anchor["entry_point"] = ""
            anchor["expected_output"] = ""
            anchor["target_endpoint"] = ""
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-007" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_business_rule_targets_a_different_service(self):
        scoped = self._scoped()
        scoped["brownfield_context"]["business_rules"][0]["target_service"] = "ReportingService"
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-010" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_anchor_endpoint_is_not_in_scoped_contracts(self):
        scoped = self._scoped()
        scoped["brownfield_context"]["regression_test_anchors"][0]["target_endpoint"] = "/reference/cmdsave-click"
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-011" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_scoped_contracts_duplicate_a_route(self):
        scoped = self._scoped()
        duplicate = dict(scoped["interface_contracts"][0])
        duplicate["contract_id"] = "contract_duplicate"
        scoped["interface_contracts"].append(duplicate)
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-CONTRACTS-002" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_wbs_points_at_different_service_or_module(self):
        scoped = self._scoped()
        scoped["wbs_items"][0]["service"] = "ReportingService"
        scoped["wbs_items"][0]["stories"][0]["source_module"] = "frmstatement"
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        gap_ids = {row.get("gap_id") for row in report.get("hard_blockers", [])}
        self.assertIn("GAP-WBS-002", gap_ids)
        self.assertIn("GAP-WBS-003", gap_ids)

    def test_prereqs_block_when_referenced_business_rule_row_is_missing(self):
        scoped = self._scoped()
        scoped["brownfield_context"]["business_rules"] = []
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-001" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_referenced_anchor_row_is_missing(self):
        scoped = self._scoped()
        scoped["brownfield_context"]["regression_test_anchors"] = []
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-BROWNFIELD-002" for row in report.get("hard_blockers", [])))

    def test_prereqs_block_when_write_sql_tables_have_no_ownership_coverage(self):
        scoped = self._scoped()
        scoped["data_ownership"] = [
            {
                "entity_name": "Customers",
                "owning_service": "CustomerService",
                "read_services": ["TransactionService"],
            }
        ]
        scoped["analyst_evidence"]["data_entities"] = []
        for row in scoped["analyst_evidence"]["sql_reference_rows"]:
            row["kind"] = "update"
            row["is_write"] = True
            row["data_mutations"] = list(row.get("tables", []))
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "BLOCKED")
        self.assertTrue(any(row.get("gap_id") == "GAP-DOMAIN-003" for row in report.get("hard_blockers", [])))

    def test_developer_agent_refuses_when_component_prereqs_fail(self):
        state = self._state()
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
            state,
        )
        handoff = normalized.get("architect_handoff_package", {})
        transaction_spec = next(
            spec for spec in handoff.get("component_specs", [])
            if spec.get("component_name") == "TransactionService"
        )
        transaction_spec["interface_refs"] = []
        state["architect_output"] = {"architect_handoff_package": handoff}

        developer = DeveloperAgent(self._llm())
        result = developer.run(state)

        self.assertEqual(result.status, "error")
        self.assertIn("prerequisite_gap_report", result.output)
        self.assertEqual(result.output.get("implementations"), [])
        self.assertIn("blocked", result.summary.lower())


if __name__ == "__main__":
    unittest.main()
