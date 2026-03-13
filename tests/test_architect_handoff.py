import asyncio
import json
import unittest
from unittest.mock import Mock, patch

from agents.architect import ArchitectAgent
from agents.base import AgentResult
import orchestrator.pipeline as pipeline
from utils.architect_handoff import validate_architect_handoff_json
from utils.developer_dispatch import build_component_scoped_handoff
from utils.developer_prereqs import evaluate_component_prerequisites
import web.server as server


class _FakeRequest:
    def __init__(self, *, path_params: dict | None = None, query_params: dict | None = None):
        self.path_params = path_params or {}
        self.query_params = query_params or {}


class ArchitectHandoffPackageTest(unittest.TestCase):
    def _state(self):
        return {
            "run_id": "run_architect_handoff",
            "use_case": "code_modernization",
            "modernization_language": "C#",
            "database_target": "PostgreSQL",
            "repo_url": "https://github.com/example/bank-vb6",
            "analyst_output": {
                "project_name": "BANK_SYSTEM_Modernization",
                "legacy_code_inventory": {
                    "chunk_manifest_v1": {
                        "chunks": [
                            {
                                "chunk_id": "chunk-auth",
                                "files": ["frmLogin.frm"],
                                "depends_on_chunks": [],
                            },
                            {
                                "chunk_id": "chunk-customer",
                                "files": ["frmcustomer.frm", "frmsettings.frm"],
                                "depends_on_chunks": ["chunk-auth"],
                            },
                            {
                                "chunk_id": "chunk-txn",
                                "files": ["frmdeposit.frm", "frmwithdraw.frm"],
                                "depends_on_chunks": ["chunk-customer"],
                            },
                        ]
                    },
                    "source_loc_by_file": [
                        {"path": "BankApp1/frmLogin.frm", "loc": 300},
                        {"path": "BankApp1/frmcustomer.frm", "loc": 1400},
                        {"path": "BankApp1/frmdeposit.frm", "loc": 900},
                        {"path": "BankApp1/frmwithdraw.frm", "loc": 850},
                        {"path": "BankApp1/frmsettings.frm", "loc": 250},
                    ],
                },
                "raw_artifacts": {
                    "form_dossier": {
                        "dossiers": [
                            {
                                "form_name": "frmLogin",
                                "base_form_name": "frmLogin",
                                "source_file": "frmLogin.frm",
                                "project_name": "BANK",
                                "purpose": "Authenticate users into the application.",
                                "source_loc": 300,
                                "coverage_score": 0.95,
                                "confidence_score": 0.93,
                                "db_tables": ["users"],
                            },
                            {
                                "form_name": "frmcustomer",
                                "base_form_name": "frmcustomer",
                                "source_file": "frmcustomer.frm",
                                "project_name": "BANK",
                                "purpose": "Customer profile onboarding and maintenance workflow.",
                                "source_loc": 1400,
                                "coverage_score": 0.84,
                                "confidence_score": 0.88,
                                "db_tables": ["customers", "accounts"],
                            },
                            {
                                "form_name": "frmdeposit",
                                "base_form_name": "frmdeposit",
                                "source_file": "frmdeposit.frm",
                                "project_name": "BANK",
                                "purpose": "Deposit capture and account balance update workflow.",
                                "source_loc": 900,
                                "coverage_score": 0.72,
                                "confidence_score": 0.78,
                                "db_tables": ["transactions", "accounts"],
                            },
                            {
                                "form_name": "frmwithdraw",
                                "base_form_name": "frmwithdraw",
                                "source_file": "frmwithdraw.frm",
                                "project_name": "BANK",
                                "purpose": "Withdrawal processing and balance validation workflow.",
                                "source_loc": 850,
                                "coverage_score": 0.69,
                                "confidence_score": 0.74,
                                "db_tables": ["transactions", "accounts"],
                            },
                            {
                                "form_name": "frmsettings",
                                "base_form_name": "frmsettings",
                                "source_file": "frmsettings.frm",
                                "project_name": "BANK",
                                "purpose": "Reference data and settings maintenance.",
                                "source_loc": 250,
                                "coverage_score": 0.81,
                                "confidence_score": 0.82,
                                "db_tables": ["account_types"],
                            },
                        ]
                    },
                    "risk_register": {
                        "risks": [
                            {"risk_id": "RISK-001", "form": "frmdeposit"},
                            {"risk_id": "RISK-002", "form": "frmwithdraw"},
                        ]
                    },
                    "business_rule_catalog": {
                        "rules": [
                            {"rule_id": "BR-001", "form": "frmcustomer", "statement": "Customer records require a unique account number."},
                            {"rule_id": "BR-002", "form": "frmdeposit", "statement": "Deposits must update balance and ledger atomically."},
                            {"rule_id": "BR-003", "form": "frmLogin", "statement": "RecordCount < 1 prevents login and increments failed attempts."},
                            {"rule_id": "BR-004", "form": "frmdeposit", "statement": "Interest = Balance * 1 / 100 before balance update."},
                            {"rule_id": "BR-005", "form": "frmcustomer", "statement": "CalendarForeColor = -2147483635"},
                            {"rule_id": "BR-006", "form": "frmcustomer", "statement": "i = i + 1"},
                        ]
                    },
                    "sql_catalog": {
                        "statements": [
                            {"form": "frmdeposit", "tables": ["tbltransaction", "tblaccount"], "sql_id": "sql:01"},
                            {"form": "frmwithdraw", "tables": ["tbltransaction", "tblaccount"], "sql_id": "sql:02"},
                            {"form": "frmcustomer", "tables": ["tblcustomers", "tblaccount"], "sql_id": "sql:03"},
                        ]
                    },
                    "dependency_inventory": {
                        "dependencies": [
                            {"name": "MSCOMCT2.OCX"},
                            {"name": "DBGRID32.OCX"},
                        ]
                    },
                    "golden_flows": {
                        "flows": [
                            {"description": "frmLogin::cmdOK_Click authenticates user and opens customer menu."},
                            {"description": "frmdeposit::cmdSave_Click records deposit and updates account balance."},
                        ]
                    },
                    "global_module_inventory": {
                        "variables": [
                            {"name": "gCustomerID", "used_in_modules": ["frmcustomer", "frmdeposit"], "owning_service": "CustomerService"},
                            {"name": "gCurrentBalance", "used_in_modules": ["frmdeposit", "frmwithdraw"], "owning_service": "TransactionService"},
                        ]
                    },
                    "static_risk_detectors": {
                        "findings": [
                            {"signal": "missing_rollback_guard", "severity": "high", "message": "Deposit and withdrawal writes have no rollback guards."},
                            {"signal": "select_max_transactionid", "severity": "high", "message": "SELECT MAX(transactionid) used for id generation."},
                        ]
                    },
                    "dead_form_references": {
                        "references": [
                            {"reference": "frmcloseacount", "reason": "Potential duplicate/typo of frmcloseaccount."},
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

    def test_architect_emits_valid_handoff_package(self):
        agent = ArchitectAgent(Mock())
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
        handoff = normalized.get("architect_handoff_package")
        self.assertIsInstance(handoff, dict)
        validate_architect_handoff_json(handoff)
        self.assertEqual(handoff.get("artifact_type"), "architect_handoff_package_v1")
        self.assertTrue(handoff.get("component_specs"))
        self.assertTrue(handoff.get("interface_contracts"))
        self.assertTrue(handoff.get("brownfield_context", {}).get("regression_test_anchors"))
        self.assertTrue(handoff.get("human_review_queue"))
        self.assertTrue(handoff.get("domain_model", {}).get("entities"))
        self.assertTrue(handoff.get("domain_model", {}).get("data_ownership"))
        self.assertTrue(handoff.get("brownfield_context", {}).get("business_rules"))
        self.assertGreaterEqual(handoff.get("brownfield_context", {}).get("source_evidence_summary", {}).get("golden_flow_count", 0), 2)
        self.assertTrue(any(str(row.get("status", "")).strip().lower() in {"accepted", "approved"} for row in handoff.get("system_context", {}).get("architectural_decisions", [])))
        first_contract = handoff.get("interface_contracts", [])[0]
        self.assertIn("request_body", first_contract.get("spec_content", {}))
        self.assertIn("auth", first_contract.get("spec_content", {}))
        self.assertTrue(first_contract.get("spec_content", {}).get("operations"))
        first_business_rule = handoff.get("brownfield_context", {}).get("business_rules", [])[0]
        self.assertTrue(first_business_rule.get("target_service"))
        self.assertTrue(first_business_rule.get("category"))
        self.assertTrue(first_business_rule.get("source_module"))
        self.assertTrue(first_business_rule.get("acceptance_criteria"))
        self.assertTrue(all(rule.get("target_service") for rule in handoff.get("brownfield_context", {}).get("business_rules", [])))
        first_anchor = handoff.get("brownfield_context", {}).get("regression_test_anchors", [])[0]
        self.assertTrue(first_anchor.get("golden_flow_ref"))
        self.assertTrue(first_anchor.get("entry_point"))
        self.assertTrue(first_anchor.get("expected_output"))
        self.assertTrue(first_anchor.get("target_endpoint"))
        self.assertTrue(first_anchor.get("data_fixture"))

    def test_component_specs_are_traced_to_contracts_and_wbs(self):
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, self._state())
        handoff = normalized.get("architect_handoff_package", {})
        component_specs = handoff.get("component_specs", [])
        self.assertTrue(component_specs)
        self.assertTrue(all(spec.get("interface_refs") for spec in component_specs))
        self.assertTrue(all(spec.get("wbs_refs") for spec in component_specs))
        self.assertTrue(any(spec.get("business_rule_refs") for spec in component_specs))
        self.assertTrue(any(spec.get("regression_anchor_refs") for spec in component_specs))
        customer_spec = next(spec for spec in component_specs if spec.get("component_name") == "CustomerService")
        transaction_spec = next(spec for spec in component_specs if spec.get("component_name") == "TransactionService")
        self.assertTrue(customer_spec.get("business_rule_refs"))
        self.assertTrue(transaction_spec.get("business_rule_refs"))
        self.assertTrue(transaction_spec.get("regression_anchor_refs"))

    def test_business_rules_filter_noise_and_route_meaningful_rules(self):
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, self._state())
        handoff = normalized.get("architect_handoff_package", {})
        business_rules = handoff.get("brownfield_context", {}).get("business_rules", [])
        statements = [str(rule.get("statement", "")) for rule in business_rules]
        self.assertFalse(any("CalendarForeColor" in statement for statement in statements))
        self.assertFalse(any(statement.strip().lower() == "i = i + 1" for statement in statements))
        auth_rule = next(rule for rule in business_rules if rule.get("rule_id") == "BR-003")
        txn_rule = next(rule for rule in business_rules if rule.get("rule_id") == "BR-004")
        self.assertEqual(auth_rule.get("target_service"), "AuthenticationService")
        self.assertEqual(txn_rule.get("target_service"), "TransactionService")

    def test_public_architect_package_ownership_and_contract_semantics_are_not_hollow(self):
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, self._state())
        architect_package = normalized.get("architect_package", {})
        ownership = architect_package.get("artifacts", {}).get("data_ownership_matrix", {}).get("entities", [])
        ownership_by_name = {str(row.get("name", "")).lower(): row for row in ownership}
        customer_row = ownership_by_name.get("tblcustomers") or ownership_by_name.get("customers")
        transaction_row = ownership_by_name.get("tbltransaction") or ownership_by_name.get("transactions")
        self.assertEqual((customer_row or {}).get("owning_service"), "CustomerService")
        self.assertEqual((transaction_row or {}).get("owning_service"), "TransactionService")
        api_services = architect_package.get("artifacts", {}).get("api_contract_sketches", {}).get("services", [])
        auth_ops = next(service for service in api_services if service.get("service") == "AuthenticationService").get("operations", [])
        self.assertTrue(any(op.get("method") == "POST" and op.get("path") == "/auth/login" for op in auth_ops))

    def test_handoff_derives_entities_from_sql_usage_site_refs(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"]["sql_catalog"] = {
            "statements": [
                {
                    "sql_id": "sql:3",
                    "tables": ["tbltransaction"],
                    "usage_sites": [
                        {"external_ref": {"ref": "(unmapped)::frmdeposit::cmdSave_Click"}},
                    ],
                },
                {
                    "sql_id": "sql:4",
                    "tables": ["tblcustomers"],
                    "usage_sites": [
                        {"external_ref": {"ref": "BANK::frmcustomer::cmdSave_Click"}},
                    ],
                },
            ]
        }
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, state)
        handoff = normalized.get("architect_handoff_package", {})
        entities = handoff.get("domain_model", {}).get("entities", [])
        ownership = handoff.get("domain_model", {}).get("data_ownership", [])
        self.assertTrue(entities)
        self.assertTrue(ownership)
        owners = {row.get("owning_service") for row in ownership}
        self.assertIn("TransactionService", owners)
        self.assertIn("CustomerService", owners)

    def test_login_contract_uses_post_and_request_fields(self):
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, self._state())
        handoff = normalized["architect_handoff_package"]
        login_contract = next(
            contract
            for contract in handoff.get("interface_contracts", [])
            if "login" in str(contract.get("spec_content", {}).get("name", "")).lower()
        )
        operations = login_contract.get("spec_content", {}).get("operations", [])
        self.assertTrue(operations)
        op = operations[0]
        self.assertEqual(op.get("method"), "POST")
        request_fields = {field.get("name") for field in op.get("request_body", {}).get("fields", [])}
        self.assertIn("username", request_fields)
        self.assertIn("password", request_fields)

    def test_upstream_ownership_is_reconciled_to_customer_and_transaction_services(self):
        agent = ArchitectAgent(Mock())
        architect = {
            "legacy_system": {},
            "data_ownership_matrix": {
                "entities": [
                    {
                        "name": "Customers",
                        "legacy_tables": ["tblcustomers"],
                        "owning_service": "ReportingService",
                        "read_services": ["TransactionService"],
                    },
                    {
                        "name": "Transaction",
                        "legacy_tables": ["tbltransaction"],
                        "owning_service": "ReportingService",
                        "read_services": ["ReportingService"],
                    },
                ],
                "relationships": [],
            },
        }
        normalized = agent._normalize_output(architect, self._state())
        handoff = normalized["architect_handoff_package"]
        ownership = {
            row.get("entity_name"): row.get("owning_service")
            for row in handoff.get("domain_model", {}).get("data_ownership", [])
        }
        self.assertEqual(ownership.get("Customers") or ownership.get("customers") or ownership.get("tblcustomers"), "CustomerService")
        self.assertEqual(ownership.get("Transaction") or ownership.get("transactions") or ownership.get("tbltransaction"), "TransactionService")

    def test_description_only_golden_flows_produce_actionable_anchors(self):
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, self._state())
        anchors = normalized.get("architect_handoff_package", {}).get("brownfield_context", {}).get("regression_test_anchors", [])
        login_anchor = next(anchor for anchor in anchors if anchor.get("golden_flow_ref") == "GF-001")
        deposit_anchor = next(anchor for anchor in anchors if anchor.get("golden_flow_ref") == "GF-002")
        self.assertEqual(login_anchor.get("entry_point"), "frmLogin::cmdOK_Click")
        self.assertTrue(login_anchor.get("target_endpoint"))
        self.assertEqual(deposit_anchor.get("entry_point"), "frmdeposit::cmdSave_Click")
        self.assertTrue(deposit_anchor.get("target_endpoint"))

    def test_description_only_flow_without_entrypoint_is_semantically_enriched(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"]["golden_flows"] = {
            "flows": [
                {
                    "flow_id": "GF-900",
                    "description": "Deposit workflow records the ledger entry and updates the account balance atomically.",
                }
            ]
        }
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, state)
        anchors = normalized.get("architect_handoff_package", {}).get("brownfield_context", {}).get("regression_test_anchors", [])
        anchor = next(anchor for anchor in anchors if anchor.get("golden_flow_ref") == "GF-900")
        self.assertTrue(anchor.get("entry_point"))
        self.assertEqual(anchor.get("target_service"), "TransactionService")
        self.assertEqual(anchor.get("target_endpoint"), "/transactions/deposit")

    def test_write_path_sql_reconciles_owner_even_when_reads_are_shared(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"]["sql_catalog"] = {
            "statements": [
                {
                    "sql_id": "sql:write",
                    "kind": "update",
                    "form": "frmdeposit",
                    "tables": ["tblaccount"],
                    "data_mutations": ["tblaccount"],
                },
                {
                    "sql_id": "sql:read-1",
                    "kind": "select",
                    "form": "frmcustomer",
                    "tables": ["tblaccount"],
                },
                {
                    "sql_id": "sql:read-2",
                    "kind": "select",
                    "form": "frmwithdraw",
                    "tables": ["tblaccount"],
                },
            ]
        }
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, state)
        handoff = normalized["architect_handoff_package"]
        ownership = {
            row.get("entity_name"): row.get("owning_service")
            for row in handoff.get("domain_model", {}).get("data_ownership", [])
        }
        self.assertEqual(ownership.get("tblaccount") or ownership.get("Account"), "TransactionService")

    def test_chained_golden_flow_refs_keep_form_and_handler_not_system_prefix(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"]["golden_flows"] = {
            "flows": [
                {
                    "flow_id": "GF-009",
                    "description": "BANK::frmLogin1::cmdOK_Click authenticates users and opens the MDI shell.",
                }
            ]
        }
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        anchor = normalized["architect_handoff_package"]["brownfield_context"]["regression_test_anchors"][0]
        self.assertEqual(anchor.get("entry_point"), "frmLogin1::cmdOK_Click")
        self.assertEqual(anchor.get("target_endpoint"), "/auth/login")
        self.assertTrue(anchor.get("expected_output"))

    def test_handoff_wbs_is_developer_ready(self):
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, self._state())["architect_handoff_package"]
        items = handoff.get("wbs", {}).get("items", [])
        self.assertTrue(items)
        first = items[0]
        self.assertTrue(first.get("epic_id"))
        self.assertTrue(first.get("epic_name"))
        self.assertTrue(first.get("acceptance_criteria"))
        self.assertTrue(first.get("stories"))
        self.assertTrue(first["stories"][0].get("description"))
        self.assertTrue(first["stories"][0].get("acceptance_criteria"))
        self.assertTrue(all(item.get("service") for item in items))
        self.assertTrue(all(str(item.get("wbs_id", "")).count("-") >= 3 for item in items))

    def test_component_scoped_wbs_does_not_leak_across_services_in_same_phase(self):
        state = self._state()
        state["analyst_output"]["legacy_code_inventory"]["source_loc_by_file"].extend(
            [
                {"path": "BankApp1/Mdi.frm", "loc": 180},
                {"path": "BankApp1/frmSplash.frm", "loc": 100},
                {"path": "BankApp1/menu.frm", "loc": 60},
            ]
        )
        state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"].extend(
            [
                {
                    "form_name": "Mdi",
                    "base_form_name": "Mdi",
                    "source_file": "Mdi.frm",
                    "project_name": "BANK",
                    "purpose": "Application shell and navigation.",
                    "source_loc": 180,
                    "coverage_score": 0.8,
                    "confidence_score": 0.82,
                    "db_tables": [],
                },
                {
                    "form_name": "frmSplash",
                    "base_form_name": "frmSplash",
                    "source_file": "frmSplash.frm",
                    "project_name": "BANK",
                    "purpose": "Startup splash screen.",
                    "source_loc": 100,
                    "coverage_score": 0.8,
                    "confidence_score": 0.82,
                    "db_tables": [],
                },
                {
                    "form_name": "menu",
                    "base_form_name": "menu",
                    "source_file": "menu.frm",
                    "project_name": "BANK",
                    "purpose": "Menu navigation.",
                    "source_loc": 60,
                    "coverage_score": 0.75,
                    "confidence_score": 0.8,
                    "db_tables": [],
                },
            ]
        )
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)["architect_handoff_package"]
        auth = build_component_scoped_handoff(handoff, "AuthenticationService")
        shell = build_component_scoped_handoff(handoff, "ExperienceShell")
        auth_modules = {story.get("source_module") for item in auth.get("wbs_items", []) for story in item.get("stories", [])}
        shell_modules = {story.get("source_module") for item in shell.get("wbs_items", []) for story in item.get("stories", [])}
        self.assertEqual(auth_modules, {"frmLogin"})
        self.assertTrue(shell_modules)
        self.assertNotEqual(auth_modules, shell_modules)
        self.assertTrue(all(item.get("service") == "AuthenticationService" for item in auth.get("wbs_items", [])))
        self.assertTrue(all(item.get("service") == "ExperienceShell" for item in shell.get("wbs_items", [])))

    def test_risk_detector_findings_are_normalized_for_nfr_and_brownfield_views(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"]["static_risk_detectors"] = {
            "findings": [
                {"severity": "high", "title": "no_rollback_on_multi_write"},
                {"severity": "medium", "description": "manual id generation concurrency risk"},
            ]
        }
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)["architect_handoff_package"]
        findings = handoff.get("brownfield_context", {}).get("technical_debt_policy", {}).get("risk_detector_findings", [])
        self.assertTrue(findings)
        self.assertTrue(all(str(row.get("signal", "")).strip() for row in findings))
        self.assertTrue(all(str(row.get("detail", "")).strip() for row in findings))
        security_rules = " ".join(handoff.get("nfr_constraints", {}).get("security", []))
        self.assertIn("no_rollback_on_multi_write", security_rules)

    def test_detector_findings_alias_is_normalized(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"].pop("static_risk_detectors", None)
        state["analyst_output"]["raw_artifacts"]["detector_findings"] = {
            "findings": [
                {
                    "detector_id": "manual_id_generation_concurrency_risk",
                    "severity": "medium",
                    "summary": "SELECT MAX(transactionid) is used for ID generation.",
                }
            ]
        }
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)["architect_handoff_package"]
        findings = handoff.get("brownfield_context", {}).get("technical_debt_policy", {}).get("risk_detector_findings", [])
        self.assertEqual(findings[0].get("signal"), "manual_id_generation_concurrency_risk")
        self.assertIn("SELECT MAX", findings[0].get("detail"))

    def test_nested_analyst_report_golden_flows_drive_counts_and_module_parity_supplement(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"].pop("golden_flows", None)
        state["analyst_output"]["analyst_report_v2"] = {
            "delivery_spec": {
                "testing_and_evidence": {
                    "golden_flows": [
                        {
                            "id": "GF-009",
                            "name": "BANK::frmLogin primary flow",
                            "entrypoint": "BANK::frmLogin::cmdOK_Click",
                            "expected_outcome": "Behavior matches legacy flow with equivalent side effects.",
                        }
                    ]
                }
            }
        }
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)["architect_handoff_package"]
        summary = handoff.get("brownfield_context", {}).get("source_evidence_summary", {})
        anchors = handoff.get("brownfield_context", {}).get("regression_test_anchors", [])
        self.assertEqual(summary.get("golden_flow_count"), 1)
        login_anchor = next(anchor for anchor in anchors if anchor.get("golden_flow_ref") == "GF-009")
        self.assertEqual(login_anchor.get("entry_point"), "frmLogin::cmdOK_Click")
        self.assertTrue(login_anchor.get("expected_output"))
        self.assertTrue(any(anchor.get("type") == "module_parity" and anchor.get("source_module") == "frmcustomer" for anchor in anchors))

    def test_rule_scope_component_id_resolves_source_module_and_legacy_core_service(self):
        state = self._state()
        state["analyst_output"]["legacy_code_inventory"]["source_loc_by_file"].append({"path": "BankApp1/frminterest.frm", "loc": 158})
        state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"].append(
            {
                "form_name": "frminterest",
                "base_form_name": "frminterest",
                "source_file": "frminterest.frm",
                "project_name": "BANK",
                "purpose": "Interest calculation and posting workflow.",
                "source_loc": 158,
                "coverage_score": 0.9,
                "confidence_score": 0.9,
                "db_tables": ["tbltransaction", "tblcustomers"],
            }
        )
        state["analyst_output"]["raw_artifacts"]["business_rule_catalog"] = {
            "rules": [
                {
                    "rule_id": "BR-010",
                    "statement": "Computed value rule: Interest = Balance * 1 / 100",
                    "scope": {"component_id": "BankApp1/frminterest.frm"},
                    "evidence": [
                        {"external_ref": {"ref": "BankApp1/frminterest.frm:99"}},
                    ],
                }
            ]
        }
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)["architect_handoff_package"]
        rule = next(row for row in handoff.get("brownfield_context", {}).get("business_rules", []) if row.get("rule_id") == "BR-010")
        self.assertEqual(rule.get("source_module"), "frminterest")
        self.assertEqual(rule.get("target_service"), "LegacyCoreService")

    def test_adr_statuses_stay_consistent_between_package_and_handoff(self):
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, self._state())
        package_adrs = normalized.get("architect_package", {}).get("artifacts", {}).get("architecture_decision_records", [])
        handoff_adrs = normalized.get("architect_handoff_package", {}).get("system_context", {}).get("architectural_decisions", [])
        package_status_by_id = {row.get("id"): row.get("status") for row in package_adrs}
        handoff_status_by_id = {row.get("decision_id"): row.get("status") for row in handoff_adrs}
        self.assertTrue(package_status_by_id)
        self.assertEqual(package_status_by_id, handoff_status_by_id)

    def test_fallback_module_parity_anchors_and_service_inferred_rules_unblock_auth_scoping(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"].pop("golden_flows", None)
        state["analyst_output"]["raw_artifacts"]["business_rule_catalog"] = {
            "rules": [
                {
                    "rule_id": "BR-LOGIN",
                    "statement": "Authenticate users and initiate secure application sessions.",
                }
            ]
        }
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        scoped = build_component_scoped_handoff(normalized["architect_handoff_package"], "AuthenticationService")
        report = evaluate_component_prerequisites(scoped)
        self.assertEqual(report.get("status"), "READY")
        auth_rule = scoped["brownfield_context"]["business_rules"][0]
        auth_anchor = scoped["brownfield_context"]["regression_test_anchors"][0]
        self.assertEqual(auth_rule.get("source_module"), "frmLogin")
        self.assertTrue(auth_anchor.get("golden_flow_ref"))
        self.assertTrue(auth_anchor.get("entry_point"))
        self.assertTrue(auth_anchor.get("target_endpoint"))
        self.assertTrue(scoped.get("data_ownership"))

    def test_threshold_auth_rule_and_settings_anchor_reconcile_to_real_component_semantics(self):
        state = self._state()
        state["analyst_output"]["legacy_code_inventory"]["source_loc_by_file"].append({"path": "BankApp1/Form1.frm", "loc": 120})
        for row in state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"]:
            if row.get("form_name") == "frmsettings":
                row["purpose"] = "Account setup and settings maintenance."
        state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"].append(
            {
                "form_name": "Form1",
                "base_form_name": "Form1",
                "source_file": "Form1.frm",
                "project_name": "BANK",
                "purpose": "Legacy print workflow.",
                "event_handlers": ["Form_Load", "cmdPrint_Click"],
                "source_loc": 120,
                "coverage_score": 0.7,
                "confidence_score": 0.72,
                "db_tables": [],
            }
        )
        state["analyst_output"]["raw_artifacts"]["business_rule_catalog"]["rules"].append(
            {
                "rule_id": "BR-LOCKOUT",
                "form": "Form1",
                "statement": "Threshold decision rule: IF rs.RecordCount < 1 THEN deny login and increment failed attempts.",
            }
        )
        handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)["architect_handoff_package"]
        rule = next(row for row in handoff.get("brownfield_context", {}).get("business_rules", []) if row.get("rule_id") == "BR-LOCKOUT")
        self.assertEqual(rule.get("target_service"), "AuthenticationService")
        customer = build_component_scoped_handoff(handoff, "CustomerService")
        settings_anchor = next(anchor for anchor in customer.get("brownfield_context", {}).get("regression_test_anchors", []) if anchor.get("source_module") == "frmsettings")
        self.assertEqual(settings_anchor.get("target_endpoint"), "/reference/settings")
        customer_paths = {contract.get("path") for contract in customer.get("interface_contracts", [])}
        self.assertIn(settings_anchor.get("target_endpoint"), customer_paths)
        settings_contract = next(contract for contract in customer.get("interface_contracts", []) if contract.get("path") == "/reference/settings")
        settings_operation = settings_contract.get("spec_content", {}).get("operations", [])[0]
        self.assertEqual(settings_operation.get("method"), "PUT")
        request_fields = {field.get("name") for field in settings_operation.get("request_body", {}).get("fields", [])}
        self.assertEqual(request_fields, {"settings"})

    def test_checkbalance_and_interest_anchors_gain_behavioral_outputs(self):
        state = self._state()
        state["analyst_output"]["legacy_code_inventory"]["source_loc_by_file"].extend(
            [
                {"path": "BankApp1/frmcheckbalance.frm", "loc": 210},
                {"path": "BankApp1/frminterest.frm", "loc": 160},
                {"path": "BankApp1/frmaddinterest.frm", "loc": 165},
            ]
        )
        state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"].extend(
            [
                {
                    "form_name": "frmcheckbalance",
                    "base_form_name": "frmcheckbalance",
                    "source_file": "frmcheckbalance.frm",
                    "project_name": "BANK",
                    "purpose": "Check account balance workflow.",
                    "event_handlers": ["cmdCheck_Click"],
                    "source_loc": 210,
                    "coverage_score": 0.84,
                    "confidence_score": 0.85,
                    "db_tables": ["tblaccount"],
                },
                {
                    "form_name": "frminterest",
                    "base_form_name": "frminterest",
                    "source_file": "frminterest.frm",
                    "project_name": "BANK",
                    "purpose": "Interest calculation workflow.",
                    "event_handlers": ["cmdCalculateInterest_Click"],
                    "source_loc": 160,
                    "coverage_score": 0.88,
                    "confidence_score": 0.9,
                    "db_tables": ["tbltransaction"],
                },
                {
                    "form_name": "frmaddinterest",
                    "base_form_name": "frmaddinterest",
                    "source_file": "frmaddinterest.frm",
                    "project_name": "BANK",
                    "purpose": "Interest posting workflow.",
                    "event_handlers": ["cmdCalculateInterest_Click"],
                    "source_loc": 165,
                    "coverage_score": 0.88,
                    "confidence_score": 0.9,
                    "db_tables": ["tbltransaction"],
                },
            ]
        )
        state["analyst_output"]["raw_artifacts"]["golden_flows"] = {
            "flows": [
                {
                    "flow_id": "GF-002",
                    "description": "(unmapped)::frmcheckbalance primary flow",
                    "entry_point": "frmcheckbalance::cmdCheck_Click",
                    "outcome": "(unmapped)::frmcheckbalance primary flow",
                },
                {
                    "flow_id": "GF-007",
                    "description": "(unmapped)::frminterest primary flow",
                    "entry_point": "frminterest::cmdCalculateInterest_Click",
                    "outcome": "(unmapped)::frminterest primary flow",
                },
                {
                    "flow_id": "GF-008",
                    "description": "(unmapped)::frmaddinterest secondary flow",
                    "entry_point": "frmaddinterest::cmdCalculateInterest_Click",
                    "outcome": "(unmapped)::frmaddinterest secondary flow",
                },
            ]
        }
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        anchors = {
            anchor.get("golden_flow_ref"): anchor
            for anchor in normalized.get("architect_handoff_package", {}).get("brownfield_context", {}).get("regression_test_anchors", [])
        }
        checkbalance_anchor = anchors["GF-002"]
        self.assertEqual(checkbalance_anchor.get("target_endpoint"), "/transaction/checkbalance")
        self.assertIn("current balance", str(checkbalance_anchor.get("expected_output", "")).lower())
        checkbalance_fixture = json.loads(checkbalance_anchor.get("data_fixture"))
        self.assertEqual(checkbalance_fixture.get("request", {}).get("method"), "GET")

        for flow_id in ("GF-007", "GF-008"):
            anchor = anchors[flow_id]
            self.assertEqual(anchor.get("target_endpoint"), "/legacycore/addinterest")
            self.assertIn("interest is calculated", str(anchor.get("expected_output", "")).lower())
            fixture = json.loads(anchor.get("data_fixture"))
            self.assertIn("currentBalance", fixture.get("seed", {}))


class ArchitectHandoffApiTest(unittest.TestCase):
    def setUp(self) -> None:
        self.original_manager = server.MANAGER

    def tearDown(self) -> None:
        server.MANAGER = self.original_manager

    def _handoff(self) -> dict:
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, ArchitectHandoffPackageTest()._state())
        return normalized.get("architect_handoff_package", {})

    def test_get_architect_handoff_from_persisted_stage_snapshot(self):
        handoff = self._handoff()

        class _FakeStore:
            def load_stage_snapshot(self, run_id, stage):
                if run_id != "run_handoff" or stage != 2:
                    return None
                return {"output": {"architect_handoff_package": handoff}}

        class _FakeManager:
            store = _FakeStore()

            def _hydrate_record(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

            def get_run(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

        server.MANAGER = _FakeManager()
        response = asyncio.run(
            server.api_get_run_architect_handoff(_FakeRequest(path_params={"run_id": "run_handoff"}))
        )
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["architect_handoff_package"]["artifact_type"], "architect_handoff_package_v1")

    def test_get_architect_handoff_from_real_snapshot_result_shape(self):
        handoff = self._handoff()

        class _FakeStore:
            def load_stage_snapshot(self, run_id, stage):
                if run_id != "run_handoff" or stage != 2:
                    return None
                return {
                    "run_id": run_id,
                    "stage": stage,
                    "result": {
                        "stage": 2,
                        "status": "success",
                        "summary": "Architect package generated",
                        "output": {"architect_handoff_package": handoff},
                        "logs": ["architect complete"],
                    },
                    "pipeline_state": {},
                    "stage_status": {"2": "success"},
                    "progress_logs": ["[ts] architect complete"],
                }

        class _FakeManager:
            store = _FakeStore()

            def _hydrate_record(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

            def get_run(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

        server.MANAGER = _FakeManager()
        response = asyncio.run(
            server.api_get_run_architect_handoff(_FakeRequest(path_params={"run_id": "run_handoff"}))
        )
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["architect_handoff_package"]["artifact_type"], "architect_handoff_package_v1")

    def test_get_architect_package_from_persisted_stage_snapshot(self):
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, ArchitectHandoffPackageTest()._state())
        architect_package = normalized.get("architect_package", {})

        class _FakeStore:
            def load_stage_snapshot(self, run_id, stage):
                if run_id != "run_handoff" or stage != 2:
                    return None
                return {"output": {"architect_package": architect_package}}

        class _FakeManager:
            store = _FakeStore()

            def _hydrate_record(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

            def get_run(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

        server.MANAGER = _FakeManager()
        response = asyncio.run(
            server.api_get_run_architect_package(_FakeRequest(path_params={"run_id": "run_handoff"}))
        )
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["architect_package"]["package_meta"]["artifact_count"], 7)

    def test_get_component_scoped_handoff_from_persisted_stage_snapshot(self):
        handoff = self._handoff()
        component_name = handoff["component_specs"][0]["component_name"]

        class _FakeStore:
            def load_stage_snapshot(self, run_id, stage):
                if run_id != "run_handoff" or stage != 2:
                    return None
                return {"output": {"architect_handoff_package": handoff}}

        class _FakeManager:
            store = _FakeStore()

            def _hydrate_record(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

            def get_run(self, run_id):
                return {"run_id": run_id, "pipeline_state": {}}

        server.MANAGER = _FakeManager()
        response = asyncio.run(
            server.api_get_run_architect_handoff_component(
                _FakeRequest(
                    path_params={"run_id": "run_handoff"},
                    query_params={"component": component_name},
                )
            )
        )
        self.assertEqual(response.status_code, 200)
        payload = json.loads(response.body)
        self.assertTrue(payload["ok"])
        self.assertEqual(payload["component_handoff"]["artifact_type"], "component_scoped_handoff_v1")
        self.assertEqual(payload["component_handoff"]["component_spec"]["component_name"], component_name)


class ArchitectHandoffPipelinePromotionTest(unittest.TestCase):
    def test_stage_two_promotes_handoff_to_top_level_pipeline_state(self):
        handoff = ArchitectHandoffPackageTest()._state()
        architect_handoff = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, handoff).get(
            "architect_handoff_package",
            {},
        )

        fake_result = AgentResult(
            agent_name="Architect Agent",
            stage=2,
            status="success",
            summary="Architect package generated",
            output={"architect_handoff_package": architect_handoff},
            logs=["architect complete"],
            raw_response="{}",
            tokens_used=10,
            latency_ms=5.0,
        )

        initial_state = pipeline.make_initial_state("Modernize legacy VB6 application")
        initial_state["analyst_output"] = handoff["analyst_output"]
        initial_state["sil_ready"] = True
        initial_state["system_context_model"] = {"ready": True}
        initial_state["convention_profile"] = {"naming": "vb6"}
        initial_state["health_assessment"] = {"status": "ok"}
        initial_state["context_bundle"] = {"artifact_type": "context_bundle_v1", "bundle_id": "bundle-1"}
        initial_state["context_contracts"] = {"artifact_type": "context_contracts_v1"}
        initial_state["context_contract_validation"] = {"status": "PASS"}
        initial_state["context_vault_ref"] = {"version_id": "ctx-1"}

        with patch.object(pipeline, "LLMClient", return_value=Mock()):
            with patch.object(pipeline.ArchitectAgent, "run", return_value=fake_result):
                updated = pipeline.run_single_stage(Mock(), initial_state, 1)

        self.assertIn("architect_output", updated)
        self.assertIn("architect_handoff_package", updated)
        self.assertEqual(
            updated["architect_handoff_package"].get("artifact_type"),
            "architect_handoff_package_v1",
        )
        self.assertEqual(
            updated["architect_output"].get("architect_handoff_package", {}).get("artifact_type"),
            "architect_handoff_package_v1",
        )
