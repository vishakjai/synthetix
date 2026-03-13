import unittest
from unittest.mock import Mock

from agents.architect import ArchitectAgent
from utils.developer_dispatch import build_component_scoped_handoff
from utils.developer_prereqs import evaluate_component_prerequisites


class ArchitectPackageTest(unittest.TestCase):
    def _state(self):
        return {
            "modernization_language": "C#",
            "database_target": "PostgreSQL",
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
                            {
                                "rule_id": "BR-001",
                                "form": "frmcustomer",
                                "statement": "Customer records require a unique account number.",
                            },
                            {
                                "rule_id": "BR-002",
                                "form": "frmdeposit",
                                "statement": "Deposits must update balance and ledger atomically.",
                            },
                        ]
                    },
                    "sql_catalog": {
                        "statements": [
                            {"form": "frmdeposit", "tables": ["transactions", "accounts"]},
                            {"form": "frmwithdraw", "tables": ["transactions", "accounts"]},
                            {"form": "frmcustomer", "tables": ["customers", "accounts"]},
                        ]
                    },
                    "dependency_inventory": {
                        "dependencies": [
                            {"name": "MSCOMCT2.OCX"},
                            {"name": "DBGRID32.OCX"},
                        ]
                    },
                },
            },
        }

    def test_architect_normalization_emits_package_and_compatibility_fields(self):
        agent = ArchitectAgent(Mock())
        parsed = {
            "architecture_name": "",
            "pattern": "",
            "overview": "",
            "services": [],
            "legacy_system": {
                "current_logic_summary": "Legacy VB6 workflow.",
                "key_logic_steps": ["Login", "Customer menu", "Transaction processing"],
            },
        }
        normalized = agent._normalize_output(parsed, self._state())
        package = normalized.get("architect_package", {})
        artifacts = package.get("artifacts", {})
        self.assertEqual(package.get("package_meta", {}).get("artifact_count"), 7)
        self.assertEqual(package.get("package_meta", {}).get("hld_document_count"), 2)
        self.assertTrue(package.get("package_meta", {}).get("artifact_hashes"))
        self.assertTrue(artifacts.get("architecture_decision_records"))
        self.assertTrue(artifacts.get("traceability_matrix", {}).get("mappings"))
        self.assertTrue(artifacts.get("data_ownership_matrix", {}).get("entities"))
        self.assertTrue(package.get("estimation_handoff", {}).get("services"))
        self.assertTrue(package.get("human_review_queue"))
        self.assertEqual(package.get("hld_documents", {}).get("generation_status"), "generated")
        self.assertTrue(normalized.get("services"))
        self.assertEqual(normalized.get("pattern"), "modular-monolith")
        self.assertIn("Target Operational Database", normalized.get("target_system_diagram_mermaid", ""))
        self.assertIn("AuthenticationService", normalized.get("target_system_diagram_mermaid", ""))
        self.assertIn("CustomerService", normalized.get("target_system_diagram_mermaid", ""))

    def test_traceability_covers_all_source_modules(self):
        agent = ArchitectAgent(Mock())
        package = agent._build_architect_package(self._state(), {})
        coverage = package.get("artifacts", {}).get("traceability_matrix", {}).get("coverage", {})
        self.assertEqual(coverage.get("total_source_modules"), 5)
        self.assertEqual(coverage.get("mapped_confident") + coverage.get("mapped_review") + coverage.get("mapped_unmapped"), 5)
        adrs = package.get("artifacts", {}).get("architecture_decision_records", [])
        self.assertTrue(all(adr.get("alternatives_considered") for adr in adrs))

    def test_data_ownership_dedupes_case_only_table_variants(self):
        state = self._state()
        state["analyst_output"]["raw_artifacts"]["sql_catalog"] = {
            "statements": [
                {"form": "frmdeposit", "kind": "update", "tables": ["tblTransaction"], "data_mutations": ["tblTransaction"]},
                {"form": "frmwithdraw", "kind": "insert", "tables": ["tbltransaction"], "data_mutations": ["tbltransaction"]},
                {"form": "frmcustomer", "kind": "update", "tables": ["tblCustomers"], "data_mutations": ["tblCustomers"]},
                {"form": "frmcustomer", "kind": "select", "tables": ["tblcustomers"]},
            ]
        }
        package = ArchitectAgent(Mock())._build_architect_package(state, {})
        entities = package.get("artifacts", {}).get("data_ownership_matrix", {}).get("entities", [])
        normalized_names = [str(row.get("name", "")).lower() for row in entities]
        self.assertEqual(sum(name in {"tbltransaction", "transaction"} for name in normalized_names), 1)
        self.assertEqual(sum(name in {"tblcustomers", "customers"} for name in normalized_names), 1)
        transaction_row = next(row for row in entities if str(row.get("name", "")).lower() in {"tbltransaction", "transaction"})
        self.assertEqual(transaction_row.get("owning_service"), "TransactionService")
        self.assertIn("legacy module", str(transaction_row.get("migration_notes", "")).lower())

    def test_top_level_services_include_auth_credential_store_and_accepted_adrs(self):
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, self._state())
        services = {row.get("name"): row for row in normalized.get("services", [])}
        self.assertEqual(services["AuthenticationService"].get("database"), "PostgreSQL credential store")
        adrs = normalized.get("architect_package", {}).get("artifacts", {}).get("architecture_decision_records", [])
        self.assertTrue(any(str(row.get("status", "")).lower() == "accepted" for row in adrs))

    def test_data_ownership_prefers_domain_owner_over_residual_writer(self):
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
                "db_tables": ["tbltransaction"],
            }
        )
        state["analyst_output"]["raw_artifacts"]["sql_catalog"] = {
            "statements": [
                {
                    "sql_id": "sql:interest",
                    "kind": "insert",
                    "tables": ["tbltransaction"],
                    "data_mutations": ["tbltransaction"],
                    "usage_sites": [{"external_ref": {"ref": "(unmapped)::frminterest::cmdCalculateInterest_Click"}}],
                },
                {
                    "sql_id": "sql:settings",
                    "kind": "update",
                    "tables": ["tblaccount"],
                    "data_mutations": ["tblaccount"],
                    "usage_sites": [{"external_ref": {"ref": "BANK::frmsettings::cmdsave_Click"}}],
                },
            ]
        }
        package = ArchitectAgent(Mock())._build_architect_package(state, {})
        entities = {str(row.get("name", "")).lower(): row for row in package.get("artifacts", {}).get("data_ownership_matrix", {}).get("entities", [])}
        self.assertEqual(entities["tbltransaction"].get("owning_service"), "TransactionService")
        self.assertEqual(entities["tblaccount"].get("owning_service"), "CustomerService")

    def test_legacy_diagram_is_shell_fanout_not_linear_navigation(self):
        normalized = ArchitectAgent(Mock())._normalize_output(
            {
                "legacy_system": {
                    "current_logic_summary": "Legacy VB6 workflow.",
                    "key_logic_steps": ["frmcustomer", "frmdeposit", "frmwithdraw", "frmADDINTEREST", "closeacount"],
                }
            },
            self._state(),
        )
        diagram = normalized.get("legacy_system", {}).get("current_system_diagram_mermaid", "")
        self.assertIn('shell_0["', diagram)
        self.assertIn("auth_0 --> shell_0[", diagram)
        self.assertIn("shell_0 -->", diagram)
        self.assertNotIn("frmcustomer\"] -->", diagram)

    def test_settings_module_collapses_into_customer_and_legacy_core_is_split(self):
        state = self._state()
        state["analyst_output"]["legacy_code_inventory"]["source_loc_by_file"].extend(
            [
                {"path": "BankApp1/frmWithinDate.frm", "loc": 200},
                {"path": "BankApp1/frmdaily.frm", "loc": 180},
                {"path": "BankApp1/frminterest.frm", "loc": 160},
                {"path": "BankApp1/Form1.frm", "loc": 150},
                {"path": "BankApp1/Mdl.bas", "loc": 120},
            ]
        )
        state["analyst_output"]["legacy_code_inventory"]["bas_module_summary"] = {"modules": ["BankApp1/Mdl.bas"]}
        for row in state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"]:
            if row.get("form_name") == "frmsettings":
                row["purpose"] = "Account type maintenance and account setup workflow."
        state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"].extend(
            [
                {
                    "form_name": "frmWithinDate",
                    "base_form_name": "frmWithinDate",
                    "source_file": "frmWithinDate.frm",
                    "project_name": "BANK",
                    "purpose": "Date-bound reporting and print workflow.",
                    "controls": ["MSComCtl2.DTPicker:DTFrom", "VB.CommandButton:cmdPrint"],
                    "event_handlers": ["Form_Load", "cmdPrint_Click"],
                    "source_loc": 200,
                    "coverage_score": 0.82,
                    "confidence_score": 0.84,
                    "db_tables": [],
                },
                {
                    "form_name": "frmdaily",
                    "base_form_name": "frmdaily",
                    "source_file": "frmdaily.frm",
                    "project_name": "BANK",
                    "purpose": "Daily report generation workflow.",
                    "controls": ["VB.CommandButton:cmdGenerate"],
                    "event_handlers": ["cmdGenerate_Click"],
                    "source_loc": 180,
                    "coverage_score": 0.8,
                    "confidence_score": 0.82,
                    "db_tables": [],
                },
                {
                    "form_name": "frminterest",
                    "base_form_name": "frminterest",
                    "source_file": "frminterest.frm",
                    "project_name": "BANK",
                    "purpose": "Interest calculation and posting workflow.",
                    "controls": ["VB.CommandButton:cmdCalculateInterest"],
                    "event_handlers": ["cmdCalculateInterest_Click"],
                    "source_loc": 160,
                    "coverage_score": 0.88,
                    "confidence_score": 0.9,
                    "db_tables": ["tbltransaction"],
                },
                {
                    "form_name": "Form1",
                    "base_form_name": "Form1",
                    "source_file": "Form1.frm",
                    "project_name": "BANK",
                    "purpose": "Legacy print workflow.",
                    "controls": ["MSComCtl2.DTPicker:DTPicker1", "VB.CommandButton:cmdPrint"],
                    "event_handlers": ["Form_Load", "cmdPrint_Click"],
                    "source_loc": 150,
                    "coverage_score": 0.78,
                    "confidence_score": 0.8,
                    "db_tables": [],
                },
            ]
        )
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        component_specs = {row.get("component_name"): row for row in normalized.get("architect_handoff_package", {}).get("component_specs", [])}
        customer_modules = [row.get("source_module") for row in component_specs["CustomerService"].get("module_structure", [])]
        legacy_modules = [row.get("source_module") for row in component_specs["LegacyCoreService"].get("module_structure", [])]
        reporting_modules = [row.get("source_module") for row in component_specs["ReportingService"].get("module_structure", [])]
        self.assertIn("frmsettings", customer_modules)
        self.assertNotIn("ReferenceDataService", component_specs)
        self.assertLess(len(legacy_modules), 5)
        self.assertTrue(any(module in reporting_modules for module in ["frmWithinDate", "Form1"]))

    def test_explicit_approved_services_unblock_customer_transaction_and_legacy_core(self):
        state = self._state()
        state["integration_context"] = {
            "architect_controls": {
                "approved_services": ["CustomerService", "TransactionService"],
            }
        }
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        handoff = normalized.get("architect_handoff_package", {})
        statuses = {
            row.get("decision_id"): row.get("status")
            for row in handoff.get("system_context", {}).get("architectural_decisions", [])
        }
        self.assertTrue(any(status == "Accepted" for status in statuses.values()))
        for component_name in ("CustomerService", "TransactionService"):
            scoped = build_component_scoped_handoff(handoff, component_name)
            report = evaluate_component_prerequisites(scoped)
            self.assertEqual(report.get("status"), "READY", component_name)

    def test_service_specific_wbs_refs_and_deduped_contract_surfaces(self):
        state = self._state()
        state["analyst_output"]["legacy_code_inventory"]["source_loc_by_file"].extend(
            [
                {"path": "BankApp1/frminterest.frm", "loc": 160},
                {"path": "BankApp1/frmaddinterest.frm", "loc": 165},
                {"path": "BankApp1/Mdi.frm", "loc": 180},
                {"path": "BankApp1/frmSplash.frm", "loc": 100},
                {"path": "BankApp1/menu.frm", "loc": 60},
            ]
        )
        state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"].extend(
            [
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
        specs = {row.get("component_name"): row for row in handoff.get("component_specs", [])}
        self.assertEqual(specs["AuthenticationService"].get("wbs_refs"), ["WBS-PHASE-01-authenticationservice"])
        self.assertEqual(specs["ExperienceShell"].get("wbs_refs"), ["WBS-PHASE-01-experienceshell"])
        legacy_contracts = [
            contract for contract in handoff.get("interface_contracts", [])
            if contract.get("owner_component") == "LegacyCoreService"
        ]
        surfaces = {(contract.get("method"), contract.get("path")) for contract in legacy_contracts}
        self.assertEqual(len(legacy_contracts), len(surfaces))

    def test_contract_shapes_cover_checkbalance_settings_and_interest_inputs(self):
        state = self._state()
        for row in state["analyst_output"]["raw_artifacts"]["form_dossier"]["dossiers"]:
            if row.get("form_name") == "frmsettings":
                row["purpose"] = "Account setup and settings maintenance."
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
        normalized = ArchitectAgent(Mock())._normalize_output({"legacy_system": {}}, state)
        api_services = {
            row.get("service"): row.get("operations", [])
            for row in normalized.get("architect_package", {}).get("artifacts", {}).get("api_contract_sketches", {}).get("services", [])
        }

        transaction_ops = {(op.get("method"), op.get("path")): op for op in api_services["TransactionService"]}
        checkbalance = transaction_ops[("GET", "/transaction/checkbalance")]
        self.assertEqual(checkbalance.get("request_body", {}).get("fields", [])[0].get("name"), "accountNo")
        checkbalance_response = {field.get("name") for field in checkbalance.get("response_body", {}).get("fields", [])}
        self.assertTrue({"accountNo", "balance", "currency"}.issubset(checkbalance_response))

        customer_ops = {(op.get("method"), op.get("path")): op for op in api_services["CustomerService"]}
        settings = customer_ops[("PUT", "/reference/settings")]
        self.assertEqual(settings.get("request_body", {}).get("fields", [])[0].get("name"), "settings")

        legacy_ops = {(op.get("method"), op.get("path")): op for op in api_services["LegacyCoreService"]}
        addinterest = legacy_ops[("POST", "/legacycore/addinterest")]
        addinterest_request = {field.get("name") for field in addinterest.get("request_body", {}).get("fields", [])}
        self.assertTrue({"accountNo", "currentBalance"}.issubset(addinterest_request))
