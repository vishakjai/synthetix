import unittest
from unittest.mock import Mock

from agents.architect import ArchitectAgent


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
        self.assertTrue(artifacts.get("architecture_decision_records"))
        self.assertTrue(artifacts.get("traceability_matrix", {}).get("mappings"))
        self.assertTrue(artifacts.get("data_ownership_matrix", {}).get("entities"))
        self.assertTrue(package.get("estimation_handoff", {}).get("services"))
        self.assertTrue(package.get("human_review_queue"))
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
