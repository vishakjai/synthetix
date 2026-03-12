import unittest
from unittest.mock import Mock

from agents.architect import ArchitectAgent
from agents.developer import _handoff_dispatch_record
from utils.developer_dispatch import build_component_scoped_handoff


class DeveloperDispatchTest(unittest.TestCase):
    def _state(self):
        return {
            "run_id": "run_developer_dispatch",
            "use_case": "code_modernization",
            "modernization_language": "C#",
            "database_target": "PostgreSQL",
            "repo_url": "https://github.com/example/bank-vb6",
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

    def test_component_scoped_handoff_is_filtered_to_selected_component(self):
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
        handoff = normalized.get("architect_handoff_package", {})
        scoped = build_component_scoped_handoff(handoff, "TransactionService")

        self.assertEqual(scoped.get("artifact_type"), "component_scoped_handoff_v1")
        self.assertEqual(scoped.get("component_name"), "TransactionService")
        self.assertEqual(scoped.get("component_spec", {}).get("component_name"), "TransactionService")
        self.assertTrue(scoped.get("interface_contracts"))
        self.assertTrue(scoped.get("wbs_items"))
        self.assertTrue(scoped.get("brownfield_context", {}).get("regression_test_anchors"))
        self.assertTrue(scoped.get("system_context", {}).get("architectural_decisions"))
        self.assertTrue(scoped.get("analyst_evidence", {}).get("business_rules"))
        self.assertTrue(scoped.get("analyst_evidence", {}).get("connection_patterns"))
        self.assertTrue(scoped.get("analyst_evidence", {}).get("risk_detector_findings"))
        self.assertTrue(scoped.get("analyst_evidence", {}).get("data_entities"))

        contract_owners = {row.get("owner_component") for row in scoped.get("interface_contracts", [])}
        self.assertEqual(contract_owners, {"TransactionService"})

        trace_refs = set(scoped.get("component_spec", {}).get("traceability_refs", []))
        for row in scoped.get("wbs_items", []):
            self.assertEqual(row.get("service"), "TransactionService")
        self.assertTrue(any("frmdeposit" in str(anchor.get("description", "")).lower() or "frmwithdraw" in str(anchor.get("description", "")).lower() for anchor in scoped.get("brownfield_context", {}).get("regression_test_anchors", [])))
        self.assertTrue(trace_refs)

    def test_dispatch_record_summarizes_scoped_handoff(self):
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
        handoff = normalized.get("architect_handoff_package", {})
        scoped = build_component_scoped_handoff(handoff, "TransactionService")
        dispatch = _handoff_dispatch_record(scoped)

        self.assertEqual(dispatch.get("component_name"), "TransactionService")
        self.assertEqual(dispatch.get("artifact_type"), "component_scoped_handoff_v1")
        self.assertTrue(dispatch.get("interface_contract_ids"))
        self.assertTrue(dispatch.get("wbs_ids"))
        self.assertTrue(dispatch.get("adr_ids"))
        self.assertGreaterEqual(dispatch.get("business_rule_count", 0), 1)
        self.assertGreaterEqual(dispatch.get("regression_anchor_count", 0), 1)
