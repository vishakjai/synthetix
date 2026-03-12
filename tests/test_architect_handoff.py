import asyncio
import json
import unittest
from unittest.mock import Mock, patch

from agents.architect import ArchitectAgent
from agents.base import AgentResult
import orchestrator.pipeline as pipeline
from utils.architect_handoff import validate_architect_handoff_json
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
        first_contract = handoff.get("interface_contracts", [])[0]
        self.assertIn("request_body", first_contract.get("spec_content", {}))
        self.assertIn("auth", first_contract.get("spec_content", {}))

    def test_component_specs_are_traced_to_contracts_and_wbs(self):
        agent = ArchitectAgent(Mock())
        normalized = agent._normalize_output({"legacy_system": {}}, self._state())
        handoff = normalized.get("architect_handoff_package", {})
        component_specs = handoff.get("component_specs", [])
        self.assertTrue(component_specs)
        self.assertTrue(all(spec.get("interface_refs") for spec in component_specs))
        self.assertTrue(all(spec.get("wbs_refs") for spec in component_specs))

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
