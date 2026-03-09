import tempfile
import unittest
from pathlib import Path

from agents.interaction_agent import InteractionAgent
from utils.knowledge_projection import build_knowledge_projection
from utils.knowledge_queries import KnowledgeQueries
from utils.knowledge_store_sqlite import SqliteKnowledgeStore


class InteractionAgentTest(unittest.TestCase):
    def setUp(self):
        self.analyst_output = {
            "run_id": "run_test_interact",
            "source_mode": "repo_scan",
            "context_reference": {
                "repo": "https://github.com/example/repo",
                "branch": "main",
                "commit_sha": "abc123",
            },
            "raw_artifacts": {
                "legacy_inventory": {
                    "summary": {"counts": {"projects": 1, "forms_or_screens": 1, "source_loc_total": 120}}
                },
                "form_dossier": {
                    "dossiers": [
                        {
                            "form_name": "frmCustomer",
                            "project": "BANK",
                            "source_file": "frmCustomer.frm",
                            "loc": 120,
                            "type": "Child",
                            "purpose": "Maintain customer profiles",
                            "db_tables": ["tblCustomers"],
                            "evidence": "form_dossier:1",
                            "coverage_score": 75,
                        }
                    ]
                },
                "procedure_summary": {
                    "procedures": [
                        {
                            "procedure": "SaveCustomer",
                            "form": "frmCustomer",
                            "project": "BANK",
                            "summary": "Validates and saves customer data",
                            "line": 42,
                            "source_file": "frmCustomer.frm",
                        }
                    ]
                },
                "business_rule_catalog": {
                    "rules": [
                        {
                            "rule_id": "BR-001",
                            "description": "Customer ID is required before save.",
                            "type": "VALIDATION",
                            "form": "frmCustomer",
                            "scope": {"project": "BANK"},
                            "confidence": 0.9,
                        }
                    ]
                },
                "risk_register": {
                    "risks": [
                        {
                            "risk_id": "RISK-001",
                            "severity": "high",
                            "category": "DATA",
                            "description": "Customer save has no rollback.",
                            "form": "frmCustomer",
                            "project": "BANK",
                        }
                    ]
                },
            },
            "analyst_report_v2": {},
        }

    def _make_agent(self):
        projection = build_knowledge_projection(
            run_id="run_test_interact",
            analyst_output=self.analyst_output,
            run_context_bundle={"integration_context": {"source_mode": "repo_scan"}},
        )
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        store = SqliteKnowledgeStore(Path(tmpdir.name) / "knowledge.sqlite")
        store.save_projection(projection)
        return InteractionAgent(KnowledgeQueries(store, "run_test_interact"))

    def test_module_question_returns_grounded_answer(self):
        agent = self._make_agent()
        response = agent.respond("What does frmCustomer do?")
        self.assertEqual(response["topic"], "module")
        self.assertIn("frmCustomer", response["answer"])
        self.assertGreater(len(response["provenance"]), 0)

    def test_rule_question_returns_rule_context(self):
        agent = self._make_agent()
        response = agent.respond("Where does BR-001 come from?")
        self.assertEqual(response["topic"], "rule")
        self.assertIn("BR-001", response["answer"])

    def test_loc_question_returns_estate_metrics_not_search_hits(self):
        agent = self._make_agent()
        response = agent.respond("How many lines of code exist in the legacy application?")
        self.assertEqual(response["topic"], "metrics")
        self.assertIn("120", response["answer"])
        self.assertNotIn("Top matches", response["answer"])

    def test_inventory_question_lists_forms_instead_of_semantic_hits(self):
        agent = self._make_agent()
        response = agent.respond("Can you list the forms in the application?")
        self.assertEqual(response["topic"], "inventory")
        self.assertIn("I found 1 forms.", response["answer"])
        self.assertIn("frmCustomer", response["answer"])
        self.assertNotIn("Top matches", response["answer"])


if __name__ == "__main__":
    unittest.main()
