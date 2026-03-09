import tempfile
import unittest
from pathlib import Path

from utils.knowledge_projection import build_knowledge_projection
from utils.knowledge_proposals import KnowledgeProposalService, KnowledgeProposalStore
from utils.knowledge_queries import KnowledgeQueries
from utils.knowledge_store_sqlite import SqliteKnowledgeStore


class KnowledgeProposalTest(unittest.TestCase):
    def setUp(self):
        self.analyst_output = {
            "run_id": "run_test_proposals",
            "source_mode": "repo_scan",
            "context_reference": {"repo": "https://github.com/example/repo", "branch": "main", "commit_sha": "abc123"},
            "raw_artifacts": {
                "form_dossier": {
                    "dossiers": [
                        {
                            "form_name": "frmCustomer",
                            "project": "BANK",
                            "source_file": "frmCustomer.frm",
                            "loc": 120,
                            "type": "Child",
                            "purpose": "Maintain customer profiles",
                            "evidence": "form_dossier:1",
                            "coverage_score": 75,
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

    def _make_service(self):
        projection = build_knowledge_projection(
            run_id="run_test_proposals",
            analyst_output=self.analyst_output,
            run_context_bundle={"integration_context": {"source_mode": "repo_scan"}},
        )
        tmpdir = tempfile.TemporaryDirectory()
        self.addCleanup(tmpdir.cleanup)
        root = Path(tmpdir.name)
        store = SqliteKnowledgeStore(root / "knowledge.sqlite")
        store.save_projection(projection)
        proposal_store = KnowledgeProposalStore(root / "proposals.json")
        return KnowledgeProposalService(KnowledgeQueries(store, "run_test_proposals"), proposal_store)

    def test_create_rule_update_proposal(self):
        service = self._make_service()
        proposal = service.create_from_message("Update BR-001 to state that customer ID is mandatory before save.", actor="tester@example.com")
        self.assertEqual(proposal["proposal_type"], "rule_update")
        self.assertEqual(proposal["status"], "pending")
        self.assertEqual(proposal["target"]["name"], "BR-001")
        self.assertIn("BRD", proposal["impact"]["impacted_documents"])

    def test_review_proposal(self):
        service = self._make_service()
        proposal = service.create_from_message("Mark RISK-001 as medium risk pending SME review.", actor="tester@example.com")
        reviewed = service.review(proposal["id"], decision="approve", rationale="Looks correct", actor="reviewer@example.com")
        self.assertEqual(reviewed["status"], "approved")
        self.assertEqual(reviewed["reviewed_by"], "reviewer@example.com")


if __name__ == "__main__":
    unittest.main()
