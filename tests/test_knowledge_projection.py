import tempfile
import unittest
from pathlib import Path

from utils.knowledge_projection import build_knowledge_projection
from utils.knowledge_queries import KnowledgeQueries
from utils.knowledge_store_sqlite import SqliteKnowledgeStore


class KnowledgeProjectionTest(unittest.TestCase):
    def test_projection_and_queries_from_minimal_analyst_output(self):
        analyst_output = {
            "run_id": "run_test_001",
            "source_mode": "repo_scan",
            "context_reference": {
                "repo": "https://github.com/example/repo",
                "branch": "main",
                "commit_sha": "abc123",
            },
            "raw_artifacts": {
                "legacy_inventory": {
                    "summary": {
                        "counts": {
                            "projects": 1,
                            "forms_or_screens": 1,
                            "source_loc_total": 120,
                        }
                    }
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
                "dependency_inventory": {
                    "dependencies": [
                        {
                            "form": "frmCustomer",
                            "target_form": "frmCustomer",
                            "project": "BANK",
                            "type": "self",
                        }
                    ]
                },
                "traceability_coverage": {
                    "rows": [
                        {"form": "frmCustomer", "project": "BANK", "score": 75, "status": "partial"}
                    ]
                },
            },
            "analyst_report_v2": {
                "decision_brief": {
                    "blocking_decisions": [
                        {
                            "id": "DEC-COMPLIANCE-001",
                            "topic": "Compliance linkage",
                            "status": "open",
                            "description": "Need confirmation of regulatory mapping.",
                        }
                    ]
                }
            },
        }
        projection = build_knowledge_projection(
            run_id="run_test_001",
            analyst_output=analyst_output,
            run_context_bundle={"integration_context": {"source_mode": "repo_scan"}},
        )
        with tempfile.TemporaryDirectory() as tmpdir:
            store = SqliteKnowledgeStore(Path(tmpdir) / "knowledge.sqlite")
            store.save_projection(projection)
            queries = KnowledgeQueries(store, "run_test_001")

            module_ctx = queries.get_module_context("frmCustomer")
            self.assertEqual(module_ctx["module"]["name"], "frmCustomer")
            self.assertEqual(len(module_ctx["functions"]), 1)
            self.assertEqual(len(module_ctx["business_rules"]), 1)
            self.assertEqual(len(module_ctx["risk_flags"]), 1)

            rule_ctx = queries.get_rule_context("BR-001")
            self.assertEqual(rule_ctx["rule"]["name"], "BR-001")

            compliance = queries.get_compliance_gaps()
            self.assertEqual(compliance["count"], 1)

            search = queries.search_concepts("customer profiles", limit=5)
            self.assertGreaterEqual(len(search["hits"]), 1)


if __name__ == "__main__":
    unittest.main()
