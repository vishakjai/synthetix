import unittest

from scripts.run_vb6_analyst_markdown import build_full_markdown


class PhpMarkdownSummaryTest(unittest.TestCase):
    def test_php_markdown_uses_php_inventory_language(self):
        output = {
            "source_language": "PHP",
            "source_target_modernization_profile": {
                "source": {"language": "PHP", "framework": "custom_php"},
                "target": {"language": "TypeScript", "framework": "NestJS"},
            },
            "legacy_skill_profile": {"selected_skill_id": "php_legacy", "version": "1.0.0", "confidence": 0.9},
            "requirements_pack": {
                "legacy_code_inventory": {
                    "summary": "Detected PHP legacy application.",
                    "modernization_readiness": {"score": 52, "risk_tier": "medium"},
                    "php_analysis": {
                        "route_inventory": {"route_count": 40},
                        "controller_inventory": {"controller_count": 12},
                        "template_inventory": {"template_count": 18},
                        "session_state_inventory": {"session_key_count": 5},
                        "authz_authn_inventory": {"auth_touchpoint_count": 7},
                        "background_job_inventory": {"job_count": 1},
                        "file_io_inventory": {"upload_file_count": 1, "export_file_count": 2},
                    },
                    "source_loc_total": 11647,
                    "source_files_scanned": 80,
                    "php_dependency_count": 9,
                    "database_tables": ["users", "orders"],
                }
            },
            "raw_artifacts": {
                "repo_landscape_v1": {
                    "datastore_signals": [{"datastore": "mysql"}],
                    "dependency_footprint": {"composer_package_count": 9},
                }
            },
        }
        markdown = build_full_markdown(output, mode="full")
        self.assertIn("12 controllers, 40 routes, 18 templates, 9 dependencies", markdown)
        self.assertIn("11647 total LOC across 80 files", markdown)
        self.assertNotIn("forms/usercontrols", markdown)
        self.assertNotIn("0 form LOC, 0 module LOC, 0 class LOC", markdown)
        self.assertIn("Route/controller parity anchors", markdown)


if __name__ == "__main__":
    unittest.main()
