import unittest

from utils.analyst_report import build_analyst_report_v2
from utils.php_framework_detection import detect_php_framework_profile


class PhpSummaryPathTest(unittest.TestCase):
    def test_php_framework_profile_reports_composer_packages(self):
        entries = [
            {"path": "composer.json", "type": "blob"},
            {"path": "app/controllers/AuthController.php", "type": "blob"},
            {"path": "views/login.phtml", "type": "blob"},
        ]
        file_contents = {
            "composer.json": '{"require":{"monolog/monolog":"^2.0","symfony/http-foundation":"^6.0"}}',
            "app/controllers/AuthController.php": "<?php session_start();",
        }
        profile = detect_php_framework_profile(entries=entries, file_contents=file_contents)
        self.assertTrue(profile["uses_composer"])
        self.assertEqual(profile["composer_package_count"], 2)
        self.assertIn("monolog/monolog", profile["composer_packages"])

    def test_php_framework_profile_falls_back_to_vendor_package_manifests(self):
        entries = [
            {"path": "vendor/sendgrid/sendgrid/composer.json", "type": "blob"},
            {"path": "vendor/sendgrid/php-http-client/composer.json", "type": "blob"},
            {"path": "Controller/PortalCustomerController.php", "type": "blob"},
        ]
        profile = detect_php_framework_profile(entries=entries, file_contents={})
        self.assertTrue(profile["uses_composer"])
        self.assertEqual(profile["composer_package_count"], 2)
        self.assertIn("sendgrid/sendgrid", profile["composer_packages"])
        self.assertIn("sendgrid/php-http-client", profile["composer_packages"])

    def test_php_analyst_report_uses_php_inventory_and_decisions(self):
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
                    "datastore_signals": [{"datastore": "mysql"}, {"datastore": "sqlserver"}, {"datastore": "mq"}],
                    "dependency_footprint": {"composer_package_count": 9},
                }
            },
        }
        report = build_analyst_report_v2(output)
        inventory = report["decision_brief"]["at_a_glance"]["inventory_summary"]
        self.assertEqual(inventory["controllers"], 12)
        self.assertEqual(inventory["routes"], 40)
        self.assertEqual(inventory["dependencies"], 9)
        ids = {row["id"] for row in report["decision_brief"]["decisions_required"]["blocking"]}
        self.assertIn("DEC-PHP-ARCH-001", ids)
        self.assertIn("DEC-PHP-ASYNC-003", ids)
        self.assertIn("DEC-PHP-SESSION-004", ids)
        self.assertNotIn("DEC-UI-001", ids)
        self.assertEqual(report["metadata"]["context_reference"]["source_language"], "PHP")


if __name__ == "__main__":
    unittest.main()
