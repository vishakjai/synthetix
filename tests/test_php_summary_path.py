import unittest
from unittest.mock import Mock

from agents.analyst import AnalystAgent
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

    def test_php_analyst_report_infers_php_mode_from_raw_artifacts(self):
        output = {
            "requirements_pack": {
                "legacy_code_inventory": {
                    "summary": "Detected PHP legacy application.",
                    "modernization_readiness": {"score": 52, "risk_tier": "medium"},
                    "source_loc_total": 11647,
                    "source_files_scanned": 80,
                    "database_tables": ["users", "orders"],
                }
            },
            "raw_artifacts": {
                "repo_landscape_v1": {
                    "languages_detected": ["PHP"],
                    "datastore_signals": [{"datastore": "mysql"}, {"datastore": "mq"}],
                    "dependency_footprint": {"composer_package_count": 13},
                },
                "php_route_inventory_v1": {"route_count": 200, "entrypoint_count": 200},
                "php_controller_inventory_v1": {"controller_count": 64},
                "php_template_inventory_v1": {"template_count": 11},
                "php_session_state_inventory_v1": {"session_key_count": 6},
                "php_authz_authn_inventory_v1": {"auth_touchpoint_count": 8},
                "php_background_job_inventory_v1": {"job_count": 1},
                "php_file_io_inventory_v1": {"upload_file_count": 1, "export_file_count": 2},
            },
        }
        report = build_analyst_report_v2(output)
        inventory = report["decision_brief"]["at_a_glance"]["inventory_summary"]
        self.assertEqual(inventory["controllers"], 64)
        self.assertEqual(inventory["routes"], 200)
        self.assertEqual(inventory["templates"], 11)
        self.assertEqual(inventory["dependencies"], 13)
        ids = {row["id"] for row in report["decision_brief"]["decisions_required"]["blocking"]}
        self.assertIn("DEC-PHP-ARCH-001", ids)
        self.assertNotIn("DEC-UI-001", ids)
        self.assertEqual(report["metadata"]["context_reference"]["source_language"], "php")

    def test_php_inventory_falls_back_from_discover_cache_landscape(self):
        agent = AnalystAgent(Mock())
        state = {
            "integration_context": {
                "discover_cache": {
                    "landscape": {
                        "repo_landscape_v1": {
                            "languages_detected": ["PHP"],
                            "loc_total": 11647,
                            "file_count_total": 80,
                            "dependency_footprint": {"composer_package_count": 13},
                            "selected_files": [
                                {"path": "index.php", "estimated_loc": 120},
                                {"path": "controllers/AuthController.php", "estimated_loc": 220},
                            ],
                        },
                        "php_framework_profile_v1": {"framework": "custom_php", "composer_package_count": 13},
                        "php_route_inventory_v1": {"route_count": 200, "entrypoint_count": 200},
                        "php_controller_inventory_v1": {"controller_count": 64, "controllers": [{"name": "AuthController", "actions": ["login"]}]},
                        "php_template_inventory_v1": {"template_count": 11},
                        "php_sql_catalog_v1": {"statement_count": 40, "statements": [{"raw": "SELECT * FROM users", "tables": ["users"]}]},
                        "php_session_state_inventory_v1": {"session_key_count": 6, "uses_session_state": True, "superglobal_usage": {"_SESSION": 4}},
                        "php_authz_authn_inventory_v1": {"auth_touchpoint_count": 8},
                        "php_include_graph_v1": {"edge_count": 4},
                        "php_background_job_inventory_v1": {"job_count": 1},
                        "php_file_io_inventory_v1": {"upload_file_count": 1, "export_file_count": 2},
                        "php_validation_rules_v1": {"rule_count": 5},
                    }
                }
            }
        }
        inventory = agent._inventory_from_discover_cache(state)
        self.assertEqual(inventory["php_analysis"]["route_inventory"]["route_count"], 200)
        self.assertEqual(inventory["php_analysis"]["controller_inventory"]["controller_count"], 64)
        self.assertEqual(inventory["php_dependency_count"], 13)
        self.assertEqual(inventory["source_loc_total"], 11647)

    def test_php_finalize_output_rebuilds_compact_inventory_when_missing(self):
        agent = AnalystAgent(Mock())
        state = {
            "legacy_code": "\n".join(
                [
                    "### FILE: index.php",
                    "<?php session_start(); require 'views/home.php'; $db = new PDO($dsn);",
                    "### FILE: controllers/AuthController.php",
                    "<?php class AuthController { public function login() { $_SESSION['user_id']=1; } }",
                    "### FILE: views/home.php",
                    "<html><?php echo 'home'; ?></html>",
                ]
            ),
            "modernization_language": "TypeScript",
        }
        finalized = agent._finalize_output({"functional_requirements": [], "non_functional_requirements": []}, {}, state)
        inventory = finalized.get("legacy_code_inventory", {})
        self.assertEqual(finalized.get("source_language"), "PHP")
        self.assertEqual(finalized.get("legacy_skill_profile", {}).get("selected_skill_id"), "php_legacy")
        self.assertTrue(isinstance(inventory.get("php_analysis", {}), dict))
        self.assertGreaterEqual(inventory.get("php_analysis", {}).get("controller_inventory", {}).get("controller_count", 0), 1)


if __name__ == "__main__":
    unittest.main()
