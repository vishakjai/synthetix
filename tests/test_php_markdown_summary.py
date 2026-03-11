import unittest

from scripts.run_vb6_analyst_markdown import build_full_markdown


class PhpMarkdownSummaryTest(unittest.TestCase):
    def test_vb6_markdown_inventory_language_is_unchanged(self):
        output = {
            "source_language": "VB6",
            "legacy_code_inventory": {
                "summary": "Detected VB6 legacy application.",
                "modernization_readiness": {"score": 70, "risk_tier": "medium"},
                "source_loc_total": 9038,
                "source_loc_forms": 5140,
                "source_loc_modules": 217,
                "source_loc_classes": 0,
                "source_files_scanned": 49,
                "database_tables": ["Customer", "Account"],
                "vb6_projects": [
                    {
                        "project_name": "BANK",
                        "forms": ["frmCustomer", "frmDeposit"],
                        "forms_count": 25,
                        "controls": ["TextBox", "CommandButton"],
                    }
                ],
                "forms": [{"form_name": "frmCustomer"}] * 25,
                "dependencies": [
                    {"name": "MSCOMCTL.OCX"},
                    {"name": "MSCOMCT2.OCX"},
                    {"name": "DBGRID32.OCX"},
                    {"name": "MSBIND.DLL"},
                ],
            },
            "raw_artifacts": {
                "legacy_inventory": {
                    "summary": {
                        "counts": {
                            "source_loc_total": 9038,
                            "source_loc_forms": 5140,
                            "source_loc_modules": 217,
                            "source_loc_classes": 0,
                            "source_files_scanned": 49,
                        }
                    }
                }
            },
        }
        markdown = build_full_markdown(output, mode="full")
        self.assertIn("1 project(s), 25 forms/usercontrols, 4 dependencies", markdown)
        self.assertIn("9038 total LOC (5140 form LOC, 217 module LOC, 0 class LOC) across 49 files", markdown)
        self.assertNotIn("controllers, 0 routes", markdown)

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

    def test_php_markdown_infers_php_mode_from_report_artifacts(self):
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
                    "datastore_signals": [{"datastore": "mysql"}],
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
        markdown = build_full_markdown(output, mode="full")
        self.assertIn("64 controllers, 200 routes, 11 templates, 13 dependencies", markdown)
        self.assertIn("11647 total LOC across 80 files", markdown)
        self.assertNotIn("forms/usercontrols", markdown)
        self.assertNotIn("DEC-UI-001", markdown)
        self.assertIn("DEC-PHP-ARCH-001", markdown)


    def test_php_markdown_appendix_uses_php_artifacts(self):
        output = {
            "source_language": "PHP",
            "requirements_pack": {
                "legacy_code_inventory": {
                    "summary": "Detected PHP legacy application.",
                    "modernization_readiness": {"score": 60, "risk_tier": "medium"},
                    "source_loc_total": 565185,
                    "source_files_scanned": 3827,
                    "php_dependency_count": 3,
                    "php_analysis": {
                        "route_inventory": {"route_count": 1, "routes": [{"route_id": "route:1", "method": "ANY", "uri": "/index", "handler": "index.php", "source_file": "index.php"}]},
                        "controller_inventory": {"controller_count": 248, "action_count": 400, "controllers": [{"name": "AuthController", "path": "Controller/AuthController.php", "actions": ["login", "logout"]}]},
                        "template_inventory": {"template_count": 11, "templates": [{"name": "login.php", "path": "views/login.php", "engine": "php"}]},
                        "sql_catalog": {"statement_count": 40, "statements": [{"sql_id": "php_sql:1", "kind": "SELECT", "raw": "SELECT * FROM users", "tables": ["users"], "risk_flags": ["possible_unparameterized_query"]}]},
                        "session_state_inventory": {"uses_session_state": True, "session_key_count": 6},
                        "authz_authn_inventory": {"auth_file_count": 8, "evidence": [{"path": "Controller/AuthController.php", "signals": ["login", "session_guard"]}]},
                        "background_job_inventory": {"job_count": 1, "jobs": [{"path": "bin/msgQueueListner.php"}]},
                        "file_io_inventory": {"upload_file_count": 1, "export_file_count": 1},
                        "validation_rules": {"file_count": 5, "entries": [{"path": "Controller/AuthController.php", "validation_signal_count": 3, "uses_required_checks": True, "uses_filter_var": True}]},
                    },
                }
            },
            "raw_artifacts": {
                "artifact_index": {"artifacts": [{"type": "php_route_inventory", "ref": "artifact://analyst/raw/php_route_inventory/v1"}]},
            },
        }
        markdown = build_full_markdown(output, mode="full")
        self.assertIn("- Route rows: 1", markdown)
        self.assertIn("- SQL catalog rows: 1", markdown)
        self.assertIn("- Dependency rows: 3", markdown)
        self.assertIn("- Business rules: 2", markdown)
        self.assertIn("- Risk register rows: 3", markdown)
        self.assertIn("- Static risk detector findings: 3", markdown)
        self.assertIn("- Source LOC: 565185 total across 3827 file(s)", markdown)
        self.assertIn("### C. Route Inventory", markdown)
        self.assertIn("- Dependencies: 3", markdown)
        self.assertNotIn("- No event map rows available.", markdown)

    def test_php_markdown_falls_back_to_framework_profile_and_landscape_summary(self):
        output = {
            "source_language": "PHP",
            "requirements_pack": {
                "legacy_code_inventory": {
                    "summary": "Detected PHP legacy application.",
                    "modernization_readiness": {"score": 60, "risk_tier": "medium"},
                    "source_loc_total": 0,
                    "source_files_scanned": 0,
                    "php_analysis": {
                        "route_inventory": {},
                        "controller_inventory": {},
                        "template_inventory": {},
                    },
                }
            },
            "raw_artifacts": {
                "repo_landscape_v1": {
                    "scan_summary": {"total_loc": 565185, "total_files": 3827},
                    "dependency_footprint": {"composer_package_count": 3},
                },
                "php_framework_profile_v1": {
                    "framework": "custom_php",
                    "controller_count": 248,
                    "template_count": 0,
                    "route_file_count": 1,
                    "composer_package_count": 3,
                    "auth_touchpoint_estimate": 18,
                },
                "php_route_hints_v1": {
                    "estimated_route_files": 1,
                    "estimated_controllers": 248,
                    "estimated_templates": 0,
                },
            },
        }
        markdown = build_full_markdown(output, mode="full")
        self.assertIn("1 application(s), 248 controllers, 1 routes, 0 templates, 3 dependencies", markdown)
        self.assertIn("565185 total LOC across 3827 files", markdown)
        self.assertNotIn("0 total LOC across 0 files", markdown)


if __name__ == "__main__":
    unittest.main()
