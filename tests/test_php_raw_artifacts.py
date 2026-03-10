import unittest

from utils.analyst_report import build_raw_artifact_set_v1


class PhpRawArtifactsTest(unittest.TestCase):
    def test_build_raw_artifacts_emits_php_artifacts(self):
        php_analysis = {
            "route_inventory": {
                "artifact_type": "php_route_inventory_v1",
                "route_count": 2,
                "routes": [{"route_id": "route:1", "uri": "/login", "method": "GET"}],
            },
            "controller_inventory": {
                "artifact_type": "php_controller_inventory_v1",
                "controller_count": 1,
                "controllers": [{"controller_id": "controller:1", "name": "AuthController", "actions": ["login"]}],
            },
            "template_inventory": {
                "artifact_type": "php_template_inventory_v1",
                "template_count": 1,
                "templates": [{"template_id": "template:1", "name": "login.phtml"}],
            },
            "sql_catalog": {
                "artifact_type": "php_sql_catalog_v1",
                "statement_count": 1,
                "statements": [{"sql_id": "php_sql:1", "raw": "SELECT * FROM users", "tables": ["users"]}],
            },
            "session_state_inventory": {
                "artifact_type": "php_session_state_inventory_v1",
                "uses_session_state": True,
                "session_key_count": 2,
                "session_keys": ["user_id", "role"],
            },
            "authz_authn_inventory": {
                "artifact_type": "php_authz_authn_inventory_v1",
                "auth_file_count": 1,
                "evidence": [{"path": "Controller/AuthController.php"}],
            },
            "include_graph": {
                "artifact_type": "php_include_graph_v1",
                "edge_count": 1,
                "edges": [{"from": "bootstrap.php", "to": "Controller/AuthController.php"}],
            },
            "background_job_inventory": {
                "artifact_type": "php_background_job_inventory_v1",
                "job_count": 1,
                "jobs": [{"path": "cron/daily.php"}],
            },
            "file_io_inventory": {
                "artifact_type": "php_file_io_inventory_v1",
                "upload_file_count": 1,
                "export_file_count": 1,
                "upload_files": ["Controller/UploadController.php"],
                "export_files": ["Controller/UploadController.php"],
            },
            "validation_rules": {
                "artifact_type": "php_validation_rules_v1",
                "file_count": 1,
                "entries": [{"path": "Controller/AuthController.php", "validation_signal_count": 2}],
            },
        }
        output = {
            "run_id": "run_php_001",
            "source_language": "PHP",
            "source_target_modernization_profile": {
                "source": {"language": "PHP", "framework": "custom_php"},
                "target": {"language": "TypeScript", "framework": "NestJS", "database": "MySQL"},
            },
            "legacy_skill_profile": {"selected_skill_id": "php_legacy", "version": "1.0.0", "confidence": 0.92},
            "requirements_pack": {
                "legacy_code_inventory": {
                    "summary": "Detected PHP legacy application.",
                    "vb6_projects": [],
                    "forms": [],
                    "project_members": ["routes/web.php", "Controller/AuthController.php"],
                    "database_tables": ["users"],
                    "sql_query_catalog": ["SELECT * FROM users"],
                    "modernization_readiness": {"score": 54, "risk_tier": "medium"},
                    "php_analysis": php_analysis,
                }
            },
        }

        raw = build_raw_artifact_set_v1(output)
        self.assertIn("php_route_inventory", raw)
        self.assertIn("php_controller_inventory", raw)
        self.assertIn("php_sql_catalog", raw)
        self.assertIn("php_validation_rules", raw)
        self.assertEqual(raw["php_controller_inventory"]["controllers"][0]["name"], "AuthController")
        self.assertEqual(raw["php_sql_catalog"]["statement_count"], 1)
        self.assertIn("php_route_inventory", raw["artifact_refs"])
        artifact_types = {entry["type"] for entry in raw["artifact_index"]["artifacts"]}
        self.assertIn("php_route_inventory", artifact_types)
        self.assertIn("php_file_io_inventory", artifact_types)


if __name__ == "__main__":
    unittest.main()
