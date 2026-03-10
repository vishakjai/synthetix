import json
import subprocess
import tempfile
import unittest
from pathlib import Path


REPO_ROOT = Path("/Users/vishak/Projects/Codex Projects/Synthetix")


class PhpDocgenSmokeTest(unittest.TestCase):
    def test_php_docgen_generates_ba_tech_and_brd(self):
        with tempfile.TemporaryDirectory(prefix="php-docgen-smoke-") as tmp_dir:
            tmp = Path(tmp_dir)
            md_path = tmp / "analyst-output.md"
            meta_path = tmp / "docgen-meta.json"
            out_dir = tmp / "out"
            out_dir.mkdir(parents=True, exist_ok=True)

            md_path.write_text(
                "# Magicbox PHP Modernization\n\n"
                "Repo: https://github.com/vishakjai/magicbox\n"
                "Generated At: 2026-03-10\n",
                encoding="utf-8",
            )
            meta_path.write_text(
                json.dumps(
                    {
                        "title": "Magicbox PHP Modernization",
                        "repoUrl": "https://github.com/vishakjai/magicbox",
                        "source_language": "PHP",
                        "source_loc_total": 123456,
                        "source_loc_forms": 0,
                        "source_loc_modules": 123456,
                        "source_files_scanned": 3345,
                        "php_analysis": {
                            "framework": "custom_php",
                            "route_inventory": {
                                "route_count": 2,
                                "routes": [
                                    {
                                        "route_id": "route:1",
                                        "method": "GET",
                                        "uri": "/login",
                                        "handler": "AuthController@login",
                                        "source_file": "routes/web.php",
                                    },
                                    {
                                        "route_id": "route:2",
                                        "method": "POST",
                                        "uri": "/orders",
                                        "handler": "OrderController@store",
                                        "source_file": "routes/web.php",
                                    },
                                ],
                            },
                            "controller_inventory": {
                                "controller_count": 2,
                                "controllers": [
                                    {
                                        "controller_id": "controller:1",
                                        "name": "AuthController",
                                        "path": "app/Controllers/AuthController.php",
                                        "action_count": 2,
                                        "actions": ["login", "logout"],
                                    },
                                    {
                                        "controller_id": "controller:2",
                                        "name": "OrderController",
                                        "path": "app/Controllers/OrderController.php",
                                        "action_count": 3,
                                        "actions": ["index", "store", "show"],
                                    },
                                ],
                            },
                            "template_inventory": {
                                "template_count": 2,
                                "templates": [
                                    {
                                        "template_id": "template:1",
                                        "name": "login.phtml",
                                        "path": "views/login.phtml",
                                        "engine": "php",
                                    },
                                    {
                                        "template_id": "template:2",
                                        "name": "orders.phtml",
                                        "path": "views/orders.phtml",
                                        "engine": "php",
                                    },
                                ],
                            },
                            "sql_catalog": {
                                "statement_count": 2,
                                "statements": [
                                    {
                                        "sql_id": "php_sql:1",
                                        "kind": "SELECT",
                                        "raw": "SELECT * FROM users",
                                        "tables": ["users"],
                                        "source_file": "app/Controllers/AuthController.php",
                                        "risk_flags": ["possible_unparameterized_query"],
                                    },
                                    {
                                        "sql_id": "php_sql:2",
                                        "kind": "INSERT",
                                        "raw": "INSERT INTO orders (id) VALUES (?)",
                                        "tables": ["orders"],
                                        "source_file": "app/Controllers/OrderController.php",
                                        "risk_flags": [],
                                    },
                                ],
                            },
                            "session_state_inventory": {
                                "uses_session_state": True,
                                "session_key_count": 2,
                                "session_start_files": ["bootstrap.php"],
                                "session_keys": ["user_id", "role"],
                            },
                            "authz_authn_inventory": {
                                "auth_file_count": 1,
                                "evidence": [
                                    {
                                        "path": "app/Controllers/AuthController.php",
                                        "signals": ["login", "session_guard"],
                                    }
                                ],
                            },
                            "include_graph": {
                                "edge_count": 1,
                                "edges": [{"source": "index.php", "target": "bootstrap.php", "type": "include"}],
                            },
                            "background_job_inventory": {
                                "job_count": 1,
                                "jobs": [{"job_id": "job:1", "path": "cron/daily.php"}],
                            },
                            "file_io_inventory": {
                                "upload_file_count": 1,
                                "export_file_count": 1,
                                "upload_files": ["app/Controllers/UploadController.php"],
                                "export_files": ["app/Controllers/ExportController.php"],
                            },
                            "validation_rules": {
                                "file_count": 2,
                                "entries": [
                                    {
                                        "path": "app/Controllers/AuthController.php",
                                        "validation_signal_count": 3,
                                        "uses_required_checks": True,
                                        "uses_regex_checks": False,
                                        "uses_filter_var": True,
                                    },
                                    {
                                        "path": "app/Controllers/OrderController.php",
                                        "validation_signal_count": 2,
                                        "uses_required_checks": True,
                                        "uses_regex_checks": True,
                                        "uses_filter_var": False,
                                    },
                                ],
                            },
                        },
                    }
                ),
                encoding="utf-8",
            )

            subprocess.run(
                [
                    "node",
                    "index.js",
                    "--md",
                    str(md_path),
                    "--out",
                    str(out_dir),
                    "--meta",
                    str(meta_path),
                ],
                cwd=str(REPO_ROOT / "synthetix-docgen"),
                check=True,
                capture_output=True,
                text=True,
            )

            self.assertTrue((out_dir / "ba_brief.docx").exists())
            self.assertTrue((out_dir / "tech_workbook.docx").exists())
            self.assertTrue((out_dir / "brd.docx").exists())
            self.assertTrue((out_dir / "data.json").exists())


if __name__ == "__main__":
    unittest.main()
