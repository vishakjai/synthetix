import unittest

from utils.php_auth import extract_php_auth_inventory
from utils.php_controllers import extract_php_controller_inventory
from utils.php_file_io import extract_php_file_io_inventory
from utils.php_includes import extract_php_include_graph
from utils.php_jobs import extract_php_background_job_inventory
from utils.php_routes import extract_php_route_inventory
from utils.php_sessions import extract_php_session_state_inventory
from utils.php_sql import extract_php_sql_catalog
from utils.php_templates import extract_php_template_inventory
from utils.php_validation import extract_php_validation_rules


class PhpExtractorsTest(unittest.TestCase):
    def test_extract_php_structural_artifacts(self):
        file_map = {
            "routes/web.php": "<?php Route::get('/login', 'AuthController@login'); Route::post('/upload', 'UploadController@store');",
            "Controller/AuthController.php": (
                "<?php class AuthController { "
                "public function login() { session_start(); if (isset($_POST['email'])) { $sql = \"SELECT * FROM users WHERE email='\".$_POST['email'].\"'\"; } } "
                "public function logout() {} }"
            ),
            "Controller/UploadController.php": (
                "<?php class UploadController { public function store() { "
                "if (!empty($_FILES['doc'])) { move_uploaded_file($_FILES['doc']['tmp_name'], '/tmp/doc'); } "
                "$csv = fputcsv($fp, ['a']); } }"
            ),
            "View/login.phtml": "<form method='post'></form>",
            "bootstrap.php": "<?php require_once 'Controller/AuthController.php'; include 'View/login.phtml';",
            "cron/daily.php": "<?php echo 'run';",
        }

        routes = extract_php_route_inventory(file_map)
        controllers = extract_php_controller_inventory(file_map)
        templates = extract_php_template_inventory(file_map)
        sql_catalog = extract_php_sql_catalog(file_map)
        sessions = extract_php_session_state_inventory(file_map)
        auth = extract_php_auth_inventory(file_map)
        include_graph = extract_php_include_graph(file_map)
        jobs = extract_php_background_job_inventory(file_map)
        file_io = extract_php_file_io_inventory(file_map)
        validation = extract_php_validation_rules(file_map)

        self.assertEqual(routes["artifact_type"], "php_route_inventory_v1")
        self.assertGreaterEqual(routes["route_count"], 2)
        self.assertGreaterEqual(controllers["controller_count"], 2)
        self.assertGreaterEqual(templates["template_count"], 1)
        self.assertGreaterEqual(sql_catalog["statement_count"], 1)
        self.assertTrue(sessions["uses_session_state"])
        self.assertGreaterEqual(auth["auth_file_count"], 1)
        self.assertGreaterEqual(include_graph["edge_count"], 2)
        self.assertGreaterEqual(jobs["job_count"], 1)
        self.assertGreaterEqual(file_io["upload_file_count"], 1)
        self.assertGreaterEqual(file_io["export_file_count"], 1)
        self.assertGreaterEqual(validation["file_count"], 1)


if __name__ == "__main__":
    unittest.main()
