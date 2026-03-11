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

    def test_extract_custom_php_entrypoints_and_template_references(self):
        file_map = {
            "dashboard/Recruiter_new.php": (
                "<?php include(\"views/ViewHeader.php\"); "
                "include('dashboard/elements/panel_links.php'); "
                "$this->template = \"{$syspath}/src/templates/direct_hire_xml_gp.php\";"
            ),
            "Controller/PortalCustomerController.php": "<?php class PortalCustomerController { public function index() {} }",
            "bin/msgQueueListner.php": "<?php include('/data/app/src/init.php');",
        }
        routes = extract_php_route_inventory(file_map)
        templates = extract_php_template_inventory(file_map)

        self.assertGreaterEqual(routes["route_count"], 1)
        self.assertTrue(any(str(row.get("path")) == "Controller/PortalCustomerController.php" for row in routes["entrypoints"]))
        self.assertFalse(any(str(row.get("path")) == "dashboard/Recruiter_new.php" for row in routes["entrypoints"]))
        self.assertFalse(any(str(row.get("path")) == "bin/msgQueueListner.php" for row in routes["entrypoints"]))
        template_paths = {row["path"] for row in templates["templates"]}
        self.assertIn("views/ViewHeader.php", template_paths)
        self.assertIn("dashboard/elements/panel_links.php", template_paths)
        self.assertIn("{$syspath}/src/templates/direct_hire_xml_gp.php", template_paths)

    def test_extract_magicbox_style_template_patterns(self):
        file_map = {
            "Controller/PortalExternController.php": (
                "<?php class PortalExternController { "
                "public function login() { return $this->renderPage('extern_login_new.php'); } "
                "}"
            ),
            "Utility/Email_message.php": (
                "<?php class Email_message { "
                "protected $renderfilename = null;"
                "public function build() { "
                "if (is_null($this->renderfilename)) { $this->renderfilename = 'views/EmailResult.php'; } "
                "return View::renderFile($this->renderfilename, ['model' => $this]); } }"
            ),
            "Utility/Dashboard.php": (
                "<?php class Dashboard { "
                "public function render() { "
                "$file = 'dashboard/elements/dashboard_element_custom.php'; include($file); "
                "include('dashboard/main.php'); } }"
            ),
            "Controller/Contractor_placementEntityController.php": (
                "<?php class Contractor_placementEntityController { "
                "public function send() { "
                "$this->mail_content = $this->getTemplateContent(\"{$syspath}/src/eop_email_templates/eop_initiation_email_html.php\"); "
                "} }"
            ),
        }

        templates = extract_php_template_inventory(file_map)
        template_paths = {row["path"] for row in templates["templates"]}

        self.assertIn("extern_login_new.php", template_paths)
        self.assertIn("views/EmailResult.php", template_paths)
        self.assertIn("dashboard/elements/dashboard_element_custom.php", template_paths)
        self.assertIn("dashboard/main.php", template_paths)
        self.assertIn("{$syspath}/src/eop_email_templates/eop_initiation_email_html.php", template_paths)

    def test_extractors_merge_full_tree_entries_with_fetched_bodies(self):
        file_map = {
            "Controller/AuthController.php": "<?php class AuthController { public function login() {} }",
            "routes/web.php": "<?php Route::get('/login', 'AuthController@login');",
        }
        entries = [
            {"path": "Controller/AuthController.php", "type": "blob"},
            {"path": "Controller/UserController.php", "type": "blob"},
            {"path": "dashboard/elements/panel_links.php", "type": "blob"},
            {"path": "views/login.php", "type": "blob"},
            {"path": "routes/web.php", "type": "blob"},
        ]

        controllers = extract_php_controller_inventory(file_map, entries=entries)
        templates = extract_php_template_inventory(file_map, entries=entries)
        routes = extract_php_route_inventory(file_map, entries=entries)

        self.assertGreaterEqual(controllers["controller_count"], 2)
        self.assertGreaterEqual(templates["template_count"], 2)
        self.assertGreaterEqual(routes["route_count"], 2)
        controller_paths = {row["path"] for row in controllers["controllers"]}
        self.assertIn("Controller/UserController.php", controller_paths)
        template_paths = {row["path"] for row in templates["templates"]}
        self.assertIn("dashboard/elements/panel_links.php", template_paths)
        self.assertIn("views/login.php", template_paths)


if __name__ == "__main__":
    unittest.main()
