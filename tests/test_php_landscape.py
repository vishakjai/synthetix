import unittest

from utils.landscape_router import build_landscape_artifacts


class PhpLandscapeTests(unittest.TestCase):
    def test_custom_php_repo_emits_php_artifacts_and_component(self):
        entries = [
            {"path": "Controller/LoginController.php", "type": "blob", "size": 4200},
            {"path": "Model/User.php", "type": "blob", "size": 2100},
            {"path": "View/login.phtml", "type": "blob", "size": 1600},
            {"path": "Includes/db.php", "type": "blob", "size": 2500},
            {"path": "config/config.ini", "type": "blob", "size": 400},
            {"path": "dashboard/index.php", "type": "blob", "size": 3200},
        ]
        file_contents = {
            "Controller/LoginController.php": "<?php session_start(); require_once '../Includes/db.php';",
            "Includes/db.php": "<?php $sql = \"SELECT * FROM users\";",
            "dashboard/index.php": "<?php include '../View/login.phtml';",
        }

        artifacts = build_landscape_artifacts(
            repo="https://github.com/example/magicbox",
            branch="main",
            commit_sha="abc123",
            entries=entries,
            file_contents=file_contents,
        )

        self.assertIn("php_framework_profile_v1", artifacts)
        self.assertIn("php_route_hints_v1", artifacts)
        self.assertIn("php_route_inventory_v1", artifacts)
        self.assertIn("php_controller_inventory_v1", artifacts)
        self.assertIn("php_template_inventory_v1", artifacts)
        profile = artifacts["php_framework_profile_v1"]
        self.assertEqual(profile["framework"], "custom_php")
        self.assertGreaterEqual(artifacts["php_route_inventory_v1"].get("route_count", 0), 1)
        self.assertGreaterEqual(artifacts["php_controller_inventory_v1"].get("controller_count", 0), 1)
        self.assertGreaterEqual(artifacts["php_template_inventory_v1"].get("template_count", 0), 1)

        component_inventory = artifacts["component_inventory_v1"]
        components = component_inventory.get("components", [])
        php_components = [row for row in components if row.get("component_kind") in {"custom_php_app", "php_project"}]
        self.assertTrue(php_components)
        self.assertEqual(php_components[0].get("framework"), "custom_php")
        self.assertIn("php_web_modernization", php_components[0]["suggested_tracks"][0]["lane"])


if __name__ == "__main__":
    unittest.main()
