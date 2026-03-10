import unittest

from utils.php_framework_detection import detect_php_framework_profile


class PhpFrameworkDetectionTests(unittest.TestCase):
    def test_detects_laravel_from_paths_and_composer(self):
        entries = [
            {"path": "artisan", "type": "blob"},
            {"path": "composer.json", "type": "blob"},
            {"path": "routes/web.php", "type": "blob"},
            {"path": "app/Http/Controllers/UserController.php", "type": "blob"},
            {"path": "resources/views/users/index.blade.php", "type": "blob"},
        ]
        file_contents = {
            "composer.json": '{"require":{"laravel/framework":"^10.0"}}',
            "routes/web.php": "<?php Route::get('/users', [UserController::class, 'index']);",
        }
        profile = detect_php_framework_profile(entries=entries, file_contents=file_contents)
        self.assertEqual(profile["framework"], "laravel")
        self.assertGreaterEqual(profile["route_file_count"], 1)
        self.assertGreaterEqual(profile["controller_count"], 1)
        self.assertGreaterEqual(profile["template_count"], 1)

    def test_detects_custom_php_monolith(self):
        entries = [
            {"path": "Controller/LoginController.php", "type": "blob"},
            {"path": "Model/User.php", "type": "blob"},
            {"path": "View/login.phtml", "type": "blob"},
            {"path": "Includes/db.php", "type": "blob"},
        ]
        file_contents = {
            "Controller/LoginController.php": "<?php session_start(); if($_SESSION['user']) { echo 'ok'; }",
            "Includes/db.php": "<?php $sql = \"SELECT * FROM users WHERE id=\" . $_GET['id'];",
        }
        profile = detect_php_framework_profile(entries=entries, file_contents=file_contents)
        self.assertEqual(profile["framework"], "custom_php")
        self.assertTrue(profile["uses_session_state"])
        self.assertGreater(profile["sql_touchpoint_estimate"], 0)
        self.assertGreaterEqual(profile["controller_count"], 1)


if __name__ == "__main__":
    unittest.main()
