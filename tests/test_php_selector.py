import unittest

from web.server import _select_source_entries_for_analysis


class PhpSelectorTest(unittest.TestCase):
    def test_large_php_selection_reserves_routes_templates_and_support_files(self):
        raw_entries = []
        for idx in range(1, 121):
            raw_entries.append({"path": f"Controller/Controller{idx}Controller.php", "type": "blob", "size": 1000, "sha": f"c{idx}"})
        for idx in range(1, 41):
            raw_entries.append({"path": f"views/View{idx}.php", "type": "blob", "size": 500, "sha": f"v{idx}"})
        for idx in range(1, 21):
            raw_entries.append({"path": f"Utility/Helper{idx}.php", "type": "blob", "size": 700, "sha": f"u{idx}"})
        raw_entries.extend(
            [
                {"path": "routes/web.php", "type": "blob", "size": 200, "sha": "r1"},
                {"path": "index.php", "type": "blob", "size": 300, "sha": "r2"},
                {"path": "dashboard/main.php", "type": "blob", "size": 400, "sha": "t1"},
            ]
        )

        selected = _select_source_entries_for_analysis(raw_entries, limit=80)
        selected_paths = {row["path"] for row in selected}

        self.assertGreaterEqual(len(selected), 80)
        self.assertIn("routes/web.php", selected_paths)
        self.assertIn("index.php", selected_paths)
        self.assertIn("dashboard/main.php", selected_paths)
        self.assertTrue(any(path.startswith("views/") for path in selected_paths))
        self.assertTrue(any(path.startswith("Utility/") for path in selected_paths))


if __name__ == "__main__":
    unittest.main()
