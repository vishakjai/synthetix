import unittest

from utils.repo_snapshot import classify_repo_scan_mode
from web.server import _select_source_entries_for_analysis


class LargeRepoRoutingTest(unittest.TestCase):
    def test_small_repo_stays_standard(self):
        entries = [
            {"path": "BANK.vbp", "size": 2400},
            {"path": "forms/frmCustomer.frm", "size": 18000},
            {"path": "forms/frmDeposit.frm", "size": 16000},
            {"path": "modules/Module1.bas", "size": 4000},
        ]
        routing = classify_repo_scan_mode(entries, total_tree_entries=24)
        self.assertEqual(routing["analysis_mode"], "standard")
        self.assertEqual(routing["vb6_projects"], 1)
        self.assertEqual(routing["vb6_forms"], 2)
        self.assertEqual(routing["vb6_modules"], 1)

    def test_many_vb6_forms_routes_to_large_repo(self):
        entries = [{"path": f"Project/BANK{i}.vbp", "size": 1200} for i in range(2)]
        entries.extend(
            {"path": f"forms/Form{i}.frm", "size": 20000}
            for i in range(90)
        )
        routing = classify_repo_scan_mode(entries, total_tree_entries=1200)
        self.assertEqual(routing["analysis_mode"], "large_repo")
        self.assertIn("vb6 forms/usercontrols=90", routing["reasons"])

    def test_large_vb6_selector_reserves_forms_and_modules(self):
        raw_entries = []
        for i in range(260):
            raw_entries.append({"path": f"variants/Project{i:03d}.vbp", "type": "blob", "size": 1200, "sha": f"v{i}"})
        for i in range(140):
            raw_entries.append({"path": f"forms/frmForm{i:03d}.frm", "type": "blob", "size": 18000, "sha": f"f{i}"})
        for i in range(70):
            raw_entries.append({"path": f"modules/Module{i:03d}.bas", "type": "blob", "size": 6000, "sha": f"m{i}"})
        for i in range(40):
            raw_entries.append({"path": f"classes/Class{i:03d}.cls", "type": "blob", "size": 5000, "sha": f"c{i}"})

        selected = _select_source_entries_for_analysis(raw_entries, limit=220)
        selected_paths = [str(row.get("path", "")).lower() for row in selected]
        project_count = sum(1 for path in selected_paths if path.endswith((".vbp", ".vbg")))
        form_count = sum(1 for path in selected_paths if path.endswith((".frm", ".ctl")))
        module_count = sum(1 for path in selected_paths if path.endswith(".bas"))
        class_count = sum(1 for path in selected_paths if path.endswith(".cls"))

        self.assertEqual(len(selected), 220)
        self.assertLess(project_count, 220)
        self.assertGreaterEqual(form_count, 60)
        self.assertGreaterEqual(module_count, 30)
        self.assertGreaterEqual(class_count, 20)

    def test_many_php_controllers_routes_to_large_repo(self):
        entries = []
        for i in range(80):
            entries.append({"path": f"src/controllers/Controller{i:03d}Controller.php", "size": 8000})
        for i in range(30):
            entries.append({"path": f"src/views/view{i:03d}.php", "size": 4000})
        routing = classify_repo_scan_mode(entries, total_tree_entries=900)
        self.assertEqual(routing["analysis_mode"], "large_repo")
        self.assertGreaterEqual(routing["php_controllers"], 80)
        self.assertIn("php controllers=80", routing["reasons"])

    def test_large_php_selector_reserves_controllers_templates_and_routes(self):
        raw_entries = []
        for i in range(160):
            raw_entries.append({"path": f"src/controllers/Controller{i:03d}Controller.php", "type": "blob", "size": 12000, "sha": f"c{i}"})
        for i in range(60):
            raw_entries.append({"path": f"src/views/view{i:03d}.php", "type": "blob", "size": 5000, "sha": f"t{i}"})
        for i in range(12):
            raw_entries.append({"path": f"routes/route{i:02d}.php", "type": "blob", "size": 2000, "sha": f"r{i}"})
        for i in range(120):
            raw_entries.append({"path": f"src/utility/Helper{i:03d}.php", "type": "blob", "size": 4000, "sha": f"h{i}"})

        selected = _select_source_entries_for_analysis(raw_entries, limit=220)
        selected_paths = [str(row.get("path", "")).lower() for row in selected]
        route_count = sum(1 for path in selected_paths if path.startswith("routes/") or "/routes/" in path or path.endswith(("/index.php", "/web.php", "/api.php")))
        controller_count = sum(1 for path in selected_paths if "/controllers/" in path or path.endswith("controller.php"))
        template_count = sum(1 for path in selected_paths if "/views/" in path or "/templates/" in path)

        self.assertEqual(len(selected), 220)
        self.assertGreaterEqual(controller_count, 90)
        self.assertGreaterEqual(template_count, 40)
        self.assertGreaterEqual(route_count, 8)


if __name__ == "__main__":
    unittest.main()
