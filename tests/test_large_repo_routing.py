import unittest

from utils.repo_snapshot import classify_repo_scan_mode


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


if __name__ == "__main__":
    unittest.main()
