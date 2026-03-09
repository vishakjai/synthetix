import unittest

from web.server import _compose_legacy_code_bundle


class RepoScanBundleTest(unittest.TestCase):
    def test_vb6_bundle_prioritizes_forms_before_modules(self):
        bundle, summary = _compose_legacy_code_bundle(
            {
                "z_module.bas": "Attribute VB_Name = \"Module1\"\nPublic Sub DoThing()\nEnd Sub\n",
                "a_form.frm": "VERSION 5.00\nBegin VB.Form frmCustomer\nEnd\n",
                "proj/BANK.vbp": "Type=Exe\nForm=a_form.frm\nModule=Module1; z_module.bas\n",
            },
            max_total_chars=1000,
        )
        self.assertIn("### FILE: proj/BANK.vbp", bundle)
        self.assertIn("### FILE: a_form.frm", bundle)
        self.assertIn("### FILE: z_module.bas", bundle)
        self.assertLess(bundle.index("### FILE: a_form.frm"), bundle.index("### FILE: z_module.bas"))
        self.assertEqual(summary["included_by_bucket"].get("project"), 1)
        self.assertEqual(summary["included_by_bucket"].get("form"), 1)
        self.assertEqual(summary["included_by_bucket"].get("module"), 1)

    def test_bundle_summary_reports_omitted_files_when_cap_hits(self):
        bundle, summary = _compose_legacy_code_bundle(
            {
                "a.frm": "VERSION 5.00\nBegin VB.Form A\n" + ("x" * 400),
                "b.frm": "VERSION 5.00\nBegin VB.Form B\n" + ("y" * 400),
                "c.bas": "Attribute VB_Name = \"Module1\"\n" + ("z" * 400),
            },
            max_total_chars=450,
        )
        self.assertTrue(bundle)
        self.assertGreaterEqual(int(summary.get("omitted_file_count", 0) or 0), 1)
        self.assertGreaterEqual(len(summary.get("omitted_paths_sample", [])), 1)


if __name__ == "__main__":
    unittest.main()
