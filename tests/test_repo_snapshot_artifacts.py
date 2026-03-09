import unittest

from utils.repo_snapshot import build_repo_snapshot_v1
from utils.symbol_index import build_symbol_index_v1


class RepoSnapshotArtifactsTest(unittest.TestCase):
    def test_snapshot_and_symbol_index_capture_vb6_inventory(self):
        file_contents = {
            "BANK.vbp": "Type=Exe\nStartup=\"frmSplash\"\nForm=frmCustomer.frm\nModule=Module1; Module1.bas\n",
            "frmCustomer.frm": (
                "VERSION 5.00\n"
                "Begin VB.Form frmCustomer\n"
                "End\n"
                "Private Sub cmdSave_Click()\n"
                "End Sub\n"
            ),
            "Module1.bas": (
                "Attribute VB_Name = \"Module1\"\n"
                "Public GlobalCounter As Integer\n"
                "Public Sub SaveCustomer()\n"
                "End Sub\n"
            ),
        }
        selected_entries = [
            {"path": path, "size": len(body), "sha": f"sha-{idx}", "ext": path[path.rfind('.'):], "depth": path.count("/")}
            for idx, (path, body) in enumerate(file_contents.items(), start=1)
        ]
        bundle_summary = {
            "included_file_count": 3,
            "omitted_file_count": 0,
            "included_by_bucket": {"project": 1, "form": 1, "module": 1},
        }

        snapshot = build_repo_snapshot_v1(
            snapshot_id="snap-123",
            repo_url="https://github.com/example/TestVB6Project1",
            owner="example",
            repository="TestVB6Project1",
            branch="main",
            commit_sha="abc123",
            tree_sha="tree123",
            include_paths=[],
            exclude_paths=[],
            raw_entries=selected_entries,
            selected_entries=selected_entries,
            file_contents=file_contents,
            failed_paths=[],
            reused_paths=[],
            changed_paths=set(),
            compare_error="",
            chunk_size=25,
            chunk_workers=4,
            family_key="family-1",
            bundle_summary=bundle_summary,
            analysis_mode="standard",
            analysis_mode_reasons=[],
        )
        self.assertEqual(snapshot["artifact_type"], "repo_snapshot_v1")
        self.assertEqual(snapshot["selected_file_count"], 3)
        self.assertEqual(snapshot["fetched_file_count"], 3)
        self.assertEqual(snapshot["counts_by_type"]["project"], 1)
        self.assertEqual(snapshot["counts_by_type"]["form"], 1)
        self.assertEqual(snapshot["counts_by_type"]["module"], 1)
        self.assertEqual(snapshot["estimated_loc_by_type"]["form"], 5)

        symbols = build_symbol_index_v1(snapshot_id="snap-123", file_contents=file_contents)
        self.assertEqual(symbols["artifact_type"], "symbol_index_v1")
        self.assertGreaterEqual(symbols["counts_by_kind"].get("project", 0), 1)
        self.assertGreaterEqual(symbols["counts_by_kind"].get("form", 0), 1)
        self.assertGreaterEqual(symbols["counts_by_kind"].get("procedure", 0), 2)
        self.assertGreaterEqual(symbols["counts_by_kind"].get("global", 0), 1)


if __name__ == "__main__":
    unittest.main()
