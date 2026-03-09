import unittest

from utils.repo_componentizer import build_component_inventory_v1, build_chunk_manifest_v1
from utils.repo_dependency_graph import build_global_dependency_graph_v1
from utils.large_repo_context import build_large_repo_context_v1


class ChunkManifestVb6Test(unittest.TestCase):
    def test_component_inventory_and_chunk_manifest_capture_vbp_groups(self):
        file_contents = {
            "BANK.vbp": "Type=Exe\nStartup=\"frmSplash\"\nForm=frmCustomer.frm\nForm=frmDeposit.frm\nModule=Module1; Module1.bas\n",
            "frmCustomer.frm": "VERSION 5.00\nBegin VB.Form frmCustomer\nEnd\nPrivate Sub cmdSave_Click()\nEnd Sub\n",
            "frmDeposit.frm": "VERSION 5.00\nBegin VB.Form frmDeposit\nEnd\nPrivate Sub cmdPost_Click()\nEnd Sub\n",
            "Module1.bas": "Attribute VB_Name = \"Module1\"\nPublic Sub SaveCustomer()\nEnd Sub\n",
            "Shared.cls": "VERSION 1.0 CLASS\nAttribute VB_Name = \"Shared\"\nPublic Function BuildId() As String\nEnd Function\n",
        }
        selected_entries = [
            {"path": path, "size": len(body), "sha": f"sha-{idx}", "ext": path[path.rfind('.'):], "depth": path.count("/")}
            for idx, (path, body) in enumerate(file_contents.items(), start=1)
        ]

        components = build_component_inventory_v1(
            snapshot_id="snap-1",
            selected_entries=selected_entries,
            file_contents=file_contents,
        )
        self.assertEqual(components["artifact_type"], "component_inventory_v1")
        self.assertGreaterEqual(components["component_count"], 2)
        project = next(row for row in components["components"] if row["component_type"] == "vb6_project")
        self.assertIn("BANK.vbp", project["paths"])
        self.assertIn("frmCustomer.frm", project["paths"])
        self.assertIn("frmDeposit.frm", project["paths"])

        manifest = build_chunk_manifest_v1(
            snapshot_id="snap-1",
            component_inventory=components,
            file_contents=file_contents,
            max_chunk_files=2,
            max_chunk_chars=120,
        )
        self.assertEqual(manifest["artifact_type"], "chunk_manifest_v1")
        self.assertGreaterEqual(manifest["chunk_count"], 2)
        self.assertTrue(any(row["component_type"] == "vb6_project" for row in manifest["chunks"]))

    def test_large_repo_context_uses_chunk_manifest_and_dependency_graph(self):
        file_contents = {
            "BANK.vbp": "Type=Exe\nForm=frmCustomer.frm\nModule=Module1; Module1.bas\n",
            "frmCustomer.frm": "VERSION 5.00\nBegin VB.Form frmCustomer\nEnd\n" + ("x" * 4000),
            "Module1.bas": "Attribute VB_Name = \"Module1\"\nPublic Sub SaveCustomer()\nEnd Sub\n" + ("y" * 4000),
        }
        selected_entries = [
            {"path": path, "size": len(body), "sha": f"sha-{idx}", "ext": path[path.rfind('.'):], "depth": path.count("/")}
            for idx, (path, body) in enumerate(file_contents.items(), start=1)
        ]
        components = build_component_inventory_v1(
            snapshot_id="snap-2",
            selected_entries=selected_entries,
            file_contents=file_contents,
        )
        manifest = build_chunk_manifest_v1(
            snapshot_id="snap-2",
            component_inventory=components,
            file_contents=file_contents,
            max_chunk_files=1,
            max_chunk_chars=4500,
        )
        graph = build_global_dependency_graph_v1(
            snapshot_id="snap-2",
            file_contents=file_contents,
            selected_entries=selected_entries,
        )
        context = build_large_repo_context_v1(
            snapshot_id="snap-2",
            repo_snapshot={"analysis_mode": "large_repo", "selected_file_count": 3, "fetched_file_count": 3, "failed_fetch_count": 0},
            component_inventory=components,
            chunk_manifest=manifest,
            dependency_graph=graph,
            file_contents=file_contents,
            max_total_chars=7000,
        )
        self.assertEqual(context["artifact_type"], "legacy_chunk_context_v1")
        self.assertGreaterEqual(context["included_chunk_count"], 1)
        self.assertIn("### LARGE REPO CONTEXT", context["context_text"])
        self.assertIn("## CHUNK:", context["context_text"])
        self.assertIn("### FILE: BANK.vbp", context["context_text"])


if __name__ == "__main__":
    unittest.main()
