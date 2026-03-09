import unittest

from utils.chunk_merge import build_chunk_qa_report_v1, build_merged_analysis_coverage_v1


class ChunkMergeCoverageTest(unittest.TestCase):
    def test_chunk_qa_and_merged_coverage_are_built(self):
        chunk_manifest = {
            "artifact_type": "chunk_manifest_v1",
            "snapshot_id": "snap-qa",
            "chunk_count": 2,
            "chunks": [
                {
                    "chunk_id": "chunk::project::1",
                    "component_id": "project::BANK",
                    "component_name": "BANK",
                    "component_type": "vb6_project",
                    "paths": ["BANK.vbp", "frmCustomer.frm"],
                    "estimated_loc": 100,
                    "coverage_expectations": {
                        "should_extract_project_membership": True,
                        "should_extract_forms": True,
                        "should_extract_modules": False,
                    },
                },
                {
                    "chunk_id": "chunk::shared::1",
                    "component_id": "shared::foundation",
                    "component_name": "Shared Foundation",
                    "component_type": "shared_foundation",
                    "paths": ["Module1.bas"],
                    "estimated_loc": 40,
                    "coverage_expectations": {
                        "should_extract_project_membership": False,
                        "should_extract_forms": False,
                        "should_extract_modules": True,
                    },
                },
            ],
        }
        chunk_executions = [
            {
                "chunk_id": "chunk::project::1",
                "forms_count": 1,
                "event_handlers_count": 2,
                "functions_count": 0,
                "project_members_count": 1,
                "summary": "project signals",
            },
            {
                "chunk_id": "chunk::shared::1",
                "forms_count": 0,
                "event_handlers_count": 0,
                "functions_count": 3,
                "project_members_count": 0,
                "summary": "module signals",
            },
        ]
        context = {
            "included_chunks": ["chunk::project::1", "chunk::shared::1"],
            "omitted_chunks": [],
        }
        qa = build_chunk_qa_report_v1(
            snapshot_id="snap-qa",
            chunk_manifest=chunk_manifest,
            chunk_executions=chunk_executions,
            large_repo_context=context,
        )
        self.assertEqual(qa["artifact_type"], "chunk_qa_report_v1")
        self.assertEqual(qa["analyzed_chunk_count"], 2)
        self.assertEqual(qa["extracted_forms"], 1)
        self.assertEqual(qa["extracted_functions"], 3)

        merged = build_merged_analysis_coverage_v1(
            snapshot_id="snap-qa",
            repo_scan_coverage={
                "analysis_mode": "large_repo",
                "selected_file_count": 3,
                "fetched_file_count": 3,
                "failed_fetch_count": 0,
                "failed_paths": [],
                "bundle_summary": {"included_file_count": 3, "omitted_file_count": 0},
                "chunk_count": 2,
                "large_repo_context": {"included_chunk_count": 2, "omitted_chunk_count": 0},
            },
            chunk_qa_report=qa,
            forms_count_reported=1,
            event_handler_count_exact=2,
            bas_module_count=1,
        )
        self.assertEqual(merged["artifact_type"], "merged_analysis_coverage_v1")
        self.assertEqual(merged["analysis_mode"], "large_repo")
        self.assertEqual(merged["analyzed_chunk_count"], 2)
        self.assertEqual(merged["forms_reported"], 1)


if __name__ == "__main__":
    unittest.main()
