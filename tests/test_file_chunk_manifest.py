import unittest

from utils.file_chunk_manifest import build_file_chunk_manifest_v1


class FileChunkManifestTest(unittest.TestCase):
    def test_manifest_marks_truncated_and_chunked_files(self):
        file_contents = {
            "frmLarge.frm": "A" * 50000,
            "Module1.bas": "B" * 2000,
        }
        meta = {
            "frmLarge.frm": {
                "original_char_count": 90000,
                "fetched_char_count": 50000,
                "truncated_at_fetch": True,
            },
            "Module1.bas": {
                "original_char_count": 2000,
                "fetched_char_count": 2000,
                "truncated_at_fetch": False,
            },
        }
        manifest = build_file_chunk_manifest_v1(
            snapshot_id="snap-1",
            file_contents=file_contents,
            file_fetch_meta=meta,
            analysis_mode="large_repo",
            chunk_threshold_chars=25000,
            chunk_size_chars=20000,
        )
        self.assertEqual(manifest["artifact_type"], "file_chunk_manifest_v1")
        self.assertEqual(manifest["file_count"], 2)
        self.assertEqual(manifest["truncated_fetch_count"], 1)
        self.assertEqual(manifest["chunked_file_count"], 1)
        by_path = {row["path"]: row for row in manifest["files"]}
        self.assertTrue(by_path["frmLarge.frm"]["truncated_at_fetch"])
        self.assertTrue(by_path["frmLarge.frm"]["chunked_for_analysis"])
        self.assertGreaterEqual(by_path["frmLarge.frm"]["chunk_count"], 2)
        self.assertFalse(by_path["Module1.bas"]["truncated_at_fetch"])


if __name__ == "__main__":
    unittest.main()
