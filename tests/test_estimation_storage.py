from pathlib import Path
import shutil
import tempfile
import unittest

from estimations import EstimationStore


class EstimationStorageTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp_dir = Path(tempfile.mkdtemp(prefix="synthetix-estimation-store-"))

    def tearDown(self) -> None:
        shutil.rmtree(self.temp_dir, ignore_errors=True)

    def test_create_run_scoped_estimate_paths(self):
        store = EstimationStore(self.temp_dir)
        paths = store.create_estimate(run_id="run_123", estimate_id="estimate_abc")
        self.assertTrue((self.temp_dir / "run_123" / "estimates" / "estimate_abc" / "meta.json").exists())
        self.assertEqual(paths.wbs_path, self.temp_dir / "run_123" / "estimates" / "estimate_abc" / "wbs_v1.json")

    def test_create_standalone_estimate_paths(self):
        store = EstimationStore(self.temp_dir)
        paths = store.create_estimate(estimate_id="estimate_xyz")
        self.assertTrue((self.temp_dir / "_estimates" / "estimate_xyz" / "meta.json").exists())
        self.assertEqual(paths.estimation_input_path, self.temp_dir / "_estimates" / "estimate_xyz" / "estimation_input_v1.json")

    def test_save_and_load_artifact(self):
        store = EstimationStore(self.temp_dir)
        paths = store.create_estimate(run_id="run_999", estimate_id="estimate_save")
        payload = {"hello": "world"}
        store.save_artifact(paths.estimate_summary_path, payload)
        self.assertEqual(store.load_artifact(paths.estimate_summary_path), payload)


if __name__ == "__main__":
    unittest.main()
