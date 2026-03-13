import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


_SCRIPT_PATH = Path(__file__).resolve().parents[1] / "scripts" / "export_architect_artifacts.py"
_SPEC = importlib.util.spec_from_file_location("synthetix_export_architect_artifacts", _SCRIPT_PATH)
if _SPEC is None or _SPEC.loader is None:
    raise RuntimeError(f"Unable to load export script from {_SCRIPT_PATH}")
cli = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(cli)


class ExportArchitectArtifactsCliTest(unittest.TestCase):
    def test_run_payload_carries_architect_controls(self):
        args = SimpleNamespace(
            objectives=cli.DEFAULT_OBJECTIVE,
            target_language="C#",
            provider="openai",
            model="gpt-5",
            analysis_depth="deep",
            repo_url="https://github.com/example/bank-vb6",
            approved_adrs=["ADR-002"],
            approved_services=["CustomerService", "TransactionService"],
            deferred_services=["LegacyCoreService"],
            phase_overrides=["ReportingService=3"],
        )
        payload = cli._run_payload(args)
        controls = payload.get("integration_context", {}).get("architect_controls", {})
        self.assertEqual(controls.get("approved_adr_ids"), ["ADR-002"])
        self.assertEqual(controls.get("approved_services"), ["CustomerService", "TransactionService"])
        self.assertEqual(controls.get("deferred_services"), ["LegacyCoreService"])
        self.assertEqual(controls.get("service_phase_overrides"), {"ReportingService": 3})

    def test_try_fetch_stage_two_artifacts_returns_empty_on_error(self):
        with patch.object(cli, "_http_json", side_effect=RuntimeError("not ready")):
            architect_package, architect_handoff = cli._try_fetch_stage_two_artifacts("https://synthetix.example.com", "run_123")
        self.assertEqual(architect_package, {})
        self.assertEqual(architect_handoff, {})

    def test_run_exports_stage_two_artifacts_without_ui(self):
        architect_package = {
            "package_meta": {"artifact_count": 7},
            "artifacts": {"traceability_matrix": {"mappings": []}},
        }
        architect_handoff = {
            "component_specs": [{"component_name": "AuthenticationService"}],
        }

        def fake_http_json(method, url, payload=None, timeout=60):
            if url.endswith("/api/health"):
                return {"ok": True}
            if url.endswith("/api/runs/preflight"):
                return {"ok": True}
            if url.endswith("/api/runs") and method == "POST":
                return {"ok": True, "run_id": "run_123"}
            if url.endswith("/api/runs/run_123") and method == "GET":
                return {
                    "ok": True,
                    "run": {
                        "run_id": "run_123",
                        "status": "running",
                        "current_stage": 2,
                        "pipeline_state": {},
                    },
                }
            if url.endswith("/api/runs/run_123/architect-package"):
                return {"ok": True, "architect_package": architect_package}
            if url.endswith("/api/runs/run_123/architect-handoff"):
                return {"ok": True, "architect_handoff_package": architect_handoff}
            raise AssertionError(f"Unexpected request: {method} {url}")

        class _FakeBytesResponse:
            def __init__(self, payload: bytes, headers: dict[str, str]):
                self._payload = payload
                self.headers = headers

            def read(self):
                return self._payload

            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

        with tempfile.TemporaryDirectory() as temp_dir:
            args = SimpleNamespace(
                base_url="https://synthetix.example.com",
                repo_url="https://github.com/example/bank-vb6",
                provider="openai",
                model="gpt-5",
                target_language="C#",
                objectives=cli.DEFAULT_OBJECTIVE,
                analysis_depth="deep",
                poll_seconds=0,
                timeout_seconds=60,
                output_dir=temp_dir,
                abort_after_export=False,
                approved_adrs=[],
                approved_services=[],
                deferred_services=[],
                phase_overrides=[],
            )
            with patch.object(cli, "_http_json", side_effect=fake_http_json):
                with patch.object(
                    cli,
                    "urlopen",
                    side_effect=[
                        _FakeBytesResponse(b"legacy-docx", {"Content-Disposition": 'attachment; filename="Legacy-HLD-demo.docx"'}),
                        _FakeBytesResponse(b"target-docx", {"Content-Disposition": 'attachment; filename="Target-HLD-demo.docx"'}),
                    ],
                ):
                    with patch.object(cli, "_fetch_component_handoffs", return_value={"AuthenticationService": {"artifact_type": "component_scoped_handoff_v1"}}):
                        exit_code = cli.run(args)

            self.assertEqual(exit_code, 0)
            output_dir = Path(temp_dir)
            self.assertTrue((output_dir / "architect_package.json").exists())
            self.assertTrue((output_dir / "architect_handoff_package.json").exists())
            self.assertTrue((output_dir / "components" / "AuthenticationService.json").exists())
            self.assertTrue((output_dir / "Legacy-HLD-demo.docx").exists())
            self.assertTrue((output_dir / "Target-HLD-demo.docx").exists())
            summary = json.loads((output_dir / "summary.json").read_text(encoding="utf-8"))
            self.assertEqual(summary.get("run_id"), "run_123")
            self.assertEqual(summary.get("component_handoff_count"), 1)
            self.assertEqual(summary.get("hld_documents"), {"legacy": "Legacy-HLD-demo.docx", "target": "Target-HLD-demo.docx"})


if __name__ == "__main__":
    unittest.main()
