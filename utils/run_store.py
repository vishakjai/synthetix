"""
Persistent storage for pipeline runs.

Each run is stored under:
  <root>/<run_id>/
    meta.json
    state.json
    stage_<n>.json
"""

from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class PipelineRunStore:
    """File-backed run history for pipeline outputs and logs."""

    def __init__(self, root_dir: str):
        self.root = Path(root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _run_dir(self, run_id: str) -> Path:
        return self.root / run_id

    @staticmethod
    def _write_json(path: Path, payload: dict[str, Any]) -> None:
        tmp = path.with_suffix(path.suffix + f".{uuid.uuid4().hex}.tmp")
        tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
        tmp.replace(path)

    def create_run(self, business_objectives: str, config_summary: dict[str, Any]) -> str:
        run_id = datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8]
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id": run_id,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "status": "running",
            "business_objectives": business_objectives,
            "config": config_summary,
        }
        self._write_json(run_dir / "meta.json", meta)
        self._write_json(
            run_dir / "state.json",
            {
                "run_id": run_id,
                "pipeline_status": "running",
                "pipeline_state": None,
                "stage_status": {},
                "progress_logs": [],
                "error_message": None,
                "saved_at": _utc_now_iso(),
            },
        )
        return run_id

    def save_stage_snapshot(
        self,
        run_id: str,
        stage: int,
        stage_result: dict[str, Any],
        pipeline_state: dict[str, Any],
        stage_status: dict[int, str],
        progress_logs: list[str],
    ) -> None:
        run_dir = self._run_dir(run_id)
        if not run_dir.exists():
            return

        self._write_json(
            run_dir / f"stage_{stage}.json",
            {
                "run_id": run_id,
                "stage": stage,
                "result": stage_result,
                "saved_at": _utc_now_iso(),
            },
        )
        self._save_state_payload(
            run_id=run_id,
            pipeline_status=pipeline_state.get("pipeline_status", "running"),
            pipeline_state=pipeline_state,
            stage_status=stage_status,
            progress_logs=progress_logs,
            error_message=None,
        )

    def finalize_run(
        self,
        run_id: str,
        status: str,
        pipeline_state: dict[str, Any] | None,
        stage_status: dict[int, str],
        progress_logs: list[str],
        error_message: str | None = None,
    ) -> None:
        run_dir = self._run_dir(run_id)
        if not run_dir.exists():
            return

        self._save_state_payload(
            run_id=run_id,
            pipeline_status=status,
            pipeline_state=pipeline_state,
            stage_status=stage_status,
            progress_logs=progress_logs,
            error_message=error_message,
        )

        meta_path = run_dir / "meta.json"
        if meta_path.exists():
            try:
                meta = json.loads(meta_path.read_text())
            except json.JSONDecodeError:
                meta = {"run_id": run_id}
            meta["status"] = status
            meta["updated_at"] = _utc_now_iso()
            self._write_json(meta_path, meta)

    def _save_state_payload(
        self,
        run_id: str,
        pipeline_status: str,
        pipeline_state: dict[str, Any] | None,
        stage_status: dict[int, str],
        progress_logs: list[str],
        error_message: str | None,
    ) -> None:
        run_dir = self._run_dir(run_id)
        self._write_json(
            run_dir / "state.json",
            {
                "run_id": run_id,
                "pipeline_status": pipeline_status,
                "pipeline_state": pipeline_state,
                "stage_status": {str(k): v for k, v in stage_status.items()},
                "progress_logs": progress_logs,
                "error_message": error_message,
                "saved_at": _utc_now_iso(),
            },
        )

    def list_runs(self, limit: int = 25) -> list[dict[str, Any]]:
        items: list[dict[str, Any]] = []
        for run_dir in self.root.iterdir():
            if not run_dir.is_dir():
                continue
            meta_path = run_dir / "meta.json"
            if not meta_path.exists():
                continue
            try:
                meta = json.loads(meta_path.read_text())
            except json.JSONDecodeError:
                continue
            items.append(meta)

        items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return items[:limit]

    def load_run(self, run_id: str) -> dict[str, Any] | None:
        state_path = self._run_dir(run_id) / "state.json"
        if not state_path.exists():
            return None
        try:
            return json.loads(state_path.read_text())
        except json.JSONDecodeError:
            return None

    def load_meta(self, run_id: str) -> dict[str, Any] | None:
        meta_path = self._run_dir(run_id) / "meta.json"
        if not meta_path.exists():
            return None
        try:
            payload = json.loads(meta_path.read_text())
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None

    def load_stage_snapshot(self, run_id: str, stage: int) -> dict[str, Any] | None:
        if stage < 0:
            return None
        path = self._run_dir(run_id) / f"stage_{stage}.json"
        if not path.exists():
            return None
        try:
            payload = json.loads(path.read_text())
        except json.JSONDecodeError:
            return None
        return payload if isinstance(payload, dict) else None
