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
import os
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    storage = None

try:
    from google.cloud import firestore  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    firestore = None


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

    def create_run(
        self,
        business_objectives: str,
        config_summary: dict[str, Any],
        *,
        run_id: str | None = None,
        initial_status: str = "running",
    ) -> str:
        run_id = str(run_id or "").strip() or (datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])
        run_dir = self._run_dir(run_id)
        run_dir.mkdir(parents=True, exist_ok=True)

        meta = {
            "run_id": run_id,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "status": initial_status,
            "business_objectives": business_objectives,
            "config": config_summary,
        }
        self._write_json(run_dir / "meta.json", meta)
        self._write_json(
            run_dir / "state.json",
            {
                "run_id": run_id,
                "pipeline_status": initial_status,
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


class GcsPipelineRunStore:
    """GCS-backed run history shared across Cloud Run instances."""

    def __init__(self, bucket: str, prefix: str = "pipeline_runs"):
        if storage is None:
            raise RuntimeError("google-cloud-storage is required for GCS run store backend")
        self.bucket_name = bucket
        self.prefix = prefix.strip("/ ")
        client = storage.Client()
        self.bucket = client.bucket(bucket)

    def _path(self, run_id: str, filename: str) -> str:
        base = f"{self.prefix}/{run_id}" if self.prefix else run_id
        return f"{base}/{filename}"

    def _read_json(self, path: str) -> dict[str, Any] | None:
        blob = self.bucket.blob(path)
        if not blob.exists():
            return None
        try:
            raw = blob.download_as_text()
            payload = json.loads(raw)
            return payload if isinstance(payload, dict) else None
        except Exception:
            return None

    def _write_json(self, path: str, payload: dict[str, Any]) -> None:
        blob = self.bucket.blob(path)
        blob.upload_from_string(
            json.dumps(payload, indent=2, ensure_ascii=True, default=str),
            content_type="application/json",
        )

    def create_run(
        self,
        business_objectives: str,
        config_summary: dict[str, Any],
        *,
        run_id: str | None = None,
        initial_status: str = "running",
    ) -> str:
        run_id = str(run_id or "").strip() or (datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])
        meta = {
            "run_id": run_id,
            "created_at": _utc_now_iso(),
            "updated_at": _utc_now_iso(),
            "status": initial_status,
            "business_objectives": business_objectives,
            "config": config_summary,
        }
        self._write_json(self._path(run_id, "meta.json"), meta)
        self._write_json(
            self._path(run_id, "state.json"),
            {
                "run_id": run_id,
                "pipeline_status": initial_status,
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
        self._write_json(
            self._path(run_id, f"stage_{stage}.json"),
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
        self._save_state_payload(
            run_id=run_id,
            pipeline_status=status,
            pipeline_state=pipeline_state,
            stage_status=stage_status,
            progress_logs=progress_logs,
            error_message=error_message,
        )
        meta = self.load_meta(run_id) or {"run_id": run_id, "created_at": _utc_now_iso()}
        meta["status"] = status
        meta["updated_at"] = _utc_now_iso()
        self._write_json(self._path(run_id, "meta.json"), meta)

    def _save_state_payload(
        self,
        run_id: str,
        pipeline_status: str,
        pipeline_state: dict[str, Any] | None,
        stage_status: dict[int, str],
        progress_logs: list[str],
        error_message: str | None,
    ) -> None:
        self._write_json(
            self._path(run_id, "state.json"),
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
        base = f"{self.prefix}/" if self.prefix else ""
        blobs = self.bucket.list_blobs(prefix=base)
        items: list[dict[str, Any]] = []
        suffix = "/meta.json"
        for blob in blobs:
            name = str(blob.name or "")
            if not name.endswith(suffix):
                continue
            payload = self._read_json(name)
            if isinstance(payload, dict):
                items.append(payload)
        items.sort(key=lambda x: x.get("created_at", ""), reverse=True)
        return items[:limit]

    def load_run(self, run_id: str) -> dict[str, Any] | None:
        return self._read_json(self._path(run_id, "state.json"))

    def load_meta(self, run_id: str) -> dict[str, Any] | None:
        return self._read_json(self._path(run_id, "meta.json"))

    def load_stage_snapshot(self, run_id: str, stage: int) -> dict[str, Any] | None:
        if stage < 0:
            return None
        return self._read_json(self._path(run_id, f"stage_{stage}.json"))


class FirestorePipelineRunStore:
    """Firestore-backed run history shared across Cloud Run instances."""

    def __init__(self, collection: str = "pipeline_runs"):
        if firestore is None:
            raise RuntimeError("google-cloud-firestore is required for Firestore run store backend")
        self.collection = str(collection or "pipeline_runs").strip() or "pipeline_runs"
        self.db = firestore.Client()

    def _doc(self, run_id: str):
        return self.db.collection(self.collection).document(run_id)

    def _stage_doc(self, run_id: str, stage: int):
        return self._doc(run_id).collection("stages").document(str(stage))

    def create_run(
        self,
        business_objectives: str,
        config_summary: dict[str, Any],
        *,
        run_id: str | None = None,
        initial_status: str = "running",
    ) -> str:
        run_id = str(run_id or "").strip() or (datetime.now().strftime("%Y%m%d_%H%M%S") + "_" + uuid.uuid4().hex[:8])
        now = _utc_now_iso()
        self._doc(run_id).set(
            {
                "run_id": run_id,
                "created_at": now,
                "updated_at": now,
                "status": initial_status,
                "business_objectives": business_objectives,
                "config": config_summary,
                "state": {
                    "run_id": run_id,
                    "pipeline_status": initial_status,
                    "pipeline_state": None,
                    "stage_status": {},
                    "progress_logs": [],
                    "error_message": None,
                    "saved_at": now,
                },
            }
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
        self._stage_doc(run_id, stage).set(
            {
                "run_id": run_id,
                "stage": stage,
                "result": stage_result,
                "saved_at": _utc_now_iso(),
            }
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
        self._save_state_payload(
            run_id=run_id,
            pipeline_status=status,
            pipeline_state=pipeline_state,
            stage_status=stage_status,
            progress_logs=progress_logs,
            error_message=error_message,
        )
        now = _utc_now_iso()
        self._doc(run_id).set({"status": status, "updated_at": now}, merge=True)

    def _save_state_payload(
        self,
        run_id: str,
        pipeline_status: str,
        pipeline_state: dict[str, Any] | None,
        stage_status: dict[int, str],
        progress_logs: list[str],
        error_message: str | None,
    ) -> None:
        now = _utc_now_iso()
        self._doc(run_id).set(
            {
                "updated_at": now,
                "status": pipeline_status,
                "state": {
                    "run_id": run_id,
                    "pipeline_status": pipeline_status,
                    "pipeline_state": pipeline_state,
                    "stage_status": {str(k): v for k, v in stage_status.items()},
                    "progress_logs": progress_logs,
                    "error_message": error_message,
                    "saved_at": now,
                },
            },
            merge=True,
        )

    def list_runs(self, limit: int = 25) -> list[dict[str, Any]]:
        query = self.db.collection(self.collection).order_by("created_at", direction=firestore.Query.DESCENDING).limit(limit)
        rows: list[dict[str, Any]] = []
        for snap in query.stream():
            payload = snap.to_dict() or {}
            if isinstance(payload, dict):
                rows.append(
                    {
                        "run_id": payload.get("run_id", snap.id),
                        "created_at": payload.get("created_at", ""),
                        "updated_at": payload.get("updated_at", ""),
                        "status": payload.get("status", ""),
                        "business_objectives": payload.get("business_objectives", ""),
                        "config": payload.get("config", {}),
                    }
                )
        return rows

    def load_run(self, run_id: str) -> dict[str, Any] | None:
        snap = self._doc(run_id).get()
        if not snap.exists:
            return None
        payload = snap.to_dict() or {}
        if not isinstance(payload, dict):
            return None
        state = payload.get("state", {})
        return state if isinstance(state, dict) else None

    def load_meta(self, run_id: str) -> dict[str, Any] | None:
        snap = self._doc(run_id).get()
        if not snap.exists:
            return None
        payload = snap.to_dict() or {}
        if not isinstance(payload, dict):
            return None
        return {
            "run_id": payload.get("run_id", run_id),
            "created_at": payload.get("created_at", ""),
            "updated_at": payload.get("updated_at", ""),
            "status": payload.get("status", ""),
            "business_objectives": payload.get("business_objectives", ""),
            "config": payload.get("config", {}),
        }

    def load_stage_snapshot(self, run_id: str, stage: int) -> dict[str, Any] | None:
        if stage < 0:
            return None
        snap = self._stage_doc(run_id, stage).get()
        if not snap.exists:
            return None
        payload = snap.to_dict() or {}
        return payload if isinstance(payload, dict) else None


def build_pipeline_run_store(root_dir: str) -> PipelineRunStore | GcsPipelineRunStore | FirestorePipelineRunStore:
    backend = str(os.getenv("RUN_STORE_BACKEND", "local")).strip().lower()
    if backend == "firestore":
        collection = str(os.getenv("RUN_STORE_FIRESTORE_COLLECTION", "pipeline_runs")).strip() or "pipeline_runs"
        return FirestorePipelineRunStore(collection=collection)
    if backend == "gcs":
        bucket = str(
            os.getenv("RUN_STORE_GCS_BUCKET")
            or os.getenv("SYNTHETIX_RUN_STORE_BUCKET")
            or ""
        ).strip()
        prefix = str(os.getenv("RUN_STORE_GCS_PREFIX", "pipeline_runs")).strip() or "pipeline_runs"
        if bucket:
            return GcsPipelineRunStore(bucket=bucket, prefix=prefix)
    return PipelineRunStore(root_dir)
