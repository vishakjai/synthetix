from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now_compact() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str), encoding="utf-8")


@dataclass(frozen=True)
class EstimationArtifactPaths:
    estimate_root: Path

    @property
    def estimation_input_path(self) -> Path:
        return self.estimate_root / "estimation_input_v1.json"

    @property
    def wbs_path(self) -> Path:
        return self.estimate_root / "wbs_v1.json"

    @property
    def estimate_summary_path(self) -> Path:
        return self.estimate_root / "estimate_summary_v1.json"

    @property
    def assumption_ledger_path(self) -> Path:
        return self.estimate_root / "assumption_ledger_v1.json"


class EstimationStore:
    def __init__(self, pipeline_run_root: str | Path):
        self.root = Path(pipeline_run_root)
        self.root.mkdir(parents=True, exist_ok=True)

    def estimate_dir(self, estimate_id: str, *, run_id: str | None = None) -> Path:
        if run_id:
            return self.root / run_id / "estimates" / estimate_id
        return self.root / "_estimates" / estimate_id

    def create_estimate(self, *, run_id: str | None = None, estimate_id: str | None = None) -> EstimationArtifactPaths:
        estimate_id = str(estimate_id or f"estimate_{_utc_now_compact()}")
        estimate_root = self.estimate_dir(estimate_id, run_id=run_id)
        estimate_root.mkdir(parents=True, exist_ok=True)
        _write_json(
            estimate_root / "meta.json",
            {
                "estimate_id": estimate_id,
                "run_id": run_id or "",
                "created_at": datetime.now(timezone.utc).isoformat(),
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "status": "draft",
            },
        )
        return EstimationArtifactPaths(estimate_root=estimate_root)

    def save_artifact(self, target: Path, payload: dict[str, Any]) -> None:
        _write_json(target, payload)
        meta_path = target.parent / "meta.json"
        if meta_path.exists():
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            meta["updated_at"] = datetime.now(timezone.utc).isoformat()
            _write_json(meta_path, meta)

    def load_artifact(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        try:
            return json.loads(path.read_text(encoding="utf-8"))
        except json.JSONDecodeError:
            return None

    def load_meta(self, estimate_root: Path) -> dict[str, Any] | None:
        return self.load_artifact(estimate_root / "meta.json")

    def list_estimates(self, *, run_id: str | None = None) -> list[dict[str, Any]]:
        base = self.root / run_id / "estimates" if run_id else self.root / "_estimates"
        if not base.exists():
            return []
        rows: list[dict[str, Any]] = []
        for child in sorted(base.iterdir(), key=lambda p: p.name):
            if not child.is_dir():
                continue
            meta = self.load_meta(child) or {}
            meta["estimate_id"] = meta.get("estimate_id") or child.name
            meta["path"] = str(child)
            rows.append(meta)
        return rows

    def find_estimate(self, estimate_id: str) -> Path | None:
        standalone = self.root / "_estimates" / estimate_id
        if standalone.exists():
            return standalone
        for run_dir in self.root.iterdir():
            if not run_dir.is_dir() or run_dir.name.startswith("_"):
                continue
            candidate = run_dir / "estimates" / estimate_id
            if candidate.exists():
                return candidate
        return None

    def load_estimate_bundle(self, estimate_id: str) -> dict[str, Any] | None:
        estimate_root = self.find_estimate(estimate_id)
        if not estimate_root:
            return None
        paths = EstimationArtifactPaths(estimate_root=estimate_root)
        return {
            "estimate_id": estimate_id,
            "meta": self.load_meta(estimate_root) or {},
            "artifacts": {
                "estimation_input": self.load_artifact(paths.estimation_input_path) or {},
                "wbs": self.load_artifact(paths.wbs_path) or {},
                "estimate_summary": self.load_artifact(paths.estimate_summary_path) or {},
                "assumption_ledger": self.load_artifact(paths.assumption_ledger_path) or {},
            },
        }
