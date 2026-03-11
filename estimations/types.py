from __future__ import annotations

from dataclasses import dataclass
import json
from pathlib import Path
from typing import Any

import jsonschema


ROOT = Path(__file__).resolve().parents[1]
SCHEMA_DIR = ROOT / "schemas"


SCHEMA_FILE_BY_KIND = {
    "estimation_input_v1": "estimation_input_v1.schema.json",
    "wbs_v1": "wbs_v1.schema.json",
    "assumption_ledger_v1": "assumption_ledger_v1.schema.json",
    "team_model_library_v1": "team_model_library_v1.schema.json",
    "estimate_summary_v1": "estimate_summary_v1.schema.json",
}


def schema_path(kind: str) -> Path:
    filename = SCHEMA_FILE_BY_KIND.get(kind)
    if not filename:
        raise KeyError(f"Unsupported estimation artifact kind: {kind}")
    return SCHEMA_DIR / filename


def load_artifact_json(path: str | Path) -> dict[str, Any]:
    return json.loads(Path(path).read_text(encoding="utf-8"))


def _schema_store() -> dict[str, dict[str, Any]]:
    store: dict[str, dict[str, Any]] = {}
    for schema_file in SCHEMA_DIR.glob("*.json"):
        payload = load_artifact_json(schema_file)
        store[schema_file.resolve().as_uri()] = payload
        schema_id = payload.get("$id")
        if schema_id:
            store[str(schema_id)] = payload
    return store


def _build_schema_resolver(base_schema_path: Path) -> jsonschema.RefResolver:
    base_uri = base_schema_path.resolve().as_uri()
    return jsonschema.RefResolver(
        base_uri=base_uri,
        referrer=load_artifact_json(base_schema_path),
        store=_schema_store(),
    )


def validate_artifact_json(kind: str, payload: dict[str, Any]) -> None:
    schema_file = schema_path(kind)
    schema = load_artifact_json(schema_file)
    resolver = _build_schema_resolver(schema_file)
    jsonschema.validate(instance=payload, schema=schema, resolver=resolver)


@dataclass(frozen=True)
class ArtifactEnvelope:
    payload: dict[str, Any]
    kind: str

    def validate(self) -> None:
        validate_artifact_json(self.kind, self.payload)


@dataclass(frozen=True)
class EstimationInputArtifact(ArtifactEnvelope):
    def __init__(self, payload: dict[str, Any]):
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "kind", "estimation_input_v1")


@dataclass(frozen=True)
class WBSArtifact(ArtifactEnvelope):
    def __init__(self, payload: dict[str, Any]):
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "kind", "wbs_v1")


@dataclass(frozen=True)
class AssumptionLedgerArtifact(ArtifactEnvelope):
    def __init__(self, payload: dict[str, Any]):
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "kind", "assumption_ledger_v1")


@dataclass(frozen=True)
class EstimateSummaryArtifact(ArtifactEnvelope):
    def __init__(self, payload: dict[str, Any]):
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "kind", "estimate_summary_v1")


@dataclass(frozen=True)
class TeamModelLibraryArtifact(ArtifactEnvelope):
    def __init__(self, payload: dict[str, Any]):
        object.__setattr__(self, "payload", payload)
        object.__setattr__(self, "kind", "team_model_library_v1")


@dataclass(frozen=True)
class TeamModelLibrary:
    payload: dict[str, Any]

    @property
    def weekly_capacity_hours(self) -> float:
        return float(self.payload.get("weekly_capacity_hours", 0))

    @property
    def models(self) -> dict[str, dict[str, Any]]:
        return dict(self.payload.get("models") or {})
