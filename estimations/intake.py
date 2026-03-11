from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from estimations.types import EstimationInputArtifact, load_artifact_json


def _artifact_ref(artifact_type: str, artifact_id: str, artifact_version: str = "1.0") -> dict[str, str]:
    return {
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "artifact_version": artifact_version,
    }


def _meta(artifact_id: str, source_mode: str) -> dict[str, Any]:
    return {
        "artifact_type": "estimation_input_v1",
        "artifact_version": "1.0",
        "artifact_id": artifact_id,
        "generated_at": "2026-03-10T00:00:00Z",
        "context": {
            "stage": "Estimate",
            "source_mode": source_mode,
            "source_ref": "synthetix://estimations/intake",
        },
    }


def _artifact_id_from_path(prefix: str, path: str | Path) -> str:
    stem = Path(path).stem.replace(".", "_").replace("-", "_")
    return f"art_{prefix}_{stem}"


@dataclass(frozen=True)
class BrownfieldIntake:
    chunk_manifest_path: str | Path
    risk_register_path: str | Path
    traceability_scores_path: str | Path
    business_need: str = "Modernize the brownfield application while preserving required business capability."
    repo_ref: str = "fixture://brownfield-repo"

    def build(self) -> EstimationInputArtifact:
        chunk_manifest = load_artifact_json(self.chunk_manifest_path)
        risk_register = load_artifact_json(self.risk_register_path)
        traceability_scores = load_artifact_json(self.traceability_scores_path)
        payload = {
            "meta": _meta("art_estimation_input_brownfield", "repo"),
            "intake": {
                "mode": "brownfield",
                "confidence_tier": "PLANNING",
                "intake_notes": [
                    f"{len(chunk_manifest.get('chunks') or [])} chunk(s) supplied from decomposer",
                    f"{len(risk_register.get('risks') or [])} risk item(s) supplied",
                    f"{len((traceability_scores.get('by_chunk') or {}).keys())} traceability score(s) supplied",
                ],
            },
            "inputs": {
                "business_need": self.business_need,
                "source_artifacts": [
                    _artifact_ref("chunk_manifest_v1", _artifact_id_from_path("chunk_manifest", self.chunk_manifest_path)),
                    _artifact_ref("risk_register_v1", _artifact_id_from_path("risk_register", self.risk_register_path)),
                    _artifact_ref("traceability_scores_v1", _artifact_id_from_path("traceability", self.traceability_scores_path)),
                ],
                "repo_refs": [self.repo_ref],
            },
            "decisions": {
                "migration_strategy": "incremental_modernization",
                "quality_bar": "functional_parity",
                "data_strategy": "keep_db",
                "deployment_target": "unknown",
            },
            "constraints": {
                "preferred_delivery_cadence": "2wk_sprints",
            },
        }
        artifact = EstimationInputArtifact(payload)
        artifact.validate()
        return artifact


@dataclass(frozen=True)
class GreenfieldIntake:
    business_need: str
    tech_specs: list[str] | None = None
    target_stack: list[str] | None = None

    def build(self) -> EstimationInputArtifact:
        payload = {
            "meta": _meta("art_estimation_input_greenfield", "greenfield"),
            "intake": {
                "mode": "greenfield",
                "confidence_tier": "INDICATIVE",
                "intake_notes": ["Greenfield estimate generated from business need and optional specification references."],
            },
            "inputs": {
                "business_need": self.business_need,
                "tech_specs": list(self.tech_specs or []),
            },
            "decisions": {
                "target_stack": list(self.target_stack or []),
                "migration_strategy": "rewrite",
                "quality_bar": "functional_parity",
                "data_strategy": "unknown",
                "deployment_target": "unknown",
            },
            "constraints": {
                "preferred_delivery_cadence": "unknown",
            },
        }
        artifact = EstimationInputArtifact(payload)
        artifact.validate()
        return artifact


@dataclass(frozen=True)
class NaturalLanguageIntake:
    business_need: str
    intake_notes: list[str] | None = None

    def build(self) -> EstimationInputArtifact:
        payload = {
            "meta": _meta("art_estimation_input_nl", "greenfield"),
            "intake": {
                "mode": "natural_language",
                "confidence_tier": "INDICATIVE",
                "intake_notes": list(self.intake_notes or ["Natural-language intake requires explicit assumptions until supporting artifacts are attached."]),
            },
            "inputs": {
                "business_need": self.business_need,
            },
            "decisions": {
                "migration_strategy": "unknown",
                "quality_bar": "unknown",
                "data_strategy": "unknown",
                "deployment_target": "unknown",
            },
            "constraints": {
                "preferred_delivery_cadence": "unknown",
            },
        }
        artifact = EstimationInputArtifact(payload)
        artifact.validate()
        return artifact
