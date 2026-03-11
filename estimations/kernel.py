from __future__ import annotations

from typing import Any

from estimations.types import load_artifact_json


DEFAULT_BROWNFIELD_TASKS = [
    {"kind": "DISCOVERY", "pct": 0.2},
    {"kind": "DESIGN", "pct": 0.1},
    {"kind": "IMPLEMENT", "pct": 0.5},
    {"kind": "TEST", "pct": 0.15},
    {"kind": "DEPLOY", "pct": 0.05},
]

SHARED_WBS_ITEMS = [
    {
        "id": "WBS-SHARED-INFRA",
        "kind": "SHARED",
        "title": "Shared: CI/CD, environments, and baseline observability",
        "estimated_hours_likely": 120.0,
        "tasks": [
            {"kind": "DISCOVERY", "pct": 0.15},
            {"kind": "IMPLEMENT", "pct": 0.55},
            {"kind": "TEST", "pct": 0.2},
            {"kind": "DEPLOY", "pct": 0.1},
        ],
    },
    {
        "id": "WBS-SHARED-GOV",
        "kind": "SHARED",
        "title": "Shared: security, governance, and audit evidence pack",
        "estimated_hours_likely": 80.0,
        "tasks": [
            {"kind": "DISCOVERY", "pct": 0.25},
            {"kind": "DESIGN", "pct": 0.25},
            {"kind": "IMPLEMENT", "pct": 0.3},
            {"kind": "TEST", "pct": 0.2},
        ],
    },
]


def _traceability_multiplier(score: int) -> float:
    if score >= 80:
        return 0.90
    if score >= 60:
        return 1.00
    if score >= 40:
        return 1.15
    return 1.30


def _risk_counts(risks: list[dict[str, Any]], chunk_id: str) -> dict[str, int]:
    counts = {"high": 0, "medium": 0, "low": 0}
    for risk in risks:
        if chunk_id not in (risk.get("applies_to_chunks") or []):
            continue
        severity = str(risk.get("severity", "")).lower()
        if severity in counts:
            counts[severity] += 1
    return counts


def _risk_multiplier(counts: dict[str, int]) -> float:
    return 1 + (0.15 * counts["high"]) + (0.08 * counts["medium"]) + (0.03 * counts["low"])


def _chunk_title(chunk_name: str) -> str:
    return f"Migrate {chunk_name} subsystem"


def build_brownfield_wbs(
    chunk_manifest: dict[str, Any],
    risk_register: dict[str, Any],
    traceability_scores: dict[str, Any],
) -> dict[str, Any]:
    items: list[dict[str, Any]] = []
    traceability_by_chunk = dict((traceability_scores or {}).get("by_chunk") or {})
    risks = list((risk_register or {}).get("risks") or [])

    for chunk in list((chunk_manifest or {}).get("chunks") or []):
        chunk_id = str(chunk["chunk_id"])
        line_count = int(chunk.get("line_count") or 0)
        complexity_score = float(chunk.get("complexity_score") or 0.0)
        complexity_points = round((line_count / 1000.0) * complexity_score, 3)
        risk_counts = _risk_counts(risks, chunk_id)
        traceability_score = int(traceability_by_chunk.get(chunk_id, 0))
        estimated_hours_likely = round(
            complexity_points * 10.0 * _risk_multiplier(risk_counts) * _traceability_multiplier(traceability_score),
            1,
        )
        items.append(
            {
                "id": f"WBS-{chunk_id.upper()}",
                "kind": "BROWNFIELD_CHUNK",
                "title": _chunk_title(str(chunk.get("name") or chunk_id)),
                "chunk_ref": chunk_id,
                "line_count": line_count,
                "tables_owned": list(chunk.get("tables_owned") or []),
                "estimated_hours_likely": estimated_hours_likely,
                "complexity_points": complexity_points,
                "risk_counts": risk_counts,
                "traceability_score": traceability_score,
                "tasks": [dict(task) for task in DEFAULT_BROWNFIELD_TASKS],
            }
        )

    items.extend(SHARED_WBS_ITEMS)
    return {
        "generated_at": (chunk_manifest or {}).get("generated_at") or (risk_register or {}).get("generated_at"),
        "source": "chunk_manifest + risk_register + traceability_scores",
        "wbs_items": items,
    }


def build_brownfield_wbs_from_files(
    chunk_manifest_path: str,
    risk_register_path: str,
    traceability_scores_path: str,
) -> dict[str, Any]:
    return build_brownfield_wbs(
        load_artifact_json(chunk_manifest_path),
        load_artifact_json(risk_register_path),
        load_artifact_json(traceability_scores_path),
    )
