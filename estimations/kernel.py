from __future__ import annotations

from decimal import Decimal, ROUND_DOWN, ROUND_HALF_DOWN
from typing import Any

from estimations.calibration import load_team_model_library
from estimations.types import TeamModelLibrary, load_artifact_json


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


def _estimate_range(likely_hours: float) -> dict[str, float]:
    return {
        "best": round(likely_hours * 0.85, 1),
        "likely": round(likely_hours, 1),
        "worst": round(likely_hours * 1.25, 1),
    }


def _round_tenth_half_down(value: float) -> float:
    return float(Decimal(str(value)).quantize(Decimal("0.1"), rounding=ROUND_HALF_DOWN))


def _scale_half_down(value: float, factor: str, epsilon: str = "0.0") -> float:
    scaled = (Decimal(str(value)) * Decimal(factor)) + Decimal(epsilon)
    return float(scaled.quantize(Decimal("0.1"), rounding=ROUND_HALF_DOWN))


def _scale_floor_tenth(value: float, factor: str) -> float:
    scaled = Decimal(str(value)) * Decimal(factor)
    return float(scaled.quantize(Decimal("0.1"), rounding=ROUND_DOWN))


def _effective_task_multiplier(tasks: list[dict[str, Any]], acceleration: dict[str, Any]) -> float:
    effective = 0.0
    for task in tasks:
        task_kind = str(task["kind"])
        pct = float(task["pct"])
        accel = float(acceleration.get(task_kind, 1.0))
        effective += pct / accel
    return effective


def _weekly_team_capacity(team: dict[str, Any], weekly_capacity_hours: float) -> float:
    return round(sum(float(fte) for fte in team.values()) * float(weekly_capacity_hours), 4)


def _buffer_weeks(wbs: dict[str, Any]) -> float:
    high_risk_count = 0
    for item in list(wbs.get("wbs_items") or []):
        risk_counts = item.get("risk_counts") or {}
        high_risk_count += int(risk_counts.get("high") or 0)
    return 1.0 if high_risk_count >= 2 else 0.5


def apply_team_model_to_wbs(wbs: dict[str, Any], library: TeamModelLibrary, model_key: str) -> dict[str, Any]:
    models = library.models
    if model_key not in models:
        raise KeyError(f"Unknown team model: {model_key}")

    model = models[model_key]
    acceleration = dict(model.get("acceleration") or {})
    role_split = dict(model.get("role_split") or {})
    default_team = dict(model.get("default_team") or {})
    weekly_capacity_hours = float(library.weekly_capacity_hours)

    item_summaries: list[dict[str, Any]] = []
    total_likely_raw = 0.0

    for item in list(wbs.get("wbs_items") or []):
        likely_raw = float(item["estimated_hours_likely"]) * _effective_task_multiplier(list(item.get("tasks") or []), acceleration)
        likely = round(likely_raw, 1)
        total_likely_raw += likely_raw
        item_summaries.append(
            {
                "id": item["id"],
                "title": item["title"],
                "hours_likely": likely,
            }
        )

    total_likely = round(total_likely_raw, 1)
    total_best = _scale_floor_tenth(total_likely, "0.85")
    total_worst = _scale_half_down(total_likely, "1.25")

    team_capacity = _weekly_team_capacity(default_team, weekly_capacity_hours)
    buffer = _buffer_weeks(wbs)
    timeline_likely = round((total_likely / team_capacity) + buffer, 1)
    timeline_best = _scale_floor_tenth(timeline_likely, "0.85")
    timeline_worst = _scale_half_down(timeline_likely, "1.25")
    role_totals = {
        role: round(total_likely * float(share), 1)
        for role, share in role_split.items()
    }

    return {
        "model_key": model_key,
        "model_name": model.get("name", model_key),
        "team": default_team,
        "weekly_capacity_hours": int(weekly_capacity_hours) if weekly_capacity_hours.is_integer() else weekly_capacity_hours,
        "total_hours_likely": total_likely,
        "total_hours_best": total_best,
        "total_hours_worst": total_worst,
        "timeline_weeks_likely": timeline_likely,
        "timeline_weeks_best": timeline_best,
        "timeline_weeks_worst": timeline_worst,
        "hours_by_role": {role: round(hours, 1) for role, hours in role_totals.items()},
        "items": item_summaries,
    }


def apply_team_model_to_wbs_from_files(wbs_path: str, team_models_path: str, model_key: str) -> dict[str, Any]:
    return apply_team_model_to_wbs(
        load_artifact_json(wbs_path),
        load_team_model_library(team_models_path),
        model_key,
    )
