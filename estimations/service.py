from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
import uuid

from estimations.calibration import load_team_model_library
from estimations.kernel import apply_team_model_to_wbs, build_brownfield_wbs
from estimations.storage import EstimationArtifactPaths, EstimationStore
from estimations.types import (
    AssumptionLedgerArtifact,
    EstimateSummaryArtifact,
    EstimationInputArtifact,
    WBSArtifact,
)


DEFAULT_TEAM_MODEL_LIBRARY_PATH = Path(__file__).resolve().parent / "config" / "team_models.yml"


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _artifact_ref(artifact_type: str, artifact_id: str, artifact_version: str = "1.0") -> dict[str, str]:
    return {
        "artifact_type": artifact_type,
        "artifact_id": artifact_id,
        "artifact_version": artifact_version,
    }


def _artifact_meta(artifact_type: str, artifact_id: str, *, source_mode: str = "repo") -> dict[str, Any]:
    return {
        "artifact_type": artifact_type,
        "artifact_version": "1.0",
        "artifact_id": artifact_id,
        "generated_at": _utc_now_iso(),
        "context": {
            "stage": "Estimate",
            "source_mode": source_mode,
            "source_ref": "synthetix://estimations/service",
        },
    }


def _make_artifact_id(prefix: str) -> str:
    return f"art_{prefix}_{uuid.uuid4().hex[:12]}"


def _hours_range(likely: float) -> dict[str, float]:
    return {
        "p10": round(likely * 0.85, 1),
        "p50": round(likely, 1),
        "p90": round(likely * 1.25, 1),
    }


def _size_tier(likely: float) -> str:
    if likely < 40:
        return "XS"
    if likely < 80:
        return "S"
    if likely < 160:
        return "M"
    if likely < 320:
        return "L"
    return "XL"


def _role_hours_range(total_likely: float, share: float) -> dict[str, float]:
    return _hours_range(round(total_likely * share, 1))


def _build_brownfield_input_payload(
    *,
    estimate_artifact_id: str,
    business_need: str,
    chunk_manifest: dict[str, Any],
    risk_register: dict[str, Any],
    traceability_scores: dict[str, Any],
    analysis_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = dict(analysis_context or {})
    decisions = dict(context.get("decisions") or {})
    quality_gates = list(context.get("quality_gates") or [])
    readiness = dict(context.get("modernization_readiness") or {})
    return {
        "meta": _artifact_meta("estimation_input_v1", estimate_artifact_id, source_mode="repo"),
        "intake": {
            "mode": "brownfield",
            "confidence_tier": "PLANNING",
            "intake_notes": [
                f"{len(chunk_manifest.get('chunks') or [])} chunk(s) supplied from decomposer",
                f"{len(risk_register.get('risks') or [])} risk item(s) supplied",
                f"{len((traceability_scores.get('by_chunk') or {}).keys())} traceability score(s) supplied",
                f"{len(list(context.get('functional_requirements') or []))} functional requirement(s) supplied",
                f"{len(list(decisions.get('blocking') or []))} blocking decision(s) supplied",
                f"{len(quality_gates)} quality gate(s) supplied",
                (
                    f"modernization readiness score {int(readiness.get('score') or readiness.get('readiness_score') or 0)}/100"
                    if readiness
                    else "modernization readiness not supplied"
                ),
            ],
        },
        "inputs": {
            "business_need": business_need,
            "source_artifacts": [
                _artifact_ref("chunk_manifest_v1", _make_artifact_id("chunk_manifest")),
                _artifact_ref("risk_register_v1", _make_artifact_id("risk_register")),
                _artifact_ref("traceability_scores_v1", _make_artifact_id("traceability")),
            ],
            "repo_refs": ["synthetix://brownfield/input"],
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


def _build_wbs_artifact_payload(
    *,
    artifact_id: str,
    raw_wbs: dict[str, Any],
    model_summary: dict[str, Any],
) -> dict[str, Any]:
    role_totals = dict(model_summary.get("hours_by_role") or {})
    total_hours = float(model_summary.get("total_hours_likely") or 0.0) or 1.0
    role_shares = {role: float(hours) / total_hours for role, hours in role_totals.items()}

    items: list[dict[str, Any]] = []
    for item in list(raw_wbs.get("wbs_items") or []):
        item_id = str(item["id"])
        likely = float(item.get("estimated_hours_likely") or 0.0)
        title = str(item.get("title") or item_id)
        risk_counts = item.get("risk_counts") or {}
        high_risk = int(risk_counts.get("high") or 0)
        traceability_score = int(item.get("traceability_score") or 0)
        if item_id == "WBS-SHARED-INFRA":
            kind = "FOUNDATION"
            phase = "Build"
        elif item_id == "WBS-SHARED-GOV":
            kind = "PROJECT_MANAGEMENT"
            phase = "Design"
        else:
            kind = "MODULE_MIGRATION"
            phase = "Build"
        items.append(
            {
                "wbs_item_id": item_id,
                "title": title,
                "kind": kind,
                "phase": phase,
                "scope_ref": {
                    "chunk_id": str(item.get("chunk_ref") or ""),
                    "tables_touched": list(item.get("tables_owned") or []),
                },
                "size_tier": _size_tier(likely),
                "effort_hours": _hours_range(likely),
                "effort_hours_risk_adjusted": _hours_range(likely),
                "roles": [
                    {
                        "role": role,
                        "hours": _role_hours_range(likely, share),
                    }
                    for role, share in role_shares.items()
                ],
                "dependencies": [],
                "risk_refs": [f"risk-high-{high_risk}"] if high_risk else [],
                "assumption_refs": [],
                "eligibility": {
                    "agent_led_hitl_allowed": high_risk == 0 and traceability_score >= 40,
                    "eligibility_reasons": (
                        []
                        if high_risk == 0 and traceability_score >= 40
                        else [
                            "high_risk_present" if high_risk else "",
                            "low_traceability" if traceability_score < 40 else "",
                        ]
                    ),
                },
                "notes": [],
            }
        )
    return {
        "meta": _artifact_meta("wbs_v1", artifact_id, source_mode="repo"),
        "wbs": {
            "wbs_id": artifact_id.replace("art_", "wbs_"),
            "items": items,
        },
    }


def _build_assumption_ledger_payload(
    *,
    artifact_id: str,
    business_need: str,
    risk_register: dict[str, Any],
    analysis_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = dict(analysis_context or {})
    decisions = dict(context.get("decisions") or {})
    quality_gates = list(context.get("quality_gates") or [])
    readiness = dict(context.get("modernization_readiness") or {})
    assumptions: list[dict[str, Any]] = [
        {
            "id": "ASSUME-001",
            "category": "GIVEN",
            "statement": f"Estimate produced for business need: {business_need}",
            "status": "resolved",
            "impact": {"schedule_days_p50": 0, "effort_hours_p50": 0, "cost_usd_p50": 0},
            "tags": ["business_need"],
        }
    ]
    high_risks = [r for r in list(risk_register.get("risks") or []) if str(r.get("severity", "")).lower() == "high"]
    if high_risks:
        assumptions.append(
            {
                "id": "ASSUME-002",
                "category": "RISK_ADJUSTED",
                "statement": f"Estimate includes premium for {len(high_risks)} high-severity risk item(s).",
                "status": "accepted_risk",
                "impact": {"schedule_days_p50": 5, "effort_hours_p50": 40, "cost_usd_p50": 0},
                "tags": ["risk_adjusted"],
            }
        )
    for idx, row in enumerate(list(decisions.get("blocking") or []), start=1):
        item = dict(row or {})
        assumptions.append(
            {
                "id": str(item.get("id") or f"BLOCKER-{idx:03d}"),
                "category": "BLOCKER",
                "statement": str(item.get("question") or item.get("description") or "Blocking decision unresolved"),
                "status": "open",
                "impact": {"schedule_days_p50": 5, "effort_hours_p50": 24, "cost_usd_p50": 0},
                "tags": ["blocking_decision"],
            }
        )
    for idx, row in enumerate([g for g in quality_gates if str((g or {}).get("result") or (g or {}).get("status") or "").upper() == "FAIL"], start=1):
        item = dict(row or {})
        assumptions.append(
            {
                "id": str(item.get("id") or f"GATE-{idx:03d}"),
                "category": "RISK_ADJUSTED",
                "statement": f"Quality gate unresolved: {str(item.get('title') or item.get('description') or item.get('id') or 'gate')}",
                "status": "accepted_risk",
                "impact": {"schedule_days_p50": 3, "effort_hours_p50": 16, "cost_usd_p50": 0},
                "tags": ["quality_gate"],
            }
        )
    readiness_score = int(readiness.get("score") or readiness.get("readiness_score") or 0)
    if readiness_score and readiness_score < 65:
        assumptions.append(
            {
                "id": "ASSUME-READINESS",
                "category": "RISK_ADJUSTED",
                "statement": f"Readiness score {readiness_score}/100 requires discovery and contingency premium.",
                "status": "accepted_risk",
                "impact": {"schedule_days_p50": 5, "effort_hours_p50": 32, "cost_usd_p50": 0},
                "tags": ["readiness"],
            }
        )
    return {
        "meta": _artifact_meta("assumption_ledger_v1", artifact_id, source_mode="repo"),
        "assumptions": assumptions,
    }


def _build_estimate_summary_payload(
    *,
    artifact_id: str,
    estimate_id: str,
    team_model_key: str,
    model_summary: dict[str, Any],
    assumption_ledger: dict[str, Any],
    input_ref: dict[str, str],
    wbs_ref: dict[str, str],
    ledger_ref: dict[str, str],
    team_models_ref: dict[str, str],
    risk_register: dict[str, Any],
    analysis_context: dict[str, Any] | None = None,
) -> dict[str, Any]:
    context = dict(analysis_context or {})
    role_totals = dict(model_summary.get("hours_by_role") or {})
    total_likely = float(model_summary.get("total_hours_likely") or 0.0)
    high_risks = [
        {
            "risk_id": str(risk.get("risk_id") or risk.get("id") or f"risk_{idx+1}"),
            "severity": str(risk.get("severity", "medium")).lower(),
            "title": str(risk.get("title") or risk.get("description") or "Risk item"),
            "notes": str(risk.get("notes") or ""),
        }
        for idx, risk in enumerate(list(risk_register.get("risks") or [])[:10])
    ]
    blockers = [
        row.get("statement", "")
        for row in list(assumption_ledger.get("assumptions") or [])
        if row.get("category") == "BLOCKER"
    ]
    phase_rows = []
    for row in list(model_summary.get("phase_breakdown") or []):
        phase_rows.append(
            {
                "phase": str(row.get("phase") or "Phase"),
                "p10_weeks": float(row.get("weeks_best") or 0.0),
                "p50_weeks": float(row.get("weeks_likely") or 0.0),
                "p90_weeks": float(row.get("weeks_worst") or 0.0),
                "notes": [f"{float(row.get('hours_likely') or 0.0):.1f}h p50"],
            }
        )
    return {
        "meta": _artifact_meta("estimate_summary_v1", artifact_id, source_mode="repo"),
        "estimate": {
            "estimate_id": estimate_id,
            "confidence_tier": "PLANNING",
            "team_model_selected": team_model_key,
            "team_model_comparison": [
                {
                    "team_model_id": team_model_key,
                    "timeline_weeks_p50": float(model_summary.get("timeline_weeks_likely") or 0.0),
                    "timeline_weeks_p90": float(model_summary.get("timeline_weeks_worst") or 0.0),
                    "effort_hours_p50": float(model_summary.get("total_hours_likely") or 0.0),
                    "notes": [str(model_summary.get("model_name") or team_model_key)] + [str(x) for x in list(model_summary.get("notes") or [])],
                }
            ],
            "timeline": {
                "total_weeks": {
                    "p10": float(model_summary.get("timeline_weeks_best") or 0.0),
                    "p50": float(model_summary.get("timeline_weeks_likely") or 0.0),
                    "p90": float(model_summary.get("timeline_weeks_worst") or 0.0),
                },
                "p10_weeks": float(model_summary.get("timeline_weeks_best") or 0.0),
                "p50_weeks": float(model_summary.get("timeline_weeks_likely") or 0.0),
                "p90_weeks": float(model_summary.get("timeline_weeks_worst") or 0.0),
                "phase_breakdown": phase_rows,
            },
            "effort": {
                "total_hours": {
                    "p10": float(model_summary.get("total_hours_best") or 0.0),
                    "p50": float(model_summary.get("total_hours_likely") or 0.0),
                    "p90": float(model_summary.get("total_hours_worst") or 0.0),
                },
                "by_role": [
                    {
                        "role": role,
                        "hours": _hours_range(float(hours)),
                    }
                    for role, hours in role_totals.items()
                ],
            },
            "staffing": {
                "roles": {
                    role: {
                        "allocation_pct": round((float(hours) / total_likely) * 100.0, 1) if total_likely else 0.0,
                        "hours_p50": round(float(hours), 1),
                        "fte": round(float((model_summary.get("team") or {}).get(role) or 0.0), 2),
                    }
                    for role, hours in role_totals.items()
                }
            },
            "cost": {"currency": "USD"},
            "key_assumptions": [str(row.get("statement") or "") for row in list(assumption_ledger.get("assumptions") or [])[:8]],
            "blockers": [str(x) for x in blockers if str(x).strip()],
            "risks": high_risks,
            "artifact_refs": {
                "estimation_input": input_ref,
                "wbs": wbs_ref,
                "assumption_ledger": ledger_ref,
                "team_models": team_models_ref,
            },
        },
    }


@dataclass(frozen=True)
class EstimateResult:
    estimate_id: str
    run_id: str | None
    paths: EstimationArtifactPaths
    estimation_input: dict[str, Any]
    wbs: dict[str, Any]
    estimate_summary: dict[str, Any]
    assumption_ledger: dict[str, Any]


def build_brownfield_estimate(
    *,
    chunk_manifest: dict[str, Any],
    risk_register: dict[str, Any],
    traceability_scores: dict[str, Any],
    business_need: str,
    store: EstimationStore,
    run_id: str | None = None,
    estimate_id: str | None = None,
    team_model_key: str = "HUMAN_ONLY",
    team_model_library_path: str | Path = DEFAULT_TEAM_MODEL_LIBRARY_PATH,
    analysis_context: dict[str, Any] | None = None,
) -> EstimateResult:
    paths = store.create_estimate(run_id=run_id, estimate_id=estimate_id)
    estimate_id = paths.estimate_root.name

    input_payload = _build_brownfield_input_payload(
        estimate_artifact_id=_make_artifact_id("estimation_input"),
        business_need=business_need,
        chunk_manifest=chunk_manifest,
        risk_register=risk_register,
        traceability_scores=traceability_scores,
        analysis_context=analysis_context,
    )
    EstimationInputArtifact(input_payload).validate()

    raw_wbs = build_brownfield_wbs(chunk_manifest, risk_register, traceability_scores, analysis_context=analysis_context)
    library = load_team_model_library(team_model_library_path)
    model_summary = apply_team_model_to_wbs(raw_wbs, library, team_model_key)

    wbs_payload = _build_wbs_artifact_payload(
        artifact_id=_make_artifact_id("wbs"),
        raw_wbs=raw_wbs,
        model_summary=model_summary,
    )
    WBSArtifact(wbs_payload).validate()

    ledger_payload = _build_assumption_ledger_payload(
        artifact_id=_make_artifact_id("assumption_ledger"),
        business_need=business_need,
        risk_register=risk_register,
        analysis_context=analysis_context,
    )
    AssumptionLedgerArtifact(ledger_payload).validate()

    input_ref = _artifact_ref("estimation_input_v1", input_payload["meta"]["artifact_id"])
    wbs_ref = _artifact_ref("wbs_v1", wbs_payload["meta"]["artifact_id"])
    ledger_ref = _artifact_ref("assumption_ledger_v1", ledger_payload["meta"]["artifact_id"])
    team_models_ref = _artifact_ref("team_model_library_v1", "art_team_models_default")

    summary_payload = _build_estimate_summary_payload(
        artifact_id=_make_artifact_id("estimate_summary"),
        estimate_id=estimate_id,
        team_model_key=team_model_key,
        model_summary=model_summary,
        assumption_ledger=ledger_payload,
        input_ref=input_ref,
        wbs_ref=wbs_ref,
        ledger_ref=ledger_ref,
        team_models_ref=team_models_ref,
        risk_register=risk_register,
        analysis_context=analysis_context,
    )
    EstimateSummaryArtifact(summary_payload).validate()

    store.save_artifact(paths.estimation_input_path, input_payload)
    store.save_artifact(paths.wbs_path, wbs_payload)
    store.save_artifact(paths.assumption_ledger_path, ledger_payload)
    store.save_artifact(paths.estimate_summary_path, summary_payload)

    return EstimateResult(
        estimate_id=estimate_id,
        run_id=run_id,
        paths=paths,
        estimation_input=input_payload,
        wbs=wbs_payload,
        estimate_summary=summary_payload,
        assumption_ledger=ledger_payload,
    )
