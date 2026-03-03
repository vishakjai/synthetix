"""
Delivery Constitution generator for run-context pinning.
"""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _legacy_skill_hint(integration_context: dict[str, Any]) -> str:
    integration = _as_dict(integration_context)
    discover = _as_dict(integration.get("discover_cache"))
    analyst_summary = _as_dict(discover.get("analyst_summary"))
    legacy_skill_profile = _as_dict(analyst_summary.get("legacy_skill_profile"))
    primary = _clean(legacy_skill_profile.get("primary_skill")).lower()
    if primary:
        return primary

    vb6_analysis = _as_dict(analyst_summary.get("vb6_analysis"))
    if vb6_analysis:
        return "vb6"
    return ""


def _known_failure_modes(skill_hint: str) -> list[dict[str, Any]]:
    if skill_hint == "vb6":
        return [
            {
                "id": "VB6-001",
                "label": "Default form instances",
                "pattern": "Implicit global/default form references.",
                "mitigation": "Refactor to explicit instance lifecycle before migration.",
            },
            {
                "id": "VB6-002",
                "label": "ADO implicit recordset updates",
                "pattern": "Recordset cursor updates hidden in UI event handlers.",
                "mitigation": "Isolate data access and add parity tests on read/write flows.",
            },
            {
                "id": "VB6-003",
                "label": "COM/OCX registration coupling",
                "pattern": "Runtime depends on machine-level COM registration.",
                "mitigation": "Inventory controls and map each to target replacements.",
            },
        ]
    if skill_hint == "cobol":
        return [
            {
                "id": "COBOL-001",
                "label": "Copybook drift",
                "pattern": "Data structure contracts diverge between copybook versions.",
                "mitigation": "Pin copybook versions and validate record parity tests.",
            },
            {
                "id": "COBOL-002",
                "label": "Packed-decimal semantics",
                "pattern": "Incorrect COMP-3 handling causes silent financial errors.",
                "mitigation": "Add deterministic decimal conversion tests before cutover.",
            },
        ]
    return [
        {
            "id": "GEN-001",
            "label": "Behavioral parity drift",
            "pattern": "Modernized implementation diverges from legacy runtime behavior.",
            "mitigation": "Use evidence-linked acceptance + regression parity checks.",
        },
        {
            "id": "GEN-002",
            "label": "Data contract breakage",
            "pattern": "Schema/API assumptions changed without migration controls.",
            "mitigation": "Require approved migration plan and rollback evidence.",
        },
    ]


def build_delivery_constitution_v1(
    *,
    run_id: str,
    workspace: str,
    project: str,
    objectives: str,
    use_case: str,
    integration_context: dict[str, Any],
    knowledge_context: dict[str, Any],
    stage_agent_ids: dict[str, Any] | None = None,
) -> dict[str, Any]:
    integration = _as_dict(integration_context)
    stage_map = stage_agent_ids if isinstance(stage_agent_ids, dict) else {}
    brownfield = _as_dict(integration.get("brownfield"))
    greenfield = _as_dict(integration.get("greenfield"))
    repo_ref = _clean(brownfield.get("repo_url")) or _clean(greenfield.get("repo_target"))
    project_state = _clean(integration.get("project_state_detected")).lower() or "unknown"
    strict_security = bool(integration.get("strict_security_mode", False))
    source_integrity = _as_dict(_as_dict(knowledge_context).get("integrity"))
    source_versions = [
        _clean(item) for item in _as_list(source_integrity.get("source_version_ids")) if _clean(item)
    ]
    domain_pack_id = _clean(integration.get("domain_pack_id")) or _clean(integration.get("domain_pack_selection"))
    skill_hint = _legacy_skill_hint(integration)

    non_negotiables = [
        "Preserve legacy behavior first; any deviation requires explicit approval and tests.",
        "Every major requirement/risk must be traceable to extracted evidence artifacts.",
        "Do not change data contracts without migration and rollback plans.",
        "Block planning when unresolved blockers exist (scope, IAM, schema-key, compliance).",
    ]
    if strict_security:
        non_negotiables.append("Security/compliance constraints are mandatory and cannot be downgraded to warnings.")
    if domain_pack_id:
        non_negotiables.append(f"Domain pack constraints must be applied: {domain_pack_id}.")
    if source_versions:
        non_negotiables.append("Use only pinned knowledge snapshots for this run; do not rely on mutable latest sources.")

    orchestration_hints = [
        {
            "when": "VB6 form/control/event patterns detected (.frm/.frx/.bas/.cls)",
            "route_to": "VB6 UI/Forms specialist",
            "reason": "Preserve event behavior and control wiring parity.",
        },
        {
            "when": "SQL or ADO patterns detected (Recordset, Open, Execute, concatenated SQL)",
            "route_to": "Data/SQL specialist",
            "reason": "Prevent injection and data-contract regressions.",
        },
        {
            "when": "Security/compliance risk detected (credentials, auth gaps, missing constraints)",
            "route_to": "Security/Compliance specialist",
            "reason": "Force citation-backed remediation and blocking decisions.",
        },
    ]

    checklists = {
        "pre_change": [
            "Confirm scope variant/project decision is recorded.",
            "Verify source snapshot and context bundle IDs are pinned.",
            "Review top blocking risks and required decisions before planning.",
        ],
        "post_change": [
            "Re-run QA structural and semantic gates.",
            "Re-validate traceability links (event/sql/rule/risk).",
            "Update delivery docs/artifacts for any changed behavior assumptions.",
        ],
    }

    linked_artifacts = [
        {"name": "system_context_model", "ref": "artifact://context/system_context_model"},
        {"name": "convention_profile", "ref": "artifact://context/convention_profile"},
        {"name": "health_assessment", "ref": "artifact://context/health_assessment_bundle"},
        {"name": "analyst_report", "ref": "artifact://analyst/report_v2"},
    ]

    payload = {
        "artifact_type": "delivery_constitution_v1",
        "artifact_version": "1.0",
        "created_at": _utc_now(),
        "project_identity": {
            "run_id": _clean(run_id),
            "workspace": _clean(workspace),
            "project": _clean(project),
            "project_state": project_state,
            "use_case": _clean(use_case),
            "repo_ref": repo_ref,
            "domain_pack_id": domain_pack_id,
        },
        "modernization_objective": _clean(objectives),
        "non_negotiables": non_negotiables[:14],
        "orchestration_hints": orchestration_hints,
        "checklists": checklists,
        "known_failure_modes": _known_failure_modes(skill_hint),
        "linked_artifacts": linked_artifacts,
        "agent_scope": {
            "stage_agent_ids": {str(k): _clean(v) for k, v in stage_map.items() if _clean(v)},
        },
        "knowledge_snapshot": {
            "snapshot_id": _clean(knowledge_context.get("snapshot_id")),
            "source_version_ids": source_versions,
            "source_count": int(source_integrity.get("source_count", 0) or 0),
            "set_count": int(source_integrity.get("set_count", 0) or 0),
        },
    }
    digest = hashlib.sha256(
        json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    payload["constitution_id"] = f"const-{digest[:12]}"
    payload["integrity"] = {
        "constitution_hash": digest,
        "knowledge_snapshot_id": _clean(knowledge_context.get("snapshot_id")),
        "source_version_ids": source_versions,
    }
    return payload


def delivery_constitution_to_markdown(constitution: dict[str, Any]) -> str:
    c = _as_dict(constitution)
    ident = _as_dict(c.get("project_identity"))
    checklists = _as_dict(c.get("checklists"))
    lines: list[str] = []
    lines.append("# Delivery Constitution")
    lines.append("")
    lines.append(f"- Constitution ID: {_clean(c.get('constitution_id')) or 'n/a'}")
    lines.append(f"- Run ID: {_clean(ident.get('run_id')) or 'n/a'}")
    lines.append(f"- Workspace/Project: {_clean(ident.get('workspace')) or 'n/a'} / {_clean(ident.get('project')) or 'n/a'}")
    lines.append(f"- Use Case: {_clean(ident.get('use_case')) or 'n/a'}")
    lines.append(f"- Snapshot ID: {_clean(_as_dict(c.get('knowledge_snapshot')).get('snapshot_id')) or 'n/a'}")
    lines.append("")
    lines.append("## Objective")
    lines.append("")
    lines.append(_clean(c.get("modernization_objective")) or "n/a")
    lines.append("")
    lines.append("## Non-Negotiables")
    lines.append("")
    non_negotiables = _as_list(c.get("non_negotiables"))
    if non_negotiables:
        lines.extend([f"- {_clean(item)}" for item in non_negotiables if _clean(item)])
    else:
        lines.append("- n/a")
    lines.append("")
    lines.append("## Orchestration Hints")
    lines.append("")
    hints = _as_list(c.get("orchestration_hints"))
    if hints:
        for row in hints:
            r = _as_dict(row)
            lines.append(
                f"- When: {_clean(r.get('when'))}; Route: {_clean(r.get('route_to'))}; Reason: {_clean(r.get('reason'))}"
            )
    else:
        lines.append("- n/a")
    lines.append("")
    lines.append("## Pre-Change Checklist")
    lines.append("")
    pre = _as_list(checklists.get("pre_change"))
    lines.extend([f"- {_clean(item)}" for item in pre if _clean(item)] or ["- n/a"])
    lines.append("")
    lines.append("## Post-Change Checklist")
    lines.append("")
    post = _as_list(checklists.get("post_change"))
    lines.extend([f"- {_clean(item)}" for item in post if _clean(item)] or ["- n/a"])
    lines.append("")
    lines.append("## Known Failure Modes")
    lines.append("")
    failures = _as_list(c.get("known_failure_modes"))
    if failures:
        for row in failures:
            r = _as_dict(row)
            lines.append(
                f"- [{_clean(r.get('id'))}] {_clean(r.get('label'))}: {_clean(r.get('pattern'))}. Mitigation: {_clean(r.get('mitigation'))}"
            )
    else:
        lines.append("- n/a")
    lines.append("")
    return "\n".join(lines).strip() + "\n"
