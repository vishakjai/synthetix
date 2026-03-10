from __future__ import annotations

from typing import Any


def _skill_to_source_metadata(skill_id: str) -> dict[str, str]:
    sid = str(skill_id or "generic_legacy").strip().lower()
    mapping: dict[str, dict[str, str]] = {
        "vb6_legacy": {
            "language": "VB6",
            "ecosystem": "VB6 + COM/ActiveX desktop",
            "runtime": "Windows VB6 runtime (32-bit compatibility)",
            "source_codebase_type": "legacy_desktop",
        },
        "asp_classic_legacy": {
            "language": "Classic ASP/VBScript",
            "ecosystem": "IIS server pages + COM integrations",
            "runtime": "IIS on Windows",
            "source_codebase_type": "legacy_web",
        },
        "php_legacy": {
            "language": "PHP",
            "ecosystem": "Legacy PHP web application",
            "runtime": "PHP web runtime",
            "source_codebase_type": "legacy_web",
        },
        "dotnet_webforms_legacy": {
            "language": ".NET WebForms",
            "ecosystem": "ASP.NET WebForms",
            "runtime": "IIS + .NET Framework",
            "source_codebase_type": "legacy_web",
        },
        "cobol_legacy": {
            "language": "COBOL",
            "ecosystem": "Batch/mainframe style processing",
            "runtime": "Mainframe or enterprise batch runtime",
            "source_codebase_type": "legacy_batch",
        },
        "powerbuilder_legacy": {
            "language": "PowerBuilder",
            "ecosystem": "PowerBuilder + DataWindow",
            "runtime": "Windows desktop/runtime",
            "source_codebase_type": "legacy_desktop",
        },
        "generic_legacy": {
            "language": "Mixed/Unknown",
            "ecosystem": "Legacy mixed stack",
            "runtime": "Unknown",
            "source_codebase_type": "legacy_mixed",
        },
    }
    return dict(mapping.get(sid, mapping["generic_legacy"]))


def _pick_target_repo_url(state: dict[str, Any]) -> str:
    integration_ctx = state.get("integration_context", {}) if isinstance(state.get("integration_context", {}), dict) else {}
    exports = integration_ctx.get("exports", {}) if isinstance(integration_ctx.get("exports", {}), dict) else {}
    github = exports.get("github", {}) if isinstance(exports.get("github", {}), dict) else {}
    candidates = [
        github.get("repo_url"),
        integration_ctx.get("export_repo_url"),
        integration_ctx.get("target_repo_url"),
        state.get("export_repo_url"),
        state.get("target_repo_url"),
    ]
    for value in candidates:
        text = str(value or "").strip()
        if text:
            return text
    return ""


def _risk_texts_for_skill(skill_id: str) -> list[str]:
    sid = str(skill_id or "").strip().lower()
    if sid == "vb6_legacy":
        return [
            "COM/ActiveX dependencies can block direct portability and require replacement or wrappers.",
            "Event-driven form logic needs parity tests to preserve workflow behavior.",
            "Win32 API declares and late binding increase migration risk and testing scope.",
        ]
    if sid == "php_legacy":
        return [
            "Request/session-coupled workflows can hide business rules across controllers and templates.",
            "Inline SQL and dynamic includes increase migration and security risk.",
            "Authentication, uploads, and export behavior should be validated before target generation.",
        ]
    if sid == "asp_classic_legacy":
        return [
            "Implicit Request/Response patterns can hide input/output contracts.",
            "COM object usage may require compatibility wrappers during migration.",
        ]
    return [
        "Legacy integration assumptions may not be explicit in code and require staged validation.",
    ]


def build_source_target_modernization_profile(
    *,
    legacy_skill_profile: dict[str, Any] | None,
    legacy_inventory: dict[str, Any] | None,
    state: dict[str, Any] | None,
) -> dict[str, Any]:
    skill = legacy_skill_profile if isinstance(legacy_skill_profile, dict) else {}
    inventory = legacy_inventory if isinstance(legacy_inventory, dict) else {}
    run_state = state if isinstance(state, dict) else {}

    source_skill_id = str(skill.get("selected_skill_id", "generic_legacy")).strip() or "generic_legacy"
    source_meta = _skill_to_source_metadata(source_skill_id)

    vb6_projects = inventory.get("vb6_projects", []) if isinstance(inventory.get("vb6_projects", []), list) else []
    readiness = inventory.get("modernization_readiness", {}) if isinstance(inventory.get("modernization_readiness", {}), dict) else {}
    readiness_strategy = readiness.get("recommended_strategy", {}) if isinstance(readiness.get("recommended_strategy", {}), dict) else {}

    target_language = str(run_state.get("modernization_language", "")).strip()
    database_target = str(run_state.get("database_target", "")).strip()
    deployment_target = str(run_state.get("deployment_target", "")).strip() or "local"
    target_platform = "docker-local" if deployment_target == "local" else "cloud-managed"
    if str(run_state.get("target_platform", "")).strip():
        target_platform = str(run_state.get("target_platform", "")).strip()

    if not target_language:
        if database_target:
            target_language = f"DB migration ({database_target})"
        else:
            target_language = "Not specified"

    source_repo = ""
    integration_ctx = run_state.get("integration_context", {}) if isinstance(run_state.get("integration_context", {}), dict) else {}
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    source_repo = str(brownfield.get("repo_url", "")).strip() or str(integration_ctx.get("repo_url", "")).strip()

    target_repo = _pick_target_repo_url(run_state)

    source_project_count = len(vb6_projects)
    source_forms_total = sum(
        int(project.get("forms_count", 0) or 0) if isinstance(project, dict) else 0
        for project in vb6_projects
    )

    risk_texts = _risk_texts_for_skill(source_skill_id)
    if isinstance(readiness.get("required_actions", []), list):
        for action in readiness.get("required_actions", [])[:4]:
            item = str(action or "").strip()
            if item:
                risk_texts.append(f"Required action: {item}.")

    strategy_name = str(readiness_strategy.get("name", "")).strip() or "Strategy pending"
    strategy_id = str(readiness_strategy.get("id", "")).strip() or "pending"

    brief_summary = (
        f"Source {source_meta.get('language', 'legacy stack')} -> target {target_language}; "
        f"recommended path: {strategy_name}."
    )
    technical_summary = (
        f"Source ecosystem={source_meta.get('ecosystem', 'n/a')}, runtime={source_meta.get('runtime', 'n/a')}, "
        f"projects={source_project_count}, forms={source_forms_total}, readiness={readiness.get('score', 'n/a')}/100, "
        f"strategy={strategy_id}, target_platform={target_platform}."
    )

    return {
        "source": {
            "skill_id": source_skill_id,
            "skill_name": str(skill.get("selected_skill_name", "Generic Legacy Skill")),
            "language": source_meta.get("language", "Mixed/Unknown"),
            "ecosystem": source_meta.get("ecosystem", "Legacy mixed stack"),
            "runtime": source_meta.get("runtime", "Unknown"),
            "source_codebase_type": source_meta.get("source_codebase_type", "legacy_mixed"),
            "repo": source_repo,
            "project_count": source_project_count,
            "forms_count": source_forms_total,
        },
        "target": {
            "language": target_language,
            "platform": target_platform,
            "deployment_target": deployment_target,
            "repo": target_repo,
        },
        "summary_variants": {
            "brief": brief_summary,
            "technical": technical_summary,
            "risk": " ".join(risk_texts[:6]),
        },
        "modernization_risks": risk_texts[:12],
        "recommended_focus": [
            "Preserve business-critical behavior through flow-level regression tests.",
            "Convert integration boundaries (COM/API/DB) before UI refinements.",
            "Track source-to-target parity using explicit acceptance criteria and evidence.",
        ],
    }


def _project_risk_tier(project: dict[str, Any], global_readiness: dict[str, Any]) -> tuple[str, int]:
    forms = len(project.get("forms", []) if isinstance(project.get("forms", []), list) else [])
    controls = len(project.get("controls", []) if isinstance(project.get("controls", []), list) else [])
    activex = len(project.get("activex_dependencies", []) if isinstance(project.get("activex_dependencies", []), list) else [])
    events = len(project.get("event_handlers", []) if isinstance(project.get("event_handlers", []), list) else [])
    raw_score = (forms * 2) + controls + (activex * 6) + (events // 2)

    readiness_tier = str(global_readiness.get("risk_tier", "medium")).strip().lower()
    if readiness_tier == "high":
        raw_score += 15
    elif readiness_tier == "low":
        raw_score = max(0, raw_score - 10)

    if raw_score >= 95:
        return "high", raw_score
    if raw_score >= 55:
        return "medium", raw_score
    return "low", raw_score


def build_project_business_summaries(
    *,
    vb6_projects: list[dict[str, Any]] | None,
    source_target_profile: dict[str, Any] | None,
    global_readiness: dict[str, Any] | None = None,
) -> list[dict[str, Any]]:
    projects = vb6_projects if isinstance(vb6_projects, list) else []
    profile = source_target_profile if isinstance(source_target_profile, dict) else {}
    readiness = global_readiness if isinstance(global_readiness, dict) else {}

    target_language = str(profile.get("target", {}).get("language", "Not specified")).strip() if isinstance(profile.get("target", {}), dict) else "Not specified"
    source_language = str(profile.get("source", {}).get("language", "Legacy stack")).strip() if isinstance(profile.get("source", {}), dict) else "Legacy stack"

    summaries: list[dict[str, Any]] = []
    for idx, row in enumerate(projects):
        if not isinstance(row, dict):
            continue
        project_name = str(row.get("project_name", f"Project-{idx + 1}")).strip() or f"Project-{idx + 1}"
        objective = str(row.get("business_objective_hypothesis", "")).strip() or "Business objective requires confirmation with stakeholders."
        capabilities = [str(x).strip() for x in row.get("key_business_capabilities", []) if str(x).strip()] if isinstance(row.get("key_business_capabilities", []), list) else []
        workflows = [str(x).strip() for x in row.get("primary_workflows", []) if str(x).strip()] if isinstance(row.get("primary_workflows", []), list) else []

        forms = row.get("forms", []) if isinstance(row.get("forms", []), list) else []
        controls = row.get("controls", []) if isinstance(row.get("controls", []), list) else []
        deps = row.get("activex_dependencies", []) if isinstance(row.get("activex_dependencies", []), list) else []
        integrations = row.get("technical_components", {}).get("integration_hints", []) if isinstance(row.get("technical_components", {}), dict) and isinstance(row.get("technical_components", {}).get("integration_hints", []), list) else []

        risk_tier, risk_score = _project_risk_tier(row, readiness)
        quick_summary = (
            f"{project_name}: {objective} "
            f"(forms={len(forms)}, controls={len(controls)}, dependencies={len(deps)}, risk={risk_tier})."
        )
        technical_summary = (
            f"Source={source_language}, target={target_language}, capabilities={', '.join(capabilities[:6]) or 'n/a'}, "
            f"integrations={', '.join(integrations[:6]) or 'none detected'}."
        )
        risk_summary = (
            "High migration pressure from VB6 runtime coupling and dependency footprint."
            if risk_tier == "high"
            else ("Moderate migration complexity; phased modernization recommended." if risk_tier == "medium" else "Lower migration complexity; direct translation/remediation may be feasible.")
        )

        summaries.append(
            {
                "project_name": project_name,
                "project_file": str(row.get("project_file", "")).strip(),
                "source_language": source_language,
                "target_language": target_language,
                "business_objective": objective,
                "business_capabilities": capabilities[:12],
                "primary_workflows": workflows[:12],
                "technical_components": {
                    "forms": forms[:40],
                    "controls": controls[:80],
                    "dependencies": deps[:40],
                    "integration_hints": integrations[:12],
                },
                "risk": {
                    "tier": risk_tier,
                    "score": risk_score,
                    "summary": risk_summary,
                },
                "summary_variants": {
                    "quick": quick_summary,
                    "technical": technical_summary,
                    "risk": risk_summary,
                },
            }
        )

    summaries.sort(key=lambda row: str(row.get("project_name", "")).lower())
    return summaries[:32]
