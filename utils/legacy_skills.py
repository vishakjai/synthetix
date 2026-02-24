from __future__ import annotations

from .legacy_skills_core import (
    LegacySkillSpec,
    build_project_business_summaries,
    build_source_target_modernization_profile,
    build_vb6_readiness_assessment,
    extract_vb6_signals,
    infer_legacy_skill,
    list_legacy_skills,
    vb6_skill_pack_manifest,
)

__all__ = [
    "LegacySkillSpec",
    "list_legacy_skills",
    "infer_legacy_skill",
    "extract_vb6_signals",
    "build_vb6_readiness_assessment",
    "vb6_skill_pack_manifest",
    "build_source_target_modernization_profile",
    "build_project_business_summaries",
]
