from __future__ import annotations

from typing import Any

from .catalog import LEGACY_SKILLS, LegacySkillSpec


def list_legacy_skills() -> list[dict[str, Any]]:
    return [
        {
            "id": item.id,
            "name": item.name,
            "description": item.description,
            "extensions": list(item.extensions),
            "analysis_focus": list(item.analysis_focus),
            "maturity": item.maturity,
        }
        for item in LEGACY_SKILLS
    ]


def _find_skill(skill_id: str) -> LegacySkillSpec | None:
    sid = str(skill_id or "").strip().lower()
    for item in LEGACY_SKILLS:
        if item.id == sid:
            return item
    return None


def infer_legacy_skill(
    *,
    file_paths: list[str] | None = None,
    file_contents: dict[str, str] | None = None,
    explicit_skill_id: str = "",
) -> dict[str, Any]:
    requested = _find_skill(explicit_skill_id)
    if requested and requested.id != "generic_legacy":
        return {
            "selected_skill_id": requested.id,
            "selected_skill_name": requested.name,
            "confidence": 1.0,
            "reasons": [f"Explicitly selected by user: {requested.id}"],
            "available_skills": list_legacy_skills(),
        }

    paths = [str(x).strip() for x in (file_paths or []) if str(x).strip()]
    contents = file_contents or {}
    combined_text = " ".join([str(v or "")[:8000] for v in contents.values()]).lower()

    scored: list[tuple[float, LegacySkillSpec, list[str]]] = []
    for spec in LEGACY_SKILLS:
        if spec.id == "generic_legacy":
            continue
        score = 0.0
        reasons: list[str] = []
        if spec.extensions:
            ext_hits = 0
            for path in paths:
                lower = path.lower()
                if any(lower.endswith(ext) for ext in spec.extensions):
                    ext_hits += 1
            if ext_hits:
                bonus = min(0.75, ext_hits * 0.08)
                score += bonus
                reasons.append(f"extension hits={ext_hits}")
        if spec.content_tokens and combined_text:
            token_hits = 0
            for token in spec.content_tokens:
                if token.lower() in combined_text:
                    token_hits += 1
            if token_hits:
                bonus = min(0.75, token_hits * 0.11)
                score += bonus
                reasons.append(f"content-token hits={token_hits}")
        if score > 0:
            scored.append((score, spec, reasons))

    if not scored:
        fallback = _find_skill("generic_legacy")
        assert fallback is not None
        return {
            "selected_skill_id": fallback.id,
            "selected_skill_name": fallback.name,
            "confidence": 0.35,
            "reasons": ["No dominant language-specific legacy signature detected."],
            "available_skills": list_legacy_skills(),
        }

    scored.sort(key=lambda row: row[0], reverse=True)
    top_score, top_spec, top_reasons = scored[0]
    confidence = max(0.4, min(0.99, top_score))
    return {
        "selected_skill_id": top_spec.id,
        "selected_skill_name": top_spec.name,
        "confidence": round(confidence, 3),
        "reasons": top_reasons,
        "available_skills": list_legacy_skills(),
    }
