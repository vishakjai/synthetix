from __future__ import annotations

from typing import Any

from .php_framework_detection import detect_php_framework_profile


def build_php_landscape_artifacts(
    *,
    repo: str,
    branch: str,
    commit_sha: str,
    entries: list[dict[str, Any]],
    file_contents: dict[str, str] | None = None,
) -> dict[str, Any]:
    profile = detect_php_framework_profile(entries=entries, file_contents=file_contents or {})
    framework = str(profile.get("framework") or "custom_php")
    route_hints = {
        "artifact_type": "php_route_hints_v1",
        "artifact_version": "1.0",
        "repo": repo,
        "branch": branch,
        "commit_sha": commit_sha,
        "framework": framework,
        "estimated_route_files": int(profile.get("route_file_count", 0) or 0),
        "estimated_controllers": int(profile.get("controller_count", 0) or 0),
        "estimated_templates": int(profile.get("template_count", 0) or 0),
        "session_state_complexity": (
            "high" if profile.get("uses_session_state") and int(profile.get("auth_touchpoint_estimate", 0) or 0) > 10
            else "medium" if profile.get("uses_session_state")
            else "low"
        ),
        "sql_coupling": (
            "high" if int(profile.get("sql_touchpoint_estimate", 0) or 0) > 20
            else "medium" if int(profile.get("sql_touchpoint_estimate", 0) or 0) > 0
            else "low"
        ),
        "notes": [
            "PHP route hints are deterministic estimates derived from file layout and sampled source content.",
        ],
    }
    framework_profile = {
        "artifact_type": "php_framework_profile_v1",
        "artifact_version": "1.0",
        "repo": repo,
        "branch": branch,
        "commit_sha": commit_sha,
        **profile,
    }
    return {
        "php_framework_profile_v1": framework_profile,
        "php_route_hints_v1": route_hints,
    }
