from __future__ import annotations

from typing import Any

from .php_auth import extract_php_auth_inventory
from .php_controllers import extract_php_controller_inventory
from .php_framework_detection import detect_php_framework_profile
from .php_file_io import extract_php_file_io_inventory
from .php_includes import extract_php_include_graph
from .php_jobs import extract_php_background_job_inventory
from .php_routes import extract_php_route_inventory
from .php_sessions import extract_php_session_state_inventory
from .php_sql import extract_php_sql_catalog
from .php_templates import extract_php_template_inventory
from .php_validation import extract_php_validation_rules


def build_php_landscape_artifacts(
    *,
    repo: str,
    branch: str,
    commit_sha: str,
    entries: list[dict[str, Any]],
    file_contents: dict[str, str] | None = None,
) -> dict[str, Any]:
    file_contents = file_contents or {}
    profile = detect_php_framework_profile(entries=entries, file_contents=file_contents)
    framework = str(profile.get("framework") or "custom_php")
    route_inventory = extract_php_route_inventory(file_contents)
    controller_inventory = extract_php_controller_inventory(file_contents)
    template_inventory = extract_php_template_inventory(file_contents)
    sql_catalog = extract_php_sql_catalog(file_contents)
    session_inventory = extract_php_session_state_inventory(file_contents)
    auth_inventory = extract_php_auth_inventory(file_contents)
    include_graph = extract_php_include_graph(file_contents)
    job_inventory = extract_php_background_job_inventory(file_contents)
    file_io_inventory = extract_php_file_io_inventory(file_contents)
    validation_rules = extract_php_validation_rules(file_contents)
    route_hints = {
        "artifact_type": "php_route_hints_v1",
        "artifact_version": "1.0",
        "repo": repo,
        "branch": branch,
        "commit_sha": commit_sha,
        "framework": framework,
        "estimated_route_files": int(route_inventory.get("route_count", 0) or profile.get("route_file_count", 0) or 0),
        "estimated_controllers": int(controller_inventory.get("controller_count", 0) or profile.get("controller_count", 0) or 0),
        "estimated_templates": int(template_inventory.get("template_count", 0) or profile.get("template_count", 0) or 0),
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
        "php_route_inventory_v1": route_inventory,
        "php_controller_inventory_v1": controller_inventory,
        "php_template_inventory_v1": template_inventory,
        "php_sql_catalog_v1": sql_catalog,
        "php_session_state_inventory_v1": session_inventory,
        "php_authz_authn_inventory_v1": auth_inventory,
        "php_include_graph_v1": include_graph,
        "php_background_job_inventory_v1": job_inventory,
        "php_file_io_inventory_v1": file_io_inventory,
        "php_validation_rules_v1": validation_rules,
    }
