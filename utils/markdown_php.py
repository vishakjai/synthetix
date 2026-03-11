from __future__ import annotations

from typing import Any

from utils.markdown_common import as_dict, as_int, as_list, clean


def php_artifact_bundle(raw_map: dict[str, Any]) -> dict[str, Any]:
    raw_map = as_dict(raw_map)
    return {
        "route_inventory": as_dict(raw_map.get("php_route_inventory") or raw_map.get("php_route_inventory_v1")),
        "controller_inventory": as_dict(raw_map.get("php_controller_inventory") or raw_map.get("php_controller_inventory_v1")),
        "template_inventory": as_dict(raw_map.get("php_template_inventory") or raw_map.get("php_template_inventory_v1")),
        "sql_catalog": as_dict(raw_map.get("php_sql_catalog") or raw_map.get("php_sql_catalog_v1")),
        "session_state_inventory": as_dict(raw_map.get("php_session_state_inventory") or raw_map.get("php_session_state_inventory_v1")),
        "authz_authn_inventory": as_dict(raw_map.get("php_authz_authn_inventory") or raw_map.get("php_authz_authn_inventory_v1")),
        "include_graph": as_dict(raw_map.get("php_include_graph") or raw_map.get("php_include_graph_v1")),
        "background_job_inventory": as_dict(raw_map.get("php_background_job_inventory") or raw_map.get("php_background_job_inventory_v1")),
        "file_io_inventory": as_dict(raw_map.get("php_file_io_inventory") or raw_map.get("php_file_io_inventory_v1")),
        "validation_rules": as_dict(raw_map.get("php_validation_rules") or raw_map.get("php_validation_rules_v1")),
    }


def has_php_summary_data(summary: dict[str, Any]) -> bool:
    summary = as_dict(summary)
    if not summary:
        return False
    routes = as_dict(summary.get("route_inventory"))
    controllers = as_dict(summary.get("controller_inventory"))
    templates = as_dict(summary.get("template_inventory"))
    sql_catalog = as_dict(summary.get("sql_catalog"))
    sessions = as_dict(summary.get("session_state_inventory"))
    authz = as_dict(summary.get("authz_authn_inventory"))
    jobs = as_dict(summary.get("background_job_inventory"))
    file_io = as_dict(summary.get("file_io_inventory"))
    validation = as_dict(summary.get("validation_rules"))
    include_graph = as_dict(summary.get("include_graph"))
    return any(
        [
            as_int(routes.get("route_count"), 0) > 0,
            as_int(routes.get("entrypoint_count"), 0) > 0,
            bool(as_list(routes.get("routes"))),
            as_int(controllers.get("controller_count"), 0) > 0,
            bool(as_list(controllers.get("controllers"))),
            as_int(templates.get("template_count"), 0) > 0,
            bool(as_list(templates.get("templates"))),
            as_int(sql_catalog.get("statement_count"), 0) > 0,
            bool(as_list(sql_catalog.get("statements"))),
            as_int(sessions.get("session_key_count"), 0) > 0,
            bool(sessions.get("uses_session_state")),
            as_int(authz.get("auth_touchpoint_count"), 0) > 0,
            as_int(authz.get("auth_file_count"), 0) > 0,
            bool(as_list(authz.get("evidence"))),
            as_int(jobs.get("job_count"), 0) > 0,
            bool(as_list(jobs.get("jobs"))),
            as_int(file_io.get("upload_file_count"), 0) > 0,
            as_int(file_io.get("export_file_count"), 0) > 0,
            as_int(validation.get("file_count"), 0) > 0,
            bool(as_list(validation.get("entries"))),
            bool(as_list(include_graph.get("edges"))),
        ]
    )


def build_php_markdown_context(
    *,
    report_source_language: Any,
    output_source_language: Any,
    inventory: dict[str, Any],
    raw_artifacts: dict[str, Any],
    source_loc_total: int,
    source_files_scanned: int,
) -> dict[str, Any]:
    inventory = as_dict(inventory)
    raw_artifacts = as_dict(raw_artifacts)
    php_summary = as_dict(as_dict(raw_artifacts.get("legacy_inventory")).get("php_analysis"))
    if not has_php_summary_data(php_summary):
        php_summary = php_artifact_bundle(raw_artifacts)

    is_php = clean(report_source_language or output_source_language).lower() == "php" or has_php_summary_data(php_summary) or any(
        inventory.get(key) not in (None, "", 0)
        for key in ("controllers", "routes", "templates", "session_keys", "auth_touchpoints", "background_jobs", "file_io_flows")
    )
    if not is_php:
        return {"is_php": False, "php_summary": {}}

    dependency_count = as_int(as_dict(raw_artifacts.get("legacy_inventory")).get("php_dependency_count"), 0)
    if dependency_count <= 0:
        dependency_count = as_int(inventory.get("dependencies"), 0)
    if dependency_count <= 0:
        dependency_count = as_int(as_dict(as_dict(raw_artifacts.get("repo_landscape_v1") or raw_artifacts.get("repo_landscape")).get("dependency_footprint")).get("composer_package_count"), 0)

    return {
        "is_php": True,
        "php_summary": php_summary,
        "php_dependency_count": dependency_count,
        "inventory_summary_text": (
            f"{as_int(inventory.get('applications'), 0)} application(s), "
            f"{as_int(inventory.get('controllers'), 0)} controllers, "
            f"{as_int(inventory.get('routes'), 0)} routes, "
            f"{as_int(inventory.get('templates'), 0)} templates, "
            f"{dependency_count} dependencies"
        ),
        "loc_summary_text": f"{source_loc_total} total LOC across {source_files_scanned} files",
    }


def build_php_appendix_metrics(
    *,
    php_summary: dict[str, Any],
    dependency_count: int,
    source_loc_total: int,
    source_files_scanned: int,
) -> dict[str, int]:
    php_summary = as_dict(php_summary)
    route_rows = as_list(as_dict(php_summary.get("route_inventory")).get("routes"))
    sql_rows = as_list(as_dict(php_summary.get("sql_catalog")).get("statements"))
    validation_rows = as_list(as_dict(php_summary.get("validation_rules")).get("entries"))
    auth_rows = as_list(as_dict(php_summary.get("authz_authn_inventory")).get("evidence"))
    risk_descriptions: list[str] = []
    for row in sql_rows[:800]:
        for risk in as_list(as_dict(row).get("risk_flags")):
            text = clean(risk)
            if text and text not in risk_descriptions:
                risk_descriptions.append(text)
    if bool(as_dict(php_summary.get("session_state_inventory")).get("uses_session_state")) and "session_state_mutation" not in risk_descriptions:
        risk_descriptions.append("session_state_mutation")
    if as_int(as_dict(php_summary.get("file_io_inventory")).get("upload_file_count"), 0) > 0 and "file_upload_handling" not in risk_descriptions:
        risk_descriptions.append("file_upload_handling")
    return {
        "event_map_rows": len(route_rows) or as_int(as_dict(php_summary.get("route_inventory")).get("route_count"), 0) or as_int(as_dict(php_summary.get("route_inventory")).get("entrypoint_count"), 0),
        "sql_catalog_rows": len(sql_rows) or as_int(as_dict(php_summary.get("sql_catalog")).get("statement_count"), 0),
        "dependency_rows": dependency_count,
        "business_rule_rows": len(validation_rows) + len(auth_rows),
        "risk_register_rows": len(risk_descriptions),
        "static_risk_rows": len(risk_descriptions),
        "source_loc_total": source_loc_total,
        "source_files_scanned": source_files_scanned,
    }
