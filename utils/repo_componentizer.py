from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any

from utils.legacy_skills import extract_vb6_signals


PHP_TEMPLATE_HINTS = (
    "/view",
    "/views",
    "/template",
    "/templates",
    "/resources/views",
    "/dashboard/elements/",
    "/dashboard/",
    "/email_templates/",
    "/mail_templates/",
)


def build_component_inventory_v1(
    *,
    snapshot_id: str,
    selected_entries: list[dict[str, Any]],
    file_contents: dict[str, str],
) -> dict[str, Any]:
    entry_map = {
        str(row.get("path", "")).replace("\\", "/").strip(): row
        for row in selected_entries
        if isinstance(row, dict) and str(row.get("path", "")).strip()
    }
    contents = {str(k).replace("\\", "/").strip(): str(v) for k, v in (file_contents or {}).items() if str(k).strip()}
    assigned: set[str] = set()
    components: list[dict[str, Any]] = []

    php_candidates = [path for path in entry_map.keys() if _kind_from_path(path).startswith("php_")]
    vb6_candidates = [path for path in entry_map.keys() if _kind_from_path(path) in {"project", "project_group", "form", "usercontrol", "class", "module"}]
    prefer_php = bool(php_candidates) and (
        len(php_candidates) >= max(12, len(vb6_candidates))
        or (len(vb6_candidates) == 0 and len(php_candidates) >= 3)
    )

    if prefer_php:
        php_routes = sorted([path for path in entry_map.keys() if _kind_from_path(path) == "php_route"])
        php_controllers = sorted([path for path in entry_map.keys() if _kind_from_path(path) == "php_controller"])
        php_templates = sorted([path for path in entry_map.keys() if _kind_from_path(path) == "php_template"])
        php_jobs = sorted([path for path in entry_map.keys() if _is_php_job(path)])
        php_support = sorted([
            path for path in entry_map.keys()
            if path not in set(php_routes + php_controllers + php_templates + php_jobs)
            and (_kind_from_path(path) == "php_source" or str(path).lower().endswith((".sql", ".json", ".yaml", ".yml", ".md")))
        ])

        if php_routes:
            assigned.update(php_routes)
            components.append(_component_record(snapshot_id=snapshot_id, component_id='php::routes', name='Route Surface', component_type='php_routes', paths=php_routes, entry_map=entry_map, file_contents=contents, risk_flags=[]))
        if php_controllers:
            assigned.update(php_controllers)
            components.append(_component_record(snapshot_id=snapshot_id, component_id='php::controllers', name='Controller Layer', component_type='php_controllers', paths=php_controllers, entry_map=entry_map, file_contents=contents, risk_flags=['controller_logic_review'] if len(php_controllers) > 80 else []))
        if php_templates:
            assigned.update(php_templates)
            components.append(_component_record(snapshot_id=snapshot_id, component_id='php::templates', name='Template/View Layer', component_type='php_templates', paths=php_templates, entry_map=entry_map, file_contents=contents, risk_flags=['template_auth_review'] if len(php_templates) > 0 else []))
        if php_jobs:
            assigned.update(php_jobs)
            components.append(_component_record(snapshot_id=snapshot_id, component_id='php::jobs', name='Async and Job Processing', component_type='php_jobs', paths=php_jobs, entry_map=entry_map, file_contents=contents, risk_flags=['async_processing_review']))
        if php_support:
            assigned.update(php_support)
            components.append(_component_record(snapshot_id=snapshot_id, component_id='php::support', name='Support and Domain Services', component_type='php_support', paths=php_support, entry_map=entry_map, file_contents=contents, risk_flags=['support_artifact_review']))
    else:
        for path, body in sorted(contents.items()):
            if not path.lower().endswith(".vbp"):
                continue
            sig = extract_vb6_signals(path, body)
            if not isinstance(sig, dict):
                continue
            project_def = sig.get("project_definition", {}) if isinstance(sig.get("project_definition", {}), dict) else {}
            project_name = str(project_def.get("project_name", "")).strip() or Path(path).stem
            members_raw = sig.get("project_members", []) if isinstance(sig.get("project_members", []), list) else []
            members = _resolve_project_members(path, members_raw, set(entry_map.keys()))
            component_paths = [path] + [member for member in members if member in entry_map and member != path]
            assigned.update(component_paths)
            components.append(
                _component_record(
                    snapshot_id=snapshot_id,
                    component_id=f"project::{project_name}",
                    name=project_name,
                    component_type="vb6_project",
                    paths=component_paths,
                    entry_map=entry_map,
                    file_contents=contents,
                    risk_flags=[],
                )
            )

        shared_paths = sorted([
            path for path in entry_map.keys() if path not in assigned and path.lower().endswith((".bas", ".cls"))
        ])
        if shared_paths:
            assigned.update(shared_paths)
            components.append(_component_record(snapshot_id=snapshot_id, component_id="shared::foundation", name="Shared Foundation", component_type="shared_foundation", paths=shared_paths, entry_map=entry_map, file_contents=contents, risk_flags=["shared_module_review"]))

        ui_paths = sorted([
            path for path in entry_map.keys() if path not in assigned and path.lower().endswith((".frm", ".ctl"))
        ])
        if ui_paths:
            assigned.update(ui_paths)
            components.append(_component_record(snapshot_id=snapshot_id, component_id="ui::unassigned", name="Unassigned UI", component_type="ui_unassigned", paths=ui_paths, entry_map=entry_map, file_contents=contents, risk_flags=["scope_resolution_required"]))

    other_paths = sorted([path for path in entry_map.keys() if path not in assigned])
    if other_paths:
        components.append(
            _component_record(
                snapshot_id=snapshot_id,
                component_id="other::support",
                name="Support Artifacts",
                component_type="support",
                paths=other_paths,
                entry_map=entry_map,
                file_contents=contents,
                risk_flags=["support_artifact_review"] if any(p.lower().endswith((".mdb", ".accdb", ".ocx", ".frx", ".ctx")) for p in other_paths) else [],
            )
        )

    return {
        "artifact_type": "component_inventory_v1",
        "snapshot_id": snapshot_id,
        "component_count": len(components),
        "components": components[:400],
    }


def build_chunk_manifest_v1(
    *,
    snapshot_id: str,
    component_inventory: dict[str, Any],
    file_contents: dict[str, str],
    max_chunk_files: int = 18,
    max_chunk_chars: int = 140000,
) -> dict[str, Any]:
    components = component_inventory.get("components", []) if isinstance(component_inventory.get("components", []), list) else []
    contents = {str(k).replace("\\", "/").strip(): str(v) for k, v in (file_contents or {}).items() if str(k).strip()}
    chunks: list[dict[str, Any]] = []
    for comp in components:
        if not isinstance(comp, dict):
            continue
        paths = [str(p).replace("\\", "/").strip() for p in comp.get("paths", []) if str(p).strip()]
        priority = 1 if str(comp.get("component_type", "")).strip() == "vb6_project" else 2
        depends_on = ["chunk::shared::foundation::1"] if str(comp.get("component_type", "")).strip() == "vb6_project" and any(
            str(c.get("component_id", "")).strip() == "shared::foundation" for c in components if isinstance(c, dict)
        ) else []
        current: list[str] = []
        current_chars = 0
        part = 1
        for path in sorted(paths, key=_chunk_order_key):
            text = contents.get(path, "")
            estimated = len(text) + len(path) + 24
            if current and (len(current) >= max_chunk_files or current_chars + estimated > max_chunk_chars):
                chunks.append(_chunk_row(snapshot_id, comp, current, part, priority, depends_on, contents))
                part += 1
                current = []
                current_chars = 0
            current.append(path)
            current_chars += estimated
        if current:
            chunks.append(_chunk_row(snapshot_id, comp, current, part, priority, depends_on, contents))

    return {
        "artifact_type": "chunk_manifest_v1",
        "snapshot_id": snapshot_id,
        "chunk_count": len(chunks),
        "chunks": chunks[:1000],
    }


def _chunk_row(
    snapshot_id: str,
    component: dict[str, Any],
    paths: list[str],
    part: int,
    priority: int,
    depends_on: list[str],
    file_contents: dict[str, str],
) -> dict[str, Any]:
    counts = Counter(_kind_from_path(path) for path in paths)
    estimated_chars = sum(len(str(file_contents.get(path, "") or "")) + len(path) + 24 for path in paths)
    estimated_loc = sum(len(str(file_contents.get(path, "") or "").splitlines()) for path in paths)
    component_id = str(component.get("component_id", "")).strip()
    chunk_id = f"chunk::{component_id or 'component'}::{part}"
    return {
        "chunk_id": chunk_id,
        "snapshot_id": snapshot_id,
        "component_id": component_id,
        "component_name": str(component.get("name", "")).strip(),
        "component_type": str(component.get("component_type", "")).strip(),
        "paths": paths,
        "counts_by_type": dict(sorted(counts.items())),
        "estimated_chars": estimated_chars,
        "estimated_loc": estimated_loc,
        "priority": priority,
        "contains_project_descriptor": any(path.lower().endswith((".vbp", ".vbg")) for path in paths),
        "depends_on": depends_on[:10],
        "coverage_expectations": {
            "should_extract_forms": any(path.lower().endswith((".frm", ".ctl")) for path in paths),
            "should_extract_modules": any(path.lower().endswith((".bas", ".cls")) for path in paths),
            "should_extract_project_membership": any(path.lower().endswith(".vbp") for path in paths),
            "should_extract_php_routes": any(_kind_from_path(path) == "php_route" for path in paths),
            "should_extract_php_controllers": any(_kind_from_path(path) == "php_controller" for path in paths),
            "should_extract_php_templates": any(_kind_from_path(path) == "php_template" for path in paths),
            "should_extract_php_sql": any(str(path).lower().endswith((".php", ".sql")) for path in paths),
        },
    }


def _component_record(
    *,
    snapshot_id: str,
    component_id: str,
    name: str,
    component_type: str,
    paths: list[str],
    entry_map: dict[str, dict[str, Any]],
    file_contents: dict[str, str],
    risk_flags: list[str],
) -> dict[str, Any]:
    counts = Counter(_kind_from_path(path) for path in paths)
    estimated_loc = sum(len(str(file_contents.get(path, "") or "").splitlines()) for path in paths)
    stack_profile = ["php"] if component_type.startswith("php_") else ["vb6"]
    return {
        "component_id": component_id,
        "snapshot_id": snapshot_id,
        "name": name,
        "component_type": component_type,
        "stack_profile": stack_profile,
        "paths": paths[:400],
        "root_path": _common_root(paths),
        "file_count": len(paths),
        "counts_by_type": dict(sorted(counts.items())),
        "estimated_loc": estimated_loc,
        "estimated_chars": sum(len(str(file_contents.get(path, "") or "")) for path in paths),
        "risk_flags": risk_flags[:20],
    }


def _resolve_project_members(project_path: str, members_raw: list[Any], selected_paths: set[str]) -> list[str]:
    base_dir = Path(project_path).parent
    resolved: list[str] = []
    normalized_selected = {path.lower(): path for path in selected_paths}
    basename_map: dict[str, list[str]] = {}
    for path in selected_paths:
        basename_map.setdefault(Path(path).name.lower(), []).append(path)

    for raw in members_raw:
        token = str(raw or "").strip()
        if not token:
            continue
        member_name = token.split(":", 1)[-1].strip().replace("\\", "/")
        candidate = str((base_dir / member_name).as_posix()).lstrip("./")
        match = normalized_selected.get(candidate.lower())
        if not match:
            exact = normalized_selected.get(member_name.lower())
            match = exact or next(iter(basename_map.get(Path(member_name).name.lower(), [])), "")
        if match and match not in resolved:
            resolved.append(match)
    return resolved


def _common_root(paths: list[str]) -> str:
    clean = [str(path).replace("\\", "/").strip() for path in paths if str(path).strip()]
    if not clean:
        return ""
    parts = clean[0].split("/")
    for path in clean[1:]:
        current = path.split("/")
        idx = 0
        while idx < len(parts) and idx < len(current) and parts[idx] == current[idx]:
            idx += 1
        parts = parts[:idx]
        if not parts:
            break
    return "/".join(parts)


def _kind_from_path(path: str) -> str:
    ext = Path(str(path or "")).suffix.lower()
    normalized = str(path or "").replace("\\", "/").strip().lower()
    if normalized.endswith('.php'):
        name = Path(normalized).name.lower()
        if normalized.startswith('routes/') or '/routes/' in normalized or name in {'web.php', 'api.php', 'routes.php'} or normalized.endswith('/index.php'):
            return 'php_route'
        if '/controller/' in normalized or '/controllers/' in normalized or name.endswith('controller.php'):
            return 'php_controller'
        if any(token in normalized for token in PHP_TEMPLATE_HINTS):
            return 'php_template'
        return 'php_source'
    return {
        ".vbp": "project",
        ".vbg": "project_group",
        ".frm": "form",
        ".ctl": "usercontrol",
        ".cls": "class",
        ".bas": "module",
        ".frx": "form_binary",
        ".ctx": "usercontrol_binary",
        ".dca": "connection_definition",
        ".dcx": "query_definition",
        ".mdb": "database",
        ".accdb": "database",
    }.get(ext, "other")


def _chunk_order_key(path: str) -> tuple[int, str]:
    bucket_order = {
        "project": 0,
        "form": 1,
        "usercontrol": 1,
        "php_route": 1,
        "class": 2,
        "php_controller": 2,
        "module": 3,
        "php_template": 3,
        "connection_definition": 4,
        "query_definition": 4,
        "database": 5,
        "php_source": 5,
        "form_binary": 6,
        "usercontrol_binary": 6,
        "other": 9,
    }
    kind = _kind_from_path(path)
    return (bucket_order.get(kind, 9), str(path).replace("\\", "/").lower())


def _is_php_job(path: str) -> bool:
    low = str(path or "").replace("\\", "/").lower()
    if not low.endswith('.php'):
        return False
    return '/bin/' in low or '/cron/' in low or 'queue' in low or 'listener' in low or 'worker' in low
