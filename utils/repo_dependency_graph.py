from __future__ import annotations

from pathlib import Path
from typing import Any

from utils.legacy_skills import extract_vb6_signals


def build_global_dependency_graph_v1(
    *,
    snapshot_id: str,
    file_contents: dict[str, str],
    selected_entries: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    contents = {str(k): str(v) for k, v in (file_contents or {}).items() if str(k).strip()}
    selected = selected_entries if isinstance(selected_entries, list) else []
    selected_paths = {str(row.get("path", "")).replace("\\", "/").strip() for row in selected if isinstance(row, dict)}
    basename_map: dict[str, list[str]] = {}
    for path in sorted(selected_paths):
        basename_map.setdefault(Path(path).name.lower(), []).append(path)

    nodes: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    node_keys: set[tuple[str, str]] = set()

    def add_node(node_type: str, key: str, **props: Any) -> str:
        clean_key = str(key or "").strip()
        if not clean_key:
            return ""
        token = (node_type, clean_key.lower())
        node_id = f"{node_type}:{clean_key}"
        if token not in node_keys:
            node_keys.add(token)
            row = {"id": node_id, "node_type": node_type, "key": clean_key}
            row.update(
                {
                    str(k): v
                    for k, v in props.items()
                    if v is not None and v != "" and v != [] and v != {}
                }
            )
            nodes.append(row)
        return node_id

    def add_edge(edge_type: str, source: str, target: str, **props: Any) -> None:
        if not source or not target:
            return
        row = {"edge_type": edge_type, "source": source, "target": target}
        row.update(
            {
                str(k): v
                for k, v in props.items()
                if v is not None and v != "" and v != [] and v != {}
            }
        )
        edges.append(row)

    for path in sorted(contents.keys()):
        text = contents[path]
        file_id = add_node("file", path, kind=_kind_from_path(path), ext=Path(path).suffix.lower())
        sig = extract_vb6_signals(path, text)
        if not isinstance(sig, dict):
            continue

        project_def = sig.get("project_definition", {}) if isinstance(sig.get("project_definition", {}), dict) else {}
        project_name = str(project_def.get("project_name", "")).strip()
        if project_name:
            project_id = add_node(
                "project",
                project_name,
                source_file=path,
                project_file=path,
                project_type=str(project_def.get("project_type", "")).strip(),
                startup_object=str(project_def.get("startup_object", "")).strip(),
            )
            add_edge("DECLARES_PROJECT", file_id, project_id)
            members = sig.get("project_members", []) if isinstance(sig.get("project_members", []), list) else []
            for member in members:
                member_text = str(member or "").strip()
                if not member_text:
                    continue
                member_name = member_text.split(":", 1)[-1].strip()
                matches = _resolve_member_paths(member_name, selected_paths, basename_map)
                for match in matches[:5]:
                    member_id = add_node("file", match, kind=_kind_from_path(match), ext=Path(match).suffix.lower())
                    add_edge("PROJECT_CONTAINS", project_id, member_id, confidence="high")

        for form_token in sig.get("forms", []) if isinstance(sig.get("forms", []), list) else []:
            token = str(form_token or "").strip()
            if ":" not in token:
                continue
            form_kind, form_name = token.split(":", 1)
            form_id = add_node(
                "form",
                form_name.strip(),
                source_file=path,
                form_kind=form_kind.strip().lower(),
            )
            add_edge("DECLARES_FORM", file_id, form_id)

        for dep in sig.get("activex_dependencies", []) if isinstance(sig.get("activex_dependencies", []), list) else []:
            dep_name = str(dep or "").strip()
            dep_id = add_node("dependency", dep_name, dependency_type="activex")
            add_edge("USES_DEPENDENCY", file_id, dep_id)

        for db_ref in sig.get("database_file_refs", []) if isinstance(sig.get("database_file_refs", []), list) else []:
            db_name = str(db_ref or "").replace("\\", "/").strip()
            db_id = add_node("data_store", db_name, store_type="database_file")
            add_edge("REFERENCES_DATA_STORE", file_id, db_id)

        for proc in sig.get("procedures", []) if isinstance(sig.get("procedures", []), list) else []:
            proc_name = str(proc or "").strip()
            if not proc_name:
                continue
            proc_id = add_node("procedure", f"{path}::{proc_name}", source_file=path, name=proc_name)
            add_edge("DECLARES_PROCEDURE", file_id, proc_id)

    return {
        "artifact_type": "global_dependency_graph_v1",
        "snapshot_id": snapshot_id,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes[:12000],
        "edges": edges[:24000],
    }


def _resolve_member_paths(member_name: str, selected_paths: set[str], basename_map: dict[str, list[str]]) -> list[str]:
    normalized = str(member_name or "").replace("\\", "/").strip()
    if not normalized:
        return []
    if normalized in selected_paths:
        return [normalized]
    lowered = normalized.lower()
    exact = [path for path in selected_paths if path.lower() == lowered]
    if exact:
        return exact
    basename = Path(normalized).name.lower()
    return basename_map.get(basename, [])


def _kind_from_path(path: str) -> str:
    ext = Path(str(path or "")).suffix.lower()
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
