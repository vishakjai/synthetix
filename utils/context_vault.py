"""
System Intelligence Layer (SIL) Context Vault utilities.

Stores and validates canonical context artifacts that downstream agents
must depend on:
  1. System Context Model (SCM)
  2. Convention Profile (CP)
  3. Health Assessment + Remediation Backlog (HA/RB)
"""

from __future__ import annotations

import hashlib
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from .static_analysis import analyze_repo_static


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_slug(value: str) -> str:
    raw = "".join(ch if ch.isalnum() or ch in {"-", "_", "."} else "-" for ch in str(value or ""))
    cleaned = "-".join(part for part in raw.split("-") if part)
    return cleaned[:120] or "unknown"


def _run_git(repo_root: Path, args: list[str]) -> str:
    try:
        proc = subprocess.run(
            ["git", *args],
            cwd=str(repo_root),
            capture_output=True,
            text=True,
            check=False,
            timeout=6,
        )
        if proc.returncode == 0:
            return proc.stdout.strip()
    except Exception:
        return ""
    return ""


def git_fingerprint(repo_root: Path) -> dict[str, str]:
    repo_name = _safe_slug(repo_root.name)
    branch = _run_git(repo_root, ["rev-parse", "--abbrev-ref", "HEAD"]) or "detached"
    commit = _run_git(repo_root, ["rev-parse", "HEAD"]) or "unknown"
    return {
        "repo": repo_name,
        "branch": _safe_slug(branch),
        "commit_sha": commit[:12],
    }


def discover_repo_snapshot(repo_root: Path, max_files: int = 450) -> dict[str, Any]:
    skip_dirs = {
        ".git",
        ".venv",
        "__pycache__",
        "pipeline_runs",
        "run_artifacts",
        "deploy_output",
        "context_vault",
        "node_modules",
        ".mypy_cache",
        ".pytest_cache",
    }
    language_by_ext = {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".go": "go",
        ".java": "java",
        ".cs": "csharp",
        ".rs": "rust",
        ".sql": "sql",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".tf": "terraform",
        ".md": "markdown",
    }
    language_counts: dict[str, int] = {}
    file_samples: list[str] = []
    endpoint_hints: list[str] = []
    static_analysis: dict[str, Any] = {}

    scanned = 0
    for path in repo_root.rglob("*"):
        if scanned >= max_files:
            break
        if not path.is_file():
            continue
        if any(part in skip_dirs for part in path.parts):
            continue
        rel = path.relative_to(repo_root).as_posix()
        file_samples.append(rel)
        ext = path.suffix.lower()
        lang = language_by_ext.get(ext, "other")
        language_counts[lang] = language_counts.get(lang, 0) + 1
        scanned += 1

        if ext in {".py", ".js", ".ts", ".go"} and len(endpoint_hints) < 40:
            text = path.read_text(errors="ignore")
            for token in ("@app.get(", "@app.post(", "router.", "Route(", "FastAPI(", "express(", "HandleFunc("):
                if token in text:
                    endpoint_hints.append(f"{rel}::{token}")
    try:
        static_analysis = analyze_repo_static(repo_root, max_files=max_files)
    except Exception as exc:
        static_analysis = {"version": "sa-v1", "error": str(exc), "stats": {"files_scanned": scanned}}

    if isinstance(static_analysis.get("route_surface", []), list):
        for r in static_analysis.get("route_surface", [])[:40]:
            if not isinstance(r, dict):
                continue
            file_ref = str(r.get("file", "")).strip()
            method = str(r.get("method", "")).strip().upper()
            path = str(r.get("path", "")).strip()
            if file_ref and method and path:
                endpoint_hints.append(f"{file_ref}::{method} {path}")

    digest = hashlib.sha1("\n".join(sorted(file_samples)).encode("utf-8")).hexdigest()[:16]
    return {
        "repo_root": str(repo_root),
        "scanned_files": scanned,
        "content_fingerprint": digest,
        "language_counts": language_counts,
        "file_samples": file_samples[:220],
        "endpoint_hints": endpoint_hints[:40],
        "static_analysis": static_analysis,
    }


def _ensure_float(value: Any, default: float = 0.55) -> float:
    try:
        num = float(value)
    except Exception:
        return default
    return max(0.0, min(1.0, num))


def _ensure_sil_shape(payload: dict[str, Any], discovery: dict[str, Any]) -> dict[str, Any]:
    out = payload if isinstance(payload, dict) else {}

    scm = out.get("system_context_model", {})
    if not isinstance(scm, dict):
        scm = {}
    nodes = scm.get("nodes", [])
    edges = scm.get("edges", [])
    if not isinstance(nodes, list):
        nodes = []
    if not isinstance(edges, list):
        edges = []
    static = discovery.get("static_analysis", {}) if isinstance(discovery.get("static_analysis", {}), dict) else {}

    path_node_index: dict[str, str] = {}
    node_ids: set[str] = set()
    node_name_index: dict[str, str] = {}

    def _index_path(path: str, node_id: str) -> None:
        raw = str(path or "").strip()
        if not raw:
            return
        p = raw.replace("\\", "/")
        path_node_index[p] = node_id
        base = p.rsplit("/", 1)[-1]
        if base:
            path_node_index.setdefault(base, node_id)

    def _ensure_node(node_id: str, node_type: str, name: str, confidence: float, provenance: list[dict[str, Any]], metadata: dict[str, Any] | None = None) -> None:
        if node_id in node_ids:
            return
        nodes.append(
            {
                "id": node_id,
                "type": node_type,
                "name": name,
                "metadata": metadata or {},
                "confidence": _ensure_float(confidence, confidence),
                "provenance": provenance or [{"file": "repo-scan", "line": 1, "evidence": "auto-filled provenance"}],
            }
        )
        node_ids.add(node_id)
        lowered_name = str(name or "").strip().lower()
        if lowered_name:
            node_name_index.setdefault(lowered_name, node_id)

    if not nodes:
        modules = static.get("modules", []) if isinstance(static.get("modules", []), list) else []
        for m in modules[:160]:
            if not isinstance(m, dict):
                continue
            node_id = str(m.get("id", "")).strip()
            path = str(m.get("path", "")).strip()
            if not node_id or not path:
                continue
            _ensure_node(
                node_id=node_id,
                node_type="Module",
                name=path,
                confidence=0.74,
                provenance=[{"file": path, "line": 1, "evidence": "static analysis module"}],
                metadata={
                    "language": str(m.get("language", "")),
                    "adapter": str(m.get("adapter", "")),
                    "import_count": int(m.get("import_count", 0) or 0),
                    "route_count": int(m.get("route_count", 0) or 0),
                },
            )
            _index_path(path, node_id)

        routes = static.get("route_surface", []) if isinstance(static.get("route_surface", []), list) else []
        for idx, r in enumerate(routes[:120], start=1):
            if not isinstance(r, dict):
                continue
            method = str(r.get("method", "GET")).upper()
            path = str(r.get("path", "/")).strip() or "/"
            file_ref = str(r.get("file", "")).strip()
            route_id = f"route-{idx}"
            _ensure_node(
                node_id=route_id,
                node_type="Endpoint",
                name=f"{method} {path}",
                confidence=_ensure_float(r.get("confidence", 0.82), 0.82),
                provenance=[{"file": file_ref or "repo-scan", "line": int(r.get("line", 1) or 1), "evidence": "route extraction"}],
                metadata={"path": path, "method": method, "framework": str(r.get("framework", "unknown"))},
            )
            owner_id = path_node_index.get(file_ref)
            if owner_id:
                edges.append(
                    {
                        "type": "OWNS_RESOURCE",
                        "from": owner_id,
                        "to": route_id,
                        "directionality": "directed",
                        "confidence": 0.84,
                        "protocol_metadata": {"kind": "endpoint"},
                        "evidence": [{"file": file_ref, "line": int(r.get("line", 1) or 1), "evidence": "route owner mapping"}],
                    }
                )

        infra = static.get("infra_resources", []) if isinstance(static.get("infra_resources", []), list) else []
        for idx, item in enumerate(infra[:60], start=1):
            if not isinstance(item, dict):
                continue
            kind = str(item.get("kind", "infra/unknown")).strip()
            name = str(item.get("name", f"resource-{idx}")).strip()
            file_ref = str(item.get("file", "")).strip()
            node_id = f"infra-{idx}"
            _ensure_node(
                node_id=node_id,
                node_type="InfraResource",
                name=f"{kind}:{name}",
                confidence=_ensure_float(item.get("confidence", 0.82), 0.82),
                provenance=[{"file": file_ref or "repo-scan", "line": 1, "evidence": "infra extraction"}],
                metadata={"kind": kind, "resource_name": name},
            )

    if not edges:
        import_graph = static.get("import_graph", {}) if isinstance(static.get("import_graph", {}), dict) else {}
        import_edges = import_graph.get("edges", []) if isinstance(import_graph.get("edges", []), list) else []
        for e in import_edges[:1500]:
            if not isinstance(e, dict):
                continue
            src = str(e.get("from", "")).strip()
            dst = str(e.get("to", "")).strip()
            if not src or not dst:
                continue
            if not any(isinstance(n, dict) and str(n.get("id", "")) == dst for n in nodes):
                target_name = str(e.get("target", dst)).strip() or dst
                target_type = "ExternalDependency" if dst.startswith("pkg:") else "Module"
                _ensure_node(
                    node_id=dst,
                    node_type=target_type,
                    name=target_name,
                    confidence=0.7 if target_type == "ExternalDependency" else 0.72,
                    provenance=[{"file": str(e.get("evidence", {}).get("file", "repo-scan")), "line": int(e.get("evidence", {}).get("line", 1) or 1), "evidence": "import target"}],
                    metadata={"source_lang": str(e.get("source_lang", ""))},
                )
            edges.append(
                {
                    "type": "IMPORTS",
                    "from": src,
                    "to": dst,
                    "directionality": "directed",
                    "confidence": _ensure_float(e.get("confidence", 0.78), 0.78),
                    "protocol_metadata": {},
                    "evidence": [e.get("evidence", {"file": "repo-scan", "line": 1, "evidence": "import graph"})],
                }
            )

    if not nodes:
        for idx, (lang, count) in enumerate((discovery.get("language_counts") or {}).items(), start=1):
            nodes.append(
                {
                    "id": f"lang-{idx}",
                    "type": "Module",
                    "name": f"{lang}-codebase",
                    "confidence": 0.7,
                    "provenance": [{"file": "repo-scan", "line": 1, "evidence": "language_counts"}],
                    "metadata": {"file_count": count},
                }
            )
    if not edges and len(nodes) > 1:
        for idx in range(1, len(nodes)):
            edges.append(
                {
                    "type": "DEPENDS_ON",
                    "from": nodes[idx - 1].get("id", f"n{idx}"),
                    "to": nodes[idx].get("id", f"n{idx+1}"),
                    "directionality": "directed",
                    "confidence": 0.55,
                    "protocol_metadata": {},
                    "evidence": [{"file": "repo-scan", "line": 1, "evidence": "inferred adjacency"}],
                }
            )

    for node in nodes:
        if not isinstance(node, dict):
            continue
        nid = str(node.get("id", "")).strip()
        if nid:
            node_ids.add(nid)
        nname = str(node.get("name", "")).strip().lower()
        if nid and nname:
            node_name_index.setdefault(nname, nid)
        metadata = node.get("metadata", {}) if isinstance(node.get("metadata", {}), dict) else {}
        path_hint = str(metadata.get("path", "")).strip()
        if nid and path_hint:
            _index_path(path_hint, nid)
        node["confidence"] = _ensure_float(node.get("confidence", 0.65), 0.65)
        prov = node.get("provenance", [])
        if not isinstance(prov, list) or not prov:
            node["provenance"] = [{"file": "repo-scan", "line": 1, "evidence": "auto-filled provenance"}]

    def _first_evidence(edge_payload: dict[str, Any]) -> dict[str, Any]:
        evidence = edge_payload.get("evidence", [])
        if isinstance(evidence, list):
            for ev in evidence:
                if isinstance(ev, dict):
                    return ev
        return {"file": "repo-scan", "line": 1, "evidence": "inferred from edge endpoint"}

    def _endpoint_candidates(raw_value: Any) -> list[str]:
        token = str(raw_value or "").strip()
        if not token:
            return []
        compact = token.replace("\\", "/")
        stem = compact.rsplit("/", 1)[-1]
        tail = compact.split(":", 1)[-1] if ":" in compact else compact
        candidates = [compact, stem, tail]
        prefixed = [f"node:{stem}", f"node:{tail}"] if stem or tail else []
        out: list[str] = []
        for item in [*candidates, *prefixed]:
            clean = str(item).strip()
            if clean and clean not in out:
                out.append(clean)
        return out

    def _resolve_or_create_endpoint(raw_value: Any, edge_payload: dict[str, Any]) -> str:
        for candidate in _endpoint_candidates(raw_value):
            if candidate in node_ids:
                return candidate
            lowered = candidate.lower()
            if lowered in node_name_index:
                return node_name_index[lowered]
            if candidate in path_node_index:
                return path_node_index[candidate]

        token = str(raw_value or "").strip()
        if not token:
            return ""
        inferred_id = token if ":" in token else f"node:{token}"
        suffix = 2
        while inferred_id in node_ids:
            inferred_id = f"{inferred_id}-{suffix}"
            suffix += 1
        ev = _first_evidence(edge_payload)
        inferred_name = token.split(":", 1)[-1] if ":" in token else token
        _ensure_node(
            node_id=inferred_id,
            node_type="Component",
            name=inferred_name or inferred_id,
            confidence=0.45,
            provenance=[
                {
                    "file": str(ev.get("file", "repo-scan")),
                    "line": int(ev.get("line", 1) or 1),
                    "evidence": f"inferred node from edge endpoint `{token}`",
                }
            ],
            metadata={"inferred": True, "inferred_from_edge": True},
        )
        return inferred_id

    sanitized_edges: list[dict[str, Any]] = []
    for edge in edges:
        if not isinstance(edge, dict):
            continue
        src = edge.get("from")
        if not src:
            src = edge.get("source")
        dst = edge.get("to")
        if not dst:
            dst = edge.get("target")
        resolved_src = _resolve_or_create_endpoint(src, edge)
        resolved_dst = _resolve_or_create_endpoint(dst, edge)
        if not resolved_src or not resolved_dst:
            continue

        edge["confidence"] = _ensure_float(edge.get("confidence", 0.55), 0.55)
        evidence = edge.get("evidence", [])
        if not isinstance(evidence, list) or not evidence:
            edge["evidence"] = [{"file": "repo-scan", "line": 1, "evidence": "auto-filled evidence"}]
        edge.setdefault("directionality", "directed")
        edge.setdefault("protocol_metadata", {})
        edge["from"] = resolved_src
        edge["to"] = resolved_dst
        if "source" in edge:
            edge.pop("source", None)
        if "target" in edge:
            edge.pop("target", None)
        sanitized_edges.append(edge)
    edges = sanitized_edges

    scm_out = {
        "version": str(scm.get("version", "scm-v1")),
        "summary": str(scm.get("summary", "")).strip() or "System context inferred from repository structure.",
        "nodes": nodes,
        "edges": edges,
        "unknowns": scm.get("unknowns", []) if isinstance(scm.get("unknowns", []), list) else [],
    }

    cp = out.get("convention_profile", {})
    if not isinstance(cp, dict):
        cp = {}
    rules = cp.get("rules", [])
    if not isinstance(rules, list):
        rules = []
    cp_out = {
        "version": str(cp.get("version", "cp-v1")),
        "summary": str(cp.get("summary", "")).strip() or "Convention profile extracted from repository signals.",
        "rules": rules,
        "lint_recommendations": cp.get("lint_recommendations", []) if isinstance(cp.get("lint_recommendations", []), list) else [],
        "scaffold_templates": cp.get("scaffold_templates", []) if isinstance(cp.get("scaffold_templates", []), list) else [],
    }
    for idx, rule in enumerate(cp_out["rules"], start=1):
        if not isinstance(rule, dict):
            continue
        rule.setdefault("id", f"CP-RULE-{idx:03d}")
        rule["confidence"] = _ensure_float(rule.get("confidence", 0.6), 0.6)
        if not isinstance(rule.get("provenance", []), list) or not rule.get("provenance"):
            rule["provenance"] = [{"file": "repo-scan", "line": 1, "evidence": "auto-filled provenance"}]

    ha = out.get("health_assessment", {})
    if not isinstance(ha, dict):
        ha = {}
    scores = ha.get("scores", {})
    if not isinstance(scores, dict):
        scores = {}
    ha_out = {
        "version": str(ha.get("version", "ha-v1")),
        "summary": str(ha.get("summary", "")).strip() or "Health assessment generated from static repository analysis.",
        "scores": {
            "maintainability": int(scores.get("maintainability", 65)),
            "security": int(scores.get("security", 60)),
            "reliability": int(scores.get("reliability", 62)),
            "testability": int(scores.get("testability", 58)),
        },
        "hotspots": ha.get("hotspots", []) if isinstance(ha.get("hotspots", []), list) else [],
        "risks": ha.get("risks", []) if isinstance(ha.get("risks", []), list) else [],
    }

    backlog = out.get("remediation_backlog", [])
    if not isinstance(backlog, list):
        backlog = []
    for idx, item in enumerate(backlog, start=1):
        if not isinstance(item, dict):
            continue
        item.setdefault("id", f"RB-{idx:03d}")
        item["confidence"] = _ensure_float(item.get("confidence", 0.6), 0.6)
        if not isinstance(item.get("provenance", []), list) or not item.get("provenance"):
            item["provenance"] = [{"file": "repo-scan", "line": 1, "evidence": "auto-filled provenance"}]

    return {
        "system_context_model": scm_out,
        "convention_profile": cp_out,
        "health_assessment": ha_out,
        "remediation_backlog": backlog,
    }


def _read_json(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text())
    except Exception:
        return None
    return data if isinstance(data, dict) else None


def _previous_manifest(vault_root: Path, repo: str, branch: str, commit_sha: str, current_dir_name: str) -> dict[str, Any] | None:
    """
    Return the latest prior manifest for this repo/branch, including prior commits.

    This enables SCM/CP/HA-RB delta tracking across commit boundaries rather than
    only within the same commit SHA.
    """
    branch_root = vault_root / repo / branch
    if not branch_root.exists():
        return None

    commit_dirs = [p for p in branch_root.iterdir() if p.is_dir()]
    commit_dirs.sort(key=lambda p: p.stat().st_mtime, reverse=True)

    for commit_dir in commit_dirs:
        version_dirs = [p for p in commit_dir.iterdir() if p.is_dir()]
        version_dirs.sort(key=lambda p: p.name, reverse=True)
        for version_dir in version_dirs:
            if commit_dir.name == commit_sha and version_dir.name == current_dir_name:
                continue
            manifest = _read_json(version_dir / "manifest.json")
            if manifest:
                return manifest
    return None


def context_diff(previous: dict[str, Any] | None, current: dict[str, Any]) -> dict[str, Any]:
    if not previous:
        return {"status": "new_baseline", "node_delta": 0, "edge_delta": 0, "rule_delta": 0, "backlog_delta": 0}
    prev_counts = previous.get("counts", {})
    cur_counts = current.get("counts", {})
    return {
        "status": "updated",
        "node_delta": int(cur_counts.get("scm_nodes", 0)) - int(prev_counts.get("scm_nodes", 0)),
        "edge_delta": int(cur_counts.get("scm_edges", 0)) - int(prev_counts.get("scm_edges", 0)),
        "rule_delta": int(cur_counts.get("cp_rules", 0)) - int(prev_counts.get("cp_rules", 0)),
        "backlog_delta": int(cur_counts.get("rb_items", 0)) - int(prev_counts.get("rb_items", 0)),
    }


def store_context_vault(
    run_id: str,
    repo_root: Path,
    vault_root: Path,
    sil_output: dict[str, Any],
    discovery: dict[str, Any] | None = None,
) -> dict[str, Any]:
    vault_root.mkdir(parents=True, exist_ok=True)
    fp = git_fingerprint(repo_root)
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    version_dir = f"{timestamp}_{_safe_slug(run_id)}"
    target = vault_root / fp["repo"] / fp["branch"] / fp["commit_sha"] / version_dir
    target.mkdir(parents=True, exist_ok=True)

    scm = sil_output.get("system_context_model", {})
    cp = sil_output.get("convention_profile", {})
    ha = sil_output.get("health_assessment", {})
    rb = sil_output.get("remediation_backlog", [])
    discovery_payload = discovery if isinstance(discovery, dict) else {}
    static_analysis = (
        discovery_payload.get("static_analysis", {})
        if isinstance(discovery_payload.get("static_analysis", {}), dict)
        else {}
    )

    def _collect_confidence_values() -> list[float]:
        values: list[float] = []
        for node in scm.get("nodes", []) if isinstance(scm.get("nodes", []), list) else []:
            if isinstance(node, dict):
                values.append(_ensure_float(node.get("confidence", 0.0), 0.0))
        for edge in scm.get("edges", []) if isinstance(scm.get("edges", []), list) else []:
            if isinstance(edge, dict):
                values.append(_ensure_float(edge.get("confidence", 0.0), 0.0))
        for rule in cp.get("rules", []) if isinstance(cp.get("rules", []), list) else []:
            if isinstance(rule, dict):
                values.append(_ensure_float(rule.get("confidence", 0.0), 0.0))
        for item in rb if isinstance(rb, list) else []:
            if isinstance(item, dict):
                values.append(_ensure_float(item.get("confidence", 0.0), 0.0))
        return values

    def _provenance_coverage() -> dict[str, int]:
        def _count(items: list[Any], key: str) -> tuple[int, int]:
            total = 0
            with_prov = 0
            for i in items:
                if not isinstance(i, dict):
                    continue
                total += 1
                p = i.get(key, [])
                if isinstance(p, list) and p:
                    with_prov += 1
            return total, with_prov

        n_total, n_prov = _count(scm.get("nodes", []) if isinstance(scm.get("nodes", []), list) else [], "provenance")
        e_total, e_prov = _count(scm.get("edges", []) if isinstance(scm.get("edges", []), list) else [], "evidence")
        r_total, r_prov = _count(cp.get("rules", []) if isinstance(cp.get("rules", []), list) else [], "provenance")
        b_total, b_prov = _count(rb if isinstance(rb, list) else [], "provenance")
        return {
            "nodes_total": n_total,
            "nodes_with_provenance": n_prov,
            "edges_total": e_total,
            "edges_with_evidence": e_prov,
            "rules_total": r_total,
            "rules_with_provenance": r_prov,
            "backlog_total": b_total,
            "backlog_with_provenance": b_prov,
        }

    (target / "scm.json").write_text(json.dumps(scm, indent=2, ensure_ascii=True, default=str))
    (target / "convention_profile.json").write_text(json.dumps(cp, indent=2, ensure_ascii=True, default=str))
    (target / "health_assessment.json").write_text(json.dumps(ha, indent=2, ensure_ascii=True, default=str))
    (target / "remediation_backlog.json").write_text(json.dumps(rb, indent=2, ensure_ascii=True, default=str))
    if static_analysis:
        (target / "static_analysis.json").write_text(json.dumps(static_analysis, indent=2, ensure_ascii=True, default=str))

    conf = _collect_confidence_values()
    confidence_summary = {
        "claim_count": len(conf),
        "average_confidence": round(sum(conf) / len(conf), 4) if conf else 0.0,
        "min_confidence": round(min(conf), 4) if conf else 0.0,
        "max_confidence": round(max(conf), 4) if conf else 0.0,
        "below_0_6_count": sum(1 for x in conf if x < 0.6),
        "provenance_coverage": _provenance_coverage(),
        "generated_at": _utc_now(),
    }
    (target / "confidence_summary.json").write_text(
        json.dumps(confidence_summary, indent=2, ensure_ascii=True, default=str)
    )

    counts = {
        "scm_nodes": len(scm.get("nodes", [])) if isinstance(scm.get("nodes", []), list) else 0,
        "scm_edges": len(scm.get("edges", [])) if isinstance(scm.get("edges", []), list) else 0,
        "cp_rules": len(cp.get("rules", [])) if isinstance(cp.get("rules", []), list) else 0,
        "rb_items": len(rb) if isinstance(rb, list) else 0,
    }
    manifest = {
        "run_id": run_id,
        "created_at": _utc_now(),
        "repo": fp["repo"],
        "branch": fp["branch"],
        "commit_sha": fp["commit_sha"],
        "version_id": version_dir,
        "counts": counts,
        "artifacts": {
            "scm": "scm.json",
            "convention_profile": "convention_profile.json",
            "health_assessment": "health_assessment.json",
            "remediation_backlog": "remediation_backlog.json",
            "confidence_summary": "confidence_summary.json",
            **({"static_analysis": "static_analysis.json"} if static_analysis else {}),
        },
    }
    previous = _previous_manifest(vault_root, fp["repo"], fp["branch"], fp["commit_sha"], version_dir)
    manifest["delta"] = context_diff(previous, manifest)
    (target / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=True, default=str))

    md = [
        "# System Intelligence Layer (SIL)",
        "",
        f"- Run ID: `{run_id}`",
        f"- Repo: `{fp['repo']}`",
        f"- Branch: `{fp['branch']}`",
        f"- Commit: `{fp['commit_sha']}`",
        f"- Version: `{version_dir}`",
        "",
        "## Artifact Counts",
        f"- SCM nodes: {counts['scm_nodes']}",
        f"- SCM edges: {counts['scm_edges']}",
        f"- CP rules: {counts['cp_rules']}",
        f"- Remediation items: {counts['rb_items']}",
        "",
        "## Delta",
        f"- Node delta: {manifest['delta']['node_delta']}",
        f"- Edge delta: {manifest['delta']['edge_delta']}",
        f"- Rule delta: {manifest['delta']['rule_delta']}",
        f"- Backlog delta: {manifest['delta']['backlog_delta']}",
    ]
    (target / "SIL.md").write_text("\n".join(md))

    return {
        "vault_path": str(target),
        "repo": fp["repo"],
        "branch": fp["branch"],
        "commit_sha": fp["commit_sha"],
        "version_id": version_dir,
        "manifest_path": str(target / "manifest.json"),
        "delta": manifest["delta"],
    }


def context_gate_issues(state: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    if not isinstance(state, dict):
        return ["pipeline state missing"]
    if not state.get("sil_ready"):
        issues.append("SIL not marked ready")
    scm = state.get("system_context_model")
    cp = state.get("convention_profile")
    ha = state.get("health_assessment")
    rb = state.get("remediation_backlog")
    ref = state.get("context_vault_ref")
    bundle = state.get("context_bundle")
    contracts = state.get("context_contracts")
    contract_validation = state.get("context_contract_validation")
    if not isinstance(scm, dict) or not scm:
        issues.append("system_context_model missing")
    if not isinstance(cp, dict) or not cp:
        issues.append("convention_profile missing")
    if not isinstance(ha, dict) or not ha:
        issues.append("health_assessment missing")
    if not isinstance(rb, list):
        issues.append("remediation_backlog missing")
    if not isinstance(ref, dict) or not ref.get("version_id"):
        issues.append("context_vault_ref/version missing")
    if not isinstance(bundle, dict) or not bundle.get("bundle_id"):
        issues.append("context_bundle missing")
    if not isinstance(contracts, dict) or not contracts:
        issues.append("context_contracts missing")
    if not isinstance(contract_validation, dict):
        issues.append("context_contract_validation missing")
    elif contract_validation.get("is_valid") is False:
        issues.append("context_contract_validation failed")
    return issues


def normalize_sil_output(raw_payload: dict[str, Any], discovery: dict[str, Any]) -> dict[str, Any]:
    return _ensure_sil_shape(raw_payload, discovery)
