"""
Static analysis adapters for SIL discovery.

This module provides lightweight, deterministic static analysis that can run
without external language servers:
- Python AST adapter (imports + route decorators)
- JS/TS import and route extraction
- Go import and route extraction
- Config parsing (JSON/YAML/TOML/INI/ENV/Terraform)
"""

from __future__ import annotations

import ast
import configparser
import json
import re
import tomllib
from pathlib import Path
from typing import Any

import yaml


HTTP_METHODS = {"get", "post", "put", "delete", "patch", "options", "head"}


def _line_no(text: str, offset: int) -> int:
    return text.count("\n", 0, max(0, offset)) + 1


def _safe_node_id(prefix: str, raw: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9_]+", "_", str(raw or "").strip().lower()).strip("_")
    if not key:
        key = "unknown"
    return f"{prefix}:{key[:120]}"


def _skip_path(path: Path) -> bool:
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
    return any(part in skip_dirs for part in path.parts)


def _safe_read(path: Path, max_bytes: int = 800_000) -> str:
    try:
        if path.stat().st_size > max_bytes:
            return ""
        return path.read_text(errors="ignore")
    except Exception:
        return ""


def _py_const_str(node: Any) -> str:
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Str):
        return str(node.s)
    return ""


def _parse_python_file(rel: str, text: str) -> dict[str, Any]:
    imports: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    parse_error = ""
    try:
        tree = ast.parse(text)
    except Exception as exc:
        return {"imports": [], "routes": [], "parse_error": str(exc)}

    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                name = str(alias.name or "").strip()
                if name:
                    imports.append(
                        {
                            "target": name,
                            "line": int(getattr(node, "lineno", 1) or 1),
                            "evidence": {"file": rel, "line": int(getattr(node, "lineno", 1) or 1)},
                            "confidence": 0.95,
                        }
                    )
        elif isinstance(node, ast.ImportFrom):
            level = int(getattr(node, "level", 0) or 0)
            mod = str(getattr(node, "module", "") or "")
            if mod or level > 0:
                target = f"{'.' * level}{mod}"
                imports.append(
                    {
                        "target": target,
                        "line": int(getattr(node, "lineno", 1) or 1),
                        "evidence": {"file": rel, "line": int(getattr(node, "lineno", 1) or 1)},
                        "confidence": 0.95,
                    }
                )
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            for dec in node.decorator_list:
                if not isinstance(dec, ast.Call) or not isinstance(dec.func, ast.Attribute):
                    continue
                method = str(dec.func.attr or "").lower()
                if method not in HTTP_METHODS and method != "route":
                    continue
                path_val = ""
                if dec.args:
                    path_val = _py_const_str(dec.args[0])
                if not path_val:
                    for kw in dec.keywords:
                        if str(kw.arg or "") in {"path", "rule"}:
                            path_val = _py_const_str(kw.value)
                            if path_val:
                                break
                methods: list[str] = []
                if method == "route":
                    for kw in dec.keywords:
                        if str(kw.arg or "") == "methods" and isinstance(kw.value, (ast.List, ast.Tuple)):
                            for x in kw.value.elts:
                                v = _py_const_str(x).upper()
                                if v:
                                    methods.append(v)
                    if not methods:
                        methods = ["GET"]
                else:
                    methods = [method.upper()]
                framework = "python-web"
                owner = ""
                if isinstance(dec.func.value, ast.Name):
                    owner = str(dec.func.value.id or "")
                if owner in {"app", "api"}:
                    framework = "fastapi-flask"
                elif owner in {"router", "bp"}:
                    framework = "router"
                for m in methods:
                    routes.append(
                        {
                            "file": rel,
                            "framework": framework,
                            "method": m,
                            "path": path_val or "/",
                            "line": int(getattr(node, "lineno", 1) or 1),
                            "confidence": 0.92,
                        }
                    )

    return {"imports": imports, "routes": routes, "parse_error": parse_error}


def _parse_js_ts_file(rel: str, text: str) -> dict[str, Any]:
    imports: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    for m in re.finditer(r"^\s*import\s+.+?\s+from\s+['\"]([^'\"]+)['\"]", text, flags=re.MULTILINE):
        imports.append(
            {
                "target": m.group(1),
                "line": _line_no(text, m.start()),
                "evidence": {"file": rel, "line": _line_no(text, m.start())},
                "confidence": 0.78,
            }
        )
    for m in re.finditer(r"^\s*(?:const|let|var)\s+.+?=\s*require\(\s*['\"]([^'\"]+)['\"]\s*\)", text, flags=re.MULTILINE):
        imports.append(
            {
                "target": m.group(1),
                "line": _line_no(text, m.start()),
                "evidence": {"file": rel, "line": _line_no(text, m.start())},
                "confidence": 0.78,
            }
        )
    for m in re.finditer(
        r"\b(?:app|router)\.(get|post|put|delete|patch|options|head)\(\s*['\"]([^'\"]+)['\"]",
        text,
        flags=re.IGNORECASE,
    ):
        routes.append(
            {
                "file": rel,
                "framework": "express",
                "method": m.group(1).upper(),
                "path": m.group(2),
                "line": _line_no(text, m.start()),
                "confidence": 0.8,
            }
        )
    return {"imports": imports, "routes": routes, "parse_error": ""}


def _parse_go_file(rel: str, text: str) -> dict[str, Any]:
    imports: list[dict[str, Any]] = []
    routes: list[dict[str, Any]] = []
    for m in re.finditer(r'^\s*import\s+"([^"]+)"', text, flags=re.MULTILINE):
        imports.append(
            {
                "target": m.group(1),
                "line": _line_no(text, m.start()),
                "evidence": {"file": rel, "line": _line_no(text, m.start())},
                "confidence": 0.82,
            }
        )
    for block in re.finditer(r"^\s*import\s*\((.*?)\)", text, flags=re.MULTILINE | re.DOTALL):
        body = block.group(1)
        for m in re.finditer(r'"([^"]+)"', body):
            imports.append(
                {
                    "target": m.group(1),
                    "line": _line_no(text, block.start() + m.start()),
                    "evidence": {"file": rel, "line": _line_no(text, block.start() + m.start())},
                    "confidence": 0.82,
                }
            )
    for m in re.finditer(r'\b(?:router|r)\.(GET|POST|PUT|DELETE|PATCH)\(\s*"([^"]+)"', text):
        routes.append(
            {
                "file": rel,
                "framework": "gin",
                "method": m.group(1).upper(),
                "path": m.group(2),
                "line": _line_no(text, m.start()),
                "confidence": 0.82,
            }
        )
    for m in re.finditer(r'\bhttp\.HandleFunc\(\s*"([^"]+)"', text):
        routes.append(
            {
                "file": rel,
                "framework": "net/http",
                "method": "GET",
                "path": m.group(1),
                "line": _line_no(text, m.start()),
                "confidence": 0.74,
            }
        )
    return {"imports": imports, "routes": routes, "parse_error": ""}


def _top_keys(data: Any) -> list[str]:
    if isinstance(data, dict):
        return [str(k) for k in list(data.keys())[:40]]
    if isinstance(data, list):
        keys: set[str] = set()
        for item in data[:20]:
            if isinstance(item, dict):
                for k in item.keys():
                    keys.add(str(k))
        return sorted(keys)[:40]
    return []


def _parse_config_file(rel: str, text: str, ext: str) -> tuple[dict[str, Any] | None, list[dict[str, Any]], str]:
    item: dict[str, Any] | None = None
    infra: list[dict[str, Any]] = []
    parse_error = ""
    try:
        if ext == ".json":
            data = json.loads(text)
            item = {"file": rel, "format": "json", "top_level_keys": _top_keys(data), "confidence": 0.95}
        elif ext in {".yaml", ".yml"}:
            docs = [d for d in yaml.safe_load_all(text) if d is not None]
            top = _top_keys(docs[0] if docs else {})
            item = {"file": rel, "format": "yaml", "top_level_keys": top, "confidence": 0.9}
            for d in docs:
                if isinstance(d, dict) and d.get("kind") and d.get("apiVersion"):
                    kind = str(d.get("kind"))
                    name = str((d.get("metadata") or {}).get("name", "unknown"))
                    infra.append(
                        {
                            "kind": f"k8s/{kind}",
                            "name": name,
                            "file": rel,
                            "confidence": 0.9,
                        }
                    )
        elif ext == ".toml":
            data = tomllib.loads(text)
            item = {"file": rel, "format": "toml", "top_level_keys": _top_keys(data), "confidence": 0.92}
        elif ext in {".ini", ".cfg"}:
            parser = configparser.ConfigParser()
            parser.read_string(text)
            item = {"file": rel, "format": "ini", "top_level_keys": parser.sections()[:40], "confidence": 0.88}
        elif ext == ".env":
            keys = []
            for ln in text.splitlines():
                line = ln.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key = line.split("=", 1)[0].strip()
                if key:
                    keys.append(key)
            item = {"file": rel, "format": "env", "top_level_keys": keys[:40], "confidence": 0.8}
        elif ext == ".tf":
            resources = re.findall(r'^\s*resource\s+"([^"]+)"\s+"([^"]+)"', text, flags=re.MULTILINE)
            modules = re.findall(r'^\s*module\s+"([^"]+)"', text, flags=re.MULTILINE)
            keys = [f"resource:{a}.{b}" for a, b in resources[:25]] + [f"module:{m}" for m in modules[:25]]
            item = {"file": rel, "format": "terraform", "top_level_keys": keys[:40], "confidence": 0.86}
            for r_type, r_name in resources[:80]:
                infra.append(
                    {
                        "kind": f"tf/{r_type}",
                        "name": r_name,
                        "file": rel,
                        "confidence": 0.86,
                    }
                )
        else:
            return None, [], ""
    except Exception as exc:
        parse_error = str(exc)
    return item, infra, parse_error


def _import_target_to_node(target: str) -> str:
    clean = str(target or "").strip()
    if clean.startswith("."):
        return _safe_node_id("module", clean)
    # Likely external library
    if "/" not in clean and clean.count(".") <= 1:
        return _safe_node_id("pkg", clean)
    return _safe_node_id("module", clean)


def analyze_repo_static(repo_root: Path, max_files: int = 450) -> dict[str, Any]:
    modules: list[dict[str, Any]] = []
    import_edges: list[dict[str, Any]] = []
    route_surface: list[dict[str, Any]] = []
    config_artifacts: list[dict[str, Any]] = []
    infra_resources: list[dict[str, Any]] = []
    parse_errors: list[dict[str, Any]] = []

    scanned = 0
    py_files = 0
    js_ts_files = 0
    go_files = 0
    config_files = 0

    for path in sorted(repo_root.rglob("*")):
        if scanned >= max_files:
            break
        if not path.is_file() or _skip_path(path):
            continue
        rel = path.relative_to(repo_root).as_posix()
        ext = path.suffix.lower()
        text = _safe_read(path)
        if not text:
            continue
        scanned += 1

        mod_id = _safe_node_id("module", rel)
        if ext == ".py":
            py_files += 1
            parsed = _parse_python_file(rel, text)
            modules.append(
                {
                    "id": mod_id,
                    "path": rel,
                    "language": "python",
                    "adapter": "python_ast",
                    "import_count": len(parsed["imports"]),
                    "route_count": len(parsed["routes"]),
                }
            )
            if parsed["parse_error"]:
                parse_errors.append({"file": rel, "adapter": "python_ast", "error": parsed["parse_error"]})
            for imp in parsed["imports"]:
                import_edges.append(
                    {
                        "type": "IMPORTS",
                        "from": mod_id,
                        "to": _import_target_to_node(str(imp.get("target", ""))),
                        "target": str(imp.get("target", "")),
                        "source_lang": "python",
                        "confidence": float(imp.get("confidence", 0.9)),
                        "evidence": imp.get("evidence", {"file": rel, "line": 1}),
                    }
                )
            route_surface.extend(parsed["routes"])
            continue

        if ext in {".js", ".ts"}:
            js_ts_files += 1
            parsed = _parse_js_ts_file(rel, text)
            modules.append(
                {
                    "id": mod_id,
                    "path": rel,
                    "language": "typescript" if ext == ".ts" else "javascript",
                    "adapter": "js_ts_parser",
                    "import_count": len(parsed["imports"]),
                    "route_count": len(parsed["routes"]),
                }
            )
            for imp in parsed["imports"]:
                import_edges.append(
                    {
                        "type": "IMPORTS",
                        "from": mod_id,
                        "to": _import_target_to_node(str(imp.get("target", ""))),
                        "target": str(imp.get("target", "")),
                        "source_lang": "js_ts",
                        "confidence": float(imp.get("confidence", 0.78)),
                        "evidence": imp.get("evidence", {"file": rel, "line": 1}),
                    }
                )
            route_surface.extend(parsed["routes"])
            continue

        if ext == ".go":
            go_files += 1
            parsed = _parse_go_file(rel, text)
            modules.append(
                {
                    "id": mod_id,
                    "path": rel,
                    "language": "go",
                    "adapter": "go_parser",
                    "import_count": len(parsed["imports"]),
                    "route_count": len(parsed["routes"]),
                }
            )
            for imp in parsed["imports"]:
                import_edges.append(
                    {
                        "type": "IMPORTS",
                        "from": mod_id,
                        "to": _import_target_to_node(str(imp.get("target", ""))),
                        "target": str(imp.get("target", "")),
                        "source_lang": "go",
                        "confidence": float(imp.get("confidence", 0.82)),
                        "evidence": imp.get("evidence", {"file": rel, "line": 1}),
                    }
                )
            route_surface.extend(parsed["routes"])
            continue

        if ext in {".json", ".yaml", ".yml", ".toml", ".ini", ".cfg", ".env", ".tf"}:
            config_files += 1
            item, infra, err = _parse_config_file(rel, text, ext)
            if item:
                config_artifacts.append(item)
            if infra:
                infra_resources.extend(infra)
            if err:
                parse_errors.append({"file": rel, "adapter": "config_parser", "error": err})

    # Deduplicate import edges by key.
    dedup_edges: list[dict[str, Any]] = []
    seen_edges: set[str] = set()
    for e in import_edges:
        key = f"{e.get('from')}|{e.get('to')}|{e.get('type')}|{e.get('target')}"
        if key in seen_edges:
            continue
        seen_edges.add(key)
        dedup_edges.append(e)

    # Deduplicate routes by method/path/file.
    dedup_routes: list[dict[str, Any]] = []
    seen_routes: set[str] = set()
    for r in route_surface:
        key = f"{r.get('file')}|{r.get('method')}|{r.get('path')}"
        if key in seen_routes:
            continue
        seen_routes.add(key)
        dedup_routes.append(r)

    return {
        "version": "sa-v1",
        "adapters": ["python_ast", "js_ts_parser", "go_parser", "config_parser"],
        "stats": {
            "files_scanned": scanned,
            "python_ast_files": py_files,
            "js_ts_files": js_ts_files,
            "go_files": go_files,
            "config_files": config_files,
            "modules_analyzed": len(modules),
            "import_edges": len(dedup_edges),
            "routes": len(dedup_routes),
            "infra_resources": len(infra_resources),
            "parse_errors": len(parse_errors),
        },
        "modules": modules[:220],
        "import_graph": {"edges": dedup_edges[:1200]},
        "route_surface": dedup_routes[:240],
        "config_artifacts": config_artifacts[:240],
        "infra_resources": infra_resources[:240],
        "parse_errors": parse_errors[:80],
    }

