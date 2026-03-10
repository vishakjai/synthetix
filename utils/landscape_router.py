from __future__ import annotations

import hashlib
import os
import re
from collections import Counter, defaultdict
from pathlib import PurePosixPath
from typing import Any

from .php_landscape import build_php_landscape_artifacts


LANGUAGE_BY_EXT = {
    ".frm": "VB6",
    ".frx": "VB6",
    ".vbp": "VB6",
    ".bas": "VB6",
    ".cls": "VB6",
    ".ctl": "VB6",
    ".pag": "VB6",
    ".dsr": "VB6",
    ".vb": "VB.NET",
    ".cs": "C#",
    ".sln": "C#",
    ".csproj": "C#",
    ".fsproj": "F#",
    ".js": "JavaScript",
    ".ts": "TypeScript",
    ".tsx": "TypeScript",
    ".jsx": "JavaScript",
    ".py": "Python",
    ".sql": "SQL",
    ".ps1": "PowerShell",
    ".psm1": "PowerShell",
    ".cmd": "Batch",
    ".bat": "Batch",
    ".sh": "Shell",
    ".java": "Java",
    ".go": "Go",
    ".php": "PHP",
    ".phtml": "PHP",
    ".json": "JSON",
    ".yaml": "YAML",
    ".yml": "YAML",
    ".xml": "XML",
    ".config": "Config",
    ".ini": "Config",
    ".env": "Config",
    ".toml": "Config",
    ".tf": "Terraform",
    ".mdb": "Access",
    ".accdb": "Access",
    ".rpt": "Reporting",
}

TEXT_EXTENSIONS = {
    ".frm", ".bas", ".cls", ".ctl", ".pag", ".vbp", ".vb", ".cs", ".sln", ".csproj",
    ".js", ".ts", ".tsx", ".jsx", ".py", ".sql", ".ps1", ".psm1", ".cmd", ".bat",
    ".sh", ".java", ".go", ".php", ".phtml", ".json", ".yaml", ".yml", ".xml", ".config", ".ini", ".env",
    ".toml", ".tf", ".md", ".txt", ".dsr",
}

BUILD_KINDS = {
    ".vbp": "vb6_vbp",
    ".sln": "dotnet_sln",
    ".csproj": "dotnet_project",
    "package.json": "node_package",
    "composer.json": "php_composer",
    "pom.xml": "maven_project",
    "build.gradle": "gradle_project",
    "build.gradle.kts": "gradle_project",
    "requirements.txt": "python_requirements",
    "pyproject.toml": "python_project",
}

ARCTYPE_HINTS = {
    "desktop_forms_vb6": [".frm", ".vbp"],
    "reporting_pack": [".dsr", ".rpt"],
    "batch_jobs": [".ps1", ".bat", ".cmd", ".sh"],
}

DB_HINT_PATTERNS = [
    ("access_mdb", re.compile(r"(?:\.mdb|\.accdb)\b", re.I)),
    ("mysql", re.compile(r"\bmysqli?\b|pdo_mysql|mysql:", re.I)),
    ("sqlserver", re.compile(r"server\s*=|initial catalog\s*=|trusted_connection\s*=", re.I)),
    ("oracle", re.compile(r"oracle|tns:|data source\s*=.*oracle", re.I)),
    ("db2", re.compile(r"\bdb2\b", re.I)),
    ("postgres", re.compile(r"host\s*=.*port\s*=.*dbname\s*=|postgresql", re.I)),
    ("mq", re.compile(r"\bmq\b|queue|rabbitmq|activemq", re.I)),
]


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _slug(value: str, *, max_len: int = 64) -> str:
    token = re.sub(r"[^a-z0-9]+", "_", _clean(value).lower()).strip("_")
    return token[:max_len] or "item"


def _estimate_loc_from_size(size: int) -> int:
    if size <= 0:
        return 0
    # conservative shallow-scan approximation for text blobs
    return max(1, int(round(size / 42.0)))


def _is_text_like(path: str) -> bool:
    lower = path.lower()
    name = PurePosixPath(lower).name
    ext = PurePosixPath(lower).suffix
    return ext in TEXT_EXTENSIONS or name in BUILD_KINDS or lower.endswith(PHP_TEMPLATE_SUFFIXES)


PHP_TEMPLATE_SUFFIXES = (".blade.php", ".twig", ".tpl.php")


def _language_for_path(path: str) -> str:
    lower = path.lower()
    name = PurePosixPath(lower).name
    ext = PurePosixPath(lower).suffix
    if lower.endswith(PHP_TEMPLATE_SUFFIXES):
        return "PHP"
    if name in BUILD_KINDS:
        kind = BUILD_KINDS[name]
        if kind.startswith("node"):
            return "JavaScript"
        if kind.startswith("php"):
            return "PHP"
        if kind.startswith("maven") or kind.startswith("gradle"):
            return "Java"
        if kind.startswith("python"):
            return "Python"
    return LANGUAGE_BY_EXT.get(ext, "Other")


def _build_kind_for_path(path: str) -> str:
    lower = path.lower()
    name = PurePosixPath(lower).name
    ext = PurePosixPath(lower).suffix
    if name == "composer.json" and lower.startswith(("vendor/", "node_modules/")):
        return ""
    if name in BUILD_KINDS:
        return BUILD_KINDS[name]
    return BUILD_KINDS.get(ext, "")


def _component_kind_from_build(kind: str) -> str:
    return {
        "vb6_vbp": "vb6_project",
        "dotnet_sln": "dotnet_solution",
        "dotnet_project": "dotnet_project",
        "node_package": "node_app",
        "php_composer": "php_project",
        "maven_project": "java_project",
        "gradle_project": "java_project",
        "python_project": "python_project",
        "python_requirements": "python_project",
    }.get(kind, "unknown_component")


def _component_display_name(build_path: str) -> str:
    path = PurePosixPath(build_path)
    if path.name.lower() in {"package.json", "requirements.txt", "pyproject.toml", "pom.xml", "build.gradle", "build.gradle.kts"}:
        return path.parent.name or path.name
    return path.name


def _archetypes_for_paths(paths: list[str]) -> list[str]:
    lower_paths = [p.lower() for p in paths]
    found: list[str] = []
    if any(p.endswith(".frm") for p in lower_paths) and any(p.endswith(".vbp") for p in lower_paths):
        found.append("desktop_forms_vb6")
    if any(p.endswith((".sln", ".csproj")) for p in lower_paths):
        found.append("api_service")
    if any(p.endswith((".rpt", ".dsr")) for p in lower_paths):
        found.append("reporting_pack")
    if any(p.endswith((".sql",)) for p in lower_paths):
        found.append("database_scripts")
    if any(p.endswith((".ps1", ".bat", ".cmd", ".sh")) for p in lower_paths):
        found.append("batch_jobs")
    if any(p.endswith((".php", ".phtml")) or p.endswith(PHP_TEMPLATE_SUFFIXES) for p in lower_paths):
        if any(marker in p for p in lower_paths for marker in ("/controller/", "/controllers/", "/view/", "/views/", "/templates/", "/includes/")):
            found.append("php_web_app")
        else:
            found.append("php_application")
    return found


def default_router_ruleset(*, repo: str = "", branch: str = "main", commit_sha: str = "") -> dict[str, Any]:
    return {
        "meta": {
            "artifact_type": "router_ruleset",
            "artifact_version": "1.0",
            "artifact_id": f"router_rules_{_slug(repo or 'repo')}",
            "run_id": "discover",
            "generated_at": "",
            "producer": {
                "agent": "LandscapeRouter",
                "skill_pack": "router_core",
                "skill_version": "1.0",
                "engine_version": "0.1.0",
            },
            "context": {"repo": repo, "branch": branch, "commit_sha": commit_sha, "stage": "Discover"},
        },
        "defaults": {
            "max_chunk_tokens": 60000,
            "foundation_summary_max_tokens": 12000,
            "min_chunk_tokens": 8000,
        },
        "rules": [
            {
                "rule_id": "R_VB6_FORMS",
                "priority": 10,
                "when": {"component_kind": "vb6_project", "language_includes": ["VB6"], "archetypes": ["desktop_forms_vb6"]},
                "then": {
                    "attach_skill_packs": ["vb6_forms_pack", "vb6_data_pack", "vb6_com_pack"],
                    "attach_agents": ["VB6FormsAgent", "VB6ADOAgent", "DependencyMatrixAgent", "DeadCodeAgent"],
                    "suggest_tracks": ["ui_modernization", "data_modernization"],
                    "add_gating_questions": [
                        "Confirm canonical .vbp variant(s).",
                        "Confirm target UI strategy: Web vs WPF/WinUI.",
                        "Confirm Access DB is production or demo.",
                    ],
                    "set_chunking_profile": "vb6_large_repo",
                },
            },
            {
                "rule_id": "R_DOTNET",
                "priority": 20,
                "when": {"component_kind": "dotnet_solution", "language_includes": ["C#"]},
                "then": {
                    "attach_skill_packs": ["dotnet_pack", "api_contract_pack"],
                    "attach_agents": ["DotNetAnalyzerAgent", "APISpecAgent", "TestHarnessAgent"],
                    "suggest_tracks": ["api_modernization"],
                    "add_gating_questions": ["Confirm build/test pipeline commands.", "Is OpenAPI spec available?"],
                    "set_chunking_profile": "dotnet_standard",
                },
            },
            {
                "rule_id": "R_DOTNET_PROJECT",
                "priority": 21,
                "when": {"component_kind": "dotnet_project", "language_includes": ["C#"]},
                "then": {
                    "attach_skill_packs": ["dotnet_pack", "api_contract_pack"],
                    "attach_agents": ["DotNetAnalyzerAgent", "TestHarnessAgent"],
                    "suggest_tracks": ["api_modernization"],
                    "add_gating_questions": ["Confirm project build/test commands.", "Confirm whether this component is an API, library, or desktop app."],
                    "set_chunking_profile": "dotnet_standard",
                },
            },
            {
                "rule_id": "R_NODE",
                "priority": 25,
                "when": {"component_kind": "node_app", "language_includes": ["JavaScript"]},
                "then": {
                    "attach_skill_packs": ["node_pack", "api_contract_pack"],
                    "attach_agents": ["NodeAnalyzerAgent", "FrontendInventoryAgent"],
                    "suggest_tracks": ["web_modernization"],
                    "add_gating_questions": ["Confirm runtime and package manager.", "Confirm whether this component is customer-facing UI or internal tooling."],
                    "set_chunking_profile": "web_standard",
                },
            },
            {
                "rule_id": "R_PHP_LARAVEL",
                "priority": 24,
                "when": {"component_kind": "php_project", "language_includes": ["PHP"], "frameworks": ["laravel"]},
                "then": {
                    "attach_skill_packs": ["php_pack", "laravel_pack", "sql_pack"],
                    "attach_agents": ["PHPAnalyzerAgent", "PHPRoutesAgent", "PHPSQLAgent"],
                    "suggest_tracks": ["php_web_modernization"],
                    "add_gating_questions": [
                        "Confirm whether the target should remain server-rendered or move to React/Next.js.",
                        "Confirm authentication/session strategy for the target runtime.",
                    ],
                    "set_chunking_profile": "php_web_standard",
                },
            },
            {
                "rule_id": "R_PHP_SYMFONY",
                "priority": 24,
                "when": {"component_kind": "php_project", "language_includes": ["PHP"], "frameworks": ["symfony"]},
                "then": {
                    "attach_skill_packs": ["php_pack", "symfony_pack", "sql_pack"],
                    "attach_agents": ["PHPAnalyzerAgent", "PHPRoutesAgent", "PHPSQLAgent"],
                    "suggest_tracks": ["php_web_modernization"],
                    "add_gating_questions": [
                        "Confirm whether the target should preserve the current controller/view split.",
                        "Confirm session and middleware behavior that must remain compatible.",
                    ],
                    "set_chunking_profile": "php_web_standard",
                },
            },
            {
                "rule_id": "R_PHP_WORDPRESS",
                "priority": 24,
                "when": {"language_includes": ["PHP"], "frameworks": ["wordpress"]},
                "then": {
                    "attach_skill_packs": ["php_pack", "cms_pack"],
                    "attach_agents": ["PHPAnalyzerAgent", "TemplateInventoryAgent"],
                    "suggest_tracks": ["php_web_modernization"],
                    "add_gating_questions": [
                        "Confirm whether WordPress should be retained, upgraded, or replaced.",
                        "Identify plugin/theme customizations that are business-critical.",
                    ],
                    "set_chunking_profile": "php_web_standard",
                },
            },
            {
                "rule_id": "R_PHP_CUSTOM",
                "priority": 24,
                "when": {"language_includes": ["PHP"], "frameworks": ["custom_php"], "archetypes": ["php_web_app"]},
                "then": {
                    "attach_skill_packs": ["php_pack", "sql_pack"],
                    "attach_agents": ["PHPAnalyzerAgent", "PHPRoutesAgent", "PHPSQLAgent", "TemplateInventoryAgent"],
                    "suggest_tracks": ["php_web_modernization"],
                    "add_gating_questions": [
                        "Confirm target architecture: Node/Nest API, Next.js full-stack, or modular TypeScript monolith.",
                        "Confirm session/authentication handling requirements before migration planning.",
                    ],
                    "set_chunking_profile": "php_web_standard",
                },
            },
            {
                "rule_id": "R_TYPESCRIPT",
                "priority": 26,
                "when": {"component_kind": "node_app", "language_includes": ["TypeScript"]},
                "then": {
                    "attach_skill_packs": ["node_pack", "api_contract_pack"],
                    "attach_agents": ["NodeAnalyzerAgent", "FrontendInventoryAgent"],
                    "suggest_tracks": ["web_modernization"],
                    "add_gating_questions": ["Confirm runtime and package manager.", "Confirm whether this component is UI, API, or shared tooling."],
                    "set_chunking_profile": "web_standard",
                },
            },
            {
                "rule_id": "R_PYTHON",
                "priority": 27,
                "when": {"component_kind": "python_project", "language_includes": ["Python"]},
                "then": {
                    "attach_skill_packs": ["python_pack"],
                    "attach_agents": ["PythonAnalyzerAgent", "BatchJobAgent"],
                    "suggest_tracks": ["service_modernization"],
                    "add_gating_questions": ["Confirm runtime/environment strategy.", "Confirm whether this component is API, worker, or scripting layer."],
                    "set_chunking_profile": "python_standard",
                },
            },
            {
                "rule_id": "R_REPORTING",
                "priority": 30,
                "when": {"component_kind": "reporting_pack", "archetypes": ["reporting_pack"]},
                "then": {
                    "attach_skill_packs": ["reporting_pack"],
                    "attach_agents": ["ReportingMigrationAgent"],
                    "suggest_tracks": ["reporting_modernization"],
                    "add_gating_questions": ["Confirm reporting target: SSRS, Power BI, or keep legacy reports."],
                    "set_chunking_profile": "vb6_large_repo",
                },
            },
            {
                "rule_id": "R_BATCH",
                "priority": 40,
                "when": {"component_kind": "batch_pack", "archetypes": ["batch_jobs"]},
                "then": {
                    "attach_skill_packs": ["batch_pack"],
                    "attach_agents": ["BatchJobAgent"],
                    "suggest_tracks": ["batch_modernization"],
                    "add_gating_questions": ["Confirm scheduler/orchestration target for batch workloads."],
                    "set_chunking_profile": "dotnet_standard",
                },
            },
        ],
    }


def _router_apply(component: dict[str, Any], ruleset: dict[str, Any]) -> dict[str, Any]:
    skills: list[str] = []
    agents: list[str] = []
    questions: list[str] = []
    track_lanes: list[str] = []
    fired: list[str] = []
    chunking_profile = "default"
    confidence = 0.0
    comp_kind = _clean(component.get("component_kind"))
    comp_framework = _clean(component.get("framework")).lower()
    langs = {str(row.get("language", "")) for row in component.get("language_mix", []) if isinstance(row, dict)}
    archetypes = set(component.get("archetypes") or [])
    for rule in sorted(ruleset.get("rules", []), key=lambda r: int(r.get("priority", 999))):
        when = rule.get("when", {}) if isinstance(rule.get("when", {}), dict) else {}
        if when.get("component_kind") and _clean(when.get("component_kind")) != comp_kind:
            continue
        required_framework = _clean(when.get("framework")).lower()
        if required_framework and required_framework != comp_framework:
            continue
        required_frameworks = {str(x).strip().lower() for x in when.get("frameworks", []) if str(x).strip()}
        if required_frameworks and comp_framework not in required_frameworks:
            continue
        required_langs = {str(x) for x in when.get("language_includes", []) if str(x)}
        if required_langs and not (required_langs & langs):
            continue
        required_arch = {str(x) for x in when.get("archetypes", []) if str(x)}
        if required_arch and not (required_arch & archetypes):
            continue
        fired.append(str(rule.get("rule_id") or "rule"))
        then = rule.get("then", {}) if isinstance(rule.get("then", {}), dict) else {}
        skills.extend([str(x) for x in then.get("attach_skill_packs", []) if str(x)])
        agents.extend([str(x) for x in then.get("attach_agents", []) if str(x)])
        questions.extend([str(x) for x in then.get("add_gating_questions", []) if str(x)])
        track_lanes.extend([str(x) for x in then.get("suggest_tracks", []) if str(x)])
        chunking_profile = str(then.get("set_chunking_profile") or chunking_profile)
        confidence = max(confidence, 0.7)
    return {
        "skill_packs": list(dict.fromkeys(skills)),
        "agents": list(dict.fromkeys(agents)),
        "gating_questions": list(dict.fromkeys(questions)),
        "track_lanes": list(dict.fromkeys(track_lanes)),
        "fired_rules": fired,
        "chunking_profile": chunking_profile,
        "confidence": confidence or 0.55,
    }


def build_landscape_artifacts(
    *,
    repo: str,
    branch: str,
    commit_sha: str,
    entries: list[dict[str, Any]],
    file_contents: dict[str, str] | None = None,
    include_paths: list[str] | None = None,
    exclude_paths: list[str] | None = None,
) -> dict[str, Any]:
    file_contents = file_contents or {}
    include_paths = include_paths or []
    exclude_paths = exclude_paths or []
    files = [row for row in entries if isinstance(row, dict) and str(row.get("type", "blob")) == "blob"]
    total_files = len(files)
    binary_files = 0
    total_loc = 0
    total_tokens = 0
    language_stats: dict[str, dict[str, Any]] = {}
    build_systems: list[dict[str, Any]] = []
    datastore_notes: dict[str, dict[str, Any]] = {}
    top_dependencies: Counter[str] = Counter()
    largest_files: list[tuple[str, int]] = []
    dir_sizes: dict[str, int] = defaultdict(int)
    all_paths: list[str] = []

    for entry in files:
        path = _clean(entry.get("path"))
        if not path:
            continue
        all_paths.append(path)
        size = int(entry.get("size", 0) or 0)
        ext = PurePosixPath(path).suffix.lower()
        lang = _language_for_path(path)
        loc = _estimate_loc_from_size(size) if _is_text_like(path) else 0
        binary = not _is_text_like(path)
        if binary:
            binary_files += 1
        total_loc += loc
        total_tokens += max(0, loc * 4)
        stats = language_stats.setdefault(lang, {"files": 0, "loc": 0, "estimated_tokens": 0, "evidence": set()})
        stats["files"] += 1
        stats["loc"] += loc
        stats["estimated_tokens"] += max(0, loc * 4)
        if ext:
            stats["evidence"].add(ext)
        largest_files.append((path, size))
        parts = path.split("/")
        top = parts[0] if parts else path
        dir_sizes[top] += size

        build_kind = _build_kind_for_path(path)
        if build_kind:
            build_systems.append({
                "kind": build_kind,
                "paths": [path],
                "confidence": 0.95 if build_kind in {"vb6_vbp", "dotnet_sln", "dotnet_project"} else 0.85,
                "evidence": [{"type": "build_file", "build_file": path, "confidence": 0.95}],
            })

        if ext in {".ocx", ".dll", ".tlb"}:
            top_dependencies[PurePosixPath(path).name] += 1
        if ext in {".mdb", ".accdb"}:
            datastore_notes.setdefault("access_mdb", {"evidence": [], "notes": ["Access database artifacts present."]})
            datastore_notes["access_mdb"]["evidence"].append({"type": "path", "path": path, "confidence": 0.85})

    for path, text in file_contents.items():
        content = str(text or "")
        for store, pattern in DB_HINT_PATTERNS:
            if pattern.search(content):
                row = datastore_notes.setdefault(store, {"evidence": [], "notes": []})
                row["evidence"].append({"type": "content", "path": path, "confidence": 0.65})
                if store == "sqlserver":
                    row["notes"] = ["Connection string hints found in config or source files."]
        for dep in re.findall(r"\b[A-Za-z0-9_\-]+\.(?:OCX|DLL|TLB)\b", content, re.I):
            top_dependencies[dep.upper()] += 1

    php_artifacts = build_php_landscape_artifacts(
        repo=repo,
        branch=branch,
        commit_sha=commit_sha,
        entries=files,
        file_contents=file_contents,
    )
    php_profile = php_artifacts.get("php_framework_profile_v1", {}) if isinstance(php_artifacts.get("php_framework_profile_v1", {}), dict) else {}
    php_route_hints = php_artifacts.get("php_route_hints_v1", {}) if isinstance(php_artifacts.get("php_route_hints_v1", {}), dict) else {}

    language_rows = []
    meaningful_loc = max(1, sum(v["loc"] for v in language_stats.values()))
    for language, stats in sorted(language_stats.items(), key=lambda item: (-item[1]["loc"], -item[1]["files"], item[0])):
        conf = 0.95 if language in {"VB6", "C#", "VB.NET", "JavaScript", "TypeScript", "Python", "SQL"} else 0.75
        language_rows.append({
            "language": language,
            "stats": {
                "files": stats["files"],
                "loc": stats["loc"],
                "blank_loc": 0,
                "comment_loc": 0,
                "estimated_tokens": stats["estimated_tokens"],
            },
            "percent_loc": round((stats["loc"] / meaningful_loc) * 100, 1) if meaningful_loc else 0.0,
            "confidence": conf,
            "evidence": [{"type": "path", "path": f"**/*{ext}", "confidence": conf - 0.05} for ext in sorted(stats["evidence"])[:3]],
        })

    build_map: dict[str, dict[str, Any]] = {}
    for row in build_systems:
        kind = str(row["kind"])
        dest = build_map.setdefault(kind, {"kind": kind, "paths": [], "confidence": row["confidence"], "evidence": []})
        dest["paths"].extend(row["paths"])
        dest["evidence"].extend(row["evidence"])
    build_rows = list(build_map.values())

    archetype_rows = []
    detected_archetypes = _archetypes_for_paths(all_paths)
    for archetype in detected_archetypes:
        notes = []
        if archetype == "desktop_forms_vb6":
            notes = ["Multiple .frm and .vbp files detected."]
        elif archetype == "api_service":
            notes = ["Service/build project structure detected."]
        elif archetype == "reporting_pack":
            notes = ["Report or DataReport artifacts detected."]
        elif archetype == "database_scripts":
            notes = ["SQL scripts detected in repository."]
        elif archetype == "batch_jobs":
            notes = ["Batch or scripting artifacts detected."]
        archetype_rows.append({
            "archetype": archetype,
            "confidence": 0.9 if archetype in {"desktop_forms_vb6", "api_service"} else 0.75,
            "primary_evidence": [{"type": "path", "path": f"**/*{hint}", "confidence": 0.8} for hint in ARCTYPE_HINTS.get(archetype, [])[:2]],
            "notes": notes,
        })

    if int(php_profile.get("app_php_file_count", 0) or 0) > 0 and not any(row.get("archetype") in {"php_web_app", "php_application"} for row in archetype_rows):
        archetype_rows.append({
            "archetype": "php_application",
            "confidence": round(float(php_profile.get("confidence", 0.55) or 0.55), 2),
            "primary_evidence": [{"type": marker.get("type", "path"), "path": marker.get("value", ""), "confidence": marker.get("confidence", 0.6)} for marker in php_profile.get("markers", [])[:2]],
            "notes": ["PHP application structure detected from directory and file signatures."],
        })

    datastore_rows = [
        {
            "datastore": name,
            "confidence": 0.85 if name == "access_mdb" else 0.65,
            "evidence": details.get("evidence", [])[:4],
            "notes": details.get("notes", [])[:2],
        }
        for name, details in sorted(datastore_notes.items())
    ]

    dep_footprint = {
        "ocx_count": sum(1 for p in all_paths if p.lower().endswith('.ocx')),
        "com_dll_count": sum(1 for p in all_paths if p.lower().endswith(('.dll', '.tlb'))),
        "nuget_package_count": sum(1 for p in all_paths if PurePosixPath(p).name.lower() in {'packages.config', 'project.assets.json'}),
        "npm_package_count": sum(1 for p in all_paths if PurePosixPath(p).name.lower() == 'package-lock.json'),
        "java_dependency_count": sum(1 for p in all_paths if PurePosixPath(p).name.lower() in {'pom.xml', 'build.gradle', 'build.gradle.kts'}),
        "python_dependency_count": sum(1 for p in all_paths if PurePosixPath(p).name.lower() in {'requirements.txt', 'pyproject.toml'}),
        "top_dependencies": [name for name, _count in top_dependencies.most_common(8)],
    }

    largest_file_rows = [
        {"path": path, "size_bytes": size, "estimated_loc": _estimate_loc_from_size(size)}
        for path, size in sorted(largest_files, key=lambda item: item[1], reverse=True)[:8]
    ]
    largest_dir_rows = [
        {"path": path, "size_bytes": size}
        for path, size in sorted(dir_sizes.items(), key=lambda item: item[1], reverse=True)[:8]
    ]

    risk_rows: list[dict[str, Any]] = []
    vbp_count = sum(1 for row in build_rows if row["kind"] == "vb6_vbp" for _ in row.get("paths", []))
    if vbp_count > 1:
        risk_rows.append({
            "signal_id": "SIG_VARIANTS_001",
            "severity": "high",
            "title": "Multiple VB6 projects detected",
            "description": "More than one .vbp file appears present. Canonical variant selection is required before deep analysis.",
            "recommendation": "Confirm which VB6 project or variant is authoritative in Define Scope.",
            "evidence": [{"type": "build_file", "path": row_path, "confidence": 0.95} for row in build_rows if row["kind"] == "vb6_vbp" for row_path in row.get("paths", [])[:4]],
        })
    huge_files = [row for row in largest_file_rows if int(row.get("estimated_loc", 0) or 0) >= 15000]
    if huge_files:
        risk_rows.append({
            "signal_id": "SIG_HUGE_FILES_001",
            "severity": "medium",
            "title": "Very large files detected",
            "description": "Some files exceed safe context limits for deep analysis and will require chunked or streaming handling.",
            "recommendation": "Use streaming/decomposed analysis for files above the landscape threshold.",
            "evidence": [{"type": "path", "path": row["path"], "confidence": 0.8} for row in huge_files[:4]],
        })
    if datastore_rows and not any(row.get("datastore") in {"sqlserver", "oracle", "postgres", "db2"} for row in datastore_rows):
        risk_rows.append({
            "signal_id": "SIG_DB_SCHEMA_001",
            "severity": "medium",
            "title": "Database schema export not detected",
            "description": "Database signals exist, but no richer DDL/schema pack was detected in the repo snapshot.",
            "recommendation": "Provide a schema/DDL export to improve DB archaeology accuracy.",
            "evidence": datastore_rows[:2],
        })
    if len([row for row in language_rows if row["stats"]["loc"] > 0]) > 1:
        risk_rows.append({
            "signal_id": "SIG_POLYGLOT_001",
            "severity": "medium",
            "title": "Polyglot repository detected",
            "description": "Multiple languages/build systems were detected. Synthetix should route separate modernization tracks per component.",
            "recommendation": "Review the suggested tracks and confirm in-scope components before deep analysis.",
            "evidence": [{"type": "language", "path": row['language'], "confidence": row['confidence']} for row in language_rows[:4]],
        })
    if int(php_profile.get("app_php_file_count", 0) or 0) > 0:
        if str(php_profile.get("framework") or "") == "custom_php":
            risk_rows.append({
                "signal_id": "SIG_PHP_CUSTOM_001",
                "severity": "medium",
                "title": "Custom PHP application detected",
                "description": "PHP application structure was detected without a dominant framework marker. Route, session, and template behavior may be implicit.",
                "recommendation": "Use the PHP modernization lane and confirm target architecture before deep analysis.",
                "evidence": [{"type": marker.get("type", "path"), "path": marker.get("value", ""), "confidence": marker.get("confidence", 0.6)} for marker in php_profile.get("markers", [])[:4]],
            })
        if php_route_hints.get("session_state_complexity") in {"medium", "high"}:
            risk_rows.append({
                "signal_id": "SIG_PHP_SESSION_001",
                "severity": "medium" if php_route_hints.get("session_state_complexity") == "medium" else "high",
                "title": "PHP session-coupled workflows detected",
                "description": "Session-state usage was detected in sampled PHP files. Authentication and workflow state may be tightly coupled to the current runtime.",
                "recommendation": "Confirm target session/auth design early in Define Scope.",
                "evidence": [{"type": marker.get("type", "path"), "path": marker.get("value", ""), "confidence": marker.get("confidence", 0.6)} for marker in php_profile.get("markers", [])[:3]],
            })

    ruleset = default_router_ruleset(repo=repo, branch=branch, commit_sha=commit_sha)

    components: list[dict[str, Any]] = []
    component_id_seen: set[str] = set()
    for row in build_rows:
        for build_path in row.get("paths", [])[:20]:
            root = PurePosixPath(build_path).parent.as_posix() or "."
            root_prefix = "" if root == "." else f"{root}/"
            comp_paths = [p for p in all_paths if p == build_path or p.startswith(root_prefix)]
            comp_lang_counter: Counter[str] = Counter(_language_for_path(p) for p in comp_paths)
            comp_loc = sum(_estimate_loc_from_size(int(next((e.get('size', 0) for e in files if _clean(e.get('path')) == p), 0) or 0)) for p in comp_paths if _is_text_like(p))
            language_mix = []
            total_comp_loc = max(1, comp_loc)
            for lang, count in comp_lang_counter.most_common(4):
                loc = sum(_estimate_loc_from_size(int(next((e.get('size', 0) for e in files if _clean(e.get('path')) == p), 0) or 0)) for p in comp_paths if _language_for_path(p) == lang and _is_text_like(p))
                language_mix.append({"language": lang, "percent_loc": round((loc / total_comp_loc) * 100, 1) if total_comp_loc else 0.0})
            comp_id = f"cmp_{_slug(build_path)}"
            if comp_id in component_id_seen:
                continue
            component_id_seen.add(comp_id)
            archetypes = _archetypes_for_paths(comp_paths)
            datastore_touch = [row2["datastore"] for row2 in datastore_rows if any(row2["datastore"] in str(file_contents.get(p, '')).lower() or p.lower().endswith(('.mdb','.accdb','.sql')) for p in comp_paths[:40])]
            component = {
                "component_id": comp_id,
                "name": _component_display_name(build_path),
                "component_kind": _component_kind_from_build(row["kind"]),
                "root_paths": [root],
                "project_files": [build_path],
                "language_mix": language_mix,
                "stats": {
                    "files": len(comp_paths),
                    "loc": comp_loc,
                    "blank_loc": 0,
                    "comment_loc": 0,
                    "estimated_tokens": max(0, comp_loc * 4),
                },
                "archetypes": archetypes,
                "datastore_touch": sorted(set(datastore_touch)),
                "dependency_footprint": {
                    "ocx": sum(1 for p in comp_paths if p.lower().endswith('.ocx')),
                    "com": sum(1 for p in comp_paths if p.lower().endswith(('.dll', '.tlb'))),
                    "nuget": sum(1 for p in comp_paths if PurePosixPath(p).name.lower() in {'packages.config', 'project.assets.json'}),
                    "npm": sum(1 for p in comp_paths if PurePosixPath(p).name.lower() == 'package-lock.json'),
                },
                "risk_flags": [],
                "variant_candidate": row["kind"] == "vb6_vbp" and vbp_count > 1,
                "shared_foundation_candidate": row["kind"] in {"dotnet_sln", "dotnet_project"} and root.lower().startswith(('shared', 'common', 'lib')),
                "confidence": 0.92 if row["kind"] in {"vb6_vbp", "dotnet_sln"} else 0.82,
                "evidence": row.get("evidence", []),
                "framework": "",
            }
            routed = _router_apply(component, ruleset)
            component["recommended_skill_packs"] = routed["skill_packs"]
            component["recommended_agents"] = routed["agents"]
            component["routing_rules_fired"] = routed["fired_rules"]
            component["chunking_profile"] = routed["chunking_profile"]
            suggested_tracks = []
            for idx, lane in enumerate(routed["track_lanes"], start=1):
                suggested_tracks.append({
                    "track_id": f"TRK_{_slug(comp_id)}_{idx:03d}",
                    "title": {
                        "ui_modernization": f"{component['name']} UI modernization",
                        "data_modernization": f"{component['name']} data modernization",
                        "api_modernization": f"{component['name']} service modernization",
                        "service_modernization": f"{component['name']} service modernization",
                        "web_modernization": f"{component['name']} web modernization",
                        "reporting_modernization": f"{component['name']} reporting modernization",
                        "batch_modernization": f"{component['name']} batch modernization",
                        "component_assessment": f"{component['name']} component assessment",
                    }.get(lane, f"{component['name']} modernization"),
                    "lane": lane,
                    "source_components": [comp_id],
                    "suggested_target": {
                        "ui_modernization": "C# WPF / WinUI / Web",
                        "data_modernization": "SQL Server or Postgres",
                        "api_modernization": "Keep .NET with API hardening",
                        "service_modernization": "Containerized service runtime",
                        "web_modernization": "Modern web stack / API + UI split",
                        "reporting_modernization": "SSRS / Power BI / keep legacy reports",
                        "batch_modernization": "Containerized scheduled jobs",
                        "component_assessment": "Target to be confirmed during Define Scope",
                    }.get(lane, "TBD"),
                    "recommended_skill_packs": routed["skill_packs"],
                    "quality_gates": ["Scope confirmed", "Architecture approved"],
                    "confidence": round(routed["confidence"], 2),
                    "why": f"Detected as {component['component_kind']} with archetypes: {', '.join(archetypes) or 'none'}.",
                    "risks": list(component.get("risk_flags") or []),
                    "gating_questions": routed["gating_questions"],
                })
            if not suggested_tracks:
                suggested_tracks.append({
                    "track_id": f"TRK_{_slug(comp_id)}_001",
                    "title": f"{component['name']} component assessment",
                    "lane": "component_assessment",
                    "source_components": [comp_id],
                    "suggested_target": "Target to be confirmed during Define Scope",
                    "recommended_skill_packs": routed["skill_packs"],
                    "quality_gates": ["Scope confirmed"],
                    "confidence": round(max(routed["confidence"], 0.45), 2),
                    "why": f"Detected as {component['component_kind']} but no specific routing rule fired yet.",
                    "risks": list(component.get("risk_flags") or []),
                    "gating_questions": routed["gating_questions"] or ["Confirm the intended modernization lane for this component."],
                })
            component["suggested_tracks"] = suggested_tracks
            components.append(component)

    if int(php_profile.get("app_php_file_count", 0) or 0) > 0 and not any(str(component.get("component_kind")) in {"php_project", "custom_php_app"} for component in components):
        php_component = {
            "component_id": "cmp_php_app",
            "name": "PHP application",
            "component_kind": "custom_php_app" if str(php_profile.get("framework") or "") == "custom_php" else "php_project",
            "root_paths": php_profile.get("app_roots", [])[:6] or ["."],
            "project_files": [],
            "language_mix": [{"language": "PHP", "percent_loc": 100.0}],
            "stats": {
                "files": int(php_profile.get("app_php_file_count", 0) or 0),
                "loc": language_stats.get("PHP", {}).get("loc", 0),
                "blank_loc": 0,
                "comment_loc": 0,
                "estimated_tokens": language_stats.get("PHP", {}).get("estimated_tokens", 0),
            },
            "archetypes": [row.get("archetype") for row in archetype_rows if row.get("archetype") in {"php_web_app", "php_application"}] or ["php_application"],
            "datastore_touch": [row["datastore"] for row in datastore_rows if row.get("datastore") in {"sqlserver", "mysql", "postgres", "oracle"}],
            "dependency_footprint": {
                "ocx": 0,
                "com": 0,
                "nuget": 0,
                "npm": 0,
                "composer": 1 if php_profile.get("uses_composer") else 0,
            },
            "risk_flags": ["custom_php_structure"] if str(php_profile.get("framework") or "") == "custom_php" else [],
            "variant_candidate": False,
            "shared_foundation_candidate": False,
            "confidence": round(float(php_profile.get("confidence", 0.55) or 0.55), 2),
            "evidence": [{"type": marker.get("type", "path"), "path": marker.get("value", ""), "confidence": marker.get("confidence", 0.6)} for marker in php_profile.get("markers", [])[:4]],
            "framework": str(php_profile.get("framework") or ""),
        }
        routed = _router_apply(php_component, ruleset)
        php_component["recommended_skill_packs"] = routed["skill_packs"]
        php_component["recommended_agents"] = routed["agents"]
        php_component["routing_rules_fired"] = routed["fired_rules"]
        php_component["chunking_profile"] = routed["chunking_profile"]
        php_component["suggested_tracks"] = [
            {
                "track_id": "TRK_cmp_php_app_001",
                "title": "PHP web modernization",
                "lane": routed["track_lanes"][0] if routed["track_lanes"] else "php_web_modernization",
                "source_components": ["cmp_php_app"],
                "suggested_target": "TypeScript modular monolith / Next.js + NestJS",
                "recommended_skill_packs": routed["skill_packs"],
                "quality_gates": ["Scope confirmed", "Architecture approved"],
                "confidence": round(routed["confidence"], 2),
                "why": f"Detected as {php_component['component_kind']} with framework profile {php_component['framework'] or 'custom_php'}.",
                "risks": php_component["risk_flags"],
                "gating_questions": routed["gating_questions"],
            }
        ]
        components.append(php_component)

    extra_components: list[dict[str, Any]] = []
    if any(p.lower().endswith((".dsr", ".rpt")) for p in all_paths):
        extra_components.append({
            "component_id": "cmp_reporting_pack",
            "name": "Reporting pack",
            "component_kind": "reporting_pack",
            "root_paths": sorted({PurePosixPath(p).parent.as_posix() or '.' for p in all_paths if p.lower().endswith((".dsr", ".rpt"))})[:6],
            "project_files": [p for p in all_paths if p.lower().endswith((".dsr", ".rpt"))][:10],
            "language_mix": [{"language": "VB6", "percent_loc": 100.0}],
            "stats": {"files": sum(1 for p in all_paths if p.lower().endswith((".dsr", ".rpt"))), "loc": 0, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": 0},
            "archetypes": ["reporting_pack"],
            "datastore_touch": [row['datastore'] for row in datastore_rows[:1]],
            "dependency_footprint": {"ocx": 0, "com": 0, "nuget": 0, "npm": 0},
            "risk_flags": ["reporting_scope_decision"],
            "variant_candidate": False,
            "shared_foundation_candidate": False,
            "confidence": 0.76,
            "evidence": [{"type": "path", "path": p, "confidence": 0.75} for p in all_paths if p.lower().endswith((".dsr", ".rpt"))][:4],
        })
    if any(p.lower().endswith((".ps1", ".bat", ".cmd", ".sh")) for p in all_paths):
        extra_components.append({
            "component_id": "cmp_batch_pack",
            "name": "Batch and automation scripts",
            "component_kind": "batch_pack",
            "root_paths": sorted({PurePosixPath(p).parent.as_posix() or '.' for p in all_paths if p.lower().endswith((".ps1", ".bat", ".cmd", ".sh"))})[:6],
            "project_files": [p for p in all_paths if p.lower().endswith((".ps1", ".bat", ".cmd", ".sh"))][:10],
            "language_mix": [{"language": "PowerShell", "percent_loc": 100.0}],
            "stats": {"files": sum(1 for p in all_paths if p.lower().endswith((".ps1", ".bat", ".cmd", ".sh"))), "loc": 0, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": 0},
            "archetypes": ["batch_jobs"],
            "datastore_touch": [],
            "dependency_footprint": {"ocx": 0, "com": 0, "nuget": 0, "npm": 0},
            "risk_flags": ["batch_orchestration_unknown"],
            "variant_candidate": False,
            "shared_foundation_candidate": False,
            "confidence": 0.7,
            "evidence": [{"type": "path", "path": p, "confidence": 0.7} for p in all_paths if p.lower().endswith((".ps1", ".bat", ".cmd", ".sh"))][:4],
        })
    for component in extra_components:
        routed = _router_apply(component, ruleset)
        component["recommended_skill_packs"] = routed["skill_packs"]
        component["recommended_agents"] = routed["agents"]
        component["routing_rules_fired"] = routed["fired_rules"]
        component["chunking_profile"] = routed["chunking_profile"]
        component["suggested_tracks"] = [
            {
                "track_id": f"TRK_{_slug(component['component_id'])}_001",
                "title": f"{component['name']} modernization",
                "lane": routed['track_lanes'][0] if routed['track_lanes'] else 'unknown',
                "source_components": [component['component_id']],
                "suggested_target": 'SSRS / Power BI / keep legacy reports' if component['component_kind'] == 'reporting_pack' else 'Containerized scheduled jobs',
                "recommended_skill_packs": routed['skill_packs'],
                "quality_gates": ['Scope confirmed'],
                "confidence": round(routed['confidence'], 2),
                "why": f"Detected as {component['component_kind']} from file signatures.",
                "risks": component['risk_flags'],
                "gating_questions": routed['gating_questions'],
            }
        ]
        components.append(component)

    edges: list[dict[str, Any]] = []
    edge_id = 1
    for i, left in enumerate(components):
        left_ds = set(left.get("datastore_touch") or [])
        left_langs = {row.get("language") for row in left.get("language_mix", []) if isinstance(row, dict)}
        for right in components[i + 1:]:
            right_ds = set(right.get("datastore_touch") or [])
            right_langs = {row.get("language") for row in right.get("language_mix", []) if isinstance(row, dict)}
            shared_db = sorted(left_ds & right_ds)
            if shared_db:
                edges.append({
                    "edge_id": f"e{edge_id}",
                    "from_component": left["component_id"],
                    "to_component": right["component_id"],
                    "edge_kind": "shares_db",
                    "confidence": 0.75,
                    "notes": [f"Shared datastore signals: {', '.join(shared_db)}"],
                    "evidence": [{"type": "datastore", "path": name, "confidence": 0.7} for name in shared_db[:3]],
                })
                edge_id += 1
            elif left_langs and right_langs and left_langs != right_langs and (left.get("root_paths") and right.get("root_paths")):
                if PurePosixPath(str(left["root_paths"][0])).parts[:1] == PurePosixPath(str(right["root_paths"][0])).parts[:1]:
                    edges.append({
                        "edge_id": f"e{edge_id}",
                        "from_component": left["component_id"],
                        "to_component": right["component_id"],
                        "edge_kind": "cross_language_boundary",
                        "confidence": 0.45,
                        "notes": ["Components live in adjacent repo areas with different primary languages."],
                        "evidence": [],
                    })
                    edge_id += 1

    tracks = []
    seen_track_ids = set()
    for component in components:
        for track in component.get("suggested_tracks", []):
            track_id = str(track.get("track_id") or "")
            if not track_id or track_id in seen_track_ids:
                continue
            seen_track_ids.add(track_id)
            tracks.append(track)

    assumptions = [
        "Landscape scan is deterministic and shallow; counts and LOC are approximate where only tree metadata was available.",
        "Deep analysis and scope lock still remain the source of truth for implementation planning.",
    ]
    open_questions = []
    if vbp_count > 1:
        open_questions.append("Which VB6 project or variant is canonical for delivery scope?")
    if any(track.get("lane") == "ui_modernization" for track in tracks):
        open_questions.append("Which target UI strategy should apply per UI track: Web, WPF, or WinUI?")
    if any(track.get("lane") == "data_modernization" for track in tracks):
        open_questions.append("Which target datastore should be used for legacy Access/embedded data workloads?")
    if any(track.get("lane") == "reporting_modernization" for track in tracks):
        open_questions.append("Should reporting be modernized now or explicitly deferred as a separate track?")

    repo_hash = hashlib.sha1(f"{repo}|{branch}|{commit_sha}".encode("utf-8")).hexdigest()[:10]
    meta = {
        "artifact_type": "repo_landscape",
        "artifact_version": "1.0",
        "artifact_id": f"art_landscape_{repo_hash}",
        "run_id": "discover",
        "generated_at": "",
        "producer": {
            "agent": "LandscapeScanner",
            "skill_pack": "landscape_core",
            "skill_version": "1.0",
            "engine_version": "0.1.0",
        },
        "context": {"repo": repo, "branch": branch, "commit_sha": commit_sha, "stage": "Discover"},
    }

    repo_landscape_v1 = {
        "meta": meta,
        "scan_summary": {
            "root_paths_scanned": ["/"],
            "excluded_paths": exclude_paths,
            "included_paths": include_paths,
            "total_files": total_files,
            "binary_files": binary_files,
            "total_loc": total_loc,
            "estimated_tokens": total_tokens,
            "duration_ms": 0,
            "largest_files": largest_file_rows,
            "largest_directories": largest_dir_rows,
            "notes": [
                "Shallow scan only (no full AST parse).",
                "Potential secrets are not surfaced; only counts and flags are reported.",
            ],
        },
        "languages": language_rows,
        "build_systems": build_rows,
        "archetypes": archetype_rows,
        "datastore_signals": datastore_rows,
        "dependency_footprint": dep_footprint,
        "high_risk_signals": risk_rows,
    }

    component_inventory_v1 = {
        "meta": {
            **meta,
            "artifact_type": "component_inventory",
            "artifact_id": f"art_components_{repo_hash}",
        },
        "graph_summary": {
            "component_count": len(components),
            "edge_count": len(edges),
            "cross_language_edges": len([e for e in edges if e.get("edge_kind") == "cross_language_boundary"]),
            "shared_db_edges": len([e for e in edges if e.get("edge_kind") == "shares_db"]),
            "notes": ["Component IDs are stable for this repo+commit fingerprint."],
        },
        "components": components,
        "edges": edges,
    }

    modernization_track_plan_v1 = {
        "meta": {
            **meta,
            "artifact_type": "modernization_track_plan",
            "artifact_id": f"art_tracks_{repo_hash}",
        },
        "tracks": tracks,
        "assumptions": assumptions,
        "open_questions": open_questions,
    }

    router_ruleset_v1 = default_router_ruleset(repo=repo, branch=branch, commit_sha=commit_sha)
    router_ruleset_v1["meta"]["generated_at"] = repo_landscape_v1["meta"]["generated_at"]

    return {
        "repo_landscape_v1": repo_landscape_v1,
        "component_inventory_v1": component_inventory_v1,
        "modernization_track_plan_v1": modernization_track_plan_v1,
        "router_ruleset_v1": router_ruleset_v1,
        **php_artifacts,
    }


def build_greenfield_landscape_artifacts(
    *,
    objectives: str,
    use_case: str,
    integration_context: dict[str, Any] | None = None,
    target_language: str = "",
    target_platform: str = "",
    database_source: str = "",
    database_target: str = "",
) -> dict[str, Any]:
    integration = integration_context if isinstance(integration_context, dict) else {}
    greenfield = integration.get("greenfield", {}) if isinstance(integration.get("greenfield", {}), dict) else {}
    domain_pack_id = _clean(integration.get("domain_pack_id"))
    jurisdiction = _clean(integration.get("jurisdiction"))
    data_classification = integration.get("data_classification", [])
    if not isinstance(data_classification, list):
        data_classification = []

    objective_text = _clean(objectives)
    objective_lower = objective_text.lower()
    repo_target = _clean(greenfield.get("repo_target"))
    repo_destination = _clean(greenfield.get("repo_destination")) or "local"
    tracker_provider = _clean(greenfield.get("tracker_provider"))
    tracker_project = _clean(greenfield.get("tracker_project"))
    normalized_target_language = _clean(target_language) or "TBD"
    normalized_target_platform = _clean(target_platform) or "TBD"
    normalized_db_target = _clean(database_target)
    normalized_db_source = _clean(database_source)

    repo_hash = hashlib.sha1(
        f"{objective_text}|{use_case}|{repo_target}|{normalized_target_language}|{normalized_db_target}".encode("utf-8")
    ).hexdigest()[:10]
    generated_at = ""

    language_rows = []
    if normalized_target_language and normalized_target_language != "TBD":
        language_rows.append({
            "language": normalized_target_language,
            "stats": {"files": 0, "loc": 0, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": 0},
            "percent_loc": 100.0,
            "confidence": 0.72,
            "evidence": [{"type": "decision", "path": f"target_language={normalized_target_language}", "confidence": 0.72}],
        })
    if normalized_db_target:
        language_rows.append({
            "language": "SQL",
            "stats": {"files": 0, "loc": 0, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": 0},
            "percent_loc": 0.0,
            "confidence": 0.65,
            "evidence": [{"type": "decision", "path": f"database_target={normalized_db_target}", "confidence": 0.65}],
        })

    build_rows = []
    if repo_destination:
        build_rows.append({
            "kind": f"planned_repo_{_slug(repo_destination)}",
            "paths": [repo_target or "(target repo pending)"],
            "confidence": 0.7,
            "evidence": [{"type": "decision", "build_file": repo_target or repo_destination, "confidence": 0.7}],
        })

    archetypes = []
    if any(token in objective_lower for token in ["web", "portal", "ui", "screen", "frontend", "front end"]):
        archetypes.append("planned_web_app")
    if any(token in objective_lower for token in ["api", "service", "integration", "backend", "back end"]):
        archetypes.append("planned_service")
    if any(token in objective_lower for token in ["report", "dashboard", "analytics"]):
        archetypes.append("planned_reporting")
    if normalized_db_target or normalized_db_source or use_case == "database_conversion":
        archetypes.append("planned_data_platform")
    if not archetypes:
        archetypes = ["planned_application"]
    archetype_rows = [
        {
            "archetype": name,
            "confidence": 0.68,
            "primary_evidence": [{"type": "objective", "path": objective_text[:120] or "scope inputs pending", "confidence": 0.68}],
            "notes": ["Derived from scope inputs rather than a legacy repo scan."],
        }
        for name in archetypes
    ]

    datastore_rows = []
    if normalized_db_target:
        datastore_rows.append({
            "datastore": _slug(normalized_db_target, max_len=32),
            "confidence": 0.74,
            "evidence": [{"type": "decision", "path": f"target={normalized_db_target}", "confidence": 0.74}],
            "notes": [f"Target datastore selected: {normalized_db_target}."],
        })
    elif normalized_db_source:
        datastore_rows.append({
            "datastore": _slug(normalized_db_source, max_len=32),
            "confidence": 0.58,
            "evidence": [{"type": "decision", "path": f"source={normalized_db_source}", "confidence": 0.58}],
            "notes": [f"Source datastore mentioned: {normalized_db_source}. Target still needs confirmation."],
        })

    components: list[dict[str, Any]] = []
    edges: list[dict[str, Any]] = []
    tracks: list[dict[str, Any]] = []

    def add_component(
        *,
        component_id: str,
        name: str,
        component_kind: str,
        lane: str,
        suggested_target: str,
        why: str,
        risk_flags: list[str] | None = None,
        gating_questions: list[str] | None = None,
    ) -> None:
        risk_flags = risk_flags or []
        gating_questions = gating_questions or []
        lang_mix = []
        if component_kind in {"planned_ui", "planned_service"} and normalized_target_language and normalized_target_language != "TBD":
            lang_mix.append({"language": normalized_target_language, "percent_loc": 100.0})
        elif component_kind == "planned_data" and normalized_db_target:
            lang_mix.append({"language": "SQL", "percent_loc": 100.0})
        component = {
            "component_id": component_id,
            "name": name,
            "component_kind": component_kind,
            "root_paths": [repo_target or "(greenfield)"],
            "project_files": [repo_target] if repo_target else [],
            "language_mix": lang_mix,
            "stats": {"files": 0, "loc": 0, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": 0},
            "archetypes": archetypes,
            "datastore_touch": [row["datastore"] for row in datastore_rows[:1]] if component_kind == "planned_data" else [],
            "dependency_footprint": {"ocx": 0, "com": 0, "nuget": 0, "npm": 0},
            "risk_flags": risk_flags,
            "variant_candidate": False,
            "shared_foundation_candidate": component_kind == "planned_service",
            "confidence": 0.66,
            "evidence": [{"type": "objective", "path": objective_text[:160] or "No objective provided yet.", "confidence": 0.66}],
            "recommended_skill_packs": [],
            "recommended_agents": [],
            "routing_rules_fired": [],
            "chunking_profile": "greenfield_planning",
        }
        track = {
            "track_id": f"TRK_{_slug(component_id)}_001",
            "title": f"{name} planning",
            "lane": lane,
            "source_components": [component_id],
            "suggested_target": suggested_target,
            "recommended_skill_packs": [],
            "quality_gates": ["Scope confirmed", "Architecture approved"],
            "confidence": 0.66,
            "why": why,
            "risks": risk_flags,
            "gating_questions": gating_questions,
        }
        component["suggested_tracks"] = [track]
        components.append(component)
        tracks.append(track)

    add_component(
        component_id="cmp_greenfield_ui",
        name="User experience",
        component_kind="planned_ui",
        lane="web_modernization",
        suggested_target=normalized_target_language if normalized_target_language != "TBD" else "Target UI stack to be confirmed",
        why="Greenfield delivery will require a user-facing experience layer aligned to the chosen target stack.",
        risk_flags=["target_ui_decision_pending"] if normalized_target_language == "TBD" else [],
        gating_questions=["Confirm target UX strategy and primary user channels."],
    )
    add_component(
        component_id="cmp_greenfield_services",
        name="Application services",
        component_kind="planned_service",
        lane="service_modernization",
        suggested_target=normalized_target_language if normalized_target_language != "TBD" else "Target service stack to be confirmed",
        why="Business capabilities need an application service layer and API boundary even when no legacy repo exists.",
        risk_flags=["service_boundary_review"],
        gating_questions=["Confirm service boundaries and integration patterns."],
    )
    if normalized_db_target or use_case == "database_conversion":
        add_component(
            component_id="cmp_greenfield_data",
            name="Data platform",
            component_kind="planned_data",
            lane="data_modernization",
            suggested_target=normalized_db_target or "Target datastore to be confirmed",
            why="Current scope includes a database concern that needs a target data platform plan.",
            risk_flags=["target_datastore_pending"] if not normalized_db_target else [],
            gating_questions=["Confirm target datastore and data governance requirements."],
        )
    if any(token in objective_lower for token in ["report", "dashboard", "analytics"]):
        add_component(
            component_id="cmp_greenfield_reporting",
            name="Reporting and insights",
            component_kind="planned_reporting",
            lane="reporting_modernization",
            suggested_target="Power BI / SSRS / reporting target to be confirmed",
            why="Reporting and analytics needs to be planned as a separate workstream.",
            risk_flags=["reporting_scope_decision"],
            gating_questions=["Confirm whether reporting is in scope for the first release."],
        )
    if any(token in objective_lower for token in ["integration", "partner", "external", "api", "file exchange"]):
        add_component(
            component_id="cmp_greenfield_integrations",
            name="Dependencies and integrations",
            component_kind="planned_integration",
            lane="component_assessment",
            suggested_target="Integration contract and target systems to be confirmed",
            why="The objective signals external dependencies or integration surfaces that need separate validation.",
            risk_flags=["integration_scope_pending"],
            gating_questions=["Confirm business-critical integrations and ownership boundaries."],
        )

    edge_defs = [
        ("cmp_greenfield_ui", "cmp_greenfield_services", "planned_flow"),
        ("cmp_greenfield_services", "cmp_greenfield_data", "planned_data_dependency"),
        ("cmp_greenfield_services", "cmp_greenfield_integrations", "planned_integration_dependency"),
        ("cmp_greenfield_reporting", "cmp_greenfield_data", "planned_reporting_dependency"),
    ]
    edge_id = 1
    component_ids = {c["component_id"] for c in components}
    for left, right, kind in edge_defs:
        if left in component_ids and right in component_ids:
            edges.append({
                "edge_id": f"e{edge_id}",
                "from_component": left,
                "to_component": right,
                "edge_kind": kind,
                "confidence": 0.62,
                "notes": ["Planned architecture dependency derived from greenfield scope inputs."],
                "evidence": [{"type": "objective", "path": objective_text[:120] or "Scope inputs pending", "confidence": 0.62}],
            })
            edge_id += 1

    risk_rows = []
    if not objective_text:
        risk_rows.append({
            "signal_id": "SIG_SCOPE_001",
            "severity": "medium",
            "title": "Business objective is still sparse",
            "description": "Greenfield landscape can render a starter view, but track confidence is low until a business objective is provided.",
            "recommendation": "Enter the target business outcome and expected workflows in Define Scope.",
            "evidence": [{"type": "objective", "path": "(empty)", "confidence": 0.8}],
        })
    if normalized_target_language == "TBD":
        risk_rows.append({
            "signal_id": "SIG_STACK_001",
            "severity": "medium",
            "title": "Target implementation stack is not confirmed",
            "description": "The intended target language or platform has not been selected yet.",
            "recommendation": "Confirm the target application stack before architecture planning continues.",
            "evidence": [{"type": "decision", "path": "target_language=TBD", "confidence": 0.75}],
        })
    if not normalized_db_target and use_case in {"database_conversion", "code_modernization"}:
        risk_rows.append({
            "signal_id": "SIG_DATA_001",
            "severity": "medium",
            "title": "Target datastore is not confirmed",
            "description": "Scope implies data design work, but the target database has not been locked yet.",
            "recommendation": "Confirm the target datastore and governance constraints in Define Scope.",
            "evidence": [{"type": "decision", "path": "database_target=TBD", "confidence": 0.72}],
        })
    if not jurisdiction and not data_classification:
        risk_rows.append({
            "signal_id": "SIG_POLICY_001",
            "severity": "low",
            "title": "Compliance posture is not yet specified",
            "description": "No explicit jurisdiction or data-classification context was supplied for the greenfield scope.",
            "recommendation": "Set compliance, jurisdiction, and data-classification expectations before planning controls.",
            "evidence": [{"type": "decision", "path": "jurisdiction/data_classification missing", "confidence": 0.7}],
        })

    assumptions = [
        "Greenfield landscape is derived from scope inputs and target selections rather than a legacy code scan.",
        "Component and track definitions are planning scaffolds and should be refined during scope lock.",
    ]
    open_questions = []
    if not objective_text:
        open_questions.append("What business outcome should this greenfield solution deliver first?")
    if normalized_target_language == "TBD":
        open_questions.append("Which target language and platform should be used for the primary delivery track?")
    if not normalized_db_target:
        open_questions.append("What target datastore should back the solution data model?")
    if tracker_provider == "none" or not tracker_project:
        open_questions.append("Should delivery tracking be linked to Jira/Linear before planning starts?")

    meta = {
        "artifact_type": "repo_landscape",
        "artifact_version": "1.0",
        "artifact_id": f"art_landscape_greenfield_{repo_hash}",
        "run_id": "discover",
        "generated_at": generated_at,
        "producer": {
            "agent": "LandscapePlanner",
            "skill_pack": "landscape_core",
            "skill_version": "1.0",
            "engine_version": "0.1.0",
        },
        "context": {"repo": repo_target or "(greenfield)", "branch": "", "commit_sha": "", "stage": "Discover"},
    }

    repo_landscape_v1 = {
        "meta": meta,
        "landscape_mode": "greenfield",
        "solution_summary": {
            "repo_destination": repo_destination,
            "repo_target": repo_target,
            "tracker_provider": tracker_provider,
            "tracker_project": tracker_project,
            "target_language": normalized_target_language,
            "target_platform": normalized_target_platform,
            "database_target": normalized_db_target,
            "domain_pack_id": domain_pack_id,
            "jurisdiction": jurisdiction,
        },
        "scan_summary": {
            "root_paths_scanned": [],
            "excluded_paths": [],
            "included_paths": [],
            "total_files": 0,
            "binary_files": 0,
            "total_loc": 0,
            "estimated_tokens": 0,
            "duration_ms": 0,
            "largest_files": [],
            "largest_directories": [],
            "notes": [
                "No legacy repo scan was required for this landscape.",
                "View is synthesized from greenfield scope inputs and target decisions.",
            ],
        },
        "languages": language_rows,
        "build_systems": build_rows,
        "archetypes": archetype_rows,
        "datastore_signals": datastore_rows,
        "dependency_footprint": {
            "ocx_count": 0,
            "com_dll_count": 0,
            "nuget_package_count": 0,
            "npm_package_count": 0,
            "java_dependency_count": 0,
            "python_dependency_count": 0,
            "top_dependencies": [],
        },
        "high_risk_signals": risk_rows,
    }

    component_inventory_v1 = {
        "meta": {**meta, "artifact_type": "component_inventory", "artifact_id": f"art_components_greenfield_{repo_hash}"},
        "graph_summary": {
            "component_count": len(components),
            "edge_count": len(edges),
            "cross_language_edges": 0,
            "shared_db_edges": len([e for e in edges if e.get("edge_kind") == "planned_data_dependency"]),
            "notes": ["Components are planned workstreams, not scanned legacy projects."],
        },
        "components": components,
        "edges": edges,
    }

    modernization_track_plan_v1 = {
        "meta": {**meta, "artifact_type": "modernization_track_plan", "artifact_id": f"art_tracks_greenfield_{repo_hash}"},
        "tracks": tracks,
        "assumptions": assumptions,
        "open_questions": open_questions,
    }

    router_ruleset_v1 = default_router_ruleset(repo=repo_target or "greenfield", branch="", commit_sha="")
    router_ruleset_v1["meta"]["generated_at"] = generated_at

    return {
        "repo_landscape_v1": repo_landscape_v1,
        "component_inventory_v1": component_inventory_v1,
        "modernization_track_plan_v1": modernization_track_plan_v1,
        "router_ruleset_v1": router_ruleset_v1,
    }
