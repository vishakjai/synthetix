from __future__ import annotations

import json
import re
from pathlib import PurePosixPath
from typing import Any


PHP_TEMPLATE_SUFFIXES = (".blade.php", ".twig", ".tpl.php", ".phtml")
PHP_CONTROLLER_MARKERS = (
    "/controller/",
    "/controllers/",
    "/app/http/controllers/",
    "/src/controller/",
    "/application/controllers/",
)


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _path_matches(path: str, markers: tuple[str, ...]) -> bool:
    lower = path.lower()
    return any(marker in lower for marker in markers)


def _load_composer_packages(file_contents: dict[str, str]) -> set[str]:
    packages: set[str] = set()
    for path, body in (file_contents or {}).items():
        if PurePosixPath(str(path)).name.lower() != "composer.json":
            continue
        try:
            payload = json.loads(str(body or ""))
        except Exception:
            continue
        for section in ("require", "require-dev"):
            block = payload.get(section, {})
            if isinstance(block, dict):
                packages.update(str(name).lower() for name in block.keys() if str(name).strip())
    return packages


def _load_vendor_packages(entries: list[dict[str, Any]]) -> set[str]:
    packages: set[str] = set()
    for row in entries or []:
        if not isinstance(row, dict):
            continue
        path = _clean(row.get("path")).replace("\\", "/")
        low = path.lower()
        if not low.endswith("/composer.json"):
            continue
        parts = PurePosixPath(path).parts
        if len(parts) >= 3 and parts[0].lower() == "vendor":
            vendor = _clean(parts[1]).lower()
            package = _clean(parts[2]).lower()
            if vendor and package:
                packages.add(f"{vendor}/{package}")
    return packages


def detect_php_framework_profile(
    *,
    entries: list[dict[str, Any]],
    file_contents: dict[str, str] | None = None,
) -> dict[str, Any]:
    file_contents = file_contents or {}
    all_paths = [
        _clean(row.get("path"))
        for row in entries
        if isinstance(row, dict) and str(row.get("type", "blob")) == "blob" and _clean(row.get("path"))
    ]
    php_paths = [path for path in all_paths if path.lower().endswith(".php") or path.lower().endswith(PHP_TEMPLATE_SUFFIXES)]
    app_php_paths = [path for path in php_paths if not path.lower().startswith(("vendor/", "third_party/", "node_modules/"))]
    vendor_php_paths = [path for path in php_paths if path not in app_php_paths]

    composer_packages = _load_composer_packages(file_contents)
    composer_packages.update(_load_vendor_packages(entries))
    controller_files = [
        path for path in app_php_paths
        if _path_matches(path, PHP_CONTROLLER_MARKERS)
        or PurePosixPath(path).name.lower().endswith("controller.php")
    ]
    template_files = [
        path for path in app_php_paths
        if path.lower().endswith(PHP_TEMPLATE_SUFFIXES)
        or "/views/" in path.lower()
        or "/templates/" in path.lower()
        or "/view/" in path.lower()
    ]
    route_files = [
        path for path in app_php_paths
        if "/routes/" in path.lower()
        or PurePosixPath(path).name.lower() in {"web.php", "api.php", "routes.php"}
        or path.lower().startswith("config/routes")
    ]

    path_set = {path.lower() for path in all_paths}
    markers: list[dict[str, Any]] = []
    framework = "custom_php"
    confidence = 0.55

    def add_marker(kind: str, value: str, conf: float) -> None:
        markers.append({"type": kind, "value": value, "confidence": conf})

    if "artisan" in path_set or any(pkg.startswith("laravel/") for pkg in composer_packages) or any(path.lower().startswith("app/http/controllers/") for path in app_php_paths):
        framework = "laravel"
        confidence = 0.92
        if "artisan" in path_set:
            add_marker("path", "artisan", 0.95)
        if any(path.lower().startswith("routes/") for path in all_paths):
            add_marker("path", "routes/", 0.9)
        if any(pkg.startswith("laravel/") for pkg in composer_packages):
            add_marker("composer", "laravel/*", 0.95)
    elif "bin/console" in path_set or any(pkg.startswith("symfony/") for pkg in composer_packages) or any(path.lower().startswith("src/controller/") for path in app_php_paths):
        framework = "symfony"
        confidence = 0.9
        if "bin/console" in path_set:
            add_marker("path", "bin/console", 0.95)
        if any(pkg.startswith("symfony/") for pkg in composer_packages):
            add_marker("composer", "symfony/*", 0.95)
    elif "wp-config.php" in path_set or any(path.lower().startswith("wp-content/") for path in all_paths):
        framework = "wordpress"
        confidence = 0.95
        if "wp-config.php" in path_set:
            add_marker("path", "wp-config.php", 0.98)
        if any(path.lower().startswith("wp-content/") for path in all_paths):
            add_marker("path", "wp-content/", 0.95)
    elif any(path.lower().startswith("application/controllers/") for path in app_php_paths):
        framework = "codeigniter"
        confidence = 0.84
        add_marker("path", "application/controllers/", 0.88)
    elif any(pkg.startswith("cakephp/") for pkg in composer_packages):
        framework = "cakephp"
        confidence = 0.85
        add_marker("composer", "cakephp/*", 0.9)
    elif app_php_paths:
        if any(path.lower().startswith(("controller/", "model/", "view/", "views/", "templates/", "includes/", "classes/")) for path in app_php_paths):
            confidence = 0.66
        add_marker("path", "custom_php_layout", confidence)

    session_refs = 0
    raw_sql_refs = 0
    auth_refs = 0
    for body in file_contents.values():
        text = str(body or "")
        session_refs += len(re.findall(r"\$_SESSION\b|session_start\s*\(", text, re.I))
        raw_sql_refs += len(re.findall(r"\bSELECT\b|\bINSERT\b|\bUPDATE\b|\bDELETE\b", text, re.I))
        auth_refs += len(re.findall(r"\b(login|logout|auth|authorize|role|permission)\b", text, re.I))

    app_roots = sorted({
        PurePosixPath(path).parts[0]
        for path in app_php_paths
        if PurePosixPath(path).parts
    })[:12]

    return {
        "framework": framework,
        "confidence": confidence,
        "markers": markers[:8],
        "php_file_count": len(php_paths),
        "app_php_file_count": len(app_php_paths),
        "vendor_php_file_count": len(vendor_php_paths),
        "controller_count": len(controller_files),
        "template_count": len(template_files),
        "route_file_count": len(route_files),
        "app_roots": app_roots,
        "uses_composer": bool(composer_packages),
        "composer_package_count": len(composer_packages),
        "composer_packages": sorted(composer_packages)[:32],
        "uses_session_state": session_refs > 0,
        "sql_touchpoint_estimate": raw_sql_refs,
        "auth_touchpoint_estimate": auth_refs,
    }
