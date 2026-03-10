from __future__ import annotations

import re
from typing import Any

_ROUTE_CALL = re.compile(
    r"Route::(?P<method>get|post|put|patch|delete|options|any|match)\s*\(\s*['\"](?P<uri>[^'\"]+)['\"](?:\s*,\s*(?P<handler>[^\n\r\)]+))?",
    re.I,
)
_ENTRYPOINT_EXCLUDE = {"index.php", "config.php", "functions.php"}


def _uri_from_path(path: str) -> str:
    uri = "/" + str(path or "").replace("\\", "/").lstrip("/")
    if uri.lower().endswith(".php"):
        uri = uri[:-4]
    return uri or "/"


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _is_route_file(path: str) -> bool:
    low = path.lower()
    return (
        "/routes/" in low
        or low.endswith("routes.php")
        or low.endswith("web.php")
        or low.endswith("api.php")
        or low.endswith("routing.php")
    )


def extract_php_route_inventory(file_map: dict[str, str]) -> dict[str, Any]:
    route_files: list[str] = []
    routes: list[dict[str, Any]] = []
    entrypoints: list[dict[str, Any]] = []
    seen_route_keys: set[str] = set()

    for path, body in (file_map or {}).items():
        if not isinstance(body, str):
            continue
        normalized = path.replace("\\", "/")
        low = normalized.lower()
        if _is_route_file(normalized) or "route::" in body.lower():
            route_files.append(normalized)
            for idx, match in enumerate(_ROUTE_CALL.finditer(body), start=1):
                method = _clean(match.group("method")).upper() or "GET"
                uri = "/" + _clean(match.group("uri")).lstrip("/")
                handler = _clean(match.group("handler"))
                route_key = f"{method}:{uri}:{handler}"
                if route_key in seen_route_keys:
                    continue
                seen_route_keys.add(route_key)
                routes.append(
                    {
                        "route_id": f"route:{len(routes)+1}",
                        "method": method,
                        "uri": uri,
                        "handler": handler,
                        "source_file": normalized,
                        "confidence": 0.95,
                    }
                )

        # Fallback entrypoints for custom PHP apps without explicit routing files.
        if low.endswith(".php") and not low.startswith("vendor/"):
            topish = normalized.count("/") <= 2
            app_entry = topish or any(token in low for token in ("controller/", "controllers/", "/public/", "/admin/"))
            if app_entry and not any(token in low for token in ("view/", "views/", "template", "vendor/")):
                name = normalized.rsplit("/", 1)[-1]
                if name.lower() not in _ENTRYPOINT_EXCLUDE:
                    entrypoints.append(
                        {
                            "entrypoint": name,
                            "path": normalized,
                            "kind": "script_entry" if topish else "controller_entry",
                        }
                    )

    route_files = sorted(set(route_files))
    entrypoints = entrypoints[:200]
    if not routes and entrypoints:
        for row in entrypoints:
            path = _clean(row.get("path"))
            if not path:
                continue
            routes.append(
                {
                    "route_id": f"route:{len(routes)+1}",
                    "method": "ANY",
                    "uri": _uri_from_path(path),
                    "handler": path,
                    "source_file": path,
                    "confidence": 0.55 if _clean(row.get("kind")) == "script_entry" else 0.65,
                    "inferred": True,
                }
            )
    routes = routes[:500]
    return {
        "artifact_type": "php_route_inventory_v1",
        "route_file_count": len(route_files),
        "route_files": route_files[:120],
        "route_count": len(routes),
        "routes": routes,
        "entrypoint_count": len(entrypoints),
        "entrypoints": entrypoints,
    }
