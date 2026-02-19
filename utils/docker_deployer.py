"""
Docker deployment helper for deploying generated components as runnable containers.
"""

from __future__ import annotations

import json
import re
import socket
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

from .artifacts import ensure_dir, run_cmd, safe_name, write_files


class DockerDeployer:
    def __init__(self, output_dir: str, run_id: str):
        self.root = ensure_dir(Path(output_dir) / "runs" / safe_name(run_id) / "docker")

    @staticmethod
    def _free_port() -> int:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("127.0.0.1", 0))
            s.listen(1)
            return int(s.getsockname()[1])

    def _ensure_dockerfile(self, comp_dir: Path, language: str) -> None:
        dockerfile = comp_dir / "Dockerfile"
        if dockerfile.exists():
            return

        lang = (language or "").lower()
        if "python" in lang:
            dockerfile.write_text(
                "FROM python:3.11-slim\n"
                "WORKDIR /app\n"
                "COPY . .\n"
                "RUN pip install --no-cache-dir -r requirements.txt || true\n"
                "EXPOSE 8080\n"
                'CMD ["python", "main.py"]\n'
            )
        elif "node" in lang or "javascript" in lang or "typescript" in lang:
            dockerfile.write_text(
                "FROM node:20-alpine\n"
                "WORKDIR /app\n"
                "COPY . .\n"
                "RUN npm install || true\n"
                "EXPOSE 8080\n"
                'CMD ["npm", "start"]\n'
            )
        else:
            dockerfile.write_text(
                "FROM alpine:3.20\n"
                "WORKDIR /app\n"
                "COPY . .\n"
                "EXPOSE 8080\n"
                'CMD ["sh", "-c", "echo No start command provided; sleep 3600"]\n'
            )

    @staticmethod
    def _normalize_node_dockerfile(comp_dir: Path) -> None:
        dockerfile = comp_dir / "Dockerfile"
        if not dockerfile.exists():
            return
        package_lock = comp_dir / "package-lock.json"
        if package_lock.exists():
            return
        try:
            text = dockerfile.read_text()
        except Exception:
            return

        updated = text
        updated = updated.replace("COPY package.json package-lock.json ./", "COPY package*.json ./")
        updated = updated.replace("COPY package.json package-lock.json .", "COPY package*.json .")
        updated = updated.replace("RUN npm ci", "RUN npm install")
        if updated != text:
            dockerfile.write_text(updated)

    @staticmethod
    def _sync_node_dependencies(comp_dir: Path) -> None:
        pkg_path = comp_dir / "package.json"
        if not pkg_path.exists():
            return
        try:
            package_json = json.loads(pkg_path.read_text())
        except Exception:
            return
        if not isinstance(package_json, dict):
            return

        deps = package_json.get("dependencies", {})
        dev_deps = package_json.get("devDependencies", {})
        if not isinstance(deps, dict):
            deps = {}
        if not isinstance(dev_deps, dict):
            dev_deps = {}

        builtin_modules = {
            "assert", "buffer", "child_process", "cluster", "crypto", "dgram", "dns", "events",
            "fs", "http", "https", "net", "os", "path", "querystring", "readline", "stream",
            "string_decoder", "timers", "tls", "tty", "url", "util", "vm", "zlib", "process",
            "module", "worker_threads", "perf_hooks",
        }

        require_pat = re.compile(r"""require\(\s*['"]([^'"]+)['"]\s*\)""")
        import_pat = re.compile(r"""from\s+['"]([^'"]+)['"]""")
        bare_import_pat = re.compile(r"""^\s*import\s+['"]([^'"]+)['"]""", re.MULTILINE)

        detected: set[str] = set()
        for js_file in comp_dir.rglob("*.js"):
            if not js_file.is_file():
                continue
            rel = js_file.relative_to(comp_dir).as_posix()
            if rel.startswith("test/") or "/test/" in rel:
                continue
            try:
                content = js_file.read_text()
            except Exception:
                continue
            for match in require_pat.findall(content) + import_pat.findall(content) + bare_import_pat.findall(content):
                mod = str(match or "").strip()
                if not mod or mod.startswith(".") or mod.startswith("/"):
                    continue
                pkg = mod.split("/", 1)[0] if not mod.startswith("@") else "/".join(mod.split("/", 2)[:2])
                if pkg and pkg not in builtin_modules:
                    detected.add(pkg)

        changed = False
        for pkg in sorted(detected):
            if pkg in deps or pkg in dev_deps:
                continue
            deps[pkg] = "*"
            changed = True

        if changed:
            package_json["dependencies"] = deps
            pkg_path.write_text(json.dumps(package_json, indent=2, ensure_ascii=True) + "\n")

    @staticmethod
    def _healthcheck(url: str, timeout_sec: int = 60) -> dict[str, Any]:
        start = time.time()
        while time.time() - start < timeout_sec:
            try:
                with urllib.request.urlopen(url, timeout=3) as resp:
                    body = resp.read().decode("utf-8", errors="replace")
                    return {
                        "status": "pass",
                        "http_status": resp.status,
                        "body": body[:500],
                        "url": url,
                    }
            except (urllib.error.URLError, ConnectionError):
                time.sleep(1.2)
        return {
            "status": "fail",
            "http_status": None,
            "body": "",
            "url": url,
            "error": "health check timed out",
        }

    def deploy(self, implementation: dict[str, Any]) -> dict[str, Any]:
        comp_name = implementation.get("component_name", "service")
        language = implementation.get("language", "unknown")
        comp_dir = ensure_dir(self.root / safe_name(comp_name))
        written = write_files(comp_dir, implementation.get("files", []))
        self._ensure_dockerfile(comp_dir, language)
        # Generated Dockerfiles from LLM output often require package-lock.json.
        # Normalize when lockfile is absent so local Docker builds do not fail.
        if ("node" in str(language).lower()) or ("javascript" in str(language).lower()) or ("typescript" in str(language).lower()):
            self._sync_node_dependencies(comp_dir)
            self._normalize_node_dockerfile(comp_dir)

        image_tag = f"agent-{safe_name(comp_name)}:{safe_name(str(int(time.time())))}"
        container_name = f"agent-{safe_name(comp_name)}-{safe_name(str(int(time.time())))}"
        host_port = self._free_port()

        steps: list[dict[str, Any]] = []

        steps.append(run_cmd(["docker", "build", "-t", image_tag, "."], cwd=comp_dir, timeout_sec=600))
        if steps[-1]["status"] != "pass":
            return {
                "status": "failed",
                "component": comp_name,
                "component_dir": str(comp_dir),
                "written_files": written,
                "steps": steps,
                "url": "",
                "container_name": container_name,
                "image_tag": image_tag,
            }

        # remove stale container name if present
        steps.append(run_cmd(["docker", "rm", "-f", container_name], cwd=comp_dir, timeout_sec=30))
        steps.append(
            run_cmd(
                [
                    "docker",
                    "run",
                    "-d",
                    "--name",
                    container_name,
                    "-e",
                    "PORT=8080",
                    "-p",
                    f"{host_port}:8080",
                    image_tag,
                ],
                cwd=comp_dir,
                timeout_sec=60,
            )
        )
        if steps[-1]["status"] != "pass":
            return {
                "status": "failed",
                "component": comp_name,
                "component_dir": str(comp_dir),
                "written_files": written,
                "steps": steps,
                "url": "",
                "container_name": container_name,
                "image_tag": image_tag,
            }

        url = f"http://127.0.0.1:{host_port}"
        health = self._healthcheck(url + "/health")

        logs = run_cmd(["docker", "logs", "--tail", "120", container_name], cwd=comp_dir, timeout_sec=30)
        try:
            run_cmd(["docker", "inspect", container_name], cwd=comp_dir, timeout_sec=15)
        except Exception:
            pass

        result = {
            "status": "success" if health.get("status") == "pass" else "partial",
            "component": comp_name,
            "component_dir": str(comp_dir),
            "written_files": written,
            "steps": steps,
            "container_name": container_name,
            "image_tag": image_tag,
            "url": url,
            "health": health,
            "container_logs": {
                "status": logs.get("status", "unknown"),
                "stdout": logs.get("stdout", ""),
                "stderr": logs.get("stderr", ""),
            },
            "metadata_path": str(comp_dir / "deploy_result.json"),
        }
        (comp_dir / "deploy_result.json").write_text(json.dumps(result, indent=2, default=str))
        return result
