"""
Docker deployment helper for deploying generated components as runnable containers.
"""

from __future__ import annotations

import json
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
