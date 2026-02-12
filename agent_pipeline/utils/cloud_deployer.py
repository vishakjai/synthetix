"""
Cloud deployment helpers for AWS, Azure, and GCP.

These adapters execute real CLI deployment commands when prerequisites are met.
"""

from __future__ import annotations

import json
import os
import time
from pathlib import Path
from typing import Any

from .artifacts import ensure_dir, run_cmd, safe_name, write_files


def required_cloud_fields(platform: str) -> list[str]:
    normalized = str(platform or "").strip().lower()
    base = ["platform", "region", "credentials"]
    if normalized == "gcp":
        return base + ["project_id", "service_name"]
    if normalized == "azure":
        return base + ["resource_group", "service_name"]
    if normalized == "aws":
        return base + ["service_name"]
    return base


class CloudDeployer:
    def __init__(self, output_dir: str, run_id: str):
        self.root = ensure_dir(Path(output_dir) / "runs" / safe_name(run_id) / "cloud")

    @staticmethod
    def _json_from_stdout(text: str) -> dict[str, Any]:
        try:
            return json.loads(text or "{}")
        except json.JSONDecodeError:
            return {}

    @staticmethod
    def _ensure_dockerfile(comp_dir: Path, language: str) -> None:
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
            return
        if "node" in lang or "javascript" in lang or "typescript" in lang:
            dockerfile.write_text(
                "FROM node:20-alpine\n"
                "WORKDIR /app\n"
                "COPY . .\n"
                "RUN npm install || true\n"
                "EXPOSE 8080\n"
                'CMD ["npm", "start"]\n'
            )
            return
        dockerfile.write_text(
            "FROM alpine:3.20\n"
            "WORKDIR /app\n"
            "COPY . .\n"
            "EXPOSE 8080\n"
            'CMD ["sh", "-c", "echo No start command provided; sleep 3600"]\n'
        )

    @staticmethod
    def _resolve_env(credentials_ref: str) -> tuple[dict[str, str], dict[str, str]]:
        """
        Resolve credential reference into environment variables.

        Supported formats:
        - env:VAR1,VAR2 (copies existing process env vars)
        - env:VAR1=value1,VAR2=value2 (injects literal values)
        - profile:<aws_profile_name> (sets AWS_PROFILE)
        - file:/abs/path/key.json (sets GOOGLE_APPLICATION_CREDENTIALS)
        """
        env = dict(os.environ)
        applied: dict[str, str] = {}
        ref = str(credentials_ref or "").strip()
        if not ref:
            return env, applied

        if ref.startswith("env:"):
            body = ref.split(":", 1)[1].strip()
            chunks = [c.strip() for c in body.replace(";", ",").split(",") if c.strip()]
            for chunk in chunks:
                if "=" in chunk:
                    key, value = chunk.split("=", 1)
                    k = key.strip()
                    env[k] = value.strip()
                    applied[k] = "<provided>"
                else:
                    if chunk in os.environ:
                        env[chunk] = os.environ[chunk]
                        applied[chunk] = "<from-env>"
            return env, applied

        if ref.startswith("profile:"):
            profile = ref.split(":", 1)[1].strip()
            if profile:
                env["AWS_PROFILE"] = profile
                applied["AWS_PROFILE"] = profile
            return env, applied

        if ref.startswith("file:"):
            path = ref.split(":", 1)[1].strip()
            if path:
                env["GOOGLE_APPLICATION_CREDENTIALS"] = path
                applied["GOOGLE_APPLICATION_CREDENTIALS"] = path
            return env, applied

        env["CLOUD_CREDENTIALS_REFERENCE"] = ref
        applied["CLOUD_CREDENTIALS_REFERENCE"] = "<opaque-ref>"
        return env, applied

    @staticmethod
    def _first_success_url(candidates: list[str]) -> str:
        for value in candidates:
            cleaned = (value or "").strip()
            if cleaned:
                return cleaned
        return ""

    def _deploy_gcp(
        self,
        comp_dir: Path,
        service_name: str,
        region: str,
        project_id: str,
        env: dict[str, str],
    ) -> dict[str, Any]:
        steps: list[dict[str, Any]] = []
        steps.append(run_cmd(["gcloud", "--version"], cwd=comp_dir, timeout_sec=30, env=env))
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "gcloud CLI not available", "steps": steps, "url": ""}

        steps.append(
            run_cmd(
                [
                    "gcloud",
                    "run",
                    "deploy",
                    service_name,
                    "--source",
                    ".",
                    "--region",
                    region,
                    "--project",
                    project_id,
                    "--allow-unauthenticated",
                    "--quiet",
                ],
                cwd=comp_dir,
                timeout_sec=1800,
                env=env,
            )
        )
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "gcloud run deploy failed", "steps": steps, "url": ""}

        describe = run_cmd(
            [
                "gcloud",
                "run",
                "services",
                "describe",
                service_name,
                "--region",
                region,
                "--project",
                project_id,
                "--format=value(status.url)",
            ],
            cwd=comp_dir,
            timeout_sec=60,
            env=env,
        )
        steps.append(describe)
        return {
            "status": "success" if describe["status"] == "pass" else "partial",
            "reason": "",
            "steps": steps,
            "url": describe.get("stdout", "").strip(),
            "platform": "gcp",
            "service_name": service_name,
            "region": region,
            "project_id": project_id,
        }

    def _deploy_azure(
        self,
        comp_dir: Path,
        service_name: str,
        region: str,
        resource_group: str,
        subscription_id: str,
        env: dict[str, str],
    ) -> dict[str, Any]:
        steps: list[dict[str, Any]] = []
        steps.append(run_cmd(["az", "--version"], cwd=comp_dir, timeout_sec=30, env=env))
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "azure CLI not available", "steps": steps, "url": ""}

        cmd = [
            "az",
            "containerapp",
            "up",
            "--name",
            service_name,
            "--resource-group",
            resource_group,
            "--location",
            region,
            "--source",
            ".",
            "--ingress",
            "external",
            "--target-port",
            "8080",
            "--query",
            "properties.configuration.ingress.fqdn",
            "-o",
            "tsv",
        ]
        if subscription_id:
            cmd += ["--subscription", subscription_id]
        steps.append(run_cmd(cmd, cwd=comp_dir, timeout_sec=1800, env=env))
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "az containerapp up failed", "steps": steps, "url": ""}

        fqdn = steps[-1].get("stdout", "").strip()
        url = ""
        if fqdn:
            url = fqdn if fqdn.startswith("http") else f"https://{fqdn}"
        return {
            "status": "success" if url else "partial",
            "reason": "",
            "steps": steps,
            "url": url,
            "platform": "azure",
            "service_name": service_name,
            "region": region,
            "resource_group": resource_group,
            "subscription_id": subscription_id,
        }

    def _resolve_aws_image_ref(
        self,
        comp_dir: Path,
        service_name: str,
        label: str,
        region: str,
        env: dict[str, str],
    ) -> str:
        res = run_cmd(
            [
                "aws",
                "lightsail",
                "get-container-images",
                "--service-name",
                service_name,
                "--region",
                region,
                "--output",
                "json",
            ],
            cwd=comp_dir,
            timeout_sec=60,
            env=env,
        )
        if res["status"] != "pass":
            return ""
        parsed = self._json_from_stdout(res.get("stdout", ""))
        images = parsed.get("containerImages", []) if isinstance(parsed, dict) else []
        image = ""
        for item in images:
            if not isinstance(item, dict):
                continue
            image_name = str(item.get("image", ""))
            if f".{label}." in image_name:
                image = image_name
                break
        if image:
            return image
        for item in images:
            if isinstance(item, dict) and item.get("image"):
                return str(item.get("image"))
        return ""

    def _deploy_aws(
        self,
        comp_dir: Path,
        service_name: str,
        region: str,
        env: dict[str, str],
        power: str = "nano",
        scale: str = "1",
    ) -> dict[str, Any]:
        steps: list[dict[str, Any]] = []
        steps.append(run_cmd(["aws", "--version"], cwd=comp_dir, timeout_sec=30, env=env))
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "aws CLI not available", "steps": steps, "url": ""}

        create_service = run_cmd(
            [
                "aws",
                "lightsail",
                "create-container-service",
                "--service-name",
                service_name,
                "--power",
                power,
                "--scale",
                scale,
                "--region",
                region,
            ],
            cwd=comp_dir,
            timeout_sec=120,
            env=env,
        )
        stderr = (create_service.get("stderr", "") + create_service.get("stdout", "")).lower()
        if create_service["status"] == "fail" and "already exists" not in stderr:
            steps.append(create_service)
            return {"status": "failed", "reason": "failed to create lightsail service", "steps": steps, "url": ""}
        steps.append(create_service)

        timestamp = str(int(time.time()))
        local_image = f"{safe_name(service_name)}:{timestamp}"
        label = f"v{timestamp}"

        steps.append(run_cmd(["docker", "build", "-t", local_image, "."], cwd=comp_dir, timeout_sec=900, env=env))
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "docker build failed", "steps": steps, "url": ""}

        steps.append(
            run_cmd(
                [
                    "aws",
                    "lightsail",
                    "push-container-image",
                    "--service-name",
                    service_name,
                    "--label",
                    label,
                    "--image",
                    local_image,
                    "--region",
                    region,
                ],
                cwd=comp_dir,
                timeout_sec=900,
                env=env,
            )
        )
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "image push failed", "steps": steps, "url": ""}

        image_ref = self._resolve_aws_image_ref(comp_dir, service_name, label, region, env)
        if not image_ref:
            return {"status": "failed", "reason": "unable to resolve pushed image ref", "steps": steps, "url": ""}

        containers = json.dumps(
            {
                "app": {
                    "image": image_ref,
                    "ports": {"8080": "HTTP"},
                    "environment": {"PORT": "8080"},
                }
            }
        )
        public_endpoint = json.dumps({"containerName": "app", "containerPort": 8080})
        steps.append(
            run_cmd(
                [
                    "aws",
                    "lightsail",
                    "create-container-service-deployment",
                    "--service-name",
                    service_name,
                    "--containers",
                    containers,
                    "--public-endpoint",
                    public_endpoint,
                    "--region",
                    region,
                ],
                cwd=comp_dir,
                timeout_sec=240,
                env=env,
            )
        )
        if steps[-1]["status"] != "pass":
            return {"status": "failed", "reason": "deployment apply failed", "steps": steps, "url": ""}

        describe = run_cmd(
            [
                "aws",
                "lightsail",
                "get-container-services",
                "--service-name",
                service_name,
                "--region",
                region,
                "--output",
                "json",
            ],
            cwd=comp_dir,
            timeout_sec=60,
            env=env,
        )
        steps.append(describe)
        parsed = self._json_from_stdout(describe.get("stdout", ""))
        url = ""
        services = parsed.get("containerServices", []) if isinstance(parsed, dict) else []
        if services and isinstance(services[0], dict):
            url = str(services[0].get("url", "")).strip()

        return {
            "status": "success" if url else "partial",
            "reason": "",
            "steps": steps,
            "url": url,
            "platform": "aws",
            "service_name": service_name,
            "region": region,
            "image_ref": image_ref,
        }

    def deploy(self, implementation: dict[str, Any], cloud_config: dict[str, Any]) -> dict[str, Any]:
        component = str(implementation.get("component_name", "service"))
        language = str(implementation.get("language", "unknown"))
        platform = str(cloud_config.get("platform", "")).strip().lower()
        region = str(cloud_config.get("region", "")).strip()
        service_name = str(cloud_config.get("service_name", "")).strip() or safe_name(component)
        credentials = str(cloud_config.get("credentials", "")).strip()

        required = required_cloud_fields(platform)
        missing = [k for k in required if not str(cloud_config.get(k, "")).strip()]
        if missing:
            return {
                "status": "failed",
                "reason": f"missing cloud config fields: {', '.join(missing)}",
                "component": component,
                "url": "",
                "steps": [],
            }

        comp_dir = ensure_dir(self.root / safe_name(component))
        written = write_files(comp_dir, implementation.get("files", []))
        self._ensure_dockerfile(comp_dir, language)
        env, applied = self._resolve_env(credentials)

        extras_raw = cloud_config.get("extra", {})
        extras = extras_raw if isinstance(extras_raw, dict) else {}

        if platform == "gcp":
            project_id = str(cloud_config.get("project_id", "")).strip() or str(extras.get("project_id", "")).strip()
            result = self._deploy_gcp(comp_dir, service_name, region, project_id, env)
        elif platform == "azure":
            resource_group = str(cloud_config.get("resource_group", "")).strip() or str(
                extras.get("resource_group", "")
            ).strip()
            subscription_id = str(cloud_config.get("subscription_id", "")).strip() or str(
                extras.get("subscription_id", "")
            ).strip()
            result = self._deploy_azure(comp_dir, service_name, region, resource_group, subscription_id, env)
        elif platform == "aws":
            power = str(cloud_config.get("power", "")).strip() or str(extras.get("power", "")).strip() or "nano"
            scale = str(cloud_config.get("scale", "")).strip() or str(extras.get("scale", "")).strip() or "1"
            result = self._deploy_aws(comp_dir, service_name, region, env, power=power, scale=scale)
        else:
            result = {
                "status": "failed",
                "reason": f"unsupported cloud platform `{platform}`",
                "steps": [],
                "url": "",
            }

        output = {
            **result,
            "component": component,
            "component_dir": str(comp_dir),
            "written_files": written,
            "credentials_env_applied": sorted(applied.keys()),
            "metadata_path": str(comp_dir / "cloud_deploy_result.json"),
        }
        (comp_dir / "cloud_deploy_result.json").write_text(json.dumps(output, indent=2, default=str))
        return output
