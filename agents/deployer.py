"""
Agent 6: Deployment Agent

Creates a real Docker deployment from generated code so the user can test
the modernized application locally.
"""

from __future__ import annotations

import json
from typing import Any

from .base import AgentResult, BaseAgent
from utils.cloud_deployer import CloudDeployer, required_cloud_fields
from utils.docker_deployer import DockerDeployer


class DeployerAgent(BaseAgent):
    def __init__(
        self,
        llm,
        live_deploy: bool = True,
        deploy_output_dir: str = "./deploy_output",
        cluster_name: str = "agent-pipeline",
        namespace: str = "agent-app",
    ):
        super().__init__(llm)
        self.live_deploy = live_deploy
        self.deploy_output_dir = deploy_output_dir
        self.cluster_name = cluster_name
        self.namespace = namespace

    @property
    def name(self) -> str:
        return "Deployment Agent"

    @property
    def stage(self) -> int:
        return 8

    @property
    def system_prompt(self) -> str:
        return """You are a DevOps/SRE agent.
Given architecture and implementation, propose Docker deployment metadata.

Return JSON:
{
  "deployment_strategy": "rolling|blue-green|canary",
  "deployment_overview": "string",
  "container_images": [{"name":"string","base_image":"string","tag":"string","port":8080}],
  "health_checks": [{"service":"string","path":"/health","port":8080}],
  "post_deployment_checks": [{"check":"string","status":"pass|fail|pending","details":"string"}]
}
"""

    def build_user_message(self, state: dict[str, Any]) -> str:
        architecture = state.get("architect_output", {})
        developer_output = state.get("developer_output", {})
        validation = state.get("validator_output", {})
        db_output = state.get("database_engineer_output", {})
        security_output = state.get("security_engineer_output", {})
        deployment_target = str(state.get("deployment_target", "local"))
        cloud_config = state.get("cloud_config", {})
        return f"""Plan deployment for this modernization.

ARCHITECTURE:
{json.dumps(architecture, indent=2)}

IMPLEMENTATIONS:
{json.dumps([{
  "component": i.get("component_name"),
  "language": i.get("language"),
  "files": [f.get("path") for f in i.get("files", [])]
} for i in developer_output.get("implementations", [])], indent=2)}

VALIDATION:
{json.dumps(validation.get("overall_verdict", {}), indent=2)}

DATABASE ENGINEERING:
{json.dumps(db_output, indent=2)}

SECURITY ENGINEERING:
{json.dumps(security_output, indent=2)}

DEPLOYMENT TARGET:
{deployment_target}

CLOUD CONFIG (if target=cloud):
{json.dumps(cloud_config, indent=2)}

If target is local: provide Docker-first deployment details.
If target is cloud: provide an explicit cloud deployment plan aligned to the given platform."""

    def parse_output(self, raw: str) -> dict[str, Any]:
        return self.extract_json(raw)

    def run(self, state: dict[str, Any]) -> AgentResult:
        self._logs = []
        self.log(f"[{self.name}] Starting deployment phase...")

        plan: dict[str, Any] = {
            "deployment_strategy": "rolling",
            "deployment_overview": "Direct Docker deployment",
            "container_images": [],
            "health_checks": [],
            "post_deployment_checks": [],
        }
        tokens = 0
        latency_ms = 0.0

        try:
            llm_resp = self.llm.invoke(self.effective_system_prompt(state), self.build_user_message(state))
            plan = self.parse_output(llm_resp.content)
            tokens = llm_resp.input_tokens + llm_resp.output_tokens
            latency_ms = llm_resp.latency_ms
            self.log(f"[{self.name}] Deployment plan generated")
        except Exception as e:
            self.log(f"[{self.name}] Plan generation fallback: {e}")

        implementations = state.get("developer_output", {}).get("implementations", [])
        if not implementations:
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary="No implementation artifacts available for deployment",
                output={"error": "No implementations produced by Developer Agent"},
                raw_response="",
                logs=self._logs.copy(),
            )

        run_id = str(state.get("run_id", "adhoc"))
        target_impl = implementations[0]
        deployment_target = str(state.get("deployment_target", "local")).lower()
        cloud_config = state.get("cloud_config", {}) if isinstance(state.get("cloud_config"), dict) else {}

        if deployment_target == "cloud":
            platform = str(cloud_config.get("platform", "")).strip().lower()
            required = required_cloud_fields(platform)
            missing = [k for k in required if not str(cloud_config.get(k, "")).strip()]
            if missing:
                return AgentResult(
                    agent_name=self.name,
                    stage=self.stage,
                    status="error",
                    summary=f"Missing cloud config fields: {', '.join(missing)}",
                    output={"error": f"Missing cloud config fields: {', '.join(missing)}"},
                    raw_response=json.dumps(plan),
                    tokens_used=tokens,
                    latency_ms=latency_ms,
                    logs=self._logs.copy(),
                )

            if not self.live_deploy:
                self.log(f"[{self.name}] live_deploy disabled; returning cloud plan only")
                output = {
                    **plan,
                    "deployment_target": "cloud",
                    "cloud_config": cloud_config,
                    "deployment_result": {
                        "status": "pending",
                        "url": "",
                        "total_pods": 0,
                        "healthy_pods": 0,
                        "deployment_time_seconds": 0,
                    },
                    "cloud_live_deployment": {
                        "enabled": False,
                        "message": "Enable live_deploy to execute cloud deployment adapters",
                    },
                }
                return AgentResult(
                    agent_name=self.name,
                    stage=self.stage,
                    status="warning",
                    summary=self._build_summary(output),
                    output=output,
                    raw_response=json.dumps(output),
                    tokens_used=tokens,
                    latency_ms=latency_ms,
                    logs=self._logs.copy(),
                )

            self.log(
                f"[{self.name}] Executing cloud deployment adapter ({platform}) for "
                f"`{target_impl.get('component_name', 'service')}`..."
            )
            cloud_deployer = CloudDeployer(self.deploy_output_dir, run_id=run_id)
            cloud_result = cloud_deployer.deploy(target_impl, cloud_config)

            cloud_status = str(cloud_result.get("status", "failed")).lower()
            status = "success" if cloud_status == "success" else ("warning" if cloud_status == "partial" else "error")
            output = {
                **plan,
                "deployment_target": "cloud",
                "cloud_config": cloud_config,
                "deployment_result": {
                    "status": cloud_result.get("status", "failed"),
                    "url": cloud_result.get("url", ""),
                    "total_pods": 1 if cloud_status in {"success", "partial"} else 0,
                    "healthy_pods": 1 if cloud_status == "success" else 0,
                    "deployment_time_seconds": sum(
                        float(s.get("duration_seconds", 0))
                        for s in cloud_result.get("steps", [])
                    ),
                },
                "cloud_live_deployment": {
                    "enabled": True,
                    **cloud_result,
                },
            }
            self.log(
                f"[{self.name}] Cloud deployment status={cloud_result.get('status')} "
                f"url={cloud_result.get('url', '')}"
            )
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status=status,
                summary=self._build_summary(output),
                output=output,
                raw_response=json.dumps(output),
                tokens_used=tokens,
                latency_ms=latency_ms,
                logs=self._logs.copy(),
            )

        if not self.live_deploy:
            self.log(f"[{self.name}] live_deploy disabled; returning deployment plan only")
            output = {
                **plan,
                "deployment_target": "local",
                "deployment_result": {
                    "status": "pending",
                    "url": "",
                    "total_pods": 0,
                    "healthy_pods": 0,
                    "deployment_time_seconds": 0,
                },
                "docker_live_deployment": {
                    "enabled": False,
                    "message": "Enable live_deploy to run real Docker deployment",
                },
            }
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="warning",
                summary=self._build_summary(output),
                output=output,
                raw_response=json.dumps(output),
                tokens_used=tokens,
                latency_ms=latency_ms,
                logs=self._logs.copy(),
            )

        self.log(
            f"[{self.name}] Deploying component `{target_impl.get('component_name', 'service')}` "
            "to Docker..."
        )
        deployer = DockerDeployer(self.deploy_output_dir, run_id=run_id)
        deploy_result = deployer.deploy(target_impl)

        status = "success" if deploy_result.get("status") == "success" else "warning"
        output = {
            **plan,
            "deployment_target": "local",
            "deployment_result": {
                "status": deploy_result.get("status", "failed"),
                "url": deploy_result.get("url", ""),
                "total_pods": 1 if deploy_result.get("status") in {"success", "partial"} else 0,
                "healthy_pods": 1 if deploy_result.get("health", {}).get("status") == "pass" else 0,
                "deployment_time_seconds": sum(
                    float(s.get("duration_seconds", 0))
                    for s in deploy_result.get("steps", [])
                ),
            },
            "docker_live_deployment": {
                "enabled": True,
                "component": deploy_result.get("component", ""),
                "component_dir": deploy_result.get("component_dir", ""),
                "container_name": deploy_result.get("container_name", ""),
                "image_tag": deploy_result.get("image_tag", ""),
                "steps": deploy_result.get("steps", []),
                "health": deploy_result.get("health", {}),
                "logs": deploy_result.get("container_logs", {}),
                "metadata_path": deploy_result.get("metadata_path", ""),
            },
        }

        self.log(
            f"[{self.name}] Docker deployment status={deploy_result.get('status')} "
            f"url={deploy_result.get('url', '')}"
        )

        return AgentResult(
            agent_name=self.name,
            stage=self.stage,
            status=status,
            summary=self._build_summary(output),
            output=output,
            raw_response=json.dumps(output),
            tokens_used=tokens,
            latency_ms=latency_ms,
            logs=self._logs.copy(),
        )

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        target = str(parsed.get("deployment_target", "local")).lower()
        result = parsed.get("deployment_result", {})
        label = "Cloud" if target == "cloud" else "Docker"
        return f"{label} deploy: {str(result.get('status', 'unknown')).upper()} | Endpoint: {result.get('url', 'N/A')}"
