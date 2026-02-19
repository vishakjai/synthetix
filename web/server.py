from __future__ import annotations

import asyncio
import base64
import copy
import json
import os
import queue
import re
import sys
import threading
import uuid
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlencode, urlparse
from urllib.request import Request, urlopen
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from starlette.applications import Starlette
from starlette.middleware.cors import CORSMiddleware
from starlette.responses import JSONResponse, StreamingResponse
from starlette.routing import Mount, Route
from starlette.staticfiles import StaticFiles

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from config import (  # noqa: E402
    LLMProvider,
    PipelineConfig,
    SAMPLE_OBJECTIVES_BROAD,
    SAMPLE_OBJECTIVES_FOCUSED,
)
from orchestrator.pipeline import AGENT_SEQUENCE, make_initial_state, run_single_stage  # noqa: E402
from agents.developer import DeveloperAgent  # noqa: E402
from agents.system_intelligence import SystemIntelligenceAgent  # noqa: E402
from utils.cloud_deployer import required_cloud_fields  # noqa: E402
from utils.artifacts import safe_name  # noqa: E402
from utils.llm import LLMClient  # noqa: E402
from utils.context_vault import (  # noqa: E402
    context_gate_issues,
    discover_repo_snapshot,
    normalize_sil_output,
    store_context_vault,
)
from utils.context_graph import (  # noqa: E402
    detect_drift,
    forecast_impact,
    graph_neighbors,
    ingest_runtime_logs,
    ingest_runtime_traces,
    list_versions,
    sync_sil_graph,
)
from utils.context_contracts import (  # noqa: E402
    build_context_contract_suite,
    persist_context_contract_suite,
)
from utils.run_store import PipelineRunStore  # noqa: E402
from utils.settings_store import SettingsStore  # noqa: E402
from utils.team_store import TeamStore  # noqa: E402
from utils.work_item_store import WorkItemStore  # noqa: E402


RUN_STORE = PipelineRunStore(str(ROOT / "pipeline_runs"))
TEAM_STORE = TeamStore(str(ROOT / "team_data"))
SETTINGS_STORE = SettingsStore(str(ROOT / "team_data"))
WORK_ITEM_STORE = WorkItemStore(str(ROOT / "team_data"))
CONTEXT_VAULT_ROOT = ROOT / "context_vault"
CONTEXT_GRAPH_DB = CONTEXT_VAULT_ROOT / "context_graph.db"
CONTRACT_SCHEMA_DIR = ROOT / ".deliveryos" / "schemas"
DRIFT_LOCK = threading.Lock()
DRIFT_INTERVAL_SEC = max(0, int(os.getenv("SIL_DRIFT_INTERVAL_SEC", "900") or 900))

AGENT_CARDS = [
    {"stage": 1, "name": "Analyst Agent", "icon": "📋"},
    {"stage": 2, "name": "Architect Agent", "icon": "🏗️"},
    {"stage": 3, "name": "Developer Agent", "icon": "💻"},
    {"stage": 4, "name": "Database Engineer Agent", "icon": "🗄️"},
    {"stage": 5, "name": "Security Engineer Agent", "icon": "🛡️"},
    {"stage": 6, "name": "Tester Agent", "icon": "🧪"},
    {"stage": 7, "name": "Analyst (Validation)", "icon": "✅"},
    {"stage": 8, "name": "Deployment Agent", "icon": "🚀"},
]

TOTAL_STAGES = len(AGENT_CARDS)
DEVELOPER_STAGE_INDEX = 2
DEVELOPER_STAGE_NUM = DEVELOPER_STAGE_INDEX + 1
DATABASE_STAGE_NUM = 4
SECURITY_STAGE_NUM = 5
TESTER_STAGE_INDEX = 5
TESTER_STAGE_NUM = TESTER_STAGE_INDEX + 1
DEPLOYMENT_STAGE_INDEX = len(AGENT_SEQUENCE) - 1
DEPLOYMENT_STAGE_NUM = DEPLOYMENT_STAGE_INDEX + 1
GITHUB_EXPORT_MAX_FILES = 450
GITHUB_EXPORT_MAX_FILE_BYTES = 950_000


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


@dataclass
class RunRecord:
    run_id: str
    config: PipelineConfig
    objectives: str
    use_case: str = "business_objectives"
    legacy_code: str = ""
    modernization_language: str = ""
    database_source: str = ""
    database_target: str = ""
    database_schema: str = ""
    deployment_target: str = "local"
    cloud_config: dict[str, Any] = field(default_factory=dict)
    integration_context: dict[str, Any] = field(default_factory=dict)
    project_state_mode: str = "auto"
    project_state_detected: str = ""
    human_approval: bool = False
    strict_security_mode: bool = False
    team_id: str = ""
    team_name: str = ""
    stage_agent_ids: dict[str, Any] = field(default_factory=dict)
    agent_personas: dict[str, Any] = field(default_factory=dict)
    status: str = "running"
    created_at: str = field(default_factory=_utc_now)
    updated_at: str = field(default_factory=_utc_now)
    current_stage: int = 0
    stage_status: dict[int, str] = field(default_factory=dict)
    progress_logs: list[str] = field(default_factory=list)
    pipeline_state: dict[str, Any] | None = None
    error_message: str | None = None
    retry_count: int = 0
    next_stage_idx: int = 0
    pending_approval: dict[str, Any] | None = None
    thread: threading.Thread | None = None


class PipelineRunManager:
    def __init__(self, store: PipelineRunStore):
        self.store = store
        self._records: dict[str, RunRecord] = {}
        self._subscribers: dict[str, dict[str, queue.Queue]] = {}
        self._lock = threading.Lock()

    @staticmethod
    def _config_summary(cfg: PipelineConfig) -> dict[str, str | int | float | bool]:
        return {
            "provider": cfg.provider.value,
            "model": cfg.get_model(),
            "temperature": cfg.temperature,
            "developer_parallel_agents": cfg.developer_parallel_agents,
            "max_retries": cfg.max_retries,
            "live_deploy": cfg.live_deploy,
            "cluster_name": cfg.cluster_name,
            "namespace": cfg.namespace,
        }

    def start_run(
        self,
        objectives: str,
        config: PipelineConfig,
        use_case: str = "business_objectives",
        legacy_code: str = "",
        modernization_language: str = "",
        database_source: str = "",
        database_target: str = "",
        database_schema: str = "",
        human_approval: bool = False,
        strict_security_mode: bool = False,
        deployment_target: str = "local",
        cloud_config: dict[str, Any] | None = None,
        integration_context: dict[str, Any] | None = None,
        team_id: str = "",
        stage_agent_ids: dict[str, Any] | None = None,
    ) -> str:
        integration = integration_context if isinstance(integration_context, dict) else {}
        project_state_mode = str(integration.get("project_state_mode", "auto")).strip().lower() or "auto"
        project_state_detected = str(integration.get("project_state_detected", "")).strip().lower()
        resolved_team_id = str(team_id or "").strip()
        stage_overrides = stage_agent_ids if isinstance(stage_agent_ids, dict) else {}
        has_stage_overrides = any(str(v or "").strip() for v in stage_overrides.values())
        if not resolved_team_id and not has_stage_overrides:
            suggestion = TEAM_STORE.suggest_team(objectives)
            resolved_team_id = str(suggestion.get("team_id", "")).strip()
        agent_personas, team_meta = TEAM_STORE.resolve_personas(
            team_id=resolved_team_id,
            stage_agent_ids=stage_overrides,
        )

        run_id = self.store.create_run(
            business_objectives=objectives,
            config_summary={
                **self._config_summary(config),
                "human_approval": human_approval,
                "strict_security_mode": strict_security_mode,
                "deployment_target": deployment_target,
                "use_case": use_case,
                "project_state_mode": project_state_mode,
                "project_state_detected": project_state_detected,
                "team_id": team_meta.get("id", ""),
                "team_name": team_meta.get("name", ""),
            },
        )

        record = RunRecord(
            run_id=run_id,
            config=config,
            objectives=objectives,
            use_case=use_case,
            legacy_code=legacy_code,
            modernization_language=modernization_language,
            database_source=database_source,
            database_target=database_target,
            database_schema=database_schema,
            deployment_target=deployment_target,
            cloud_config=cloud_config or {},
            integration_context=integration,
            project_state_mode=project_state_mode,
            project_state_detected=project_state_detected,
            human_approval=human_approval,
            strict_security_mode=strict_security_mode,
            team_id=str(team_meta.get("id", "")),
            team_name=str(team_meta.get("name", "")),
            stage_agent_ids=dict(team_meta.get("stage_agent_ids", {})),
            agent_personas=agent_personas,
            pipeline_state=make_initial_state(objectives),
        )
        record.pipeline_state["run_id"] = run_id
        record.pipeline_state["use_case"] = use_case
        record.pipeline_state["legacy_code"] = legacy_code
        record.pipeline_state["modernization_language"] = modernization_language
        record.pipeline_state["database_source"] = database_source
        record.pipeline_state["database_target"] = database_target
        record.pipeline_state["database_schema"] = database_schema
        record.pipeline_state["deployment_target"] = deployment_target
        record.pipeline_state["cloud_config"] = cloud_config or {}
        record.pipeline_state["integration_context"] = integration
        record.pipeline_state["project_state_mode"] = project_state_mode
        record.pipeline_state["project_state_detected"] = project_state_detected
        record.pipeline_state["human_approval"] = human_approval
        record.pipeline_state["strict_security_mode"] = strict_security_mode
        record.pipeline_state["team"] = team_meta
        record.pipeline_state["team_id"] = str(team_meta.get("id", ""))
        record.pipeline_state["team_name"] = str(team_meta.get("name", ""))
        record.pipeline_state["stage_agent_ids"] = dict(team_meta.get("stage_agent_ids", {}))
        record.pipeline_state["agent_personas"] = agent_personas
        record.pipeline_state["sil_ready"] = False
        record.pipeline_state["context_layer_status"] = "pending"
        record.pipeline_state["system_context_model"] = {}
        record.pipeline_state["convention_profile"] = {}
        record.pipeline_state["health_assessment"] = {}
        record.pipeline_state["remediation_backlog"] = []
        record.pipeline_state["context_vault_ref"] = {}
        record.pipeline_state["sil_discovery"] = discover_repo_snapshot(ROOT)
        self._append_log(record, f"▶ Pipeline started (run_id={run_id})")
        self._append_log(record, f"👥 Team selected: {record.team_name or 'Ad-hoc Team'}")
        self._append_log(record, "🧠 System Intelligence Layer scheduled (SCM / CP / HA-RB)")
        self._append_log(record, "ℹ️ Analyst Q&A disabled in web mode; using direct execution")

        with self._lock:
            self._records[run_id] = record

        self.store.finalize_run(
            run_id=run_id,
            status="running",
            pipeline_state=record.pipeline_state,
            stage_status=record.stage_status,
            progress_logs=record.progress_logs,
            error_message=None,
        )

        thread = threading.Thread(
            target=self._execute_run,
            args=(run_id,),
            daemon=True,
            name=f"pipeline-run-{run_id}",
        )
        record.thread = thread
        thread.start()
        return run_id

    def _run_context_layer(self, record: RunRecord) -> bool:
        if record.pipeline_state is None:
            record.pipeline_state = make_initial_state(record.objectives)
        if record.pipeline_state.get("sil_ready"):
            return True

        self._append_log(record, "⏳ Context Layer started: System Intelligence Layer (SIL)")
        try:
            llm = LLMClient(record.config)
            sil_agent = SystemIntelligenceAgent(llm)
            result = sil_agent.run(record.pipeline_state)
            sil_output = result.output if isinstance(result.output, dict) else {}
            sil_output = normalize_sil_output(sil_output, record.pipeline_state.get("sil_discovery", {}))
            vault_ref = store_context_vault(
                run_id=record.run_id,
                repo_root=ROOT,
                vault_root=CONTEXT_VAULT_ROOT,
                sil_output=sil_output,
                discovery=record.pipeline_state.get("sil_discovery", {}) if isinstance(record.pipeline_state, dict) else {},
            )
            graph_summary = sync_sil_graph(
                CONTEXT_GRAPH_DB,
                sil_output=sil_output,
                context_ref=vault_ref,
                run_id=record.run_id,
            )
            vault_ref["graph_summary"] = graph_summary
            vault_ref["graph_db_path"] = str(CONTEXT_GRAPH_DB)

            contract_suite = build_context_contract_suite(
                sil_output,
                context_ref=vault_ref,
                run_id=record.run_id,
                schema_dir=CONTRACT_SCHEMA_DIR,
                repo_root=ROOT,
                labels={
                    "pipeline": "synthetix",
                    "run_id": record.run_id,
                    "bundle_version": str(vault_ref.get("version_id", "")),
                },
                model=record.config.get_model(),
            )
            contract_paths = persist_context_contract_suite(
                contract_suite,
                Path(str(vault_ref.get("vault_path", ""))).resolve() / "contract_bundle",
            )
            vault_ref["contract_bundle_path"] = str(Path(str(vault_ref.get("vault_path", ""))).resolve() / "contract_bundle")
            vault_ref["contract_paths"] = contract_paths
        except Exception as exc:
            self._fail(record, f"Context Layer failed before Stage 1: {exc}")
            return False

        record.pipeline_state["sil_output"] = sil_output
        record.pipeline_state["system_context_model"] = sil_output.get("system_context_model", {})
        record.pipeline_state["convention_profile"] = sil_output.get("convention_profile", {})
        record.pipeline_state["health_assessment"] = sil_output.get("health_assessment", {})
        record.pipeline_state["remediation_backlog"] = sil_output.get("remediation_backlog", [])
        record.pipeline_state["context_vault_ref"] = vault_ref
        record.pipeline_state["context_contracts"] = {
            "system_context_model": contract_suite.get("system_context_model", {}),
            "convention_profile": contract_suite.get("convention_profile", {}),
            "health_assessment_bundle": contract_suite.get("health_assessment_bundle", {}),
            "context_bundle": contract_suite.get("context_bundle", {}),
        }
        record.pipeline_state["context_bundle"] = contract_suite.get("context_bundle", {})
        record.pipeline_state["context_contract_validation"] = contract_suite.get("validation_report", {})
        record.pipeline_state["sil_ready"] = True
        report = contract_suite.get("validation_report", {}) if isinstance(contract_suite.get("validation_report", {}), dict) else {}
        semantic_issues = report.get("semantic_issues", []) if isinstance(report.get("semantic_issues", []), list) else []
        schema_issues = report.get("schema_issues", []) if isinstance(report.get("schema_issues", []), list) else []
        blocking_issues = [x for x in (schema_issues + semantic_issues) if not str(x).startswith("Schema validation skipped")]
        if blocking_issues:
            record.pipeline_state["context_layer_status"] = "failed"
            self._append_log(record, f"❌ Context contract validation failed with {len(blocking_issues)} issues")
            for issue in blocking_issues[:12]:
                self._append_log(record, f"  • {issue}")
            self._fail(record, "Context Layer contract validation failed. Fix SCM/CP/HAB contract issues before downstream stages.")
            return False
        record.pipeline_state["context_layer_status"] = "ready"

        existing_results = list(record.pipeline_state.get("agent_results", []))
        sil_result = {
            "agent_name": result.agent_name,
            "stage": 0,
            "status": result.status,
            "summary": result.summary,
            "output": {
                "context_reference": {
                    "version_id": vault_ref.get("version_id", ""),
                    "repo": vault_ref.get("repo", ""),
                    "branch": vault_ref.get("branch", ""),
                    "commit_sha": vault_ref.get("commit_sha", ""),
                },
                "context_bundle": contract_suite.get("context_bundle", {}),
                "contract_validation": contract_suite.get("validation_report", {}),
                **sil_output,
            },
            "tokens_used": result.tokens_used,
            "latency_ms": result.latency_ms,
            "logs": result.logs,
        }
        existing_results.append(sil_result)
        record.pipeline_state["agent_results"] = existing_results
        record.pipeline_state["total_tokens"] = record.pipeline_state.get("total_tokens", 0) + result.tokens_used
        record.pipeline_state["total_latency_ms"] = record.pipeline_state.get("total_latency_ms", 0) + result.latency_ms

        for line in result.logs:
            self._append_log(record, line, timestamped=True)
        self._append_log(
            record,
            f"✅ Context Layer ready: {vault_ref.get('version_id', 'unknown')} "
            f"({vault_ref.get('repo', '')}@{vault_ref.get('branch', '')})",
        )
        self.store.save_stage_snapshot(
            run_id=record.run_id,
            stage=0,
            stage_result=sil_result,
            pipeline_state=record.pipeline_state,
            stage_status=record.stage_status,
            progress_logs=record.progress_logs,
        )
        self._persist(record)
        return True

    @staticmethod
    def _context_reference_from_state(state: dict[str, Any]) -> dict[str, Any]:
        ref = state.get("context_vault_ref", {}) if isinstance(state, dict) else {}
        scm = state.get("system_context_model", {}) if isinstance(state, dict) else {}
        cp = state.get("convention_profile", {}) if isinstance(state, dict) else {}
        ha = state.get("health_assessment", {}) if isinstance(state, dict) else {}
        return {
            "version_id": str(ref.get("version_id", "")),
            "repo": str(ref.get("repo", "")),
            "branch": str(ref.get("branch", "")),
            "commit_sha": str(ref.get("commit_sha", "")),
            "scm_version": str(scm.get("version", "scm-v1")),
            "cp_version": str(cp.get("version", "cp-v1")),
            "ha_version": str(ha.get("version", "ha-v1")),
        }

    @staticmethod
    def _collect_runtime_logs_from_output(output: dict[str, Any]) -> list[dict[str, Any]]:
        if not isinstance(output, dict):
            return []
        logs: list[dict[str, Any]] = []
        deployment = output.get("deployment_result", {}) if isinstance(output.get("deployment_result", {}), dict) else {}
        service_name = str(output.get("component", "")).strip() or "deployed-service"

        docker_live = output.get("docker_live_deployment", {}) if isinstance(output.get("docker_live_deployment", {}), dict) else {}
        if docker_live:
            service_name = str(docker_live.get("component", service_name) or service_name)
            raw_logs = docker_live.get("logs", {}) if isinstance(docker_live.get("logs", {}), dict) else {}
            for key in ("stdout", "stderr"):
                block = str(raw_logs.get(key, "") or "")
                if not block:
                    continue
                for line in block.splitlines():
                    txt = line.strip()
                    if txt:
                        logs.append({"service": service_name, "message": txt, "stream": key})

        cloud_live = output.get("cloud_live_deployment", {}) if isinstance(output.get("cloud_live_deployment", {}), dict) else {}
        if cloud_live:
            platform = str(cloud_live.get("platform", "cloud")).strip() or "cloud"
            steps = cloud_live.get("steps", []) if isinstance(cloud_live.get("steps", []), list) else []
            for step in steps:
                if not isinstance(step, dict):
                    continue
                msg = str(step.get("message", "")).strip()
                if not msg:
                    continue
                logs.append(
                    {
                        "service": f"{platform}-deployment",
                        "message": msg,
                        "status": str(step.get("status", "")),
                    }
                )

        url = str(deployment.get("url", "")).strip()
        if url:
            logs.append(
                {
                    "service": service_name,
                    "message": f"GET /health url={url}",
                    "http_method": "GET",
                    "http_path": "/health",
                    "url": url,
                }
            )

        return logs[:1600]

    def _execute_run(self, run_id: str) -> None:
        while True:
            record = self._get_record(run_id)
            if not record:
                return
            if record.status != "running":
                return
            if record.pending_approval is not None:
                return

            if not record.pipeline_state or not record.pipeline_state.get("sil_ready"):
                if not self._run_context_layer(record):
                    return

            stage_idx = int(record.next_stage_idx)
            if stage_idx >= len(AGENT_SEQUENCE):
                record.status = "completed"
                record.updated_at = _utc_now()
                if record.pipeline_state is not None:
                    record.pipeline_state["pipeline_status"] = "completed"
                self._append_log(record, "🏁 Pipeline completed successfully")
                self._attempt_github_export(record, final=True)
                self._persist(record)
                return

            stage_num = stage_idx + 1
            agent = AGENT_CARDS[stage_idx]

            gate_issues = context_gate_issues(record.pipeline_state or {})
            if gate_issues:
                self._fail(
                    record,
                    f"Context gate failed before Stage {stage_num}: " + "; ".join(gate_issues),
                )
                return

            # Developer planning checkpoint (always before code generation)
            if stage_idx == DEVELOPER_STAGE_INDEX and not record.pipeline_state.get("developer_plan_approved"):
                self._mark_stage_running(record, stage_num, f"{agent['name']} (planning)")
                try:
                    llm = LLMClient(record.config)
                    dev_agent = DeveloperAgent(llm)
                    plan, plan_tokens, plan_latency, raw_response = dev_agent.generate_plan(record.pipeline_state)
                except Exception as exc:
                    self._fail(record, f"Developer planning failed: {exc}")
                    return

                record.pipeline_state["developer_plan"] = plan
                record.stage_status[stage_num] = "waiting_approval"
                record.current_stage = stage_num
                existing_results = list(record.pipeline_state.get("agent_results", []))
                existing_results.append(
                    {
                        "agent_name": "Developer Agent",
                        "stage": DEVELOPER_STAGE_NUM,
                        "status": "waiting_approval",
                        "summary": "Developer planning complete — awaiting human approval",
                        "output": {"developer_plan": plan},
                        "tokens_used": plan_tokens,
                        "latency_ms": plan_latency,
                        "logs": ["Developer planning complete; waiting for approval"],
                        "raw_response": raw_response,
                    }
                )
                record.pipeline_state["agent_results"] = existing_results
                record.pipeline_state["total_tokens"] = record.pipeline_state.get("total_tokens", 0) + plan_tokens
                record.pipeline_state["total_latency_ms"] = record.pipeline_state.get("total_latency_ms", 0) + plan_latency

                self._append_log(record, "🧭 Developer plan generated. Awaiting human approval.")
                record.pending_approval = {
                    "type": "developer_plan",
                    "stage": DEVELOPER_STAGE_NUM,
                    "message": "Review developer plan and approve with selected options.",
                    "options": plan.get("options", {}),
                }
                record.status = "waiting_approval"
                self._persist(record)
                return

            # Cloud deployment details checkpoint before deployment stage
            if stage_idx == DEPLOYMENT_STAGE_INDEX and str(record.deployment_target).lower() == "cloud":
                platform = str(record.cloud_config.get("platform", "")).strip().lower()
                required_fields = required_cloud_fields(platform)
                missing = [k for k in required_fields if not str(record.cloud_config.get(k, "")).strip()]
                if missing:
                    record.stage_status[DEPLOYMENT_STAGE_NUM] = "waiting_approval"
                    record.pending_approval = {
                        "type": "cloud_details",
                        "stage": DEPLOYMENT_STAGE_NUM,
                        "message": "Cloud deployment selected. Provide platform, region, and credentials reference.",
                        "required_fields": required_fields,
                        "missing_fields": missing,
                    }
                    record.status = "waiting_approval"
                    self._append_log(record, "☁️ Waiting for cloud deployment details before deployment stage.")
                    self._persist(record)
                    return

            self._mark_stage_running(record, stage_num, agent["name"])

            try:
                updated_state = run_single_stage(
                    config=record.config,
                    state=record.pipeline_state or make_initial_state(record.objectives),
                    stage_index=stage_idx,
                )
                latest_result = updated_state.get("agent_results", [{}])[-1]
                _, output_key = AGENT_SEQUENCE[stage_idx]
                context_ref = self._context_reference_from_state(updated_state)
                if stage_num >= 2 and not isinstance(latest_result.get("output"), dict):
                    self._fail(
                        record,
                        f"Context contract violation at stage {stage_num}: output must be a JSON object with context_reference",
                    )
                    return
                if isinstance(latest_result.get("output"), dict):
                    latest_result["output"]["context_reference"] = context_ref
                if isinstance(updated_state.get(output_key), dict):
                    updated_state[output_key]["context_reference"] = context_ref
                record.pipeline_state = updated_state
                record.current_stage = stage_num
                record.stage_status[stage_num] = latest_result.get("status", "error")
                if stage_num == 3:
                    written_count = _materialize_generated_code_artifacts(record.run_id, record.pipeline_state)
                    if written_count > 0:
                        artifact_root = f"run_artifacts/{safe_name(record.run_id)}/generated_code"
                        if isinstance(latest_result.get("output"), dict):
                            latest_result["output"]["artifact_root"] = artifact_root
                        if isinstance(record.pipeline_state.get("developer_output"), dict):
                            record.pipeline_state["developer_output"]["artifact_root"] = artifact_root
                        self._append_log(
                            record,
                            f"🗂️ Materialized {written_count} generated code files under {artifact_root}",
                        )

                for line in latest_result.get("logs", []):
                    self._append_log(record, line, timestamped=True)
                summary = latest_result.get("summary", "")
                result_status = str(latest_result.get("status", "")).strip().lower()
                if result_status == "success":
                    status_icon = "✅"
                elif result_status == "warning":
                    status_icon = "⚠️"
                else:
                    status_icon = "❌"
                self._append_log(
                    record,
                    f"{status_icon} Stage {stage_num} finished: {summary}",
                )

                self.store.save_stage_snapshot(
                    run_id=record.run_id,
                    stage=stage_num,
                    stage_result=latest_result,
                    pipeline_state=record.pipeline_state,
                    stage_status=record.stage_status,
                    progress_logs=record.progress_logs,
                )

                # Progressive export so artifacts are preserved even if later stages fail.
                self._attempt_github_export(record, final=False)

                if latest_result.get("status") == "error":
                    self._fail(record, f"Pipeline failed at stage {stage_num}: {summary}")
                    return

                # Deployment stage: augment context graph from runtime logs when available.
                if stage_idx == DEPLOYMENT_STAGE_INDEX:
                    runtime_logs = self._collect_runtime_logs_from_output(
                        latest_result.get("output", {}) if isinstance(latest_result.get("output", {}), dict) else {}
                    )
                    if runtime_logs:
                        try:
                            context_ref = (
                                record.pipeline_state.get("context_vault_ref", {})
                                if isinstance(record.pipeline_state.get("context_vault_ref", {}), dict)
                                else {}
                            )
                            if context_ref.get("version_id"):
                                ingest_summary = ingest_runtime_logs(
                                    CONTEXT_GRAPH_DB,
                                    context_ref=context_ref,
                                    logs=runtime_logs,
                                    run_id=record.run_id,
                                )
                                artifact_path = _persist_context_report(
                                    context_ref,
                                    "runtime_log_ingest",
                                    {
                                        "summary": ingest_summary,
                                        "sample_logs": runtime_logs[:50],
                                        "ingested_at": _utc_now(),
                                    },
                                )
                                if artifact_path:
                                    ingest_summary["artifact_path"] = artifact_path
                                record.pipeline_state["runtime_log_ingestion"] = ingest_summary
                                self._append_log(
                                    record,
                                    "📈 Runtime log augmentation: "
                                    f"{ingest_summary.get('runtime_edges_upserted', 0)} edges from "
                                    f"{ingest_summary.get('log_entries_parsed', 0)} log entries",
                                )
                        except Exception as exc:
                            self._append_log(record, f"⚠️ Runtime log augmentation skipped: {exc}")

                # Mark retry plan as applied once Developer stage finishes.
                if stage_idx == DEVELOPER_STAGE_INDEX:
                    retry_plan = (
                        dict(record.pipeline_state.get("retry_plan", {}))
                        if isinstance(record.pipeline_state.get("retry_plan"), dict)
                        else {}
                    )
                    if retry_plan and retry_plan.get("status") == "pending":
                        execution = latest_result.get("output", {}).get("execution", {})
                        applied = execution.get("self_heal_applied", []) if isinstance(execution, dict) else []
                        retry_plan["status"] = "applied"
                        retry_plan["applied_at"] = _utc_now()
                        retry_plan["applied_self_heal_actions"] = applied
                        record.pipeline_state["retry_plan"] = retry_plan
                        history = list(record.pipeline_state.get("retry_history", []))
                        if history:
                            history[-1] = retry_plan
                            record.pipeline_state["retry_history"] = history
                        self._append_log(
                            record,
                            "🛠️ Retry plan applied by Developer: "
                            f"{len(applied) if isinstance(applied, list) else 0} actions executed",
                        )

                # Tester -> Developer retry loop
                if stage_idx == TESTER_STAGE_INDEX:
                    tester_output = latest_result.get("output", {})
                    gate = tester_output.get("overall_results", {}).get("quality_gate", "pass")
                    if gate == "fail" and record.retry_count < record.config.max_retries:
                        record.retry_count += 1
                        diagnosis: dict[str, Any] = {}
                        diagnosis_tokens = 0
                        diagnosis_latency_ms = 0.0
                        diagnosis_raw = ""
                        try:
                            llm = LLMClient(record.config)
                            dev_agent = DeveloperAgent(llm)
                            diagnosis, diagnosis_tokens, diagnosis_latency_ms, diagnosis_raw = (
                                dev_agent.generate_retry_diagnosis(record.pipeline_state or {}, tester_output)
                            )
                            self._append_log(
                                record,
                                "🧠 Pre-retry diagnosis generated: "
                                f"{diagnosis.get('diagnosis_summary', 'no summary')}",
                            )
                        except Exception as exc:
                            self._append_log(record, f"⚠️ Pre-retry diagnosis fallback: {exc}")
                        failure_analysis = (
                            tester_output.get("failure_analysis", {})
                            if isinstance(tester_output.get("failure_analysis", {}), dict)
                            else {}
                        )
                        diagnosis_strategy = diagnosis.get("retry_strategy", {}) if isinstance(diagnosis, dict) else {}
                        planned_actions = failure_analysis.get(
                            "self_heal_actions",
                            [
                                "Regenerate failing components only",
                                "Apply remediations from failed checks",
                                "Keep target modernization language consistent",
                            ],
                        )
                        if isinstance(diagnosis_strategy, dict):
                            planned_actions = list(planned_actions) + list(diagnosis_strategy.get("environment_actions", []))
                        retry_plan = {
                            "attempt": record.retry_count,
                            "max_retries": record.config.max_retries,
                            "status": "pending",
                            "created_at": _utc_now(),
                            "trigger": "tester_quality_gate_failed",
                            "quality_gate": gate,
                            "blocking_issues": tester_output.get("overall_results", {}).get("blocking_issues", []),
                            "critical_failures": failure_analysis.get("critical_failures", []),
                            "planned_self_heal_actions": planned_actions,
                            "planned_component_exclusions": (
                                list(failure_analysis.get("suggested_component_exclusions", []))
                                + list(diagnosis_strategy.get("component_exclusions", []))
                            ),
                            "pre_retry_diagnosis": diagnosis,
                            "pre_retry_diagnosis_raw": diagnosis_raw[:4000],
                            "pre_retry_diagnosis_tokens": diagnosis_tokens,
                            "pre_retry_diagnosis_latency_ms": diagnosis_latency_ms,
                            "target_modernization_language": record.pipeline_state.get(
                                "modernization_language", record.modernization_language
                            ),
                        }
                        history = list(record.pipeline_state.get("retry_history", []))
                        history.append(retry_plan)
                        record.pipeline_state["retry_plan"] = retry_plan
                        record.pipeline_state["retry_history"] = history
                        record.pipeline_state["retry_diagnosis"] = diagnosis
                        record.pipeline_state["total_tokens"] = record.pipeline_state.get("total_tokens", 0) + diagnosis_tokens
                        record.pipeline_state["total_latency_ms"] = record.pipeline_state.get("total_latency_ms", 0) + diagnosis_latency_ms
                        record.pipeline_state["tester_feedback"] = tester_output
                        record.stage_status[DEVELOPER_STAGE_NUM] = "running"
                        record.stage_status[DATABASE_STAGE_NUM] = "pending"
                        record.stage_status[SECURITY_STAGE_NUM] = "pending"
                        record.stage_status[TESTER_STAGE_NUM] = "pending"
                        self._append_log(
                            record,
                            "🔄 Quality gate FAILED — sending feedback to Developer "
                            f"(retry {record.retry_count}/{record.config.max_retries})"
                        )
                        self._append_log(
                            record,
                            "🩹 Retry plan prepared: "
                            f"{len(retry_plan.get('planned_self_heal_actions', []))} self-heal actions, "
                            f"{len(retry_plan.get('planned_component_exclusions', []))} component exclusions",
                        )
                        record.next_stage_idx = DEVELOPER_STAGE_INDEX
                        self._persist(record)
                        continue
                    if gate == "fail":
                        self._fail(
                            record,
                            "Quality gate FAILED after "
                            f"{record.retry_count}/{record.config.max_retries} retries. "
                            "Pipeline stopped before downstream validation/deployment stages.",
                        )
                        return

                    if "tester_feedback" in record.pipeline_state:
                        del record.pipeline_state["tester_feedback"]

                record.next_stage_idx = stage_idx + 1

                # Human approval gate after each completed stage
                if record.human_approval and record.next_stage_idx < len(AGENT_SEQUENCE):
                    record.pending_approval = {
                        "type": "stage_gate",
                        "stage": stage_num,
                        "next_stage": record.next_stage_idx + 1,
                        "message": f"Approve transition from Stage {stage_num} to Stage {record.next_stage_idx + 1}",
                    }
                    record.status = "waiting_approval"
                    record.stage_status[stage_num] = latest_result.get("status", "success")
                    self._append_log(
                        record,
                        f"🛑 Human approval required before Stage {record.next_stage_idx + 1}",
                    )
                    self._persist(record)
                    return

                self._persist(record)
            except Exception as exc:
                self._fail(record, f"Stage {stage_num} exception: {exc}")
                return

    def _mark_stage_running(self, record: RunRecord, stage_num: int, name: str) -> None:
        record.stage_status[stage_num] = "running"
        record.updated_at = _utc_now()
        self._append_log(record, f"⏳ Stage {stage_num} started: {name}")
        self._persist(record)

    def _fail(self, record: RunRecord, error_message: str) -> None:
        record.status = "failed"
        record.error_message = error_message
        record.updated_at = _utc_now()
        self._append_log(record, f"❌ {error_message}")
        if record.pipeline_state is not None:
            record.pipeline_state["pipeline_status"] = "failed"
        self._attempt_github_export(record, final=True)
        self._persist(record)

    def _attempt_github_export(self, record: RunRecord, final: bool = True) -> None:
        if not isinstance(record.pipeline_state, dict):
            return
        existing = record.pipeline_state.get("github_export", {})
        if final and isinstance(existing, dict) and existing.get("final_attempted"):
            return
        current_stage = int(record.current_stage or 0)
        progress = (
            dict(record.pipeline_state.get("github_export_progress", {}))
            if isinstance(record.pipeline_state.get("github_export_progress", {}), dict)
            else {}
        )
        if (not final) and int(progress.get("last_stage_exported", 0) or 0) >= current_stage:
            return
        try:
            run_payload = self._record_payload(record)
            report = _export_run_to_github(record.run_id, run_payload)
        except Exception as exc:
            report = {"status": "failed", "reason": str(exc)}
        report["attempted"] = True
        report["attempted_at"] = _utc_now()
        report["final_attempted"] = bool(final)
        report["stage_at_export"] = current_stage
        record.pipeline_state["github_export"] = report
        if not final:
            progress["last_stage_exported"] = current_stage
            progress["last_export_at"] = _utc_now()
            progress["last_status"] = report.get("status", "")
            progress["last_reason"] = report.get("reason", "")
            progress["attempt_count"] = int(progress.get("attempt_count", 0) or 0) + 1
            record.pipeline_state["github_export_progress"] = progress
        status = str(report.get("status", "")).strip().lower()
        phase = "final" if final else f"stage {current_stage}"
        if status == "exported":
            self._append_log(
                record,
                f"📦 GitHub export ({phase}) succeeded: "
                f"{report.get('owner', '')}/{report.get('repository', '')}@{report.get('branch', '')} "
                f"({report.get('exported_files', 0)} files)",
            )
        elif status == "skipped":
            self._append_log(record, f"ℹ️ GitHub export ({phase}) skipped: {report.get('reason', 'not enabled')}")
        elif status == "partial":
            self._append_log(
                record,
                f"⚠️ GitHub export ({phase}) partially completed: "
                f"{report.get('exported_files', 0)} exported, {report.get('failed_files', 0)} failed",
            )
        else:
            self._append_log(record, f"⚠️ GitHub export ({phase}) failed: {report.get('reason', 'unknown error')}")

    def _persist(self, record: RunRecord) -> None:
        if record.pipeline_state is not None:
            record.pipeline_state["pending_approval"] = copy.deepcopy(record.pending_approval)
            record.pipeline_state["next_stage_idx"] = int(record.next_stage_idx)
        self.store.finalize_run(
            run_id=record.run_id,
            status=record.status,
            pipeline_state=record.pipeline_state,
            stage_status=record.stage_status,
            progress_logs=record.progress_logs,
            error_message=record.error_message,
        )
        snapshot = self._record_payload(record)
        self._emit_event(record.run_id, "update", {"run": snapshot})
        if record.status != "running":
            self._emit_event(record.run_id, "done", {"status": record.status, "run_id": record.run_id})

    def _append_log(self, record: RunRecord, message: str, timestamped: bool = False) -> None:
        line = message if timestamped else f"[{_ts()}] {message}"
        record.progress_logs.append(line)
        self._emit_event(record.run_id, "log", {"line": line, "run_id": record.run_id})

    def _emit_event(self, run_id: str, event: str, data: dict[str, Any]) -> None:
        with self._lock:
            subscribers = list(self._subscribers.get(run_id, {}).values())
        if not subscribers:
            return
        payload = {"event": event, "data": data}
        for sub in subscribers:
            try:
                sub.put_nowait(payload)
            except queue.Full:
                # Drop stale updates if a consumer is too slow.
                continue

    def subscribe(self, run_id: str) -> tuple[str, queue.Queue] | None:
        record = self._get_record(run_id)
        if not record or record.status != "running":
            return None
        sub_id = uuid.uuid4().hex
        q: queue.Queue = queue.Queue(maxsize=500)
        with self._lock:
            self._subscribers.setdefault(run_id, {})[sub_id] = q
        return sub_id, q

    def unsubscribe(self, run_id: str, sub_id: str) -> None:
        with self._lock:
            group = self._subscribers.get(run_id)
            if not group:
                return
            group.pop(sub_id, None)
            if not group:
                self._subscribers.pop(run_id, None)

    def _resume_thread(self, record: RunRecord) -> None:
        thread = threading.Thread(
            target=self._execute_run,
            args=(record.run_id,),
            daemon=True,
            name=f"pipeline-resume-{record.run_id}-{uuid.uuid4().hex[:6]}",
        )
        record.thread = thread
        thread.start()

    def approve(self, run_id: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            record = self._records.get(run_id)

        if not record:
            return {"ok": False, "error": "run not found or no longer active"}
        if not record.pending_approval:
            return {"ok": False, "error": "run is not awaiting approval"}

        pending = dict(record.pending_approval)
        decision = str(payload.get("decision", "approve")).strip().lower()
        if decision not in {"approve", "reject"}:
            return {"ok": False, "error": "decision must be approve or reject"}

        if decision == "reject":
            self._fail(
                record,
                f"Human rejected at stage {pending.get('stage', '?')} ({pending.get('type', 'approval')})",
            )
            return {"ok": True, "status": "failed", "run_id": run_id}

        pending_type = pending.get("type")
        if pending_type == "developer_plan":
            plan = record.pipeline_state.get("developer_plan", {})
            defaults = {
                "microservices_count": plan.get("default_microservices_count", record.config.developer_parallel_agents),
                "split_strategy": plan.get("default_split_strategy", "domain-driven"),
                "target_language": plan.get("default_target_language", record.modernization_language or "python"),
                "target_platform": plan.get("default_target_platform", "docker-local"),
            }
            choices = dict(defaults)
            choices.update(payload.get("developer_choices", {}) or {})
            record.pipeline_state["developer_choices"] = choices
            record.pipeline_state["developer_plan_approved"] = True
            record.modernization_language = str(choices.get("target_language", record.modernization_language))
            record.config.developer_parallel_agents = int(
                choices.get("microservices_count", record.config.developer_parallel_agents)
            )
            record.next_stage_idx = DEVELOPER_STAGE_INDEX
            self._append_log(
                record,
                f"✅ Developer plan approved (count={record.config.developer_parallel_agents}, "
                f"strategy={choices.get('split_strategy', '')})",
            )
        elif pending_type == "cloud_details":
            cloud_cfg = dict(record.cloud_config)
            cloud_cfg.update(payload.get("cloud_config", {}) or {})
            platform = str(cloud_cfg.get("platform", "")).strip().lower()
            required = set(required_cloud_fields(platform))
            missing = [k for k in required if not str(cloud_cfg.get(k, "")).strip()]
            if missing:
                return {"ok": False, "error": f"missing cloud_config fields: {', '.join(missing)}"}
            record.cloud_config = cloud_cfg
            record.pipeline_state["cloud_config"] = cloud_cfg
            record.next_stage_idx = DEPLOYMENT_STAGE_INDEX
            self._append_log(record, "✅ Cloud deployment details approved.")
        elif pending_type == "stage_gate":
            next_stage = int(pending.get("next_stage", record.next_stage_idx + 1))
            record.next_stage_idx = max(0, next_stage - 1)
            self._append_log(record, f"✅ Human approved transition to Stage {next_stage}")
        else:
            return {"ok": False, "error": f"unsupported approval type: {pending_type}"}

        record.pending_approval = None
        record.status = "running"
        record.updated_at = _utc_now()
        self._persist(record)
        self._resume_thread(record)
        return {"ok": True, "status": "running", "run_id": run_id}

    def _get_record(self, run_id: str) -> RunRecord | None:
        with self._lock:
            return self._records.get(run_id)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        record = self._get_record(run_id)
        if record:
            return self._record_payload(record)

        persisted = self.store.load_run(run_id)
        if not persisted:
            return None
        status = persisted.get("pipeline_status", "unknown")
        stage_status_raw = persisted.get("stage_status", {})
        stage_status = {
            int(k): v for k, v in stage_status_raw.items() if str(k).isdigit()
        }
        pipeline_state = persisted.get("pipeline_state") or {}
        pending = pipeline_state.get("pending_approval")
        return {
            "run_id": run_id,
            "status": status,
            "current_stage": max(stage_status.keys(), default=0),
            "next_stage_idx": int(pipeline_state.get("next_stage_idx", 0) or 0),
            "stage_status": stage_status,
            "progress_logs": persisted.get("progress_logs", []),
            "pipeline_state": pipeline_state,
            "error_message": persisted.get("error_message"),
            "retry_count": 0,
            "pending_approval": pending,
            "human_approval": bool(pipeline_state.get("human_approval", False)),
            "strict_security_mode": bool(pipeline_state.get("strict_security_mode", False)),
            "deployment_target": str(pipeline_state.get("deployment_target", "local")),
            "integration_context": pipeline_state.get("integration_context", {}) if isinstance(pipeline_state.get("integration_context"), dict) else {},
            "project_state_mode": str(pipeline_state.get("project_state_mode", "auto")),
            "project_state_detected": str(pipeline_state.get("project_state_detected", "")),
            "database_source": str(pipeline_state.get("database_source", "")),
            "database_target": str(pipeline_state.get("database_target", "")),
            "database_schema": str(pipeline_state.get("database_schema", "")),
            "team_id": str(pipeline_state.get("team_id", "")),
            "team_name": str(pipeline_state.get("team_name", "")),
            "stage_agent_ids": pipeline_state.get("stage_agent_ids", {}) if isinstance(pipeline_state.get("stage_agent_ids"), dict) else {},
            "agent_personas": pipeline_state.get("agent_personas", {}) if isinstance(pipeline_state.get("agent_personas"), dict) else {},
            "created_at": "",
            "updated_at": persisted.get("saved_at", ""),
        }

    @staticmethod
    def _record_payload(record: RunRecord) -> dict[str, Any]:
        return {
            "run_id": record.run_id,
            "status": record.status,
            "current_stage": record.current_stage,
            "next_stage_idx": record.next_stage_idx,
            "stage_status": copy.deepcopy(record.stage_status),
            "progress_logs": list(record.progress_logs),
            "pipeline_state": copy.deepcopy(record.pipeline_state),
            "error_message": record.error_message,
            "retry_count": record.retry_count,
            "pending_approval": copy.deepcopy(record.pending_approval),
            "human_approval": record.human_approval,
            "strict_security_mode": record.strict_security_mode,
            "deployment_target": record.deployment_target,
            "integration_context": copy.deepcopy(record.integration_context),
            "project_state_mode": record.project_state_mode,
            "project_state_detected": record.project_state_detected,
            "database_source": record.database_source,
            "database_target": record.database_target,
            "database_schema": record.database_schema,
            "team_id": record.team_id,
            "team_name": record.team_name,
            "stage_agent_ids": copy.deepcopy(record.stage_agent_ids),
            "agent_personas": copy.deepcopy(record.agent_personas),
            "use_case": record.use_case,
            "created_at": record.created_at,
            "updated_at": record.updated_at,
        }

    def list_runs(self, limit: int = 40) -> list[dict[str, Any]]:
        return self.store.list_runs(limit=limit)


MANAGER = PipelineRunManager(RUN_STORE)
STATIC_DIR = ROOT / "web" / "static"


def _get_json(body: bytes) -> dict[str, Any]:
    if not body:
        return {}
    try:
        data = json.loads(body.decode("utf-8"))
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _extract_integration_context(payload: dict[str, Any]) -> dict[str, Any]:
    context = payload.get("integration_context", {})
    return context if isinstance(context, dict) else {}


def _http_json_request(
    url: str,
    *,
    method: str = "GET",
    headers: dict[str, str] | None = None,
    payload: dict[str, Any] | None = None,
    timeout: int = 20,
) -> dict[str, Any] | list[Any]:
    request_headers = dict(headers or {})
    body_bytes = None
    if payload is not None:
        body_bytes = json.dumps(payload).encode("utf-8")
        request_headers.setdefault("Content-Type", "application/json")
    request = Request(url, data=body_bytes, headers=request_headers, method=method.upper())
    try:
        with urlopen(request, timeout=timeout) as response:
            raw = response.read().decode("utf-8", errors="replace")
    except HTTPError as exc:
        detail = ""
        try:
            detail = exc.read().decode("utf-8", errors="replace")
        except Exception:
            detail = ""
        try:
            parsed = json.loads(detail) if detail else {}
        except Exception:
            parsed = {}
        message = ""
        if isinstance(parsed, dict):
            message = str(parsed.get("message") or parsed.get("error") or "").strip()
        if not message:
            message = (detail or str(exc)).strip()
        raise ValueError(f"http {exc.code}: {message[:280]}")
    except URLError as exc:
        raise ValueError(f"network error: {exc.reason}")

    if not raw.strip():
        return {}
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        raise ValueError("upstream returned non-json response")
    if isinstance(parsed, (dict, list)):
        return parsed
    raise ValueError("upstream returned unsupported JSON payload")


def _parse_github_repo_url(repo_url: str) -> tuple[str, str]:
    raw = str(repo_url or "").strip()
    if not raw:
        return "", ""
    # git@github.com:org/repo(.git)
    if raw.startswith("git@") and ":" in raw:
        path_part = raw.split(":", 1)[1]
        segments = [seg for seg in path_part.split("/") if seg]
        if len(segments) >= 2:
            owner = segments[0]
            repo = segments[1]
            if repo.endswith(".git"):
                repo = repo[:-4]
            return owner, repo
    parsed = urlparse(raw)
    segments = [seg for seg in parsed.path.split("/") if seg]
    if len(segments) >= 2:
        owner = segments[0]
        repo = segments[1]
        if repo.endswith(".git"):
            repo = repo[:-4]
        return owner, repo
    return "", ""


def _parse_linear_board(board_text: str, fallback_team: str = "") -> tuple[str, str]:
    raw = str(board_text or "").strip()
    fallback = str(fallback_team or "").strip()
    if not raw:
        return fallback, ""
    if "/" in raw:
        team, project = raw.split("/", 1)
        return team.strip() or fallback, project.strip()
    # Treat short uppercase-ish values as team keys (e.g., ENG, ACME)
    compact = raw.replace("-", "").replace("_", "")
    if compact and compact.upper() == compact and " " not in raw and len(compact) <= 10:
        return raw, ""
    return fallback, raw


def _github_live_integration_checks(github_cfg: dict[str, Any]) -> list[dict[str, Any]]:
    checks: list[dict[str, Any]] = []
    token = str(github_cfg.get("token", "")).strip()
    if not token:
        return checks

    base_url = str(github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "synthetix-integrations/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    # Validate token by querying the authenticated user.
    try:
        user_payload = _http_json_request(f"{base_url}/user", headers=headers)
        login = str(user_payload.get("login", "")).strip() if isinstance(user_payload, dict) else ""
        checks.append(
            {
                "name": "github_api_auth",
                "ok": bool(login),
                "message": "GitHub API token is valid" if login else "GitHub API auth failed",
            }
        )
    except ValueError as exc:
        checks.append(
            {
                "name": "github_api_auth",
                "ok": False,
                "message": str(exc),
            }
        )
        return checks

    repo_payloads: dict[str, dict[str, Any]] = {}

    def _repo_check(name: str, owner: str, repository: str) -> None:
        if not owner or not repository:
            checks.append(
                {
                    "name": name,
                    "ok": False,
                    "message": "owner/repository missing",
                }
            )
            return
        try:
            payload = _http_json_request(
                f"{base_url}/repos/{quote(owner)}/{quote(repository)}",
                headers=headers,
            )
            if isinstance(payload, dict):
                repo_payloads[f"{owner}/{repository}"] = payload
            checks.append(
                {
                    "name": name,
                    "ok": True,
                    "message": f"Repo access verified: {owner}/{repository}",
                }
            )
        except ValueError as exc:
            checks.append(
                {
                    "name": name,
                    "ok": False,
                    "message": str(exc),
                }
            )

    source_owner = str(github_cfg.get("owner", "")).strip()
    source_repo = str(github_cfg.get("repository", "")).strip()
    _repo_check("source_repo_access", source_owner, source_repo)

    if bool(github_cfg.get("run_export_enabled", False)):
        export_owner = str(github_cfg.get("export_owner", "")).strip()
        export_repo = str(github_cfg.get("export_repository", "")).strip()
        parsed_owner, parsed_repo = _parse_github_repo_url(export_repo)
        if parsed_repo:
            export_repo = parsed_repo
            if not export_owner and parsed_owner:
                export_owner = parsed_owner
        target_owner = export_owner or source_owner
        target_repo = export_repo or source_repo
        _repo_check("export_repo_access", target_owner, target_repo)
        if not bool(github_cfg.get("read_only", True)):
            repo_key = f"{target_owner}/{target_repo}"
            payload = repo_payloads.get(repo_key, {})
            perms = payload.get("permissions", {}) if isinstance(payload, dict) else {}
            can_write = False
            if isinstance(perms, dict):
                can_write = bool(
                    perms.get("push")
                    or perms.get("maintain")
                    or perms.get("admin")
                )
            checks.append(
                {
                    "name": "export_repo_write_access",
                    "ok": can_write,
                    "message": (
                        f"Write permission verified for {repo_key}"
                        if can_write
                        else f"Token appears read-only for {repo_key}; write permission is required for artifact export"
                    ),
                }
            )
            try:
                _http_json_request(
                    f"{base_url}/repos/{quote(target_owner)}/{quote(target_repo)}/git/blobs",
                    method="POST",
                    headers=headers,
                    payload={"content": "synthetix-write-probe", "encoding": "utf-8"},
                )
                checks.append(
                    {
                        "name": "export_repo_write_probe",
                        "ok": True,
                        "message": f"Write probe succeeded for {repo_key}",
                    }
                )
            except ValueError as exc:
                checks.append(
                    {
                        "name": "export_repo_write_probe",
                        "ok": False,
                        "message": f"Write probe failed for {repo_key}: {exc}",
                    }
                )

    return checks


def _sample_github_tree(owner: str, repository: str) -> dict[str, Any]:
    sample_entries = [
        {"path": "services", "type": "dir", "depth": 0},
        {"path": "services/orders-service", "type": "dir", "depth": 1},
        {"path": "services/orders-service/src", "type": "dir", "depth": 2},
        {"path": "services/orders-service/src/main.java", "type": "file", "depth": 3},
        {"path": "services/payments-service", "type": "dir", "depth": 1},
        {"path": "services/payments-service/cmd/api/main.go", "type": "file", "depth": 3},
        {"path": "services/inventory-service", "type": "dir", "depth": 1},
        {"path": "legacy/billing-monolith", "type": "dir", "depth": 1},
        {"path": "infra/terraform/main.tf", "type": "file", "depth": 2},
    ]
    return {
        "repo": {"owner": owner or "acme", "repository": repository or "acme-commerce-platform", "default_branch": "main"},
        "tree": {
            "entries": sample_entries,
            "total_entries": len(sample_entries),
            "truncated": False,
            "folders": sum(1 for item in sample_entries if item["type"] == "dir"),
            "files": sum(1 for item in sample_entries if item["type"] == "file"),
            "source": "sample_dataset",
        },
    }


def _sample_linear_issues(team_key: str, project: str) -> dict[str, Any]:
    sample_issues = [
        {"id": "lin-101", "identifier": "ACME-101", "title": "Add idempotency support to Payments API", "state": "In Progress", "priority": 1, "assignee": "A. Patel", "project": "Payment reliability & compliance", "updated_at": "2026-02-14T16:30:00Z"},
        {"id": "lin-102", "identifier": "ACME-102", "title": "Duplicate charges on client retries", "state": "Todo", "priority": 1, "assignee": "S. Rao", "project": "Payment reliability & compliance", "updated_at": "2026-02-13T13:12:00Z"},
        {"id": "lin-103", "identifier": "ACME-103", "title": "Expose refund status in Orders API", "state": "Backlog", "priority": 2, "assignee": "", "project": "Checkout modernization", "updated_at": "2026-02-12T09:40:00Z"},
    ]
    filtered = sample_issues
    if project:
        token = project.lower()
        filtered = [item for item in sample_issues if token in str(item.get("project", "")).lower() or token in str(item.get("title", "")).lower()]
    return {
        "team": {"key": team_key or "ACME", "name": "ACME Team"},
        "issues": filtered,
        "total_issues": len(filtered),
        "source": "sample_dataset",
    }


def _sample_jira_issues(project_key: str) -> dict[str, Any]:
    key = project_key or "ACME"
    issues = [
        {"id": "jira-101", "identifier": f"{key}-101", "title": "Add idempotency support to Payments API", "state": "In Progress", "priority": 1, "assignee": "A. Patel", "project": key, "updated_at": "2026-02-14T16:30:00Z"},
        {"id": "jira-102", "identifier": f"{key}-102", "title": "Duplicate charges on client retries", "state": "To Do", "priority": 1, "assignee": "S. Rao", "project": key, "updated_at": "2026-02-13T13:12:00Z"},
        {"id": "jira-103", "identifier": f"{key}-103", "title": "Expose refund status in Orders API", "state": "Backlog", "priority": 2, "assignee": "", "project": key, "updated_at": "2026-02-12T09:40:00Z"},
    ]
    return {
        "team": {"key": key, "name": f"{key} Project"},
        "issues": issues,
        "total_issues": len(issues),
        "source": "sample_dataset",
    }


def _guess_language(path: str) -> str:
    ext = Path(path).suffix.lower()
    return {
        ".py": "python",
        ".js": "javascript",
        ".ts": "typescript",
        ".tsx": "typescript",
        ".jsx": "javascript",
        ".go": "go",
        ".java": "java",
        ".cs": "csharp",
        ".rb": "ruby",
        ".php": "php",
        ".asp": "asp_classic",
        ".aspx": "aspnet",
        ".asa": "asp_classic",
        ".vb": "vbscript",
        ".vbs": "vbscript",
        ".sql": "sql",
        ".yaml": "yaml",
        ".yml": "yaml",
        ".json": "json",
        ".md": "markdown",
    }.get(ext, "other")


def _normalize_lines(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(x).strip().strip("/") for x in value if str(x).strip()]
    if isinstance(value, str):
        return [line.strip().strip("/") for line in value.splitlines() if line.strip()]
    return []


def _extract_routes_from_text(path: str, text: str) -> list[str]:
    patterns = [
        re.compile(r'@app\.(get|post|put|patch|delete)\(\s*[\'"]([^\'"]+)[\'"]', re.IGNORECASE),
        re.compile(r'router\.(get|post|put|patch|delete)\(\s*[\'"]([^\'"]+)[\'"]', re.IGNORECASE),
        re.compile(r'app\.(get|post|put|patch|delete)\(\s*[\'"]([^\'"]+)[\'"]', re.IGNORECASE),
        re.compile(r'@(?:Get|Post|Put|Patch|Delete)Mapping\(\s*[\'"]([^\'"]+)[\'"]', re.IGNORECASE),
        re.compile(r'HandleFunc\(\s*[\'"]([^\'"]+)[\'"]', re.IGNORECASE),
    ]
    routes: list[str] = []
    for pattern in patterns:
        for match in pattern.findall(text):
            if isinstance(match, tuple):
                if len(match) == 2:
                    method = str(match[0]).upper().strip()
                    route = str(match[1]).strip()
                    if route:
                        routes.append(f"{method} {route}")
                elif len(match) == 1:
                    route = str(match[0]).strip()
                    if route:
                        routes.append(route)
            else:
                route = str(match).strip()
                if route:
                    routes.append(route)
    return routes[:100]


def _domain_capabilities(paths: list[str], routes: list[str]) -> list[str]:
    haystack = " ".join(paths + routes).lower()
    mapping = {
        "order": "Order lifecycle and checkout orchestration",
        "payment": "Payment authorization/capture and retry handling",
        "invoice": "Billing and invoice processing",
        "refund": "Refund and reversal workflows",
        "inventory": "Inventory allocation and stock updates",
        "customer": "Customer profile and account workflows",
        "auth": "Authentication/authorization and session handling",
        "notification": "Notification dispatch (email/SMS/webhooks)",
        "report": "Reporting/analytics pipelines",
        "search": "Search and query indexing functionality",
        "cart": "Shopping cart and basket state management",
    }
    capabilities = [desc for token, desc in mapping.items() if token in haystack]
    return capabilities[:8]


def _framework_signals(text: str) -> list[str]:
    checks = [
        ("FastAPI", ["fastapi", "@app.get(", "FastAPI("]),
        ("Flask", ["flask", "@app.route(", "Flask("]),
        ("Express", ["express(", "app.get(", "router.get("]),
        ("NestJS", ["@nestjs", "Controller(", "@Get("]),
        ("Spring Boot", ["@RestController", "@RequestMapping", "@GetMapping"]),
        ("Gin", ["gin.Default(", "r.GET(", "router.GET("]),
        ("Django", ["django", "urlpatterns", "path("]),
    ]
    lower = text.lower()
    out = []
    for name, tokens in checks:
        if any(token.lower() in lower for token in tokens):
            out.append(name)
    return out


def _analyze_source_bundle(
    *,
    objectives: str,
    repo_label: str,
    file_entries: list[dict[str, Any]],
    file_contents: dict[str, str],
) -> dict[str, Any]:
    language_counts: dict[str, int] = {}
    routes: list[str] = []
    frameworks: set[str] = set()
    evidence_files: list[str] = []
    data_hints: set[str] = set()
    integration_hints: set[str] = set()
    function_hints: set[str] = set()
    input_hints: set[str] = set()
    output_hints: set[str] = set()
    table_hints: set[str] = set()

    ordered_paths = [str(item.get("path", "")).strip() for item in file_entries if isinstance(item, dict)]
    for path in ordered_paths:
        if path:
            lang = _guess_language(path)
            language_counts[lang] = language_counts.get(lang, 0) + 1

    for path, text in file_contents.items():
        if not text:
            continue
        evidence_files.append(path)
        routes.extend(_extract_routes_from_text(path, text))
        for fw in _framework_signals(text):
            frameworks.add(fw)
        lowered = text.lower()
        if any(token in lowered for token in ["select ", "insert ", "update ", "delete ", "from "]):
            data_hints.add("Repository contains SQL/data access logic")
        if any(token in lowered for token in ["redis", "postgres", "mysql", "mongodb", "sqlalchemy", "gorm", "sequelize"]):
            data_hints.add("Application appears to use persistent storage backends")
        if any(token in lowered for token in ["kafka", "rabbitmq", "sns", "sqs", "pubsub", "webhook"]):
            integration_hints.add("Event-driven/message queue integration detected")
        if any(token in lowered for token in ["stripe", "twilio", "sendgrid", "slack", "salesforce"]):
            integration_hints.add("Third-party SaaS/API integrations detected")
        for match in re.findall(r'(?im)^\s*(?:public|private|protected)?\s*(?:function|sub|def)\s+([a-zA-Z_][a-zA-Z0-9_]*)', text):
            if len(function_hints) < 40:
                function_hints.add(str(match))
        for match in re.findall(r'(?i)request\.(?:querystring|form)\(\s*"([^"]+)"\s*\)', text):
            if len(input_hints) < 20:
                input_hints.add(str(match))
        for match in re.findall(r'(?i)request\.(?:querystring|form)\(\s*\'([^\']+)\'\s*\)', text):
            if len(input_hints) < 20:
                input_hints.add(str(match))
        if "request(" in lowered or "request." in lowered:
            input_hints.add("Request parameters are consumed from HTTP input context")
        if "response.write" in lowered or "response.redirect" in lowered:
            output_hints.add("Response output appears to be rendered directly from server-side code")
        for match in re.findall(r'(?i)\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)', text):
            if len(table_hints) < 20:
                table_hints.add(str(match))
        for match in re.findall(r'(?i)\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)', text):
            if len(table_hints) < 20:
                table_hints.add(str(match))

    sample_paths = [p for p in ordered_paths if p][:160]
    capabilities = _domain_capabilities(sample_paths, routes)
    if not capabilities:
        capabilities = [
            "Service exposes business workflows through API/service modules",
            "Repository structure suggests layered application components",
        ]

    objectives_line = str(objectives or "").strip()
    overview = (
        f"Analyst inference for {repo_label}: repository structure and sampled source files indicate "
        f"{' ,'.join(sorted(frameworks)) if frameworks else 'a multi-module application'} "
        f"with {len(routes)} discovered route hints."
    )
    if objectives_line:
        overview += f" Objective alignment used: {objectives_line[:220]}"

    component_candidates = []
    for path in sample_paths[:180]:
        parts = path.split("/")
        if len(parts) >= 2:
            component_candidates.append("/".join(parts[:2]))
        elif parts:
            component_candidates.append(parts[0])
    key_components = sorted({c for c in component_candidates if c})[:10]

    io_contracts = []
    if routes:
        io_contracts.append("HTTP endpoints likely accept JSON payloads and return JSON/API responses.")
    if frameworks:
        io_contracts.append(f"Frameworks detected ({', '.join(sorted(frameworks))}) imply controller/handler based request processing.")
    if input_hints:
        io_contracts.append(f"Input parameters inferred from request context: {', '.join(sorted(input_hints)[:8])}.")
    if output_hints:
        io_contracts.append("; ".join(sorted(output_hints)[:4]))
    if not io_contracts:
        io_contracts.append("Source modules suggest internal function inputs/outputs with service-level orchestration.")

    unknowns = []
    if len(file_contents) < 3:
        unknowns.append("Limited file content available; summary based mostly on folder/file names.")
    if not routes:
        unknowns.append("No explicit route decorators found in sampled files; interfaces may be indirect or in unsampled modules.")

    return {
        "overview": overview,
        "likely_capabilities": capabilities,
        "input_output_contracts": io_contracts,
        "key_components": key_components,
        "interfaces": sorted(set(routes))[:20],
        "data_and_state": sorted(data_hints)[:6] or ["Data access details require deeper scan for full confidence."],
        "domain_functions": sorted(function_hints)[:12],
        "data_entities": sorted(table_hints)[:12],
        "integrations": sorted(integration_hints)[:6] or ["No explicit external integration tokens detected in sampled files."],
        "unknowns": unknowns,
        "evidence_files": evidence_files[:14],
        "stats": {
            "sampled_tree_entries": len(file_entries),
            "sampled_files": len(file_contents),
            "languages": language_counts,
            "route_hints": len(routes),
        },
    }


def _fetch_github_file_content(
    *,
    base_url: str,
    owner: str,
    repository: str,
    path: str,
    ref: str,
    headers: dict[str, str],
    max_chars: int = 12000,
) -> str:
    content_url = f"{base_url}/repos/{quote(owner)}/{quote(repository)}/contents/{quote(path, safe='/')}?ref={quote(ref, safe='')}"
    payload = _http_json_request(content_url, headers=headers)
    if not isinstance(payload, dict):
        return ""
    raw_content = str(payload.get("content", "") or "")
    encoding = str(payload.get("encoding", "")).strip().lower()
    if not raw_content:
        return ""
    text = ""
    if encoding == "base64":
        try:
            text = base64.b64decode(raw_content.encode("utf-8"), validate=False).decode("utf-8", errors="replace")
        except Exception:
            text = ""
    else:
        text = raw_content
    return text[:max_chars]


def _allowed_source_extensions() -> set[str]:
    return {
        ".py", ".js", ".ts", ".tsx", ".go", ".java", ".cs", ".rb", ".php",
        ".asp", ".aspx", ".asa", ".vb", ".vbs",
        ".sql", ".md", ".yaml", ".yml", ".json",
    }


def _select_source_entries_for_analysis(
    raw_entries: list[dict[str, Any]],
    *,
    include_paths: list[str] | None = None,
    exclude_paths: list[str] | None = None,
    limit: int = 28,
) -> list[dict[str, Any]]:
    include_paths = include_paths or []
    exclude_paths = exclude_paths or []
    allowed_ext = _allowed_source_extensions()
    candidate_entries: list[dict[str, Any]] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path", "")).strip()
        node_type = str(item.get("type", "")).strip().lower()
        if not path or node_type != "blob":
            continue
        normalized = path.replace("\\", "/")
        if include_paths and not any(normalized.startswith(prefix + "/") or normalized == prefix for prefix in include_paths):
            continue
        if exclude_paths and any(normalized.startswith(prefix + "/") or normalized == prefix for prefix in exclude_paths):
            continue
        suffix = Path(normalized).suffix.lower()
        base_name = Path(normalized).name.lower()
        if suffix in allowed_ext or base_name.startswith("readme"):
            candidate_entries.append({"path": normalized, "type": "file", "depth": normalized.count("/")})

    priority_rank = []
    for entry in candidate_entries:
        path = entry["path"].lower()
        rank = 99
        if "readme" in path:
            rank = 0
        elif any(tok in path for tok in ["/main.", "/app.", "/index.", "/server."]):
            rank = 1
        elif "/api/" in path or "/controller" in path or "/routes" in path:
            rank = 2
        elif "/service" in path or "/handler" in path:
            rank = 3
        elif "/model" in path or "/schema" in path:
            rank = 4
        priority_rank.append((rank, path, entry))
    priority_rank.sort(key=lambda row: (row[0], row[1]))
    return [row[2] for row in priority_rank[: max(1, limit)]]


def _compose_legacy_code_bundle(file_contents: dict[str, str], max_total_chars: int = 160000) -> str:
    chunks: list[str] = []
    total = 0
    for path in sorted(file_contents.keys()):
        content = str(file_contents.get(path, "") or "")
        if not content:
            continue
        block = f"\n### FILE: {path}\n{content}\n"
        if total + len(block) > max_total_chars:
            remaining = max_total_chars - total
            if remaining <= 200:
                break
            block = block[:remaining]
        chunks.append(block)
        total += len(block)
        if total >= max_total_chars:
            break
    return "".join(chunks).strip()


def _config_from_payload(payload: dict[str, Any]) -> PipelineConfig:
    provider_raw = str(payload.get("provider", "anthropic")).strip().lower()
    if provider_raw not in {"anthropic", "openai"}:
        raise ValueError("provider must be 'anthropic' or 'openai'")

    provider = LLMProvider.ANTHROPIC if provider_raw == "anthropic" else LLMProvider.OPENAI
    model = str(payload.get("model", "")).strip()
    api_key = str(payload.get("api_key", "")).strip()
    if not api_key:
        try:
            llm_cfg = SETTINGS_STORE.resolve_llm_credentials(provider_raw, requested_model=model)
        except ValueError:
            llm_cfg = {"api_key": "", "model": model}
        api_key = str(llm_cfg.get("api_key", "")).strip()
        if not model:
            model = str(llm_cfg.get("model", "")).strip()
    if not api_key:
        raise ValueError("api_key is required. Save it in Settings > LLM credentials.")

    if not model:
        model = "claude-sonnet-4-20250514" if provider == LLMProvider.ANTHROPIC else "gpt-4o"

    live_deploy = bool(payload.get("live_deploy", True))
    deployment_target = str(payload.get("deployment_target", "local")).strip().lower()
    if deployment_target not in {"local", "cloud"}:
        raise ValueError("deployment_target must be 'local' or 'cloud'")

    return PipelineConfig(
        provider=provider,
        anthropic_api_key=api_key if provider == LLMProvider.ANTHROPIC else "",
        openai_api_key=api_key if provider == LLMProvider.OPENAI else "",
        anthropic_model=model if provider == LLMProvider.ANTHROPIC else "claude-sonnet-4-20250514",
        openai_model=model if provider == LLMProvider.OPENAI else "gpt-4o",
        temperature=float(payload.get("temperature", 0.3)),
        developer_parallel_agents=int(payload.get("parallel_agents", 5)),
        max_retries=int(payload.get("max_retries", 2)),
        live_deploy=live_deploy,
        deploy_output_dir=str(payload.get("deploy_output_dir", "./deploy_output")),
        cluster_name=str(payload.get("cluster_name", "agent-pipeline")),
        namespace=str(payload.get("namespace", "agent-app")),
    )


def _read_json_file(path: Path) -> Any:
    if not path.exists() or not path.is_file():
        return None
    try:
        return json.loads(path.read_text())
    except Exception:
        return None


def _write_json_file(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=True, default=str))


def _resolve_context_reference(payload: dict[str, Any], query: dict[str, Any] | None = None) -> tuple[dict[str, Any] | None, str]:
    query = query or {}
    run_id = str(payload.get("run_id") or query.get("run_id") or "").strip()
    ref: dict[str, Any] = {}

    if run_id:
        run = MANAGER.get_run(run_id)
        if not run:
            return None, f"run not found: {run_id}"
        state = run.get("pipeline_state", {}) if isinstance(run, dict) else {}
        ref = state.get("context_vault_ref", {}) if isinstance(state, dict) else {}
        if not isinstance(ref, dict) or not ref.get("version_id"):
            return None, f"context_vault_ref missing for run: {run_id}"
        ref = dict(ref)
        ref["run_id"] = run_id
        return ref, ""

    ref_payload = payload.get("context_reference", payload.get("context_vault_ref", {}))
    if isinstance(ref_payload, dict):
        ref = dict(ref_payload)

    for key in ("repo", "branch", "commit_sha", "version_id", "vault_path"):
        value = str(payload.get(key, query.get(key, "")) or "").strip()
        if value:
            ref[key] = value

    if ref.get("version_id") and not ref.get("vault_path"):
        target_version = str(ref.get("version_id", "")).strip()
        candidates = list_versions(CONTEXT_GRAPH_DB, limit=600)
        for row in candidates:
            if str(row.get("version_id", "")) != target_version:
                continue
            if ref.get("repo") and str(row.get("repo", "")) != str(ref.get("repo", "")):
                continue
            if ref.get("branch") and str(row.get("branch", "")) != str(ref.get("branch", "")):
                continue
            ref.setdefault("repo", row.get("repo", ""))
            ref.setdefault("branch", row.get("branch", ""))
            ref.setdefault("commit_sha", row.get("commit_sha", ""))
            ref.setdefault("vault_path", row.get("vault_path", ""))
            break

    if not ref.get("version_id"):
        repo = str(ref.get("repo", "")).strip()
        branch = str(ref.get("branch", "")).strip()
        latest = list_versions(CONTEXT_GRAPH_DB, repo=repo, branch=branch, limit=1)
        if latest:
            row = latest[0]
            ref.setdefault("repo", row.get("repo", ""))
            ref.setdefault("branch", row.get("branch", ""))
            ref.setdefault("commit_sha", row.get("commit_sha", ""))
            ref.setdefault("vault_path", row.get("vault_path", ""))
            ref["version_id"] = row.get("version_id", "")

    if not ref.get("version_id"):
        return None, "context version not resolvable (provide run_id or context_reference)"
    return ref, ""


def _load_context_artifacts(context_ref: dict[str, Any]) -> dict[str, Any]:
    raw_vault = str(context_ref.get("vault_path", "")).strip()
    if not raw_vault:
        return {}
    vault_path = Path(raw_vault)
    if not vault_path.exists() or not vault_path.is_dir():
        return {}
    artifacts = {
        "scm": _read_json_file(vault_path / "scm.json") or {},
        "convention_profile": _read_json_file(vault_path / "convention_profile.json") or {},
        "health_assessment": _read_json_file(vault_path / "health_assessment.json") or {},
        "remediation_backlog": _read_json_file(vault_path / "remediation_backlog.json") or [],
        "manifest": _read_json_file(vault_path / "manifest.json") or {},
    }
    contract_root = vault_path / "contract_bundle"
    if contract_root.exists() and contract_root.is_dir():
        artifacts.update(
            {
                "contract_system_context_model": _read_json_file(contract_root / "system_context_model.json") or {},
                "contract_convention_profile": _read_json_file(contract_root / "convention_profile.json") or {},
                "contract_health_assessment_bundle": _read_json_file(contract_root / "health_assessment_bundle.json") or {},
                "context_bundle": _read_json_file(contract_root / "context_bundle.json") or {},
                "contract_validation_report": _read_json_file(contract_root / "validation_report.json") or {},
            }
        )
    return artifacts


def _persist_context_report(context_ref: dict[str, Any], filename_prefix: str, payload: dict[str, Any]) -> str:
    raw_vault = str(context_ref.get("vault_path", "")).strip()
    if not raw_vault:
        return ""
    vault_path = Path(raw_vault)
    if not vault_path.exists() or not vault_path.is_dir():
        return ""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = vault_path / f"{filename_prefix}_{stamp}.json"
    _write_json_file(path, payload)
    return str(path)


def _persist_branch_drift_report(report: dict[str, Any]) -> str:
    repo = str(report.get("repo", "unknown")).strip() or "unknown"
    branch = str(report.get("branch", "unknown")).strip() or "unknown"
    base = CONTEXT_VAULT_ROOT / repo / branch / "_drift"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    path = base / f"drift_report_{stamp}.json"
    _write_json_file(path, report)
    return str(path)


async def api_health(_request):
    return JSONResponse({"ok": True, "status": "healthy"})


async def api_samples(_request):
    return JSONResponse(
        {
            "ok": True,
            "focused": SAMPLE_OBJECTIVES_FOCUSED,
            "broad": SAMPLE_OBJECTIVES_BROAD,
            "combined": {**SAMPLE_OBJECTIVES_FOCUSED, **SAMPLE_OBJECTIVES_BROAD},
            "agents": AGENT_CARDS,
        }
    )


def _request_actor(request) -> str:
    actor = str(request.headers.get("x-user-email", "")).strip().lower()
    if actor:
        return actor
    actor = str(request.headers.get("x-user", "")).strip()
    if actor:
        return actor
    return "local-user"


async def api_get_settings(_request):
    return JSONResponse({"ok": True, "settings": SETTINGS_STORE.get_settings()})


async def api_connect_integration(request):
    provider = request.path_params.get("provider", "")
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.update_integration(provider, payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    integration = settings.get("integrations", {}).get(str(provider).strip().lower(), {})
    return JSONResponse({"ok": True, "settings": settings, "integration": integration})


async def api_test_integration(request):
    provider = request.path_params.get("provider", "")
    payload = _get_json(await request.body())
    actor = _request_actor(request)
    try:
        # Allow "Test" to validate currently-entered form values (including secrets)
        # without forcing a separate "Connect" click first.
        if isinstance(payload, dict) and payload:
            SETTINGS_STORE.update_integration(provider, payload, actor=actor)
        result = SETTINGS_STORE.test_integration(provider, actor=actor)
        if str(provider).strip().lower() == "github":
            github_cfg = SETTINGS_STORE.get_integration_config("github")
            live_checks = _github_live_integration_checks(github_cfg)
            if live_checks:
                existing = result.get("checks", [])
                if not isinstance(existing, list):
                    existing = []
                existing.extend(live_checks)
                result["checks"] = existing
                result["test_ok"] = bool(result.get("test_ok", False)) and all(
                    bool(item.get("ok")) for item in live_checks
                )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, **result})


async def api_disconnect_integration(request):
    provider = request.path_params.get("provider", "")
    payload = _get_json(await request.body())
    clear_secret = bool(payload.get("clear_secret", False))
    try:
        settings = SETTINGS_STORE.disconnect_integration(
            provider,
            clear_secret=clear_secret,
            actor=_request_actor(request),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    integration = settings.get("integrations", {}).get(str(provider).strip().lower(), {})
    return JSONResponse({"ok": True, "settings": settings, "integration": integration})


async def api_connect_llm_provider(request):
    provider = request.path_params.get("provider", "")
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.update_llm_provider(provider, payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    llm_provider = settings.get("llm", {}).get("providers", {}).get(str(provider).strip().lower(), {})
    return JSONResponse({"ok": True, "settings": settings, "llm_provider": llm_provider})


async def api_test_llm_provider(request):
    provider = request.path_params.get("provider", "")
    payload = _get_json(await request.body())
    actor = _request_actor(request)
    try:
        if isinstance(payload, dict) and payload:
            SETTINGS_STORE.update_llm_provider(provider, payload, actor=actor)
        result = SETTINGS_STORE.test_llm_provider(provider, actor=actor)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, **result})


async def api_disconnect_llm_provider(request):
    provider = request.path_params.get("provider", "")
    payload = _get_json(await request.body())
    clear_secret = bool(payload.get("clear_secret", False))
    try:
        settings = SETTINGS_STORE.disconnect_llm_provider(
            provider,
            clear_secret=clear_secret,
            actor=_request_actor(request),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    llm_provider = settings.get("llm", {}).get("providers", {}).get(str(provider).strip().lower(), {})
    return JSONResponse({"ok": True, "settings": settings, "llm_provider": llm_provider})


async def api_discover_github_tree(request):
    payload = _get_json(await request.body())
    integration_ctx = _extract_integration_context(payload)
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    sample_mode = bool(integration_ctx.get("sample_dataset_enabled", False) or payload.get("sample_dataset_enabled", False))

    repo_url = str(payload.get("repo_url") or brownfield.get("repo_url") or "").strip()
    branch = str(payload.get("branch", "")).strip()
    path_prefix = str(payload.get("path_prefix", "")).strip().strip("/")
    max_entries = max(50, min(5000, int(payload.get("max_entries", 1200) or 1200)))

    github_cfg = SETTINGS_STORE.get_integration_config("github")
    owner = str(payload.get("owner") or github_cfg.get("owner") or "").strip()
    repository = str(payload.get("repository") or github_cfg.get("repository") or "").strip()
    parsed_owner, parsed_repo = _parse_github_repo_url(repo_url)
    owner = parsed_owner or owner
    repository = parsed_repo or repository

    if not owner or not repository:
        return JSONResponse(
            {
                "ok": False,
                "error": "Unable to determine GitHub owner/repository. Provide a valid repo URL like https://github.com/org/repo.",
            },
            status_code=400,
        )

    if sample_mode:
        return JSONResponse({"ok": True, **_sample_github_tree(owner, repository)})

    token = str(github_cfg.get("token", "")).strip()
    if not token:
        return JSONResponse(
            {
                "ok": False,
                "error": "GitHub token is required. Save it in Settings > Integrations > GitHub, then retry.",
            },
            status_code=400,
        )

    base_url = str(github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "synthetix-discover/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        repo_meta = _http_json_request(f"{base_url}/repos/{quote(owner)}/{quote(repository)}", headers=headers)
        if not isinstance(repo_meta, dict):
            raise ValueError("invalid repository metadata")
        if not branch:
            branch = str(repo_meta.get("default_branch") or "main")
        tree_payload = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/trees/{quote(branch, safe='')}?recursive=1",
            headers=headers,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": f"GitHub fetch failed: {exc}"}, status_code=400)

    if not isinstance(tree_payload, dict):
        return JSONResponse({"ok": False, "error": "GitHub tree response is invalid."}, status_code=400)
    tree_entries = tree_payload.get("tree", [])
    if not isinstance(tree_entries, list):
        tree_entries = []

    entries: list[dict[str, Any]] = []
    for item in tree_entries:
        if not isinstance(item, dict):
            continue
        raw_path = str(item.get("path", "")).strip()
        if not raw_path:
            continue
        if path_prefix and not raw_path.startswith(path_prefix):
            continue
        node_type = "dir" if str(item.get("type", "")).strip().lower() == "tree" else "file"
        entries.append(
            {
                "path": raw_path,
                "type": node_type,
                "depth": raw_path.count("/"),
                "size": int(item.get("size", 0) or 0),
            }
        )

    entries.sort(key=lambda row: (row.get("depth", 0), row.get("path", "")))
    if len(entries) > max_entries:
        entries = entries[:max_entries]

    folder_count = sum(1 for row in entries if row.get("type") == "dir")
    file_count = sum(1 for row in entries if row.get("type") == "file")
    return JSONResponse(
        {
            "ok": True,
            "repo": {"owner": owner, "repository": repository, "default_branch": branch, "url": repo_url},
            "tree": {
                "entries": entries,
                "total_entries": len(entries),
                "truncated": bool(tree_payload.get("truncated", False)),
                "folders": folder_count,
                "files": file_count,
                "source": "github_api",
            },
        }
    )


async def api_discover_analyst_brief(request):
    payload = _get_json(await request.body())
    integration_ctx = _extract_integration_context(payload)
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    sample_mode = bool(integration_ctx.get("sample_dataset_enabled", False) or payload.get("sample_dataset_enabled", False))

    objectives = str(payload.get("objectives", "")).strip()
    use_case = str(payload.get("use_case", "business_objectives")).strip().lower() or "business_objectives"
    legacy_code = str(payload.get("legacy_code", "")).strip()
    repo_provider = str(payload.get("repo_provider") or brownfield.get("repo_provider") or "").strip().lower()
    repo_url = str(payload.get("repo_url") or brownfield.get("repo_url") or "").strip()
    include_paths = _normalize_lines(integration_ctx.get("scan_scope", {}).get("include_paths", []))
    exclude_paths = _normalize_lines(integration_ctx.get("scan_scope", {}).get("exclude_paths", []))

    # Legacy modernization path: analyze provided code directly.
    if legacy_code and use_case == "code_modernization":
        file_entries = [{"path": "inline/legacy_code.txt", "type": "file", "depth": 1}]
        file_contents = {"inline/legacy_code.txt": legacy_code[:25000]}
        analysis = _analyze_source_bundle(
            objectives=objectives,
            repo_label="provided legacy code",
            file_entries=file_entries,
            file_contents=file_contents,
        )
        return JSONResponse(
            {
                "ok": True,
                "source": "inline_legacy_code",
                "analyst_brief": {
                    "title": "Analyst functionality understanding",
                    "summary": analysis,
                },
            }
        )

    if repo_provider and repo_provider != "github" and not sample_mode:
        return JSONResponse(
            {
                "ok": False,
                "error": f"Analyst code preview currently supports GitHub repos in Discover. Selected provider: {repo_provider}.",
            },
            status_code=400,
        )

    owner = str(payload.get("owner", "")).strip()
    repository = str(payload.get("repository", "")).strip()
    parsed_owner, parsed_repo = _parse_github_repo_url(repo_url)
    owner = parsed_owner or owner
    repository = parsed_repo or repository
    if not owner or not repository:
        return JSONResponse(
            {
                "ok": False,
                "error": "Repo URL is required for analyst code understanding. Provide a valid GitHub URL in Discover Connect.",
            },
            status_code=400,
        )

    if sample_mode:
        sample_tree = _sample_github_tree(owner, repository)
        entries = sample_tree.get("tree", {}).get("entries", []) if isinstance(sample_tree.get("tree", {}), dict) else []
        if not isinstance(entries, list):
            entries = []
        sample_contents = {
            "services/orders-service/src/main.java": "@RestController\n@RequestMapping(\"/v1/orders\")\npublic class OrdersController {}",
            "services/payments-service/cmd/api/main.go": "router.POST(\"/v1/payments\", handlePayments)\n// uses redis idempotency",
            "services/inventory-service/index.js": "app.post('/v1/inventory/reserve', reserveInventory)",
            "legacy/billing-monolith/README.md": "Legacy billing and invoicing flow.",
        }
        analysis = _analyze_source_bundle(
            objectives=objectives,
            repo_label=f"{owner}/{repository}",
            file_entries=[item for item in entries if isinstance(item, dict)],
            file_contents=sample_contents,
        )
        return JSONResponse(
            {
                "ok": True,
                "source": "sample_dataset",
                "repo": {"owner": owner, "repository": repository, "default_branch": sample_tree.get("repo", {}).get("default_branch", "main")},
                "analyst_brief": {
                    "title": "Analyst functionality understanding",
                    "summary": analysis,
                },
            }
        )

    github_cfg = SETTINGS_STORE.get_integration_config("github")
    token = str(github_cfg.get("token", "")).strip()
    if not token:
        return JSONResponse(
            {
                "ok": False,
                "error": "GitHub token is required for analyst code understanding. Save it in Settings > Integrations > GitHub.",
            },
            status_code=400,
        )

    base_url = str(github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "synthetix-discover/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }

    try:
        repo_meta = _http_json_request(f"{base_url}/repos/{quote(owner)}/{quote(repository)}", headers=headers)
        if not isinstance(repo_meta, dict):
            raise ValueError("invalid repository metadata")
        branch = str(payload.get("branch", "")).strip() or str(repo_meta.get("default_branch") or "main")
        tree_payload = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/trees/{quote(branch, safe='')}?recursive=1",
            headers=headers,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": f"GitHub fetch failed: {exc}"}, status_code=400)

    if not isinstance(tree_payload, dict):
        return JSONResponse({"ok": False, "error": "GitHub tree response is invalid."}, status_code=400)

    raw_entries = tree_payload.get("tree", [])
    if not isinstance(raw_entries, list):
        raw_entries = []

    selected_entries = _select_source_entries_for_analysis(
        [item for item in raw_entries if isinstance(item, dict)],
        include_paths=include_paths,
        exclude_paths=exclude_paths,
        limit=28,
    )

    file_contents: dict[str, str] = {}
    fetch_errors: list[str] = []
    for entry in selected_entries:
        path = str(entry.get("path", "")).strip()
        if not path:
            continue
        try:
            content = _fetch_github_file_content(
                base_url=base_url,
                owner=owner,
                repository=repository,
                path=path,
                ref=branch,
                headers=headers,
                max_chars=12000,
            )
            if content:
                file_contents[path] = content
        except ValueError as exc:
            if len(fetch_errors) < 5:
                fetch_errors.append(f"{path}: {exc}")

    analysis = _analyze_source_bundle(
        objectives=objectives,
        repo_label=f"{owner}/{repository}",
        file_entries=selected_entries,
        file_contents=file_contents,
    )
    if fetch_errors:
        analysis.setdefault("unknowns", []).append(
            f"Some files could not be read from GitHub API: {' | '.join(fetch_errors)}"
        )

    return JSONResponse(
        {
            "ok": True,
            "source": "github_api",
            "repo": {"owner": owner, "repository": repository, "default_branch": branch, "url": repo_url},
            "analyst_brief": {
                "title": "Analyst functionality understanding",
                "summary": analysis,
            },
        }
    )


def _discover_linear_issues_response(payload: dict[str, Any]) -> JSONResponse:
    integration_ctx = _extract_integration_context(payload)
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    sample_mode = bool(integration_ctx.get("sample_dataset_enabled", False) or payload.get("sample_dataset_enabled", False))

    issue_board = str(payload.get("issue_project") or brownfield.get("issue_project") or "").strip()
    max_issues = max(5, min(200, int(payload.get("max_issues", 60) or 60)))

    linear_cfg = SETTINGS_STORE.get_integration_config("linear")
    fallback_team = str(linear_cfg.get("team_key", "")).strip()
    team_key = str(payload.get("team_key", "")).strip()
    project_filter = str(payload.get("project_filter", "")).strip()
    if not team_key:
        parsed_team, parsed_project = _parse_linear_board(issue_board, fallback_team=fallback_team)
        team_key = parsed_team
        if not project_filter:
            project_filter = parsed_project

    if not team_key and not sample_mode:
        return JSONResponse(
            {"ok": False, "error": "Linear team key is required. Set it in Settings > Integrations > Linear or include Team/Board details."},
            status_code=400,
        )

    if sample_mode:
        return JSONResponse({"ok": True, **_sample_linear_issues(team_key, project_filter), "provider": "linear"})

    token = str(linear_cfg.get("api_token", "")).strip()
    if not token:
        return JSONResponse(
            {
                "ok": False,
                "error": "Linear API token is required. Save it in Settings > Integrations > Linear, then retry.",
            },
            status_code=400,
        )

    base_url = str(linear_cfg.get("base_url") or "https://api.linear.app").rstrip("/")
    graphql_url = base_url if base_url.endswith("/graphql") else f"{base_url}/graphql"
    headers = {
        "Authorization": token,
        "Content-Type": "application/json",
        "User-Agent": "synthetix-discover/1.0",
    }

    def _linear_request(query: str, variables: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
        response: dict[str, Any] | list[Any]
        response = _http_json_request(
            graphql_url,
            method="POST",
            headers=headers,
            payload={"query": query, "variables": variables},
        )
        if not isinstance(response, dict):
            return {}, ["invalid response payload"]
        errors = response.get("errors", [])
        messages: list[str] = []
        if isinstance(errors, list):
            for item in errors:
                if isinstance(item, dict):
                    msg = str(item.get("message", "")).strip()
                    if msg:
                        messages.append(msg)
        data = response.get("data", {})
        if not isinstance(data, dict):
            data = {}
        return data, messages

    team_query_viewer = """
query DiscoverLinearTeamsViewer($first: Int!) {
  viewer {
    teams(first: $first) {
      nodes {
        id
        key
        name
      }
    }
  }
}
""".strip()
    team_query_root = """
query DiscoverLinearTeamsRoot($first: Int!) {
  teams(first: $first) {
    nodes {
      id
      key
      name
    }
  }
}
""".strip()
    issues_query = """
query DiscoverLinearIssuesByTeamId($teamId: String!, $first: Int!) {
  team(id: $teamId) {
    id
    key
    name
    issues(first: $first) {
      nodes {
        id
        identifier
        title
        priority
        updatedAt
        state { name type }
        assignee { name email }
        project { id name }
      }
    }
  }
}
""".strip()

    team_id = str(payload.get("team_id", "")).strip()
    team: dict[str, Any] = {}
    aggregated_errors: list[str] = []

    if not team_id:
        # Resolve key/name to ID by listing accessible teams.
        teams_nodes: list[dict[str, Any]] = []
        for team_lookup_query in (team_query_viewer, team_query_root):
            try:
                team_data, team_errors = _linear_request(team_lookup_query, {"first": 250})
            except ValueError as exc:
                aggregated_errors.append(str(exc))
                continue
            if team_errors:
                aggregated_errors.extend(team_errors)
                continue
            # viewer.teams.nodes
            viewer = team_data.get("viewer", {}) if isinstance(team_data.get("viewer", {}), dict) else {}
            viewer_nodes = viewer.get("teams", {}).get("nodes", []) if isinstance(viewer.get("teams", {}), dict) else []
            root_nodes = team_data.get("teams", {}).get("nodes", []) if isinstance(team_data.get("teams", {}), dict) else []
            for collection in (viewer_nodes, root_nodes):
                if isinstance(collection, list):
                    teams_nodes.extend(item for item in collection if isinstance(item, dict))
            if teams_nodes:
                break

        normalized_key = team_key.strip().lower()
        for node in teams_nodes:
            key_value = str(node.get("key", "")).strip().lower()
            if key_value and key_value == normalized_key:
                team_id = str(node.get("id", "")).strip()
                team = node
                break
        if not team_id:
            for node in teams_nodes:
                name_value = str(node.get("name", "")).strip().lower()
                if name_value and normalized_key and normalized_key in name_value:
                    team_id = str(node.get("id", "")).strip()
                    team = node
                    break

    if not team_id:
        detail = ""
        if aggregated_errors:
            detail = f" GraphQL errors: {' | '.join(aggregated_errors[:3])}"
        return JSONResponse(
            {
                "ok": False,
                "error": f"No Linear team found for `{team_key}`. Use a valid team key in Settings/Discover (e.g., ENG, ACME), or provide team_id.{detail}",
            },
            status_code=400,
        )

    try:
        data, errors = _linear_request(issues_query, {"teamId": team_id, "first": max_issues})
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": f"Linear fetch failed: {exc}"}, status_code=400)
    if errors:
        return JSONResponse({"ok": False, "error": f"Linear query failed: {' | '.join(errors)}"}, status_code=400)

    team = data.get("team", {}) if isinstance(data, dict) else {}
    if not isinstance(team, dict) or not team:
        return JSONResponse({"ok": False, "error": f"Linear team `{team_key}` resolved to id `{team_id}` but returned no team data."}, status_code=400)
    issues_nodes = team.get("issues", {}).get("nodes", []) if isinstance(team.get("issues", {}), dict) else []
    if not isinstance(issues_nodes, list):
        issues_nodes = []

    normalized_issues: list[dict[str, Any]] = []
    token_filter = project_filter.lower()
    for item in issues_nodes:
        if not isinstance(item, dict):
            continue
        project_name = ""
        project_data = item.get("project", {})
        if isinstance(project_data, dict):
            project_name = str(project_data.get("name", "")).strip()
        issue = {
            "id": str(item.get("id", "")),
            "identifier": str(item.get("identifier", "")),
            "title": str(item.get("title", "")),
            "state": str(item.get("state", {}).get("name", "")) if isinstance(item.get("state", {}), dict) else "",
            "priority": int(item.get("priority", 0) or 0),
            "assignee": str(item.get("assignee", {}).get("name", "")) if isinstance(item.get("assignee", {}), dict) else "",
            "project": project_name,
            "updated_at": str(item.get("updatedAt", "")),
        }
        if token_filter:
            haystack = " ".join([issue["identifier"], issue["title"], issue["project"]]).lower()
            if token_filter not in haystack:
                continue
        normalized_issues.append(issue)

    return JSONResponse(
        {
            "ok": True,
            "provider": "linear",
            "team": {"id": str(team.get("id", "")), "key": str(team.get("key", "")), "name": str(team.get("name", ""))},
            "issues": normalized_issues,
            "total_issues": len(normalized_issues),
            "source": "linear_api",
            "filters": {"project_filter": project_filter},
        }
    )


def _discover_jira_issues_response(payload: dict[str, Any]) -> JSONResponse:
    integration_ctx = _extract_integration_context(payload)
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    sample_mode = bool(integration_ctx.get("sample_dataset_enabled", False) or payload.get("sample_dataset_enabled", False))

    issue_board = str(payload.get("issue_project") or brownfield.get("issue_project") or "").strip()
    max_issues = max(5, min(200, int(payload.get("max_issues", 60) or 60)))

    jira_cfg = SETTINGS_STORE.get_integration_config("jira")
    project_key = str(payload.get("project_key") or jira_cfg.get("project_key") or "").strip()
    if not project_key and issue_board:
        project_key = issue_board.split("/", 1)[0].strip()
    if not project_key and not sample_mode:
        return JSONResponse(
            {"ok": False, "error": "Jira project key is required. Set it in Settings > Integrations > Jira or include Issue project/board."},
            status_code=400,
        )

    if sample_mode:
        return JSONResponse({"ok": True, **_sample_jira_issues(project_key), "provider": "jira"})

    email = str(jira_cfg.get("email", "")).strip()
    api_token = str(jira_cfg.get("api_token", "")).strip()
    if not email or not api_token:
        return JSONResponse(
            {"ok": False, "error": "Jira credentials are required (email + API token). Save them in Settings > Integrations > Jira."},
            status_code=400,
        )

    base_url = str(jira_cfg.get("base_url") or "").strip().rstrip("/")
    if not base_url:
        return JSONResponse({"ok": False, "error": "Jira base URL is required."}, status_code=400)

    jql = str(payload.get("jql", "")).strip()
    if not jql:
        jql = f"project={project_key} ORDER BY updated DESC"

    query = urlencode(
        {
            "jql": jql,
            "maxResults": max_issues,
            "fields": "summary,status,priority,assignee,project,updated",
        }
    )
    search_url = f"{base_url}/rest/api/3/search?{query}"
    auth = base64.b64encode(f"{email}:{api_token}".encode("utf-8")).decode("ascii")
    headers = {
        "Accept": "application/json",
        "Authorization": f"Basic {auth}",
        "User-Agent": "synthetix-discover/1.0",
    }
    try:
        response = _http_json_request(search_url, headers=headers)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": f"Jira fetch failed: {exc}"}, status_code=400)

    if not isinstance(response, dict):
        return JSONResponse({"ok": False, "error": "Jira response is invalid."}, status_code=400)

    raw_issues = response.get("issues", [])
    if not isinstance(raw_issues, list):
        raw_issues = []

    priority_rank = {"highest": 1, "high": 2, "medium": 3, "low": 4, "lowest": 5}
    normalized_issues: list[dict[str, Any]] = []
    for item in raw_issues:
        if not isinstance(item, dict):
            continue
        fields = item.get("fields", {}) if isinstance(item.get("fields", {}), dict) else {}
        status = fields.get("status", {}) if isinstance(fields.get("status", {}), dict) else {}
        priority = fields.get("priority", {}) if isinstance(fields.get("priority", {}), dict) else {}
        assignee = fields.get("assignee", {}) if isinstance(fields.get("assignee", {}), dict) else {}
        project = fields.get("project", {}) if isinstance(fields.get("project", {}), dict) else {}
        priority_name = str(priority.get("name", "")).strip()
        normalized_issues.append(
            {
                "id": str(item.get("id", "")),
                "identifier": str(item.get("key", "")),
                "title": str(fields.get("summary", "")),
                "state": str(status.get("name", "")),
                "priority": priority_rank.get(priority_name.lower(), 0),
                "assignee": str(assignee.get("displayName", "")),
                "project": str(project.get("name", "") or project_key),
                "updated_at": str(fields.get("updated", "")),
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "provider": "jira",
            "team": {"id": "", "key": project_key, "name": project_key},
            "issues": normalized_issues,
            "total_issues": len(normalized_issues),
            "source": "jira_api",
            "filters": {"jql": jql},
        }
    )


def _discover_issue_provider(payload: dict[str, Any]) -> str:
    integration_ctx = _extract_integration_context(payload)
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    provider = str(payload.get("issue_provider") or brownfield.get("issue_provider") or "").strip().lower()
    return provider or "linear"


async def api_discover_issues(request):
    payload = _get_json(await request.body())
    provider = _discover_issue_provider(payload)
    if provider == "linear":
        return _discover_linear_issues_response(payload)
    if provider == "jira":
        return _discover_jira_issues_response(payload)
    return JSONResponse(
        {
            "ok": False,
            "error": f"Issue provider `{provider}` is not supported yet for issue preview. Supported: linear, jira.",
        },
        status_code=400,
    )


async def api_discover_linear_issues(request):
    payload = _get_json(await request.body())
    return _discover_linear_issues_response(payload)


async def api_save_policies(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.update_policies(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "settings": settings, "policies": settings.get("policies", {})})


async def api_add_policy_exception(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.add_exception(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "settings": settings, "exceptions": settings.get("exceptions", [])})


async def api_resolve_policy_exception(request):
    exception_id = request.path_params.get("exception_id", "")
    try:
        settings = SETTINGS_STORE.resolve_exception(exception_id, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "settings": settings, "exceptions": settings.get("exceptions", [])})


async def api_save_rbac_role(request):
    role = request.path_params.get("role", "")
    payload = _get_json(await request.body())
    permissions = payload.get("permissions", [])
    try:
        settings = SETTINGS_STORE.update_role_permissions(role, permissions, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    roles = settings.get("rbac", {}).get("roles", {})
    return JSONResponse({"ok": True, "settings": settings, "roles": roles})


async def api_upsert_rbac_assignment(request):
    payload = _get_json(await request.body())
    email = str(payload.get("email", "")).strip()
    role = str(payload.get("role", "")).strip()
    try:
        settings = SETTINGS_STORE.upsert_assignment(email, role, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    assignments = settings.get("rbac", {}).get("assignments", [])
    return JSONResponse({"ok": True, "settings": settings, "assignments": assignments})


async def api_remove_rbac_assignment(request):
    payload = _get_json(await request.body())
    email = str(payload.get("email", "")).strip()
    try:
        settings = SETTINGS_STORE.remove_assignment(email, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    assignments = settings.get("rbac", {}).get("assignments", [])
    return JSONResponse({"ok": True, "settings": settings, "assignments": assignments})


def _agents_by_stage(agents: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {str(i): [] for i in range(1, TOTAL_STAGES + 1)}
    for agent in agents:
        stage = str(agent.get("stage", ""))
        if stage in grouped:
            grouped[stage].append(agent)
    return grouped


async def api_list_agents(_request):
    payload = TEAM_STORE.list_agents()
    return JSONResponse(
        {
            "ok": True,
            "premade": payload.get("premade", []),
            "custom": payload.get("custom", []),
            "all": payload.get("all", []),
            "by_stage": _agents_by_stage(payload.get("all", [])),
        }
    )


async def api_clone_agent(request):
    payload = _get_json(await request.body())
    base_agent_id = str(payload.get("base_agent_id", "")).strip()
    display_name = str(payload.get("display_name", "")).strip()
    persona = str(payload.get("persona", "")).strip()
    if not base_agent_id:
        return JSONResponse({"ok": False, "error": "base_agent_id is required"}, status_code=400)
    try:
        cloned = TEAM_STORE.clone_agent(
            base_agent_id=base_agent_id,
            display_name=display_name,
            persona=persona,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "agent": cloned})


async def api_list_teams(_request):
    return JSONResponse({"ok": True, "teams": TEAM_STORE.list_teams()})


async def api_get_team(request):
    team_id = request.path_params.get("team_id", "")
    team = TEAM_STORE.get_team(team_id)
    if not team:
        return JSONResponse({"ok": False, "error": "team not found"}, status_code=404)
    personas, resolved_team = TEAM_STORE.resolve_personas(team_id=team_id)
    return JSONResponse({"ok": True, "team": resolved_team, "agent_personas": personas})


async def api_save_team(request):
    payload = _get_json(await request.body())
    name = str(payload.get("name", "")).strip()
    description = str(payload.get("description", "")).strip()
    stage_agent_ids = payload.get("stage_agent_ids", {})
    team_id = str(payload.get("team_id", "")).strip()
    if not isinstance(stage_agent_ids, dict):
        return JSONResponse({"ok": False, "error": "stage_agent_ids must be an object"}, status_code=400)
    team = TEAM_STORE.save_team(
        name=name,
        description=description,
        stage_agent_ids=stage_agent_ids,
        team_id=team_id,
    )
    personas, resolved_team = TEAM_STORE.resolve_personas(team_id=team.get("id", ""))
    return JSONResponse({"ok": True, "team": resolved_team, "agent_personas": personas})


async def api_suggest_team(request):
    payload = _get_json(await request.body())
    challenge = str(payload.get("challenge", payload.get("objectives", ""))).strip()
    if not challenge:
        return JSONResponse({"ok": False, "error": "challenge is required"}, status_code=400)
    suggestion = TEAM_STORE.suggest_team(challenge)
    personas, team = TEAM_STORE.resolve_personas(team_id=str(suggestion.get("team_id", "")))
    return JSONResponse(
        {
            "ok": True,
            "suggestion": suggestion,
            "team": team,
            "agent_personas": personas,
        }
    )


async def api_list_tasks(request):
    limit_raw = request.query_params.get("limit", "60")
    try:
        limit = max(1, min(300, int(limit_raw)))
    except ValueError:
        limit = 60

    runs = MANAGER.list_runs(limit=limit)
    tasks: list[dict[str, Any]] = []
    for run in runs:
        run_id = str(run.get("run_id", ""))
        if not run_id:
            continue
        state_payload = RUN_STORE.load_run(run_id) or {}
        pipeline_state = state_payload.get("pipeline_state") if isinstance(state_payload, dict) else {}
        if not isinstance(pipeline_state, dict):
            pipeline_state = {}
        objective = str(
            pipeline_state.get("business_objectives")
            or run.get("business_objectives", "")
        )
        tasks.append(
            {
                "run_id": run_id,
                "status": run.get("status", "unknown"),
                "created_at": run.get("created_at", ""),
                "updated_at": run.get("updated_at", ""),
                "business_objective": objective,
                "objective_preview": objective.replace("\n", " ")[:220],
                "use_case": pipeline_state.get("use_case", "business_objectives"),
                "project_state_detected": pipeline_state.get("project_state_detected", ""),
                "team_id": pipeline_state.get("team_id", ""),
                "team_name": pipeline_state.get("team_name", ""),
                "deployment_target": pipeline_state.get("deployment_target", "local"),
            }
        )
    return JSONResponse({"ok": True, "tasks": tasks})


def _pipeline_graph_counts(pipeline_state: dict[str, Any]) -> tuple[int, int, int]:
    scm = pipeline_state.get("system_context_model", {}) if isinstance(pipeline_state, dict) else {}
    if not isinstance(scm, dict):
        return 0, 0, 0
    graph = scm.get("graph", {}) if isinstance(scm.get("graph", {}), dict) else {}
    nodes = graph.get("nodes", scm.get("nodes", []))
    edges = graph.get("edges", scm.get("edges", []))
    node_list = [n for n in nodes if isinstance(n, dict)] if isinstance(nodes, list) else []
    edge_list = [e for e in edges if isinstance(e, dict)] if isinstance(edges, list) else []
    service_like = 0
    for node in node_list:
        t = str(node.get("type", "")).strip().lower()
        if t in {"service", "container", "component"}:
            service_like += 1
    return len(node_list), len(edge_list), service_like


def _work_item_recommendation(payload: dict[str, Any]) -> dict[str, Any]:
    run_id = str(payload.get("run_id", "")).strip()
    title = str(payload.get("title", "")).strip()
    description = str(payload.get("description", "")).strip()
    text = f"{title} {description}".lower()

    nodes = edges = services = findings = backlog = 0
    if run_id:
        run = MANAGER.get_run(run_id)
        p = run.get("pipeline_state", {}) if isinstance(run, dict) else {}
        if isinstance(p, dict):
            nodes, edges, services = _pipeline_graph_counts(p)
            health = p.get("health_assessment", {}) if isinstance(p.get("health_assessment", {}), dict) else {}
            findings_arr = health.get("findings", [])
            findings = len(findings_arr) if isinstance(findings_arr, list) else 0
            backlog_arr = p.get("remediation_backlog", [])
            backlog = len(backlog_arr) if isinstance(backlog_arr, list) else 0

    keyword_weight = 0
    for token, score in (
        ("cross-service", 8),
        ("multi-service", 8),
        ("migration", 7),
        ("database", 6),
        ("security", 6),
        ("shared", 4),
        ("contract", 5),
        ("api change", 4),
    ):
        if token in text:
            keyword_weight += score

    blast_radius = int(min(100, max(0, (services * 9) + (findings * 3) + keyword_weight)))
    complexity_score = round(
        min(100.0, (nodes * 0.15) + (edges * 0.1) + (findings * 4.0) + (backlog * 2.0) + keyword_weight),
        2,
    )

    if complexity_score >= 70 or blast_radius >= 55:
        risk_tier = "critical"
    elif complexity_score >= 45 or blast_radius >= 35:
        risk_tier = "high"
    elif complexity_score >= 25 or blast_radius >= 18:
        risk_tier = "medium"
    else:
        risk_tier = "low"

    recommended_type = "project" if (complexity_score >= 40 or blast_radius >= 30 or services >= 3) else "task"
    rationale = (
        f"SCM signals: {nodes} nodes/{edges} edges/{services} service-like components; "
        f"health findings: {findings}; backlog items: {backlog}; keyword weight: {keyword_weight}."
    )
    return {
        "recommended_type": recommended_type,
        "risk_tier": risk_tier,
        "complexity_score": complexity_score,
        "blast_radius": blast_radius,
        "rationale": rationale,
    }


async def api_list_work_items(_request):
    return JSONResponse({"ok": True, "work_items": WORK_ITEM_STORE.list_items()})


async def api_create_work_item(request):
    payload = _get_json(await request.body())
    title = str(payload.get("title", "")).strip()
    description = str(payload.get("description", "")).strip()
    if not title or not description:
        return JSONResponse({"ok": False, "error": "title and description are required"}, status_code=400)

    recommendation = _work_item_recommendation(payload)
    req_type = str(payload.get("type", "auto")).strip().lower() or "auto"
    resolved_type = recommendation["recommended_type"] if req_type == "auto" else req_type
    if resolved_type not in {"task", "project"}:
        resolved_type = recommendation["recommended_type"]

    row = WORK_ITEM_STORE.create_item(
        {
            "title": title,
            "description": description,
            "type": resolved_type,
            "recommended_type": recommendation["recommended_type"],
            "status": str(payload.get("status", "open")).strip().lower() or "open",
            "governance_tier": str(payload.get("governance_tier", "standard")).strip().lower() or "standard",
            "risk_tier": recommendation["risk_tier"],
            "complexity_score": recommendation["complexity_score"],
            "blast_radius": recommendation["blast_radius"],
            "linked_issue": str(payload.get("linked_issue", "")).strip(),
            "run_id": str(payload.get("run_id", "")).strip(),
            "source": str(payload.get("source", "manual")).strip() or "manual",
        },
        actor="local-user",
    )
    return JSONResponse({"ok": True, "work_item": row, "recommendation": recommendation})


async def api_set_work_item_status(request):
    item_id = request.path_params.get("item_id", "")
    payload = _get_json(await request.body())
    status = str(payload.get("status", "")).strip().lower()
    if not status:
        return JSONResponse({"ok": False, "error": "status is required"}, status_code=400)
    row = WORK_ITEM_STORE.set_status(item_id, status, actor="local-user")
    if not row:
        return JSONResponse({"ok": False, "error": "work item not found"}, status_code=404)
    return JSONResponse({"ok": True, "work_item": row})


async def api_clone_task(request):
    run_id = request.path_params.get("run_id", "")
    state_payload = RUN_STORE.load_run(run_id)
    if not state_payload:
        return JSONResponse({"ok": False, "error": "task not found"}, status_code=404)
    pipeline_state = state_payload.get("pipeline_state")
    if not isinstance(pipeline_state, dict):
        pipeline_state = {}

    meta = next((r for r in MANAGER.list_runs(limit=400) if str(r.get("run_id", "")) == run_id), {})
    config = meta.get("config", {}) if isinstance(meta.get("config"), dict) else {}

    template = {
        "run_id": run_id,
        "objectives": str(
            pipeline_state.get("business_objectives")
            or meta.get("business_objectives", "")
        ),
        "use_case": str(pipeline_state.get("use_case", "business_objectives")),
        "legacy_code": str(pipeline_state.get("legacy_code", "")),
        "modernization_language": str(pipeline_state.get("modernization_language", "")),
        "database_source": str(pipeline_state.get("database_source", "")),
        "database_target": str(pipeline_state.get("database_target", "")),
        "database_schema": str(pipeline_state.get("database_schema", "")),
        "deployment_target": str(pipeline_state.get("deployment_target", "local")),
        "cloud_config": pipeline_state.get("cloud_config", {}) if isinstance(pipeline_state.get("cloud_config"), dict) else {},
        "integration_context": pipeline_state.get("integration_context", {}) if isinstance(pipeline_state.get("integration_context"), dict) else {},
        "project_state_mode": str(pipeline_state.get("project_state_mode", "auto")),
        "project_state_detected": str(pipeline_state.get("project_state_detected", "")),
        "human_approval": bool(pipeline_state.get("human_approval", False)),
        "strict_security_mode": bool(pipeline_state.get("strict_security_mode", False)),
        "team_id": str(pipeline_state.get("team_id", "")),
        "stage_agent_ids": pipeline_state.get("stage_agent_ids", {}) if isinstance(pipeline_state.get("stage_agent_ids"), dict) else {},
        "config_hints": {
            "provider": str(config.get("provider", "anthropic")),
            "model": str(config.get("model", "")),
            "temperature": float(config.get("temperature", 0.3) or 0.3),
            "parallel_agents": int(config.get("developer_parallel_agents", 5) or 5),
            "max_retries": int(config.get("max_retries", 2) or 2),
            "live_deploy": bool(config.get("live_deploy", True)),
            "cluster_name": str(config.get("cluster_name", "agent-pipeline")),
            "namespace": str(config.get("namespace", "agent-app")),
        },
    }
    return JSONResponse({"ok": True, "template": template})


async def api_start_run(request):
    payload = _get_json(await request.body())
    objectives = str(payload.get("objectives", "")).strip()
    use_case = str(payload.get("use_case", "business_objectives")).strip().lower() or "business_objectives"
    legacy_code = str(payload.get("legacy_code", "")).strip()
    modernization_language = str(payload.get("modernization_language", "")).strip()
    database_source = str(payload.get("database_source", "")).strip()
    database_target = str(payload.get("database_target", "")).strip()
    database_schema = str(payload.get("database_schema", "")).strip()
    deployment_target = str(payload.get("deployment_target", "local")).strip().lower() or "local"
    human_approval = bool(payload.get("human_approval", False))
    strict_security_mode = bool(payload.get("strict_security_mode", False))
    cloud_config = payload.get("cloud_config", {}) if isinstance(payload.get("cloud_config", {}), dict) else {}
    integration_context = payload.get("integration_context", {}) if isinstance(payload.get("integration_context", {}), dict) else {}
    scan_scope = integration_context.get("scan_scope", {}) if isinstance(integration_context.get("scan_scope", {}), dict) else {}
    brownfield = integration_context.get("brownfield", {}) if isinstance(integration_context.get("brownfield", {}), dict) else {}
    modernization_source_mode = str(scan_scope.get("modernization_source_mode", "manual")).strip().lower() or "manual"
    team_id = str(payload.get("team_id", "")).strip()
    stage_agent_ids = payload.get("stage_agent_ids", {}) if isinstance(payload.get("stage_agent_ids", {}), dict) else {}
    if not objectives:
        return JSONResponse({"ok": False, "error": "objectives are required"}, status_code=400)
    if use_case not in {"business_objectives", "code_modernization", "database_conversion"}:
        return JSONResponse(
            {"ok": False, "error": "use_case must be business_objectives, code_modernization, or database_conversion"},
            status_code=400,
        )
    if use_case == "code_modernization" and not legacy_code and modernization_source_mode == "repo_scan":
        sample_mode = bool(integration_context.get("sample_dataset_enabled", False))
        repo_provider = str(brownfield.get("repo_provider", "")).strip().lower()
        repo_url = str(brownfield.get("repo_url", "")).strip()
        include_paths = _normalize_lines(scan_scope.get("include_paths", []))
        exclude_paths = _normalize_lines(scan_scope.get("exclude_paths", []))
        if repo_provider == "github" and repo_url:
            owner, repository = _parse_github_repo_url(repo_url)
            github_cfg = SETTINGS_STORE.get_integration_config("github")
            if not owner:
                owner = str(github_cfg.get("owner", "")).strip()
            if not repository:
                repository = str(github_cfg.get("repository", "")).strip()
            if owner and repository:
                if sample_mode:
                    sample_contents = {
                        "services/orders-service/src/main.java": "@RestController\n@RequestMapping(\"/v1/orders\")\npublic class OrdersController {}",
                        "services/payments-service/cmd/api/main.go": "router.POST(\"/v1/payments\", handlePayments)\n// uses redis idempotency",
                        "services/inventory-service/index.js": "app.post('/v1/inventory/reserve', reserveInventory)",
                        "legacy/billing-monolith/README.md": "Legacy billing and invoicing flow.",
                    }
                    legacy_code = _compose_legacy_code_bundle(sample_contents)
                else:
                    token = str(github_cfg.get("token", "")).strip()
                    base_url = str(github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
                    if token:
                        headers = {
                            "Accept": "application/vnd.github+json",
                            "Authorization": f"Bearer {token}",
                            "User-Agent": "synthetix-discover/1.0",
                            "X-GitHub-Api-Version": "2022-11-28",
                        }
                        try:
                            repo_meta = _http_json_request(
                                f"{base_url}/repos/{quote(owner)}/{quote(repository)}",
                                headers=headers,
                            )
                            branch = "main"
                            if isinstance(repo_meta, dict):
                                branch = str(repo_meta.get("default_branch") or "main")
                            tree_payload = _http_json_request(
                                f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/trees/{quote(branch, safe='')}?recursive=1",
                                headers=headers,
                            )
                            raw_entries = tree_payload.get("tree", []) if isinstance(tree_payload, dict) else []
                            selected_entries = _select_source_entries_for_analysis(
                                [item for item in raw_entries if isinstance(item, dict)],
                                include_paths=include_paths,
                                exclude_paths=exclude_paths,
                                limit=32,
                            )
                            file_contents: dict[str, str] = {}
                            for entry in selected_entries:
                                path = str(entry.get("path", "")).strip()
                                if not path:
                                    continue
                                content = _fetch_github_file_content(
                                    base_url=base_url,
                                    owner=owner,
                                    repository=repository,
                                    path=path,
                                    ref=branch,
                                    headers=headers,
                                    max_chars=12000,
                                )
                                if content:
                                    file_contents[path] = content
                            legacy_code = _compose_legacy_code_bundle(file_contents)
                            if legacy_code:
                                integration_context.setdefault("repo_scan_cache", {})
                                cache = integration_context.get("repo_scan_cache", {})
                                if isinstance(cache, dict):
                                    cache["owner"] = owner
                                    cache["repository"] = repository
                                    cache["default_branch"] = branch
                                    cache["sampled_files"] = sorted(file_contents.keys())[:40]
                                    integration_context["repo_scan_cache"] = cache
                        except ValueError:
                            legacy_code = ""
    if use_case == "code_modernization" and not legacy_code:
        if modernization_source_mode == "repo_scan":
            return JSONResponse(
                {
                    "ok": False,
                    "error": "legacy_code is unavailable. Repo scan mode could not extract source from connected GitHub repository.",
                },
                status_code=400,
            )
        return JSONResponse({"ok": False, "error": "legacy_code is required for code_modernization use case"}, status_code=400)
    if use_case == "database_conversion" and not database_schema:
        return JSONResponse({"ok": False, "error": "database_schema is required for database_conversion use case"}, status_code=400)

    try:
        config = _config_from_payload(payload)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    run_id = MANAGER.start_run(
        objectives=objectives,
        config=config,
        use_case=use_case,
        legacy_code=legacy_code,
        modernization_language=modernization_language,
        database_source=database_source,
        database_target=database_target,
        database_schema=database_schema,
        human_approval=human_approval,
        strict_security_mode=strict_security_mode,
        deployment_target=deployment_target,
        cloud_config=cloud_config,
        integration_context=integration_context,
        team_id=team_id,
        stage_agent_ids=stage_agent_ids,
    )
    return JSONResponse({"ok": True, "run_id": run_id})


async def api_get_run(request):
    run_id = request.path_params.get("run_id", "")
    data = MANAGER.get_run(run_id)
    if not data:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    return JSONResponse({"ok": True, "run": data})


async def api_list_runs(_request):
    return JSONResponse({"ok": True, "runs": MANAGER.list_runs(limit=60)})


async def api_approve_run(request):
    run_id = request.path_params.get("run_id", "")
    payload = _get_json(await request.body())
    result = MANAGER.approve(run_id, payload)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


def _merge_analyst_output_into_state(state: dict[str, Any], analyst_output: dict[str, Any], markdown_doc: str = "") -> None:
    state["analyst_output"] = analyst_output
    if markdown_doc:
        state["analyst_document_markdown"] = markdown_doc

    results = list(state.get("agent_results", [])) if isinstance(state.get("agent_results", []), list) else []
    idx = -1
    for i, item in enumerate(results):
        if isinstance(item, dict) and int(item.get("stage", 0) or 0) == 1:
            idx = i
    human_note = "Analyst output revised from uploaded technical requirements document"
    if idx >= 0:
        item = dict(results[idx])
        item["output"] = analyst_output
        item["status"] = "success"
        item["summary"] = human_note
        item_logs = list(item.get("logs", [])) if isinstance(item.get("logs", []), list) else []
        item_logs.append(human_note)
        item["logs"] = item_logs[-50:]
        results[idx] = item
    else:
        results.append(
            {
                "agent_name": "Analyst Agent",
                "stage": 1,
                "status": "success",
                "summary": human_note,
                "output": analyst_output,
                "tokens_used": 0,
                "latency_ms": 0,
                "logs": [human_note],
            }
        )
    state["agent_results"] = results


async def api_update_analyst_doc(request):
    run_id = request.path_params.get("run_id", "")
    payload = _get_json(await request.body())
    fmt = str(payload.get("format", "markdown")).strip().lower()
    content = str(payload.get("content", "") or "")
    if fmt not in {"json", "markdown"}:
        return JSONResponse({"ok": False, "error": "format must be 'json' or 'markdown'"}, status_code=400)
    if not content.strip():
        return JSONResponse({"ok": False, "error": "content is required"}, status_code=400)

    current = MANAGER.get_run(run_id)
    if not current:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    current_state = current.get("pipeline_state", {}) if isinstance(current.get("pipeline_state", {}), dict) else {}
    analyst_output = dict(current_state.get("analyst_output", {})) if isinstance(current_state.get("analyst_output", {}), dict) else {}
    markdown_doc = ""

    if fmt == "json":
        try:
            parsed = json.loads(content)
        except json.JSONDecodeError:
            return JSONResponse({"ok": False, "error": "Uploaded JSON is invalid"}, status_code=400)
        if not isinstance(parsed, dict):
            return JSONResponse({"ok": False, "error": "Uploaded JSON must be an object"}, status_code=400)
        analyst_output = parsed
    else:
        markdown_doc = content
        analyst_output["human_revised_document_markdown"] = content
        analyst_output["human_revision_applied_at"] = _utc_now()

    active = MANAGER._get_record(run_id)
    if active and isinstance(active.pipeline_state, dict):
        _merge_analyst_output_into_state(active.pipeline_state, analyst_output, markdown_doc=markdown_doc)
        active.updated_at = _utc_now()
        MANAGER._append_log(active, "📝 Analyst tech requirements document updated from uploaded file")
        MANAGER._persist(active)
        return JSONResponse({"ok": True, "run": MANAGER._record_payload(active)})

    persisted = RUN_STORE.load_run(run_id)
    if not persisted:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    state = persisted.get("pipeline_state", {}) if isinstance(persisted.get("pipeline_state", {}), dict) else {}
    _merge_analyst_output_into_state(state, analyst_output, markdown_doc=markdown_doc)

    logs = list(persisted.get("progress_logs", [])) if isinstance(persisted.get("progress_logs", []), list) else []
    logs.append(f"[{_ts()}] 📝 Analyst tech requirements document updated from uploaded file")
    stage_status = persisted.get("stage_status", {}) if isinstance(persisted.get("stage_status", {}), dict) else {}
    if "1" not in stage_status:
        stage_status["1"] = "success"
    RUN_STORE.finalize_run(
        run_id=run_id,
        status=str(persisted.get("pipeline_status", "completed")),
        pipeline_state=state,
        stage_status=stage_status,
        progress_logs=logs,
        error_message=persisted.get("error_message"),
    )
    updated = MANAGER.get_run(run_id)
    return JSONResponse({"ok": True, "run": updated})


async def api_context_versions(request):
    repo = str(request.query_params.get("repo", "")).strip()
    branch = str(request.query_params.get("branch", "")).strip()
    limit_raw = str(request.query_params.get("limit", "40")).strip()
    try:
        limit = max(1, min(300, int(limit_raw)))
    except ValueError:
        limit = 40
    versions = list_versions(CONTEXT_GRAPH_DB, repo=repo, branch=branch, limit=limit)
    return JSONResponse({"ok": True, "versions": versions})


async def api_context_bundle(request):
    ref, err = _resolve_context_reference({}, dict(request.query_params))
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    artifacts = _load_context_artifacts(ref)
    bundle = artifacts.get("context_bundle", {})
    if not bundle:
        return JSONResponse({"ok": False, "error": "context_bundle not found for selected reference"}, status_code=404)
    return JSONResponse(
        {
            "ok": True,
            "context_reference": ref,
            "context_bundle": bundle,
            "validation_report": artifacts.get("contract_validation_report", {}),
            "artifact_paths": {
                "vault_path": ref.get("vault_path", ""),
                "contract_bundle_path": str(Path(str(ref.get("vault_path", ""))) / "contract_bundle"),
            },
        }
    )


async def api_context_contracts(request):
    ref, err = _resolve_context_reference({}, dict(request.query_params))
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)
    artifacts = _load_context_artifacts(ref)
    contract = {
        "system_context_model": artifacts.get("contract_system_context_model", {}),
        "convention_profile": artifacts.get("contract_convention_profile", {}),
        "health_assessment_bundle": artifacts.get("contract_health_assessment_bundle", {}),
        "context_bundle": artifacts.get("context_bundle", {}),
        "validation_report": artifacts.get("contract_validation_report", {}),
    }
    if not any(bool(v) for v in contract.values()):
        return JSONResponse({"ok": False, "error": "contract artifacts not found"}, status_code=404)
    return JSONResponse({"ok": True, "context_reference": ref, "contracts": contract})


async def api_context_graph_neighbors(request):
    node_id = str(request.query_params.get("node_id", "")).strip()
    if not node_id:
        return JSONResponse({"ok": False, "error": "node_id is required"}, status_code=400)
    direction = str(request.query_params.get("direction", "both")).strip().lower() or "both"
    edge_types_raw = str(request.query_params.get("edge_types", "")).strip()
    edge_types = [x.strip() for x in edge_types_raw.split(",") if x.strip()] if edge_types_raw else []

    ref, err = _resolve_context_reference({}, dict(request.query_params))
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)

    data = graph_neighbors(
        CONTEXT_GRAPH_DB,
        version_id=str(ref.get("version_id", "")),
        node_id=node_id,
        direction=direction,
        edge_types=edge_types or None,
    )
    return JSONResponse({"ok": True, "context_reference": ref, "result": data})


async def api_context_trace_ingest(request):
    payload = _get_json(await request.body())
    spans = payload.get("spans", [])
    if not isinstance(spans, list) or not spans:
        return JSONResponse({"ok": False, "error": "spans[] is required"}, status_code=400)

    ref, err = _resolve_context_reference(payload)
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)

    run_id = str(payload.get("run_id", "")).strip()
    summary = ingest_runtime_traces(
        CONTEXT_GRAPH_DB,
        context_ref=ref,
        spans=[s for s in spans if isinstance(s, dict)],
        run_id=run_id,
    )
    artifact_path = _persist_context_report(
        ref,
        "runtime_trace_ingest",
        {"summary": summary, "sample_spans": spans[:30], "ingested_at": _utc_now()},
    )
    if artifact_path:
        summary["artifact_path"] = artifact_path
    return JSONResponse({"ok": True, "context_reference": ref, "ingestion": summary})


async def api_context_log_ingest(request):
    payload = _get_json(await request.body())
    raw_logs = payload.get("logs", [])
    logs: list[Any]
    if isinstance(raw_logs, str):
        logs = [line for line in raw_logs.splitlines() if str(line).strip()]
    elif isinstance(raw_logs, list):
        logs = raw_logs
    else:
        return JSONResponse({"ok": False, "error": "logs (array|string) is required"}, status_code=400)
    if not logs:
        return JSONResponse({"ok": False, "error": "logs is empty"}, status_code=400)

    ref, err = _resolve_context_reference(payload)
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)

    run_id = str(payload.get("run_id", "")).strip()
    summary = ingest_runtime_logs(
        CONTEXT_GRAPH_DB,
        context_ref=ref,
        logs=logs,
        run_id=run_id,
    )
    artifact_path = _persist_context_report(
        ref,
        "runtime_log_ingest",
        {"summary": summary, "sample_logs": logs[:50], "ingested_at": _utc_now()},
    )
    if artifact_path:
        summary["artifact_path"] = artifact_path
    return JSONResponse({"ok": True, "context_reference": ref, "ingestion": summary})


async def api_context_impact_forecast(request):
    payload = _get_json(await request.body())
    requirement_text = str(
        payload.get("requirement_text")
        or payload.get("requirement")
        or payload.get("business_challenge")
        or ""
    ).strip()
    if not requirement_text:
        return JSONResponse({"ok": False, "error": "requirement_text is required"}, status_code=400)

    changed_files = payload.get("changed_files", [])
    if not isinstance(changed_files, list):
        changed_files = []
    changed_files = [str(x).strip() for x in changed_files if str(x).strip()]

    ref, err = _resolve_context_reference(payload)
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)

    context_artifacts = _load_context_artifacts(ref)
    report = forecast_impact(
        CONTEXT_GRAPH_DB,
        version_id=str(ref.get("version_id", "")),
        requirement_text=requirement_text,
        changed_files=changed_files,
        health_assessment=context_artifacts.get("health_assessment", {}),
        convention_profile=context_artifacts.get("convention_profile", {}),
    )
    report["requirement_text"] = requirement_text
    report["context_reference"] = {
        "version_id": ref.get("version_id", ""),
        "repo": ref.get("repo", ""),
        "branch": ref.get("branch", ""),
        "commit_sha": ref.get("commit_sha", ""),
    }

    artifact_path = _persist_context_report(ref, "impact_forecast", report)
    if artifact_path:
        report["artifact_path"] = artifact_path
    return JSONResponse({"ok": True, "forecast": report})


async def api_context_drift_run(request):
    payload = _get_json(await request.body())
    ref, err = _resolve_context_reference(payload)
    if err or not ref:
        return JSONResponse({"ok": False, "error": err}, status_code=400)

    repo = str(payload.get("repo", ref.get("repo", ""))).strip()
    branch = str(payload.get("branch", ref.get("branch", ""))).strip()
    current_version_id = str(payload.get("current_version_id", ref.get("version_id", ""))).strip()
    previous_version_id = str(payload.get("previous_version_id", "")).strip()

    with DRIFT_LOCK:
        report = detect_drift(
            CONTEXT_GRAPH_DB,
            repo=repo,
            branch=branch,
            current_version_id=current_version_id,
            previous_version_id=previous_version_id,
        )

    report_path = _persist_context_report(ref, "drift_report", report)
    if not report_path:
        report_path = _persist_branch_drift_report(report)
    report["report_path"] = report_path
    return JSONResponse({"ok": True, "drift_report": report})


async def api_context_drift_reports(request):
    repo = str(request.query_params.get("repo", "")).strip()
    branch = str(request.query_params.get("branch", "")).strip()
    limit_raw = str(request.query_params.get("limit", "40")).strip()
    try:
        limit = max(1, min(300, int(limit_raw)))
    except ValueError:
        limit = 40

    base = CONTEXT_VAULT_ROOT
    if repo:
        base = base / repo
    if branch:
        base = base / branch

    reports: list[dict[str, Any]] = []
    if base.exists() and base.is_dir():
        paths = sorted(base.rglob("drift_report_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
        for path in paths[:limit]:
            content = _read_json_file(path) or {}
            reports.append(
                {
                    "path": str(path),
                    "repo": content.get("repo", ""),
                    "branch": content.get("branch", ""),
                    "status": content.get("status", ""),
                    "generated_at": content.get("generated_at", ""),
                    "finding_count": len(content.get("findings", [])) if isinstance(content.get("findings", []), list) else 0,
                }
            )
    return JSONResponse({"ok": True, "reports": reports})


def _artifact_roots(run_id: str, run_payload: dict[str, Any] | None = None) -> dict[str, Path]:
    safe_run_id = safe_name(run_id)
    roots = {
        "pipeline": ROOT / "pipeline_runs" / run_id,
        "qa": ROOT / "run_artifacts" / safe_run_id,
        "deploy": ROOT / "deploy_output" / "runs" / safe_run_id,
    }
    payload = run_payload or {}
    state = payload.get("pipeline_state", {}) if isinstance(payload, dict) else {}
    ref = state.get("context_vault_ref", {}) if isinstance(state, dict) else {}
    vault_path = str(ref.get("vault_path", "")).strip() if isinstance(ref, dict) else ""
    if vault_path:
        p = Path(vault_path)
        if p.exists() and p.is_dir():
            roots["context"] = p
    return roots


def _is_within(root: Path, target: Path) -> bool:
    try:
        target.resolve().relative_to(root.resolve())
        return True
    except ValueError:
        return False


def _sanitize_repo_path(raw: str, default: str = "") -> str:
    parts: list[str] = []
    for segment in str(raw or "").replace("\\", "/").split("/"):
        token = str(segment).strip()
        if not token or token in {".", ".."}:
            continue
        cleaned = re.sub(r"[^a-zA-Z0-9_.-]+", "-", token).strip("-").lower()
        if cleaned:
            parts.append(cleaned)
    path = "/".join(parts)
    return path or default


def _github_lookup_content_sha(
    *,
    base_url: str,
    owner: str,
    repository: str,
    branch: str,
    repo_path: str,
    headers: dict[str, str],
) -> tuple[str, str]:
    url = (
        f"{base_url}/repos/{quote(owner)}/{quote(repository)}/contents/"
        f"{quote(repo_path, safe='/')}?ref={quote(branch, safe='')}"
    )
    try:
        payload = _http_json_request(url, headers=headers)
    except ValueError as exc:
        msg = str(exc)
        if msg.lower().startswith("http 404"):
            return "", ""
        return "", msg
    if isinstance(payload, dict):
        return str(payload.get("sha", "")).strip(), ""
    return "", "content lookup returned unexpected payload"


def _github_put_content(
    *,
    base_url: str,
    owner: str,
    repository: str,
    branch: str,
    repo_path: str,
    content: bytes,
    message: str,
    headers: dict[str, str],
) -> tuple[bool, str]:
    empty_repo_bootstrap = False
    sha, err = _github_lookup_content_sha(
        base_url=base_url,
        owner=owner,
        repository=repository,
        branch=branch,
        repo_path=repo_path,
        headers=headers,
    )
    if err:
        lowered = err.lower()
        if "git repository is empty" in lowered:
            empty_repo_bootstrap = True
        else:
            return False, f"sha lookup failed: {err}"
    payload: dict[str, Any] = {
        "message": message,
        "content": base64.b64encode(content).decode("ascii"),
    }
    if branch and not empty_repo_bootstrap:
        payload["branch"] = branch
    if sha:
        payload["sha"] = sha
    url = (
        f"{base_url}/repos/{quote(owner)}/{quote(repository)}/contents/"
        f"{quote(repo_path, safe='/')}"
    )
    try:
        _http_json_request(url, method="PUT", headers=headers, payload=payload)
    except ValueError as exc:
        return False, str(exc)
    return True, ""


def _collect_virtual_generated_code(run_payload: dict[str, Any]) -> dict[str, bytes]:
    pipeline_state = (
        run_payload.get("pipeline_state", {})
        if isinstance(run_payload.get("pipeline_state", {}), dict)
        else {}
    )
    developer_output = (
        pipeline_state.get("developer_output", {})
        if isinstance(pipeline_state.get("developer_output", {}), dict)
        else {}
    )
    implementations = (
        developer_output.get("implementations", [])
        if isinstance(developer_output.get("implementations", []), list)
        else []
    )
    virtual_files: dict[str, bytes] = {}
    for impl in implementations:
        if not isinstance(impl, dict):
            continue
        component = _sanitize_repo_path(str(impl.get("component_name", "")), default="component")
        files = impl.get("files", [])
        if not isinstance(files, list):
            continue
        for file_spec in files:
            if not isinstance(file_spec, dict):
                continue
            rel = _sanitize_repo_path(str(file_spec.get("path", "")))
            if not rel:
                continue
            code = str(file_spec.get("code", "") or "")
            if not code:
                continue
            target = f"generated_code/{component}/{rel}"
            virtual_files[target] = code.encode("utf-8")
    return virtual_files


def _materialize_generated_code_artifacts(run_id: str, pipeline_state: dict[str, Any]) -> int:
    if not isinstance(pipeline_state, dict):
        return 0
    developer_output = (
        pipeline_state.get("developer_output", {})
        if isinstance(pipeline_state.get("developer_output", {}), dict)
        else {}
    )
    implementations = (
        developer_output.get("implementations", [])
        if isinstance(developer_output.get("implementations", []), list)
        else []
    )
    if not implementations:
        return 0

    generated_root = ROOT / "run_artifacts" / safe_name(str(run_id)) / "generated_code"
    generated_root.mkdir(parents=True, exist_ok=True)
    written = 0
    manifest: list[dict[str, Any]] = []
    for impl in implementations:
        if not isinstance(impl, dict):
            continue
        component = safe_name(str(impl.get("component_name", "component")))
        files = impl.get("files", [])
        if not isinstance(files, list):
            continue
        for file_spec in files:
            if not isinstance(file_spec, dict):
                continue
            raw_rel = str(file_spec.get("path", "") or "").replace("\\", "/").strip().lstrip("/")
            if not raw_rel or raw_rel.startswith("../") or "/../" in raw_rel:
                continue
            code = str(file_spec.get("code", "") or "")
            if not code:
                continue
            target = generated_root / component / raw_rel
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_text(code)
            written += 1
            manifest.append(
                {
                    "component": component,
                    "path": f"{component}/{raw_rel}",
                    "lines_of_code": int(file_spec.get("lines_of_code", 0) or 0),
                }
            )

    if written:
        manifest_path = generated_root / "manifest.json"
        _write_json_file(
            manifest_path,
            {
                "run_id": run_id,
                "generated_at": _utc_now(),
                "total_files": written,
                "files": manifest,
            },
        )
    return written


def _github_ensure_branch(
    *,
    base_url: str,
    owner: str,
    repository: str,
    branch: str,
    from_branch: str,
    headers: dict[str, str],
) -> tuple[bool, str]:
    branch_name = str(branch or "").strip()
    base_name = str(from_branch or "").strip() or "main"
    if not branch_name:
        return False, "target branch is empty"
    try:
        _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/ref/heads/{quote(branch_name, safe='')}",
            headers=headers,
        )
        return True, ""
    except ValueError as exc:
        if not str(exc).lower().startswith("http 404"):
            return False, str(exc)
    try:
        base_ref = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/ref/heads/{quote(base_name, safe='')}",
            headers=headers,
        )
    except ValueError as exc:
        return False, f"unable to resolve base branch `{base_name}`: {exc}"
    sha = str(base_ref.get("object", {}).get("sha", "")).strip() if isinstance(base_ref, dict) else ""
    if not sha:
        return False, f"unable to resolve commit sha for base branch `{base_name}`"
    try:
        _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/refs",
            method="POST",
            headers=headers,
            payload={"ref": f"refs/heads/{branch_name}", "sha": sha},
        )
        return True, ""
    except ValueError as exc:
        return False, f"unable to create branch `{branch_name}` from `{base_name}`: {exc}"


def _export_run_to_github(run_id: str, run_payload: dict[str, Any]) -> dict[str, Any]:
    github_cfg = SETTINGS_STORE.get_integration_config("github")
    if not bool(github_cfg.get("run_export_enabled", False)):
        return {"status": "skipped", "reason": "Run export disabled in Settings > Integrations > GitHub."}
    if bool(github_cfg.get("read_only", True)):
        return {"status": "skipped", "reason": "GitHub integration is configured as read-only."}

    source_owner = str(github_cfg.get("owner", "")).strip()
    source_repository = str(github_cfg.get("repository", "")).strip()
    export_owner = str(github_cfg.get("export_owner", "")).strip()
    export_repository = str(github_cfg.get("export_repository", "")).strip()
    parsed_owner, parsed_repo = _parse_github_repo_url(export_repository)
    if parsed_repo:
        export_repository = parsed_repo
        if not export_owner and parsed_owner:
            export_owner = parsed_owner
    owner = export_owner or source_owner
    repository = export_repository or source_repository
    token = str(github_cfg.get("token", "")).strip()
    base_url = str(github_cfg.get("export_base_url") or github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
    if not owner or not repository or not token:
        return {"status": "skipped", "reason": "GitHub owner/repository/token must be configured for run export."}

    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "synthetix-run-export/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    configured_branch = str(github_cfg.get("export_branch", "")).strip()
    branch = configured_branch
    default_branch = "main"
    if not branch:
        try:
            repo_meta = _http_json_request(
                f"{base_url}/repos/{quote(owner)}/{quote(repository)}",
                headers=headers,
            )
            if isinstance(repo_meta, dict):
                default_branch = str(repo_meta.get("default_branch", "")).strip() or "main"
        except ValueError as exc:
            return {"status": "failed", "reason": f"Unable to resolve repository metadata: {exc}"}
    repo_empty = False
    try:
        _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/ref/heads/{quote(default_branch, safe='')}",
            headers=headers,
        )
    except ValueError as exc:
        if "git repository is empty" in str(exc).lower():
            repo_empty = True
    if not branch:
        branch = default_branch if repo_empty else "synthetix-runs"
    if not repo_empty:
        branch_ok, branch_err = _github_ensure_branch(
            base_url=base_url,
            owner=owner,
            repository=repository,
            branch=branch,
            from_branch=default_branch,
            headers=headers,
        )
        if not branch_ok:
            return {"status": "failed", "reason": branch_err}

    prefix = _sanitize_repo_path(str(github_cfg.get("export_prefix", "")), default="synthetix")
    run_folder = f"{prefix}/runs/{_sanitize_repo_path(run_id, default=safe_name(run_id))}"

    entries: list[dict[str, Any]] = []
    roots = _artifact_roots(run_id, run_payload)
    for root_key, root in roots.items():
        if not root.exists() or not root.is_dir():
            continue
        root_prefix = _sanitize_repo_path(root_key, default="artifacts")
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = _sanitize_repo_path(path.relative_to(root).as_posix())
            if not rel:
                continue
            entries.append(
                {
                    "repo_path": f"{run_folder}/{root_prefix}/{rel}",
                    "source": "file",
                    "path": path,
                    "size_bytes": int(path.stat().st_size),
                }
            )

    virtual_files = _collect_virtual_generated_code(run_payload)
    for rel, content in sorted(virtual_files.items(), key=lambda x: x[0]):
        entries.append(
            {
                "repo_path": f"{run_folder}/{_sanitize_repo_path(rel)}",
                "source": "virtual",
                "content": content,
                "size_bytes": len(content),
            }
        )

    entries.sort(key=lambda x: str(x.get("repo_path", "")))
    skipped: list[dict[str, Any]] = []
    if len(entries) > GITHUB_EXPORT_MAX_FILES:
        for item in entries[GITHUB_EXPORT_MAX_FILES:]:
            skipped.append(
                {
                    "repo_path": item.get("repo_path", ""),
                    "reason": f"skipped: max file limit reached ({GITHUB_EXPORT_MAX_FILES})",
                }
            )
        entries = entries[:GITHUB_EXPORT_MAX_FILES]

    exported_files = 0
    failed_files = 0
    failed_samples: list[dict[str, Any]] = []

    for item in entries:
        size_bytes = int(item.get("size_bytes", 0) or 0)
        if size_bytes > GITHUB_EXPORT_MAX_FILE_BYTES:
            skipped.append(
                {
                    "repo_path": item.get("repo_path", ""),
                    "reason": f"skipped: file exceeds {GITHUB_EXPORT_MAX_FILE_BYTES} bytes",
                }
            )
            continue
        if item.get("source") == "virtual":
            content = item.get("content", b"")
            content_bytes = content if isinstance(content, bytes) else str(content).encode("utf-8")
        else:
            path = item.get("path")
            if not isinstance(path, Path):
                skipped.append({"repo_path": item.get("repo_path", ""), "reason": "skipped: invalid source path"})
                continue
            try:
                content_bytes = path.read_bytes()
            except Exception as exc:
                failed_files += 1
                if len(failed_samples) < 20:
                    failed_samples.append({"repo_path": item.get("repo_path", ""), "reason": str(exc)})
                continue
        ok, err = _github_put_content(
            base_url=base_url,
            owner=owner,
            repository=repository,
            branch=branch,
            repo_path=str(item.get("repo_path", "")),
            content=content_bytes,
            message=f"Synthetix run {run_id}: export {item.get('repo_path', '')}",
            headers=headers,
        )
        if ok:
            exported_files += 1
        else:
            failed_files += 1
            if len(failed_samples) < 20:
                failed_samples.append(
                    {
                        "repo_path": item.get("repo_path", ""),
                        "reason": err,
                    }
                )

    manifest = {
        "run_id": run_id,
        "generated_at": _utc_now(),
        "run_status": str(run_payload.get("status", "")),
        "base_path": run_folder,
        "owner": owner,
        "repository": repository,
        "branch": branch,
        "artifact_roots": sorted(list(roots.keys())),
        "attempted_files": len(entries) + 1,
        "exported_files": exported_files,
        "failed_files": failed_files,
        "skipped_files": len(skipped),
        "failed_samples": failed_samples,
        "skipped_samples": skipped[:40],
    }

    manifest_path = f"{run_folder}/export_manifest.json"
    manifest_ok, manifest_err = _github_put_content(
        base_url=base_url,
        owner=owner,
        repository=repository,
        branch=branch,
        repo_path=manifest_path,
        content=json.dumps(manifest, indent=2, ensure_ascii=True).encode("utf-8"),
        message=f"Synthetix run {run_id}: export manifest",
        headers=headers,
    )
    if manifest_ok:
        exported_files += 1
    else:
        failed_files += 1
        if len(failed_samples) < 20:
            failed_samples.append({"repo_path": manifest_path, "reason": manifest_err})

    status = "exported"
    reason = ""
    first_failure_reason = ""
    if failed_samples:
        first_failure_reason = str(failed_samples[0].get("reason", "")).strip()
    if failed_files > 0 and exported_files > 0:
        status = "partial"
        reason = "Some files failed to export"
    elif failed_files > 0 and exported_files == 0:
        status = "failed"
        reason = "No files could be exported"
    if first_failure_reason:
        reason = f"{reason}: {first_failure_reason}" if reason else first_failure_reason

    return {
        "status": status,
        "reason": reason,
        "owner": owner,
        "repository": repository,
        "branch": branch,
        "base_path": run_folder,
        "artifact_roots": sorted(list(roots.keys())),
        "attempted_files": len(entries) + 1,
        "exported_files": exported_files,
        "failed_files": failed_files,
        "skipped_files": len(skipped),
        "failed_samples": failed_samples,
        "skipped_samples": skipped[:40],
        "manifest_path": manifest_path,
        "configured_branch": configured_branch,
        "default_branch": default_branch,
        "repo_empty_at_start": repo_empty,
    }


async def api_list_artifacts(request):
    run_id = request.path_params.get("run_id", "")
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    artifacts: list[dict[str, Any]] = []
    for root_key, root in _artifact_roots(run_id, run).items():
        if not root.exists():
            continue
        for path in sorted(root.rglob("*")):
            if not path.is_file():
                continue
            rel = path.relative_to(root).as_posix()
            stat = path.stat()
            artifacts.append(
                {
                    "artifact_id": f"{root_key}::{rel}",
                    "root": root_key,
                    "relative_path": rel,
                    "size_bytes": stat.st_size,
                    "modified_at": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).isoformat(),
                }
            )
            if len(artifacts) >= 1500:
                break
        if len(artifacts) >= 1500:
            break

    artifacts.sort(key=lambda x: x["modified_at"], reverse=True)
    return JSONResponse({"ok": True, "run_id": run_id, "artifacts": artifacts})


async def api_artifact_content(request):
    run_id = request.path_params.get("run_id", "")
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    artifact_id = str(request.query_params.get("artifact_id", "")).strip()
    if "::" not in artifact_id:
        return JSONResponse({"ok": False, "error": "artifact_id is required"}, status_code=400)
    root_key, rel = artifact_id.split("::", 1)
    roots = _artifact_roots(run_id, run)
    root = roots.get(root_key)
    if not root:
        return JSONResponse({"ok": False, "error": "invalid artifact root"}, status_code=400)

    target = (root / rel).resolve()
    if not _is_within(root, target):
        return JSONResponse({"ok": False, "error": "invalid artifact path"}, status_code=400)
    if not target.exists() or not target.is_file():
        return JSONResponse({"ok": False, "error": "artifact not found"}, status_code=404)

    raw = target.read_bytes()
    is_binary = b"\x00" in raw[:1024]
    if is_binary:
        return JSONResponse(
            {
                "ok": True,
                "artifact_id": artifact_id,
                "path": str(target),
                "is_binary": True,
                "content": "",
                "truncated": False,
            }
        )

    limit = 200_000
    text = raw[:limit].decode("utf-8", errors="replace")
    truncated = len(raw) > limit
    return JSONResponse(
        {
            "ok": True,
            "artifact_id": artifact_id,
            "path": str(target),
            "is_binary": False,
            "content": text,
            "truncated": truncated,
        }
    )


def _sse_event(event: str, payload: dict[str, Any]) -> str:
    data = json.dumps(payload, default=str)
    return f"event: {event}\ndata: {data}\n\n"


async def api_run_stream(request):
    run_id = request.path_params.get("run_id", "")
    initial_run = MANAGER.get_run(run_id)
    if not initial_run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    async def event_stream():
        # Always send an initial snapshot.
        yield _sse_event("snapshot", {"run": initial_run})

        if initial_run.get("status") != "running":
            yield _sse_event("done", {"status": initial_run.get("status", "unknown"), "run_id": run_id})
            return

        subscription = MANAGER.subscribe(run_id)
        if not subscription:
            latest = MANAGER.get_run(run_id)
            if latest:
                yield _sse_event("update", {"run": latest})
                yield _sse_event("done", {"status": latest.get("status", "unknown"), "run_id": run_id})
            return

        sub_id, q = subscription
        try:
            while True:
                try:
                    item = await asyncio.to_thread(q.get, True, 20)
                except queue.Empty:
                    # Keep the connection warm through proxies/load balancers.
                    yield ": keepalive\n\n"
                    continue

                event = item.get("event", "update")
                data = item.get("data", {})
                yield _sse_event(event, data)
                if event == "done":
                    return
        finally:
            MANAGER.unsubscribe(run_id, sub_id)

    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


def _run_automated_drift_scan() -> list[dict[str, Any]]:
    versions = list_versions(CONTEXT_GRAPH_DB, limit=600)
    by_repo_branch: dict[tuple[str, str], list[dict[str, Any]]] = {}
    for row in versions:
        repo = str(row.get("repo", "")).strip()
        branch = str(row.get("branch", "")).strip()
        if not repo or not branch:
            continue
        by_repo_branch.setdefault((repo, branch), []).append(row)

    reports: list[dict[str, Any]] = []
    for (repo, branch), rows in by_repo_branch.items():
        if len(rows) < 2:
            continue
        report = detect_drift(CONTEXT_GRAPH_DB, repo=repo, branch=branch)
        report_path = _persist_branch_drift_report(report)
        report["report_path"] = report_path
        reports.append(report)
    return reports


def _drift_scheduler_loop() -> None:
    while True:
        try:
            with DRIFT_LOCK:
                _run_automated_drift_scan()
        except Exception:
            pass
        # Keep looping even if the vault is empty or one scan fails.
        threading.Event().wait(max(30, DRIFT_INTERVAL_SEC))


if DRIFT_INTERVAL_SEC > 0:
    threading.Thread(
        target=_drift_scheduler_loop,
        daemon=True,
        name="sil-drift-monitor",
    ).start()


routes = [
    Route("/api/health", api_health, methods=["GET"]),
    Route("/api/samples", api_samples, methods=["GET"]),
    Route("/api/settings", api_get_settings, methods=["GET"]),
    Route("/api/settings/integrations/{provider:str}/connect", api_connect_integration, methods=["POST"]),
    Route("/api/settings/integrations/{provider:str}/test", api_test_integration, methods=["POST"]),
    Route("/api/settings/integrations/{provider:str}/disconnect", api_disconnect_integration, methods=["POST"]),
    Route("/api/settings/llm/{provider:str}/connect", api_connect_llm_provider, methods=["POST"]),
    Route("/api/settings/llm/{provider:str}/test", api_test_llm_provider, methods=["POST"]),
    Route("/api/settings/llm/{provider:str}/disconnect", api_disconnect_llm_provider, methods=["POST"]),
    Route("/api/discover/github/tree", api_discover_github_tree, methods=["POST"]),
    Route("/api/discover/analyst-brief", api_discover_analyst_brief, methods=["POST"]),
    Route("/api/discover/issues", api_discover_issues, methods=["POST"]),
    Route("/api/discover/linear/issues", api_discover_linear_issues, methods=["POST"]),
    Route("/api/settings/policies", api_save_policies, methods=["POST"]),
    Route("/api/settings/exceptions", api_add_policy_exception, methods=["POST"]),
    Route("/api/settings/exceptions/{exception_id:str}/resolve", api_resolve_policy_exception, methods=["POST"]),
    Route("/api/settings/rbac/roles/{role:str}", api_save_rbac_role, methods=["POST"]),
    Route("/api/settings/rbac/assignments", api_upsert_rbac_assignment, methods=["POST"]),
    Route("/api/settings/rbac/assignments/remove", api_remove_rbac_assignment, methods=["POST"]),
    Route("/api/context/versions", api_context_versions, methods=["GET"]),
    Route("/api/context/bundle", api_context_bundle, methods=["GET"]),
    Route("/api/context/contracts", api_context_contracts, methods=["GET"]),
    Route("/api/context/graph/neighbors", api_context_graph_neighbors, methods=["GET"]),
    Route("/api/context/traces", api_context_trace_ingest, methods=["POST"]),
    Route("/api/context/logs", api_context_log_ingest, methods=["POST"]),
    Route("/api/context/impact-forecast", api_context_impact_forecast, methods=["POST"]),
    Route("/api/context/drift/run", api_context_drift_run, methods=["POST"]),
    Route("/api/context/drift/reports", api_context_drift_reports, methods=["GET"]),
    Route("/api/agents", api_list_agents, methods=["GET"]),
    Route("/api/agents/clone", api_clone_agent, methods=["POST"]),
    Route("/api/teams", api_list_teams, methods=["GET"]),
    Route("/api/teams", api_save_team, methods=["POST"]),
    Route("/api/teams/suggest", api_suggest_team, methods=["POST"]),
    Route("/api/teams/{team_id:str}", api_get_team, methods=["GET"]),
    Route("/api/tasks", api_list_tasks, methods=["GET"]),
    Route("/api/tasks/{run_id:str}/clone", api_clone_task, methods=["GET"]),
    Route("/api/work-items", api_list_work_items, methods=["GET"]),
    Route("/api/work-items", api_create_work_item, methods=["POST"]),
    Route("/api/work-items/{item_id:str}/status", api_set_work_item_status, methods=["POST"]),
    Route("/api/runs", api_list_runs, methods=["GET"]),
    Route("/api/runs", api_start_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}", api_get_run, methods=["GET"]),
    Route("/api/runs/{run_id:str}/approve", api_approve_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}/analyst-doc", api_update_analyst_doc, methods=["POST"]),
    Route("/api/runs/{run_id:str}/artifacts", api_list_artifacts, methods=["GET"]),
    Route("/api/runs/{run_id:str}/artifacts/content", api_artifact_content, methods=["GET"]),
    Route("/api/runs/{run_id:str}/stream", api_run_stream, methods=["GET"]),
    Mount("/", app=StaticFiles(directory=str(STATIC_DIR), html=True), name="static"),
]

app = Starlette(routes=routes)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)


def main() -> None:
    import uvicorn

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8788"))
    uvicorn.run("web.server:app", host=host, port=port, reload=False)


if __name__ == "__main__":
    main()
