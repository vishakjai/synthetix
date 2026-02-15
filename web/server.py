from __future__ import annotations

import asyncio
import copy
import json
import os
import queue
import sys
import threading
import uuid
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


RUN_STORE = PipelineRunStore(str(ROOT / "pipeline_runs"))
TEAM_STORE = TeamStore(str(ROOT / "team_data"))
SETTINGS_STORE = SettingsStore(str(ROOT / "team_data"))
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

                for line in latest_result.get("logs", []):
                    self._append_log(record, line, timestamped=True)
                summary = latest_result.get("summary", "")
                status_icon = "✅" if latest_result.get("status") == "success" else "❌"
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
        self._persist(record)

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


def _config_from_payload(payload: dict[str, Any]) -> PipelineConfig:
    provider_raw = str(payload.get("provider", "anthropic")).strip().lower()
    if provider_raw not in {"anthropic", "openai"}:
        raise ValueError("provider must be 'anthropic' or 'openai'")

    provider = LLMProvider.ANTHROPIC if provider_raw == "anthropic" else LLMProvider.OPENAI
    api_key = str(payload.get("api_key", "")).strip()
    if not api_key:
        raise ValueError("api_key is required")

    model = str(payload.get("model", "")).strip()
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
    try:
        result = SETTINGS_STORE.test_integration(provider, actor=_request_actor(request))
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
    team_id = str(payload.get("team_id", "")).strip()
    stage_agent_ids = payload.get("stage_agent_ids", {}) if isinstance(payload.get("stage_agent_ids", {}), dict) else {}
    if not objectives:
        return JSONResponse({"ok": False, "error": "objectives are required"}, status_code=400)
    if use_case not in {"business_objectives", "code_modernization", "database_conversion"}:
        return JSONResponse(
            {"ok": False, "error": "use_case must be business_objectives, code_modernization, or database_conversion"},
            status_code=400,
        )
    if use_case == "code_modernization" and not legacy_code:
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
    Route("/api/runs", api_list_runs, methods=["GET"]),
    Route("/api/runs", api_start_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}", api_get_run, methods=["GET"]),
    Route("/api/runs/{run_id:str}/approve", api_approve_run, methods=["POST"]),
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
