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
from utils.cloud_deployer import required_cloud_fields  # noqa: E402
from utils.artifacts import safe_name  # noqa: E402
from utils.llm import LLMClient  # noqa: E402
from utils.run_store import PipelineRunStore  # noqa: E402
from utils.team_store import TeamStore  # noqa: E402


RUN_STORE = PipelineRunStore(str(ROOT / "pipeline_runs"))
TEAM_STORE = TeamStore(str(ROOT / "team_data"))

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
        team_id: str = "",
        stage_agent_ids: dict[str, Any] | None = None,
    ) -> str:
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
        record.pipeline_state["human_approval"] = human_approval
        record.pipeline_state["strict_security_mode"] = strict_security_mode
        record.pipeline_state["team"] = team_meta
        record.pipeline_state["team_id"] = str(team_meta.get("id", ""))
        record.pipeline_state["team_name"] = str(team_meta.get("name", ""))
        record.pipeline_state["stage_agent_ids"] = dict(team_meta.get("stage_agent_ids", {}))
        record.pipeline_state["agent_personas"] = agent_personas
        self._append_log(record, f"▶ Pipeline started (run_id={run_id})")
        self._append_log(record, f"👥 Team selected: {record.team_name or 'Ad-hoc Team'}")
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

    def _execute_run(self, run_id: str) -> None:
        while True:
            record = self._get_record(run_id)
            if not record:
                return
            if record.status != "running":
                return
            if record.pending_approval is not None:
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


def _artifact_roots(run_id: str) -> dict[str, Path]:
    safe_run_id = safe_name(run_id)
    return {
        "pipeline": ROOT / "pipeline_runs" / run_id,
        "qa": ROOT / "run_artifacts" / safe_run_id,
        "deploy": ROOT / "deploy_output" / "runs" / safe_run_id,
    }


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
    for root_key, root in _artifact_roots(run_id).items():
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
    roots = _artifact_roots(run_id)
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


routes = [
    Route("/api/health", api_health, methods=["GET"]),
    Route("/api/samples", api_samples, methods=["GET"]),
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
