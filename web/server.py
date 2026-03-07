from __future__ import annotations

import asyncio
import base64
import copy
import hashlib
import io
import json
import os
import queue
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
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

try:
    from google.cloud import tasks_v2  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    tasks_v2 = None

try:
    from google.cloud import storage  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    storage = None

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
from utils.run_store import build_pipeline_run_store, PipelineRunStore  # noqa: E402
from utils.settings_store import SettingsStore  # noqa: E402
from utils.team_store import TeamStore  # noqa: E402
from utils.work_item_store import WorkItemStore  # noqa: E402
from utils.domain_packs import (  # noqa: E402
    get_domain_pack,
    infer_data_classification,
    infer_domain_pack_id,
    infer_jurisdiction,
    list_domain_packs,
    map_to_capabilities,
    normalize_requirement,
    retrieve_gold_patterns,
)
from utils.persona_registry import PersonaRegistry  # noqa: E402
from utils.knowledge_gateway import KnowledgeGateway  # noqa: E402
from utils.tenant_memory import TenantMemoryStore  # noqa: E402
from utils.analyst_aas import AnalystAASService  # noqa: E402
from utils.analyst_docx import build_business_docx_bytes  # noqa: E402
from utils.analyst_report import build_analyst_report_v2, build_raw_artifact_set_v1  # noqa: E402
from utils.analyst_markdown_migration import migrate_markdown_to_analyst_output  # noqa: E402
from utils.landscape_router import build_greenfield_landscape_artifacts, build_landscape_artifacts  # noqa: E402
from utils.evidence_mode import create_evidence_bundle, load_evidence_bundle  # noqa: E402
from utils.delivery_constitution import (  # noqa: E402
    build_delivery_constitution_v1,
    delivery_constitution_to_markdown,
)
from utils.legacy_skills import (  # noqa: E402
    extract_vb6_signals as legacy_extract_vb6_signals,
    build_vb6_readiness_assessment,
    build_source_target_modernization_profile,
    build_project_business_summaries,
    infer_legacy_skill,
    list_legacy_skills,
    vb6_skill_pack_manifest,
)


TEAM_DATA_ROOT = Path(
    str(os.getenv("SYNTHETIX_TEAM_DATA_DIR", str(ROOT / "team_data"))).strip() or str(ROOT / "team_data")
).expanduser()
TEAM_DATA_ROOT.mkdir(parents=True, exist_ok=True)

RUN_STORE = build_pipeline_run_store(str(ROOT / "pipeline_runs"))
SETTINGS_STORE = SettingsStore(str(TEAM_DATA_ROOT))
TEAM_STORE = TeamStore(
    str(TEAM_DATA_ROOT),
    state_reader=SETTINGS_STORE.get_team_store_state,
    state_writer=lambda payload: SETTINGS_STORE.save_team_store_state(payload, actor="system"),
)
WORK_ITEM_STORE = WorkItemStore(str(TEAM_DATA_ROOT))
PERSONA_REGISTRY = PersonaRegistry(str(ROOT / "agent_personas"))
KNOWLEDGE_GATEWAY = KnowledgeGateway(str(ROOT))
TENANT_MEMORY_STORE = TenantMemoryStore(str(TEAM_DATA_ROOT / "tenant_memory"))
ANALYST_AAS = AnalystAASService(
    persona_registry=PERSONA_REGISTRY,
    knowledge_gateway=KNOWLEDGE_GATEWAY,
    memory_store=TENANT_MEMORY_STORE,
    settings_store=SETTINGS_STORE,
)
CONTEXT_VAULT_ROOT = ROOT / "context_vault"
CONTEXT_GRAPH_DB = CONTEXT_VAULT_ROOT / "context_graph.db"
CONTRACT_SCHEMA_DIR = ROOT / ".deliveryos" / "schemas"
DRIFT_LOCK = threading.Lock()
DRIFT_INTERVAL_SEC = max(0, int(os.getenv("SIL_DRIFT_INTERVAL_SEC", "900") or 900))
DOCGEN_ROOT = ROOT / "synthetix-docgen"
KNOWLEDGE_SOURCE_UPLOAD_ROOT = TEAM_DATA_ROOT / "knowledge_sources"
KNOWLEDGE_SOURCE_UPLOAD_MAX_BYTES = 30 * 1024 * 1024
RUN_CONTEXT_ARTIFACT_ROOT = ROOT / "run_artifacts"
ASYNC_RUN_QUEUE_ENABLED = str(os.getenv("ASYNC_RUN_QUEUE_ENABLED", "true")).strip().lower() in {"1", "true", "yes", "on"}
RUN_WORKER_TOKEN = str(os.getenv("RUN_WORKER_TOKEN", "")).strip()
RUN_WORKER_URL = str(os.getenv("RUN_WORKER_URL", "")).strip()
RUN_QUEUED_RESCUE_SEC = max(10, int(os.getenv("RUN_QUEUED_RESCUE_SEC", "25") or 25))
RUN_QUEUED_RESCUE_COOLDOWN_SEC = max(10, int(os.getenv("RUN_QUEUED_RESCUE_COOLDOWN_SEC", "60") or 60))
RUN_TASK_QUEUE_PATH = str(os.getenv("RUN_TASK_QUEUE_PATH", "")).strip()
RUN_STALE_RESUME_SEC = max(60, int(os.getenv("RUN_STALE_RESUME_SEC", "300") or 300))
REPO_SCAN_MAX_FILES = max(24, int(os.getenv("REPO_SCAN_MAX_FILES", "220") or 220))
REPO_SCAN_CHUNK_SIZE = max(8, int(os.getenv("REPO_SCAN_CHUNK_SIZE", "24") or 24))
REPO_SCAN_CHUNK_WORKERS = max(1, min(16, int(os.getenv("REPO_SCAN_CHUNK_WORKERS", "4") or 4)))
REPO_SCAN_BUNDLE_MAX_CHARS = max(120000, int(os.getenv("REPO_SCAN_BUNDLE_MAX_CHARS", "420000") or 420000))
REPO_SNAPSHOT_GCS_BUCKET = str(
    os.getenv("REPO_SNAPSHOT_GCS_BUCKET")
    or os.getenv("RUN_STORE_GCS_BUCKET")
    or os.getenv("SYNTHETIX_RUN_STORE_BUCKET")
    or ""
).strip()
REPO_SNAPSHOT_GCS_PREFIX = str(os.getenv("REPO_SNAPSHOT_GCS_PREFIX", "repo_snapshots")).strip() or "repo_snapshots"

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
STAGE_COLLAB_CHAT_LIMIT = 400
STAGE_COLLAB_DIRECTIVE_LIMIT = 240
STAGE_COLLAB_PROPOSAL_LIMIT = 320
STAGE_COLLAB_DECISION_LIMIT = 320
STAGE_CHAT_LLM_STAGES = {1, 2, 3, 6}
DISCOVER_REVIEW_MIN_COVERAGE = 0.85
DISCOVER_ANALYST_BRIEF_CACHE_TTL_SEC = max(0, int(os.getenv("DISCOVER_ANALYST_BRIEF_CACHE_TTL_SEC", "180") or 180))
DISCOVER_ANALYST_BRIEF_LOCK = asyncio.Lock()
DISCOVER_ANALYST_BRIEF_INFLIGHT: dict[str, asyncio.Task[dict[str, Any]]] = {}
DISCOVER_ANALYST_BRIEF_CACHE: dict[str, tuple[float, dict[str, Any]]] = {}


def _active_stage_indices_from_stage_map(stage_agent_ids: Any) -> list[int]:
    indices: list[int] = []
    if isinstance(stage_agent_ids, dict):
        for stage_raw, agent_id in stage_agent_ids.items():
            stage_str = str(stage_raw).strip()
            if not stage_str.isdigit():
                continue
            if not str(agent_id or "").strip():
                continue
            stage_num = int(stage_str)
            idx = stage_num - 1
            if 0 <= idx < len(AGENT_SEQUENCE):
                indices.append(idx)
    unique_sorted = sorted(set(indices))
    return unique_sorted if unique_sorted else list(range(len(AGENT_SEQUENCE)))


def _active_stage_indices_from_state(state: Any) -> list[int]:
    if isinstance(state, dict):
        return _active_stage_indices_from_stage_map(state.get("stage_agent_ids", {}))
    return list(range(len(AGENT_SEQUENCE)))


def _active_stage_numbers_from_state(state: Any) -> list[int]:
    return [idx + 1 for idx in _active_stage_indices_from_state(state)]


def _next_active_stage_idx(active_stage_indices: list[int], start_idx: int) -> int:
    target = max(0, int(start_idx or 0))
    for idx in active_stage_indices:
        if idx >= target:
            return idx
    return len(AGENT_SEQUENCE)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ts() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _discover_analyst_brief_cache_key(payload: dict[str, Any]) -> str:
    try:
        encoded = json.dumps(payload, sort_keys=True, separators=(",", ":"), default=str, ensure_ascii=True)
    except Exception:
        encoded = str(payload)
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def _prune_discover_analyst_brief_cache_locked(now: float) -> None:
    if DISCOVER_ANALYST_BRIEF_CACHE_TTL_SEC <= 0:
        DISCOVER_ANALYST_BRIEF_CACHE.clear()
        return
    stale_keys = [
        key
        for key, (stamp, _) in DISCOVER_ANALYST_BRIEF_CACHE.items()
        if now - float(stamp or 0.0) > DISCOVER_ANALYST_BRIEF_CACHE_TTL_SEC
    ]
    for key in stale_keys:
        DISCOVER_ANALYST_BRIEF_CACHE.pop(key, None)


async def _discover_analyst_brief_singleflight(
    cache_key: str,
    compute: Any,
) -> dict[str, Any]:
    now = time.monotonic()
    async with DISCOVER_ANALYST_BRIEF_LOCK:
        _prune_discover_analyst_brief_cache_locked(now)
        cached = DISCOVER_ANALYST_BRIEF_CACHE.get(cache_key)
        if cached and DISCOVER_ANALYST_BRIEF_CACHE_TTL_SEC > 0:
            return copy.deepcopy(cached[1])
        task = DISCOVER_ANALYST_BRIEF_INFLIGHT.get(cache_key)
        if task is None:
            task = asyncio.create_task(compute())
            DISCOVER_ANALYST_BRIEF_INFLIGHT[cache_key] = task
    try:
        result = await task
    except Exception:
        async with DISCOVER_ANALYST_BRIEF_LOCK:
            if DISCOVER_ANALYST_BRIEF_INFLIGHT.get(cache_key) is task:
                DISCOVER_ANALYST_BRIEF_INFLIGHT.pop(cache_key, None)
        raise

    async with DISCOVER_ANALYST_BRIEF_LOCK:
        if DISCOVER_ANALYST_BRIEF_INFLIGHT.get(cache_key) is task:
            DISCOVER_ANALYST_BRIEF_INFLIGHT.pop(cache_key, None)
        if DISCOVER_ANALYST_BRIEF_CACHE_TTL_SEC > 0:
            DISCOVER_ANALYST_BRIEF_CACHE[cache_key] = (time.monotonic(), copy.deepcopy(result))
            _prune_discover_analyst_brief_cache_locked(time.monotonic())
    return copy.deepcopy(result)


def _workflow_state_for_stage(stage_num: int) -> str:
    if stage_num <= 0:
        return "DISCOVERING"
    if stage_num == 1:
        return "DISCOVERED"
    if stage_num == 2:
        return "PLANNED"
    if stage_num <= 5:
        return "IN_BUILD"
    if stage_num <= 7:
        return "VERIFIED_BUILD"
    return "DEPLOYED"


def _analyst_output_from_state(state: dict[str, Any]) -> dict[str, Any]:
    def _is_full_output(candidate: Any) -> bool:
        return isinstance(candidate, dict) and bool(
            candidate.get("raw_artifacts")
            or candidate.get("analyst_report_v2")
            or candidate.get("requirements_pack")
            or candidate.get("project_name")
            or candidate.get("vb6_analysis")
        )

    if not isinstance(state, dict):
        return {}
    direct = state.get("analyst_output", {})
    if _is_full_output(direct):
        return direct
    results = state.get("agent_results", [])
    if isinstance(results, list):
        for row in reversed(results):
            if not isinstance(row, dict):
                continue
            if int(row.get("stage", 0) or 0) != 1:
                continue
            output = row.get("output", {})
            if _is_full_output(output):
                return output
    run_id = str(state.get("run_id", "")).strip()
    if run_id:
        try:
            snapshot = RUN_STORE.load_stage_snapshot(run_id, 1)
        except Exception:
            snapshot = None
        if isinstance(snapshot, dict):
            result = snapshot.get("result", {})
            if isinstance(result, dict):
                output = result.get("output", {})
                if _is_full_output(output):
                    return output
            snap_state = snapshot.get("pipeline_state", {})
            if isinstance(snap_state, dict):
                direct = snap_state.get("analyst_output", {})
                if _is_full_output(direct):
                    return direct
                snap_results = snap_state.get("agent_results", [])
                if isinstance(snap_results, list):
                    for row in reversed(snap_results):
                        if not isinstance(row, dict):
                            continue
                        if int(row.get("stage", 0) or 0) != 1:
                            continue
                        output = row.get("output", {})
                        if _is_full_output(output):
                            return output
    return {}


def _review_check_row(
    *,
    check_id: str,
    status: str,
    title: str,
    detail: str,
    severity: str = "medium",
    source: str = "derived",
) -> dict[str, Any]:
    normalized_status = str(status or "warn").strip().lower()
    if normalized_status not in {"pass", "warn", "fail"}:
        normalized_status = "warn"
    normalized_severity = str(severity or "medium").strip().lower()
    if normalized_severity not in {"blocker", "high", "medium", "low"}:
        normalized_severity = "medium"
    return {
        "id": str(check_id or f"review_{uuid.uuid4().hex[:8]}").strip(),
        "status": normalized_status,
        "severity": normalized_severity,
        "title": str(title or "Discover review check").strip(),
        "detail": str(detail or "").strip(),
        "source": str(source or "derived").strip(),
    }


def _discover_review_state(state: dict[str, Any]) -> dict[str, Any]:
    analyst_output = _analyst_output_from_state(state)
    raw_artifacts = analyst_output.get("raw_artifacts", {}) if isinstance(analyst_output.get("raw_artifacts", {}), dict) else {}
    report = analyst_output.get("analyst_report_v2", {}) if isinstance(analyst_output.get("analyst_report_v2", {}), dict) else {}
    report_review = report.get("discover_review", {}) if isinstance(report.get("discover_review", {}), dict) else {}
    raw_checklist = raw_artifacts.get("discover_review_checklist", {}) if isinstance(raw_artifacts.get("discover_review_checklist", {}), dict) else {}
    prior_review = state.get("discover_review", {}) if isinstance(state.get("discover_review", {}), dict) else {}

    resolved_ids = {
        str(x).strip()
        for x in prior_review.get("resolved_ids", [])
        if str(x).strip()
    } if isinstance(prior_review.get("resolved_ids", []), list) else set()
    waived_ids = {
        str(x).strip()
        for x in prior_review.get("waived_ids", [])
        if str(x).strip()
    } if isinstance(prior_review.get("waived_ids", []), list) else set()

    rows_by_id: dict[str, dict[str, Any]] = {}

    def add_row(row: dict[str, Any]) -> None:
        rid = str(row.get("id", "")).strip()
        if not rid:
            return
        rows_by_id[rid] = row

    source_rows: list[Any] = []
    if isinstance(report_review.get("checks", []), list):
        source_rows.extend(report_review.get("checks", []))
    if isinstance(raw_checklist.get("checks", []), list):
        source_rows.extend(raw_checklist.get("checks", []))
    if isinstance(raw_checklist.get("items", []), list):
        source_rows.extend(raw_checklist.get("items", []))

    for idx, row in enumerate(source_rows, start=1):
        if not isinstance(row, dict):
            continue
        check_id = str(row.get("id", "")).strip() or f"check_{idx}"
        title = str(
            row.get("title")
            or row.get("check")
            or row.get("why")
            or row.get("action")
            or row.get("detail")
            or "Discover review check"
        ).strip()
        detail = str(row.get("detail") or row.get("notes") or row.get("why") or row.get("action") or "").strip()
        add_row(
            _review_check_row(
                check_id=check_id,
                status=str(row.get("status") or row.get("result") or "warn"),
                severity=str(row.get("severity") or "medium"),
                title=title,
                detail=detail,
                source="artifact",
            )
        )

    repo_landscape = raw_artifacts.get("repo_landscape", {}) if isinstance(raw_artifacts.get("repo_landscape", {}), dict) else {}
    variant_inventory = raw_artifacts.get("variant_inventory", {}) if isinstance(raw_artifacts.get("variant_inventory", {}), dict) else {}
    variant_diff = raw_artifacts.get("variant_diff_report", {}) if isinstance(raw_artifacts.get("variant_diff_report", {}), dict) else {}
    scope_lock = raw_artifacts.get("scope_lock", {}) if isinstance(raw_artifacts.get("scope_lock", {}), dict) else {}
    data_access_map = raw_artifacts.get("data_access_map", {}) if isinstance(raw_artifacts.get("data_access_map", {}), dict) else {}
    reporting_model = raw_artifacts.get("reporting_model", {}) if isinstance(raw_artifacts.get("reporting_model", {}), dict) else {}
    legacy_inventory = raw_artifacts.get("legacy_inventory", {}) if isinstance(raw_artifacts.get("legacy_inventory", {}), dict) else {}

    project_count = max(
        int(variant_diff.get("project_count", 0) or 0),
        len(repo_landscape.get("projects", [])) if isinstance(repo_landscape.get("projects", []), list) else 0,
        len(variant_inventory.get("variants", [])) if isinstance(variant_inventory.get("variants", []), list) else 0,
    )
    scope_decision = str(scope_lock.get("decision") or "").strip()
    scope_status = str(scope_lock.get("status") or "").strip().lower()
    scope_resolved = bool(scope_decision) or scope_status in {"locked", "approved", "resolved"}
    if project_count > 1:
        add_row(
            _review_check_row(
                check_id="variant_scope_lock",
                status="pass" if scope_resolved else "fail",
                severity="blocker",
                title="Variant scope decision",
                detail=(
                    f"Detected {project_count} project variants. "
                    + (f"Scope lock set: {scope_decision or scope_status}" if scope_resolved else "No scope lock decision captured.")
                ),
            )
        )

    map_complete = bool(data_access_map.get("complete", False))
    map_rows = data_access_map.get("rows", [])
    map_row_count = len(map_rows) if isinstance(map_rows, list) else 0
    map_coverage = float(data_access_map.get("coverage_score", 0) or 0)
    if map_row_count > 0 or isinstance(data_access_map, dict):
        add_row(
            _review_check_row(
                check_id="data_access_map_complete",
                status="pass" if (map_complete and map_coverage >= 1.0) else ("warn" if map_coverage >= 0.8 else "fail"),
                severity="high",
                title="Canonical data access map completeness",
                detail=f"complete={map_complete}, rows={map_row_count}, coverage={map_coverage:.2f}",
            )
        )

    unknown_bindings = reporting_model.get("unknown_bindings", [])
    unknown_count = len(unknown_bindings) if isinstance(unknown_bindings, list) else 0
    add_row(
        _review_check_row(
            check_id="reporting_reconciliation",
            status="fail" if unknown_count > 0 else "pass",
            severity="high",
            title="Reporting model reconciliation",
            detail=(
                f"Unknown reporting bindings={unknown_count}."
                if unknown_count > 0
                else "All detected report bindings reconciled."
            ),
        )
    )

    form_coverage = legacy_inventory.get("form_coverage", [])
    low_coverage = []
    if isinstance(form_coverage, list):
        for row in form_coverage:
            if not isinstance(row, dict):
                continue
            score = float(row.get("coverage_score", 0) or 0)
            if score < DISCOVER_REVIEW_MIN_COVERAGE:
                form_name = str(row.get("form_name") or row.get("form") or "unknown").strip()
                low_coverage.append(f"{form_name}={score:.2f}")
    add_row(
        _review_check_row(
            check_id="handler_inventory_coverage",
            status="fail" if low_coverage else "pass",
            severity="high",
            title="Handler inventory completeness",
            detail=(
                "Forms below threshold (<0.85): " + ", ".join(low_coverage[:12])
                if low_coverage
                else "All forms meet coverage threshold (>=0.85)."
            ),
        )
    )

    if not rows_by_id:
        add_row(
            _review_check_row(
                check_id="discover_review_checklist_missing",
                status="fail",
                severity="blocker",
                title="Discover review checklist missing",
                detail="No discover review checks were generated from Analyst artifacts.",
            )
        )

    checks = list(rows_by_id.values())
    blocking = [row for row in checks if str(row.get("status", "")).lower() == "fail"]
    unresolved_blocking = [
        row for row in blocking
        if str(row.get("id", "")).strip() not in resolved_ids
        and str(row.get("id", "")).strip() not in waived_ids
    ]
    unresolved_non_pass = [
        row for row in checks
        if str(row.get("status", "")).lower() != "pass"
        and str(row.get("id", "")).strip() not in resolved_ids
        and str(row.get("id", "")).strip() not in waived_ids
    ]
    overall_status = "FAIL" if unresolved_blocking else ("WARN" if unresolved_non_pass else "PASS")
    return {
        "overall_status": overall_status,
        "checks": checks,
        "blocking": blocking,
        "unresolved_blocking": unresolved_blocking,
        "resolved_ids": sorted(resolved_ids),
        "waived_ids": sorted(waived_ids),
        "updated_at": _utc_now(),
    }


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
    progress_log_count: int = 0
    pipeline_state: dict[str, Any] | None = None
    error_message: str | None = None
    retry_count: int = 0
    next_stage_idx: int = 0
    pending_approval: dict[str, Any] | None = None
    thread: threading.Thread | None = None
    last_persist_monotonic: float = 0.0


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
            "deploy_output_dir": cfg.deploy_output_dir,
            "cluster_name": cfg.cluster_name,
            "namespace": cfg.namespace,
        }

    @staticmethod
    def _config_from_summary(summary: dict[str, Any]) -> PipelineConfig:
        provider_raw = str(summary.get("provider", LLMProvider.ANTHROPIC.value)).strip().lower()
        provider = LLMProvider.OPENAI if provider_raw == LLMProvider.OPENAI.value else LLMProvider.ANTHROPIC
        model = str(summary.get("model", "")).strip()
        cfg = PipelineConfig(
            provider=provider,
            temperature=float(summary.get("temperature", 0.3) or 0.3),
            developer_parallel_agents=int(summary.get("developer_parallel_agents", 5) or 5),
            max_retries=int(summary.get("max_retries", 2) or 2),
            live_deploy=bool(summary.get("live_deploy", False)),
            deploy_output_dir=str(summary.get("deploy_output_dir", "./deploy_output") or "./deploy_output"),
            cluster_name=str(summary.get("cluster_name", "agent-pipeline") or "agent-pipeline"),
            namespace=str(summary.get("namespace", "agent-app") or "agent-app"),
        )
        if provider == LLMProvider.OPENAI:
            cfg.openai_model = model or cfg.openai_model
        else:
            cfg.anthropic_model = model or cfg.anthropic_model
        return cfg

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
        *,
        run_id_override: str = "",
        defer_execution: bool = False,
        run_inline: bool = False,
    ) -> str:
        integration = integration_context if isinstance(integration_context, dict) else {}
        project_state_mode = str(integration.get("project_state_mode", "auto")).strip().lower() or "auto"
        project_state_detected = str(integration.get("project_state_detected", "")).strip().lower()
        scan_scope = integration.get("scan_scope", {}) if isinstance(integration.get("scan_scope", {}), dict) else {}
        modernization_source_mode = str(scan_scope.get("modernization_source_mode", "manual")).strip().lower() or "manual"
        evidence_mode = modernization_source_mode in {"evidence", "hybrid"}
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

        run_status = "queued" if defer_execution else "running"
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
            run_id=str(run_id_override or "").strip() or None,
            initial_status=run_status,
        )
        run_context_bundle: dict[str, Any] = {}
        constitution: dict[str, Any] = {}
        routing: dict[str, Any] = {}
        if not defer_execution:
            run_context_bundle = _build_run_context_bundle(
                run_id=run_id,
                objectives=objectives,
                use_case=use_case,
                integration_context=integration,
                legacy_code=legacy_code,
                stage_agent_ids=team_meta.get("stage_agent_ids", {}),
            )
            run_context_bundle = _persist_run_context_bundle_artifacts(run_context_bundle)
            constitution = _as_dict_safe(run_context_bundle.get("delivery_constitution"))
            routing = _as_dict_safe(run_context_bundle.get("specialist_routing"))
            integration["run_context_snapshot"] = {
                "bundle_id": _clean_text(run_context_bundle.get("bundle_id")),
                "knowledge_snapshot_id": _clean_text(_as_dict_safe(run_context_bundle.get("knowledge_context")).get("snapshot_id")),
                "delivery_constitution_id": _clean_text(constitution.get("constitution_id")),
                "selected_specialist_ids": _as_list_safe(routing.get("selected_specialist_ids")),
            }

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
        record.status = run_status
        active_stage_indices = _active_stage_indices_from_stage_map(team_meta.get("stage_agent_ids", {}))
        active_stage_numbers = [idx + 1 for idx in active_stage_indices]
        record.stage_status = {
            stage_num: ("pending" if (stage_num - 1) in active_stage_indices else "skipped_team")
            for stage_num in range(1, TOTAL_STAGES + 1)
        }
        record.next_stage_idx = _next_active_stage_idx(active_stage_indices, 0)
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
        record.pipeline_state["active_stage_numbers"] = list(active_stage_numbers)
        record.pipeline_state["sil_ready"] = False
        record.pipeline_state["context_layer_status"] = "pending"
        record.pipeline_state["system_context_model"] = {}
        record.pipeline_state["convention_profile"] = {}
        record.pipeline_state["health_assessment"] = {}
        record.pipeline_state["remediation_backlog"] = []
        record.pipeline_state["context_vault_ref"] = {}
        imported_seed = _imported_analysis_response_payload(objectives, integration) if evidence_mode else {}
        sil_discovery = _evidence_sil_discovery(imported_seed) if imported_seed else {}
        if imported_seed:
            discover_cache = integration.get("discover_cache", {}) if isinstance(integration.get("discover_cache", {}), dict) else {}
            discover_cache["analyst_source"] = str(imported_seed.get("source", "imported_analysis"))
            discover_cache["analyst_summary"] = (
                imported_seed.get("analyst_brief", {}).get("summary", {})
                if isinstance(imported_seed.get("analyst_brief", {}), dict)
                else {}
            )
            integration["discover_cache"] = discover_cache
            record.pipeline_state["legacy_compact_context"] = {
                "inventory": imported_seed.get("legacy_code_inventory", {}) if isinstance(imported_seed.get("legacy_code_inventory", {}), dict) else {},
                "legacy_skill_profile": imported_seed.get("legacy_skill_profile", {}) if isinstance(imported_seed.get("legacy_skill_profile", {}), dict) else {},
            }
        record.pipeline_state["sil_discovery"] = sil_discovery if sil_discovery else ({} if defer_execution else discover_repo_snapshot(ROOT))
        record.pipeline_state["run_context_bundle"] = run_context_bundle
        record.pipeline_state["workflow_state"] = "QUEUED" if defer_execution else "DISCOVERING"
        record.pipeline_state["discover_review"] = {
            "overall_status": "PENDING",
            "checks": [],
            "blocking": [],
            "unresolved_blocking": [],
            "resolved_ids": [],
            "waived_ids": [],
            "updated_at": _utc_now(),
        }
        if defer_execution:
            record.pipeline_state["queued_request"] = {
                "objectives": objectives,
                "use_case": use_case,
                "legacy_code": legacy_code,
                "modernization_language": modernization_language,
                "database_source": database_source,
                "database_target": database_target,
                "database_schema": database_schema,
                "human_approval": human_approval,
                "strict_security_mode": strict_security_mode,
                "deployment_target": deployment_target,
                "cloud_config": cloud_config or {},
                "integration_context": integration,
                "team_id": str(team_meta.get("id", "")),
                "stage_agent_ids": dict(team_meta.get("stage_agent_ids", {})),
                "config_summary": self._config_summary(config),
            }
            self._append_log(record, f"🕒 Pipeline queued (run_id={run_id})")
        else:
            self._append_log(record, f"▶ Pipeline started (run_id={run_id})")
        self._append_log(record, f"👥 Team selected: {record.team_name or 'Ad-hoc Team'}")
        self._append_log(record, f"🧩 Active build stages: {', '.join([str(x) for x in active_stage_numbers])}")
        self._append_log(record, "🧠 System Intelligence Layer scheduled (SCM / CP / HA-RB)")
        selected_specialists = _as_list_safe(routing.get("selected"))
        if selected_specialists:
            specialist_names = ", ".join(
                [
                    _clean_text(_as_dict_safe(row).get("name"))
                    for row in selected_specialists[:6]
                    if _clean_text(_as_dict_safe(row).get("name"))
                ]
            )
            self._append_log(record, f"🧭 Specialist routing selected {len(selected_specialists)} profile(s): {specialist_names or 'see run context bundle'}")
        else:
            self._append_log(record, "🧭 Specialist routing selected 0 profiles for current run context.")
        self._append_log(record, "💬 Stage collaboration enabled (chat, directives, proposals, decisions)")

        with self._lock:
            self._records[run_id] = record

        self.store.finalize_run(
            run_id=run_id,
            status=run_status,
            pipeline_state=record.pipeline_state,
            stage_status=record.stage_status,
            progress_logs=record.progress_logs,
            error_message=None,
        )

        if defer_execution:
            return run_id

        if run_inline:
            self._execute_run(run_id)
            return run_id

        thread = threading.Thread(
            target=self._execute_run,
            args=(run_id,),
            daemon=True,
            name=f"pipeline-run-{run_id}",
        )
        record.thread = thread
        thread.start()
        return run_id

    def launch_deferred_run(self, run_id: str) -> dict[str, Any]:
        rid = str(run_id or "").strip()
        if not rid:
            return {"ok": False, "error": "run_id is required"}

        persisted = self.store.load_run(rid) or {}
        if not isinstance(persisted, dict) or not persisted:
            return {"ok": False, "error": "run not found"}
        status = str(persisted.get("pipeline_status", "")).strip().lower()
        if status == "running":
            meta = self.store.load_meta(rid) or {}
            updated_raw = str(meta.get("updated_at", "")).strip() or str(persisted.get("saved_at", "")).strip()
            stale = False
            if updated_raw:
                try:
                    ts = datetime.fromisoformat(updated_raw.replace("Z", "+00:00"))
                    stale = (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds() >= RUN_STALE_RESUME_SEC
                except Exception:
                    stale = False
            if not stale:
                return {"ok": True, "status": status, "run_id": rid}

            record = self._get_record(rid) or self._hydrate_record(rid)
            if not record:
                return {"ok": False, "error": "stale running run could not be hydrated"}
            if record.pending_approval is not None:
                return {"ok": True, "status": "waiting_approval", "run_id": rid}
            self._append_log(
                record,
                f"♻️ Stale running run detected (>={RUN_STALE_RESUME_SEC}s since heartbeat). Worker is resuming execution inline.",
            )
            self._persist(record)
            self._execute_run(rid)
            refreshed = self.store.load_run(rid) or {}
            refreshed_status = str(refreshed.get("pipeline_status", "running")).strip().lower() or "running"
            return {"ok": True, "status": refreshed_status, "run_id": rid, "resumed": True}

        if status in {"completed", "failed", "aborted"}:
            return {"ok": True, "status": status, "run_id": rid}

        pipeline_state = persisted.get("pipeline_state", {}) if isinstance(persisted.get("pipeline_state", {}), dict) else {}
        queued = pipeline_state.get("queued_request", {}) if isinstance(pipeline_state.get("queued_request", {}), dict) else {}
        if not queued:
            return {"ok": False, "error": "queued request payload missing"}

        cfg_summary = queued.get("config_summary", {}) if isinstance(queued.get("config_summary", {}), dict) else {}
        try:
            cfg = self._build_config_from_summary(cfg_summary)
        except Exception as exc:
            return {"ok": False, "error": f"cannot launch deferred run: {exc}"}
        self.start_run(
            objectives=str(queued.get("objectives", "")),
            config=cfg,
            use_case=str(queued.get("use_case", "business_objectives")),
            legacy_code=str(queued.get("legacy_code", "")),
            modernization_language=str(queued.get("modernization_language", "")),
            database_source=str(queued.get("database_source", "")),
            database_target=str(queued.get("database_target", "")),
            database_schema=str(queued.get("database_schema", "")),
            human_approval=bool(queued.get("human_approval", False)),
            strict_security_mode=bool(queued.get("strict_security_mode", False)),
            deployment_target=str(queued.get("deployment_target", "local") or "local"),
            cloud_config=queued.get("cloud_config", {}) if isinstance(queued.get("cloud_config", {}), dict) else {},
            integration_context=queued.get("integration_context", {}) if isinstance(queued.get("integration_context", {}), dict) else {},
            team_id=str(queued.get("team_id", "")),
            stage_agent_ids=queued.get("stage_agent_ids", {}) if isinstance(queued.get("stage_agent_ids", {}), dict) else {},
            run_id_override=rid,
            defer_execution=False,
            run_inline=True,
        )
        refreshed = self.store.load_run(rid) or {}
        refreshed_status = str(refreshed.get("pipeline_status", "running")).strip().lower() or "running"
        return {"ok": True, "status": refreshed_status, "run_id": rid}

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
            run_context_bundle = (
                record.pipeline_state.get("run_context_bundle", {})
                if isinstance(record.pipeline_state.get("run_context_bundle", {}), dict)
                else {}
            )
            if isinstance(contract_suite.get("context_bundle", {}), dict):
                bundle = dict(contract_suite.get("context_bundle", {}))
                bundle["run_context_bundle_ref"] = {
                    "bundle_id": _clean_text(run_context_bundle.get("bundle_id")),
                    "knowledge_snapshot_id": _clean_text(
                        _as_dict_safe(run_context_bundle.get("knowledge_context")).get("snapshot_id")
                    ),
                    "delivery_constitution_id": _clean_text(
                        _as_dict_safe(_as_dict_safe(run_context_bundle.get("delivery_constitution"))).get("constitution_id")
                    ),
                    "workspace": _clean_text(run_context_bundle.get("workspace")),
                    "project": _clean_text(run_context_bundle.get("project")),
                }
                bundle["knowledge_snapshot"] = {
                    "source_version_ids": _as_dict_safe(
                        _as_dict_safe(run_context_bundle.get("knowledge_context")).get("integrity")
                    ).get("source_version_ids", []),
                    "source_count": _as_dict_safe(
                        _as_dict_safe(run_context_bundle.get("knowledge_context")).get("integrity")
                    ).get("source_count", 0),
                }
                contract_suite["context_bundle"] = bundle
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

    def _ensure_repo_scan_legacy_code(self, record: RunRecord) -> bool:
        if str(record.use_case).strip().lower() != "code_modernization":
            return True
        if str(record.legacy_code or "").strip():
            return True
        integration = record.integration_context if isinstance(record.integration_context, dict) else {}
        scan_scope = integration.get("scan_scope", {}) if isinstance(integration.get("scan_scope", {}), dict) else {}
        mode = str(scan_scope.get("modernization_source_mode", "manual")).strip().lower() or "manual"
        if mode != "repo_scan":
            return True

        self._append_log(record, "📡 Repo scan started (background source extraction)")
        legacy_code, cache, err = _resolve_legacy_code_from_repo_scan(
            integration,
            progress_cb=lambda msg: self._append_log(record, str(msg)),
        )
        if err:
            self._fail(record, f"Repo scan failed before Stage 1: {err}")
            return False
        record.legacy_code = legacy_code
        record.integration_context = integration
        if isinstance(cache, dict) and cache:
            integration["repo_scan_cache"] = cache
        if isinstance(record.pipeline_state, dict):
            record.pipeline_state["legacy_code"] = legacy_code
            record.pipeline_state["integration_context"] = integration
            if isinstance(cache.get("repo_snapshot", {}), dict):
                record.pipeline_state["repo_snapshot"] = copy.deepcopy(cache.get("repo_snapshot", {}))
        self._append_log(record, f"✅ Repo scan extracted source bundle ({len(legacy_code)} chars)")
        self._persist(record)
        return True

    def _execute_run(self, run_id: str) -> None:
        while True:
            record = self._get_record(run_id)
            if not record:
                return
            if record.status != "running":
                return
            if record.pending_approval is not None:
                return

            if not self._ensure_repo_scan_legacy_code(record):
                return

            if not record.pipeline_state or not record.pipeline_state.get("sil_ready"):
                if not self._run_context_layer(record):
                    return

            active_stage_indices = _active_stage_indices_from_state(record.pipeline_state or {})
            stage_idx = _next_active_stage_idx(active_stage_indices, int(record.next_stage_idx))
            record.next_stage_idx = stage_idx
            if stage_idx >= len(AGENT_SEQUENCE):
                record.status = "completed"
                record.updated_at = _utc_now()
                if record.pipeline_state is not None:
                    record.pipeline_state["pipeline_status"] = "completed"
                    record.pipeline_state["workflow_state"] = (
                        "DEPLOYED" if DEPLOYMENT_STAGE_INDEX in active_stage_indices else "COMPLETED"
                    )
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

            # Discover review gate: Stage 2+ cannot proceed with unresolved blocking checks.
            if stage_idx >= 1 and isinstance(record.pipeline_state, dict):
                discover_review = _discover_review_state(record.pipeline_state)
                record.pipeline_state["discover_review"] = discover_review
                if discover_review.get("unresolved_blocking"):
                    record.pending_approval = {
                        "type": "discover_review",
                        "stage": 1,
                        "next_stage": 2,
                        "message": (
                            "Discover review has unresolved blocking items. "
                            "Resolve or waive blockers before moving beyond Discover."
                        ),
                        "overall_status": discover_review.get("overall_status", "FAIL"),
                        "unresolved_blocking": discover_review.get("unresolved_blocking", [])[:20],
                        "checklist": discover_review.get("checks", [])[:60],
                    }
                    record.status = "waiting_approval"
                    record.pipeline_state["workflow_state"] = "IN_REVIEW"
                    self._append_log(
                        record,
                        "🧭 Discover review blocked progression: "
                        f"{len(discover_review.get('unresolved_blocking', []))} unresolved blocking item(s).",
                    )
                    self._persist(record)
                    return
                if record.pipeline_state.get("workflow_state") in {"DISCOVERED", "IN_REVIEW", "DISCOVERING"}:
                    record.pipeline_state["workflow_state"] = "VERIFIED"

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
                if stage_num == 1 and isinstance(latest_result, dict):
                    stage_output = latest_result.get("output", {})
                    if not isinstance(stage_output, dict):
                        stage_output = {}
                    enriched_output = _ensure_analyst_report_v2(stage_output)
                    run_context_bundle = (
                        updated_state.get("run_context_bundle", {})
                        if isinstance(updated_state.get("run_context_bundle", {}), dict)
                        else (
                            record.pipeline_state.get("run_context_bundle", {})
                            if isinstance(record.pipeline_state.get("run_context_bundle", {}), dict)
                            else {}
                        )
                    )
                    enriched_output, guardrails = _enforce_analyst_source_guardrails(
                        enriched_output,
                        run_context_bundle,
                    )
                    latest_result["output"] = enriched_output
                    guardrail_status = _clean_text(_as_dict_safe(guardrails).get("status")).upper() or "PASS"
                    if guardrail_status == "FAIL":
                        reasons = _as_list_safe(guardrails.get("reasons"))
                        reason_text = ", ".join([str(item) for item in reasons if str(item).strip()][:5]) or "guardrail violation"
                        message = f"Analyst source guardrails failed: {reason_text}"
                        latest_result["status"] = "error"
                        latest_result["summary"] = message
                        stage_logs = _as_list_safe(latest_result.get("logs"))
                        stage_logs.append(message)
                        latest_result["logs"] = stage_logs[-80:]
                        updated_state["pipeline_status"] = "failed"
                    updated_state[output_key] = enriched_output
                    agent_results = updated_state.get("agent_results", [])
                    if isinstance(agent_results, list) and agent_results:
                        agent_results[-1] = latest_result
                        updated_state["agent_results"] = agent_results
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
                if isinstance(record.pipeline_state, dict):
                    record.pipeline_state["workflow_state"] = _workflow_state_for_stage(stage_num)
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
                        if DEVELOPER_STAGE_INDEX not in active_stage_indices:
                            self._fail(
                                record,
                                "Quality gate failed but Developer stage is not part of the selected team; cannot execute retry loop.",
                            )
                            return
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
                        record.stage_status[DATABASE_STAGE_NUM] = (
                            "pending" if (DATABASE_STAGE_NUM - 1) in active_stage_indices else "skipped_team"
                        )
                        record.stage_status[SECURITY_STAGE_NUM] = (
                            "pending" if (SECURITY_STAGE_NUM - 1) in active_stage_indices else "skipped_team"
                        )
                        record.stage_status[TESTER_STAGE_NUM] = (
                            "pending" if (TESTER_STAGE_NUM - 1) in active_stage_indices else "skipped_team"
                        )
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

                next_active_idx = _next_active_stage_idx(active_stage_indices, stage_idx + 1)
                record.next_stage_idx = next_active_idx

                # Human approval gate after each completed stage
                if record.human_approval and next_active_idx < len(AGENT_SEQUENCE):
                    next_stage_num = next_active_idx + 1
                    record.pending_approval = {
                        "type": "stage_gate",
                        "stage": stage_num,
                        "next_stage": next_stage_num,
                        "message": f"Approve transition from Stage {stage_num} to Stage {next_stage_num}",
                    }
                    record.status = "waiting_approval"
                    record.stage_status[stage_num] = latest_result.get("status", "success")
                    self._append_log(
                        record,
                        f"🛑 Human approval required before Stage {next_stage_num}",
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
            record.pipeline_state["workflow_state"] = "FAILED"
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
        record.updated_at = _utc_now()
        if record.pipeline_state is not None:
            record.pipeline_state["pending_approval"] = copy.deepcopy(record.pending_approval)
            record.pipeline_state["next_stage_idx"] = int(record.next_stage_idx)
            record.pipeline_state["current_stage"] = int(record.current_stage)
        self.store.finalize_run(
            run_id=record.run_id,
            status=record.status,
            pipeline_state=record.pipeline_state,
            stage_status=record.stage_status,
            progress_logs=record.progress_logs,
            error_message=record.error_message,
        )
        record.last_persist_monotonic = time.monotonic()
        snapshot = self._record_payload(record)
        self._emit_event(record.run_id, "update", {"run": snapshot})
        if record.status != "running":
            self._emit_event(record.run_id, "done", {"status": record.status, "run_id": record.run_id})

    def _append_log(self, record: RunRecord, message: str, timestamped: bool = False) -> None:
        line = message if timestamped else f"[{_ts()}] {message}"
        record.progress_logs.append(line)
        record.progress_log_count = max(int(record.progress_log_count or 0) + 1, len(record.progress_logs))
        record.updated_at = _utc_now()
        if hasattr(self.store, "append_log_event"):
            try:
                self.store.append_log_event(record.run_id, line)
            except Exception:
                pass
        self._emit_event(record.run_id, "log", {"line": line, "run_id": record.run_id})
        now_mono = time.monotonic()
        should_persist = (
            record.status in {"queued", "running", "waiting_approval", "paused"}
            and (
                str(os.getenv("RUN_STORE_BACKEND", "local")).strip().lower() == "firestore"
                or (now_mono - float(record.last_persist_monotonic or 0.0)) >= 1.25
                or len(record.progress_logs) <= 3
                or len(record.progress_logs) % 20 == 0
            )
        )
        if should_persist:
            if record.pipeline_state is not None:
                record.pipeline_state["pending_approval"] = copy.deepcopy(record.pending_approval)
                record.pipeline_state["next_stage_idx"] = int(record.next_stage_idx)
                record.pipeline_state["current_stage"] = int(record.current_stage)
            self.store.finalize_run(
                run_id=record.run_id,
                status=record.status,
                pipeline_state=record.pipeline_state,
                stage_status=record.stage_status,
                progress_logs=record.progress_logs,
                error_message=record.error_message,
            )
            record.last_persist_monotonic = now_mono

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
        record = self._hydrate_record(run_id) or self._get_record(run_id)
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
        record = self._hydrate_record(run_id)
        if not record:
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
        elif pending_type == "discover_review":
            review = _discover_review_state(record.pipeline_state if isinstance(record.pipeline_state, dict) else {})
            resolved_ids = {
                str(x).strip()
                for x in payload.get("resolved_ids", [])
                if str(x).strip()
            } if isinstance(payload.get("resolved_ids", []), list) else set()
            waived_ids = {
                str(x).strip()
                for x in payload.get("waived_ids", [])
                if str(x).strip()
            } if isinstance(payload.get("waived_ids", []), list) else set()
            # Backward-compatible behavior for existing approve UI:
            # approving discover_review with no explicit ids waives current blockers.
            if not resolved_ids and not waived_ids:
                waived_ids = {
                    str(row.get("id", "")).strip()
                    for row in review.get("unresolved_blocking", [])
                    if isinstance(row, dict) and str(row.get("id", "")).strip()
                }
            prior_resolved = {
                str(x).strip()
                for x in review.get("resolved_ids", [])
                if str(x).strip()
            }
            prior_waived = {
                str(x).strip()
                for x in review.get("waived_ids", [])
                if str(x).strip()
            }
            merged_resolved = sorted(prior_resolved | resolved_ids)
            merged_waived = sorted(prior_waived | waived_ids)
            if isinstance(record.pipeline_state, dict):
                record.pipeline_state["discover_review"] = {
                    **review,
                    "resolved_ids": merged_resolved,
                    "waived_ids": merged_waived,
                    "approved_at": _utc_now(),
                    "approval_note": str(payload.get("note", "")).strip(),
                }
                review = _discover_review_state(record.pipeline_state)
                record.pipeline_state["discover_review"] = review
                if review.get("unresolved_blocking"):
                    unresolved = ", ".join(
                        [str(row.get("id", "")).strip() for row in review.get("unresolved_blocking", [])][:6]
                    )
                    return {
                        "ok": False,
                        "error": (
                            "Discover review still has unresolved blockers: "
                            + (unresolved or "see checklist")
                        ),
                    }
                record.pipeline_state["workflow_state"] = "VERIFIED"
            record.next_stage_idx = max(int(record.next_stage_idx or 0), 1)
            self._append_log(
                record,
                "✅ Discover review approved. "
                f"resolved={len(merged_resolved)}, waived={len(merged_waived)}.",
            )
        else:
            return {"ok": False, "error": f"unsupported approval type: {pending_type}"}

        record.pending_approval = None
        record.status = "running"
        record.updated_at = _utc_now()
        self._persist(record)
        self._resume_thread(record)
        return {"ok": True, "status": "running", "run_id": run_id}

    @staticmethod
    def _to_int(value: Any, default: int) -> int:
        try:
            return int(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _build_config_from_summary(self, summary: dict[str, Any]) -> PipelineConfig:
        provider_raw = str(summary.get("provider", "anthropic")).strip().lower()
        if provider_raw not in {"anthropic", "openai"}:
            provider_raw = "anthropic"
        provider = LLMProvider.ANTHROPIC if provider_raw == "anthropic" else LLMProvider.OPENAI
        requested_model = str(summary.get("model", "")).strip()
        credentials = SETTINGS_STORE.resolve_llm_credentials(provider_raw, requested_model=requested_model)
        api_key = str(credentials.get("api_key", "")).strip()
        if not api_key:
            raise ValueError(
                f"Cannot resume run without a configured {provider_raw} API key. "
                f"Set it in Settings > LLM credentials."
            )
        model = str(credentials.get("model", "")).strip() or (
            "claude-sonnet-4-20250514" if provider == LLMProvider.ANTHROPIC else "gpt-4o"
        )

        return PipelineConfig(
            provider=provider,
            anthropic_api_key=api_key if provider == LLMProvider.ANTHROPIC else "",
            openai_api_key=api_key if provider == LLMProvider.OPENAI else "",
            anthropic_model=model if provider == LLMProvider.ANTHROPIC else "claude-sonnet-4-20250514",
            openai_model=model if provider == LLMProvider.OPENAI else "gpt-4o",
            temperature=self._to_float(summary.get("temperature", 0.3), 0.3),
            developer_parallel_agents=max(1, self._to_int(summary.get("developer_parallel_agents", 5), 5)),
            max_retries=max(0, self._to_int(summary.get("max_retries", 2), 2)),
            live_deploy=bool(summary.get("live_deploy", False)),
            deploy_output_dir=str(summary.get("deploy_output_dir", "./deploy_output")),
            cluster_name=str(summary.get("cluster_name", "agent-pipeline")),
            namespace=str(summary.get("namespace", "agent-app")),
        )

    def _hydrate_record(self, run_id: str) -> RunRecord | None:
        state_payload = self.store.load_run(run_id)
        meta = self.store.load_meta(run_id)
        if not state_payload or not meta:
            return None
        config_summary = meta.get("config", {}) if isinstance(meta.get("config", {}), dict) else {}
        try:
            cfg = self._build_config_from_summary(config_summary)
        except Exception:
            return None

        pipeline_state = (
            copy.deepcopy(state_payload.get("pipeline_state", {}))
            if isinstance(state_payload.get("pipeline_state", {}), dict)
            else {}
        )
        raw_stage_status = state_payload.get("stage_status", {})
        stage_status = {
            int(k): str(v) for k, v in raw_stage_status.items() if str(k).isdigit()
        } if isinstance(raw_stage_status, dict) else {}

        with self._lock:
            existing = self._records.get(run_id)
            if existing and existing.thread and existing.thread.is_alive():
                return existing
            if existing:
                persisted_updated = _parse_iso_dt(meta.get("updated_at") or state_payload.get("saved_at"))
                local_updated = _parse_iso_dt(existing.updated_at)
                persisted_status = str(state_payload.get("pipeline_status", existing.status)).strip() or existing.status
                persisted_logs = (
                    len(state_payload.get("progress_logs", []))
                    if isinstance(state_payload.get("progress_logs", []), list)
                    else 0
                )
                should_refresh = (
                    persisted_status != existing.status
                    or persisted_logs != len(existing.progress_logs)
                    or (persisted_updated and (not local_updated or persisted_updated > local_updated))
                )
                if not should_refresh:
                    return existing

                existing.objectives = str(meta.get("business_objectives", "")).strip() or str(pipeline_state.get("business_objectives", "")).strip()
                existing.use_case = str(pipeline_state.get("use_case", "business_objectives"))
                existing.legacy_code = str(pipeline_state.get("legacy_code", ""))
                existing.modernization_language = str(pipeline_state.get("modernization_language", ""))
                existing.database_source = str(pipeline_state.get("database_source", ""))
                existing.database_target = str(pipeline_state.get("database_target", ""))
                existing.database_schema = str(pipeline_state.get("database_schema", ""))
                existing.deployment_target = str(pipeline_state.get("deployment_target", "local"))
                existing.cloud_config = (
                    copy.deepcopy(pipeline_state.get("cloud_config", {}))
                    if isinstance(pipeline_state.get("cloud_config", {}), dict)
                    else {}
                )
                existing.integration_context = (
                    copy.deepcopy(pipeline_state.get("integration_context", {}))
                    if isinstance(pipeline_state.get("integration_context", {}), dict)
                    else {}
                )
                existing.project_state_mode = str(pipeline_state.get("project_state_mode", "auto"))
                existing.project_state_detected = str(pipeline_state.get("project_state_detected", ""))
                existing.human_approval = bool(pipeline_state.get("human_approval", False))
                existing.strict_security_mode = bool(pipeline_state.get("strict_security_mode", False))
                existing.team_id = str(pipeline_state.get("team_id", ""))
                existing.team_name = str(pipeline_state.get("team_name", ""))
                existing.stage_agent_ids = (
                    copy.deepcopy(pipeline_state.get("stage_agent_ids", {}))
                    if isinstance(pipeline_state.get("stage_agent_ids", {}), dict)
                    else {}
                )
                existing.agent_personas = (
                    copy.deepcopy(pipeline_state.get("agent_personas", {}))
                    if isinstance(pipeline_state.get("agent_personas", {}), dict)
                    else {}
                )
                existing.status = str(state_payload.get("pipeline_status", "completed"))
                existing.created_at = str(meta.get("created_at", ""))
                existing.updated_at = str(meta.get("updated_at", ""))
                existing.current_stage = _current_stage_from_status_map(stage_status, pipeline_state)
                existing.stage_status = stage_status
                existing.progress_logs = (
                    list(state_payload.get("progress_logs", []))
                    if isinstance(state_payload.get("progress_logs", []), list)
                    else []
                )
                existing.progress_log_count = int(state_payload.get("progress_log_count", len(existing.progress_logs)) or len(existing.progress_logs))
                existing.pipeline_state = pipeline_state
                existing.error_message = str(state_payload.get("error_message", "") or "")
                existing.retry_count = self._to_int(pipeline_state.get("retry_count", 0), 0)
                existing.next_stage_idx = self._to_int(pipeline_state.get("next_stage_idx", 0), 0)
                existing.pending_approval = (
                    copy.deepcopy(pipeline_state.get("pending_approval", None))
                    if isinstance(pipeline_state.get("pending_approval", None), dict)
                    else None
                )
                if existing.next_stage_idx <= 0:
                    existing.next_stage_idx = max(0, existing.current_stage)
                return existing

        record = RunRecord(
            run_id=run_id,
            config=cfg,
            objectives=str(meta.get("business_objectives", "")).strip() or str(pipeline_state.get("business_objectives", "")).strip(),
            use_case=str(pipeline_state.get("use_case", "business_objectives")),
            legacy_code=str(pipeline_state.get("legacy_code", "")),
            modernization_language=str(pipeline_state.get("modernization_language", "")),
            database_source=str(pipeline_state.get("database_source", "")),
            database_target=str(pipeline_state.get("database_target", "")),
            database_schema=str(pipeline_state.get("database_schema", "")),
            deployment_target=str(pipeline_state.get("deployment_target", "local")),
            cloud_config=copy.deepcopy(pipeline_state.get("cloud_config", {}))
            if isinstance(pipeline_state.get("cloud_config", {}), dict)
            else {},
            integration_context=copy.deepcopy(pipeline_state.get("integration_context", {}))
            if isinstance(pipeline_state.get("integration_context", {}), dict)
            else {},
            project_state_mode=str(pipeline_state.get("project_state_mode", "auto")),
            project_state_detected=str(pipeline_state.get("project_state_detected", "")),
            human_approval=bool(pipeline_state.get("human_approval", False)),
            strict_security_mode=bool(pipeline_state.get("strict_security_mode", False)),
            team_id=str(pipeline_state.get("team_id", "")),
            team_name=str(pipeline_state.get("team_name", "")),
            stage_agent_ids=copy.deepcopy(pipeline_state.get("stage_agent_ids", {}))
            if isinstance(pipeline_state.get("stage_agent_ids", {}), dict)
            else {},
            agent_personas=copy.deepcopy(pipeline_state.get("agent_personas", {}))
            if isinstance(pipeline_state.get("agent_personas", {}), dict)
            else {},
            status=str(state_payload.get("pipeline_status", "completed")),
            created_at=str(meta.get("created_at", "")),
            updated_at=str(meta.get("updated_at", "")),
            current_stage=_current_stage_from_status_map(stage_status, pipeline_state),
            stage_status=stage_status,
            progress_logs=list(state_payload.get("progress_logs", []))
            if isinstance(state_payload.get("progress_logs", []), list)
            else [],
            progress_log_count=int(state_payload.get("progress_log_count", 0) or 0),
            pipeline_state=pipeline_state,
            error_message=str(state_payload.get("error_message", "") or ""),
            retry_count=self._to_int(pipeline_state.get("retry_count", 0), 0),
            next_stage_idx=self._to_int(pipeline_state.get("next_stage_idx", 0), 0),
            pending_approval=copy.deepcopy(pipeline_state.get("pending_approval", None))
            if isinstance(pipeline_state.get("pending_approval", None), dict)
            else None,
        )
        if record.next_stage_idx <= 0:
            record.next_stage_idx = max(0, record.current_stage)

        with self._lock:
            self._records[run_id] = record
        return record

    def pause(self, run_id: str) -> dict[str, Any]:
        record = self._hydrate_record(run_id) or self._get_record(run_id)
        if not record:
            return {"ok": False, "error": "run not found"}
        if record.status == "paused":
            return {"ok": True, "status": "paused", "run_id": run_id}
        if record.status in {"completed", "failed", "aborted"}:
            return {"ok": False, "error": f"cannot pause run in `{record.status}` state"}
        if record.current_stage and str(record.stage_status.get(record.current_stage, "")).strip().lower() == "running":
            record.stage_status[record.current_stage] = "paused"
        record.status = "paused"
        record.updated_at = _utc_now()
        if isinstance(record.pipeline_state, dict):
            record.pipeline_state["pipeline_status"] = "paused"
        self._append_log(record, "⏸️ Run paused by user request")
        self._persist(record)
        return {"ok": True, "status": "paused", "run_id": run_id}

    def resume(self, run_id: str) -> dict[str, Any]:
        record = self._hydrate_record(run_id) or self._get_record(run_id)
        if not record:
            return {"ok": False, "error": "run not found"}
        if record.status == "running":
            return {"ok": True, "status": "running", "run_id": run_id}
        if record.pending_approval:
            return {"ok": False, "error": "run is waiting for approval; use Approve/Reject controls"}
        if record.status in {"completed", "failed", "aborted"}:
            return {"ok": False, "error": f"cannot resume run in `{record.status}` state; use rerun stage instead"}
        if record.status != "paused":
            return {"ok": False, "error": f"cannot resume run in `{record.status}` state"}
        if record.current_stage and str(record.stage_status.get(record.current_stage, "")).strip().lower() == "paused":
            record.stage_status[record.current_stage] = "running"
        record.status = "running"
        record.updated_at = _utc_now()
        if isinstance(record.pipeline_state, dict):
            record.pipeline_state["pipeline_status"] = "running"
        self._append_log(record, "▶️ Run resumed by user request")
        self._persist(record)
        if not (record.thread and record.thread.is_alive()):
            self._resume_thread(record)
        return {"ok": True, "status": "running", "run_id": run_id}

    def abort(self, run_id: str, reason: str = "") -> dict[str, Any]:
        record = self._hydrate_record(run_id) or self._get_record(run_id)
        if not record:
            return {"ok": False, "error": "run not found"}
        if record.status == "aborted":
            return {"ok": True, "status": "aborted", "run_id": run_id}
        if record.status in {"completed", "failed"}:
            return {"ok": False, "error": f"cannot abort run in `{record.status}` state"}
        reason_text = str(reason).strip() or "aborted by user"
        if record.current_stage and str(record.stage_status.get(record.current_stage, "")).strip().lower() in {
            "running",
            "paused",
        }:
            record.stage_status[record.current_stage] = "aborted"
        record.pending_approval = None
        record.status = "aborted"
        record.error_message = f"Run aborted: {reason_text}"
        record.updated_at = _utc_now()
        if isinstance(record.pipeline_state, dict):
            record.pipeline_state["pipeline_status"] = "aborted"
            record.pipeline_state["abort_reason"] = reason_text
        self._append_log(record, f"🛑 Run aborted by user: {reason_text}")
        self._attempt_github_export(record, final=True)
        self._persist(record)
        return {"ok": True, "status": "aborted", "run_id": run_id}

    def rerun_stage(self, run_id: str, stage: int) -> dict[str, Any]:
        if stage < 1 or stage > TOTAL_STAGES:
            return {"ok": False, "error": f"stage must be between 1 and {TOTAL_STAGES}"}
        record = self._hydrate_record(run_id) or self._get_record(run_id)
        if not record:
            return {"ok": False, "error": "run not found"}
        if record.status == "running":
            return {"ok": False, "error": "run is currently running; pause or wait before rerun"}
        active_stage_numbers = set(_active_stage_numbers_from_state(record.pipeline_state or {}))
        if stage not in active_stage_numbers:
            active_str = ", ".join([str(x) for x in sorted(active_stage_numbers)])
            return {"ok": False, "error": f"stage {stage} is not active for this team. Active stages: {active_str}"}

        prior_stage = stage - 1
        stage_snapshot = self.store.load_stage_snapshot(run_id, prior_stage)
        base_state = (
            copy.deepcopy(stage_snapshot.get("pipeline_state", {}))
            if isinstance(stage_snapshot, dict) and isinstance(stage_snapshot.get("pipeline_state", {}), dict)
            else {}
        )
        raw_status = (
            stage_snapshot.get("stage_status", {})
            if isinstance(stage_snapshot, dict) and isinstance(stage_snapshot.get("stage_status", {}), dict)
            else {}
        )
        base_stage_status = {
            int(k): str(v) for k, v in raw_status.items() if str(k).isdigit()
        } if isinstance(raw_status, dict) else {}

        if not base_state:
            persisted = self.store.load_run(run_id) or {}
            base_state = (
                copy.deepcopy(persisted.get("pipeline_state", {}))
                if isinstance(persisted.get("pipeline_state", {}), dict)
                else {}
            )
            raw_persisted_status = persisted.get("stage_status", {})
            if isinstance(raw_persisted_status, dict):
                base_stage_status = {
                    int(k): str(v) for k, v in raw_persisted_status.items() if str(k).isdigit()
                }

        if not base_state:
            base_state = make_initial_state(record.objectives)

        trimmed_stage_status = {k: v for k, v in base_stage_status.items() if k < stage}
        for idx in range(1, TOTAL_STAGES + 1):
            if idx in active_stage_numbers:
                if idx >= stage:
                    trimmed_stage_status[idx] = "pending"
            else:
                trimmed_stage_status[idx] = "skipped_team"

        base_state["pipeline_status"] = "running"
        base_state["workflow_state"] = _workflow_state_for_stage(max(0, stage - 1))
        base_state["pending_approval"] = None
        base_state["next_stage_idx"] = stage - 1
        base_state["active_stage_numbers"] = sorted(active_stage_numbers)
        if stage <= 1:
            base_state["discover_review"] = {
                "overall_status": "PENDING",
                "checks": [],
                "blocking": [],
                "unresolved_blocking": [],
                "resolved_ids": [],
                "waived_ids": [],
                "updated_at": _utc_now(),
            }
        if "retry_plan" in base_state:
            del base_state["retry_plan"]
        if "retry_history" in base_state and not isinstance(base_state.get("retry_history"), list):
            del base_state["retry_history"]

        record.pipeline_state = base_state
        record.stage_status = trimmed_stage_status
        record.current_stage = max(0, stage - 1)
        record.next_stage_idx = stage - 1
        record.pending_approval = None
        record.retry_count = 0
        record.status = "running"
        record.error_message = None
        record.updated_at = _utc_now()
        self._append_log(record, f"🔁 Rerun requested from Stage {stage}")
        self._persist(record)
        if not (record.thread and record.thread.is_alive()):
            self._resume_thread(record)
        return {"ok": True, "status": "running", "run_id": run_id, "stage": stage}

    def _get_record(self, run_id: str) -> RunRecord | None:
        with self._lock:
            return self._records.get(run_id)

    def get_run(self, run_id: str) -> dict[str, Any] | None:
        record = self._hydrate_record(run_id) or self._get_record(run_id)
        if record:
            return self._record_payload(record)

        persisted = self.store.load_run(run_id)
        if not persisted:
            return None
        _maybe_rescue_queued_run(run_id, persisted)
        status = persisted.get("pipeline_status", "unknown")
        stage_status_raw = persisted.get("stage_status", {})
        stage_status = {
            int(k): v for k, v in stage_status_raw.items() if str(k).isdigit()
        }
        pipeline_state = persisted.get("pipeline_state") or {}
        current_stage = int(pipeline_state.get("current_stage", 0) or 0) if isinstance(pipeline_state, dict) else 0
        if current_stage <= 0 and stage_status:
            active_states = {"running", "completed", "failed", "waiting_approval", "paused"}
            seen = [stage for stage, state in stage_status.items() if str(state).strip().lower() in active_states]
            if seen:
                current_stage = max(seen)
        pending = pipeline_state.get("pending_approval")
        return {
            "run_id": run_id,
            "status": status,
            "current_stage": current_stage,
            "next_stage_idx": int(pipeline_state.get("next_stage_idx", 0) or 0),
            "stage_status": stage_status,
            "progress_logs": persisted.get("progress_logs", []),
            "progress_log_count": int(persisted.get("progress_log_count", len(persisted.get("progress_logs", [])) if isinstance(persisted.get("progress_logs", []), list) else 0) or 0),
            "pipeline_state": pipeline_state,
            "error_message": persisted.get("error_message"),
            "retry_count": 0,
            "pending_approval": pending,
            "human_approval": bool(pipeline_state.get("human_approval", False)),
            "strict_security_mode": bool(pipeline_state.get("strict_security_mode", False)),
            "deployment_target": str(pipeline_state.get("deployment_target", "local")),
            "integration_context": pipeline_state.get("integration_context", {}) if isinstance(pipeline_state.get("integration_context"), dict) else {},
            "run_context_bundle": pipeline_state.get("run_context_bundle", {}) if isinstance(pipeline_state.get("run_context_bundle"), dict) else {},
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
            "progress_log_count": max(int(record.progress_log_count or 0), len(record.progress_logs)),
            "pipeline_state": copy.deepcopy(record.pipeline_state),
            "error_message": record.error_message,
            "retry_count": record.retry_count,
            "pending_approval": copy.deepcopy(record.pending_approval),
            "human_approval": record.human_approval,
            "strict_security_mode": record.strict_security_mode,
            "deployment_target": record.deployment_target,
            "integration_context": copy.deepcopy(record.integration_context),
            "run_context_bundle": (
                copy.deepcopy(record.pipeline_state.get("run_context_bundle", {}))
                if isinstance(record.pipeline_state, dict) and isinstance(record.pipeline_state.get("run_context_bundle", {}), dict)
                else {}
            ),
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
    context = copy.deepcopy(context) if isinstance(context, dict) else {}
    if not context:
        context = {}
    if not isinstance(context.get("brownfield", {}), dict) and isinstance(payload.get("brownfield", {}), dict):
        context["brownfield"] = copy.deepcopy(payload.get("brownfield", {}))
    if not isinstance(context.get("greenfield", {}), dict) and isinstance(payload.get("greenfield", {}), dict):
        context["greenfield"] = copy.deepcopy(payload.get("greenfield", {}))
    if not isinstance(context.get("scan_scope", {}), dict) and isinstance(payload.get("scan_scope", {}), dict):
        context["scan_scope"] = copy.deepcopy(payload.get("scan_scope", {}))
    for key in (
        "project_state_mode",
        "project_state_detected",
        "project_state_confidence",
        "project_state_reason",
        "domain_pack_id",
        "custom_domain_pack",
        "jurisdiction",
        "data_classification",
        "cloud_promotion_enabled",
        "sample_dataset_enabled",
    ):
        if key not in context and key in payload:
            context[key] = copy.deepcopy(payload.get(key))
    return context


def _imported_analysis_response_payload(objectives: str, integration_ctx: dict[str, Any]) -> dict[str, Any]:
    integration_ctx = integration_ctx if isinstance(integration_ctx, dict) else {}
    evidence_ctx = integration_ctx.get("evidence", {}) if isinstance(integration_ctx.get("evidence", {}), dict) else {}
    evidence_source_mode = str(evidence_ctx.get("source_mode", "")).strip().lower()
    evidence_bundle_id = str(evidence_ctx.get("bundle_id", "")).strip()
    if evidence_source_mode not in {"evidence", "hybrid"} or not evidence_bundle_id:
        return {}
    bundle_payload = load_evidence_bundle(evidence_bundle_id)
    if not bundle_payload:
        return {}
    normalized_artifacts = (
        bundle_payload.get("normalized_artifacts", {})
        if isinstance(bundle_payload.get("normalized_artifacts", {}), dict)
        else {}
    )
    if not normalized_artifacts:
        bundle_dir = Path(ROOT / "run_artifacts" / "evidence_bundles" / safe_name(evidence_bundle_id))
        normalized_path = bundle_dir / "normalized_artifacts.json"
        if normalized_path.exists():
            try:
                normalized_artifacts = _get_json(normalized_path.read_bytes())
            except Exception:
                normalized_artifacts = {}
    coverage = (
        normalized_artifacts.get("evidence_coverage_report_v1", {})
        if isinstance(normalized_artifacts.get("evidence_coverage_report_v1", {}), dict)
        else bundle_payload.get("evidence_coverage_report_v1", {})
        if isinstance(bundle_payload.get("evidence_coverage_report_v1", {}), dict)
        else {}
    )
    landscape = normalized_artifacts.get("repo_landscape_v1", {}) if isinstance(normalized_artifacts.get("repo_landscape_v1", {}), dict) else {}
    components = normalized_artifacts.get("component_inventory_v1", {}) if isinstance(normalized_artifacts.get("component_inventory_v1", {}), dict) else {}
    tracks = normalized_artifacts.get("modernization_track_plan_v1", {}) if isinstance(normalized_artifacts.get("modernization_track_plan_v1", {}), dict) else {}
    component_rows = components.get("components", []) if isinstance(components.get("components", []), list) else []
    track_rows = tracks.get("tracks", []) if isinstance(tracks.get("tracks", []), list) else []
    dimension_scores = coverage.get("dimensions", {}) if isinstance(coverage.get("dimensions", {}), dict) else {}
    blockers = coverage.get("blockers", []) if isinstance(coverage.get("blockers", []), list) else []
    analysis = {
        "overview": (
            str(objectives).strip()
            or f"Imported analysis evidence describes {len(component_rows)} component(s) with architecture coverage {int(dimension_scores.get('architecture', 0) or 0)} and behavior coverage {int(dimension_scores.get('behavior', 0) or 0)}."
        ),
        "likely_capabilities": [str(row.get("title", "")).strip() for row in track_rows if str(row.get("title", "")).strip()][:8],
        "input_output_contracts": [f"Coverage {key}: {int(value or 0)}" for key, value in dimension_scores.items()][:8],
        "key_components": [str(row.get("name", "")).strip() for row in component_rows if str(row.get("name", "")).strip()][:12],
        "interfaces": [str(row.get("title", "")).strip() for row in landscape.get("high_risk_signals", []) if isinstance(row, dict) and str(row.get("title", "")).strip()][:6],
        "data_entities": [],
        "domain_functions": [str(row.get("lane", "")).strip() for row in track_rows if str(row.get("lane", "")).strip()][:8],
        "unknowns": [str(item).strip() for item in blockers if str(item).strip()][:8],
        "evidence_files": [str(row.get("file_name", "")).strip() for row in bundle_payload.get("files", []) if isinstance(row, dict) and str(row.get("file_name", "")).strip()][:24],
        "stats": {
            "sampled_files": int(bundle_payload.get("file_count", 0) or 0),
            "sampled_tree_entries": 0,
            "route_hints": len(track_rows),
        },
        "evidence_mode": {
            "provider": str(bundle_payload.get("provider_match_report_v1", {}).get("selected_provider", "")).strip(),
            "coverage": coverage,
        },
    }
    compatibility_fields = {
        "project_name": str(normalized_artifacts.get("project_name", "")).strip() or "Imported VB6 system",
        "analysis_walkthrough": normalized_artifacts.get("analysis_walkthrough", {}) if isinstance(normalized_artifacts.get("analysis_walkthrough", {}), dict) else {},
        "context_reference": normalized_artifacts.get("context_reference", {}) if isinstance(normalized_artifacts.get("context_reference", {}), dict) else {},
        "legacy_skill_profile": normalized_artifacts.get("legacy_skill_profile", {}) if isinstance(normalized_artifacts.get("legacy_skill_profile", {}), dict) else {},
        "source_target_modernization_profile": normalized_artifacts.get("source_target_modernization_profile", {}) if isinstance(normalized_artifacts.get("source_target_modernization_profile", {}), dict) else {},
        "legacy_code_inventory": normalized_artifacts.get("legacy_code_inventory", {}) if isinstance(normalized_artifacts.get("legacy_code_inventory", {}), dict) else {},
        "vb6_analysis": normalized_artifacts.get("vb6_analysis", {}) if isinstance(normalized_artifacts.get("vb6_analysis", {}), dict) else {},
        "open_questions": normalized_artifacts.get("open_questions", []) if isinstance(normalized_artifacts.get("open_questions", []), list) else blockers,
        "quality_gates": normalized_artifacts.get("quality_gates", []) if isinstance(normalized_artifacts.get("quality_gates", []), list) else coverage.get("checks", []),
    }
    report_seed = {
        "ok": True,
        "source": "imported_analysis",
        "repo": {"owner": "", "repository": "", "default_branch": "", "url": ""},
        "analyst_brief": {"title": "Analyst functionality understanding", "summary": analysis},
        "evidence_bundle_v1": bundle_payload,
        "provider_match_report_v1": bundle_payload.get("provider_match_report_v1", {}),
        "evidence_coverage_report_v1": coverage,
        **compatibility_fields,
    }
    raw_artifacts = build_raw_artifact_set_v1(report_seed)
    if not isinstance(raw_artifacts, dict):
        raw_artifacts = {}
    raw_artifacts.update(normalized_artifacts)
    report_seed["raw_artifacts"] = raw_artifacts
    response_payload = {
        "ok": True,
        "source": "imported_analysis",
        "repo": {"owner": "", "repository": "", "default_branch": "", "url": ""},
        "analyst_brief": {"title": "Analyst functionality understanding", "summary": analysis},
        **compatibility_fields,
        "raw_artifacts": raw_artifacts,
        "evidence_bundle_v1": bundle_payload,
        "provider_match_report_v1": bundle_payload.get("provider_match_report_v1", {}),
        "evidence_coverage_report_v1": coverage,
        "analyst_report_v2": build_analyst_report_v2(report_seed),
    }
    return response_payload


def _evidence_sil_discovery(payload: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(payload, dict) or str(payload.get("source", "")).strip().lower() != "imported_analysis":
        return {}
    raw = payload.get("raw_artifacts", {}) if isinstance(payload.get("raw_artifacts", {}), dict) else {}
    landscape = raw.get("repo_landscape_v1", {}) if isinstance(raw.get("repo_landscape_v1", {}), dict) else {}
    components = raw.get("component_inventory_v1", {}) if isinstance(raw.get("component_inventory_v1", {}), dict) else {}
    component_rows = components.get("components", []) if isinstance(components.get("components", []), list) else []
    language_counts = landscape.get("language_loc", {}) if isinstance(landscape.get("language_loc", {}), dict) else {}
    file_counts = landscape.get("language_files", {}) if isinstance(landscape.get("language_files", {}), dict) else {}
    modules = []
    for idx, row in enumerate(component_rows[:160], start=1):
        if not isinstance(row, dict):
            continue
        root_paths = row.get("root_paths", []) if isinstance(row.get("root_paths", []), list) else []
        path = str(root_paths[0] if root_paths else row.get("name", f"component-{idx}")).strip()
        if not path:
            continue
        modules.append({
            "id": f"component:{safe_name(path)}",
            "path": path,
            "language": str(row.get("primary_language", "")).strip() or str(row.get("language", "")).strip(),
            "adapter": str(row.get("component_type", "component")).strip() or "component",
            "import_count": int(row.get("coupling_count", 0) or 0),
            "route_count": 0,
        })
    scanned_files = int(sum(int(v or 0) for v in file_counts.values()) or 0)
    return {
        "repo_root": "imported_analysis",
        "scanned_files": scanned_files,
        "content_fingerprint": str(_as_dict_safe(payload.get("evidence_bundle_v1")).get("bundle_id", "")).strip(),
        "language_counts": {str(k): int(v or 0) for k, v in language_counts.items() if str(k).strip()},
        "file_samples": [str(x) for x in _as_list_safe(_as_dict_safe(payload.get("analyst_brief")).get("summary", {}).get("evidence_files"))][:220],
        "endpoint_hints": [],
        "static_analysis": {
            "version": "evidence-import-v1",
            "adapters": ["imported_analysis"],
            "stats": {
                "component_count": len(component_rows),
                "architecture_coverage": int(_as_dict_safe(payload.get("evidence_coverage_report_v1")).get("dimensions", {}).get("architecture", 0) or 0),
            },
            "modules": modules,
            "import_graph": {"edges": []},
            "route_surface": [],
            "config_artifacts": [],
            "infra_resources": [],
            "parse_errors": [],
        },
        "source_mode": "imported_analysis",
    }


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


def _github_request_context() -> tuple[str, dict[str, str], bool]:
    github_cfg = SETTINGS_STORE.get_integration_config("github")
    token = str(github_cfg.get("token", "")).strip()
    base_url = str(github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "synthetix-discover/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if token:
        headers["Authorization"] = f"Bearer {token}"
    return base_url, headers, bool(token)


def _github_fetch_error_message(exc: ValueError, *, authenticated: bool) -> str:
    msg = str(exc).strip()
    lower = msg.lower()
    if "rate limit" in lower:
        if authenticated:
            return f"GitHub API rate limit exceeded: {msg}"
        return (
            "GitHub anonymous API rate limit exceeded for this public repo. "
            "Save a GitHub token in Settings > Integrations > GitHub and retry."
        )
    if "http 404" in lower or "not found" in lower:
        if authenticated:
            return f"GitHub fetch failed: {msg}"
        return (
            "GitHub repo was not accessible anonymously. "
            "If the repo is private, save a GitHub token in Settings > Integrations > GitHub. "
            f"Details: {msg}"
        )
    if "http 401" in lower or "http 403" in lower:
        if authenticated:
            return f"GitHub fetch failed: {msg}"
        return (
            "GitHub rejected anonymous access for this repo. "
            "Save a GitHub token in Settings > Integrations > GitHub and retry. "
            f"Details: {msg}"
        )
    return f"GitHub fetch failed: {msg}"


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
        ".bas": "vb6_module",
        ".cls": "vb6_class",
        ".frm": "vb6_form",
        ".frx": "vb6_form_binary",
        ".ctl": "vb6_usercontrol",
        ".ctx": "vb6_usercontrol_binary",
        ".vbp": "vb6_project",
        ".vbg": "vb6_group",
        ".res": "vb6_resource",
        ".ocx": "vb6_activex_binary",
        ".dcx": "vb6_query_definition",
        ".dca": "vb6_connection_definition",
        ".mdb": "ms_access_db",
        ".accdb": "ms_access_db",
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


def _extract_vb6_signals(path: str, text: str) -> dict[str, Any]:
    return legacy_extract_vb6_signals(path, text)


def _parse_vbp_dependency_reference(reference_line: Any) -> dict[str, str] | None:
    text = str(reference_line or "").strip()
    if not text:
        return None
    name = ""
    reference = text
    pair_match = re.search(r'"([^"]+)"\s*;\s*"([^"]+)"', text)
    if pair_match:
        reference = str(pair_match.group(1) or "").strip()
        name = str(pair_match.group(2) or "").strip()
    if not name:
        ext_match = re.search(r"([A-Za-z0-9_.-]+\.(?:ocx|dll|dcx|dca))", text, flags=re.IGNORECASE)
        if ext_match:
            name = str(ext_match.group(1) or "").strip()
    if not name:
        return None
    guid_match = re.search(r"\{[0-9A-Fa-f-]{36}\}", reference or text)
    guid = str(guid_match.group(0) or "").strip() if guid_match else ""
    return {"name": name, "reference": reference, "guid": guid}


def _extract_vbp_dependency_references(project_defs: list[dict[str, Any]]) -> list[dict[str, str]]:
    rows: list[dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for project in project_defs:
        if not isinstance(project, dict):
            continue
        project_name = str(project.get("project_name", "")).strip()
        project_file = str(project.get("project_file", "")).strip()
        refs = project.get("references", []) if isinstance(project.get("references", []), list) else []
        for raw_ref in refs:
            parsed = _parse_vbp_dependency_reference(raw_ref)
            if not parsed:
                continue
            name = str(parsed.get("name", "")).strip()
            reference = str(parsed.get("reference", "")).strip()
            key = (project_file.lower(), name.lower(), reference.lower())
            if key in seen:
                continue
            seen.add(key)
            rows.append(
                {
                    "name": name,
                    "reference": reference,
                    "guid": str(parsed.get("guid", "")).strip(),
                    "project_name": project_name,
                    "project_file": project_file,
                }
            )
    return rows[:480]


def _analyze_source_bundle(
    *,
    objectives: str,
    repo_label: str,
    file_entries: list[dict[str, Any]],
    file_contents: dict[str, str],
    target_language: str = "",
    target_platform: str = "",
    deployment_target: str = "local",
    source_repo_url: str = "",
    target_repo_url: str = "",
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
    vb6_forms: set[str] = set()
    vb6_controls: set[str] = set()
    vb6_activex: set[str] = set()
    vb6_events: set[str] = set()
    vb6_event_keys: set[str] = set()
    vb6_project_members: set[str] = set()
    vb6_by_path: dict[str, dict[str, Any]] = {}
    vb6_project_defs: list[dict[str, Any]] = []
    vb6_sql_queries: set[str] = set()
    vb6_connection_strings: set[str] = set()
    vb6_database_file_refs: set[str] = set()
    vb6_connection_string_rows: list[dict[str, Any]] = []
    vb6_database_file_reference_rows: list[dict[str, Any]] = []
    vb6_module_global_declarations: list[dict[str, Any]] = []
    vb6_win32_declares: set[str] = set()
    vb6_com_progids: set[str] = set()
    vb6_com_references: set[str] = set()
    vb6_file_type_coverage: dict[str, int] = {}
    vb6_bas_modules: set[str] = set()
    vb6_bas_procedure_count = 0
    vb6_binary_companions: list[dict[str, Any]] = []
    vb6_callbyname_sites = 0
    vb6_createobject_sites = 0
    vb6_ui_event_map: dict[str, dict[str, Any]] = {}
    vb6_pitfalls: dict[str, dict[str, Any]] = {}
    vb6_error_profile: dict[str, int] = {
        "on_error_resume_next": 0,
        "on_error_goto": 0,
        "on_error_goto0": 0,
        "control_array_index_markers": 0,
        "late_bound_com_calls": 0,
        "variant_declarations": 0,
        "default_instance_references": 0,
        "doevents_calls": 0,
        "registry_operations": 0,
    }

    ordered_paths = [str(item.get("path", "")).strip() for item in file_entries if isinstance(item, dict)]
    legacy_skill_profile = infer_legacy_skill(
        file_paths=[path for path in ordered_paths if path],
        file_contents=file_contents,
    )
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
        vb6 = _extract_vb6_signals(path, text)
        if vb6:
            vb6_by_path[str(path).replace("\\", "/")] = vb6
            file_type = str(vb6.get("vb6_file_type", "")).strip() or "unknown"
            vb6_file_type_coverage[file_type] = int(vb6_file_type_coverage.get(file_type, 0) or 0) + 1
            if bool(vb6.get("is_binary_companion", False)):
                info = vb6.get("binary_companion_info", {})
                if isinstance(info, dict):
                    vb6_binary_companions.append(
                        {
                            "path": str(info.get("path", path)).strip(),
                            "extension": str(info.get("extension", "")).strip(),
                            "note": str(info.get("note", "Binary companion file detected.")).strip(),
                        }
                    )
            if str(path).lower().endswith(".bas"):
                vb6_bas_modules.add(str(path).replace("\\", "/"))
                vb6_bas_procedure_count += len(vb6.get("procedures", []) if isinstance(vb6.get("procedures", []), list) else [])
            vb6_forms.update(vb6.get("forms", []) if isinstance(vb6.get("forms", []), list) else [])
            vb6_controls.update(vb6.get("controls", []) if isinstance(vb6.get("controls", []), list) else [])
            vb6_activex.update(vb6.get("activex_dependencies", []) if isinstance(vb6.get("activex_dependencies", []), list) else [])
            vb6_events.update(vb6.get("event_handlers", []) if isinstance(vb6.get("event_handlers", []), list) else [])
            vb6_event_keys.update(
                vb6.get("event_handler_keys", [])
                if isinstance(vb6.get("event_handler_keys", []), list)
                else []
            )
            vb6_project_members.update(
                vb6.get("project_members", []) if isinstance(vb6.get("project_members", []), list) else []
            )
            vb6_sql_queries.update(vb6.get("sql_queries", []) if isinstance(vb6.get("sql_queries", []), list) else [])
            source_file_norm = str(path).replace("\\", "/")
            sig_forms = vb6.get("forms", []) if isinstance(vb6.get("forms", []), list) else []
            form_hint = ""
            if sig_forms:
                form_hint = str(sig_forms[0]).split(":", 1)[-1].strip()
            module_hint = Path(str(path)).stem if str(path).lower().endswith(".bas") else ""
            vb6_connection_strings.update(
                vb6.get("connection_strings", [])
                if isinstance(vb6.get("connection_strings", []), list)
                else []
            )
            for conn in vb6.get("connection_strings", []) if isinstance(vb6.get("connection_strings", []), list) else []:
                token = str(conn or "").strip()
                if token:
                    vb6_connection_string_rows.append(
                        {
                            "connection_string": token,
                            "source_file": source_file_norm,
                            "form": form_hint,
                            "module": module_hint,
                            "evidence": source_file_norm,
                        }
                    )
            for db_ref in vb6.get("database_file_refs", []) if isinstance(vb6.get("database_file_refs", []), list) else []:
                token = str(db_ref or "").strip().replace("\\", "/")
                if token:
                    vb6_database_file_refs.add(token)
                    vb6_database_file_reference_rows.append(
                        {
                            "path": token,
                            "source_file": source_file_norm,
                            "form": form_hint,
                            "module": module_hint,
                            "evidence": source_file_norm,
                        }
                    )
            for gdecl in vb6.get("module_global_declarations", []) if isinstance(vb6.get("module_global_declarations", []), list) else []:
                if not isinstance(gdecl, dict):
                    continue
                symbol = str(gdecl.get("symbol", "")).strip()
                if not symbol:
                    continue
                vb6_module_global_declarations.append(
                    {
                        "symbol": symbol,
                        "declared_type": str(gdecl.get("declared_type", "Variant")).strip() or "Variant",
                        "scope": str(gdecl.get("scope", "dim")).strip() or "dim",
                        "source_file": str(gdecl.get("source_file", "")).strip() or source_file_norm,
                        "line": int(gdecl.get("line", 0) or 0),
                        "declaration": str(gdecl.get("declaration", "")).strip()[:320],
                    }
                )
            vb6_win32_declares.update(vb6.get("win32_declares", []) if isinstance(vb6.get("win32_declares", []), list) else [])
            for row in vb6.get("ui_event_map", []) if isinstance(vb6.get("ui_event_map", []), list) else []:
                if not isinstance(row, dict):
                    continue
                key = str(row.get("event_handler", "")).strip() or str(row.get("control", "")).strip()
                if key and key not in vb6_ui_event_map:
                    vb6_ui_event_map[key] = row
            com_surface = vb6.get("com_surface_map", {}) if isinstance(vb6.get("com_surface_map", {}), dict) else {}
            vb6_com_progids.update(
                com_surface.get("late_bound_progids", [])
                if isinstance(com_surface.get("late_bound_progids", []), list)
                else []
            )
            vb6_com_references.update(
                com_surface.get("references", [])
                if isinstance(com_surface.get("references", []), list)
                else []
            )
            vb6_callbyname_sites += int(com_surface.get("call_by_name_sites", 0) or 0)
            vb6_createobject_sites += int(com_surface.get("createobject_getobject_sites", 0) or 0)
            err_profile = vb6.get("error_handling_profile", {}) if isinstance(vb6.get("error_handling_profile", {}), dict) else {}
            for k in vb6_error_profile:
                vb6_error_profile[k] = int(vb6_error_profile.get(k, 0) or 0) + int(err_profile.get(k, 0) or 0)
            for detector in vb6.get("pitfall_detectors", []) if isinstance(vb6.get("pitfall_detectors", []), list) else []:
                if not isinstance(detector, dict):
                    continue
                did = str(detector.get("id", "")).strip()
                if not did:
                    continue
                existing = vb6_pitfalls.get(did)
                if not existing:
                    vb6_pitfalls[did] = {
                        "id": did,
                        "severity": str(detector.get("severity", "medium")),
                        "count": int(detector.get("count", 0) or 0),
                        "requires": detector.get("requires", []) if isinstance(detector.get("requires", []), list) else [],
                        "evidence": str(detector.get("evidence", "")).strip(),
                    }
                else:
                    existing["count"] = int(existing.get("count", 0) or 0) + int(detector.get("count", 0) or 0)
                    if not str(existing.get("evidence", "")).strip():
                        existing["evidence"] = str(detector.get("evidence", "")).strip()
            project_def = vb6.get("project_definition", {})
            if isinstance(project_def, dict) and project_def:
                vb6_project_defs.append(project_def)
            for evt in vb6_events:
                if len(function_hints) < 40:
                    function_hints.add(evt)
            if vb6_controls:
                input_hints.add("VB6 form controls indicate user-entered desktop UI input fields")

    known_paths_by_lower = {str(p).replace("\\", "/").lower(): str(p).replace("\\", "/") for p in ordered_paths if p}

    def _project_text_signals(text: str) -> dict[str, list[str]]:
        raw = str(text or "")
        lower = raw.lower()
        procedures: list[str] = []
        tables: list[str] = []
        inputs: list[str] = []
        outputs: list[str] = []
        integrations: list[str] = []
        for match in re.findall(r'(?im)^\s*(?:public|private|friend|protected)?\s*(?:function|sub)\s+([a-zA-Z_][a-zA-Z0-9_]*)', raw):
            name = str(match or "").strip()
            if name and name not in procedures:
                procedures.append(name)
            if len(procedures) >= 80:
                break
        for pat in [r'(?i)\bfrom\s+([a-zA-Z_][a-zA-Z0-9_]*)', r'(?i)\bjoin\s+([a-zA-Z_][a-zA-Z0-9_]*)', r'(?i)\binto\s+([a-zA-Z_][a-zA-Z0-9_]*)']:
            for match in re.findall(pat, raw):
                t = str(match or "").strip()
                if t and t.lower() not in {"where", "select"} and t not in tables:
                    tables.append(t)
                if len(tables) >= 40:
                    break
            if len(tables) >= 40:
                break
        req = re.findall(r'(?i)request\.(?:querystring|form)\(\s*"([^"]+)"\s*\)', raw)
        req += re.findall(r"(?i)request\.(?:querystring|form)\(\s*'([^']+)'\s*\)", raw)
        for m in req:
            v = str(m or "").strip()
            if v and v not in inputs:
                inputs.append(v)
        if ("request(" in lower or "request." in lower) and "HTTP request parameters" not in inputs:
            inputs.append("HTTP request parameters")
        if "response.write" in lower:
            outputs.append("HTML/text response output")
        if "response.redirect" in lower:
            outputs.append("HTTP redirect output")
        hints = [
            ("createobject(", "COM object invocation"),
            ("msxml2.xmlhttp", "HTTP integration via MSXML"),
            ("ado", "ADO data access"),
            ("dao", "DAO data access"),
            ("scripting.filesystemobject", "Filesystem automation"),
        ]
        for token, label in hints:
            if token in lower and label not in integrations:
                integrations.append(label)
        return {
            "procedures": procedures[:80],
            "tables": tables[:40],
            "input_signals": inputs[:20],
            "output_signals": outputs[:20],
            "integration_hints": integrations[:12],
        }

    def _infer_project_objective(name: str, forms: set[str], procedures: set[str], tables: set[str]) -> tuple[str, list[str]]:
        haystack = " ".join(
            [str(name or "").lower()]
            + [str(x).lower() for x in list(forms)[:30]]
            + [str(x).lower() for x in list(procedures)[:60]]
            + [str(x).lower() for x in list(tables)[:30]]
        )
        mapping = [
            ("login", "Authenticate users and manage session entry.", "Authentication and user access"),
            ("auth", "Enforce user authentication and authorization workflows.", "Authentication and user access"),
            ("customer", "Manage customer profile lookup and maintenance.", "Customer profile operations"),
            ("account", "Support account operations and account state updates.", "Account operations"),
            ("payment", "Capture and process payment-related transactions.", "Payment processing"),
            ("transfer", "Orchestrate transfer initiation and posting workflows.", "Funds transfer"),
            ("transaction", "Record and process transactional business events.", "Transaction processing"),
            ("invoice", "Handle billing and invoice workflows.", "Billing and invoicing"),
            ("report", "Generate reporting and analytics outputs.", "Reporting and analytics"),
            ("settlement", "Handle settlement and reconciliation workflows.", "Settlement and reconciliation"),
        ]
        objective = ""
        capabilities: list[str] = []
        for token, desc, cap in mapping:
            if token in haystack:
                if not objective:
                    objective = desc
                if cap not in capabilities:
                    capabilities.append(cap)
        if not objective:
            objective = "Deliver event-driven desktop workflows through VB6 forms/modules and COM integrations."
        if not capabilities:
            capabilities = ["Desktop workflow orchestration"]
        return objective, capabilities[:8]

    def _resolve_member(member_path: str, project_file: str) -> str:
        member = str(member_path or "").strip().replace("\\", "/")
        while member.startswith("./"):
            member = member[2:]
        project = str(project_file or "").strip().replace("\\", "/")
        project_dir = project.rsplit("/", 1)[0] if "/" in project else ""
        candidates = [member]
        if project_dir:
            candidates.insert(0, f"{project_dir}/{member}")
        for cand in candidates:
            hit = known_paths_by_lower.get(cand.lower())
            if hit:
                return hit
        return ""

    vb6_projects: list[dict[str, Any]] = []
    for project in vb6_project_defs:
        members = project.get("members", []) if isinstance(project.get("members", []), list) else []
        member_files: set[str] = set()
        member_type_counts: dict[str, int] = {}
        forms: set[str] = set()
        controls: set[str] = set()
        activex: set[str] = set()
        events: set[str] = set()
        procedures: set[str] = set()
        tables: set[str] = set()
        input_signals: set[str] = set()
        output_signals: set[str] = set()
        integration_hints_project: set[str] = set()
        project_file = str(project.get("project_file", "")).strip().replace("\\", "/")
        for member in members:
            if not isinstance(member, dict):
                continue
            mtype = str(member.get("member_type", "")).strip()
            if mtype:
                member_type_counts[mtype] = int(member_type_counts.get(mtype, 0)) + 1
            resolved = _resolve_member(str(member.get("member_path", "")), project_file)
            if not resolved:
                continue
            member_files.add(resolved)
            sig = vb6_by_path.get(resolved, {})
            forms.update(sig.get("forms", []) if isinstance(sig.get("forms", []), list) else [])
            controls.update(sig.get("controls", []) if isinstance(sig.get("controls", []), list) else [])
            activex.update(sig.get("activex_dependencies", []) if isinstance(sig.get("activex_dependencies", []), list) else [])
            events.update(sig.get("event_handlers", []) if isinstance(sig.get("event_handlers", []), list) else [])
            file_text = str(file_contents.get(resolved, ""))
            if file_text:
                ps = _project_text_signals(file_text)
                procedures.update(ps.get("procedures", []))
                tables.update(ps.get("tables", []))
                input_signals.update(ps.get("input_signals", []))
                output_signals.update(ps.get("output_signals", []))
                integration_hints_project.update(ps.get("integration_hints", []))
        project_name = str(project.get("project_name", "")).strip() or (project_file.rsplit("/", 1)[-1].rsplit(".", 1)[0] if project_file else "VB6 Project")
        objective, caps = _infer_project_objective(project_name, forms, procedures, tables)
        workflows: list[str] = []
        form_names = [str(x).split(":", 1)[-1] for x in sorted(forms)[:8]]
        for fname in form_names:
            related = [ev for ev in sorted(events) if str(ev).lower().startswith(str(fname).lower() + "_")][:4]
            workflows.append(f"{fname}: {'; '.join(related) if related else 'event-driven workflow via UI controls'}")
        if not workflows and procedures:
            workflows.append("Procedural workflow: " + ", ".join(sorted(procedures)[:6]))
        vb6_projects.append(
            {
                "project_name": project_name,
                "project_file": project_file,
                "project_type": str(project.get("project_type", "")).strip(),
                "startup_object": str(project.get("startup_object", "")).strip(),
                "member_count": len(members),
                "member_files": sorted(member_files)[:120],
                "member_type_counts": member_type_counts,
                "forms": sorted(forms)[:40],
                "controls": sorted(controls)[:80],
                "activex_dependencies": sorted(activex)[:40],
                "event_handlers": sorted(events)[:80],
                "business_objective_hypothesis": objective,
                "key_business_capabilities": caps,
                "primary_workflows": workflows[:12],
                "data_touchpoints": {
                    "tables": sorted(tables)[:40],
                    "procedures": sorted(procedures)[:80],
                    "input_signals": sorted(input_signals)[:20],
                    "output_signals": sorted(output_signals)[:20],
                },
                "technical_components": {
                    "notable_components": sorted(member_files)[:20],
                    "external_dependencies": sorted(activex)[:40],
                    "integration_hints": sorted(integration_hints_project)[:12],
                },
                "modernization_considerations": [
                    "Preserve project boundaries and startup behavior during migration.",
                    "Maintain VB6 event-handler semantics for equivalent workflows.",
                ],
                "forms_count": len(forms),
                "controls_count": len(controls),
                "activex_dependency_count": len(activex),
                "event_handler_count": len(events),
            }
        )

    if not vb6_projects and vb6_by_path:
        grouped: dict[str, dict[str, Any]] = {}
        for path, sig in vb6_by_path.items():
            root = path.split("/", 1)[0] if "/" in path else "(root)"
            bucket = grouped.setdefault(
                root,
                {
                    "project_name": f"Inferred:{root}",
                    "project_file": "",
                    "project_type": "inferred",
                    "startup_object": "",
                    "member_files": set(),
                    "member_type_counts": {},
                    "forms": set(),
                    "controls": set(),
                    "activex_dependencies": set(),
                    "event_handlers": set(),
                    "procedures": set(),
                    "tables": set(),
                    "input_signals": set(),
                    "output_signals": set(),
                    "integration_hints": set(),
                },
            )
            bucket["member_files"].add(path)
            bucket["forms"].update(sig.get("forms", []) if isinstance(sig.get("forms", []), list) else [])
            bucket["controls"].update(sig.get("controls", []) if isinstance(sig.get("controls", []), list) else [])
            bucket["activex_dependencies"].update(sig.get("activex_dependencies", []) if isinstance(sig.get("activex_dependencies", []), list) else [])
            bucket["event_handlers"].update(sig.get("event_handlers", []) if isinstance(sig.get("event_handlers", []), list) else [])
            path_lower = path.lower()
            if path_lower.endswith(".frm"):
                bucket["member_type_counts"]["Form"] = int(bucket["member_type_counts"].get("Form", 0)) + 1
            elif path_lower.endswith(".bas"):
                bucket["member_type_counts"]["Module"] = int(bucket["member_type_counts"].get("Module", 0)) + 1
            elif path_lower.endswith(".cls"):
                bucket["member_type_counts"]["Class"] = int(bucket["member_type_counts"].get("Class", 0)) + 1
            elif path_lower.endswith(".ctl"):
                bucket["member_type_counts"]["UserControl"] = int(bucket["member_type_counts"].get("UserControl", 0)) + 1
            ps = _project_text_signals(str(file_contents.get(path, "")))
            bucket["procedures"].update(ps.get("procedures", []))
            bucket["tables"].update(ps.get("tables", []))
            bucket["input_signals"].update(ps.get("input_signals", []))
            bucket["output_signals"].update(ps.get("output_signals", []))
            bucket["integration_hints"].update(ps.get("integration_hints", []))
        for row in grouped.values():
            forms = set(row.get("forms", set()))
            controls = set(row.get("controls", set()))
            activex = set(row.get("activex_dependencies", set()))
            events = set(row.get("event_handlers", set()))
            members = set(row.get("member_files", set()))
            procedures = set(row.get("procedures", set()))
            tables = set(row.get("tables", set()))
            input_signals = set(row.get("input_signals", set()))
            output_signals = set(row.get("output_signals", set()))
            integration_hints_project = set(row.get("integration_hints", set()))
            objective, caps = _infer_project_objective(str(row.get("project_name", "")), forms, procedures, tables)
            vb6_projects.append(
                {
                    "project_name": str(row.get("project_name", "")),
                    "project_file": str(row.get("project_file", "")),
                    "project_type": str(row.get("project_type", "")),
                    "startup_object": str(row.get("startup_object", "")),
                    "member_count": len(members),
                    "member_files": sorted(members)[:120],
                    "member_type_counts": dict(row.get("member_type_counts", {})),
                    "forms": sorted(forms)[:40],
                    "controls": sorted(controls)[:80],
                    "activex_dependencies": sorted(activex)[:40],
                    "event_handlers": sorted(events)[:80],
                    "business_objective_hypothesis": objective,
                    "key_business_capabilities": caps,
                    "primary_workflows": [f"{str(x).split(':', 1)[-1]}: event-driven workflow via UI controls" for x in sorted(forms)[:8]],
                    "data_touchpoints": {
                        "tables": sorted(tables)[:40],
                        "procedures": sorted(procedures)[:80],
                        "input_signals": sorted(input_signals)[:20],
                        "output_signals": sorted(output_signals)[:20],
                    },
                    "technical_components": {
                        "notable_components": sorted(members)[:20],
                        "external_dependencies": sorted(activex)[:40],
                        "integration_hints": sorted(integration_hints_project)[:12],
                    },
                    "modernization_considerations": [
                        "Validate inferred project boundaries and startup flow with stakeholders.",
                        "Preserve workflow behavior and data contracts during conversion.",
                    ],
                    "forms_count": len(forms),
                    "controls_count": len(controls),
                    "activex_dependency_count": len(activex),
                    "event_handler_count": len(events),
                }
            )

    vb6_projects = sorted(vb6_projects, key=lambda row: str(row.get("project_name", "")).lower())[:24]
    vb6_readiness = build_vb6_readiness_assessment(vb6_by_path=vb6_by_path, vb6_projects=vb6_projects)
    source_target_profile = build_source_target_modernization_profile(
        legacy_skill_profile=legacy_skill_profile,
        legacy_inventory={
            "vb6_projects": vb6_projects,
            "modernization_readiness": vb6_readiness,
        },
        state={
            "modernization_language": str(target_language or "").strip(),
            "target_platform": str(target_platform or "").strip(),
            "deployment_target": str(deployment_target or "local").strip() or "local",
            "integration_context": {
                "brownfield": {"repo_url": str(source_repo_url or "").strip()},
                "exports": {"github": {"repo_url": str(target_repo_url or "").strip()}},
            },
        },
    )
    project_business_summaries = build_project_business_summaries(
        vb6_projects=vb6_projects,
        source_target_profile=source_target_profile,
        global_readiness=vb6_readiness,
    )
    vb6_event_handler_count_exact = max(len(vb6_event_keys), len(vb6_ui_event_map), len(vb6_events))

    sample_paths = [p for p in ordered_paths if p][:160]
    capabilities = _domain_capabilities(sample_paths, routes)
    if vb6_forms or vb6_controls:
        capabilities.insert(0, "Legacy desktop workflow implemented through VB6 forms and event-driven handlers")
    if vb6_activex:
        capabilities.insert(1, "ActiveX/COM components are part of runtime behavior and modernization scope")
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
    if isinstance(legacy_skill_profile, dict) and legacy_skill_profile.get("selected_skill_id"):
        overview += (
            " Legacy skill selected: "
            f"{str(legacy_skill_profile.get('selected_skill_name', 'Generic Legacy Skill'))} "
            f"({str(legacy_skill_profile.get('selected_skill_id', 'generic_legacy'))}, "
            f"confidence={legacy_skill_profile.get('confidence', 'n/a')})."
        )
    if vb6_forms:
        overview += (
            f" VB6 signals detected: {len(vb6_forms)} forms/usercontrols, "
            f"{len(vb6_controls)} controls, {len(vb6_activex)} ActiveX/COM dependencies, "
            f"{len(vb6_projects)} project(s), {vb6_event_handler_count_exact} event handlers."
        )
        overview += (
            f" .bas modules={len(vb6_bas_modules)} (procedures={vb6_bas_procedure_count}), "
            f"binary companions={len(vb6_binary_companions)}."
        )
        overview += (
            f" Readiness={int(vb6_readiness.get('score', 0) or 0)}/100 "
            f"strategy={str(vb6_readiness.get('recommended_strategy', {}).get('id', 'pending')).replace('_', ' ')}."
        )
    if str(target_language or "").strip():
        overview += f" Target modernization language={str(target_language).strip()}."
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
    if vb6_forms:
        key_components = (key_components + sorted(vb6_forms)[:6])[:16]

    io_contracts = []
    if routes:
        io_contracts.append("HTTP endpoints likely accept JSON payloads and return JSON/API responses.")
    if frameworks:
        io_contracts.append(f"Frameworks detected ({', '.join(sorted(frameworks))}) imply controller/handler based request processing.")
    if vb6_forms:
        io_contracts.append(
            "VB6 event-driven contracts detected: form control inputs trigger Sub handlers that drive business transactions."
        )
    if vb6_activex:
        io_contracts.append(
            f"ActiveX/COM dependency contracts detected: {', '.join(sorted(vb6_activex)[:8])}."
        )
    if vb6_projects:
        io_contracts.append(
            "VB6 multi-project structure detected: "
            + "; ".join(
                [
                    f"{str(p.get('project_name', 'Project'))}(members={p.get('member_count', 0)}, forms={p.get('forms_count', 0)})"
                    for p in vb6_projects[:6]
                    if isinstance(p, dict)
                ]
            )
            + "."
        )
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
    if vb6_forms and not vb6_events:
        unknowns.append("VB6 form files detected but event handler procedures were not extracted from sampled content.")

    vb6_dependency_references = _extract_vbp_dependency_references(vb6_project_defs)

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
        "legacy_skill_profile": legacy_skill_profile,
        "source_target_modernization_profile": source_target_profile,
        "project_business_summaries": project_business_summaries,
        "vb6_file_type_coverage": vb6_file_type_coverage,
        "bas_module_summary": {
            "module_count": len(vb6_bas_modules),
            "modules": sorted(vb6_bas_modules)[:120],
            "procedure_count": vb6_bas_procedure_count,
            "note": "Standard module (.bas) files are treated as primary business-logic sources.",
        },
        "binary_companion_files": vb6_binary_companions[:120],
        "connection_strings": sorted(vb6_connection_strings)[:200],
        "database_file_references": sorted(vb6_database_file_refs)[:200],
        "connection_string_rows": vb6_connection_string_rows[:800],
        "database_file_reference_rows": vb6_database_file_reference_rows[:800],
        "module_global_declarations": vb6_module_global_declarations[:1000],
        "vb6_analysis": {
            "project_count": len(vb6_projects),
            "projects": vb6_projects,
            "forms": sorted(vb6_forms)[:400],
            "controls": sorted(vb6_controls)[:800],
            "activex_dependencies": sorted(vb6_activex)[:400],
            "dependency_references": vb6_dependency_references,
            "event_handlers": sorted(vb6_events)[:1200],
            "event_handler_keys": sorted(vb6_event_keys)[:2400],
            "event_handler_count_exact": vb6_event_handler_count_exact,
            "project_members": sorted(vb6_project_members)[:1200],
            "ui_event_map": list(vb6_ui_event_map.values())[:1200],
            "sql_query_catalog": sorted(vb6_sql_queries)[:160],
            "connection_strings": sorted(vb6_connection_strings)[:200],
            "database_file_references": sorted(vb6_database_file_refs)[:200],
            "connection_string_rows": vb6_connection_string_rows[:800],
            "database_file_reference_rows": vb6_database_file_reference_rows[:800],
            "module_global_declarations": vb6_module_global_declarations[:1000],
            "com_surface_map": {
                "late_bound_progids": sorted(vb6_com_progids)[:120],
                "call_by_name_sites": vb6_callbyname_sites,
                "createobject_getobject_sites": vb6_createobject_sites,
                "references": sorted(vb6_com_references)[:120],
            },
            "win32_declares": sorted(vb6_win32_declares)[:120],
            "error_handling_profile": vb6_error_profile,
            "pitfall_detectors": sorted(vb6_pitfalls.values(), key=lambda row: str(row.get("id", "")))[:120],
            "modernization_readiness": vb6_readiness,
            "source_target_modernization_profile": source_target_profile,
            "project_business_summaries": project_business_summaries,
            "vb6_file_type_coverage": vb6_file_type_coverage,
            "bas_module_summary": {
                "module_count": len(vb6_bas_modules),
                "modules": sorted(vb6_bas_modules)[:120],
                "procedure_count": vb6_bas_procedure_count,
            },
            "binary_companion_files": vb6_binary_companions[:120],
            "vb6_skill_pack_manifest": vb6_skill_pack_manifest(),
        },
        "unknowns": unknowns,
        "evidence_files": evidence_files[:14],
        "stats": {
            "sampled_tree_entries": len(file_entries),
            "sampled_files": len(file_contents),
            "languages": language_counts,
            "route_hints": len(routes),
            "vb6_forms": len(vb6_forms),
            "vb6_controls": len(vb6_controls),
            "vb6_activex_dependencies": len(vb6_activex),
            "vb6_projects": len(vb6_projects),
            "vb6_event_handlers": vb6_event_handler_count_exact,
            "vb6_bas_modules": len(vb6_bas_modules),
            "vb6_bas_procedures": vb6_bas_procedure_count,
            "vb6_binary_companion_files": len(vb6_binary_companions),
            "vb6_connection_string_variants": len(vb6_connection_strings),
            "vb6_database_file_references": len(vb6_database_file_refs),
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
    suffix = Path(path).suffix.lower()
    text = ""
    if encoding == "base64":
        try:
            raw_bytes = base64.b64decode(raw_content.encode("utf-8"), validate=False)
            if suffix in {".frx", ".ctx", ".res", ".ocx", ".mdb", ".accdb"}:
                digest = hashlib.sha1(raw_bytes).hexdigest()[:16]
                if suffix == ".ocx":
                    note = "ActiveX binary component detected."
                elif suffix in {".mdb", ".accdb"}:
                    note = "MS Access database file detected."
                else:
                    note = "Companion binary/resource file detected."
                return (
                    f"[BINARY_COMPANION] file={path} ext={suffix or 'unknown'} bytes={len(raw_bytes)} "
                    f"sha1={digest} note={note}"
                )[:max_chars]
            text = raw_bytes.decode("utf-8", errors="replace")
        except Exception:
            text = ""
    else:
        text = raw_content
    return text[:max_chars]


def _allowed_source_extensions() -> set[str]:
    return {
        ".py", ".js", ".ts", ".tsx", ".go", ".java", ".cs", ".rb", ".php",
        ".asp", ".aspx", ".asa", ".vb", ".vbs",
        ".bas", ".cls", ".frm", ".frx", ".ctl", ".ctx", ".vbp", ".vbg", ".res", ".ocx", ".dcx", ".dca",
        ".mdb", ".accdb",
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
    supplemental_project_entries: list[dict[str, Any]] = []
    for item in raw_entries:
        if not isinstance(item, dict):
            continue
        path = str(item.get("path", "")).strip()
        node_type = str(item.get("type", "")).strip().lower()
        if not path or node_type != "blob":
            continue
        normalized = path.replace("\\", "/")
        suffix = Path(normalized).suffix.lower()
        is_project_descriptor = suffix in {".vbp", ".vbg"}
        in_scope = (not include_paths) or any(normalized.startswith(prefix + "/") or normalized == prefix for prefix in include_paths)
        if exclude_paths and any(normalized.startswith(prefix + "/") or normalized == prefix for prefix in exclude_paths):
            continue
        # Always keep VB6 project descriptor files in scope for attribution, even when include paths
        # are narrowed to subfolders that may omit the root project file.
        if not in_scope and not is_project_descriptor:
            continue
        base_name = Path(normalized).name.lower()
        if suffix in allowed_ext or base_name.startswith("readme"):
            row = {
                "path": normalized,
                "type": "file",
                "depth": normalized.count("/"),
                "size": int(item.get("size", 0) or 0),
                "sha": str(item.get("sha", "") or ""),
                "ext": suffix,
            }
            candidate_entries.append(row)
            if is_project_descriptor and not in_scope:
                supplemental_project_entries.append(row)

    if supplemental_project_entries:
        seen = {str(entry.get("path", "")).strip().lower() for entry in candidate_entries}
        for row in supplemental_project_entries:
            path = str(row.get("path", "")).strip().lower()
            if path and path not in seen:
                candidate_entries.append(row)
                seen.add(path)

    vb6_file_hits = sum(
        1 for entry in candidate_entries
        if str(entry.get("path", "")).lower().endswith((".vbp", ".vbg", ".frm", ".frx", ".bas", ".cls", ".ctl", ".ctx", ".res", ".ocx", ".dcx", ".dca", ".mdb", ".accdb"))
    )
    effective_limit = max(limit, 220) if vb6_file_hits >= 8 else limit

    priority_rank = []
    for entry in candidate_entries:
        path = entry["path"].lower()
        rank = 99
        if "readme" in path:
            rank = 0
        elif path.endswith(".vbp") or path.endswith(".vbg"):
            rank = 0
        elif path.endswith(".bas"):
            rank = 1
        elif path.endswith(".dcx") or path.endswith(".dca"):
            rank = 1
        elif path.endswith(".frm") or path.endswith(".ctl") or path.endswith(".cls"):
            rank = 2
        elif path.endswith(".frx") or path.endswith(".ctx") or path.endswith(".res") or path.endswith(".ocx") or path.endswith(".mdb") or path.endswith(".accdb"):
            # Keep binary companions in scope, but prioritize code-bearing files first for accurate decomposition.
            rank = 8
        elif any(tok in path for tok in ["/main.", "/app.", "/index.", "/server."]):
            rank = 2
        elif "/api/" in path or "/controller" in path or "/routes" in path:
            rank = 3
        elif "/service" in path or "/handler" in path:
            rank = 4
        elif "/model" in path or "/schema" in path:
            rank = 5
        priority_rank.append((rank, path, entry))
    priority_rank.sort(key=lambda row: (row[0], row[1]))
    return [row[2] for row in priority_rank[: max(1, effective_limit)]]


def _compose_legacy_code_bundle(file_contents: dict[str, str], max_total_chars: int = 420000) -> str:
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


def _repo_snapshot_local_dir(snapshot_id: str) -> Path:
    root = RUN_CONTEXT_ARTIFACT_ROOT / "repo_snapshots" / snapshot_id
    root.mkdir(parents=True, exist_ok=True)
    return root


def _repo_snapshot_blob_name(snapshot_id: str, filename: str) -> str:
    prefix = REPO_SNAPSHOT_GCS_PREFIX.strip("/ ")
    return f"{prefix}/{snapshot_id}/{filename}" if prefix else f"{snapshot_id}/{filename}"


def _repo_snapshot_family_key(
    *,
    owner: str,
    repository: str,
    branch: str,
    include_paths: list[str],
    exclude_paths: list[str],
    max_files: int,
) -> str:
    seed = "|".join(
        [
            owner.lower().strip(),
            repository.lower().strip(),
            branch.lower().strip(),
            hashlib.sha1(",".join(sorted(include_paths)).encode("utf-8")).hexdigest()[:12],
            hashlib.sha1(",".join(sorted(exclude_paths)).encode("utf-8")).hexdigest()[:12],
            str(int(max_files or 0)),
        ]
    )
    return hashlib.sha1(seed.encode("utf-8")).hexdigest()[:24]


def _repo_snapshot_latest_ref_blob_name(family_key: str) -> str:
    prefix = REPO_SNAPSHOT_GCS_PREFIX.strip("/ ")
    return f"{prefix}/_index/{family_key}.json" if prefix else f"_index/{family_key}.json"


def _repo_snapshot_latest_ref_load(family_key: str) -> str:
    key = str(family_key or "").strip()
    if not key:
        return ""
    if REPO_SNAPSHOT_GCS_BUCKET and storage is not None:
        try:
            client = storage.Client()
            bucket = client.bucket(REPO_SNAPSHOT_GCS_BUCKET)
            blob = bucket.blob(_repo_snapshot_latest_ref_blob_name(key))
            if blob.exists():
                payload = json.loads(blob.download_as_text())
                if isinstance(payload, dict):
                    sid = str(payload.get("snapshot_id", "")).strip()
                    if sid:
                        return sid
        except Exception:
            pass
    local = RUN_CONTEXT_ARTIFACT_ROOT / "repo_snapshots" / "_index" / f"{key}.json"
    try:
        if local.exists():
            payload = json.loads(local.read_text())
            if isinstance(payload, dict):
                return str(payload.get("snapshot_id", "")).strip()
    except Exception:
        return ""
    return ""


def _repo_snapshot_latest_ref_save(family_key: str, snapshot_id: str) -> None:
    key = str(family_key or "").strip()
    sid = str(snapshot_id or "").strip()
    if not key or not sid:
        return
    payload = {"snapshot_id": sid, "updated_at": _utc_now()}
    local_dir = RUN_CONTEXT_ARTIFACT_ROOT / "repo_snapshots" / "_index"
    local_dir.mkdir(parents=True, exist_ok=True)
    (local_dir / f"{key}.json").write_text(json.dumps(payload, indent=2, ensure_ascii=True))
    if REPO_SNAPSHOT_GCS_BUCKET and storage is not None:
        try:
            client = storage.Client()
            bucket = client.bucket(REPO_SNAPSHOT_GCS_BUCKET)
            bucket.blob(_repo_snapshot_latest_ref_blob_name(key)).upload_from_string(
                json.dumps(payload, indent=2, ensure_ascii=True),
                content_type="application/json",
            )
        except Exception:
            pass


def _repo_changed_paths_via_compare(
    *,
    base_url: str,
    owner: str,
    repository: str,
    base_commit: str,
    head_commit: str,
    headers: dict[str, str],
) -> tuple[set[str], str]:
    if not base_commit or not head_commit or base_commit == head_commit:
        return set(), ""
    try:
        compare = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/compare/{quote(base_commit, safe='')}...{quote(head_commit, safe='')}",
            headers=headers,
        )
    except Exception as exc:
        return set(), str(exc)
    if not isinstance(compare, dict):
        return set(), "invalid compare payload"
    files = compare.get("files", [])
    if not isinstance(files, list):
        return set(), "compare payload missing files list"
    changed: set[str] = set()
    for row in files:
        if not isinstance(row, dict):
            continue
        filename = str(row.get("filename", "")).strip().replace("\\", "/")
        if filename:
            changed.add(filename)
        prev = str(row.get("previous_filename", "")).strip().replace("\\", "/")
        if prev:
            changed.add(prev)
    return changed, ""


def _repo_snapshot_load(snapshot_id: str) -> dict[str, Any] | None:
    sid = str(snapshot_id or "").strip()
    if not sid:
        return None
    # Prefer GCS shared cache when configured.
    if REPO_SNAPSHOT_GCS_BUCKET and storage is not None:
        try:
            client = storage.Client()
            bucket = client.bucket(REPO_SNAPSHOT_GCS_BUCKET)
            snap_blob = bucket.blob(_repo_snapshot_blob_name(sid, "snapshot.json"))
            if snap_blob.exists():
                payload = json.loads(snap_blob.download_as_text())
                if isinstance(payload, dict):
                    return payload
        except Exception:
            pass
    # Local fallback cache.
    path = RUN_CONTEXT_ARTIFACT_ROOT / "repo_snapshots" / sid / "snapshot.json"
    try:
        if path.exists():
            payload = json.loads(path.read_text())
            if isinstance(payload, dict):
                return payload
    except Exception:
        return None
    return None


def _repo_snapshot_save(snapshot_id: str, payload: dict[str, Any]) -> None:
    sid = str(snapshot_id or "").strip()
    if not sid or not isinstance(payload, dict):
        return
    local_dir = _repo_snapshot_local_dir(sid)
    snapshot_path = local_dir / "snapshot.json"
    bundle_path = local_dir / "bundle.txt"
    snapshot_path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
    bundle_path.write_text(str(payload.get("legacy_code", "") or ""))

    if REPO_SNAPSHOT_GCS_BUCKET and storage is not None:
        try:
            client = storage.Client()
            bucket = client.bucket(REPO_SNAPSHOT_GCS_BUCKET)
            bucket.blob(_repo_snapshot_blob_name(sid, "snapshot.json")).upload_from_string(
                json.dumps(payload, indent=2, ensure_ascii=True, default=str),
                content_type="application/json",
            )
            bucket.blob(_repo_snapshot_blob_name(sid, "bundle.txt")).upload_from_string(
                str(payload.get("legacy_code", "") or ""),
                content_type="text/plain",
            )
        except Exception:
            # Keep local snapshot even if GCS upload fails.
            pass


def _path_language_hint(path: str) -> str:
    suffix = Path(path).suffix.lower()
    if suffix in {".frm", ".bas", ".cls", ".vbp", ".vbg", ".frx", ".ctl", ".ctx", ".dca", ".dcx", ".mdb", ".accdb"}:
        return "vb6"
    if suffix in {".py"}:
        return "python"
    if suffix in {".js", ".ts", ".tsx"}:
        return "javascript"
    if suffix in {".go"}:
        return "go"
    if suffix in {".java"}:
        return "java"
    if suffix in {".cs"}:
        return "csharp"
    if suffix in {".sql"}:
        return "sql"
    if suffix in {".md"}:
        return "markdown"
    return "unknown"


def _chunk_entries(entries: list[dict[str, Any]], chunk_size: int) -> list[list[dict[str, Any]]]:
    size = max(1, int(chunk_size or 1))
    out: list[list[dict[str, Any]]] = []
    for i in range(0, len(entries), size):
        out.append(entries[i : i + size])
    return out


def _resolve_legacy_code_from_repo_scan(
    integration_context: dict[str, Any],
    *,
    progress_cb: Any | None = None,
) -> tuple[str, dict[str, Any], str]:
    """
    Returns (legacy_code, repo_scan_cache, error_message).
    Expensive network fetch happens here so callers can run it in background worker threads.
    """
    def emit(message: str) -> None:
        if callable(progress_cb):
            try:
                progress_cb(str(message))
            except Exception:
                pass

    integration = integration_context if isinstance(integration_context, dict) else {}
    scan_scope = integration.get("scan_scope", {}) if isinstance(integration.get("scan_scope", {}), dict) else {}
    brownfield = integration.get("brownfield", {}) if isinstance(integration.get("brownfield", {}), dict) else {}
    sample_mode = bool(integration.get("sample_dataset_enabled", False))

    repo_provider = str(brownfield.get("repo_provider", "")).strip().lower()
    repo_url = str(brownfield.get("repo_url", "")).strip()
    include_paths = _normalize_lines(scan_scope.get("include_paths", []))
    exclude_paths = _normalize_lines(scan_scope.get("exclude_paths", []))

    if repo_provider != "github" or not repo_url:
        return "", {}, "Repo scan mode requires a connected GitHub repository URL."

    owner, repository = _parse_github_repo_url(repo_url)
    github_cfg = SETTINGS_STORE.get_integration_config("github")
    if not owner:
        owner = str(github_cfg.get("owner", "")).strip()
    if not repository:
        repository = str(github_cfg.get("repository", "")).strip()
    if not owner or not repository:
        return "", {}, "Unable to resolve GitHub owner/repository from integration settings."

    if sample_mode:
        sample_contents = {
            "services/orders-service/src/main.java": "@RestController\n@RequestMapping(\"/v1/orders\")\npublic class OrdersController {}",
            "services/payments-service/cmd/api/main.go": "router.POST(\"/v1/payments\", handlePayments)\n// uses redis idempotency",
            "services/inventory-service/index.js": "app.post('/v1/inventory/reserve', reserveInventory)",
            "legacy/billing-monolith/README.md": "Legacy billing and invoicing flow.",
        }
        legacy_code = _compose_legacy_code_bundle(sample_contents, max_total_chars=REPO_SCAN_BUNDLE_MAX_CHARS)
        sid_seed = f"sample::{owner}/{repository}::{','.join(sorted(sample_contents.keys()))}"
        snapshot_id = hashlib.sha1(sid_seed.encode("utf-8")).hexdigest()[:20]
        payload = {
            "snapshot_id": snapshot_id,
            "owner": owner,
            "repository": repository,
            "branch": "sample",
            "commit_sha": "sample",
            "tree_sha": "sample",
            "repo_provider": "github",
            "repo_url": repo_url,
            "created_at": _utc_now(),
            "cache_hit": False,
            "selected_file_count": len(sample_contents),
            "manifest": [
                {
                    "path": p,
                    "size": len(sample_contents.get(p, "")),
                    "sha": hashlib.sha1(str(sample_contents.get(p, "")).encode("utf-8")).hexdigest()[:12],
                    "language": _path_language_hint(p),
                    "is_binary": False,
                }
                for p in sorted(sample_contents.keys())
            ],
            "legacy_code": legacy_code,
        }
        _repo_snapshot_save(snapshot_id, payload)
        return legacy_code, {"owner": owner, "repository": repository, "sampled_files": sorted(sample_contents.keys()), "repo_snapshot": payload}, ""

    token = str(github_cfg.get("token", "")).strip()
    if not token:
        return "", {}, "GitHub token is missing. Configure it in Settings > Integrations."

    base_url = str(github_cfg.get("base_url") or "https://api.github.com").rstrip("/")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "User-Agent": "synthetix-discover/1.0",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    try:
        emit("📁 Repo snapshot: resolving repository metadata")
        repo_meta = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}",
            headers=headers,
        )
        branch = "main"
        if isinstance(repo_meta, dict):
            branch = str(repo_meta.get("default_branch") or "main")
        commit_payload = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/commits/{quote(branch, safe='')}",
            headers=headers,
        )
        commit_sha = ""
        if isinstance(commit_payload, dict):
            commit_sha = str(commit_payload.get("sha", "")).strip()
        tree_payload = _http_json_request(
            f"{base_url}/repos/{quote(owner)}/{quote(repository)}/git/trees/{quote(branch, safe='')}?recursive=1",
            headers=headers,
        )
        raw_entries = tree_payload.get("tree", []) if isinstance(tree_payload, dict) else []
        tree_sha = str(tree_payload.get("sha", "")).strip() if isinstance(tree_payload, dict) else ""
        selected_entries = _select_source_entries_for_analysis(
            [item for item in raw_entries if isinstance(item, dict)],
            include_paths=include_paths,
            exclude_paths=exclude_paths,
            limit=REPO_SCAN_MAX_FILES,
        )
        if not selected_entries:
            return "", {}, "Repo scan completed but no eligible source files matched current include/exclude filters."

        family_key = _repo_snapshot_family_key(
            owner=owner,
            repository=repository,
            branch=branch,
            include_paths=include_paths,
            exclude_paths=exclude_paths,
            max_files=REPO_SCAN_MAX_FILES,
        )

        snapshot_seed = "|".join(
            [
                "github",
                owner.lower(),
                repository.lower(),
                branch.lower(),
                commit_sha or tree_sha or "unknown",
                hashlib.sha1(",".join(include_paths).encode("utf-8")).hexdigest()[:12],
                hashlib.sha1(",".join(exclude_paths).encode("utf-8")).hexdigest()[:12],
                str(REPO_SCAN_MAX_FILES),
                str(REPO_SCAN_CHUNK_SIZE),
            ]
        )
        snapshot_id = hashlib.sha1(snapshot_seed.encode("utf-8")).hexdigest()[:20]
        cached = _repo_snapshot_load(snapshot_id)
        if isinstance(cached, dict) and str(cached.get("legacy_code", "")).strip():
            emit(f"📦 Repo snapshot cache hit: {snapshot_id} (commit {commit_sha[:12] or 'unknown'})")
            sampled = cached.get("sampled_files", [])
            return (
                str(cached.get("legacy_code", "")),
                {
                    "owner": owner,
                    "repository": repository,
                    "default_branch": branch,
                    "commit_sha": commit_sha,
                    "sampled_files": sampled[:40] if isinstance(sampled, list) else [],
                    "repo_snapshot": {
                        **{k: v for k, v in cached.items() if k not in {"legacy_code", "file_contents"}},
                        "cache_hit": True,
                    },
                },
                "",
            )

        base_snapshot_id = _repo_snapshot_latest_ref_load(family_key)
        base_snapshot = _repo_snapshot_load(base_snapshot_id) if base_snapshot_id and base_snapshot_id != snapshot_id else None
        base_file_contents = (
            {str(k): str(v) for k, v in base_snapshot.get("file_contents", {}).items()}
            if isinstance(base_snapshot, dict) and isinstance(base_snapshot.get("file_contents", {}), dict)
            else {}
        )
        base_commit = str(base_snapshot.get("commit_sha", "")).strip() if isinstance(base_snapshot, dict) else ""
        changed_paths, compare_error = _repo_changed_paths_via_compare(
            base_url=base_url,
            owner=owner,
            repository=repository,
            base_commit=base_commit,
            head_commit=commit_sha,
            headers=headers,
        ) if base_file_contents and base_commit and commit_sha and base_commit != commit_sha else (set(), "")
        if base_file_contents and base_commit and commit_sha and base_commit != commit_sha:
            if compare_error:
                emit(f"⚠️ Incremental compare unavailable; full chunk fetch for changed commit ({compare_error})")
            else:
                emit(
                    f"🧮 Incremental diff loaded: base={base_commit[:12]} head={commit_sha[:12]} "
                    f"changed_paths={len(changed_paths)}"
                )

        chunks = _chunk_entries(selected_entries, REPO_SCAN_CHUNK_SIZE)
        emit(
            f"📦 Repo snapshot created: {snapshot_id} "
            f"(commit {commit_sha[:12] or 'unknown'}, files={len(selected_entries)}, chunks={len(chunks)})"
        )

        file_contents: dict[str, str] = {}
        failed_paths: list[str] = []
        reused_paths: list[str] = []
        compare_enabled = bool(base_file_contents and base_commit and commit_sha and base_commit != commit_sha and not compare_error)
        selected_path_set = {str(entry.get("path", "")).strip() for entry in selected_entries if str(entry.get("path", "")).strip()}
        for idx, chunk in enumerate(chunks, start=1):
            emit(f"🧩 Repo scan chunk {idx}/{len(chunks)}: {len(chunk)} file(s)")
            with ThreadPoolExecutor(max_workers=REPO_SCAN_CHUNK_WORKERS) as pool:
                futures = {}
                for entry in chunk:
                    path = str(entry.get("path", "")).strip()
                    if not path:
                        continue
                    if compare_enabled and path in base_file_contents and path not in changed_paths:
                        content = str(base_file_contents.get(path, "") or "")
                        if content:
                            file_contents[path] = content
                            reused_paths.append(path)
                            continue
                    fut = pool.submit(
                        _fetch_github_file_content,
                        base_url=base_url,
                        owner=owner,
                        repository=repository,
                        path=path,
                        ref=branch,
                        headers=headers,
                        max_chars=12000,
                    )
                    futures[fut] = path
                for fut in as_completed(list(futures.keys())):
                    path = futures.get(fut, "")
                    try:
                        content = str(fut.result() or "")
                    except Exception:
                        content = ""
                    if content:
                        file_contents[path] = content
                    else:
                        if path:
                            failed_paths.append(path)
            emit(
                f"✅ Chunk {idx}/{len(chunks)} complete: "
                f"resolved={len(file_contents)} reused={len(reused_paths)} failed={len(failed_paths)}"
            )

        legacy_code = _compose_legacy_code_bundle(file_contents, max_total_chars=REPO_SCAN_BUNDLE_MAX_CHARS)
        if not legacy_code:
            return "", {}, "Repo scan completed but no analyzable source content was extracted."
        manifest = []
        for entry in selected_entries:
            path = str(entry.get("path", "")).strip()
            if not path:
                continue
            ext = str(entry.get("ext", "") or "").strip().lower()
            manifest.append(
                {
                    "path": path,
                    "size": int(entry.get("size", 0) or 0),
                    "sha": str(entry.get("sha", "") or ""),
                    "ext": ext,
                    "depth": int(entry.get("depth", 0) or 0),
                    "language": _path_language_hint(path),
                    "is_binary": ext in {".frx", ".ctx", ".res", ".ocx", ".mdb", ".accdb"},
                }
            )

        snapshot_payload = {
            "snapshot_id": snapshot_id,
            "family_key": family_key,
            "repo_provider": "github",
            "repo_url": repo_url,
            "owner": owner,
            "repository": repository,
            "branch": branch,
            "commit_sha": commit_sha,
            "base_snapshot_id": base_snapshot_id if isinstance(base_snapshot, dict) else "",
            "base_commit_sha": base_commit if isinstance(base_snapshot, dict) else "",
            "tree_sha": tree_sha,
            "created_at": _utc_now(),
            "include_paths": include_paths,
            "exclude_paths": exclude_paths,
            "total_tree_entries": len(raw_entries),
            "selected_file_count": len(selected_entries),
            "selected_paths": sorted(selected_path_set)[:400],
            "chunk_count": len(chunks),
            "chunk_size": REPO_SCAN_CHUNK_SIZE,
            "chunk_workers": REPO_SCAN_CHUNK_WORKERS,
            "compare_error": compare_error,
            "changed_path_count": len(changed_paths),
            "changed_paths_sample": sorted(list(changed_paths))[:200],
            "reused_file_count": len(reused_paths),
            "reused_paths_sample": sorted(reused_paths)[:200],
            "failed_paths": failed_paths[:200],
            "sampled_files": sorted(file_contents.keys())[:80],
            "manifest": manifest,
            "file_contents": file_contents,
            "legacy_code": legacy_code,
        }
        _repo_snapshot_save(snapshot_id, snapshot_payload)
        _repo_snapshot_latest_ref_save(family_key, snapshot_id)
        emit(f"🗂️ Repo snapshot persisted: {snapshot_id} (bundle chars={len(legacy_code)})")
        return (
            legacy_code,
            {
                "owner": owner,
                "repository": repository,
                "default_branch": branch,
                "commit_sha": commit_sha,
                "sampled_files": sorted(file_contents.keys())[:40],
                "repo_snapshot": {k: v for k, v in snapshot_payload.items() if k not in {"legacy_code", "file_contents"}},
            },
            "",
        )
    except Exception as exc:
        return "", {}, f"Repo scan failed: {exc}"


def _normalize_mdbtools_engine(engine: str) -> str:
    token = str(engine or "").strip().lower()
    if token in {"postgres", "postgresql"}:
        return "postgres"
    if token in {"mysql", "mariadb"}:
        return "mysql"
    if token in {"oracle"}:
        return "oracle"
    if token in {"access", "jet"}:
        return "access"
    if token in {"sqlite"}:
        # mdb-schema does not consistently provide SQLite flavor in all builds.
        return "postgres"
    if token in {"sqlserver", "sql server", "mssql"}:
        return "postgres"
    return "postgres"


def _run_command(args: list[str], timeout: int = 60) -> tuple[int, str, str]:
    try:
        proc = subprocess.run(
            args,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            timeout=timeout,
            check=False,
        )
    except Exception as exc:
        return 1, "", str(exc)
    stdout = proc.stdout.decode("utf-8", errors="replace") if isinstance(proc.stdout, (bytes, bytearray)) else str(proc.stdout or "")
    stderr = proc.stderr.decode("utf-8", errors="replace") if isinstance(proc.stderr, (bytes, bytearray)) else str(proc.stderr or "")
    return int(proc.returncode), stdout, stderr


def _parse_mdb_table_list(raw: str) -> list[str]:
    tables: list[str] = []
    for line in str(raw or "").splitlines():
        for token in re.split(r"[,\s]+", line.strip()):
            name = str(token or "").strip()
            if not name:
                continue
            if name not in tables:
                tables.append(name)
    return tables


def _inspect_access_database(file_path: Path, *, target_engine: str = "postgres") -> dict[str, Any]:
    mdb_tables = shutil.which("mdb-tables")
    mdb_schema = shutil.which("mdb-schema")
    if not mdb_tables or not mdb_schema:
        missing = []
        if not mdb_tables:
            missing.append("mdb-tables")
        if not mdb_schema:
            missing.append("mdb-schema")
        return {
            "ok": False,
            "error": (
                f"Access parser requires mdbtools commands: {', '.join(missing)}. "
                "Install mdbtools to enable .mdb/.accdb schema extraction."
            ),
            "parser": "mdbtools",
            "warnings": [
                "Fallback unavailable for binary Access files without mdbtools.",
            ],
        }

    rc_tables, out_tables, err_tables = _run_command([mdb_tables, "-1", str(file_path)], timeout=40)
    if rc_tables != 0:
        return {
            "ok": False,
            "error": f"Failed to read Access tables: {err_tables.strip() or 'unknown mdb-tables error'}",
            "parser": "mdbtools",
        }

    all_tables = _parse_mdb_table_list(out_tables)
    user_tables = [t for t in all_tables if not str(t).lower().startswith("msys")]
    system_tables = [t for t in all_tables if str(t).lower().startswith("msys")]
    if not user_tables:
        return {
            "ok": False,
            "error": "No user tables found in Access database.",
            "parser": "mdbtools",
        }

    backend = _normalize_mdbtools_engine(target_engine)
    rc_schema, out_schema, err_schema = _run_command([mdb_schema, str(file_path), backend], timeout=120)
    if rc_schema != 0 or not str(out_schema).strip():
        # Retry with default backend if selected backend flavor is unsupported by local mdbtools build.
        rc_schema, out_schema, err_schema = _run_command([mdb_schema, str(file_path)], timeout=120)
    if rc_schema != 0:
        return {
            "ok": False,
            "error": f"Failed to extract Access schema: {err_schema.strip() or 'unknown mdb-schema error'}",
            "parser": "mdbtools",
        }

    schema_text = str(out_schema or "").strip()
    header = [
        "-- Source database: Microsoft Access",
        f"-- Parsed with: mdbtools",
        f"-- User tables detected: {len(user_tables)}",
        f"-- Sample tables: {', '.join(user_tables[:12])}",
    ]
    if system_tables:
        header.append(f"-- System tables ignored: {len(system_tables)}")
    final_schema = "\n".join(header) + "\n\n" + schema_text

    warnings: list[str] = []
    if str(file_path.suffix).lower() == ".accdb":
        warnings.append("ACCDB parsing support depends on local mdbtools build; verify output accuracy.")

    return {
        "ok": True,
        "parser": "mdbtools",
        "target_flavor": backend,
        "table_count": len(user_tables),
        "tables": user_tables[:200],
        "system_table_count": len(system_tables),
        "warnings": warnings,
        "database_schema": final_schema[:600000],
    }


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


def _llm_preflight_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    provider_raw = str(payload.get("provider", "anthropic")).strip().lower()
    if provider_raw not in {"anthropic", "openai"}:
        raise ValueError("provider must be 'anthropic' or 'openai'")

    requested_model = str(payload.get("model", "")).strip()
    incoming_key = str(payload.get("api_key", "")).strip()
    key_source = "payload" if incoming_key else "settings"
    llm_cfg = SETTINGS_STORE.get_llm_provider_config(provider_raw)
    resolved_key = incoming_key or str(llm_cfg.get("api_key", "")).strip()
    resolved_model = requested_model or str(llm_cfg.get("model", "")).strip() or (
        "gpt-4o" if provider_raw == "openai" else "claude-sonnet-4-20250514"
    )
    base_url = str(llm_cfg.get("base_url", "")).strip().lower()

    if not resolved_key:
        return {
            "ok": False,
            "provider": provider_raw,
            "model": resolved_model,
            "key_source": key_source,
            "error": (
                f"No {provider_raw} API key is configured. "
                "Save it in Settings > LLM credentials."
            ),
            "code": "missing_key",
        }

    key_lower = resolved_key.lower()
    if provider_raw == "openai":
        if key_lower.startswith("bearer "):
            return {
                "ok": False,
                "provider": provider_raw,
                "model": resolved_model,
                "key_source": key_source,
                "error": "OpenAI API key is malformed. Paste only the raw key value (sk-... / sk-proj-...), not 'Bearer ...'.",
                "code": "malformed_key",
            }
        if key_lower.startswith("openai_api_key") or key_lower.startswith("export openai_api_key"):
            return {
                "ok": False,
                "provider": provider_raw,
                "model": resolved_model,
                "key_source": key_source,
                "error": "OpenAI API key is malformed. Paste only the key value, not 'OPENAI_API_KEY=...'.",
                "code": "malformed_key",
            }
        is_openai_host = ("api.openai.com" in base_url) or (not base_url)
        if is_openai_host and not re.match(r"^sk-(proj-)?[A-Za-z0-9_\-]{20,}$", resolved_key):
            return {
                "ok": False,
                "provider": provider_raw,
                "model": resolved_model,
                "key_source": key_source,
                "error": "OpenAI API key format looks invalid for api.openai.com. Expected sk-... or sk-proj-....",
                "code": "malformed_key",
            }
    elif provider_raw == "anthropic":
        if key_lower.startswith("bearer "):
            return {
                "ok": False,
                "provider": provider_raw,
                "model": resolved_model,
                "key_source": key_source,
                "error": "Anthropic API key is malformed. Paste only the raw key value (sk-ant-...), not 'Bearer ...'.",
                "code": "malformed_key",
            }
        if key_lower.startswith("anthropic_api_key") or key_lower.startswith("export anthropic_api_key"):
            return {
                "ok": False,
                "provider": provider_raw,
                "model": resolved_model,
                "key_source": key_source,
                "error": "Anthropic API key is malformed. Paste only the key value, not 'ANTHROPIC_API_KEY=...'.",
                "code": "malformed_key",
            }
        if not re.match(r"^sk-ant-[A-Za-z0-9_\-]{16,}$", resolved_key):
            return {
                "ok": False,
                "provider": provider_raw,
                "model": resolved_model,
                "key_source": key_source,
                "error": "Anthropic API key format looks invalid. Expected sk-ant-....",
                "code": "malformed_key",
            }

    return {
        "ok": True,
        "provider": provider_raw,
        "model": resolved_model,
        "key_source": key_source,
    }


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


async def api_domain_packs(_request):
    packs = list_domain_packs()
    rows: list[dict[str, Any]] = []
    for pack in packs:
        if not isinstance(pack, dict):
            continue
        ontology = pack.get("ontology", {}) if isinstance(pack.get("ontology", {}), dict) else {}
        capabilities = ontology.get("capabilities", []) if isinstance(ontology.get("capabilities", []), list) else []
        rows.append(
            {
                "id": str(pack.get("id", "")).strip(),
                "name": str(pack.get("name", "")).strip() or "Domain Pack",
                "version": str(pack.get("version", "")).strip() or "1.0.0",
                "framework": str(ontology.get("framework", "")).strip() or "Capability Taxonomy",
                "capability_count": len([x for x in capabilities if isinstance(x, dict)]),
            }
        )
    rows.sort(key=lambda x: str(x.get("name", "")).lower())
    return JSONResponse({"ok": True, "domain_packs": rows})


async def api_legacy_skills(_request):
    rows = list_legacy_skills()
    rows.sort(key=lambda x: str(x.get("name", "")).lower())
    return JSONResponse({"ok": True, "legacy_skills": rows})


async def api_agent_personas(request):
    params = request.query_params
    role = str(params.get("role", "")).strip().lower()
    persona_id = str(params.get("persona_id", "")).strip().lower()
    version = str(params.get("version", "")).strip()
    if persona_id:
        persona = PERSONA_REGISTRY.get_persona(persona_id, version=version)
        if not persona:
            return JSONResponse({"ok": False, "error": "persona not found"}, status_code=404)
        return JSONResponse({"ok": True, "persona": persona})
    return JSONResponse({"ok": True, "personas": PERSONA_REGISTRY.list_personas(role=role)})


async def api_agent_persona_upsert(request):
    payload = _get_json(await request.body())
    try:
        persona = PERSONA_REGISTRY.upsert_persona(payload)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "persona": persona})


async def api_analyst_aas_analyze(request):
    payload = _get_json(await request.body())
    requirement = str(payload.get("requirement") or payload.get("business_objective") or "").strip()
    if not requirement:
        return JSONResponse({"ok": False, "error": "requirement is required"}, status_code=400)
    try:
        result = ANALYST_AAS.analyze(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"analyst AAS failed: {exc}"}, status_code=500)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


def _memory_scope_from_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return {
        "workspace_id": str(payload.get("workspace_id", "default-workspace")).strip() or "default-workspace",
        "client_id": str(payload.get("client_id", "default-client")).strip() or "default-client",
        "project_id": str(payload.get("project_id", "default-project")).strip() or "default-project",
    }


def _memory_scope_from_query(params: Any) -> dict[str, Any]:
    return {
        "workspace_id": str(params.get("workspace_id", "default-workspace")).strip() or "default-workspace",
        "client_id": str(params.get("client_id", "default-client")).strip() or "default-client",
        "project_id": str(params.get("project_id", "default-project")).strip() or "default-project",
    }


async def api_memory_add_constraint(request):
    payload = _get_json(await request.body())
    scope = _memory_scope_from_payload(payload)
    text = str(payload.get("text", "")).strip()
    if not text:
        return JSONResponse({"ok": False, "error": "text is required"}, status_code=400)
    try:
        row = TENANT_MEMORY_STORE.add_constraint(
            scope,
            text=text,
            source=str(payload.get("source", "manual")),
            created_by=_request_actor(request),
            priority=str(payload.get("priority", "medium")),
            applies_to=str(payload.get("applies_to", "all")),
            promote_to=str(payload.get("promote_to", "work_item")),
            applies_when=payload.get("applies_when", {}),
            enforcement=payload.get("enforcement", {}),
            evidence_refs=payload.get("evidence_refs", []),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "constraint": row})


async def api_memory_thread(request):
    params = request.query_params
    scope = _memory_scope_from_query(params)
    thread_id = str(params.get("thread_id", "default-thread"))
    limit = int(str(params.get("limit", "60")) or 60)
    rows = TENANT_MEMORY_STORE.get_thread(scope, thread_id=thread_id, limit=max(1, min(limit, 200)))
    return JSONResponse({"ok": True, "thread_id": thread_id, "messages": rows})


async def api_memory_items(request):
    if request.method == "GET":
        params = request.query_params
        scope = _memory_scope_from_query(params)
        query = str(params.get("query", "")).strip()
        status = str(params.get("status", "approved")).strip()
        tier = str(params.get("tier", "")).strip()
        limit = max(1, min(int(str(params.get("limit", "80")) or 80), 400))
        fingerprint = _get_json(str(params.get("fingerprint", "{}")).encode("utf-8"))
        if query:
            rows = TENANT_MEMORY_STORE.search_memory_items(
                scope,
                query=query,
                fingerprint=fingerprint if isinstance(fingerprint, dict) else {},
                limit=limit,
                statuses=[status] if status else None,
                tiers=[tier] if tier else None,
            )
        else:
            rows = TENANT_MEMORY_STORE.list_memory_items(
                scope,
                status=status,
                tier=tier,
                limit=limit,
            )
        return JSONResponse({"ok": True, "items": rows})

    payload = _get_json(await request.body())
    scope = _memory_scope_from_payload(payload)
    statement = str(payload.get("statement", "")).strip()
    if not statement:
        return JSONResponse({"ok": False, "error": "statement is required"}, status_code=400)
    try:
        row = TENANT_MEMORY_STORE.add_memory_item(
            scope,
            item_type=str(payload.get("type", "constraint")),
            title=str(payload.get("title", "")).strip() or statement[:96],
            statement=statement,
            created_by=_request_actor(request),
            source=str(payload.get("source", "manual")),
            tier=str(payload.get("promote_to", payload.get("tier", "work_item"))),
            status=str(payload.get("status", "")).strip() or None,
            applies_when=payload.get("applies_when", {}),
            enforcement=payload.get("enforcement", {}),
            evidence_refs=payload.get("evidence_refs", []),
            metadata=payload.get("metadata", {}),
            approved_by=str(payload.get("approved_by", "")),
            expires_at=str(payload.get("expires_at", "")) or None,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "item": row})


async def api_memory_item_status(request):
    item_id = str(request.path_params.get("item_id", "")).strip()
    payload = _get_json(await request.body())
    scope = _memory_scope_from_payload(payload)
    status = str(payload.get("status", "")).strip()
    if not item_id:
        return JSONResponse({"ok": False, "error": "item_id is required"}, status_code=400)
    if not status:
        return JSONResponse({"ok": False, "error": "status is required"}, status_code=400)
    row = TENANT_MEMORY_STORE.update_memory_item_status(
        scope,
        item_id=item_id,
        status=status,
        actor=_request_actor(request),
        approved_by=str(payload.get("approved_by", "")),
    )
    if not row:
        return JSONResponse({"ok": False, "error": "memory item not found"}, status_code=404)
    return JSONResponse({"ok": True, "item": row})


async def api_memory_review_queue(request):
    if request.method == "GET":
        params = request.query_params
        scope = _memory_scope_from_query(params)
        status = str(params.get("status", "pending")).strip()
        limit = max(1, min(int(str(params.get("limit", "80")) or 80), 500))
        rows = TENANT_MEMORY_STORE.list_review_queue(scope, status=status, limit=limit)
        return JSONResponse({"ok": True, "candidates": rows})

    payload = _get_json(await request.body())
    scope = _memory_scope_from_payload(payload)
    summary = str(payload.get("summary", "")).strip()
    if not summary:
        return JSONResponse({"ok": False, "error": "summary is required"}, status_code=400)
    try:
        row = TENANT_MEMORY_STORE.add_review_candidate(
            scope,
            summary=summary,
            source=str(payload.get("source", "manual")),
            created_by=_request_actor(request),
            proposed_item=payload.get("proposed_item", {}),
            patch=payload.get("patch", []),
            evidence_refs=payload.get("evidence_refs", []),
            metadata=payload.get("metadata", {}),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "candidate": row})


async def api_memory_review_resolve(request):
    candidate_id = str(request.path_params.get("candidate_id", "")).strip()
    payload = _get_json(await request.body())
    scope = _memory_scope_from_payload(payload)
    if not candidate_id:
        return JSONResponse({"ok": False, "error": "candidate_id is required"}, status_code=400)
    action = str(payload.get("action", "")).strip().lower()
    try:
        result = TENANT_MEMORY_STORE.resolve_review_candidate(
            scope,
            candidate_id=candidate_id,
            action=action,
            actor=_request_actor(request),
            promote_to=str(payload.get("promote_to", "")),
            note=str(payload.get("note", "")),
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, **result})


async def api_memory_audit(request):
    params = request.query_params
    scope = _memory_scope_from_query(params)
    limit = max(1, min(int(str(params.get("limit", "120")) or 120), 1000))
    rows = TENANT_MEMORY_STORE.get_audit_log(scope, limit=limit)
    return JSONResponse({"ok": True, "audit_log": rows})


def _request_actor(request) -> str:
    actor = str(request.headers.get("x-user-email", "")).strip().lower()
    if actor:
        return actor
    actor = str(request.headers.get("x-user", "")).strip()
    if actor:
        return actor
    return "local-user@synthetix.local"


async def api_get_settings(_request):
    return JSONResponse({"ok": True, "settings": SETTINGS_STORE.get_settings()})


async def api_get_current_user(request):
    actor = _request_actor(request)
    profile = SETTINGS_STORE.resolve_user_access(actor)
    return JSONResponse({"ok": True, "actor": actor, "user": profile})


async def api_upsert_user(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.upsert_user(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "settings": settings, "users": settings.get("users", [])})


async def api_set_user_status(request):
    payload = _get_json(await request.body())
    email = str(payload.get("email", "")).strip()
    status = str(payload.get("status", "")).strip()
    try:
        settings = SETTINGS_STORE.set_user_status(email, status, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "settings": settings, "users": settings.get("users", [])})


async def api_remove_user(request):
    payload = _get_json(await request.body())
    email = str(payload.get("email", "")).strip()
    try:
        settings = SETTINGS_STORE.remove_user(email, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "settings": settings, "users": settings.get("users", [])})


async def api_upsert_knowledge_source(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.upsert_knowledge_source(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "sources": hub.get("sources", [])})


async def api_upload_knowledge_source(request):
    try:
        form = await request.form()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Invalid multipart form data: {exc}"}, status_code=400)

    upload = form.get("file")
    if upload is None:
        return JSONResponse({"ok": False, "error": "file is required"}, status_code=400)

    filename = str(getattr(upload, "filename", "") or "").strip()
    if not filename:
        filename = "knowledge_source.bin"
    try:
        blob = await upload.read()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Failed to read uploaded file: {exc}"}, status_code=400)
    if not blob:
        return JSONResponse({"ok": False, "error": "Uploaded file is empty."}, status_code=400)
    if len(blob) > KNOWLEDGE_SOURCE_UPLOAD_MAX_BYTES:
        return JSONResponse(
            {"ok": False, "error": f"Uploaded file too large (max {KNOWLEDGE_SOURCE_UPLOAD_MAX_BYTES // (1024 * 1024)}MB)."},
            status_code=400,
        )

    source_id_raw = str(form.get("source_id", "")).strip() or f"src-{uuid.uuid4().hex[:10]}"
    source_id = safe_name(source_id_raw) or f"src-{uuid.uuid4().hex[:10]}"
    stem = safe_name(Path(filename).stem) or "knowledge_source"
    suffix = Path(filename).suffix[:20]
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stored_name = f"{timestamp}_{stem}{suffix}"
    target_dir = KNOWLEDGE_SOURCE_UPLOAD_ROOT / source_id
    target_dir.mkdir(parents=True, exist_ok=True)
    target_path = target_dir / stored_name
    target_path.write_bytes(blob)

    name = str(form.get("name", "")).strip() or Path(filename).stem or source_id
    location = str(form.get("location", "")).strip() or str(target_path.relative_to(ROOT))
    tags_raw = str(form.get("tags", "")).strip()
    tags = [part.strip() for part in tags_raw.split(",") if part.strip()] if tags_raw else []
    payload = {
        "source_id": source_id,
        "name": name,
        "type": str(form.get("type", "file")).strip().lower() or "file",
        "scope": str(form.get("scope", "project")).strip().lower() or "project",
        "data_classification": str(form.get("data_classification", "internal")).strip().lower() or "internal",
        "description": str(form.get("description", "")).strip() or f"Uploaded file: {filename}",
        "location": location,
        "refresh_policy": str(form.get("refresh_policy", "manual")).strip() or "manual",
        "retention_policy": str(form.get("retention_policy", "persist")).strip() or "persist",
        "status": str(form.get("status", "active")).strip().lower() or "active",
        "tags": tags,
    }
    try:
        settings = SETTINGS_STORE.upsert_knowledge_source(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse(
        {
            "ok": True,
            "settings": settings,
            "sources": hub.get("sources", []),
            "uploaded": {
                "source_id": source_id,
                "original_file_name": filename,
                "stored_path": str(target_path.relative_to(ROOT)),
                "bytes": len(blob),
            },
        }
    )


async def api_remove_knowledge_source(request):
    payload = _get_json(await request.body())
    source_id = str(payload.get("source_id", "")).strip()
    try:
        settings = SETTINGS_STORE.remove_knowledge_source(source_id, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "sources": hub.get("sources", [])})


async def api_create_evidence_bundle(request):
    try:
        form = await request.form()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Invalid multipart form data: {exc}"}, status_code=400)

    uploads = form.getlist("files")
    if not uploads:
        single = form.get("file")
        if single is not None:
            uploads = [single]
    if not uploads:
        return JSONResponse({"ok": False, "error": "At least one uploaded analysis file is required."}, status_code=400)

    files_payload: list[dict[str, Any]] = []
    for upload in uploads:
        try:
            blob = await upload.read()
        except Exception as exc:
            return JSONResponse({"ok": False, "error": f"Failed to read uploaded file: {exc}"}, status_code=400)
        files_payload.append(
            {
                "filename": str(getattr(upload, "filename", "") or "").strip() or "evidence.bin",
                "content_type": str(getattr(upload, "content_type", "") or "").strip() or "application/octet-stream",
                "content": blob,
            }
        )
    try:
        evidence = create_evidence_bundle(files_payload)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Evidence import failed: {exc}"}, status_code=500)
    return JSONResponse({"ok": True, **evidence})


async def api_get_evidence_bundle(request):
    bundle_id = request.path_params.get("bundle_id", "")
    payload = load_evidence_bundle(bundle_id)
    if not payload:
        return JSONResponse({"ok": False, "error": "evidence bundle not found"}, status_code=404)
    return JSONResponse({"ok": True, "evidence_bundle_v1": payload})


async def api_upsert_knowledge_set(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.upsert_knowledge_set(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "sets": hub.get("sets", [])})


async def api_remove_knowledge_set(request):
    payload = _get_json(await request.body())
    set_id = str(payload.get("set_id", "")).strip()
    try:
        settings = SETTINGS_STORE.remove_knowledge_set(set_id, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "sets": hub.get("sets", [])})


async def api_upsert_agent_brain(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.upsert_agent_brain(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "agent_brains": hub.get("agent_brains", [])})


async def api_upsert_project_binding(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.upsert_project_binding(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "project_bindings": hub.get("project_bindings", [])})


async def api_upsert_specialist(request):
    payload = _get_json(await request.body())
    try:
        settings = SETTINGS_STORE.upsert_specialist(payload, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "specialists": hub.get("specialists", [])})


async def api_remove_specialist(request):
    payload = _get_json(await request.body())
    specialist_id = str(payload.get("specialist_id", "")).strip()
    try:
        settings = SETTINGS_STORE.remove_specialist(specialist_id, actor=_request_actor(request))
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    hub = settings.get("knowledge_hub", {})
    return JSONResponse({"ok": True, "settings": settings, "specialists": hub.get("specialists", [])})


def _brain_eval_workspace_project(payload: dict[str, Any]) -> tuple[str, str]:
    brain_ctx = payload.get("brain_context", {})
    brain_ctx = brain_ctx if isinstance(brain_ctx, dict) else {}
    workspace = _clean_text(payload.get("workspace")) or _clean_text(brain_ctx.get("workspace")) or "default-workspace"
    project = _clean_text(payload.get("project")) or _clean_text(brain_ctx.get("project")) or "default-project"
    return workspace, project


def _brain_eval_stage_agent_ids(payload: dict[str, Any]) -> dict[str, str]:
    stage_map_raw = payload.get("stage_agent_ids", {})
    stage_map = stage_map_raw if isinstance(stage_map_raw, dict) else {}
    out: dict[str, str] = {}
    for key, value in stage_map.items():
        k = _clean_text(key)
        v = _clean_text(value)
        if not k or not v:
            continue
        out[k] = v
    agent_key = _clean_text(payload.get("agent_key"))
    stage_hint = int(payload.get("stage", 0) or 0)
    if agent_key and stage_hint > 0:
        out.setdefault(str(stage_hint), agent_key)
    return out


def _brain_eval_domain_pack(payload: dict[str, Any], query: str, objectives: str) -> tuple[str, dict[str, Any]]:
    custom = payload.get("domain_pack", {})
    if isinstance(custom, dict) and custom:
        explicit_id = _clean_text(custom.get("id")) or _clean_text(payload.get("domain_pack_id")) or "custom"
        return explicit_id, custom
    explicit_id = _clean_text(payload.get("domain_pack_id"))
    inferred = infer_domain_pack_id(" ".join([objectives, query]).strip(), explicit_pack_id=explicit_id)
    return inferred, get_domain_pack(inferred)


def _agent_policy_for_key(knowledge_context: dict[str, Any], agent_key: str) -> dict[str, Any]:
    key = str(agent_key or "").strip().lower()
    rows = _as_list_safe(_as_dict_safe(knowledge_context).get("agent_policies"))
    for row in rows:
        item = _as_dict_safe(row)
        if str(item.get("agent_key", "")).strip().lower() == key:
            return item
    return {}


async def api_find_relevant_context(request):
    payload = _get_json(await request.body())
    query = str(payload.get("query", "")).strip()
    objectives = str(payload.get("objectives", "")).strip()
    if not query and not objectives:
        return JSONResponse({"ok": False, "error": "query or objectives is required"}, status_code=400)
    query_text = query or objectives
    agent_key = _clean_text(payload.get("agent_key"))

    workspace, project = _brain_eval_workspace_project(payload)
    stage_agent_ids = _brain_eval_stage_agent_ids(payload)
    knowledge_context = SETTINGS_STORE.resolve_knowledge_run_context(
        workspace=workspace,
        project=project,
        stage_agent_ids=stage_agent_ids,
    )
    agent_policy = _agent_policy_for_key(knowledge_context, agent_key)
    top_k = max(
        1,
        min(
            50,
            int(payload.get("top_k", 0) or agent_policy.get("top_k", 8) or 8),
        ),
    )
    citation_required = bool(payload.get("citation_required", agent_policy.get("citation_required", True)))

    domain_pack_id, domain_pack = _brain_eval_domain_pack(payload, query_text, objectives)
    normalized = normalize_requirement(query_text)
    capability_map = map_to_capabilities(domain_pack, normalized)
    primary_caps = capability_map.get("primary_capabilities", [])
    capability_ids = [
        str(row.get("id", "")).strip()
        for row in primary_caps if isinstance(primary_caps, list) and isinstance(row, dict)
        if str(row.get("id", "")).strip()
    ]
    jurisdiction = str(payload.get("jurisdiction", "")).strip() or infer_jurisdiction(query_text)
    data_classes_raw = payload.get("data_classification", [])
    data_classes = (
        [str(x).strip() for x in data_classes_raw if str(x).strip()]
        if isinstance(data_classes_raw, list)
        else infer_data_classification(query_text)
    )

    vector_hits = KNOWLEDGE_GATEWAY.query_vector_context(
        query=query_text,
        domain_pack=domain_pack,
        top_k=top_k,
    )
    graph_edges = KNOWLEDGE_GATEWAY.query_capability_dependencies(
        domain_pack=domain_pack,
        capability_ids=capability_ids,
    )
    compliance = KNOWLEDGE_GATEWAY.query_regulatory_constraints(
        domain_pack=domain_pack,
        capability_ids=capability_ids,
        jurisdiction=jurisdiction,
        data_classes=data_classes,
    )
    gold_patterns = retrieve_gold_patterns(domain_pack, capability_ids)

    vector_index = {
        str(row.get("id", "")).strip().lower(): row
        for row in vector_hits
        if isinstance(row, dict) and str(row.get("id", "")).strip()
    }
    constraint_citations: list[dict[str, Any]] = []
    missing_constraint_citations: list[str] = []
    for row in compliance:
        item = _as_dict_safe(row)
        cid = str(item.get("id", "")).strip()
        if not cid:
            continue
        hits: list[dict[str, Any]] = []
        maybe_hit = vector_index.get(cid.lower())
        if isinstance(maybe_hit, dict):
            hits.append(
                {
                    "citation_id": f"vector:{_clean_text(maybe_hit.get('id'))}",
                    "title": str(maybe_hit.get("title", "")).strip(),
                    "source_class": str(maybe_hit.get("source_class", "domain_pack")).strip() or "domain_pack",
                    "score": float(maybe_hit.get("score", 0.0) or 0.0),
                }
            )
        if not hits:
            # Domain-pack regulations count as deterministic citations when vector recall misses.
            hits.append(
                {
                    "citation_id": f"domain_pack:{cid}",
                    "title": str(item.get("name", "")).strip() or cid,
                    "source_class": "domain_pack",
                    "score": 1.0,
                }
            )
        if not hits:
            missing_constraint_citations.append(cid)
        constraint_citations.append(
            {
                "constraint_id": cid,
                "constraint_name": str(item.get("name", "")).strip(),
                "citations": hits,
            }
        )

    assumption_blocker = bool(citation_required and missing_constraint_citations)
    guardrail_status = "pass" if not assumption_blocker else "fail"
    source_versions = _as_list_safe(_as_dict_safe(knowledge_context.get("integrity")).get("source_version_ids"))
    source_catalog = [
        {
            "source_id": str(row.get("source_id", "")).strip(),
            "version_id": str(row.get("version_id", "")).strip(),
            "name": str(row.get("name", "")).strip(),
            "classification": str(row.get("data_classification", "")).strip(),
            "scope": str(row.get("scope", "")).strip(),
        }
        for row in _as_list_safe(knowledge_context.get("sources"))
        if isinstance(row, dict)
    ]

    return JSONResponse(
        {
            "ok": True,
            "query": query_text,
            "agent_key": agent_key,
            "domain_pack_id": domain_pack_id,
            "retrieval": {
                "top_k": top_k,
                "capability_mapping": capability_map,
                "graph_edges": graph_edges,
                "vector_hits": vector_hits,
                "compliance_constraints": compliance,
                "gold_patterns": gold_patterns,
                "source_catalog": source_catalog,
            },
            "guardrails": {
                "citation_required": citation_required,
                "constraint_citations": constraint_citations,
                "missing_constraint_citations": missing_constraint_citations,
                "assumption_blocker": assumption_blocker,
                "status": guardrail_status,
                "qa_required": bool(source_versions),
            },
            "context_snapshot": {
                "workspace": workspace,
                "project": project,
                "knowledge_snapshot_id": _clean_text(knowledge_context.get("snapshot_id")),
                "knowledge_snapshot_hash": _clean_text(_as_dict_safe(knowledge_context.get("integrity")).get("snapshot_hash")),
                "source_version_ids": source_versions,
            },
        }
    )


async def api_suggest_agent(request):
    payload = _get_json(await request.body())
    task = str(payload.get("task", "")).strip()
    objectives = str(payload.get("objectives", "")).strip()
    legacy_code = str(payload.get("legacy_code", "")).strip()
    if not task and not objectives and not legacy_code:
        return JSONResponse({"ok": False, "error": "task, objectives, or legacy_code is required"}, status_code=400)
    use_case = str(payload.get("use_case", "business_objectives")).strip().lower() or "business_objectives"
    stage_agent_ids = _brain_eval_stage_agent_ids(payload)
    integration_context = payload.get("integration_context", {})
    integration_context = integration_context if isinstance(integration_context, dict) else {}
    routing = _route_specialists_for_run(
        objectives=" ".join([objectives, task]).strip(),
        use_case=use_case,
        integration_context=integration_context,
        legacy_code=legacy_code,
        stage_agent_ids=stage_agent_ids,
    )
    selected = _as_list_safe(_as_dict_safe(routing).get("selected"))
    dispatchable = [row for row in selected if isinstance(row, dict) and bool(row.get("dispatchable"))]
    target_weights: dict[str, int] = {}
    for row in dispatchable:
        target = str(row.get("route_target_agent_key", "")).strip()
        if not target:
            continue
        target_weights[target] = int(target_weights.get(target, 0)) + int(row.get("score", 0) or 0)

    agents_payload = TEAM_STORE.list_agents()
    all_agents = [row for row in _as_list_safe(agents_payload.get("all")) if isinstance(row, dict)]
    agent_index = {
        str(row.get("id", "")).strip(): row
        for row in all_agents
        if str(row.get("id", "")).strip()
    }

    ranked_targets = sorted(target_weights.items(), key=lambda item: (-item[1], item[0]))
    primary_target = ranked_targets[0][0] if ranked_targets else _clean_text(payload.get("agent_key"))
    primary_agent = _as_dict_safe(agent_index.get(primary_target))

    alternatives: list[dict[str, Any]] = []
    for target, score in ranked_targets[1:6]:
        row = _as_dict_safe(agent_index.get(target))
        if not row:
            continue
        alternatives.append(
            {
                "agent_key": target,
                "display_name": str(row.get("display_name", row.get("role", target))).strip(),
                "stage": int(row.get("stage", 0) or 0),
                "score": score,
            }
        )

    return JSONResponse(
        {
            "ok": True,
            "task": task or objectives,
            "suggestion": {
                "primary_agent": {
                    "agent_key": str(primary_agent.get("id", primary_target)).strip(),
                    "display_name": str(primary_agent.get("display_name", primary_agent.get("role", primary_target))).strip(),
                    "stage": int(primary_agent.get("stage", 0) or 0),
                }
                if primary_target
                else {},
                "alternatives": alternatives,
                "specialist_matches": selected[:12],
            },
            "routing": routing,
        }
    )


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

    base_url, headers, authenticated = _github_request_context()

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
        return JSONResponse({"ok": False, "error": _github_fetch_error_message(exc, authenticated=authenticated)}, status_code=400)

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


async def _api_discover_analyst_brief_impl(request, payload: dict[str, Any]) -> JSONResponse:
    integration_ctx = _extract_integration_context(payload)
    brownfield = integration_ctx.get("brownfield", {}) if isinstance(integration_ctx.get("brownfield", {}), dict) else {}
    greenfield = integration_ctx.get("greenfield", {}) if isinstance(integration_ctx.get("greenfield", {}), dict) else {}
    sample_mode = bool(integration_ctx.get("sample_dataset_enabled", False) or payload.get("sample_dataset_enabled", False))

    objectives = str(payload.get("objectives", "")).strip()
    use_case = str(payload.get("use_case", "business_objectives")).strip().lower() or "business_objectives"
    legacy_code = str(payload.get("legacy_code", "")).strip()
    modernization_language = str(payload.get("modernization_language") or payload.get("target_language") or "").strip()
    target_platform = str(payload.get("target_platform", "")).strip()
    deployment_target = str(payload.get("deployment_target", "local")).strip().lower() or "local"
    database_source = str(payload.get("database_source", "")).strip()
    database_target = str(payload.get("database_target", "")).strip()
    database_schema = str(payload.get("database_schema", "")).strip()
    repo_provider = str(payload.get("repo_provider") or brownfield.get("repo_provider") or "").strip().lower()
    repo_url = str(payload.get("repo_url") or brownfield.get("repo_url") or "").strip()
    include_paths = _normalize_lines(integration_ctx.get("scan_scope", {}).get("include_paths", []))
    exclude_paths = _normalize_lines(integration_ctx.get("scan_scope", {}).get("exclude_paths", []))
    exports_cfg = integration_ctx.get("exports", {}) if isinstance(integration_ctx.get("exports", {}), dict) else {}
    exports_gh = exports_cfg.get("github", {}) if isinstance(exports_cfg.get("github", {}), dict) else {}
    target_repo_url = str(
        payload.get("target_repo_url")
        or integration_ctx.get("export_repo_url")
        or exports_gh.get("repo_url")
        or ""
    ).strip()

    def _enrich_with_analyst_aas(response_payload: dict[str, Any], *, fallback_requirement: str) -> dict[str, Any]:
        req = str(objectives or "").strip() or str(fallback_requirement or "").strip()
        if not req:
            return response_payload
        repo_hint = str(repo_url or "discover").strip().lower()
        repo_hint = re.sub(r"[^a-z0-9]+", "-", repo_hint).strip("-")[:80] or "discover"
        aas_payload: dict[str, Any] = {
            "requirement": req,
            "business_objective": req,
            "use_case": use_case,
            "thread_id": str(payload.get("thread_id", "")).strip() or f"discover-{repo_hint}",
            "workspace_id": str(payload.get("workspace_id", "default-workspace")),
            "client_id": str(payload.get("client_id", "default-client")),
            "project_id": str(payload.get("project_id", repo_hint or "discover-project")),
            "integration_context": integration_ctx,
            "repo_provider": repo_provider,
            "repo_url": repo_url,
            "legacy_code": legacy_code[:20000] if legacy_code else "",
            "database_source": database_source,
            "database_target": database_target,
            "database_schema": database_schema[:20000] if database_schema else "",
        }
        domain_pack_id = str(integration_ctx.get("domain_pack_id", "")).strip()
        if domain_pack_id:
            aas_payload["domain_pack_id"] = domain_pack_id
        custom_pack = integration_ctx.get("custom_domain_pack")
        if isinstance(custom_pack, dict) and custom_pack:
            aas_payload["domain_pack"] = custom_pack
        jurisdiction = str(integration_ctx.get("jurisdiction", "")).strip()
        if jurisdiction:
            aas_payload["jurisdiction"] = jurisdiction
        data_classes = integration_ctx.get("data_classification", [])
        if isinstance(data_classes, list) and data_classes:
            aas_payload["data_classification"] = data_classes

        try:
            aas_result = ANALYST_AAS.analyze(aas_payload, actor=_request_actor(request))
        except Exception as exc:
            response_payload["aas"] = {"ok": False, "error": f"analyst AAS enrichment failed: {exc}"}
            return response_payload

        response_payload["aas"] = aas_result
        response_payload["thread_id"] = str(aas_result.get("thread_id", "")).strip()
        response_payload["assistant_summary"] = str(aas_result.get("assistant_summary", "")).strip()
        if isinstance(aas_result.get("requirements_pack", {}), dict):
            response_payload["requirements_pack"] = aas_result.get("requirements_pack", {})
            analysis_summary = (
                response_payload.get("analyst_brief", {}).get("summary", {})
                if isinstance(response_payload.get("analyst_brief", {}), dict)
                else {}
            )
            if not isinstance(analysis_summary, dict):
                analysis_summary = {}
            vb6_from_analysis = (
                analysis_summary.get("vb6_analysis", {})
                if isinstance(analysis_summary.get("vb6_analysis", {}), dict)
                else {}
            )
            report_seed = {
                "project_name": "Discover Analysis",
                "analysis_walkthrough": {
                    "business_objective_summary": str(response_payload.get("assistant_summary", "")).strip()
                },
                "requirements_pack": aas_result.get("requirements_pack", {}),
                "quality_gates": aas_result.get("quality_gates", []),
                "open_questions": aas_result.get("requirements_pack", {}).get("open_questions", []),
                # Include source-bundle static analysis so Discover Result tabs
                # (Code Quality, Dead Code, Dependency Matrix, Trends, Data)
                # can render immediately without requiring a full Build run.
                "vb6_analysis": vb6_from_analysis,
                "legacy_code_inventory": {
                    "summary": str(analysis_summary.get("overview", "")).strip(),
                    "vb6_analysis": vb6_from_analysis,
                },
            }
            response_payload["raw_artifacts"] = build_raw_artifact_set_v1(report_seed)
            response_payload["analyst_report_v2"] = build_analyst_report_v2(report_seed)
        if isinstance(aas_result.get("quality_gates", []), list):
            response_payload["quality_gates"] = aas_result.get("quality_gates", [])
        return response_payload

    def _attach_landscape_artifacts(
        response_payload: dict[str, Any],
        *,
        repo_ref: str,
        branch_ref: str,
        commit_ref: str,
        tree_entries: list[dict[str, Any]],
        fetched_files: dict[str, str],
    ) -> dict[str, Any]:
        try:
            artifacts = build_landscape_artifacts(
                repo=repo_ref,
                branch=branch_ref or "main",
                commit_sha=commit_ref,
                entries=[row for row in tree_entries if isinstance(row, dict)],
                file_contents=fetched_files,
                include_paths=include_paths,
                exclude_paths=exclude_paths,
            )
        except Exception as exc:
            response_payload.setdefault("landscape_error", f"landscape generation failed: {exc}")
            return response_payload
        raw = response_payload.get("raw_artifacts", {})
        if not isinstance(raw, dict):
            raw = {}
        raw.update(artifacts)
        response_payload["raw_artifacts"] = raw
        report = response_payload.get("analyst_report_v2", {})
        if isinstance(report, dict):
            report_raw = report.get("raw_artifacts", {})
            if not isinstance(report_raw, dict):
                report_raw = {}
            report_raw.update(artifacts)
            report["raw_artifacts"] = report_raw
            response_payload["analyst_report_v2"] = report
        return response_payload

    def _greenfield_summary_from_landscape(artifacts: dict[str, Any]) -> dict[str, Any]:
        landscape = artifacts.get("repo_landscape_v1", {}) if isinstance(artifacts.get("repo_landscape_v1", {}), dict) else {}
        components = artifacts.get("component_inventory_v1", {}) if isinstance(artifacts.get("component_inventory_v1", {}), dict) else {}
        tracks = artifacts.get("modernization_track_plan_v1", {}) if isinstance(artifacts.get("modernization_track_plan_v1", {}), dict) else {}
        solution = landscape.get("solution_summary", {}) if isinstance(landscape.get("solution_summary", {}), dict) else {}
        component_rows = components.get("components", []) if isinstance(components.get("components", []), list) else []
        track_rows = tracks.get("tracks", []) if isinstance(tracks.get("tracks", []), list) else []
        risk_rows = landscape.get("high_risk_signals", []) if isinstance(landscape.get("high_risk_signals", []), list) else []
        target_language_label = str(solution.get("target_language", "")).strip() or "the chosen target stack"
        overview = (
            str(objectives).strip()
            or f"Greenfield solution landscape for {target_language_label} with {len(component_rows)} planned component(s) and {len(track_rows)} suggested delivery track(s)."
        )
        return {
            "overview": overview,
            "likely_capabilities": [str(row.get("title", "")).strip() for row in track_rows if str(row.get("title", "")).strip()][:8],
            "input_output_contracts": [str(row.get("suggested_target", "")).strip() for row in track_rows if str(row.get("suggested_target", "")).strip()][:6],
            "key_components": [str(row.get("name", "")).strip() for row in component_rows if str(row.get("name", "")).strip()][:8],
            "interfaces": [str(row.get("title", "")).strip() for row in risk_rows if str(row.get("title", "")).strip()][:6],
            "data_and_state": [str(row.get("datastore", "")).strip() for row in landscape.get("datastore_signals", []) if isinstance(row, dict) and str(row.get("datastore", "")).strip()][:6],
            "domain_functions": [str(row.get("lane", "")).strip() for row in track_rows if str(row.get("lane", "")).strip()][:8],
            "data_entities": [str(row.get("datastore", "")).strip() for row in landscape.get("datastore_signals", []) if isinstance(row, dict) and str(row.get("datastore", "")).strip()][:8],
            "unknowns": [str(item).strip() for item in tracks.get("open_questions", []) if str(item).strip()][:8],
            "evidence_files": [str(solution.get("repo_target", "")).strip()] if str(solution.get("repo_target", "")).strip() else [],
            "stats": {
                "sampled_files": 0,
                "sampled_tree_entries": 0,
                "route_hints": len(track_rows),
            },
        }

    project_state_detected = str(
        integration_ctx.get("project_state_detected", "")
        or payload.get("project_state_detected", "")
        or ("greenfield" if greenfield else "")
    ).strip().lower()
    evidence_ctx = integration_ctx.get("evidence", {}) if isinstance(integration_ctx.get("evidence", {}), dict) else {}
    evidence_source_mode = str(evidence_ctx.get("source_mode", "")).strip().lower()
    evidence_bundle_id = str(evidence_ctx.get("bundle_id", "")).strip()

    if evidence_source_mode in {"evidence", "hybrid"} and evidence_bundle_id:
        response_payload = _imported_analysis_response_payload(objectives, integration_ctx)
        if not response_payload:
            return JSONResponse({"ok": False, "error": "Evidence bundle could not be loaded. Re-upload the analysis outputs."}, status_code=400)
        return JSONResponse(response_payload)

    if project_state_detected == "greenfield" and not repo_url and not legacy_code:
        artifacts = build_greenfield_landscape_artifacts(
            objectives=objectives,
            use_case=use_case,
            integration_context=integration_ctx,
            target_language=modernization_language,
            target_platform=target_platform,
            database_source=database_source,
            database_target=database_target,
        )
        analysis = _greenfield_summary_from_landscape(artifacts)
        response_payload = {
            "ok": True,
            "source": "greenfield_landscape",
            "repo": {
                "owner": "",
                "repository": "",
                "default_branch": "",
                "url": str(greenfield.get("repo_target", "")).strip(),
            },
            "analyst_brief": {
                "title": "Analyst functionality understanding",
                "summary": analysis,
            },
            "raw_artifacts": artifacts,
        }
        response_payload = _enrich_with_analyst_aas(
            response_payload,
            fallback_requirement=overview if (overview := str(analysis.get("overview", "")).strip()) else "Analyze greenfield scope and generate a solution landscape.",
        )
        return JSONResponse(response_payload)

    # Legacy modernization path: analyze provided code directly.
    if legacy_code and use_case == "code_modernization":
        file_entries = [{"path": "inline/legacy_code.txt", "type": "file", "depth": 1}]
        file_contents = {"inline/legacy_code.txt": legacy_code[:25000]}
        if database_schema:
            file_entries.append({"path": "inline/database_schema.sql", "type": "file", "depth": 1})
            file_contents["inline/database_schema.sql"] = database_schema[:50000]
        analysis = _analyze_source_bundle(
            objectives=objectives,
            repo_label="provided legacy code",
            file_entries=file_entries,
            file_contents=file_contents,
            target_language=modernization_language,
            target_platform=target_platform,
            deployment_target=deployment_target,
            source_repo_url=repo_url,
            target_repo_url=target_repo_url,
        )
        response_payload = {
            "ok": True,
            "source": "inline_legacy_code",
            "analyst_brief": {
                "title": "Analyst functionality understanding",
                "summary": analysis,
            },
        }
        response_payload = _enrich_with_analyst_aas(
            response_payload,
            fallback_requirement=str(analysis.get("overview", "")).strip() or "Analyze provided legacy code.",
        )
        return JSONResponse(response_payload)

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
        if database_schema:
            db_label = f"{database_source or 'source-db'} -> {database_target or 'target-db'}"
            file_entries = [{"path": "inline/database_schema.sql", "type": "file", "depth": 1}]
            file_contents = {"inline/database_schema.sql": database_schema[:50000]}
            analysis = _analyze_source_bundle(
                objectives=objectives,
                repo_label=f"provided database schema ({db_label})",
                file_entries=file_entries,
                file_contents=file_contents,
                target_language=modernization_language,
                target_platform=target_platform,
                deployment_target=deployment_target,
                source_repo_url=repo_url,
                target_repo_url=target_repo_url,
            )
            response_payload = {
                "ok": True,
                "source": "inline_database_schema",
                "analyst_brief": {
                    "title": "Analyst functionality understanding",
                    "summary": analysis,
                },
            }
            response_payload = _enrich_with_analyst_aas(
                response_payload,
                fallback_requirement=(
                    str(analysis.get("overview", "")).strip()
                    or "Analyze provided legacy database schema and migration objectives."
                ),
            )
            return JSONResponse(response_payload)
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
        if database_schema:
            sample_contents["inline/database_schema.sql"] = database_schema[:50000]
        analysis = _analyze_source_bundle(
            objectives=objectives,
            repo_label=f"{owner}/{repository}",
            file_entries=(
                [item for item in entries if isinstance(item, dict)]
                + ([{"path": "inline/database_schema.sql", "type": "file", "depth": 1}] if database_schema else [])
            ),
            file_contents=sample_contents,
            target_language=modernization_language,
            target_platform=target_platform,
            deployment_target=deployment_target,
            source_repo_url=repo_url,
            target_repo_url=target_repo_url,
        )
        response_payload = {
            "ok": True,
            "source": "sample_dataset",
            "repo": {"owner": owner, "repository": repository, "default_branch": sample_tree.get("repo", {}).get("default_branch", "main")},
            "analyst_brief": {
                "title": "Analyst functionality understanding",
                "summary": analysis,
            },
        }
        response_payload = _enrich_with_analyst_aas(
            response_payload,
            fallback_requirement=str(analysis.get("overview", "")).strip() or f"Analyze repository {owner}/{repository}.",
        )
        response_payload = _attach_landscape_artifacts(
            response_payload,
            repo_ref=repo_url or f"https://github.com/{owner}/{repository}",
            branch_ref=str(sample_tree.get("repo", {}).get("default_branch", "main")),
            commit_ref="sample",
            tree_entries=[item for item in entries if isinstance(item, dict)],
            fetched_files=sample_contents,
        )
        return JSONResponse(response_payload)

    base_url, headers, authenticated = _github_request_context()

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
        return JSONResponse({"ok": False, "error": _github_fetch_error_message(exc, authenticated=authenticated)}, status_code=400)

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

    if database_schema:
        selected_entries.append({"path": "inline/database_schema.sql", "type": "file", "depth": 1})
        file_contents["inline/database_schema.sql"] = database_schema[:50000]

    analysis = _analyze_source_bundle(
        objectives=objectives,
        repo_label=f"{owner}/{repository}",
        file_entries=selected_entries,
        file_contents=file_contents,
        target_language=modernization_language,
        target_platform=target_platform,
        deployment_target=deployment_target,
        source_repo_url=repo_url,
        target_repo_url=target_repo_url,
    )
    if fetch_errors:
        analysis.setdefault("unknowns", []).append(
            f"Some files could not be read from GitHub API: {' | '.join(fetch_errors)}"
        )

    response_payload = {
        "ok": True,
        "source": "github_api",
        "repo": {"owner": owner, "repository": repository, "default_branch": branch, "url": repo_url},
        "analyst_brief": {
            "title": "Analyst functionality understanding",
            "summary": analysis,
        },
    }
    response_payload = _enrich_with_analyst_aas(
        response_payload,
        fallback_requirement=str(analysis.get("overview", "")).strip() or f"Analyze repository {owner}/{repository}.",
    )
    response_payload = _attach_landscape_artifacts(
        response_payload,
        repo_ref=repo_url or f"https://github.com/{owner}/{repository}",
        branch_ref=branch,
        commit_ref=str(tree_payload.get("sha") or repo_meta.get("default_branch") or ""),
        tree_entries=[item for item in raw_entries if isinstance(item, dict)],
        fetched_files=file_contents,
    )
    return JSONResponse(response_payload)


async def api_discover_analyst_brief(request):
    payload = _get_json(await request.body())
    cache_key = _discover_analyst_brief_cache_key(payload)

    async def _compute_marshaled() -> dict[str, Any]:
        response = await _api_discover_analyst_brief_impl(request, payload)
        try:
            raw_body = bytes(response.body or b"")
            body_obj = _get_json(raw_body)
            if not isinstance(body_obj, dict):
                body_obj = {"ok": False, "error": "discover analyst brief returned a non-object payload"}
        except Exception as exc:
            body_obj = {"ok": False, "error": f"failed to decode discover analyst brief response: {exc}"}
        return {
            "status_code": int(getattr(response, "status_code", 200) or 200),
            "body": body_obj,
        }

    marshaled = await _discover_analyst_brief_singleflight(cache_key, _compute_marshaled)
    status_code = int(marshaled.get("status_code", 200) or 200)
    body = marshaled.get("body", {})
    if not isinstance(body, dict):
        body = {"ok": False, "error": "discover analyst brief cache payload was invalid"}
        status_code = 500
    return JSONResponse(body, status_code=status_code)


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
    requirements_pack_profile = str(payload.get("requirements_pack_profile", "")).strip()
    requirements_pack_template = payload.get("requirements_pack_template", {})
    if not isinstance(requirements_pack_template, dict):
        requirements_pack_template = {}
    if not base_agent_id:
        return JSONResponse({"ok": False, "error": "base_agent_id is required"}, status_code=400)
    try:
        cloned = TEAM_STORE.clone_agent(
            base_agent_id=base_agent_id,
            display_name=display_name,
            persona=persona,
            requirements_pack_profile=requirements_pack_profile,
            requirements_pack_template=requirements_pack_template,
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
    try:
        team = TEAM_STORE.save_team(
            name=name,
            description=description,
            stage_agent_ids=stage_agent_ids,
            team_id=team_id,
        )
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    personas, resolved_team = TEAM_STORE.resolve_personas(team_id=team.get("id", ""))
    return JSONResponse({"ok": True, "team": resolved_team, "agent_personas": personas})


async def api_duplicate_team(request):
    payload = _get_json(await request.body())
    source_team_id = str(payload.get("team_id", "")).strip()
    name = str(payload.get("name", "")).strip()
    if not source_team_id:
        return JSONResponse({"ok": False, "error": "team_id is required"}, status_code=400)
    try:
        team = TEAM_STORE.duplicate_team(source_team_id=source_team_id, name=name)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    personas, resolved_team = TEAM_STORE.resolve_personas(team_id=team.get("id", ""))
    return JSONResponse({"ok": True, "team": resolved_team, "agent_personas": personas})


async def api_delete_team(request):
    payload = _get_json(await request.body())
    team_id = str(payload.get("team_id", "")).strip()
    if not team_id:
        return JSONResponse({"ok": False, "error": "team_id is required"}, status_code=400)
    try:
        deleted = TEAM_STORE.delete_team(team_id=team_id)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)
    return JSONResponse({"ok": True, "deleted_team": deleted, "teams": TEAM_STORE.list_teams()})


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


async def api_discover_access_inspect(request):
    try:
        form = await request.form()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Invalid multipart form data: {exc}"}, status_code=400)

    upload = form.get("file")
    if upload is None:
        return JSONResponse({"ok": False, "error": "file is required"}, status_code=400)

    filename = str(getattr(upload, "filename", "") or "").strip()
    suffix = Path(filename).suffix.lower()
    if suffix not in {".mdb", ".accdb"}:
        return JSONResponse({"ok": False, "error": "Only .mdb or .accdb files are supported by this endpoint."}, status_code=400)

    try:
        blob = await upload.read()
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"Failed to read uploaded file: {exc}"}, status_code=400)

    if not blob:
        return JSONResponse({"ok": False, "error": "Uploaded file is empty."}, status_code=400)
    if len(blob) > 60 * 1024 * 1024:
        return JSONResponse({"ok": False, "error": "Uploaded Access file is too large (max 60MB)."}, status_code=400)

    target_engine = str(form.get("target_engine", "postgres")).strip()
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(delete=False, suffix=suffix) as tmp:
            temp_path = Path(tmp.name)
            tmp.write(blob)
        inspected = _inspect_access_database(temp_path, target_engine=target_engine)
    finally:
        if temp_path is not None:
            try:
                temp_path.unlink(missing_ok=True)
            except Exception:
                pass

    if not bool(inspected.get("ok")):
        return JSONResponse(inspected, status_code=400)

    return JSONResponse(
        {
            "ok": True,
            "source": "access_file",
            "file_name": filename,
            "database_schema": str(inspected.get("database_schema", "")),
            "analysis": {
                "parser": inspected.get("parser", "mdbtools"),
                "target_flavor": inspected.get("target_flavor", "postgres"),
                "table_count": int(inspected.get("table_count", 0) or 0),
                "tables": inspected.get("tables", []),
                "system_table_count": int(inspected.get("system_table_count", 0) or 0),
                "warnings": inspected.get("warnings", []),
            },
        }
    )


def _local_worker_fallback(run_id: str) -> None:
    # Local/dev fallback when Cloud Tasks isn't configured.
    try:
        MANAGER.launch_deferred_run(run_id)
    except Exception:
        pass


def _parse_iso_dt(value: Any) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    try:
        return datetime.fromisoformat(raw.replace("Z", "+00:00"))
    except Exception:
        return None


def _dispatch_worker_direct(run_id: str) -> None:
    rid = str(run_id or "").strip()
    if not rid or not RUN_WORKER_URL:
        return
    headers = {"Content-Type": "application/json"}
    if RUN_WORKER_TOKEN:
        headers["X-Run-Worker-Token"] = RUN_WORKER_TOKEN
    body = json.dumps({"run_id": rid}).encode("utf-8")
    request = Request(RUN_WORKER_URL, data=body, headers=headers, method="POST")
    with urlopen(request, timeout=30):
        return


def _maybe_rescue_queued_run(run_id: str, persisted: dict[str, Any]) -> None:
    status = str(persisted.get("pipeline_status", "")).strip().lower()
    if status != "queued":
        return
    now = datetime.now(timezone.utc)
    saved_at = _parse_iso_dt(persisted.get("saved_at"))
    if not saved_at or (now - saved_at.astimezone(timezone.utc)).total_seconds() < RUN_QUEUED_RESCUE_SEC:
        return
    pipeline_state = persisted.get("pipeline_state", {}) if isinstance(persisted.get("pipeline_state", {}), dict) else {}
    rescue_at = _parse_iso_dt(pipeline_state.get("queue_dispatch_requested_at"))
    if rescue_at and (now - rescue_at.astimezone(timezone.utc)).total_seconds() < RUN_QUEUED_RESCUE_COOLDOWN_SEC:
        return
    if hasattr(RUN_STORE, "mark_queue_dispatch_attempt"):
        try:
            RUN_STORE.mark_queue_dispatch_attempt(run_id, "api_rescue")
        except Exception:
            pass
    threading.Thread(
        target=_dispatch_worker_direct,
        args=(run_id,),
        daemon=True,
        name=f"run-worker-rescue-{run_id}",
    ).start()


def _current_stage_from_status_map(stage_status: dict[int, str], pipeline_state: dict[str, Any] | None = None) -> int:
    active_states = {"running", "completed", "failed", "waiting_approval", "paused"}
    seen = [stage for stage, state in stage_status.items() if str(state).strip().lower() in active_states]
    if seen:
        return max(seen)
    current_stage = (
        int((pipeline_state or {}).get("current_stage", 0) or 0)
        if isinstance(pipeline_state, dict)
        else 0
    )
    return current_stage if current_stage > 0 else 0


def _enqueue_run_worker_task(run_id: str) -> tuple[bool, str]:
    rid = str(run_id or "").strip()
    if not rid:
        return False, "missing run_id"

    if tasks_v2 is None or not RUN_TASK_QUEUE_PATH:
        threading.Thread(
            target=_local_worker_fallback,
            args=(rid,),
            daemon=True,
            name=f"run-worker-fallback-{rid}",
        ).start()
        return True, ""

    target_url = RUN_WORKER_URL
    if not target_url:
        target_url = f"/internal/run-worker"
    if target_url.startswith("/"):
        # Relative URL cannot be used by Cloud Tasks without host, fallback locally.
        threading.Thread(
            target=_local_worker_fallback,
            args=(rid,),
            daemon=True,
            name=f"run-worker-fallback-{rid}",
        ).start()
        return True, ""

    body = {"run_id": rid}
    headers = {"Content-Type": "application/json"}
    if RUN_WORKER_TOKEN:
        headers["X-Run-Worker-Token"] = RUN_WORKER_TOKEN

    try:
        client = tasks_v2.CloudTasksClient()
        task = {
            "http_request": {
                "http_method": tasks_v2.HttpMethod.POST,
                "url": target_url,
                "headers": headers,
                "body": json.dumps(body).encode("utf-8"),
            }
        }
        client.create_task(parent=RUN_TASK_QUEUE_PATH, task=task)
        return True, ""
    except Exception as exc:
        # If queue infra has issues, fallback to in-process to avoid run loss.
        threading.Thread(
            target=_local_worker_fallback,
            args=(rid,),
            daemon=True,
            name=f"run-worker-fallback-{rid}",
        ).start()
        return True, f"cloud tasks unavailable, used local fallback: {exc}"


async def api_internal_run_worker(request):
    if RUN_WORKER_TOKEN:
        incoming = str(request.headers.get("x-run-worker-token", "")).strip()
        if incoming != RUN_WORKER_TOKEN:
            return JSONResponse({"ok": False, "error": "unauthorized worker invocation"}, status_code=401)
    payload = _get_json(await request.body())
    run_id = str(payload.get("run_id", "")).strip()
    result = MANAGER.launch_deferred_run(run_id)
    status = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status)


def _evidence_mode_preflight(payload: dict[str, Any], integration_context: dict[str, Any]) -> dict[str, Any]:
    scan_scope = integration_context.get("scan_scope", {}) if isinstance(integration_context.get("scan_scope", {}), dict) else {}
    evidence_ctx = integration_context.get("evidence", {}) if isinstance(integration_context.get("evidence", {}), dict) else {}
    source_mode = str(evidence_ctx.get("source_mode") or scan_scope.get("modernization_source_mode") or "").strip().lower()
    if source_mode not in {"evidence", "hybrid"}:
        return {"ok": True, "source_mode": source_mode or "manual"}

    bundle_id = str(evidence_ctx.get("bundle_id", "")).strip()
    output_target = str(evidence_ctx.get("output_target", "deliverable_pack_only")).strip().lower() or "deliverable_pack_only"
    accept_risk = bool(evidence_ctx.get("accept_low_coverage_risk", False))
    if not bundle_id:
        return {
            "ok": False,
            "code": "missing_evidence_bundle",
            "source_mode": source_mode,
            "error": "Evidence Mode requires an uploaded analysis bundle before scope lock.",
        }

    bundle_payload = load_evidence_bundle(bundle_id)
    if not bundle_payload:
        return {
            "ok": False,
            "code": "missing_evidence_bundle",
            "source_mode": source_mode,
            "error": "Uploaded evidence bundle could not be found. Re-upload the analysis bundle.",
        }

    coverage = evidence_ctx.get("coverage", {}) if isinstance(evidence_ctx.get("coverage", {}), dict) else {}
    if not coverage:
        coverage = bundle_payload.get("evidence_coverage_report_v1", {}) if isinstance(bundle_payload.get("evidence_coverage_report_v1", {}), dict) else {}
    dimensions = coverage.get("dimensions", {}) if isinstance(coverage.get("dimensions", {}), dict) else {}
    data_score = int(dimensions.get("data", 0) or 0)
    behavior_score = int(dimensions.get("behavior", 0) or 0)
    build_allowed = bool(coverage.get("build_allowed", False))
    blockers = coverage.get("blockers", []) if isinstance(coverage.get("blockers", []), list) else []
    if source_mode == "evidence" and output_target != "deliverable_pack_only" and not build_allowed and not accept_risk:
        return {
            "ok": False,
            "code": "evidence_coverage_blocked",
            "source_mode": source_mode,
            "output_target": output_target,
            "coverage": coverage,
            "error": (
                "Imported analysis coverage is not sufficient for build-oriented execution. "
                "Use 'Deliverable pack only' or explicitly accept evidence risk after review."
            ),
            "blockers": blockers,
            "thresholds": {"behavior": behavior_score, "data": data_score},
        }

    return {
        "ok": True,
        "source_mode": source_mode,
        "output_target": output_target,
        "coverage": coverage,
        "accept_low_coverage_risk": accept_risk,
        "blockers": blockers,
    }


async def api_run_preflight(request):
    payload = _get_json(await request.body())
    integration_context = payload.get("integration_context", {}) if isinstance(payload.get("integration_context", {}), dict) else {}
    evidence_check = _evidence_mode_preflight(payload, integration_context)
    if not bool(evidence_check.get("ok", True)):
        return JSONResponse(
            {
                "ok": False,
                "error": str(evidence_check.get("error", "Evidence preflight failed")),
                "preflight": {"ok": False, "code": str(evidence_check.get("code", "evidence_mode_blocked"))},
                "evidence_preflight": evidence_check,
            },
            status_code=400,
        )
    try:
        llm_preflight = _llm_preflight_from_payload(payload)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc), "preflight": {"ok": False, "code": "invalid_provider"}}, status_code=400)
    if not bool(llm_preflight.get("ok")):
        return JSONResponse(
            {
                "ok": False,
                "error": str(llm_preflight.get("error", "LLM preflight failed")),
                "preflight": llm_preflight,
            },
            status_code=400,
        )
    return JSONResponse({"ok": True, "preflight": llm_preflight, "evidence_preflight": evidence_check})


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
    evidence_ctx = integration_context.get("evidence", {}) if isinstance(integration_context.get("evidence", {}), dict) else {}
    team_id = str(payload.get("team_id", "")).strip()
    stage_agent_ids = payload.get("stage_agent_ids", {}) if isinstance(payload.get("stage_agent_ids", {}), dict) else {}
    if not objectives:
        return JSONResponse({"ok": False, "error": "objectives are required"}, status_code=400)
    if use_case not in {"business_objectives", "code_modernization", "database_conversion"}:
        return JSONResponse(
            {"ok": False, "error": "use_case must be business_objectives, code_modernization, or database_conversion"},
            status_code=400,
        )
    evidence_bundle_id = str(evidence_ctx.get("bundle_id", "")).strip()
    evidence_mode = modernization_source_mode in {"evidence", "hybrid"}
    if use_case == "code_modernization" and not legacy_code and modernization_source_mode == "repo_scan":
        repo_provider = str(brownfield.get("repo_provider", "")).strip().lower()
        repo_url = str(brownfield.get("repo_url", "")).strip()
        if repo_provider != "github" or not repo_url:
            return JSONResponse(
                {
                    "ok": False,
                    "error": "Repo scan mode requires a connected GitHub repository in Discover Connect.",
                },
                status_code=400,
            )
    if use_case == "code_modernization" and not legacy_code:
        if modernization_source_mode == "manual":
            return JSONResponse({"ok": False, "error": "legacy_code is required for code_modernization use case"}, status_code=400)
        if modernization_source_mode == "evidence" and not evidence_bundle_id:
            return JSONResponse({"ok": False, "error": "Evidence import mode requires an uploaded evidence bundle in Discover Connect."}, status_code=400)
        if modernization_source_mode == "hybrid":
            repo_provider = str(brownfield.get("repo_provider", "")).strip().lower()
            repo_url = str(brownfield.get("repo_url", "")).strip()
            if not evidence_bundle_id and (repo_provider != "github" or not repo_url):
                return JSONResponse({"ok": False, "error": "Hybrid mode requires a connected GitHub repository or an uploaded evidence bundle."}, status_code=400)
    if use_case == "database_conversion" and not database_schema:
        return JSONResponse({"ok": False, "error": "database_schema is required for database_conversion use case"}, status_code=400)
    evidence_check = _evidence_mode_preflight(payload, integration_context)
    if not bool(evidence_check.get("ok", True)):
        return JSONResponse(
            {
                "ok": False,
                "error": str(evidence_check.get("error", "Evidence preflight failed")),
                "preflight": {"ok": False, "code": str(evidence_check.get("code", "evidence_mode_blocked"))},
                "evidence_preflight": evidence_check,
            },
            status_code=400,
        )

    try:
        llm_preflight = _llm_preflight_from_payload(payload)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc), "preflight": {"ok": False, "code": "invalid_provider"}}, status_code=400)
    if not bool(llm_preflight.get("ok")):
        return JSONResponse(
            {
                "ok": False,
                "error": str(llm_preflight.get("error", "LLM preflight failed")),
                "preflight": llm_preflight,
            },
            status_code=400,
        )

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
        defer_execution=ASYNC_RUN_QUEUE_ENABLED,
    )
    if ASYNC_RUN_QUEUE_ENABLED:
        enqueued, enqueue_error = _enqueue_run_worker_task(run_id)
        if not enqueued:
            return JSONResponse(
                {
                    "ok": False,
                    "error": f"run queued but worker dispatch failed: {enqueue_error}",
                    "run_id": run_id,
                },
                status_code=500,
            )
        resp = {"ok": True, "run_id": run_id, "status": "queued"}
        if enqueue_error:
            resp["warning"] = enqueue_error
        return JSONResponse(resp, status_code=202)
    return JSONResponse({"ok": True, "run_id": run_id, "status": "running"})


async def api_get_run(request):
    run_id = request.path_params.get("run_id", "")
    data = MANAGER.get_run(run_id)
    if not data:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    view = str(request.query_params.get("view", "")).strip().lower()
    if view in {"summary", "status", "compact"}:
        return JSONResponse({"ok": True, "run": _compact_run_status(data)})
    return JSONResponse({"ok": True, "run": data})


def _compact_run_status(run: dict[str, Any]) -> dict[str, Any]:
    logs = run.get("progress_logs", []) if isinstance(run.get("progress_logs", []), list) else []
    stage_status = run.get("stage_status", {}) if isinstance(run.get("stage_status", {}), dict) else {}
    log_count = int(run.get("progress_log_count", len(logs)) or len(logs))
    return {
        "run_id": str(run.get("run_id", "")),
        "status": str(run.get("status", "unknown")),
        "current_stage": int(run.get("current_stage", 0) or 0),
        "next_stage_idx": int(run.get("next_stage_idx", 0) or 0),
        "stage_status": stage_status,
        "error_message": run.get("error_message"),
        "updated_at": str(run.get("updated_at", "")),
        "progress_log_count": log_count,
        "progress_logs_tail": logs[-80:],
    }


async def api_get_run_status(request):
    run_id = request.path_params.get("run_id", "")
    data = MANAGER.get_run(run_id)
    if not data:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    return JSONResponse({"ok": True, "status": _compact_run_status(data)})


async def api_get_run_logs(request):
    run_id = request.path_params.get("run_id", "")
    try:
        limit = max(1, min(1000, int(str(request.query_params.get("limit", "200")).strip() or "200")))
    except ValueError:
        limit = 200
    if not run_id:
        return JSONResponse({"ok": False, "error": "run_id is required"}, status_code=400)
    data = MANAGER.get_run(run_id)
    if not data:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    logs: list[str] = []
    if hasattr(RUN_STORE, "load_logs"):
        try:
            logs = RUN_STORE.load_logs(run_id, limit=limit)
        except Exception:
            logs = []
    if not logs:
        raw = data.get("progress_logs", []) if isinstance(data.get("progress_logs", []), list) else []
        logs = raw[-limit:]
    return JSONResponse(
        {
            "ok": True,
            "run_id": run_id,
            "logs": logs,
            "count": int(data.get("progress_log_count", len(logs)) or len(logs)),
        }
    )


async def api_list_runs(_request):
    return JSONResponse({"ok": True, "runs": MANAGER.list_runs(limit=60)})


async def api_approve_run(request):
    run_id = request.path_params.get("run_id", "")
    payload = _get_json(await request.body())
    result = MANAGER.approve(run_id, payload)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


async def api_pause_run(request):
    run_id = request.path_params.get("run_id", "")
    result = MANAGER.pause(run_id)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


async def api_resume_run(request):
    run_id = request.path_params.get("run_id", "")
    result = MANAGER.resume(run_id)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


async def api_abort_run(request):
    run_id = request.path_params.get("run_id", "")
    payload = _get_json(await request.body())
    reason = str(payload.get("reason", "")).strip()
    result = MANAGER.abort(run_id, reason=reason)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


async def api_rerun_stage(request):
    run_id = request.path_params.get("run_id", "")
    payload = _get_json(await request.body())
    stage = _coerce_stage(payload.get("stage", 0))
    if stage <= 0:
        current = MANAGER.get_run(run_id) or {}
        stage = _coerce_stage(current.get("current_stage", 1) or 1)
        if stage <= 0:
            stage = 1
    result = MANAGER.rerun_stage(run_id, stage=stage)
    status_code = 200 if result.get("ok") else 400
    return JSONResponse(result, status_code=status_code)


def _append_analyst_conversation_change(
    analyst_output: dict[str, Any],
    run_id: str,
    summary: str,
    proposed_patch: list[dict[str, Any]],
) -> dict[str, Any]:
    output = dict(analyst_output) if isinstance(analyst_output, dict) else {}
    audit = output.get("conversation_audit", {})
    if not isinstance(audit, dict):
        audit = {}
    changes = audit.get("changes", [])
    if not isinstance(changes, list):
        changes = []
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    change = {
        "change_id": f"chg_{stamp}",
        "requested_by": "user",
        "summary": summary,
        "proposed_patch": [p for p in proposed_patch if isinstance(p, dict)],
        "approved_by": "user",
        "applied": True,
        "timestamp": _utc_now(),
    }
    changes.append(change)
    audit["thread_id"] = str(audit.get("thread_id", "")).strip() or (f"THREAD-{run_id}" if run_id else f"THREAD-{stamp}")
    audit["changes"] = changes[-200:]
    output["conversation_audit"] = audit
    return output


def _ensure_analyst_report_v2(output: dict[str, Any]) -> dict[str, Any]:
    if not isinstance(output, dict):
        return {}
    enriched = dict(output)
    try:
        if not isinstance(enriched.get("raw_artifacts", {}), dict):
            enriched["raw_artifacts"] = build_raw_artifact_set_v1(enriched)
        elif not enriched.get("raw_artifacts"):
            enriched["raw_artifacts"] = build_raw_artifact_set_v1(enriched)
    except Exception:
        pass
    try:
        enriched["analyst_report_v2"] = build_analyst_report_v2(enriched)
        qa_report = enriched.get("analyst_report_v2", {}).get("qa_report_v1", {})
        if isinstance(qa_report, dict) and qa_report:
            enriched["qa_report_v1"] = qa_report
    except Exception:
        pass
    return enriched


def _clean_text(value: Any) -> str:
    return str(value or "").strip()


def _as_dict_safe(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list_safe(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _run_context_workspace_project(integration_context: dict[str, Any]) -> tuple[str, str]:
    integration = integration_context if isinstance(integration_context, dict) else {}
    brain_ctx = integration.get("brain_context", {})
    brain_ctx = brain_ctx if isinstance(brain_ctx, dict) else {}
    workspace = _clean_text(brain_ctx.get("workspace")) or "default-workspace"
    project = _clean_text(brain_ctx.get("project")) or "default-project"
    return workspace, project


def _routing_text_blob(
    *,
    objectives: str,
    use_case: str,
    integration_context: dict[str, Any],
    legacy_code: str,
) -> tuple[str, list[str], set[str]]:
    integration = integration_context if isinstance(integration_context, dict) else {}
    brownfield = _as_dict_safe(integration.get("brownfield"))
    greenfield = _as_dict_safe(integration.get("greenfield"))
    scan_scope = _as_dict_safe(integration.get("scan_scope"))
    discover_cache = _as_dict_safe(integration.get("discover_cache"))
    analyst_summary = _as_dict_safe(discover_cache.get("analyst_summary"))

    evidence_files = _as_list_safe(analyst_summary.get("evidence_files"))
    include_paths = _as_list_safe(scan_scope.get("include_paths"))
    exclude_paths = _as_list_safe(scan_scope.get("exclude_paths"))
    file_like = [
        _clean_text(item).lower()
        for item in [*evidence_files, *include_paths, *exclude_paths]
        if _clean_text(item)
    ]
    artifact_signals: set[str] = set()
    if _clean_text(analyst_summary.get("overview")):
        artifact_signals.add("analyst_summary")
    for key in (
        "likely_capabilities",
        "key_components",
        "domain_functions",
        "data_entities",
        "input_output_contracts",
        "vb6_analysis",
        "legacy_skill_profile",
    ):
        val = analyst_summary.get(key)
        if isinstance(val, dict) and val:
            artifact_signals.add(key.lower())
        elif isinstance(val, list) and val:
            artifact_signals.add(key.lower())
        elif _clean_text(val):
            artifact_signals.add(key.lower())
    if file_like:
        artifact_signals.add("evidence_files")
    if _clean_text(brownfield.get("repo_url")):
        artifact_signals.add("repo_scan")

    parts = [
        _clean_text(objectives),
        _clean_text(use_case),
        _clean_text(integration.get("project_state_detected")),
        _clean_text(brownfield.get("repo_provider")),
        _clean_text(brownfield.get("repo_url")),
        _clean_text(greenfield.get("repo_destination")),
        _clean_text(greenfield.get("repo_target")),
        _clean_text(analyst_summary.get("overview")),
        " ".join([_clean_text(x) for x in _as_list_safe(analyst_summary.get("key_components"))[:24] if _clean_text(x)]),
        " ".join(file_like[:120]),
        _clean_text(legacy_code)[:120000],
    ]
    text_blob = " ".join([part for part in parts if part]).lower()
    return text_blob, file_like, artifact_signals


def _normalize_file_pattern(pattern: str) -> str:
    token = _clean_text(pattern).lower()
    if token.startswith("*."):
        return token[1:]
    return token.replace("*", "")


def _route_specialists_for_run(
    *,
    objectives: str,
    use_case: str,
    integration_context: dict[str, Any],
    legacy_code: str,
    stage_agent_ids: dict[str, Any] | None = None,
) -> dict[str, Any]:
    settings = SETTINGS_STORE.get_settings()
    hub = _as_dict_safe(settings.get("knowledge_hub"))
    specialists = _as_list_safe(hub.get("specialists"))
    stage_map = stage_agent_ids if isinstance(stage_agent_ids, dict) else {}
    stage_agent_values = {str(v).strip() for v in stage_map.values() if str(v).strip()}
    text_blob, file_like, artifact_signals = _routing_text_blob(
        objectives=objectives,
        use_case=use_case,
        integration_context=integration_context if isinstance(integration_context, dict) else {},
        legacy_code=legacy_code,
    )
    candidates: list[dict[str, Any]] = []
    for row in specialists:
        spec = _as_dict_safe(row)
        if not spec or not bool(spec.get("enabled", True)) or not bool(spec.get("auto_route", True)):
            continue
        specialist_id = _clean_text(spec.get("specialist_id"))
        name = _clean_text(spec.get("name")) or specialist_id or "specialist"
        linked_agent_key = _clean_text(spec.get("linked_agent_key"))
        stage_hint = int(spec.get("stage_hint", 0) or 0)
        route_target = ""
        if linked_agent_key and linked_agent_key in stage_agent_values:
            route_target = linked_agent_key
        elif stage_hint > 0:
            route_target = _clean_text(stage_map.get(str(stage_hint)))

        intent_keywords = [
            _clean_text(item).lower()
            for item in _as_list_safe(spec.get("intent_keywords"))
            if _clean_text(item)
        ]
        file_patterns = [
            _normalize_file_pattern(str(item))
            for item in _as_list_safe(spec.get("file_patterns"))
            if _normalize_file_pattern(str(item))
        ]
        artifact_triggers = [
            _clean_text(item).lower()
            for item in _as_list_safe(spec.get("artifact_triggers"))
            if _clean_text(item)
        ]
        matched_intents = [kw for kw in intent_keywords if kw and kw in text_blob][:6]
        matched_files = [
            pattern
            for pattern in file_patterns
            if pattern and (
                pattern in text_blob
                or any(pattern in path for path in file_like)
            )
        ][:6]
        matched_artifacts = [sig for sig in artifact_triggers if sig in artifact_signals][:6]
        score = (len(matched_intents) * 2) + len(matched_files) + len(matched_artifacts)
        threshold = max(1, min(10, int(spec.get("min_match_score", 1) or 1)))
        selected = score >= threshold
        candidates.append(
            {
                "specialist_id": specialist_id,
                "name": name,
                "domain": _clean_text(spec.get("domain")),
                "linked_agent_key": linked_agent_key,
                "route_target_agent_key": route_target,
                "tool_mode": _clean_text(spec.get("tool_mode")) or "read_only",
                "depth_tier": _clean_text(spec.get("depth_tier")) or "standard",
                "threshold": threshold,
                "score": score,
                "selected": selected,
                "dispatchable": bool(route_target),
                "matched_intents": matched_intents,
                "matched_files": matched_files,
                "matched_artifacts": matched_artifacts,
            }
        )
    candidates.sort(
        key=lambda row: (
            -int(row.get("score", 0) or 0),
            0 if bool(row.get("dispatchable")) else 1,
            str(row.get("name", "")).lower(),
        )
    )
    selected = [row for row in candidates if bool(row.get("selected"))]
    selected_ids = [str(row.get("specialist_id", "")).strip() for row in selected if str(row.get("specialist_id", "")).strip()]
    return {
        "evaluated_at": _utc_now(),
        "input_summary": {
            "candidate_count": len(candidates),
            "artifact_signals": sorted(list(artifact_signals))[:40],
            "stage_agent_count": len(stage_agent_values),
        },
        "selected": selected[:16],
        "selected_specialist_ids": selected_ids[:32],
        "selected_count": len(selected),
        "dispatchable_count": len([row for row in selected if bool(row.get("dispatchable"))]),
        "candidates": candidates[:80],
    }


def _build_run_context_bundle(
    *,
    run_id: str,
    objectives: str,
    use_case: str,
    integration_context: dict[str, Any],
    legacy_code: str = "",
    stage_agent_ids: dict[str, Any] | None = None,
) -> dict[str, Any]:
    integration = copy.deepcopy(integration_context if isinstance(integration_context, dict) else {})
    stage_map = stage_agent_ids if isinstance(stage_agent_ids, dict) else {}
    workspace, project = _run_context_workspace_project(integration)
    knowledge_context = SETTINGS_STORE.resolve_knowledge_run_context(
        workspace=workspace,
        project=project,
        stage_agent_ids=stage_map,
    )
    actor = integration.get("actor_context", {})
    actor = actor if isinstance(actor, dict) else {}
    actor_payload = {
        "email": _clean_text(actor.get("email")).lower(),
        "role": _clean_text(actor.get("role")).lower(),
        "display_name": _clean_text(actor.get("display_name")),
    }
    stage_agent_map = {str(k): _clean_text(v) for k, v in stage_map.items() if _clean_text(v)}
    digest_material = {
        "run_id": _clean_text(run_id),
        "workspace": workspace,
        "project": project,
        "actor": actor_payload,
        "stage_agent_ids": stage_agent_map,
        "knowledge_snapshot_id": _clean_text(knowledge_context.get("snapshot_id")),
        "knowledge_snapshot_hash": _clean_text(_as_dict_safe(knowledge_context.get("integrity")).get("snapshot_hash")),
    }
    bundle_hash = hashlib.sha256(
        json.dumps(digest_material, sort_keys=True, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
    ).hexdigest()
    specialist_routing = _route_specialists_for_run(
        objectives=objectives,
        use_case=use_case,
        integration_context=integration,
        legacy_code=legacy_code,
        stage_agent_ids=stage_map,
    )
    constitution = build_delivery_constitution_v1(
        run_id=run_id,
        workspace=workspace,
        project=project,
        objectives=objectives,
        use_case=use_case,
        integration_context=integration,
        knowledge_context=knowledge_context,
        stage_agent_ids=stage_map,
    )
    constitution_id = _clean_text(_as_dict_safe(constitution).get("constitution_id"))
    return {
        "bundle_id": f"runctx-{bundle_hash[:16]}",
        "created_at": _utc_now(),
        "run_id": _clean_text(run_id),
        "workspace": workspace,
        "project": project,
        "actor": actor_payload,
        "stage_agent_ids": stage_agent_map,
        "knowledge_context": knowledge_context,
        "specialist_routing": specialist_routing,
        "delivery_constitution": constitution,
        "constraints": {
            "snapshot_pinned": True,
            "immutable_snapshot": True,
            "disallow_latest_drift": True,
        },
        "integrity": {
            "bundle_hash": bundle_hash,
            "knowledge_snapshot_id": _clean_text(knowledge_context.get("snapshot_id")),
            "source_version_ids": _as_dict_safe(knowledge_context.get("integrity")).get("source_version_ids", []),
            "delivery_constitution_id": constitution_id,
            "selected_specialist_ids": _as_list_safe(_as_dict_safe(specialist_routing).get("selected_specialist_ids")),
        },
    }


def _persist_run_context_bundle_artifacts(run_context_bundle: dict[str, Any]) -> dict[str, Any]:
    bundle = dict(run_context_bundle) if isinstance(run_context_bundle, dict) else {}
    run_id = _clean_text(bundle.get("run_id")) or "run"
    constitution = _as_dict_safe(bundle.get("delivery_constitution"))
    if not constitution:
        return bundle
    out_dir = RUN_CONTEXT_ARTIFACT_ROOT / safe_name(run_id) / "context"
    out_dir.mkdir(parents=True, exist_ok=True)
    json_path = out_dir / "delivery_constitution_v1.json"
    md_path = out_dir / "delivery_constitution_v1.md"
    _write_json_file(json_path, constitution)
    md_path.write_text(delivery_constitution_to_markdown(constitution))

    artifact_paths = _as_dict_safe(bundle.get("artifact_paths"))
    artifact_paths["delivery_constitution_json"] = str(json_path.relative_to(ROOT))
    artifact_paths["delivery_constitution_md"] = str(md_path.relative_to(ROOT))
    bundle["artifact_paths"] = artifact_paths
    return bundle


def _append_gate_row(output: dict[str, Any], gate: dict[str, Any]) -> None:
    if not isinstance(output, dict):
        return
    gate_name = _clean_text(gate.get("name"))
    gate_row = {
        "name": gate_name or f"gate_{uuid.uuid4().hex[:8]}",
        "status": _clean_text(gate.get("status")).upper() or "WARN",
        "message": _clean_text(gate.get("message")) or "Guardrail check executed.",
        "details": _as_dict_safe(gate.get("details")),
    }
    quality = _as_list_safe(output.get("quality_gates"))
    replaced = False
    for idx, existing in enumerate(quality):
        if not isinstance(existing, dict):
            continue
        if _clean_text(existing.get("name")).lower() == gate_row["name"].lower():
            quality[idx] = gate_row
            replaced = True
            break
    if not replaced:
        quality.append(gate_row)
    output["quality_gates"] = quality

    req_pack = _as_dict_safe(output.get("requirements_pack"))
    if req_pack:
        req_gates = _as_list_safe(req_pack.get("quality_gates"))
        repack = False
        for idx, existing in enumerate(req_gates):
            if not isinstance(existing, dict):
                continue
            if _clean_text(existing.get("name")).lower() == gate_row["name"].lower():
                req_gates[idx] = gate_row
                repack = True
                break
        if not repack:
            req_gates.append(gate_row)
        req_pack["quality_gates"] = req_gates
        output["requirements_pack"] = req_pack


def _attach_run_context_to_analyst_output(output: dict[str, Any], run_context_bundle: dict[str, Any]) -> dict[str, Any]:
    out = dict(output) if isinstance(output, dict) else {}
    run_ctx = run_context_bundle if isinstance(run_context_bundle, dict) else {}
    knowledge_ctx = _as_dict_safe(run_ctx.get("knowledge_context"))
    constitution = _as_dict_safe(run_ctx.get("delivery_constitution"))
    specialist_routing = _as_dict_safe(run_ctx.get("specialist_routing"))
    source_versions = _as_dict_safe(knowledge_ctx.get("integrity")).get("source_version_ids", [])
    if not isinstance(source_versions, list):
        source_versions = []
    snapshot_id = _clean_text(knowledge_ctx.get("snapshot_id"))
    bundle_id = _clean_text(run_ctx.get("bundle_id"))
    constitution_id = _clean_text(constitution.get("constitution_id"))
    out["run_context_bundle_ref"] = {
        "bundle_id": bundle_id,
        "knowledge_snapshot_id": snapshot_id,
        "delivery_constitution_id": constitution_id,
        "workspace": _clean_text(run_ctx.get("workspace")),
        "project": _clean_text(run_ctx.get("project")),
        "source_version_ids": [str(item).strip() for item in source_versions if str(item).strip()],
        "selected_specialist_ids": _as_list_safe(specialist_routing.get("selected_specialist_ids")),
    }

    req_pack = _as_dict_safe(out.get("requirements_pack"))
    if req_pack:
        ctx_ref = _as_dict_safe(req_pack.get("context_reference"))
        ctx_ref["knowledge_snapshot_id"] = snapshot_id
        ctx_ref["run_context_bundle_id"] = bundle_id
        ctx_ref["source_version_ids"] = [str(item).strip() for item in source_versions if str(item).strip()]
        req_pack["context_reference"] = ctx_ref
        out["requirements_pack"] = req_pack

    report = _as_dict_safe(out.get("analyst_report_v2"))
    if report:
        appendix = _as_dict_safe(report.get("appendix"))
        refs = _as_dict_safe(appendix.get("artifact_refs"))
        if snapshot_id and bundle_id:
            refs["knowledge_snapshot_ref"] = f"runctx://{bundle_id}/{snapshot_id}"
        if constitution_id and bundle_id:
            refs["run_delivery_constitution_ref"] = f"runctx://{bundle_id}/delivery_constitution/{constitution_id}"
        appendix["artifact_refs"] = refs
        report["appendix"] = appendix
        report["run_context_bundle_ref"] = out.get("run_context_bundle_ref", {})
        out["analyst_report_v2"] = report
    return out


def _evaluate_compliance_citation_gate(output: dict[str, Any], run_context_bundle: dict[str, Any]) -> dict[str, Any]:
    run_ctx = run_context_bundle if isinstance(run_context_bundle, dict) else {}
    knowledge_ctx = _as_dict_safe(run_ctx.get("knowledge_context"))
    stage_agents = _as_dict_safe(run_ctx.get("stage_agent_ids"))
    analyst_key = _clean_text(stage_agents.get("1"))
    policies = _as_list_safe(knowledge_ctx.get("agent_policies"))
    source_count = int(_as_dict_safe(knowledge_ctx.get("integrity")).get("source_count", 0) or 0)
    set_count = int(_as_dict_safe(knowledge_ctx.get("integrity")).get("set_count", 0) or 0)
    citation_required = source_count > 0
    for row in policies:
        if not isinstance(row, dict):
            continue
        if _clean_text(row.get("agent_key")).lower() != analyst_key.lower():
            continue
        citation_required = bool(row.get("citation_required", False))
        break
    req_pack = _as_dict_safe(output.get("requirements_pack"))
    compliance = _as_dict_safe(req_pack.get("compliance"))
    controls = _as_list_safe(compliance.get("controls_triggered"))
    missing: list[str] = []
    if citation_required:
        for row in controls:
            if not isinstance(row, dict):
                continue
            cid = _clean_text(row.get("id")) or _clean_text(row.get("name")) or "control"
            sources = _as_list_safe(row.get("sources"))
            has_valid = False
            for src in sources:
                if not isinstance(src, dict):
                    continue
                if _clean_text(src.get("doc")) or _clean_text(src.get("section")) or _clean_text(src.get("url_or_ref")):
                    has_valid = True
                    break
            if not has_valid:
                missing.append(cid)
    enforce = bool(citation_required and (source_count > 0 or set_count > 0))
    blocked = bool(enforce and (source_count <= 0 or missing))
    assumptions_added: list[str] = []
    if blocked:
        assumptions = _as_list_safe(output.get("assumptions"))
        assumption = (
            "Compliance citation evidence is incomplete for one or more constraints. "
            "Treat compliance output as assumption until sources are attached and cited."
        )
        if assumption not in assumptions:
            assumptions.append(assumption)
            assumptions_added.append(assumption)
        output["assumptions"] = assumptions

        open_questions = _as_list_safe(req_pack.get("open_questions"))
        q_id = f"Q-CIT-{len(open_questions) + 1:03d}"
        detail = (
            f"Missing source citation(s) for controls: {', '.join(missing[:8])}."
            if missing
            else "No active knowledge sources found for compliance citation policy."
        )
        open_questions.append(
            {
                "id": q_id,
                "question": "Attach at least one citation source per compliance constraint before downstream planning.",
                "owner": "Compliance Lead",
                "severity": "blocker",
                "context": detail,
            }
        )
        req_pack["open_questions"] = open_questions
        output["requirements_pack"] = req_pack
    return {
        "blocked": blocked,
        "citation_required": citation_required,
        "enforce": enforce,
        "source_count": source_count,
        "set_count": set_count,
        "controls_count": len(controls),
        "missing_citations": missing,
        "assumptions_added": assumptions_added,
    }


def _evaluate_source_influenced_qa_gate(output: dict[str, Any], run_context_bundle: dict[str, Any]) -> dict[str, Any]:
    run_ctx = run_context_bundle if isinstance(run_context_bundle, dict) else {}
    knowledge_ctx = _as_dict_safe(run_ctx.get("knowledge_context"))
    source_count = int(_as_dict_safe(knowledge_ctx.get("integrity")).get("source_count", 0) or 0)
    set_count = int(_as_dict_safe(knowledge_ctx.get("integrity")).get("set_count", 0) or 0)
    source_influenced = source_count > 0 or set_count > 0
    if not source_influenced:
        return {
            "blocked": False,
            "source_influenced": False,
            "reason": "No active knowledge sources in run context snapshot.",
        }
    qa = _as_dict_safe(output.get("qa_report_v1"))
    if not qa:
        qa = _as_dict_safe(_as_dict_safe(output.get("analyst_report_v2")).get("qa_report_v1"))
    if not qa:
        return {
            "blocked": True,
            "source_influenced": True,
            "reason": "qa_report_v1 missing while source-influenced mode is active.",
        }
    structural = _as_dict_safe(qa.get("structural"))
    structural_status = _clean_text(structural.get("status")).upper() or "WARN"
    checks = _as_list_safe(structural.get("checks"))
    check_ids = {
        _clean_text(_as_dict_safe(row).get("check_id") or _as_dict_safe(row).get("id")).lower()
        for row in checks
        if isinstance(row, dict)
    }
    required_checks = {
        "cross_event_handler_reconciliation",
        "render_form_count_fidelity",
        "render_artifact_refs_present",
    }
    missing_checks = sorted([cid for cid in required_checks if cid not in check_ids])
    failing_required: list[str] = []
    for row in checks:
        if not isinstance(row, dict):
            continue
        cid = _clean_text(row.get("check_id") or row.get("id")).lower()
        if cid not in required_checks:
            continue
        if _clean_text(row.get("result")).lower() == "fail":
            failing_required.append(cid)
    blocked = bool(
        structural_status == "FAIL"
        or missing_checks
        or failing_required
    )
    reason_parts: list[str] = []
    if structural_status == "FAIL":
        reason_parts.append("QA structural status is FAIL.")
    if missing_checks:
        reason_parts.append(f"Required QA checks missing: {', '.join(missing_checks)}.")
    if failing_required:
        reason_parts.append(f"Required QA checks failed: {', '.join(sorted(set(failing_required)))}.")
    if not reason_parts:
        reason_parts.append("Source-influenced QA checks passed.")
    return {
        "blocked": blocked,
        "source_influenced": True,
        "source_count": source_count,
        "set_count": set_count,
        "structural_status": structural_status,
        "missing_checks": missing_checks,
        "failing_required_checks": sorted(set(failing_required)),
        "reason": " ".join(reason_parts),
    }


def _enforce_analyst_source_guardrails(output: dict[str, Any], run_context_bundle: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    out = _attach_run_context_to_analyst_output(output, run_context_bundle)
    run_ctx = run_context_bundle if isinstance(run_context_bundle, dict) else {}
    knowledge_ctx = _as_dict_safe(run_ctx.get("knowledge_context"))
    snapshot_id = _clean_text(knowledge_ctx.get("snapshot_id"))
    source_count = int(_as_dict_safe(knowledge_ctx.get("integrity")).get("source_count", 0) or 0)

    citation_eval = _evaluate_compliance_citation_gate(out, run_ctx)
    qa_eval = _evaluate_source_influenced_qa_gate(out, run_ctx)

    _append_gate_row(
        out,
        {
            "name": "knowledge_snapshot_pinned",
            "status": "PASS" if snapshot_id else "FAIL",
            "message": "Run is pinned to immutable knowledge source snapshots.",
            "details": {
                "snapshot_id": snapshot_id,
                "source_count": source_count,
            },
        },
    )
    _append_gate_row(
        out,
        {
            "name": "compliance_citation_grounding",
            "status": "FAIL" if citation_eval.get("blocked") else "PASS",
            "message": (
                "Compliance controls have required citations."
                if not citation_eval.get("blocked")
                else "Compliance controls are missing required citations or sources."
            ),
            "details": citation_eval,
        },
    )
    _append_gate_row(
        out,
        {
            "name": "source_influenced_qa_mandatory",
            "status": "FAIL" if qa_eval.get("blocked") else "PASS",
            "message": qa_eval.get("reason", ""),
            "details": qa_eval,
        },
    )

    blocked_reasons: list[str] = []
    if not snapshot_id:
        blocked_reasons.append("knowledge snapshot is missing")
    if citation_eval.get("blocked"):
        blocked_reasons.append("compliance citation gate failed")
    if qa_eval.get("blocked"):
        blocked_reasons.append("source-influenced QA gate failed")
    guardrail_report = {
        "status": "FAIL" if blocked_reasons else "PASS",
        "blocked": bool(blocked_reasons),
        "reasons": blocked_reasons,
        "knowledge_snapshot_id": snapshot_id,
        "citation_gate": citation_eval,
        "qa_gate": qa_eval,
        "evaluated_at": _utc_now(),
    }
    out["source_guardrails"] = guardrail_report
    return out, guardrail_report


def _merge_analyst_output_into_state(
    state: dict[str, Any],
    analyst_output: dict[str, Any],
    markdown_doc: str = "",
    upload_format: str = "markdown",
) -> None:
    run_id = str(state.get("run_id", "")).strip()
    analyst_output = _ensure_analyst_report_v2(analyst_output)
    if upload_format == "markdown":
        analyst_output = _append_analyst_conversation_change(
            analyst_output,
            run_id=run_id,
            summary="Human revised technical requirements markdown uploaded",
            proposed_patch=[
                {
                    "op": "replace",
                    "path": "/human_revised_document_markdown",
                    "value": markdown_doc[:2000] if markdown_doc else "[updated]",
                }
            ],
        )
    elif upload_format == "json":
        analyst_output = _append_analyst_conversation_change(
            analyst_output,
            run_id=run_id,
            summary="Human revised requirements pack JSON uploaded",
            proposed_patch=[
                {
                    "op": "replace",
                    "path": "/requirements_pack",
                    "value": {"artifact_type": str(analyst_output.get("requirements_pack", {}).get("artifact_type", "requirements_pack"))}
                    if isinstance(analyst_output.get("requirements_pack", {}), dict)
                    else {"artifact_type": "requirements_pack"},
                }
            ],
        )
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
        try:
            migrated = migrate_markdown_to_analyst_output(content, base_output=analyst_output)
            if isinstance(migrated, dict):
                analyst_output.update(migrated)
        except Exception as exc:
            analyst_output["markdown_migration"] = {
                "ok": False,
                "migrated_at": _utc_now(),
                "source_format": "markdown_v1",
                "error": str(exc),
            }

    active = MANAGER._get_record(run_id)
    if active and isinstance(active.pipeline_state, dict):
        _merge_analyst_output_into_state(
            active.pipeline_state,
            analyst_output,
            markdown_doc=markdown_doc,
            upload_format=fmt,
        )
        active.updated_at = _utc_now()
        MANAGER._append_log(active, "📝 Analyst tech requirements document updated from uploaded file")
        MANAGER._persist(active)
        return JSONResponse({"ok": True, "run": MANAGER._record_payload(active)})

    persisted = RUN_STORE.load_run(run_id)
    if not persisted:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    state = persisted.get("pipeline_state", {}) if isinstance(persisted.get("pipeline_state", {}), dict) else {}
    _merge_analyst_output_into_state(
        state,
        analyst_output,
        markdown_doc=markdown_doc,
        upload_format=fmt,
    )

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


def _extract_json_from_llm_text(raw: str) -> dict[str, Any]:
    text = str(raw or "").strip()
    if not text:
        return {}
    fenced = re.search(r"```(?:json)?\s*(\{[\s\S]*\})\s*```", text, flags=re.IGNORECASE)
    if fenced:
        text = fenced.group(1).strip()
    if not text.startswith("{"):
        start = text.find("{")
        end = text.rfind("}")
        if start >= 0 and end > start:
            text = text[start : end + 1]
    try:
        parsed = json.loads(text)
    except Exception:
        return {}
    return parsed if isinstance(parsed, dict) else {}


def _build_analyst_markdown_for_docgen(
    *,
    pipeline_state: dict[str, Any],
    analyst_output: dict[str, Any],
) -> str:
    state_md = str(pipeline_state.get("analyst_document_markdown", "")).strip()
    if state_md:
        return state_md
    revised_md = str(analyst_output.get("human_revised_document_markdown", "")).strip()
    if revised_md:
        return revised_md
    try:
        # Reuse the same markdown composer used by the CLI exporter so UI/CLI remain aligned.
        from scripts.run_vb6_analyst_markdown import build_full_markdown  # noqa: WPS433

        return str(build_full_markdown(analyst_output, mode="full") or "").strip()
    except Exception as exc:
        raise RuntimeError(f"Unable to compose analyst markdown for docgen: {exc}") from exc


def _ensure_synthetix_docgen_dependencies() -> None:
    package_json = DOCGEN_ROOT / "package.json"
    if not package_json.exists():
        raise RuntimeError(f"synthetix-docgen package.json not found at {DOCGEN_ROOT}")
    node_bin = shutil.which("node")
    npm_bin = shutil.which("npm")
    if not node_bin:
        raise RuntimeError("Node.js is required for synthetix-docgen (node not found in PATH)")
    if not npm_bin:
        raise RuntimeError("npm is required for synthetix-docgen (npm not found in PATH)")
    docx_module = DOCGEN_ROOT / "node_modules" / "docx"
    if docx_module.exists():
        return
    proc = subprocess.run(
        [npm_bin, "install", "--no-audit", "--no-fund"],
        cwd=str(DOCGEN_ROOT),
        capture_output=True,
        text=True,
        timeout=300,
    )
    if proc.returncode != 0:
        stderr = str(proc.stderr or "").strip()
        stdout = str(proc.stdout or "").strip()
        tail = stderr or stdout or "npm install failed"
        raise RuntimeError(f"Failed to install synthetix-docgen dependencies: {tail[-600:]}")


def _run_synthetix_docgen(
    *,
    markdown_text: str,
    doc_type: str,
    run_id: str,
    analyst_output: dict[str, Any] | None = None,
) -> tuple[bytes, str]:
    if doc_type not in {"ba_brief", "tech_workbook", "brd"}:
        raise RuntimeError(f"Unsupported doc type: {doc_type}")
    _ensure_synthetix_docgen_dependencies()
    node_bin = shutil.which("node")
    if not node_bin:
        raise RuntimeError("Node.js is required for synthetix-docgen")

    with tempfile.TemporaryDirectory(prefix=f"docgen-{safe_name(run_id)}-") as tmp_dir:
        tmp = Path(tmp_dir)
        md_path = tmp / "analyst-output.md"
        meta_path = tmp / "docgen-meta.json"
        out_dir = tmp / "output"
        out_dir.mkdir(parents=True, exist_ok=True)
        md_path.write_text(markdown_text, encoding="utf-8")

        docgen_meta: dict[str, Any] = {}
        if isinstance(analyst_output, dict) and analyst_output:
            try:
                report = build_analyst_report_v2(analyst_output)
                brief = _as_dict_safe(report.get("decision_brief"))
                glance = _as_dict_safe(brief.get("at_a_glance"))
                inv = _as_dict_safe(glance.get("inventory_summary"))
                raw = _as_dict_safe(report.get("raw_artifacts"))
                source_schema = _as_dict_safe(raw.get("source_schema_model"))
                provenance = _as_dict_safe(source_schema.get("provenance"))
                track_a = _as_dict_safe(provenance.get("track_a"))
                route = _clean_text(track_a.get("route")).lower()
                docgen_meta = {
                    "title": _clean_text(_as_dict_safe(report.get("metadata")).get("title")),
                    "repoUrl": _clean_text(_as_dict_safe(_as_dict_safe(report.get("metadata")).get("context_reference")).get("repo")),
                    "source_loc_total": int(inv.get("source_loc_total", 0) or 0),
                    "source_loc_forms": int(inv.get("source_loc_forms", 0) or 0),
                    "source_loc_modules": int(inv.get("source_loc_modules", 0) or 0),
                    "source_loc_classes": int(inv.get("source_loc_classes", 0) or 0),
                    "source_loc_designers": int(inv.get("source_loc_designers", 0) or 0),
                    "source_files_scanned": int(inv.get("source_files_scanned", 0) or 0),
                    "source_schema_route": route,
                    "mdb_detected": route == "mdb_direct_read",
                    "run_id": str(run_id or ""),
                }
            except Exception:
                docgen_meta = {}
        if docgen_meta:
            meta_path.write_text(json.dumps(docgen_meta, ensure_ascii=True), encoding="utf-8")

        proc = subprocess.run(
            [
                node_bin,
                "index.js",
                "--md",
                str(md_path),
                "--out",
                str(out_dir),
                "--meta",
                str(meta_path),
            ],
            cwd=str(DOCGEN_ROOT),
            capture_output=True,
            text=True,
            timeout=300,
        )
        if proc.returncode != 0:
            stderr = str(proc.stderr or "").strip()
            stdout = str(proc.stdout or "").strip()
            detail = stderr or stdout or "docgen command failed"
            raise RuntimeError(f"synthetix-docgen failed: {detail[-800:]}")

        file_name = (
            "ba_brief.docx"
            if doc_type == "ba_brief"
            else ("tech_workbook.docx" if doc_type == "tech_workbook" else "brd.docx")
        )
        target = out_dir / file_name
        if not target.exists():
            if doc_type == "brd":
                manifest_path = out_dir / "brd_render_manifest_v1.json"
                if manifest_path.exists():
                    try:
                        manifest = _get_json(manifest_path.read_bytes())
                    except Exception:
                        manifest = {}
                    if isinstance(manifest, dict):
                        placeholder_status = _clean_text(manifest.get("placeholder_fill_status")).lower()
                        render_error = _clean_text(manifest.get("render_error"))
                        if placeholder_status == "blocked_by_qa":
                            raise RuntimeError(render_error or "BRD rendering was blocked by QA structural failures.")
                        if render_error:
                            raise RuntimeError(f"BRD DOCX export failed: {render_error}")
            raise RuntimeError(f"Expected output not found: {target}")

        # Persist a complete export bundle per request so MD/DOCX/DB design artifacts
        # remain available outside transient temp folders.
        try:
            stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
            export_root = RUN_CONTEXT_ARTIFACT_ROOT / safe_name(str(run_id or "run")) / "docgen_exports" / stamp
            export_root.mkdir(parents=True, exist_ok=True)

            shutil.copy2(md_path, export_root / "analyst-output.md")
            for name in [
                "data.json",
                "code_literal_scan.json",
                "ba_brief.docx",
                "tech_workbook.docx",
                "brd.docx",
                "brd_package_v1.json",
                "brd_qa_report_v1.json",
                "brd_render_manifest_v1.json",
                "brd_project_meta_v1.json",
                "brd_version_history_v1.json",
                "brd_context_v1.json",
                "brd_general_requirements_v1.json",
                "brd_module_registry_v1.json",
                "brd_module_dossier_v1.json",
                "brd_appendices_v1.json",
                "brd_process_map_v1.json",
                "brd_template_anchor_map_v1.json",
            ]:
                src = out_dir / name
                if src.exists():
                    shutil.copy2(src, export_root / name)

            if isinstance(analyst_output, dict) and analyst_output:
                report = build_analyst_report_v2(analyst_output)
                raw = report.get("raw_artifacts", {}) if isinstance(report.get("raw_artifacts", {}), dict) else {}
                db_keys = [
                    "source_db_profile",
                    "source_schema_model",
                    "source_erd",
                    "source_query_catalog",
                    "source_relationship_candidates",
                    "source_data_dictionary",
                    "source_data_dictionary_markdown",
                    "source_hotspot_report",
                    "target_schema_model",
                    "target_erd",
                    "target_data_dictionary",
                    "schema_mapping_matrix",
                    "migration_plan",
                    "validation_harness_spec",
                    "db_qa_report",
                    "schema_approval_record",
                    "schema_drift_report",
                ]
                if not any(raw.get(k) is not None for k in db_keys):
                    # Fallback for older run payloads that keep a trimmed report shape.
                    rebuilt = build_raw_artifact_set_v1(analyst_output)
                    if isinstance(rebuilt, dict):
                        raw = rebuilt
                for key in db_keys:
                    payload = raw.get(key)
                    if payload is None:
                        continue
                    (export_root / f"{key}.json").write_text(
                        json.dumps(payload, indent=2, ensure_ascii=True, default=str),
                        encoding="utf-8",
                    )
                source_schema_model = raw.get("source_schema_model")
                if isinstance(source_schema_model, dict) and source_schema_model:
                    (export_root / "source_schema.json").write_text(
                        json.dumps(source_schema_model, indent=2, ensure_ascii=True, default=str),
                        encoding="utf-8",
                    )
                source_erd = raw.get("source_erd")
                if isinstance(source_erd, dict):
                    mermaid = str(source_erd.get("mermaid", "") or "").strip()
                    if mermaid:
                        (export_root / "source_erd.mmd").write_text(mermaid.rstrip() + "\n", encoding="utf-8")
                source_dict_md = raw.get("source_data_dictionary_markdown")
                if isinstance(source_dict_md, dict):
                    markdown = str(source_dict_md.get("markdown", "") or "").strip()
                    if markdown:
                        (export_root / "data_dictionary.md").write_text(markdown.rstrip() + "\n", encoding="utf-8")
        except Exception:
            # Keep download resilient even if persistence fails.
            pass

        return target.read_bytes(), file_name


def _maybe_build_docx_llm_plan(
    *,
    run_id: str,
    report: dict[str, Any],
    provider_hint: str = "",
    model_hint: str = "",
    temperature: float = 0.2,
) -> tuple[dict[str, Any] | None, dict[str, Any]]:
    meta: dict[str, Any] = {"used": False, "provider": "", "model": "", "reason": ""}
    provider = str(provider_hint or "").strip().lower()
    if provider not in {"anthropic", "openai"}:
        provider = str(
            SETTINGS_STORE.get_settings().get("llm", {}).get("default_provider", "anthropic")
        ).strip().lower()
    if provider not in {"anthropic", "openai"}:
        provider = "anthropic"

    try:
        creds = SETTINGS_STORE.resolve_llm_credentials(provider, requested_model=str(model_hint or "").strip())
    except Exception as exc:
        meta["reason"] = f"credential_resolution_failed:{exc}"
        return None, meta

    api_key = str(creds.get("api_key", "")).strip()
    if not api_key:
        meta["reason"] = "no_api_key"
        return None, meta
    model = str(creds.get("model", "")).strip() or ("gpt-4o" if provider == "openai" else "claude-sonnet-4-20250514")

    cfg = PipelineConfig(
        provider=LLMProvider.OPENAI if provider == "openai" else LLMProvider.ANTHROPIC,
        anthropic_api_key=api_key if provider == "anthropic" else "",
        openai_api_key=api_key if provider == "openai" else "",
        anthropic_model=model if provider == "anthropic" else "claude-sonnet-4-20250514",
        openai_model=model if provider == "openai" else "gpt-4o",
        temperature=max(0.0, min(1.0, float(temperature))),
        max_output_tokens=1800,
    )

    client = LLMClient(cfg)
    metadata = report.get("metadata", {}) if isinstance(report.get("metadata", {}), dict) else {}
    project = metadata.get("project", {}) if isinstance(metadata.get("project", {}), dict) else {}
    context = metadata.get("context_reference", {}) if isinstance(metadata.get("context_reference", {}), dict) else {}
    brief = report.get("decision_brief", {}) if isinstance(report.get("decision_brief", {}), dict) else {}
    glance = brief.get("at_a_glance", {}) if isinstance(brief.get("at_a_glance", {}), dict) else {}
    inventory = glance.get("inventory_summary", {}) if isinstance(glance.get("inventory_summary", {}), dict) else {}
    strategy = brief.get("recommended_strategy", {}) if isinstance(brief.get("recommended_strategy", {}), dict) else {}
    top_risks = brief.get("top_risks", []) if isinstance(brief.get("top_risks", []), list) else []
    delivery = report.get("delivery_spec", {}) if isinstance(report.get("delivery_spec", {}), dict) else {}
    backlog_items = (
        delivery.get("backlog", {}).get("items", [])
        if isinstance(delivery.get("backlog", {}), dict)
        else []
    )
    open_questions = delivery.get("open_questions", []) if isinstance(delivery.get("open_questions", []), list) else []

    context_payload = {
        "run_id": run_id,
        "project_name": str(project.get("name", "")),
        "objective": str(project.get("objective", "")),
        "repo": str(context.get("repo", "")),
        "readiness_score": glance.get("readiness_score"),
        "risk_tier": str(glance.get("risk_tier", "")),
        "inventory": {
            "projects": inventory.get("projects"),
            "forms": inventory.get("forms"),
            "dependencies": inventory.get("dependencies"),
            "event_handlers": inventory.get("event_handlers"),
            "tables_touched": (inventory.get("tables_touched") or [])[:20],
        },
        "strategy": {
            "name": str(strategy.get("name", "")),
            "rationale": str(strategy.get("rationale", "")),
            "phases": (strategy.get("phases") or [])[:5],
        },
        "top_risks": top_risks[:8],
        "backlog_items": (backlog_items or [])[:8],
        "open_questions": open_questions[:8],
    }

    system_prompt = (
        "You are an enterprise business analyst and document designer. "
        "Generate a concise DOCX blueprint JSON for a business-facing modernization workbook. "
        "Keep claims grounded in the provided context only. "
        "Do not invent technologies or requirements not present."
    )
    user_prompt = (
        "Return JSON only (no prose, no markdown) with this schema:\n"
        "{\n"
        '  "title": "string <= 120 chars",\n'
        '  "subtitle": "string <= 180 chars",\n'
        '  "narrative": "single paragraph <= 1200 chars",\n'
        '  "executive_bullets": ["3-6 bullets, each <= 200 chars"],\n'
        '  "callouts": [{"label":"short","message":"<=180 chars","severity":"low|medium|high"}],\n'
        '  "section_intros": {\n'
        '    "executive_snapshot":"<=220 chars",\n'
        '    "dependency_map":"<=220 chars",\n'
        '    "form_dossiers":"<=220 chars",\n'
        '    "flow_traces":"<=220 chars",\n'
        '    "traceability":"<=220 chars",\n'
        '    "sprints":"<=220 chars",\n'
        '    "risks":"<=220 chars"\n'
        "  }\n"
        "}\n"
        f"Context ({_prompt_payload_format()}): {_json_compact(context_payload, max_chars=4200)}"
    )

    try:
        response = client.invoke(system_prompt=system_prompt, user_message=user_prompt)
    except Exception as exc:
        meta["reason"] = f"llm_invoke_failed:{exc}"
        return None, meta

    parsed = _extract_json_from_llm_text(str(response.content or ""))
    if not parsed:
        meta["reason"] = "invalid_json_response"
        return None, meta

    meta["used"] = True
    meta["provider"] = str(response.provider or provider)
    meta["model"] = str(response.model or model)
    meta["reason"] = "ok"
    return parsed, meta


async def api_download_analyst_docx(request):
    run_id = request.path_params.get("run_id", "")
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
    analyst_output = _analyst_output_from_state(pipeline_state)
    if not isinstance(analyst_output, dict) or not analyst_output:
        return JSONResponse(
            {"ok": False, "error": "analyst output not found for this run"},
            status_code=404,
        )
    enriched = _ensure_analyst_report_v2(analyst_output)
    report = (
        enriched.get("analyst_report_v2", {})
        if isinstance(enriched.get("analyst_report_v2", {}), dict)
        else {}
    )
    if not report:
        return JSONResponse(
            {"ok": False, "error": "analyst report is unavailable for DOCX export"},
            status_code=404,
        )

    query = request.query_params
    requested_mode = str(query.get("mode", "llm_rich")).strip().lower()
    if requested_mode not in {"deterministic", "llm_rich"}:
        requested_mode = "llm_rich"

    llm_provider = str(query.get("provider", "")).strip().lower()
    llm_model = str(query.get("model", "")).strip()
    style_mode = str(query.get("style", "strict_template")).strip().lower()
    strict_template = style_mode in {"strict_template", "template", "locked"}
    template_path = str(query.get("template_path", "")).strip()
    llm_temp_raw = query.get("temperature", "0.2")
    try:
        llm_temp = float(llm_temp_raw)
    except (TypeError, ValueError):
        llm_temp = 0.2
    llm_temp = max(0.0, min(1.0, llm_temp))

    llm_doc_plan: dict[str, Any] | None = None
    llm_meta: dict[str, Any] = {"used": False, "provider": "", "model": "", "reason": "not_requested"}
    if requested_mode == "llm_rich":
        llm_doc_plan, llm_meta = _maybe_build_docx_llm_plan(
            run_id=run_id,
            report=report,
            provider_hint=llm_provider,
            model_hint=llm_model,
            temperature=llm_temp,
        )

    actual_mode = "llm_rich" if requested_mode == "llm_rich" and isinstance(llm_doc_plan, dict) and bool(llm_doc_plan) else "deterministic"

    try:
        # Pass enriched payload so DOCX renderer can use both report and raw artifacts.
        docx_bytes = build_business_docx_bytes(
            enriched,
            run_id=run_id,
            render_mode=actual_mode,
            llm_doc_plan=llm_doc_plan,
            template_path=template_path or None,
            strict_template=strict_template,
        )
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"docx generation failed: {exc}"}, status_code=500)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_run_id = safe_name(str(run_id or "run"))
    filename = f"analyst-business-brief-{safe_run_id}-{stamp}.docx"
    return StreamingResponse(
        io.BytesIO(docx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
            "X-Docx-Requested-Mode": requested_mode,
            "X-Docx-Render-Mode": actual_mode,
            "X-Docx-Style-Mode": style_mode,
            "X-Docx-LLM-Reason": str(llm_meta.get("reason", ""))[:220],
            "X-Docx-LLM-Provider": str(llm_meta.get("provider", ""))[:80],
            "X-Docx-LLM-Model": str(llm_meta.get("model", ""))[:120],
        },
    )


async def api_download_analyst_docgen_docx(request):
    run_id = request.path_params.get("run_id", "")
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    query = request.query_params
    doc_type = str(query.get("type", "ba_brief")).strip().lower()
    if doc_type not in {"ba_brief", "tech_workbook", "brd"}:
        return JSONResponse({"ok": False, "error": "type must be ba_brief, tech_workbook, or brd"}, status_code=400)

    pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
    analyst_output = _analyst_output_from_state(pipeline_state)
    integration_context = pipeline_state.get("integration_context", {}) if isinstance(pipeline_state.get("integration_context", {}), dict) else {}
    imported_seed = _imported_analysis_response_payload(
        str(pipeline_state.get("business_objectives", "") or run.get("business_objectives", "")).strip(),
        integration_context,
    )
    if imported_seed:
        existing_inventory = analyst_output.get("legacy_code_inventory", {}) if isinstance(analyst_output.get("legacy_code_inventory", {}), dict) else {}
        if not existing_inventory or str(analyst_output.get("source", "")).strip().lower() != "imported_analysis":
            analyst_output = imported_seed
    if not isinstance(analyst_output, dict) or not analyst_output:
        return JSONResponse({"ok": False, "error": "analyst output not found for this run"}, status_code=404)

    try:
        markdown_text = _build_analyst_markdown_for_docgen(
            pipeline_state=pipeline_state,
            analyst_output=analyst_output,
        )
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"unable to prepare analyst markdown: {exc}"}, status_code=500)
    if not markdown_text:
        return JSONResponse({"ok": False, "error": "analyst markdown is empty"}, status_code=500)

    try:
        docx_bytes, base_name = _run_synthetix_docgen(
            markdown_text=markdown_text,
            doc_type=doc_type,
            run_id=run_id,
            analyst_output=analyst_output,
        )
    except Exception as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=500)

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    safe_run_id = safe_name(str(run_id or "run"))
    file_stub = "ba-brief" if doc_type == "ba_brief" else ("tech-workbook" if doc_type == "tech_workbook" else "brd")
    filename = f"analyst-{file_stub}-{safe_run_id}-{stamp}.docx"
    return StreamingResponse(
        io.BytesIO(docx_bytes),
        media_type="application/vnd.openxmlformats-officedocument.wordprocessingml.document",
        headers={
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Cache-Control": "no-store",
            "X-Docgen-Type": doc_type,
            "X-Docgen-Source": base_name,
        },
    )


async def api_download_db_artifact(request):
    run_id = request.path_params.get("run_id", "")
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    query = request.query_params
    artifact_type = str(query.get("type", "source_schema")).strip().lower()
    if artifact_type not in {"source_schema", "source_erd", "data_dictionary"}:
        return JSONResponse(
            {"ok": False, "error": "type must be source_schema, source_erd, or data_dictionary"},
            status_code=400,
        )

    pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
    analyst_output = _analyst_output_from_state(pipeline_state)
    if not isinstance(analyst_output, dict) or not analyst_output:
        return JSONResponse({"ok": False, "error": "analyst output not found for this run"}, status_code=404)

    try:
        report = build_analyst_report_v2(analyst_output)
        raw = report.get("raw_artifacts", {}) if isinstance(report.get("raw_artifacts", {}), dict) else {}
        if not raw:
            raw = build_raw_artifact_set_v1(analyst_output)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"failed to build raw artifacts: {exc}"}, status_code=500)

    if artifact_type == "source_schema":
        payload = raw.get("source_schema_model")
        if not isinstance(payload, dict) or not payload:
            return JSONResponse({"ok": False, "error": "source schema artifact unavailable"}, status_code=404)
        file_name = f"source_schema-{safe_name(str(run_id or 'run'))}.json"
        content = json.dumps(payload, indent=2, ensure_ascii=True, default=str).encode("utf-8")
        media = "application/json"
    elif artifact_type == "source_erd":
        payload = raw.get("source_erd")
        mermaid = str(payload.get("mermaid", "")).strip() if isinstance(payload, dict) else ""
        if not mermaid:
            return JSONResponse({"ok": False, "error": "source ERD artifact unavailable"}, status_code=404)
        file_name = f"source_erd-{safe_name(str(run_id or 'run'))}.mmd"
        content = (mermaid.rstrip() + "\n").encode("utf-8")
        media = "text/plain; charset=utf-8"
    else:
        payload = raw.get("source_data_dictionary_markdown")
        markdown = str(payload.get("markdown", "")).strip() if isinstance(payload, dict) else ""
        if not markdown:
            dd = _as_dict(raw.get("source_data_dictionary"))
            rows = _as_list(dd.get("rows"))
            if rows:
                markdown = "# Source Schema - Data Dictionary\n\n"
                markdown += f"Rows: {len(rows)}\n"
        if not markdown:
            return JSONResponse({"ok": False, "error": "data dictionary artifact unavailable"}, status_code=404)
        file_name = f"data_dictionary-{safe_name(str(run_id or 'run'))}.md"
        content = (markdown.rstrip() + "\n").encode("utf-8")
        media = "text/markdown; charset=utf-8"

    return StreamingResponse(
        io.BytesIO(content),
        media_type=media,
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Cache-Control": "no-store",
            "X-DB-Artifact-Type": artifact_type,
        },
    )


async def api_download_discover_artifact(request):
    run_id = request.path_params.get("run_id", "")
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    query = request.query_params
    artifact_type = str(query.get("type", "project_metrics")).strip().lower()
    allowlist: dict[str, tuple[str, str]] = {
        "project_metrics": ("project_metrics", "project_metrics"),
        "static_forensics": ("static_forensics_layer", "static_forensics"),
        "quality_rules": ("code_quality_rules", "quality_rules"),
        "quality_violations": ("quality_violation_report", "quality_violations"),
        "dead_code": ("dead_code_report", "dead_code"),
        "type_dependency_matrix": ("type_dependency_matrix", "type_dependency_matrix"),
        "runtime_dependency_matrix": ("runtime_dependency_matrix", "runtime_dependency_matrix"),
        "third_party_usage": ("third_party_usage", "third_party_usage"),
        "trend_snapshot": ("trend_snapshot", "trend_snapshot"),
        "trend_series": ("trend_series", "trend_series"),
        "mdb_inventory": ("mdb_inventory", "mdb_inventory"),
        "form_loc_profile": ("form_loc_profile", "form_loc_profile"),
        "connection_string_variants": ("connection_string_variants", "connection_string_variants"),
        "module_global_inventory": ("module_global_inventory", "module_global_inventory"),
        "dead_form_refs": ("dead_form_refs", "dead_form_refs"),
        "dataenvironment_report_mapping": ("dataenvironment_report_mapping", "dataenvironment_report_mapping"),
        "static_risk_detectors": ("static_risk_detectors", "static_risk_detectors"),
    }
    if artifact_type not in allowlist:
        return JSONResponse(
            {
                "ok": False,
                "error": (
                    "type must be one of: project_metrics, static_forensics, quality_rules, quality_violations, "
                    "dead_code, type_dependency_matrix, runtime_dependency_matrix, "
                    "third_party_usage, trend_snapshot, trend_series, mdb_inventory, form_loc_profile, "
                    "connection_string_variants, module_global_inventory, dead_form_refs, "
                    "dataenvironment_report_mapping, static_risk_detectors"
                ),
            },
            status_code=400,
        )

    pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
    analyst_output = _analyst_output_from_state(pipeline_state)
    if not isinstance(analyst_output, dict) or not analyst_output:
        return JSONResponse({"ok": False, "error": "analyst output not found for this run"}, status_code=404)

    try:
        report = build_analyst_report_v2(analyst_output)
        raw = report.get("raw_artifacts", {}) if isinstance(report.get("raw_artifacts", {}), dict) else {}
        if not raw:
            raw = build_raw_artifact_set_v1(analyst_output)
    except Exception as exc:
        return JSONResponse({"ok": False, "error": f"failed to build raw artifacts: {exc}"}, status_code=500)

    artifact_key, file_stub = allowlist[artifact_type]
    payload = raw.get(artifact_key)
    if not payload:
        return JSONResponse({"ok": False, "error": f"{artifact_type} artifact unavailable"}, status_code=404)

    file_name = f"{file_stub}-{safe_name(str(run_id or 'run'))}.json"
    content = json.dumps(payload, indent=2, ensure_ascii=True, default=str).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/json",
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
            "Cache-Control": "no-store",
            "X-Discover-Artifact-Type": artifact_type,
        },
    )


def _coerce_stage(stage_raw: Any) -> int:
    try:
        stage = int(stage_raw)
    except (TypeError, ValueError):
        stage = 0
    return stage


def _stage_output_key(stage: int) -> str:
    idx = stage - 1
    if idx < 0 or idx >= len(AGENT_SEQUENCE):
        return ""
    _, output_key = AGENT_SEQUENCE[idx]
    return str(output_key or "")


def _stage_agent_name(stage: int) -> str:
    for card in AGENT_CARDS:
        if int(card.get("stage", 0) or 0) == stage:
            return str(card.get("name", f"Stage {stage}"))
    return f"Stage {stage}"


def _next_collab_id(prefix: str) -> str:
    return f"{prefix}_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_{uuid.uuid4().hex[:6]}"


def _stage_latest_result(state: dict[str, Any], stage: int) -> tuple[int, dict[str, Any] | None]:
    results = state.get("agent_results", []) if isinstance(state.get("agent_results", []), list) else []
    for idx in range(len(results) - 1, -1, -1):
        item = results[idx]
        if not isinstance(item, dict):
            continue
        try:
            item_stage = int(item.get("stage", 0) or 0)
        except (TypeError, ValueError):
            continue
        if item_stage == stage:
            return idx, item
    return -1, None


def _stage_output_snapshot(state: dict[str, Any], stage: int) -> dict[str, Any]:
    _, result = _stage_latest_result(state, stage)
    if isinstance(result, dict) and isinstance(result.get("output", {}), dict):
        return copy.deepcopy(result.get("output", {}))
    output_key = _stage_output_key(stage)
    output = state.get(output_key, {}) if output_key else {}
    if isinstance(output, dict):
        return copy.deepcopy(output)
    return {}


def _set_stage_output(state: dict[str, Any], stage: int, updated_output: dict[str, Any], summary: str) -> None:
    output_key = _stage_output_key(stage)
    if output_key:
        state[output_key] = updated_output

    results = list(state.get("agent_results", [])) if isinstance(state.get("agent_results", []), list) else []
    idx, result = _stage_latest_result(state, stage)
    stage_note = summary or f"Human collaboration update applied to Stage {stage}"
    if idx >= 0 and isinstance(result, dict):
        current = dict(result)
        current["output"] = updated_output
        current["status"] = str(current.get("status", "success") or "success")
        current["summary"] = stage_note
        logs = list(current.get("logs", [])) if isinstance(current.get("logs", []), list) else []
        logs.append(stage_note)
        current["logs"] = logs[-80:]
        results[idx] = current
    else:
        results.append(
            {
                "agent_name": _stage_agent_name(stage),
                "stage": stage,
                "status": "success",
                "summary": stage_note,
                "output": updated_output,
                "tokens_used": 0,
                "latency_ms": 0,
                "logs": [stage_note],
            }
        )
    state["agent_results"] = results


def _append_collab_log(progress_logs: list[str], message: str) -> None:
    progress_logs.append(f"[{_ts()}] 💬 {message}")
    if len(progress_logs) > 2000:
        del progress_logs[:-2000]


def _stage_collaboration_bucket(state: dict[str, Any], stage: int, *, create: bool) -> dict[str, Any]:
    if not isinstance(state, dict):
        raise ValueError("pipeline state is missing")
    root_existing = state.get("agent_collaboration")
    if not isinstance(root_existing, dict):
        root = {}
        state["agent_collaboration"] = root
    else:
        root = root_existing
        # Ensure the root is attached even when key was previously absent and a default
        # local dict would have been used.
        state["agent_collaboration"] = root
    key = str(stage)
    bucket = root.get(key, {})
    if not isinstance(bucket, dict):
        bucket = {}
    if create:
        bucket.setdefault("chat", [])
        bucket.setdefault("directives", [])
        bucket.setdefault("proposals", [])
        bucket.setdefault("decisions", [])
        bucket.setdefault("evidence", [])
        bucket["updated_at"] = _utc_now()
        root[key] = bucket
    return bucket


def _extract_stage_evidence(state: dict[str, Any], stage: int) -> list[dict[str, Any]]:
    output = _stage_output_snapshot(state, stage)
    _, result = _stage_latest_result(state, stage)
    evidence: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()

    context_ref = output.get("context_reference", {}) if isinstance(output.get("context_reference", {}), dict) else {}
    if context_ref:
        repo = str(context_ref.get("repo", "")).strip()
        branch = str(context_ref.get("branch", "")).strip()
        commit = str(context_ref.get("commit_sha", "")).strip()
        version_id = str(context_ref.get("version_id", "")).strip()
        ref_text = " | ".join([x for x in [repo, branch, commit, version_id] if x])
        if ref_text:
            key = ("context_ref", ref_text)
            seen.add(key)
            evidence.append(
                {
                    "id": _next_collab_id("ev"),
                    "kind": "context_ref",
                    "label": "Context reference",
                    "ref": ref_text,
                    "confidence": 1.0,
                }
            )

    logs = result.get("logs", []) if isinstance(result, dict) and isinstance(result.get("logs", []), list) else []
    for line in logs[-6:]:
        text = str(line).strip()
        if not text:
            continue
        key = ("stage_log", text)
        if key in seen:
            continue
        seen.add(key)
        evidence.append(
            {
                "id": _next_collab_id("ev"),
                "kind": "stage_log",
                "label": "Stage log",
                "ref": text,
                "confidence": 0.7,
            }
        )

    for field in ("evidence", "sources", "provenance"):
        value = output.get(field)
        if not isinstance(value, list):
            continue
        for item in value[:20]:
            if isinstance(item, dict):
                ref = str(item.get("ref") or item.get("file") or item.get("path") or item.get("source") or "").strip()
                if not ref:
                    continue
                key = ("artifact_evidence", ref)
                if key in seen:
                    continue
                seen.add(key)
                evidence.append(
                    {
                        "id": _next_collab_id("ev"),
                        "kind": "artifact_evidence",
                        "label": str(item.get("kind") or field).strip() or "evidence",
                        "ref": ref,
                        "confidence": float(item.get("confidence", 0.6) or 0.6),
                    }
                )
            elif isinstance(item, str):
                ref = item.strip()
                if not ref:
                    continue
                key = ("artifact_evidence", ref)
                if key in seen:
                    continue
                seen.add(key)
                evidence.append(
                    {
                        "id": _next_collab_id("ev"),
                        "kind": "artifact_evidence",
                        "label": field,
                        "ref": ref,
                        "confidence": 0.55,
                    }
                )
    return evidence[:40]


def _extract_directive_from_message(stage: int, message: str, actor: str) -> dict[str, Any] | None:
    text = str(message or "").strip()
    if not text:
        return None
    if len(text) > 2000:
        text = text[:2000]
    priority = "medium"
    lower = text.lower()
    if "must" in lower or "non-negotiable" in lower:
        priority = "high"
    if "should" in lower and priority != "high":
        priority = "medium"
    if "nice to have" in lower:
        priority = "low"
    return {
        "id": _next_collab_id("dir"),
        "stage": stage,
        "agent_name": _stage_agent_name(stage),
        "text": text,
        "priority": priority,
        "created_at": _utc_now(),
        "created_by": actor or "user",
        "status": "active",
    }


def _json_pointer_tokens(path: str) -> list[str]:
    if path == "":
        return []
    if not path.startswith("/"):
        raise ValueError(f"JSON pointer must start with '/': {path}")
    if path == "/":
        return [""]
    return [segment.replace("~1", "/").replace("~0", "~") for segment in path[1:].split("/")]


def _normalize_patch_ops(ops: Any) -> list[dict[str, Any]]:
    if not isinstance(ops, list):
        raise ValueError("patch must be an array")
    normalized: list[dict[str, Any]] = []
    for raw in ops:
        if not isinstance(raw, dict):
            continue
        op = str(raw.get("op", "")).strip().lower()
        path = str(raw.get("path", "")).strip()
        if op not in {"add", "replace", "remove"}:
            continue
        if not path:
            continue
        row: dict[str, Any] = {"op": op, "path": path}
        if op in {"add", "replace"}:
            row["value"] = raw.get("value")
        normalized.append(row)
    if not normalized:
        raise ValueError("patch must include at least one valid operation")
    return normalized


def _apply_json_patch_ops(document: dict[str, Any], operations: list[dict[str, Any]]) -> tuple[dict[str, Any], list[str]]:
    doc: Any = copy.deepcopy(document)
    changed_paths: list[str] = []
    for op_row in operations:
        op = str(op_row.get("op", "")).strip().lower()
        path = str(op_row.get("path", "")).strip()
        tokens = _json_pointer_tokens(path)
        if not tokens:
            if op == "remove":
                raise ValueError("remove operation cannot target document root")
            doc = op_row.get("value")
            changed_paths.append(path or "/")
            continue

        parent = doc
        for idx, token in enumerate(tokens[:-1]):
            next_token = tokens[idx + 1]
            if isinstance(parent, dict):
                if token not in parent or parent[token] is None:
                    if op == "remove":
                        raise ValueError(f"path does not exist: {path}")
                    parent[token] = [] if (next_token == "-" or next_token.isdigit()) else {}
                child = parent[token]
                if not isinstance(child, (dict, list)):
                    if op == "remove":
                        raise ValueError(f"path traverses non-container value: {path}")
                    child = [] if (next_token == "-" or next_token.isdigit()) else {}
                    parent[token] = child
                parent = child
                continue
            if isinstance(parent, list):
                if token == "-":
                    raise ValueError(f"invalid list pointer token '-' in middle of path: {path}")
                try:
                    list_index = int(token)
                except ValueError as exc:
                    raise ValueError(f"invalid list index `{token}` for path {path}") from exc
                if list_index < 0 or list_index >= len(parent):
                    raise ValueError(f"list index out of range for path {path}")
                child = parent[list_index]
                if not isinstance(child, (dict, list)):
                    if op == "remove":
                        raise ValueError(f"path traverses non-container value: {path}")
                    child = [] if (next_token == "-" or next_token.isdigit()) else {}
                    parent[list_index] = child
                parent = child
                continue
            raise ValueError(f"path traverses non-container value: {path}")

        last = tokens[-1]
        if isinstance(parent, dict):
            if op == "remove":
                if last not in parent:
                    raise ValueError(f"path does not exist: {path}")
                parent.pop(last, None)
            elif op == "add":
                parent[last] = op_row.get("value")
            elif op == "replace":
                if last not in parent:
                    raise ValueError(f"path does not exist for replace: {path}")
                parent[last] = op_row.get("value")
            changed_paths.append(path)
            continue

        if isinstance(parent, list):
            if last == "-":
                if op != "add":
                    raise ValueError(f"only add supports '-' list pointer: {path}")
                parent.append(op_row.get("value"))
                changed_paths.append(path)
                continue
            try:
                list_index = int(last)
            except ValueError as exc:
                raise ValueError(f"invalid list index `{last}` for path {path}") from exc
            if op == "add":
                if list_index < 0 or list_index > len(parent):
                    raise ValueError(f"list index out of range for add: {path}")
                parent.insert(list_index, op_row.get("value"))
            elif op == "replace":
                if list_index < 0 or list_index >= len(parent):
                    raise ValueError(f"list index out of range for replace: {path}")
                parent[list_index] = op_row.get("value")
            elif op == "remove":
                if list_index < 0 or list_index >= len(parent):
                    raise ValueError(f"list index out of range for remove: {path}")
                parent.pop(list_index)
            changed_paths.append(path)
            continue
        raise ValueError(f"path traverses non-container value: {path}")

    if not isinstance(doc, dict):
        raise ValueError("stage output must remain a JSON object after patch application")
    return doc, changed_paths


def _auto_patch_from_message(stage: int, message: str, state: dict[str, Any], actor: str) -> dict[str, Any] | None:
    text = str(message or "").strip()
    if not text:
        return None
    lower = text.lower()
    output = _stage_output_snapshot(state, stage)
    patch_ops: list[dict[str, Any]] = []
    title = "Stage output update"
    summary = "Add human feedback note"

    if stage == 1 and ("open question" in lower or lower.startswith("q:")):
        title = "Add open question"
        summary = "Append open question to analyst requirements pack"
        clean = text.split(":", 1)[1].strip() if ":" in text else text
        patch_ops.append({"op": "add", "path": "/open_questions/-", "value": clean})
    elif stage == 1 and ("non-functional" in lower or " nfr" in f" {lower}"):
        title = "Add non-functional requirement"
        summary = "Append non-functional requirement from collaboration request"
        existing = output.get("non_functional_requirements", [])
        count = len(existing) if isinstance(existing, list) else 0
        patch_ops.append(
            {
                "op": "add",
                "path": "/non_functional_requirements/-",
                "value": {
                    "id": f"NFR-{count + 1:03d}",
                    "title": text[:96],
                    "description": text,
                    "source": "human_collaboration",
                },
            }
        )
    elif stage == 1 and ("risk" in lower or "compliance" in lower):
        title = "Add risk note"
        summary = "Append risk/compliance note to analyst output"
        patch_ops.append(
            {
                "op": "add",
                "path": "/risks/-",
                "value": {
                    "impact": "medium",
                    "description": text,
                    "mitigation": "Validate in Architect/Test stages",
                    "source": "human_collaboration",
                },
            }
        )
    elif stage == 1 and ("requirement" in lower or "acceptance" in lower):
        title = "Add functional requirement"
        summary = "Append functional requirement from collaboration request"
        existing = output.get("functional_requirements", [])
        count = len(existing) if isinstance(existing, list) else 0
        patch_ops.append(
            {
                "op": "add",
                "path": "/functional_requirements/-",
                "value": {
                    "id": f"FR-{count + 1:03d}",
                    "title": text[:96],
                    "description": text,
                    "acceptance_criteria": [],
                    "source": "human_collaboration",
                },
            }
        )
    elif stage == 2 and ("constraint" in lower or "boundary" in lower or "option" in lower):
        title = "Update architecture constraints"
        summary = "Append architecture constraint/decision from collaboration"
        patch_ops.append({"op": "add", "path": "/design_constraints/-", "value": text})
    elif stage == 3 and ("implementation" in lower or "code" in lower or "library" in lower or "dependency" in lower):
        title = "Add implementation directive"
        summary = "Append implementation note for developer stage"
        patch_ops.append({"op": "add", "path": "/implementation_notes/-", "value": text})
    elif stage == 4 and ("migration" in lower or "schema" in lower or "sql" in lower):
        title = "Add database change request"
        summary = "Append database task from collaboration request"
        patch_ops.append({"op": "add", "path": "/database_tasks/-", "value": text})
    elif stage == 5 and ("security" in lower or "threat" in lower or "auth" in lower):
        title = "Add security control request"
        summary = "Append security control request from collaboration"
        patch_ops.append({"op": "add", "path": "/security_controls/-", "value": text})
    elif stage == 6 and ("test" in lower or "coverage" in lower or "scenario" in lower):
        title = "Add testing scenario"
        summary = "Append additional testing scenario request"
        patch_ops.append({"op": "add", "path": "/additional_tests/-", "value": text})
    elif stage == 7 and ("validation" in lower or "functional" in lower):
        title = "Add validation note"
        summary = "Append validation note from collaboration request"
        patch_ops.append({"op": "add", "path": "/validation_notes/-", "value": text})
    elif stage == 8 and ("deploy" in lower or "rollout" in lower or "rollback" in lower):
        title = "Add deployment constraint"
        summary = "Append deployment/rollout constraint"
        patch_ops.append({"op": "add", "path": "/release_constraints/-", "value": text})
    else:
        patch_ops.append(
            {
                "op": "add",
                "path": "/human_feedback/-",
                "value": {
                    "message": text,
                    "stage": stage,
                    "created_at": _utc_now(),
                    "created_by": actor or "user",
                },
            }
        )

    return {
        "id": _next_collab_id("prop"),
        "stage": stage,
        "agent_name": _stage_agent_name(stage),
        "title": title,
        "summary": summary,
        "status": "pending",
        "source": "chat",
        "created_at": _utc_now(),
        "created_by": actor or "user",
        "confidence": 0.6,
        "patch": patch_ops,
    }


def _stage_collaboration_view(state: dict[str, Any], stage: int) -> dict[str, Any]:
    bucket = _stage_collaboration_bucket(state, stage, create=True)
    bucket["evidence"] = _extract_stage_evidence(state, stage)
    output = _stage_output_snapshot(state, stage)
    _, result = _stage_latest_result(state, stage)
    summary = str(result.get("summary", "")) if isinstance(result, dict) else ""
    return {
        "stage": stage,
        "agent_name": _stage_agent_name(stage),
        "output_summary": summary,
        "output_keys": sorted([str(k) for k in output.keys()])[:40],
        "chat": list(bucket.get("chat", []))[-STAGE_COLLAB_CHAT_LIMIT:],
        "directives": list(bucket.get("directives", []))[-STAGE_COLLAB_DIRECTIVE_LIMIT:],
        "proposals": list(bucket.get("proposals", []))[-STAGE_COLLAB_PROPOSAL_LIMIT:],
        "decisions": list(bucket.get("decisions", []))[-STAGE_COLLAB_DECISION_LIMIT:],
        "evidence": list(bucket.get("evidence", []))[:40],
        "memory_applied": list(bucket.get("memory_applied", []))[:20],
        "memory_fingerprint": bucket.get("memory_fingerprint", {}),
        "llm_chat": bucket.get("llm_chat", {}),
        "updated_at": bucket.get("updated_at", _utc_now()),
    }


def _extract_titles(rows: Any, *, limit: int = 4) -> list[str]:
    if not isinstance(rows, list):
        return []
    out: list[str] = []
    for row in rows:
        if isinstance(row, dict):
            text = str(row.get("title") or row.get("name") or row.get("id") or "").strip()
            if text:
                out.append(text)
        elif isinstance(row, str):
            text = row.strip()
            if text:
                out.append(text)
        if len(out) >= limit:
            break
    return out


def _legacy_ui_analysis_from_state(output: dict[str, Any], state: dict[str, Any] | None = None) -> dict[str, Any]:
    def _normalize(vb6: dict[str, Any], legacy_inventory: dict[str, Any] | None = None) -> dict[str, Any]:
        if not isinstance(vb6, dict):
            return {}
        normalized = dict(vb6)
        projects = normalized.get("projects", []) if isinstance(normalized.get("projects", []), list) else []
        forms = normalized.get("forms", []) if isinstance(normalized.get("forms", []), list) else []
        form_names: list[str] = [str(x).strip() for x in forms if str(x).strip()]
        if isinstance(legacy_inventory, dict):
            inventory_projects = legacy_inventory.get("vb6_projects", [])
            if not projects and isinstance(inventory_projects, list):
                projects = [row for row in inventory_projects if isinstance(row, dict)]
                normalized["projects"] = projects
            inventory_forms = legacy_inventory.get("forms", [])
            if isinstance(inventory_forms, list):
                for row in inventory_forms:
                    if not isinstance(row, dict):
                        continue
                    form_type = str(row.get("form_type", "Form")).strip() or "Form"
                    form_name = str(row.get("form_name", "")).strip()
                    if not form_name:
                        continue
                    candidate = f"{form_type}:{form_name}"
                    if candidate not in form_names:
                        form_names.append(candidate)
            inventory_rules = legacy_inventory.get("business_rules_catalog", [])
            existing_rules = normalized.get("business_rules_catalog", [])
            if (
                isinstance(inventory_rules, list)
                and inventory_rules
                and (not isinstance(existing_rules, list) or not existing_rules)
            ):
                normalized["business_rules_catalog"] = inventory_rules

        project_forms: list[str] = []
        for row in projects:
            if not isinstance(row, dict):
                continue
            for value in row.get("forms", []) if isinstance(row.get("forms", []), list) else []:
                text = str(value).strip()
                if text:
                    project_forms.append(text)
        for pf in project_forms:
            if pf not in form_names:
                form_names.append(pf)
        normalized["forms"] = form_names[:200]
        return normalized

    legacy_inventory = output.get("legacy_code_inventory", {}) if isinstance(output.get("legacy_code_inventory", {}), dict) else {}
    primary = output.get("vb6_analysis", {}) if isinstance(output.get("vb6_analysis", {}), dict) else {}
    if primary:
        return _normalize(primary, legacy_inventory)
    pack = output.get("requirements_pack", {}) if isinstance(output.get("requirements_pack", {}), dict) else {}
    nested = pack.get("vb6_analysis", {}) if isinstance(pack.get("vb6_analysis", {}), dict) else {}
    if nested:
        pack_inventory = (
            pack.get("legacy_code_inventory", {})
            if isinstance(pack.get("legacy_code_inventory", {}), dict)
            else legacy_inventory
        )
        return _normalize(nested, pack_inventory)
    if isinstance(state, dict):
        integration_context = (
            state.get("integration_context", {})
            if isinstance(state.get("integration_context", {}), dict)
            else {}
        )
        discover_cache = (
            integration_context.get("discover_cache", {})
            if isinstance(integration_context.get("discover_cache", {}), dict)
            else {}
        )
        analyst_summary = (
            discover_cache.get("analyst_summary", {})
            if isinstance(discover_cache.get("analyst_summary", {}), dict)
            else {}
        )
        cached = (
            analyst_summary.get("vb6_analysis", {})
            if isinstance(analyst_summary.get("vb6_analysis", {}), dict)
            else {}
        )
        if cached:
            return _normalize(cached, legacy_inventory)
    return {}


def _analyst_context_reply(message: str, output: dict[str, Any], state: dict[str, Any] | None = None) -> str | None:
    lower = message.lower()
    asks_section_brief = (
        "section" in lower
        and any(token in lower for token in ["document", "documents", "doc", "brief", "summary", "overview", "explain"])
    )
    asks_count = bool(re.search(r"\b(how many|count|number of)\b", lower))
    asks_list_detail = any(token in lower for token in ["list", "which", "show", "detail", "details", "name", "names"])
    asks_project = any(token in lower for token in ["project", "projects", ".vbp", "vbp", "solution", "workspace"])
    asks_for_io = any(token in lower for token in ["input", "output", "i/o", "io contract", "interface contract"])
    asks_for_objective = any(
        token in lower
        for token in [
            "objective",
            "functionality",
            "what does",
            "what is this code",
            "legacy code",
            "summarize",
            "summary",
            "explain",
        ]
    )
    asks_legacy_ui = any(
        token in lower
        for token in [
            "form",
            "forms",
            "activex",
            "active x",
            "ocx",
            "control",
            "controls",
            "event handler",
            "vb6",
            "usercontrol",
            "mdi form",
        ]
    )
    asks_risks = any(token in lower for token in ["risk", "risks", "hazard", "security issue", "vulnerability"])
    asks_sql = any(token in lower for token in ["sql", "query", "queries", "table", "tables", "database statement"])
    asks_backlog = any(token in lower for token in ["backlog", "requirements", "fr-", "nfr-", "delivery spec", "work item"])
    asks_decisions = any(token in lower for token in ["decision", "blocking decision", "dec-", "approve", "approval"])
    asks_gates = any(token in lower for token in ["gate", "quality gate", "pass/fail", "validation gate"])
    asks_rules = any(token in lower for token in ["business rule", "rule catalog", "rule", "calculation logic"])
    asks_orphans = any(token in lower for token in ["orphan", "unmapped form", "membership", "reconcile"])
    asks_appendix = any(token in lower for token in ["appendix", "evidence", "artifact", "detailed output"])
    if asks_section_brief:
        report = (
            output.get("analyst_report_v2", {})
            if isinstance(output.get("analyst_report_v2", {}), dict)
            else {}
        )
        if not report:
            try:
                report = build_analyst_report_v2(output)
            except Exception:
                report = {}
        raw = (
            report.get("raw_artifacts", {})
            if isinstance(report.get("raw_artifacts", {}), dict)
            else output.get("raw_artifacts", {})
            if isinstance(output.get("raw_artifacts", {}), dict)
            else {}
        )
        if not isinstance(raw, dict) or not raw:
            return (
                "I do not have a completed Analyst artifact yet for this run. "
                "Run Stage 1 analysis first, then I can produce a section-by-section brief."
            )

        legacy_counts = (
            _as_dict_safe(_as_dict_safe(_as_dict_safe(raw.get("legacy_inventory")).get("summary")).get("counts"))
            if isinstance(raw.get("legacy_inventory"), dict)
            else {}
        )
        section_rows: list[tuple[str, str]] = [
            (
                "A. Legacy Inventory",
                "Project and source footprint baseline"
                + f" (projects={int(legacy_counts.get('projects', 0) or 0)},"
                + f" forms={int(legacy_counts.get('forms_or_screens', 0) or 0)},"
                + f" loc={int(legacy_counts.get('source_loc_total', 0) or 0)}).",
            ),
            (
                "B. Dependency Inventory",
                f"External/runtime dependencies and references (rows={len(_as_list_safe(_as_dict_safe(raw.get('dependency_inventory')).get('dependencies')))}).",
            ),
            (
                "C. Event Map",
                f"UI event handlers and call chains (rows={len(_as_list_safe(_as_dict_safe(raw.get('event_map')).get('entries')))}).",
            ),
            (
                "D. SQL Catalog",
                f"Extracted SQL statements and touched tables (rows={len(_as_list_safe(_as_dict_safe(raw.get('sql_catalog')).get('statements')))}).",
            ),
            (
                "E. Business Rules",
                f"Business-rule statements inferred from code behavior (rows={len(_as_list_safe(_as_dict_safe(raw.get('business_rule_catalog')).get('rules')))}).",
            ),
            (
                "F. Detector Findings",
                f"Static detector outcomes and remediation guidance (rows={len(_as_list_safe(_as_dict_safe(raw.get('detector_findings')).get('findings')))}).",
            ),
            (
                "H. SQL Map",
                f"Form/procedure to SQL attribution map (rows={len(_as_list_safe(_as_dict_safe(raw.get('sql_map')).get('entries')))}).",
            ),
            (
                "I. Procedure Summaries",
                f"Handler/procedure behavior summaries (rows={len(_as_list_safe(_as_dict_safe(raw.get('procedure_summary')).get('procedures')))}).",
            ),
            (
                "K. Form Dossiers",
                f"Per-form purpose, inputs/outputs, and coverage profile (rows={len(_as_list_safe(_as_dict_safe(raw.get('form_dossier')).get('dossiers')))}).",
            ),
            (
                "L. Risk Register",
                f"Risk list with severity and recommended actions (rows={len(_as_list_safe(_as_dict_safe(raw.get('risk_register')).get('risks')))}).",
            ),
            (
                "M. Orphan Analysis",
                f"Unmapped/orphaned form or file analysis (rows={len(_as_list_safe(_as_dict_safe(raw.get('orphan_analysis')).get('orphans')))}).",
            ),
            (
                "N. Repository Landscape",
                f"Project/variant inventory and topology (projects={len(_as_list_safe(_as_dict_safe(raw.get('repo_landscape')).get('projects')))}, variants={len(_as_list_safe(_as_dict_safe(raw.get('variant_inventory')).get('variants')))}).",
            ),
            (
                "Database Archaeology",
                f"Source schema + query catalog + mapping artifacts (tables={len(_as_list_safe(_as_dict_safe(raw.get('source_schema_model')).get('tables')))}, queries={len(_as_list_safe(_as_dict_safe(raw.get('source_query_catalog')).get('queries')))}, mappings={len(_as_list_safe(_as_dict_safe(raw.get('schema_mapping_matrix')).get('mappings')))}).",
            ),
        ]
        lines = ["Section-by-section brief (grounded in this run):"]
        lines.extend([f"- {name}: {summary}" for name, summary in section_rows])
        lines.append("If you want, I can also produce the same brief split into BA Brief vs Tech Workbook sections.")
        return "\n".join(lines)

    if not any(
        [
            asks_for_objective,
            asks_for_io,
            asks_legacy_ui,
            asks_project,
            asks_risks,
            asks_sql,
            asks_backlog,
            asks_decisions,
            asks_gates,
            asks_rules,
            asks_orphans,
            asks_appendix,
        ]
    ):
        return None

    vb6_analysis = _legacy_ui_analysis_from_state(output, state)
    if asks_legacy_ui or asks_project:
        forms = vb6_analysis.get("forms", []) if isinstance(vb6_analysis.get("forms", []), list) else []
        controls = vb6_analysis.get("controls", []) if isinstance(vb6_analysis.get("controls", []), list) else []
        activex = (
            vb6_analysis.get("activex_dependencies", [])
            if isinstance(vb6_analysis.get("activex_dependencies", []), list)
            else []
        )
        handlers = vb6_analysis.get("event_handlers", []) if isinstance(vb6_analysis.get("event_handlers", []), list) else []
        members = vb6_analysis.get("project_members", []) if isinstance(vb6_analysis.get("project_members", []), list) else []
        projects = vb6_analysis.get("projects", []) if isinstance(vb6_analysis.get("projects", []), list) else []
        if forms or controls or activex or handlers or projects:
            if asks_count and not asks_list_detail:
                metrics: list[str] = []
                if asks_project:
                    metrics.append(f"projects={len(projects)}")
                if "form" in lower:
                    metrics.append(f"forms={len(forms)}")
                if "control" in lower:
                    metrics.append(f"controls={len(controls)}")
                if any(token in lower for token in ["activex", "active x", "ocx", "com"]):
                    metrics.append(f"activex/com dependencies={len(activex)}")
                if "event" in lower or "handler" in lower:
                    metrics.append(f"event handlers={len(handlers)}")
                if not metrics:
                    metrics = [
                        f"projects={len(projects)}",
                        f"forms={len(forms)}",
                        f"controls={len(controls)}",
                        f"activex/com dependencies={len(activex)}",
                        f"event handlers={len(handlers)}",
                    ]
                return "Legacy VB6 counts: " + ", ".join(metrics) + "."
            lines = [
                (
                    "Legacy VB6 structure detected: "
                    f"projects={len(projects)}, forms={len(forms)}, controls={len(controls)}, ActiveX/COM dependencies={len(activex)}, "
                    f"event handlers={len(handlers)}."
                )
            ]
            if projects:
                lines.append(
                    "Projects: "
                    + "; ".join(
                        [
                            (
                                f"{str(row.get('project_name', 'Project'))} "
                                f"(members={int(row.get('member_count', 0) or 0)}, "
                                f"forms={int(row.get('forms_count', len(row.get('forms', [])) if isinstance(row.get('forms', []), list) else 0) or 0)})"
                                + (
                                    f" objective={str(row.get('business_objective_hypothesis', '')).strip()}"
                                    if str(row.get("business_objective_hypothesis", "")).strip()
                                    else ""
                                )
                            )
                            for row in projects[:8]
                            if isinstance(row, dict)
                        ]
                    )
                )
            if forms:
                lines.append("Forms/usercontrols: " + ", ".join([str(x) for x in forms[:12]]))
            if activex:
                lines.append("ActiveX/COM: " + ", ".join([str(x) for x in activex[:10]]))
            if members:
                lines.append("Project members: " + ", ".join([str(x) for x in members[:10]]))
            return "\n".join(lines)
        return (
            "I do not have extracted VB6 form/control metadata in this stage artifact. "
            "Run Discover analysis on the legacy repo/code first so I can answer exact form/ActiveX counts."
        )

    report = (
        output.get("analyst_report_v2", {})
        if isinstance(output.get("analyst_report_v2", {}), dict)
        else {}
    )
    if not report:
        try:
            report = build_analyst_report_v2(output)
        except Exception:
            report = {}
    raw_artifacts = (
        output.get("raw_artifacts", {})
        if isinstance(output.get("raw_artifacts", {}), dict)
        else {}
    )
    delivery_spec = report.get("delivery_spec", {}) if isinstance(report.get("delivery_spec", {}), dict) else {}
    testing = delivery_spec.get("testing_and_evidence", {}) if isinstance(delivery_spec.get("testing_and_evidence", {}), dict) else {}
    brief = report.get("decision_brief", {}) if isinstance(report.get("decision_brief", {}), dict) else {}

    if any([asks_risks, asks_sql, asks_backlog, asks_decisions, asks_gates, asks_rules, asks_orphans, asks_appendix]):
        detail_lines: list[str] = []
        if asks_risks:
            risk_rows = (
                raw_artifacts.get("risk_register", {}).get("risks", [])
                if isinstance(raw_artifacts.get("risk_register", {}), dict)
                else []
            )
            if not isinstance(risk_rows, list):
                risk_rows = []
            if not risk_rows and isinstance(brief.get("top_risks", []), list):
                risk_rows = brief.get("top_risks", [])
            detail_lines.append(f"Risk register rows: {len(risk_rows)}.")
            top_risks: list[str] = []
            for row in risk_rows[:4]:
                if not isinstance(row, dict):
                    continue
                rid = str(row.get("risk_id") or row.get("id") or "").strip()
                sev = str(row.get("severity") or "medium").strip().upper()
                desc = str(row.get("description") or "").strip()
                if desc:
                    prefix = f"[{sev}] {rid}: " if rid else f"[{sev}] "
                    top_risks.append((prefix + desc)[:180])
            if top_risks:
                detail_lines.append("Top risks: " + " | ".join(top_risks))

        if asks_sql:
            sql_rows = (
                raw_artifacts.get("sql_catalog", {}).get("statements", [])
                if isinstance(raw_artifacts.get("sql_catalog", {}), dict)
                else []
            )
            if not isinstance(sql_rows, list):
                sql_rows = []
            tables: set[str] = set()
            flagged = 0
            for row in sql_rows:
                if not isinstance(row, dict):
                    continue
                for t in row.get("tables", []) if isinstance(row.get("tables", []), list) else []:
                    txt = str(t).strip()
                    if txt:
                        tables.add(txt)
                flags = row.get("risk_flags", [])
                if isinstance(flags, list) and flags:
                    flagged += 1
            table_preview = ", ".join(sorted(list(tables))[:10]) if tables else "n/a"
            detail_lines.append(
                f"SQL catalog: {len(sql_rows)} statements, {len(tables)} tables, {flagged} flagged statement(s)."
            )
            detail_lines.append(f"Tables touched: {table_preview}")

        if asks_rules:
            rule_rows = (
                raw_artifacts.get("business_rule_catalog", {}).get("rules", [])
                if isinstance(raw_artifacts.get("business_rule_catalog", {}), dict)
                else []
            )
            if not isinstance(rule_rows, list):
                rule_rows = []
            category_counts: dict[str, int] = {}
            for row in rule_rows:
                if not isinstance(row, dict):
                    continue
                cat = str(row.get("category") or "other").strip() or "other"
                category_counts[cat] = int(category_counts.get(cat, 0) or 0) + 1
            top_cats = ", ".join(
                [
                    f"{k}={v}"
                    for k, v in sorted(category_counts.items(), key=lambda item: item[1], reverse=True)[:5]
                ]
            )
            detail_lines.append(f"Business rules: {len(rule_rows)} row(s)." + (f" Categories: {top_cats}." if top_cats else ""))

        if asks_orphans:
            orphan_rows = (
                raw_artifacts.get("orphan_analysis", {}).get("orphans", [])
                if isinstance(raw_artifacts.get("orphan_analysis", {}), dict)
                else []
            )
            if not isinstance(orphan_rows, list):
                orphan_rows = []
            detail_lines.append(f"Orphan analysis rows: {len(orphan_rows)}.")
            preview: list[str] = []
            for row in orphan_rows[:4]:
                if not isinstance(row, dict):
                    continue
                path = str(row.get("path") or row.get("project_name") or "").strip() or "n/a"
                rec = str(row.get("recommendation") or "verify").strip()
                preview.append(f"{path} -> {rec}")
            if preview:
                detail_lines.append("Orphan preview: " + " | ".join(preview))

        if asks_backlog:
            backlog = delivery_spec.get("backlog", {}) if isinstance(delivery_spec.get("backlog", {}), dict) else {}
            items = backlog.get("items", []) if isinstance(backlog.get("items", []), list) else []
            detail_lines.append(f"Delivery backlog items: {len(items)}.")
            top_items: list[str] = []
            for row in items[:5]:
                if not isinstance(row, dict):
                    continue
                rid = str(row.get("id") or "").strip()
                pri = str(row.get("priority") or "").strip()
                title = str(row.get("title") or row.get("outcome") or "").strip()
                if title:
                    top_items.append(f"{rid} ({pri}) {title}".strip())
            if top_items:
                detail_lines.append("Backlog preview: " + " | ".join(top_items))

        if asks_decisions:
            decisions = brief.get("decisions_required", {}) if isinstance(brief.get("decisions_required", {}), dict) else {}
            blocking = decisions.get("blocking", []) if isinstance(decisions.get("blocking", []), list) else []
            non_blocking = decisions.get("non_blocking", []) if isinstance(decisions.get("non_blocking", []), list) else []
            detail_lines.append(
                f"Decisions required: blocking={len(blocking)}, non-blocking={len(non_blocking)}."
            )
            previews: list[str] = []
            for row in blocking[:4]:
                if not isinstance(row, dict):
                    continue
                did = str(row.get("id") or "DEC").strip()
                question = str(row.get("question") or "").strip()
                if question:
                    previews.append(f"{did}: {question[:130]}")
            if previews:
                detail_lines.append("Blocking decisions: " + " | ".join(previews))

        if asks_gates:
            gates = testing.get("quality_gates", []) if isinstance(testing.get("quality_gates", []), list) else []
            detail_lines.append(f"Quality gates: {len(gates)}.")
            gate_preview: list[str] = []
            for row in gates[:6]:
                if not isinstance(row, dict):
                    continue
                gid = str(row.get("id") or "gate").strip()
                result = str(row.get("result") or "warn").strip().upper()
                gate_preview.append(f"{gid}={result}")
            if gate_preview:
                detail_lines.append("Gate status: " + ", ".join(gate_preview))

        if asks_appendix and not detail_lines:
            sql_rows = (
                raw_artifacts.get("sql_catalog", {}).get("statements", [])
                if isinstance(raw_artifacts.get("sql_catalog", {}), dict)
                else []
            )
            event_rows = (
                raw_artifacts.get("event_map", {}).get("entries", [])
                if isinstance(raw_artifacts.get("event_map", {}), dict)
                else []
            )
            rules = (
                raw_artifacts.get("business_rule_catalog", {}).get("rules", [])
                if isinstance(raw_artifacts.get("business_rule_catalog", {}), dict)
                else []
            )
            risks = (
                raw_artifacts.get("risk_register", {}).get("risks", [])
                if isinstance(raw_artifacts.get("risk_register", {}), dict)
                else []
            )
            detail_lines.append(
                "Detailed appendix snapshot: "
                f"event_map={len(event_rows) if isinstance(event_rows, list) else 0}, "
                f"sql_catalog={len(sql_rows) if isinstance(sql_rows, list) else 0}, "
                f"business_rules={len(rules) if isinstance(rules, list) else 0}, "
                f"risks={len(risks) if isinstance(risks, list) else 0}."
            )

        if detail_lines:
            detail_lines.append("Ask a narrower follow-up like 'show top 5 high risks' or 'list SQL touching LOGIN/logi'.")
            return "\n".join(detail_lines)

    pack = output.get("requirements_pack", {}) if isinstance(output.get("requirements_pack", {}), dict) else {}
    intake = output.get("intake", {}) if isinstance(output.get("intake", {}), dict) else {}
    if not intake:
        intake = pack.get("intake", {}) if isinstance(pack.get("intake", {}), dict) else {}
    requirements = output.get("requirements", {}) if isinstance(output.get("requirements", {}), dict) else {}
    if not requirements:
        requirements = pack.get("requirements", {}) if isinstance(pack.get("requirements", {}), dict) else {}
    domain_mapping = output.get("domain_mapping", {}) if isinstance(output.get("domain_mapping", {}), dict) else {}
    if not domain_mapping:
        domain_mapping = pack.get("domain_mapping", {}) if isinstance(pack.get("domain_mapping", {}), dict) else {}

    objective = (
        str(intake.get("business_objective_summary", "")).strip()
        or str(pack.get("business_objective_summary", "")).strip()
        or str(output.get("business_objective_summary", "")).strip()
        or str(output.get("executive_summary", "")).strip()
        or str(pack.get("executive_summary", "")).strip()
        or str(output.get("summary", "")).strip()
        or str(output.get("objective", "")).strip()
    )
    functional = requirements.get("functional", []) if isinstance(requirements.get("functional", []), list) else []
    if not functional:
        functional = output.get("functional_requirements", []) if isinstance(output.get("functional_requirements", []), list) else []

    capabilities = domain_mapping.get("capabilities", []) if isinstance(domain_mapping.get("capabilities", []), list) else []
    if not capabilities:
        capabilities = domain_mapping.get("capability_mapping", []) if isinstance(domain_mapping.get("capability_mapping", []), list) else []

    open_questions = output.get("open_questions", []) if isinstance(output.get("open_questions", []), list) else []
    if not open_questions:
        open_questions = pack.get("open_questions", []) if isinstance(pack.get("open_questions", []), list) else []
    function_titles = _extract_titles(functional, limit=4)
    capability_titles = _extract_titles(capabilities, limit=3)
    legacy_contract = (
        output.get("legacy_functional_contract", [])
        if isinstance(output.get("legacy_functional_contract", []), list)
        else []
    )
    legacy_inventory = output.get("legacy_code_inventory", {}) if isinstance(output.get("legacy_code_inventory", {}), dict) else {}
    if not legacy_inventory and isinstance(pack.get("legacy_code_inventory", {}), dict):
        legacy_inventory = pack.get("legacy_code_inventory", {})
    legacy_skill = output.get("legacy_skill_profile", {}) if isinstance(output.get("legacy_skill_profile", {}), dict) else {}
    if not legacy_skill and isinstance(pack.get("legacy_skill_profile", {}), dict):
        legacy_skill = pack.get("legacy_skill_profile", {})
    inventory_forms = legacy_inventory.get("forms", []) if isinstance(legacy_inventory.get("forms", []), list) else []
    inventory_projects = legacy_inventory.get("vb6_projects", []) if isinstance(legacy_inventory.get("vb6_projects", []), list) else []
    inventory_activex = legacy_inventory.get("activex_controls", []) if isinstance(legacy_inventory.get("activex_controls", []), list) else []
    inventory_dll = legacy_inventory.get("dll_dependencies", []) if isinstance(legacy_inventory.get("dll_dependencies", []), list) else []
    inventory_ocx = legacy_inventory.get("ocx_dependencies", []) if isinstance(legacy_inventory.get("ocx_dependencies", []), list) else []
    if not objective and function_titles:
        objective = (
            "Legacy system objective appears to be managing: "
            + ", ".join(function_titles[:3]).lower()
            + "."
        )

    if asks_for_io:
        io_lines: list[str] = []
        io_lines.append("Legacy input/output contract (from Analyst artifact):")
        for row in legacy_contract[:5]:
            if not isinstance(row, dict):
                continue
            fname = str(row.get("function_name", "")).strip() or "Unnamed function"
            ins = row.get("inputs", [])
            outs = row.get("outputs", [])
            in_text = ", ".join([str(x).strip() for x in ins if str(x).strip()]) if isinstance(ins, list) else ""
            out_text = ", ".join([str(x).strip() for x in outs if str(x).strip()]) if isinstance(outs, list) else ""
            io_lines.append(
                f"- {fname}: inputs=({in_text or 'none explicit'}), outputs=({out_text or 'none explicit'})"
            )
        if len(io_lines) > 1:
            io_lines.append("Ask 'expand function <name>' if you want a deeper behavior breakdown.")
            return "\n".join(io_lines)

    if not any([objective, function_titles, capability_titles, open_questions]):
        return "I do not have a completed Analyst artifact yet for this run. Run or rerun Stage 1 first, then I can summarize the legacy code objective and behavior."

    lines: list[str] = []
    lines.append("Analyst understanding:")
    if objective:
        lines.append(f"- Objective: {objective}")
    if function_titles:
        lines.append(f"- Core functional areas: {', '.join(function_titles)}")
    if legacy_skill:
        lines.append(
            "- Legacy skill in use: "
            f"{str(legacy_skill.get('selected_skill_name', 'Generic Legacy Skill'))} "
            f"({str(legacy_skill.get('selected_skill_id', 'generic_legacy'))}, "
            f"confidence={legacy_skill.get('confidence', 'n/a')})"
        )
    if inventory_forms or inventory_activex:
        lines.append(
            "- Legacy UI/component footprint: "
            f"projects={len(inventory_projects)}, forms={len(inventory_forms)}, ActiveX/COM={len(inventory_activex)}, "
            f"DLL={len(inventory_dll)}, OCX={len(inventory_ocx)}"
        )
        if inventory_forms:
            form_names: list[str] = []
            for row in inventory_forms[:8]:
                if isinstance(row, dict):
                    name = str(row.get("form_name", "")).strip()
                    ftype = str(row.get("form_type", "")).strip()
                    label = f"{ftype} {name}".strip()
                    if label:
                        form_names.append(label)
            if form_names:
                lines.append(f"- Forms detected: {', '.join(form_names)}")
    if capability_titles:
        lines.append(f"- Domain mapping: {', '.join(capability_titles)}")
    if isinstance(open_questions, list) and open_questions:
        lines.append(f"- Open questions to resolve: {len(open_questions)}")
    if "?" not in message and not asks_count:
        lines.append("Ask follow-ups like 'explain inputs/outputs' or 'list acceptance criteria' for a deeper breakdown.")
    return "\n".join(lines)


def _objective_question(message: str) -> bool:
    lower = str(message or "").lower()
    return any(
        token in lower
        for token in [
            "business objective",
            "objective",
            "legacy code",
            "legacy app",
            "what does",
            "what is this code",
            "functionality",
            "purpose",
            "summarize",
            "summary",
        ]
    )


def _is_simple_analyst_count_question(message: str) -> bool:
    lower = str(message or "").strip().lower()
    if not lower:
        return False
    has_count = bool(re.search(r"\b(how many|count|number of)\b", lower))
    has_vb6_topic = any(
        token in lower
        for token in ["project", "projects", "form", "forms", "control", "controls", "activex", "active x", "ocx", "event handler"]
    )
    return has_count and has_vb6_topic and len(lower.split()) <= 20


def _is_analyst_section_brief_question(message: str) -> bool:
    lower = str(message or "").strip().lower()
    if not lower:
        return False
    if "section" not in lower:
        return False
    mentions_docs = any(token in lower for token in ["document", "documents", "doc", "brief", "workbook"])
    asks_summary = any(token in lower for token in ["brief", "summary", "overview", "summarize", "explain"])
    return mentions_docs and asks_summary


def _latest_docgen_export_dir(run_id: str) -> Path | None:
    root = RUN_CONTEXT_ARTIFACT_ROOT / safe_name(str(run_id or "run")) / "docgen_exports"
    if not root.exists() or not root.is_dir():
        return None
    dirs = [p for p in root.iterdir() if p.is_dir()]
    if not dirs:
        return None
    # Directory names are UTC timestamps, lexical sort matches recency.
    return sorted(dirs, key=lambda p: p.name)[-1]


def _load_docgen_data_json(run_id: str) -> dict[str, Any]:
    export_dir = _latest_docgen_export_dir(run_id)
    if not export_dir:
        return {}
    path = export_dir / "data.json"
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _load_docgen_markdown(run_id: str) -> str:
    export_dir = _latest_docgen_export_dir(run_id)
    if not export_dir:
        return ""
    path = export_dir / "analyst-output.md"
    if not path.exists():
        return ""
    try:
        return str(path.read_text(encoding="utf-8"))
    except Exception:
        return ""


def _tokenize_rag_query(text: str) -> list[str]:
    lower = str(text or "").lower()
    raw = re.findall(r"[a-z0-9_]{3,}", lower)
    stop = {
        "the",
        "and",
        "for",
        "with",
        "from",
        "this",
        "that",
        "what",
        "when",
        "where",
        "which",
        "about",
        "each",
        "section",
        "document",
        "documents",
        "brief",
        "summary",
        "please",
        "help",
    }
    out: list[str] = []
    seen: set[str] = set()
    for token in raw:
        if token in stop or token in seen:
            continue
        seen.add(token)
        out.append(token)
    return out[:14]


def _extract_markdown_sections(markdown: str, max_sections: int = 80) -> list[tuple[str, str]]:
    text = str(markdown or "").strip()
    if not text:
        return []
    lines = text.splitlines()
    sections: list[tuple[str, str]] = []
    current_title = "Document Overview"
    buffer: list[str] = []
    for line in lines:
        if re.match(r"^\s*#{2,4}\s+", line):
            body = "\n".join(buffer).strip()
            if current_title or body:
                sections.append((current_title.strip(), body))
            current_title = re.sub(r"^\s*#{2,4}\s+", "", line).strip() or "Untitled Section"
            buffer = []
            if len(sections) >= max_sections:
                break
            continue
        buffer.append(line)
    if len(sections) < max_sections:
        body = "\n".join(buffer).strip()
        if current_title or body:
            sections.append((current_title.strip(), body))
    return sections[:max_sections]


def _section_relevance_score(title: str, body: str, tokens: list[str]) -> int:
    hay = f"{title}\n{body}".lower()
    score = 0
    for token in tokens:
        if token in title.lower():
            score += 5
        score += hay.count(token)
    return score


def _doc_rag_context_for_chat(run_id: str, message: str, max_chars: int = 1800) -> str:
    md = _load_docgen_markdown(run_id)
    data = _load_docgen_data_json(run_id)
    tokens = _tokenize_rag_query(message)

    snippets: list[str] = []
    if isinstance(data, dict) and data:
        meta = data.get("meta", {}) if isinstance(data.get("meta", {}), dict) else {}
        risks = data.get("risks", []) if isinstance(data.get("risks", []), list) else []
        sql_entries = data.get("sql_entries", []) if isinstance(data.get("sql_entries", []), list) else []
        mapped_forms = data.get("mapped_forms", []) if isinstance(data.get("mapped_forms", []), list) else []
        traceability = data.get("traceability", []) if isinstance(data.get("traceability", []), list) else []
        snippets.append(
            "Doc data snapshot: "
            f"title={str(meta.get('title', '')).strip() or 'n/a'}; "
            f"generated_at={str(meta.get('generated_at', '')).strip() or 'n/a'}; "
            f"forms={len(mapped_forms)}; sql_rows={len(sql_entries)}; risks={len(risks)}; traceability_rows={len(traceability)}."
        )

    sections = _extract_markdown_sections(md, max_sections=120)
    if sections:
        ranked = sorted(
            sections,
            key=lambda row: _section_relevance_score(row[0], row[1], tokens),
            reverse=True,
        )
        selected = []
        for title, body in ranked[:4]:
            if not title and not body:
                continue
            excerpt = re.sub(r"\s+", " ", body).strip()
            if len(excerpt) > 420:
                excerpt = excerpt[:420] + "..."
            selected.append(f"{title}: {excerpt}" if excerpt else title)
        if selected:
            snippets.append("Retrieved document sections:\n- " + "\n- ".join(selected))

    payload = "\n".join([s for s in snippets if s.strip()]).strip()
    if len(payload) <= max_chars:
        return payload
    return payload[:max_chars] + " ...[truncated]"


def _analyst_document_sections_reply(
    *,
    run_id: str,
    message: str,
    output: dict[str, Any],
    state: dict[str, Any] | None = None,
) -> str | None:
    lower = str(message or "").lower()
    if not _is_analyst_section_brief_question(lower):
        return None

    asks_ba = any(token in lower for token in ["ba brief", "business analyst brief", "ba document", "business brief"])
    asks_tech = any(token in lower for token in ["tech workbook", "technical workbook", "technical document", "tech document"])
    mode = "both"
    if asks_ba and not asks_tech:
        mode = "ba"
    elif asks_tech and not asks_ba:
        mode = "tech"

    data = _load_docgen_data_json(run_id)
    report = (
        output.get("analyst_report_v2", {})
        if isinstance(output.get("analyst_report_v2", {}), dict)
        else {}
    )
    raw = (
        report.get("raw_artifacts", {})
        if isinstance(report.get("raw_artifacts", {}), dict)
        else output.get("raw_artifacts", {})
        if isinstance(output.get("raw_artifacts", {}), dict)
        else {}
    )
    if not isinstance(raw, dict):
        raw = {}

    def _count(path_key: str, raw_fallback: int = 0) -> int:
        if isinstance(data, dict) and path_key in data and isinstance(data.get(path_key), list):
            return len(data.get(path_key))
        return int(raw_fallback or 0)

    lines: list[str] = []
    if mode in {"ba", "both"}:
        lines.append("BA Brief sections:")
        lines.append(
            f"- Executive Snapshot: delivery status and KPI view (active_q={_count('active_q')}, decisions={_count('decisions')})."
        )
        lines.append(
            f"- Decision Log: blockers/open decisions and ownership (rows={_count('decisions')})."
        )
        lines.append(
            f"- Form Inventory: business-facing form purposes, inputs, outputs (rows={_count('mapped_forms', len(_as_list_safe(_as_dict_safe(raw.get('form_dossier')).get('dossiers'))))})."
        )
        lines.append(
            f"- Business Rules: consolidated plain-English rules (rows={_count('rules', len(_as_list_safe(_as_dict_safe(raw.get('business_rule_catalog')).get('rules'))))})."
        )
        lines.append(
            f"- Traceability Matrix: form-level coverage and gaps (rows={_count('traceability')})."
        )
        lines.append(
            f"- Sprint Dependency Map: sprint assignment and blockers (rows={_count('sprints')})."
        )
        lines.append(
            f"- Risk Register: risks, severity, remediation actions (rows={_count('risks', len(_as_list_safe(_as_dict_safe(raw.get('risk_register')).get('risks'))))})."
        )

    if mode in {"tech", "both"}:
        lines.append("Tech Workbook sections:")
        lines.append(
            f"- Project Inventory: variants/projects, startup/member footprint (rows={_count('projects', len(_as_list_safe(_as_dict_safe(raw.get('repo_landscape')).get('projects'))))})."
        )
        lines.append(
            f"- Dependency Catalog: COM/OCX/DLL dependencies with references (rows={_count('dependencies', len(_as_list_safe(_as_dict_safe(raw.get('dependency_inventory')).get('dependencies'))))})."
        )
        lines.append(
            f"- Event Map + Procedure Summaries: event-driven call flows (events={_count('events', len(_as_list_safe(_as_dict_safe(raw.get('event_map')).get('entries'))))}, procedures={_count('procedure_summaries', len(_as_list_safe(_as_dict_safe(raw.get('procedure_summary')).get('procedures'))))})."
        )
        lines.append(
            f"- SQL Catalog + SQL Map: SQL extraction and form/procedure mapping (sql={_count('sql_entries', len(_as_list_safe(_as_dict_safe(raw.get('sql_catalog')).get('statements'))))}, sql_map={_count('sql_map_rows', len(_as_list_safe(_as_dict_safe(raw.get('sql_map')).get('entries'))))})."
        )
        lines.append(
            f"- Form Flow Traces: handler-to-SQL trace continuity (rows={_count('form_traces')})."
        )
        lines.append(
            f"- Project Dependency Map: cross-form/module/report dependencies (rows={_count('dep_map')})."
        )
        lines.append(
            f"- Detector Findings + Schema Conflicts: code smell and variant risk surfacing (findings={_count('findings', len(_as_list_safe(_as_dict_safe(raw.get('detector_findings')).get('findings'))))})."
        )

    if not lines:
        return None
    lines.append("Ask 'expand <section name>' and I’ll break it down with key rows and implications.")
    return "\n".join(lines)


def _cross_stage_objective_reply(stage: int, message: str, state: dict[str, Any]) -> str | None:
    if not _objective_question(message):
        return None
    analyst_output = _stage_output_snapshot(state, 1)
    if not analyst_output:
        discover_cache = (
            state.get("integration_context", {}).get("discover_cache", {})
            if isinstance(state.get("integration_context", {}), dict)
            and isinstance(state.get("integration_context", {}).get("discover_cache", {}), dict)
            else {}
        )
        summary = (
            discover_cache.get("analyst_summary", {})
            if isinstance(discover_cache.get("analyst_summary", {}), dict)
            else {}
        )
        overview = str(summary.get("overview", "")).strip()
        capabilities = summary.get("likely_capabilities", []) if isinstance(summary.get("likely_capabilities", []), list) else []
        if overview or capabilities:
            lines = ["Legacy objective summary (Discover baseline):"]
            if overview:
                lines.append(f"- Objective/context: {overview}")
            if capabilities:
                lines.append(f"- Functional areas: {', '.join([str(x) for x in capabilities[:4] if str(x).strip()])}")
            if stage == 2:
                lines.append("Architecture decisions should preserve these behaviors while modernizing interfaces and deployment.")
            return "\n".join(lines)
        return None

    analyst_text = _analyst_context_reply(message, analyst_output, state)
    if not analyst_text:
        return None
    if stage == 2:
        return analyst_text + "\nArchitecture stage should treat this as the modernization objective baseline."
    return analyst_text


def _generic_stage_reply(stage: int, output: dict[str, Any], summary: str) -> str | None:
    if not output and not summary:
        return None
    keys = sorted([str(k) for k in output.keys()])[:8]
    parts: list[str] = []
    if summary:
        parts.append(f"Latest stage summary: {summary}")
    if keys:
        parts.append(f"Available output fields: {', '.join(keys)}")
    if not parts:
        return None
    return " ".join(parts)


def _stage_memory_scope(run_id: str, state: dict[str, Any], stage: int) -> dict[str, Any]:
    integration_context = (
        state.get("integration_context", {})
        if isinstance(state.get("integration_context", {}), dict)
        else {}
    )
    brownfield = (
        integration_context.get("brownfield", {})
        if isinstance(integration_context.get("brownfield", {}), dict)
        else {}
    )
    greenfield = (
        integration_context.get("greenfield", {})
        if isinstance(integration_context.get("greenfield", {}), dict)
        else {}
    )
    seed = str(
        brownfield.get("repo_url")
        or greenfield.get("repo_target")
        or state.get("repo_url")
        or f"run-{run_id}"
    ).strip().lower()
    project_id = re.sub(r"[^a-z0-9]+", "-", seed).strip("-")[:80] or f"run-{run_id}"
    workspace_id = str(state.get("workspace_id", "default-workspace")).strip() or "default-workspace"
    client_id = str(state.get("client_id", "default-client")).strip() or "default-client"
    return {
        "workspace_id": workspace_id,
        "client_id": client_id,
        "project_id": project_id,
        "stage": stage,
    }


def _stage_memory_fingerprint(stage: int, state: dict[str, Any], message: str) -> dict[str, Any]:
    use_case = str(state.get("use_case", "business_objectives")).strip().lower() or "business_objectives"
    target_language = str(state.get("modernization_language", "")).strip()
    integration_context = (
        state.get("integration_context", {})
        if isinstance(state.get("integration_context", {}), dict)
        else {}
    )
    domain_pack_id = str(integration_context.get("domain_pack_id", "")).strip().lower()

    languages: list[str] = []
    if target_language:
        languages.append(target_language)
    discovery = state.get("sil_discovery", {}) if isinstance(state.get("sil_discovery", {}), dict) else {}
    language_counts = discovery.get("language_counts", {}) if isinstance(discovery.get("language_counts", {}), dict) else {}
    languages.extend([str(k) for k in language_counts.keys()])
    message_lower = str(message or "").lower()
    for marker in ("vb6", "asp", "asp.net", "c#", "java", "python", "go", "node", "typescript"):
        if marker in message_lower:
            languages.append(marker)

    legacy_signals: list[str] = []
    for marker in ("activex", "ocx", "com", "recordset", "on error resume next", "win32"):
        if marker in message_lower:
            legacy_signals.append(marker)
    stage_output = _stage_output_snapshot(state, 1) if stage > 1 else _stage_output_snapshot(state, stage)
    vb6 = (
        stage_output.get("legacy_ui_analysis", {}).get("vb6")
        if isinstance(stage_output.get("legacy_ui_analysis", {}), dict)
        else {}
    )
    detectors = vb6.get("detectors", []) if isinstance(vb6, dict) and isinstance(vb6.get("detectors", []), list) else []
    legacy_signals.extend([str(x.get("id", "")) for x in detectors if isinstance(x, dict)])

    components: list[str] = []
    scm = state.get("system_context_model", {}) if isinstance(state.get("system_context_model", {}), dict) else {}
    graph = scm.get("graph", {}) if isinstance(scm.get("graph", {}), dict) else {}
    nodes = graph.get("nodes", []) if isinstance(graph.get("nodes", []), list) else []
    for row in nodes[:24]:
        if not isinstance(row, dict):
            continue
        name = str(row.get("name", "")).strip()
        if name:
            components.append(name)

    domains: list[str] = [domain_pack_id] if domain_pack_id else []
    if "banking" in domain_pack_id:
        domains.extend(["banking", "payments"])
    if "database" in use_case:
        domains.append("database")

    def _dedupe(values: list[str]) -> list[str]:
        out: list[str] = []
        seen: set[str] = set()
        for value in values:
            text = str(value).strip()
            if not text:
                continue
            key = text.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(text)
        return out

    return {
        "agent_stages": [str(stage)],
        "use_case": use_case,
        "target_language": target_language,
        "languages": _dedupe(languages),
        "domains": _dedupe(domains),
        "components": _dedupe(components),
        "legacy_signals": _dedupe(legacy_signals),
    }


def _stage_thread_id(run_id: str, state: dict[str, Any], bucket: dict[str, Any], stage: int) -> str:
    bucket_tid = str(bucket.get("thread_id", "")).strip()
    map_rows = state.get("stage_thread_ids", {})
    mapped_tid = ""
    if isinstance(map_rows, dict):
        mapped_tid = str(map_rows.get(str(stage), "")).strip()
    if stage == 1:
        preferred = str(state.get("analyst_aas_thread_id", "")).strip()
    else:
        preferred = ""
    thread_id = bucket_tid or mapped_tid or preferred or f"run-{run_id}-stage{stage}"
    if not isinstance(map_rows, dict):
        map_rows = {}
    map_rows[str(stage)] = thread_id
    state["stage_thread_ids"] = map_rows
    bucket["thread_id"] = thread_id
    return thread_id


def _architect_context_reply(message: str, output: dict[str, Any], state: dict[str, Any]) -> str | None:
    lower = message.lower()
    asks_diagram = any(tok in lower for tok in ["diagram", "c4", "mermaid", "topology", "architecture"])
    asks_impact = any(tok in lower for tok in ["impact", "scope", "blast", "dependency", "dependencies"])
    if not asks_diagram and not asks_impact:
        return None
    pattern = str(output.get("pattern", "")).strip()
    overview = str(output.get("overview", "")).strip()
    current_diagram = (
        str(output.get("current_system_diagram_mermaid", "")).strip()
        or str(output.get("legacy_system_diagram_mermaid", "")).strip()
    )
    target_diagram = (
        str(output.get("target_system_diagram_mermaid", "")).strip()
        or str(output.get("target_architecture_diagram_mermaid", "")).strip()
        or str(output.get("architecture_diagram_mermaid", "")).strip()
    )
    scm = state.get("system_context_model", {}) if isinstance(state.get("system_context_model", {}), dict) else {}
    graph = scm.get("graph", {}) if isinstance(scm.get("graph", {}), dict) else {}
    nodes = graph.get("nodes", []) if isinstance(graph.get("nodes", []), list) else []
    edges = graph.get("edges", []) if isinstance(graph.get("edges", []), list) else []
    lines: list[str] = []
    if pattern:
        lines.append(f"Architecture pattern: {pattern}.")
    if overview:
        lines.append(f"Overview: {overview}")
    if asks_diagram:
        lines.append(
            "Diagram availability: "
            f"legacy/current={'yes' if current_diagram else 'no'}, target={'yes' if target_diagram else 'no'}."
        )
    if asks_impact:
        lines.append(f"Context topology baseline: nodes={len(nodes)}, edges={len(edges)}.")
    return " ".join(lines) if lines else None


def _developer_context_reply(message: str, output: dict[str, Any]) -> str | None:
    lower = message.lower()
    asks_code = any(tok in lower for tok in ["code", "file", "loc", "implementation", "artifact", "where"])
    if not asks_code:
        return None
    implementations = output.get("implementations", []) if isinstance(output.get("implementations", []), list) else []
    artifact_root = str(output.get("artifact_root", "")).strip()
    total_loc = int(output.get("total_loc", 0) or 0)
    total_files = int(output.get("total_files", 0) or 0)
    total_components = int(output.get("total_components", 0) or 0)
    top = []
    for row in implementations[:4]:
        if not isinstance(row, dict):
            continue
        name = str(row.get("component_name", "")).strip() or "component"
        lang = str(row.get("language", "")).strip() or "unknown"
        top.append(f"{name} ({lang})")
    lines = [
        f"Developer artifact summary: components={total_components}, files={total_files}, total_loc={total_loc}.",
    ]
    if artifact_root:
        lines.append(f"Primary artifact root: {artifact_root}")
    if top:
        lines.append(f"Top generated components: {', '.join(top)}.")
    return " ".join(lines)


def _database_context_reply(message: str, output: dict[str, Any]) -> str | None:
    lower = message.lower()
    if not any(tok in lower for tok in ["database", "schema", "migration", "script", "sql"]):
        return None
    source = str(output.get("source_engine", "")).strip()
    target = str(output.get("target_engine", "")).strip()
    scripts = output.get("generated_scripts", []) if isinstance(output.get("generated_scripts", []), list) else []
    summary = str(output.get("migration_summary", "")).strip()
    lines = [
        f"Database conversion summary: source={source or 'n/a'}, target={target or 'n/a'}, scripts={len(scripts)}.",
    ]
    if summary:
        lines.append(summary)
    return " ".join(lines)


def _security_context_reply(message: str, output: dict[str, Any]) -> str | None:
    lower = message.lower()
    if not any(tok in lower for tok in ["security", "threat", "control", "risk", "auth", "vuln", "vulnerability"]):
        return None
    threats = output.get("threat_model", []) if isinstance(output.get("threat_model", []), list) else []
    controls = output.get("required_controls", []) if isinstance(output.get("required_controls", []), list) else []
    release = output.get("release_recommendation", {}) if isinstance(output.get("release_recommendation", {}), dict) else {}
    status = str(release.get("status", "conditional")).strip().upper() or "CONDITIONAL"
    summary = str(output.get("security_summary", "")).strip()
    lines = [f"Security posture: release recommendation={status}, threats={len(threats)}, controls={len(controls)}."]
    if summary:
        lines.append(summary)
    return " ".join(lines)


def _tester_context_reply(message: str, output: dict[str, Any]) -> str | None:
    lower = message.lower()
    asks_test = any(tok in lower for tok in ["test", "failure", "failed", "pass", "quality gate", "coverage", "qa"])
    if not asks_test:
        return None
    overall = output.get("overall_results", {}) if isinstance(output.get("overall_results", {}), dict) else {}
    failed = output.get("failed_checks", []) if isinstance(output.get("failed_checks", []), list) else []
    warnings = int(overall.get("warnings", 0) or 0)
    passed = int(overall.get("passed", 0) or 0)
    total = int(overall.get("total_tests", 0) or 0)
    gate = str(overall.get("quality_gate", "unknown")).strip().upper()
    lines = [f"QA results: quality_gate={gate}, passed={passed}/{total}, failed={len(failed)}, warnings={warnings}."]
    top_failures = []
    for row in failed[:3]:
        if isinstance(row, dict):
            name = str(row.get("name", "")).strip() or "check"
            cause = str(row.get("root_cause", "")).strip()
            top_failures.append(f"{name}{(': ' + cause) if cause else ''}")
    if top_failures:
        lines.append(f"Top failures: {' | '.join(top_failures)}.")
    return " ".join(lines)


def _validation_context_reply(message: str, output: dict[str, Any]) -> str | None:
    lower = message.lower()
    if not any(tok in lower for tok in ["validation", "functional", "coverage", "criteria", "acceptance"]):
        return None
    verdict = output.get("overall_verdict", {}) if isinstance(output.get("overall_verdict", {}), dict) else {}
    status = str(verdict.get("status", "unknown")).strip().upper()
    functional = verdict.get("functional_coverage_percent", 0)
    nfr = verdict.get("nfr_compliance_percent", 0)
    summary = str(output.get("validation_summary", "")).strip()
    lines = [f"Validation verdict={status}, functional_coverage={functional}%, nfr_compliance={nfr}%."]
    if summary:
        lines.append(summary)
    return " ".join(lines)


def _deployment_context_reply(message: str, output: dict[str, Any]) -> str | None:
    lower = message.lower()
    if not any(tok in lower for tok in ["deploy", "deployment", "release", "rollback", "health", "url", "container"]):
        return None
    result = output.get("deployment_result", {}) if isinstance(output.get("deployment_result", {}), dict) else {}
    target = str(output.get("deployment_target", "")).strip() or "local"
    status = str(result.get("status", "unknown")).strip().upper()
    url = str(result.get("url", "")).strip()
    lines = [f"Deployment status: target={target}, result={status}."]
    if url:
        lines.append(f"Endpoint: {url}")
    return " ".join(lines)


def _stage_context_reply(stage: int, message: str, output: dict[str, Any], state: dict[str, Any]) -> str | None:
    cross_stage = _cross_stage_objective_reply(stage, message, state)
    if cross_stage:
        return cross_stage
    if stage == 1:
        return _analyst_context_reply(message, output, state)
    if stage == 2:
        return _architect_context_reply(message, output, state)
    if stage == 3:
        return _developer_context_reply(message, output)
    if stage == 4:
        return _database_context_reply(message, output)
    if stage == 5:
        return _security_context_reply(message, output)
    if stage == 6:
        return _tester_context_reply(message, output)
    if stage == 7:
        return _validation_context_reply(message, output)
    if stage == 8:
        return _deployment_context_reply(message, output)
    return None


def _stage_agent_role(stage: int) -> str:
    return {
        1: "analyst",
        2: "architect",
        3: "developer",
        4: "database_engineer",
        5: "security_engineer",
        6: "tester",
        7: "analyst_validation",
        8: "deployment",
    }.get(stage, f"stage_{stage}")


def _prompt_payload_format() -> str:
    return str(os.getenv("SYNTHETIX_PROMPT_PAYLOAD_FORMAT", "TOON")).strip().upper()


def _toon_key(key: Any) -> str:
    raw = str(key or "").strip()
    if not raw:
        return "key"
    cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", raw).strip("_")
    return cleaned or "key"


def _toon_atom(value: Any, max_str: int = 220) -> str:
    if value is None:
        return "null"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float)):
        return str(value)
    text = str(value or "").replace("\n", "\\n")
    if len(text) > max_str:
        text = text[:max_str] + "...[t]"
    if re.fullmatch(r"[A-Za-z0-9_.:/-]{1,64}", text):
        return text
    return json.dumps(text, ensure_ascii=True)


def _toon_compact(value: Any, max_chars: int = 3200, max_items: int = 8) -> str:
    lines: list[str] = []

    def emit(obj: Any, depth: int, key: str | None = None):
        if len("\n".join(lines)) >= max_chars:
            return
        indent = "  " * depth
        kprefix = _toon_key(key) if key else ""
        if isinstance(obj, dict):
            if kprefix:
                lines.append(f"{indent}{kprefix}:")
                indent = "  " * (depth + 1)
            rows = list(obj.items())
            for k, v in rows[:max_items]:
                emit(v, depth + (1 if kprefix else 0), str(k))
                if len("\n".join(lines)) >= max_chars:
                    return
            if len(rows) > max_items:
                lines.append(f"{indent}_truncated_keys={len(rows) - max_items}")
            return
        if isinstance(obj, list):
            if kprefix:
                lines.append(f"{indent}{kprefix}:")
                indent = "  " * (depth + 1)
            rows = obj[:max_items]
            for item in rows:
                if isinstance(item, dict):
                    pair_bits: list[str] = []
                    for ik, iv in list(item.items())[:5]:
                        if isinstance(iv, (dict, list, tuple)):
                            pair_bits.append(f"{_toon_key(ik)}=<nested>")
                        else:
                            pair_bits.append(f"{_toon_key(ik)}={_toon_atom(iv)}")
                    lines.append(f"{indent}- " + "; ".join(pair_bits))
                elif isinstance(item, list):
                    scalar = [x for x in item[:6] if not isinstance(x, (dict, list, tuple))]
                    joined = ",".join(_toon_atom(x, max_str=80) for x in scalar)
                    lines.append(f"{indent}- [{joined}]")
                else:
                    lines.append(f"{indent}- {_toon_atom(item)}")
                if len("\n".join(lines)) >= max_chars:
                    return
            if len(obj) > max_items:
                lines.append(f"{indent}_truncated_items={len(obj) - max_items}")
            return
        if kprefix:
            lines.append(f"{indent}{kprefix}={_toon_atom(obj)}")
        else:
            lines.append(f"{indent}{_toon_atom(obj)}")

    emit(value, 0, None)
    text = "\n".join(lines).strip() or "<empty>"
    if len(text) <= max_chars:
        return text
    return text[:max_chars] + " ...[truncated]"


def _json_compact(value: Any, max_chars: int = 3200) -> str:
    if _prompt_payload_format() == "JSON":
        try:
            text = json.dumps(value, ensure_ascii=True, separators=(",", ":"), default=str)
        except Exception:
            text = str(value)
        if len(text) <= max_chars:
            return text
        return text[:max_chars] + " ...[truncated]"
    return _toon_compact(value, max_chars=max_chars)


def _resolve_stage_chat_llm_options(llm_options: dict[str, Any] | None) -> dict[str, Any]:
    options = llm_options if isinstance(llm_options, dict) else {}
    enabled = options.get("enabled", True)
    provider = str(options.get("provider", "")).strip().lower()
    model = str(options.get("model", "")).strip()
    temperature_raw = options.get("temperature", 0.2)
    try:
        temperature = float(temperature_raw)
    except (TypeError, ValueError):
        temperature = 0.2
    if temperature < 0:
        temperature = 0.0
    if temperature > 1:
        temperature = 1.0
    return {
        "enabled": bool(enabled),
        "provider": provider,
        "model": model,
        "temperature": temperature,
    }


def _stage_chat_system_prompt(stage: int, state: dict[str, Any]) -> str:
    persona_row = (
        state.get("agent_personas", {}).get(str(stage), {})
        if isinstance(state.get("agent_personas", {}), dict)
        else {}
    )
    persona_name = str(persona_row.get("display_name", "")).strip() or _stage_agent_name(stage)
    persona_text = str(persona_row.get("persona", "")).strip()
    stage_goal = {
        1: "Answer questions using the detailed analyst report and raw evidence artifacts; keep responses grounded in extracted facts.",
        2: "Design architecture options, dependency impact, and boundary-safe decisions.",
        3: "Explain implementation scope, generated components/files, and remediation plan.",
        6: "Explain test outcomes, failures, quality gate implications, and concrete fixes.",
    }.get(stage, "Explain stage output and next concrete actions.")
    base = [
        f"You are the {persona_name}.",
        "Respond with concise, practical engineering guidance tied to the current stage artifact.",
        "Do not invent missing facts; explicitly say when artifact data is absent.",
        "Use plain text, no markdown tables, and keep output under 220 words.",
        stage_goal,
    ]
    if persona_text:
        base.append("Persona directives:")
        base.append(persona_text[:1200])
    return "\n".join(base)


def _maybe_llm_stage_chat_response(
    *,
    run_id: str,
    stage: int,
    message: str,
    state: dict[str, Any],
    summary: str,
    contextual: str,
    constraints: list[dict[str, Any]],
    prior: list[dict[str, Any]],
    llm_options: dict[str, Any] | None,
) -> tuple[str | None, dict[str, Any]]:
    meta: dict[str, Any] = {
        "used": False,
        "provider": "",
        "model": "",
        "reason": "",
    }
    if stage not in STAGE_CHAT_LLM_STAGES:
        meta["reason"] = "stage_not_enabled"
        return None, meta

    opts = _resolve_stage_chat_llm_options(llm_options)
    if not opts.get("enabled", True):
        meta["reason"] = "disabled"
        return None, meta

    provider = str(opts.get("provider", "")).strip().lower()
    requested_model = str(opts.get("model", "")).strip()
    if provider not in {"anthropic", "openai"}:
        provider = str(
            SETTINGS_STORE.get_settings().get("llm", {}).get("default_provider", "anthropic")
        ).strip().lower()
    if provider not in {"anthropic", "openai"}:
        provider = "anthropic"

    try:
        creds = SETTINGS_STORE.resolve_llm_credentials(provider, requested_model=requested_model)
    except Exception as exc:
        meta["reason"] = f"credential_resolution_failed:{exc}"
        return None, meta

    api_key = str(creds.get("api_key", "")).strip()
    if not api_key:
        meta["reason"] = "no_api_key"
        return None, meta
    model = str(creds.get("model", "")).strip() or ("gpt-4o" if provider == "openai" else "claude-sonnet-4-20250514")

    cfg = PipelineConfig(
        provider=LLMProvider.OPENAI if provider == "openai" else LLMProvider.ANTHROPIC,
        anthropic_api_key=api_key if provider == "anthropic" else "",
        openai_api_key=api_key if provider == "openai" else "",
        anthropic_model=model if provider == "anthropic" else "claude-sonnet-4-20250514",
        openai_model=model if provider == "openai" else "gpt-4o",
        temperature=float(opts.get("temperature", 0.2) or 0.2),
        max_output_tokens=1200,
    )
    client = LLMClient(cfg)

    output = _stage_output_snapshot(state, stage)
    doc_rag_context = _doc_rag_context_for_chat(run_id, message) if stage == 1 else ""
    constraints_text = "; ".join(
        [
            f"{str(row.get('priority', 'medium')).upper()} {str(row.get('text', '')).strip()[:140]}"
            for row in constraints[:4]
            if isinstance(row, dict) and str(row.get("text", "")).strip()
        ]
    ) or "none"
    thread_lines: list[str] = []
    for row in prior[-8:]:
        if not isinstance(row, dict):
            continue
        role = str(row.get("role", "")).strip().lower()
        if role not in {"user", "assistant"}:
            continue
        msg = str(row.get("message", "")).strip()
        if not msg:
            continue
        thread_lines.append(f"{role}: {msg[:180]}")
    thread_text = "\n".join(thread_lines) if thread_lines else "none"

    system_prompt = _stage_chat_system_prompt(stage, state)
    user_prompt = (
        f"Run ID: {run_id}\n"
        f"Stage: {stage} ({_stage_agent_name(stage)})\n"
        f"Latest stage summary: {summary or 'none'}\n"
        f"Deterministic context draft: {contextual}\n"
        f"Retrieved document context: {doc_rag_context or 'none'}\n"
        f"Stored constraints: {constraints_text}\n"
        f"Recent thread:\n{thread_text}\n"
        f"Stage artifact snapshot ({_prompt_payload_format()}): {_json_compact(output, max_chars=3600)}\n"
        f"User message: {message}\n"
        "Respond directly to the user request with concrete guidance and next actions."
    )

    try:
        response = client.invoke(system_prompt=system_prompt, user_message=user_prompt)
    except Exception as exc:
        meta["reason"] = f"llm_invoke_failed:{exc}"
        return None, meta

    content = str(response.content or "").strip()
    if not content:
        meta["reason"] = "empty_response"
        return None, meta
    meta["used"] = True
    meta["provider"] = str(response.provider or provider)
    meta["model"] = str(response.model or model)
    meta["reason"] = "ok"
    return content[:4000], meta


def _build_stage_memory_response(
    *,
    run_id: str,
    stage: int,
    message: str,
    state: dict[str, Any],
    bucket: dict[str, Any],
    directive_created: dict[str, Any] | None,
    proposal_created: dict[str, Any] | None,
    llm_options: dict[str, Any] | None = None,
) -> str:
    scope = _stage_memory_scope(run_id, state, stage)
    thread_id = _stage_thread_id(run_id, state, bucket, stage)
    role = _stage_agent_role(stage)
    output = _stage_output_snapshot(state, stage)
    _, result = _stage_latest_result(state, stage)
    summary = str(result.get("summary", "")).strip() if isinstance(result, dict) else ""
    prior = TENANT_MEMORY_STORE.get_thread(scope, thread_id=thread_id, limit=12)
    constraints = TENANT_MEMORY_STORE.search_constraints(scope, message, limit=6)
    fingerprint = _stage_memory_fingerprint(stage, state, message)
    memory_items = TENANT_MEMORY_STORE.search_memory_items(
        scope,
        query=message,
        fingerprint=fingerprint,
        limit=6,
        statuses=["approved"],
    )
    bucket["memory_fingerprint"] = fingerprint
    bucket["memory_applied"] = memory_items[:20]

    TENANT_MEMORY_STORE.append_thread_message(
        scope,
        thread_id=thread_id,
        agent_role=role,
        role="user",
        message=message,
        metadata={"stage": stage, "run_id": run_id, "source": "stage_chat"},
    )

    contextual = _stage_context_reply(stage, message, output, state)
    if stage == 1 and _is_analyst_section_brief_question(message):
        doc_sections = _analyst_document_sections_reply(
            run_id=run_id,
            message=message,
            output=output,
            state=state,
        )
        if doc_sections:
            contextual = doc_sections
    if not contextual:
        contextual = _generic_stage_reply(stage, output, summary)
    if not contextual:
        contextual = "No completed artifact exists yet for this stage. Run or rerun this stage, then ask again for a detailed breakdown."

    if stage == 1 and _is_simple_analyst_count_question(message):
        concise = f"{_stage_agent_name(stage)} response: {contextual}"
        if memory_items:
            concise += f" Applied memory items: {len(memory_items)}."
        if directive_created:
            concise += " Saved as a persistent directive for downstream stages."
        if proposal_created:
            concise += " Created a proposed artifact change for review."
        TENANT_MEMORY_STORE.append_thread_message(
            scope,
            thread_id=thread_id,
            agent_role=role,
            role="assistant",
            message=concise,
            metadata={"stage": stage, "run_id": run_id, "source": "stage_chat"},
        )
        bucket["llm_chat"] = {"used": False, "provider": "", "model": "", "reason": "simple_count_shortcut"}
        return concise

    if stage == 1 and _is_analyst_section_brief_question(message):
        concise = f"{_stage_agent_name(stage)} response: {contextual}"
        if memory_items:
            concise += f" Applied memory items: {len(memory_items)}."
        if directive_created:
            concise += " Saved as a persistent directive for downstream stages."
        if proposal_created:
            concise += " Created a proposed artifact change for review."
        TENANT_MEMORY_STORE.append_thread_message(
            scope,
            thread_id=thread_id,
            agent_role=role,
            role="assistant",
            message=concise,
            metadata={"stage": stage, "run_id": run_id, "source": "stage_chat"},
        )
        bucket["llm_chat"] = {"used": False, "provider": "", "model": "", "reason": "section_brief_shortcut"}
        return concise

    lines = [f"{_stage_agent_name(stage)} response:", contextual]
    if constraints:
        constraint_lines = [
            f"{str(c.get('priority', 'medium')).upper()} {str(c.get('text', '')).strip()[:120]}"
            for c in constraints[:3]
            if isinstance(c, dict) and str(c.get("text", "")).strip()
        ]
        if constraint_lines:
            lines.append("Relevant stored constraints: " + "; ".join(constraint_lines) + ".")
    if memory_items:
        memory_lines = []
        for row in memory_items[:3]:
            if not isinstance(row, dict):
                continue
            title = str(row.get("title", "")).strip()
            statement = str(row.get("statement", "")).strip()
            match = row.get("match", {}) if isinstance(row.get("match", {}), dict) else {}
            hints = ", ".join(
                [str(x).strip() for x in match.get("hints", [])[:2] if str(x).strip()]
            )
            detail = title or statement
            if not detail:
                continue
            if hints:
                memory_lines.append(f"{detail[:90]} ({hints})")
            else:
                memory_lines.append(detail[:90])
        if memory_lines:
            lines.append("Applied memory items: " + "; ".join(memory_lines) + ".")
    if prior:
        previous_user = [
            str(row.get("message", "")).strip()
            for row in prior
            if isinstance(row, dict) and str(row.get("role", "")).strip().lower() == "user"
        ]
        if previous_user:
            lines.append(f"Thread memory: I retained {len(prior)} recent messages for this stage thread.")
            lines.append(f"Previous user topic: {previous_user[-1][:140]}")
    if directive_created:
        lines.append("Saved as a persistent directive for downstream stages.")
    if proposal_created:
        lines.append("Created a proposed artifact change. Review it in Proposed Changes and approve to apply.")

    hint_map = {
        2: "Ask for architecture option tradeoffs, boundary impacts, or diagram interpretation.",
        3: "Ask for file-level implementation scope, component split, or code artifact location.",
        4: "Ask for migration script details, schema impact, or rollback steps.",
        5: "Ask for threat rationale, control mapping, or security gate implications.",
        6: "Ask for failed-test root cause, rerun strategy, or missing test additions.",
        7: "Ask for requirement-to-validation coverage or unmet acceptance criteria.",
        8: "Ask for deployment plan, health checks, rollback, or runtime verification.",
    }
    lines.append(hint_map.get(stage, "Ask follow-up questions to refine this stage artifact."))
    deterministic_message = " ".join([line for line in lines if line]).strip()
    llm_message, llm_meta = _maybe_llm_stage_chat_response(
        run_id=run_id,
        stage=stage,
        message=message,
        state=state,
        summary=summary,
        contextual=contextual,
        constraints=constraints,
        prior=prior,
        llm_options=llm_options,
    )
    if llm_message:
        assistant_message = llm_message
        if directive_created:
            assistant_message += " Saved as a persistent directive for downstream stages."
        if proposal_created:
            assistant_message += " Created a proposed artifact change for review."
    else:
        assistant_message = deterministic_message
    bucket["llm_chat"] = llm_meta

    TENANT_MEMORY_STORE.append_thread_message(
        scope,
        thread_id=thread_id,
        agent_role=role,
        role="assistant",
        message=assistant_message,
        metadata={"stage": stage, "run_id": run_id, "source": "stage_chat"},
    )
    return assistant_message


def _assistant_response_for_stage(
    stage: int,
    message: str,
    state: dict[str, Any],
    directive_saved: bool,
    proposal_created: bool,
) -> str:
    hints = {
        1: "Analyst updates will feed the requirements pack and downstream traceability.",
        2: "Architect updates should preserve SCM boundaries unless explicitly approved.",
        3: "Developer updates should align with convention profile and existing repo patterns.",
        4: "Database updates should include migration safety and rollback considerations.",
        5: "Security updates should map to controls and verification gates.",
        6: "Tester updates should map scenarios to executable checks.",
        7: "Validation updates should map directly to acceptance criteria.",
        8: "Deployment updates should include rollout, health checks, and rollback conditions.",
    }
    output = _stage_output_snapshot(state, stage)
    _, result = _stage_latest_result(state, stage)
    summary = str(result.get("summary", "")) if isinstance(result, dict) else ""

    contextual: str | None = None
    if stage == 1:
        contextual = _analyst_context_reply(message, output, state)
    if not contextual:
        contextual = _generic_stage_reply(stage, output, summary)

    response = [f"{_stage_agent_name(stage)} received your request."]
    if contextual:
        response.append(contextual)
    if directive_saved:
        response.append("Saved as a persistent directive for downstream stages.")
    if proposal_created:
        response.append("Created a proposed artifact change. Review it in Proposed Changes and approve to apply.")
    response.append(hints.get(stage, "Review evidence and decisions before proceeding to the next stage."))
    if not directive_saved and not proposal_created:
        response.append("No structured change was created from this message.")
    response.append(f"Latest request: {message[:220]}")
    return " ".join(response)


def _load_mutable_run(run_id: str) -> tuple[RunRecord | None, dict[str, Any] | None, dict[int, str], list[str], str, str]:
    active = MANAGER._get_record(run_id)
    if active and isinstance(active.pipeline_state, dict):
        return (
            active,
            active.pipeline_state,
            active.stage_status,
            active.progress_logs,
            active.status,
            active.error_message or "",
        )

    persisted = RUN_STORE.load_run(run_id)
    if not persisted:
        return None, None, {}, [], "", ""
    pipeline_state = persisted.get("pipeline_state", {})
    if not isinstance(pipeline_state, dict):
        pipeline_state = {}
    raw_stage_status = persisted.get("stage_status", {})
    stage_status = {
        int(k): str(v) for k, v in raw_stage_status.items() if str(k).isdigit()
    } if isinstance(raw_stage_status, dict) else {}
    progress_logs = list(persisted.get("progress_logs", [])) if isinstance(persisted.get("progress_logs", []), list) else []
    status = str(persisted.get("pipeline_status", "completed"))
    error_message = str(persisted.get("error_message", "") or "")
    return None, pipeline_state, stage_status, progress_logs, status, error_message


def _commit_mutable_run(
    run_id: str,
    active: RunRecord | None,
    pipeline_state: dict[str, Any],
    stage_status: dict[int, str],
    progress_logs: list[str],
    status: str,
    error_message: str,
) -> dict[str, Any] | None:
    if active:
        active.pipeline_state = pipeline_state
        active.stage_status = stage_status
        active.progress_logs = progress_logs
        active.updated_at = _utc_now()
        MANAGER._persist(active)
        return MANAGER._record_payload(active)

    RUN_STORE.finalize_run(
        run_id=run_id,
        status=status or "completed",
        pipeline_state=pipeline_state,
        stage_status=stage_status,
        progress_logs=progress_logs,
        error_message=error_message or None,
    )
    return MANAGER.get_run(run_id)


async def api_get_stage_collaboration(request):
    run_id = request.path_params.get("run_id", "")
    stage = _coerce_stage(request.path_params.get("stage", 0))
    if stage < 1 or stage > TOTAL_STAGES:
        return JSONResponse({"ok": False, "error": "stage must be between 1 and 8"}, status_code=400)
    run = MANAGER.get_run(run_id)
    if not run:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)
    pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
    collaboration = _stage_collaboration_view(pipeline_state, stage)
    return JSONResponse({"ok": True, "run_id": run_id, "stage": stage, "collaboration": collaboration, "run": run})


async def api_stage_chat(request):
    run_id = request.path_params.get("run_id", "")
    stage = _coerce_stage(request.path_params.get("stage", 0))
    if stage < 1 or stage > TOTAL_STAGES:
        return JSONResponse({"ok": False, "error": "stage must be between 1 and 8"}, status_code=400)

    payload = _get_json(await request.body())
    message = str(payload.get("message", "")).strip()
    if not message:
        return JSONResponse({"ok": False, "error": "message is required"}, status_code=400)
    save_as_directive = bool(payload.get("save_as_directive", False))
    propose_change = bool(payload.get("propose_change", False))
    actor = _request_actor(request)
    llm_payload = payload.get("llm", {}) if isinstance(payload.get("llm", {}), dict) else {}
    llm_options = {
        "enabled": llm_payload.get("enabled", True),
        "provider": str(llm_payload.get("provider", payload.get("provider", ""))).strip(),
        "model": str(llm_payload.get("model", payload.get("model", ""))).strip(),
        "temperature": llm_payload.get("temperature", payload.get("temperature", 0.2)),
    }

    active, pipeline_state, stage_status, progress_logs, status, error_message = _load_mutable_run(run_id)
    if pipeline_state is None:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    bucket = _stage_collaboration_bucket(pipeline_state, stage, create=True)
    chat_rows = list(bucket.get("chat", [])) if isinstance(bucket.get("chat", []), list) else []
    user_chat = {
        "id": _next_collab_id("chat"),
        "role": "user",
        "stage": stage,
        "created_at": _utc_now(),
        "created_by": actor or "user",
        "message": message[:4000],
    }
    chat_rows.append(user_chat)

    directive_created: dict[str, Any] | None = None
    if save_as_directive or "save as directive" in message.lower() or message.lower().startswith("directive:"):
        directive_created = _extract_directive_from_message(stage, message, actor)
        if directive_created:
            directives = list(bucket.get("directives", [])) if isinstance(bucket.get("directives", []), list) else []
            directives.append(directive_created)
            bucket["directives"] = directives[-STAGE_COLLAB_DIRECTIVE_LIMIT:]
            global_directives = (
                list(pipeline_state.get("global_directives", []))
                if isinstance(pipeline_state.get("global_directives", []), list)
                else []
            )
            global_directives.append(directive_created)
            pipeline_state["global_directives"] = global_directives[-600:]

    proposal_created: dict[str, Any] | None = None
    if propose_change:
        proposal_created = _auto_patch_from_message(stage, message, pipeline_state, actor)
        if proposal_created:
            proposals = list(bucket.get("proposals", [])) if isinstance(bucket.get("proposals", []), list) else []
            proposals.append(proposal_created)
            bucket["proposals"] = proposals[-STAGE_COLLAB_PROPOSAL_LIMIT:]

    memory_scope = _stage_memory_scope(run_id, pipeline_state, stage)
    memory_fingerprint = _stage_memory_fingerprint(stage, pipeline_state, message)
    memory_tier = str(payload.get("promote_to", payload.get("memory_tier", "work_item"))).strip().lower()
    if memory_tier not in {"run", "work_item", "project", "client", "firm_pattern"}:
        memory_tier = "work_item"
    enforcement_payload = payload.get("memory_enforcement", {})
    enforcement = {
        "agent_stages": [str(stage)],
        "gate_level": "warn",
        "checks": [],
    }
    if isinstance(enforcement_payload, dict):
        gate_level = str(enforcement_payload.get("gate_level", "warn")).strip().lower()
        if gate_level in {"warn", "block"}:
            enforcement["gate_level"] = gate_level
        checks = enforcement_payload.get("checks", [])
        if isinstance(checks, list):
            enforcement["checks"] = [str(x).strip() for x in checks if str(x).strip()]
        stages = enforcement_payload.get("agent_stages", [])
        if isinstance(stages, list):
            stage_rows = [str(x).strip() for x in stages if str(x).strip()]
            if stage_rows:
                enforcement["agent_stages"] = stage_rows
    memory_item_created: dict[str, Any] | None = None
    review_candidate_created: dict[str, Any] | None = None
    bucket["memory_fingerprint"] = memory_fingerprint
    bucket["memory_applied"] = TENANT_MEMORY_STORE.search_memory_items(
        memory_scope,
        query=message,
        fingerprint=memory_fingerprint,
        limit=6,
        statuses=["approved"],
    )
    if directive_created:
        statement = str(directive_created.get("text", "")).strip()
        if statement:
            try:
                memory_item_created = TENANT_MEMORY_STORE.add_memory_item(
                    memory_scope,
                    item_type="constraint",
                    title=f"{_stage_agent_name(stage)} directive",
                    statement=statement,
                    created_by=actor,
                    source="stage_chat",
                    tier=memory_tier,
                    applies_when=memory_fingerprint,
                    enforcement=enforcement,
                    evidence_refs=[
                        {"type": "run", "ref": run_id},
                        {"type": "stage", "ref": str(stage)},
                        {"type": "directive", "ref": str(directive_created.get("id", ""))},
                    ],
                    metadata={
                        "stage": stage,
                        "run_id": run_id,
                        "directive_id": str(directive_created.get("id", "")),
                    },
                )
                directive_created["memory_item_id"] = memory_item_created.get("id", "")
            except Exception as exc:
                _append_collab_log(progress_logs, f"Stage {stage} memory save failed: {exc}")
        if memory_item_created and memory_tier in {"client", "firm_pattern"}:
            try:
                review_candidate_created = TENANT_MEMORY_STORE.add_review_candidate(
                    memory_scope,
                    summary=f"Promote directive to {memory_tier}: {statement[:140]}",
                    source="stage_chat",
                    created_by=actor,
                    proposed_item={
                        "type": "constraint",
                        "title": f"{_stage_agent_name(stage)} directive",
                        "statement": statement,
                        "tier": memory_tier,
                        "applies_when": memory_fingerprint,
                        "enforcement": enforcement,
                    },
                    patch=[],
                    evidence_refs=[
                        {"type": "run", "ref": run_id},
                        {"type": "directive", "ref": str(directive_created.get("id", ""))},
                    ],
                    metadata={"stage": stage},
                )
                directive_created["review_candidate_id"] = review_candidate_created.get("id", "")
            except Exception as exc:
                _append_collab_log(progress_logs, f"Stage {stage} review queue add failed: {exc}")
    if proposal_created:
        try:
            proposal_statement = str(proposal_created.get("summary", "")).strip() or str(
                proposal_created.get("title", "")
            ).strip()
            proposal_candidate = TENANT_MEMORY_STORE.add_review_candidate(
                memory_scope,
                summary=f"Capture stage {stage} proposal learning: {proposal_statement[:140]}",
                source="stage_chat",
                created_by=actor,
                proposed_item={
                    "type": "correction",
                    "title": str(proposal_created.get("title", "")).strip() or f"Stage {stage} correction",
                    "statement": proposal_statement or "Stage collaboration correction",
                    "tier": memory_tier,
                    "applies_when": memory_fingerprint,
                    "enforcement": enforcement,
                },
                patch=proposal_created.get("patch", []) if isinstance(proposal_created.get("patch", []), list) else [],
                evidence_refs=[
                    {"type": "run", "ref": run_id},
                    {"type": "proposal", "ref": str(proposal_created.get("id", ""))},
                ],
                metadata={"stage": stage, "proposal_id": str(proposal_created.get("id", ""))},
            )
            proposal_created["review_candidate_id"] = proposal_candidate.get("id", "")
            if not review_candidate_created:
                review_candidate_created = proposal_candidate
        except Exception as exc:
            _append_collab_log(progress_logs, f"Stage {stage} proposal review queue failed: {exc}")

    assistant_message = ""
    if stage == 1:
        msg_lower = message.lower()
        analyst_update_intent = (
            save_as_directive
            or propose_change
            or any(
                token in msg_lower
                for token in [
                    "add requirement",
                    "update requirement",
                    "change requirement",
                    "modify requirement",
                    "rewrite requirement",
                    "regenerate requirements",
                    "rebuild requirements",
                    "apply this change",
                    "save as directive",
                ]
            )
        )
        if not analyst_update_intent:
            assistant_message = _build_stage_memory_response(
                run_id=run_id,
                stage=stage,
                message=message,
                state=pipeline_state,
                bucket=bucket,
                directive_created=directive_created,
                proposal_created=proposal_created,
                llm_options=llm_options,
            )
        else:
            aas_failed = False
            aas_error = ""
            try:
                integration_context = (
                    pipeline_state.get("integration_context", {})
                    if isinstance(pipeline_state.get("integration_context", {}), dict)
                    else {}
                )
                domain_pack_id = str(integration_context.get("domain_pack_id", "")).strip()
                custom_domain_pack = integration_context.get("custom_domain_pack")
                jurisdiction = str(integration_context.get("jurisdiction", "")).strip()
                data_classes = integration_context.get("data_classification", [])
                stage_persona = (
                    pipeline_state.get("agent_personas", {}).get("1", {})
                    if isinstance(pipeline_state.get("agent_personas", {}), dict)
                    else {}
                )
                thread_id = str(bucket.get("thread_id", "")).strip() or str(
                    pipeline_state.get("analyst_aas_thread_id", "")
                ).strip() or f"run-{run_id}-stage1"
                brownfield_ctx = (
                    integration_context.get("brownfield", {})
                    if isinstance(integration_context.get("brownfield", {}), dict)
                    else {}
                )
                repo_hint = str(brownfield_ctx.get("repo_url", "")).strip()
                repo_slug = re.sub(r"[^a-z0-9]+", "-", repo_hint.lower()).strip("-")[:80] if repo_hint else ""
                project_id = repo_slug or f"run-{run_id}"
                save_constraints: list[dict[str, Any]] = []
                if directive_created:
                    save_constraints.append(
                        {
                            "text": str(directive_created.get("text", "")).strip(),
                            "priority": str(directive_created.get("priority", "medium")),
                            "applies_to": "all",
                        }
                    )
                aas_payload: dict[str, Any] = {
                    "requirement": message,
                    "business_objective": message,
                    "use_case": str(pipeline_state.get("use_case", "business_objectives")),
                    "thread_id": thread_id,
                    "workspace_id": str(pipeline_state.get("workspace_id", "default-workspace")),
                    "client_id": str(pipeline_state.get("client_id", "default-client")),
                    "project_id": project_id,
                    "integration_context": integration_context,
                    "context_bundle": pipeline_state.get("context_bundle", {}),
                    "system_context_model": pipeline_state.get("system_context_model", {}),
                    "convention_profile": pipeline_state.get("convention_profile", {}),
                    "health_assessment_bundle": pipeline_state.get("health_assessment_bundle", {}),
                    "save_constraints": save_constraints,
                }
                if domain_pack_id:
                    aas_payload["domain_pack_id"] = domain_pack_id
                if isinstance(custom_domain_pack, dict) and custom_domain_pack:
                    aas_payload["domain_pack"] = custom_domain_pack
                if jurisdiction:
                    aas_payload["jurisdiction"] = jurisdiction
                if isinstance(data_classes, list) and data_classes:
                    aas_payload["data_classification"] = data_classes
                persona_id = str(stage_persona.get("agent_id", "")).strip()
                if persona_id:
                    aas_payload["persona_id"] = persona_id

                aas_result = ANALYST_AAS.analyze(aas_payload, actor=actor)
                assistant_message = str(aas_result.get("assistant_summary", "")).strip() or "Analyst update completed."
                aas_thread_id = str(aas_result.get("thread_id", "")).strip() or thread_id
                bucket["thread_id"] = aas_thread_id
                bucket["aas_last_result"] = {
                    "run_id": str(aas_result.get("run_id", "")).strip(),
                    "generated_at": str(aas_result.get("generated_at", "")).strip(),
                    "warnings": aas_result.get("warnings", []),
                    "errors": aas_result.get("errors", []),
                    "quality_gates": aas_result.get("quality_gates", []),
                }
                pipeline_state["analyst_aas_thread_id"] = aas_thread_id

                req_pack = aas_result.get("requirements_pack", {})
                if isinstance(req_pack, dict) and req_pack:
                    current_output = _stage_output_snapshot(pipeline_state, stage)
                    merged_output = dict(current_output) if isinstance(current_output, dict) else {}
                    merged_output["requirements_pack"] = req_pack
                    merged_output["domain_pack"] = aas_result.get("domain_pack", {})
                    merged_output["quality_gates"] = aas_result.get("quality_gates", [])
                    merged_output["open_questions"] = (
                        req_pack.get("open_questions", [])
                        if isinstance(req_pack.get("open_questions", []), list)
                        else merged_output.get("open_questions", [])
                    )
                    merged_output["assistant_summary"] = assistant_message
                    if isinstance(aas_result.get("trace", []), list):
                        merged_output["trace"] = aas_result.get("trace", [])
                    merged_output = _ensure_analyst_report_v2(merged_output)
                    _set_stage_output(
                        pipeline_state,
                        stage,
                        merged_output,
                        summary="Analyst requirements pack updated via stage collaboration chat",
                    )
            except Exception as exc:
                aas_failed = True
                aas_error = str(exc)

            if aas_failed:
                assistant_message = _assistant_response_for_stage(
                    stage,
                    message,
                    pipeline_state,
                    directive_saved=directive_created is not None,
                    proposal_created=proposal_created is not None,
                )
                if aas_error:
                    assistant_message += f" Analyst AAS fallback reason: {aas_error[:220]}"
            else:
                if directive_created:
                    assistant_message += " Saved as a persistent directive for downstream stages."
                if proposal_created:
                    assistant_message += " Proposed artifact change queued; review it in Proposed Changes."
    else:
        assistant_message = _build_stage_memory_response(
            run_id=run_id,
            stage=stage,
            message=message,
            state=pipeline_state,
            bucket=bucket,
            directive_created=directive_created,
            proposal_created=proposal_created,
            llm_options=llm_options,
        )
    assistant_chat = {
        "id": _next_collab_id("chat"),
        "role": "assistant",
        "stage": stage,
        "created_at": _utc_now(),
        "created_by": "agent",
        "message": assistant_message,
    }
    chat_rows.append(assistant_chat)
    bucket["chat"] = chat_rows[-STAGE_COLLAB_CHAT_LIMIT:]
    bucket["updated_at"] = _utc_now()
    bucket["evidence"] = _extract_stage_evidence(pipeline_state, stage)

    _append_collab_log(progress_logs, f"Stage {stage} collaboration message recorded")
    if stage != 1 and isinstance(bucket.get("llm_chat", {}), dict):
        llm_meta = bucket.get("llm_chat", {})
        if llm_meta.get("used"):
            _append_collab_log(
                progress_logs,
                f"Stage {stage} LLM chat response generated ({llm_meta.get('provider', '')}/{llm_meta.get('model', '')})",
            )
        else:
            reason = str(llm_meta.get("reason", "")).strip()
            if reason and reason != "stage_not_enabled":
                _append_collab_log(progress_logs, f"Stage {stage} LLM chat fallback: {reason}")
    if directive_created:
        _append_collab_log(progress_logs, f"Stage {stage} directive saved: {directive_created.get('id')}")
        if memory_item_created:
            _append_collab_log(progress_logs, f"Stage {stage} memory item saved: {memory_item_created.get('id')}")
    if proposal_created:
        _append_collab_log(progress_logs, f"Stage {stage} proposal queued: {proposal_created.get('id')}")
    if review_candidate_created:
        _append_collab_log(
            progress_logs,
            f"Stage {stage} memory review candidate queued: {review_candidate_created.get('id')}",
        )
    if memory_item_created:
        assistant_message += f" Memory item saved ({memory_item_created.get('id', '')})."
    if review_candidate_created:
        assistant_message += (
            f" Learning review candidate queued ({review_candidate_created.get('id', '')}) for approval."
        )

    updated_run = _commit_mutable_run(
        run_id=run_id,
        active=active,
        pipeline_state=pipeline_state,
        stage_status=stage_status,
        progress_logs=progress_logs,
        status=status,
        error_message=error_message,
    )
    collaboration = _stage_collaboration_view(pipeline_state, stage)
    return JSONResponse(
        {
            "ok": True,
            "run_id": run_id,
            "stage": stage,
            "assistant_message": assistant_message,
            "directive": directive_created,
            "proposal": proposal_created,
            "memory_item": memory_item_created,
            "review_candidate": review_candidate_created,
            "collaboration": collaboration,
            "run": updated_run,
        }
    )


async def api_stage_create_proposal(request):
    run_id = request.path_params.get("run_id", "")
    stage = _coerce_stage(request.path_params.get("stage", 0))
    if stage < 1 or stage > TOTAL_STAGES:
        return JSONResponse({"ok": False, "error": "stage must be between 1 and 8"}, status_code=400)

    payload = _get_json(await request.body())
    actor = _request_actor(request)
    title = str(payload.get("title", "")).strip() or "Manual proposal"
    summary = str(payload.get("summary", "")).strip() or "Manual stage output change"
    raw_patch = payload.get("patch")
    if not isinstance(raw_patch, list):
        raw_patch = [
            {
                "op": str(payload.get("op", "")).strip().lower(),
                "path": str(payload.get("path", "")).strip(),
                "value": payload.get("value"),
            }
        ]
    try:
        patch = _normalize_patch_ops(raw_patch)
    except ValueError as exc:
        return JSONResponse({"ok": False, "error": str(exc)}, status_code=400)

    active, pipeline_state, stage_status, progress_logs, status, error_message = _load_mutable_run(run_id)
    if pipeline_state is None:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    bucket = _stage_collaboration_bucket(pipeline_state, stage, create=True)
    proposals = list(bucket.get("proposals", [])) if isinstance(bucket.get("proposals", []), list) else []
    proposal = {
        "id": _next_collab_id("prop"),
        "stage": stage,
        "agent_name": _stage_agent_name(stage),
        "title": title[:140],
        "summary": summary[:300],
        "status": "pending",
        "source": "manual",
        "created_at": _utc_now(),
        "created_by": actor or "user",
        "confidence": 1.0,
        "patch": patch,
    }
    proposals.append(proposal)
    bucket["proposals"] = proposals[-STAGE_COLLAB_PROPOSAL_LIMIT:]
    bucket["updated_at"] = _utc_now()
    bucket["evidence"] = _extract_stage_evidence(pipeline_state, stage)
    _append_collab_log(progress_logs, f"Stage {stage} manual proposal created: {proposal['id']}")

    updated_run = _commit_mutable_run(
        run_id=run_id,
        active=active,
        pipeline_state=pipeline_state,
        stage_status=stage_status,
        progress_logs=progress_logs,
        status=status,
        error_message=error_message,
    )
    collaboration = _stage_collaboration_view(pipeline_state, stage)
    return JSONResponse({"ok": True, "proposal": proposal, "collaboration": collaboration, "run": updated_run})


async def api_stage_proposal_decision(request):
    run_id = request.path_params.get("run_id", "")
    stage = _coerce_stage(request.path_params.get("stage", 0))
    proposal_id = str(request.path_params.get("proposal_id", "")).strip()
    if stage < 1 or stage > TOTAL_STAGES:
        return JSONResponse({"ok": False, "error": "stage must be between 1 and 8"}, status_code=400)
    if not proposal_id:
        return JSONResponse({"ok": False, "error": "proposal_id is required"}, status_code=400)

    payload = _get_json(await request.body())
    decision = str(payload.get("decision", "reject")).strip().lower()
    if decision not in {"approve", "reject"}:
        return JSONResponse({"ok": False, "error": "decision must be approve or reject"}, status_code=400)
    rationale = str(payload.get("rationale", "")).strip()
    actor = _request_actor(request)

    active, pipeline_state, stage_status, progress_logs, status, error_message = _load_mutable_run(run_id)
    if pipeline_state is None:
        return JSONResponse({"ok": False, "error": "run not found"}, status_code=404)

    bucket = _stage_collaboration_bucket(pipeline_state, stage, create=True)
    proposals = list(bucket.get("proposals", [])) if isinstance(bucket.get("proposals", []), list) else []
    proposal_index = -1
    proposal: dict[str, Any] | None = None
    for idx, item in enumerate(proposals):
        if isinstance(item, dict) and str(item.get("id", "")).strip() == proposal_id:
            proposal_index = idx
            proposal = dict(item)
            break
    if proposal_index < 0 or proposal is None:
        return JSONResponse({"ok": False, "error": "proposal not found"}, status_code=404)

    current_status = str(proposal.get("status", "pending")).strip().lower() or "pending"
    if current_status not in {"pending", "queued"}:
        return JSONResponse({"ok": False, "error": f"proposal already {current_status}"}, status_code=400)

    decision_record = {
        "id": _next_collab_id("dec"),
        "proposal_id": proposal_id,
        "stage": stage,
        "agent_name": _stage_agent_name(stage),
        "decision": decision,
        "rationale": rationale,
        "decided_by": actor or "user",
        "decided_at": _utc_now(),
        "changed_paths": [],
    }

    if decision == "approve":
        stage_status_value = str(stage_status.get(stage, "")).strip().lower()
        if stage_status_value == "running":
            return JSONResponse(
                {"ok": False, "error": "cannot apply proposal while stage is running; pause or wait for stage completion"},
                status_code=409,
            )
        patch = proposal.get("patch", [])
        try:
            normalized_patch = _normalize_patch_ops(patch)
            before = _stage_output_snapshot(pipeline_state, stage)
            after, changed_paths = _apply_json_patch_ops(before, normalized_patch)
        except ValueError as exc:
            return JSONResponse({"ok": False, "error": f"patch application failed: {exc}"}, status_code=400)
        stage_note = f"Human-approved collaboration proposal applied ({proposal_id})"
        _set_stage_output(pipeline_state, stage, after, stage_note)
        decision_record["changed_paths"] = changed_paths
        proposal["status"] = "applied"
        proposal["applied_at"] = _utc_now()
        proposal["applied_by"] = actor or "user"
        proposal["changed_paths"] = changed_paths
        _append_collab_log(progress_logs, f"Stage {stage} proposal applied: {proposal_id}")
    else:
        proposal["status"] = "rejected"
        proposal["rejected_at"] = _utc_now()
        proposal["rejected_by"] = actor or "user"
        _append_collab_log(progress_logs, f"Stage {stage} proposal rejected: {proposal_id}")

    proposals[proposal_index] = proposal
    bucket["proposals"] = proposals[-STAGE_COLLAB_PROPOSAL_LIMIT:]
    decisions = list(bucket.get("decisions", [])) if isinstance(bucket.get("decisions", []), list) else []
    decisions.append(decision_record)
    bucket["decisions"] = decisions[-STAGE_COLLAB_DECISION_LIMIT:]
    bucket["updated_at"] = _utc_now()
    bucket["evidence"] = _extract_stage_evidence(pipeline_state, stage)

    updated_run = _commit_mutable_run(
        run_id=run_id,
        active=active,
        pipeline_state=pipeline_state,
        stage_status=stage_status,
        progress_logs=progress_logs,
        status=status,
        error_message=error_message,
    )
    collaboration = _stage_collaboration_view(pipeline_state, stage)
    return JSONResponse(
        {
            "ok": True,
            "decision": decision_record,
            "proposal": proposal,
            "collaboration": collaboration,
            "run": updated_run,
        }
    )


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
    run_bundle: dict[str, Any] = {}
    run_id = _clean_text(ref.get("run_id"))
    if run_id:
        run = MANAGER.get_run(run_id)
        if isinstance(run, dict):
            pipeline_state = run.get("pipeline_state", {}) if isinstance(run.get("pipeline_state", {}), dict) else {}
            run_bundle = _as_dict_safe(pipeline_state.get("run_context_bundle"))

    return JSONResponse(
        {
            "ok": True,
            "context_reference": ref,
            "context_bundle": bundle,
            "run_context_bundle": run_bundle,
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

        if initial_run.get("status") not in {"running", "queued"}:
            yield _sse_event("done", {"status": initial_run.get("status", "unknown"), "run_id": run_id})
            return

        subscription = MANAGER.subscribe(run_id)
        if not subscription:
            # Run may be executing on another stateless instance. Poll shared store instead of ending stream.
            last_marker = ""
            while True:
                latest = MANAGER.get_run(run_id)
                if not latest:
                    yield _sse_event("done", {"status": "unknown", "run_id": run_id})
                    return
                marker = "|".join(
                    [
                        str(latest.get("status", "")),
                        str(latest.get("updated_at", "")),
                        str(len(latest.get("progress_logs", []) if isinstance(latest.get("progress_logs", []), list) else [])),
                    ]
                )
                if marker != last_marker:
                    last_marker = marker
                    yield _sse_event("update", {"run": latest})
                if latest.get("status") not in {"running", "queued"}:
                    yield _sse_event("done", {"status": latest.get("status", "unknown"), "run_id": run_id})
                    return
                await asyncio.sleep(2.0)

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
    Route("/internal/run-worker", api_internal_run_worker, methods=["POST"]),
    Route("/api/samples", api_samples, methods=["GET"]),
    Route("/api/domain-packs", api_domain_packs, methods=["GET"]),
    Route("/api/legacy-skills", api_legacy_skills, methods=["GET"]),
    Route("/api/agents/personas", api_agent_personas, methods=["GET"]),
    Route("/api/agents/personas", api_agent_persona_upsert, methods=["POST"]),
    Route("/api/agents/analyst/analyze-requirement", api_analyst_aas_analyze, methods=["POST"]),
    Route("/api/memory/constraints", api_memory_add_constraint, methods=["POST"]),
    Route("/api/memory/thread", api_memory_thread, methods=["GET"]),
    Route("/api/memory/items", api_memory_items, methods=["GET", "POST"]),
    Route("/api/memory/items/{item_id:str}/status", api_memory_item_status, methods=["POST"]),
    Route("/api/memory/review-queue", api_memory_review_queue, methods=["GET", "POST"]),
    Route("/api/memory/review-queue/{candidate_id:str}/resolve", api_memory_review_resolve, methods=["POST"]),
    Route("/api/memory/audit", api_memory_audit, methods=["GET"]),
    Route("/api/settings", api_get_settings, methods=["GET"]),
    Route("/api/settings/me", api_get_current_user, methods=["GET"]),
    Route("/api/settings/users", api_upsert_user, methods=["POST"]),
    Route("/api/settings/users/status", api_set_user_status, methods=["POST"]),
    Route("/api/settings/users/remove", api_remove_user, methods=["POST"]),
    Route("/api/settings/knowledge/sources", api_upsert_knowledge_source, methods=["POST"]),
    Route("/api/settings/knowledge/sources/upload", api_upload_knowledge_source, methods=["POST"]),
    Route("/api/settings/knowledge/sources/remove", api_remove_knowledge_source, methods=["POST"]),
    Route("/api/settings/knowledge/sets", api_upsert_knowledge_set, methods=["POST"]),
    Route("/api/settings/knowledge/sets/remove", api_remove_knowledge_set, methods=["POST"]),
    Route("/api/settings/knowledge/brains", api_upsert_agent_brain, methods=["POST"]),
    Route("/api/settings/knowledge/project-bindings", api_upsert_project_binding, methods=["POST"]),
    Route("/api/settings/knowledge/specialists", api_upsert_specialist, methods=["POST"]),
    Route("/api/settings/knowledge/specialists/remove", api_remove_specialist, methods=["POST"]),
    Route("/api/agent-studio/find-relevant-context", api_find_relevant_context, methods=["POST"]),
    Route("/api/agent-studio/suggest-agent", api_suggest_agent, methods=["POST"]),
    Route("/api/settings/integrations/{provider:str}/connect", api_connect_integration, methods=["POST"]),
    Route("/api/settings/integrations/{provider:str}/test", api_test_integration, methods=["POST"]),
    Route("/api/settings/integrations/{provider:str}/disconnect", api_disconnect_integration, methods=["POST"]),
    Route("/api/settings/llm/{provider:str}/connect", api_connect_llm_provider, methods=["POST"]),
    Route("/api/settings/llm/{provider:str}/test", api_test_llm_provider, methods=["POST"]),
    Route("/api/settings/llm/{provider:str}/disconnect", api_disconnect_llm_provider, methods=["POST"]),
    Route("/api/discover/github/tree", api_discover_github_tree, methods=["POST"]),
    Route("/api/discover/access/inspect", api_discover_access_inspect, methods=["POST"]),
    Route("/api/evidence/bundles", api_create_evidence_bundle, methods=["POST"]),
    Route("/api/evidence/bundles/{bundle_id:str}", api_get_evidence_bundle, methods=["GET"]),
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
    Route("/api/teams/duplicate", api_duplicate_team, methods=["POST"]),
    Route("/api/teams/delete", api_delete_team, methods=["POST"]),
    Route("/api/teams/suggest", api_suggest_team, methods=["POST"]),
    Route("/api/teams/{team_id:str}", api_get_team, methods=["GET"]),
    Route("/api/tasks", api_list_tasks, methods=["GET"]),
    Route("/api/tasks/{run_id:str}/clone", api_clone_task, methods=["GET"]),
    Route("/api/work-items", api_list_work_items, methods=["GET"]),
    Route("/api/work-items", api_create_work_item, methods=["POST"]),
    Route("/api/work-items/{item_id:str}/status", api_set_work_item_status, methods=["POST"]),
    Route("/api/runs/preflight", api_run_preflight, methods=["POST"]),
    Route("/api/runs", api_list_runs, methods=["GET"]),
    Route("/api/runs", api_start_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}", api_get_run, methods=["GET"]),
    Route("/api/runs/{run_id:str}/status", api_get_run_status, methods=["GET"]),
    Route("/api/runs/{run_id:str}/logs", api_get_run_logs, methods=["GET"]),
    Route("/api/runs/{run_id:str}/approve", api_approve_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}/pause", api_pause_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}/resume", api_resume_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}/abort", api_abort_run, methods=["POST"]),
    Route("/api/runs/{run_id:str}/rerun", api_rerun_stage, methods=["POST"]),
    Route("/api/runs/{run_id:str}/analyst-doc", api_update_analyst_doc, methods=["POST"]),
    Route("/api/runs/{run_id:str}/analyst-docx", api_download_analyst_docx, methods=["GET"]),
    Route("/api/runs/{run_id:str}/analyst-docgen-docx", api_download_analyst_docgen_docx, methods=["GET"]),
    Route("/api/runs/{run_id:str}/db-artifact", api_download_db_artifact, methods=["GET"]),
    Route("/api/runs/{run_id:str}/discover-artifact", api_download_discover_artifact, methods=["GET"]),
    Route("/api/runs/{run_id:str}/stages/{stage:int}/collaboration", api_get_stage_collaboration, methods=["GET"]),
    Route("/api/runs/{run_id:str}/stages/{stage:int}/collaboration/chat", api_stage_chat, methods=["POST"]),
    Route("/api/runs/{run_id:str}/stages/{stage:int}/collaboration/proposals", api_stage_create_proposal, methods=["POST"]),
    Route(
        "/api/runs/{run_id:str}/stages/{stage:int}/collaboration/proposals/{proposal_id:str}/decision",
        api_stage_proposal_decision,
        methods=["POST"],
    ),
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
