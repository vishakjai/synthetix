"""
Tenant-isolated memory store for conversations and reusable memory artifacts.

Design goals:
- Backward compatible thread + constraint APIs.
- Artifact-first memory items with scope/promotion/applicability metadata.
- Review queue for promoting candidate learnings.
"""

from __future__ import annotations

import json
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


MEMORY_ITEM_TYPES = {
    "constraint",
    "correction",
    "pattern",
    "playbook_choice",
    "glossary",
    "exception",
}
MEMORY_TIERS = {"run", "work_item", "project", "client", "firm_pattern"}
MEMORY_STATUSES = {"proposed", "approved", "deprecated"}
REVIEW_STATUSES = {"pending", "approved", "rejected"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _safe_segment(value: str, default: str) -> str:
    raw = str(value or "").strip().lower()
    cleaned = re.sub(r"[^a-z0-9._-]+", "-", raw).strip("-")
    return cleaned or default


def _tokens(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9_]+", str(text or "").lower()) if token}


def _safe_json_load(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def _safe_json_write(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str))
    tmp.replace(path)


def _normalize_tier(value: Any, default: str = "work_item") -> str:
    tier = str(value or "").strip().lower()
    return tier if tier in MEMORY_TIERS else default


def _normalize_item_type(value: Any, default: str = "constraint") -> str:
    item_type = str(value or "").strip().lower()
    return item_type if item_type in MEMORY_ITEM_TYPES else default


def _default_status_for_tier(tier: str) -> str:
    return "proposed" if tier in {"client", "firm_pattern"} else "approved"


def _normalize_status(value: Any, default: str) -> str:
    status = str(value or "").strip().lower()
    return status if status in MEMORY_STATUSES else default


def _to_list_str(values: Any) -> list[str]:
    if not isinstance(values, list):
        return []
    return [str(v).strip() for v in values if str(v).strip()]


def _normalize_scope(scope: dict[str, Any], tier: str) -> dict[str, Any]:
    return {
        "workspace_id": str(scope.get("workspace_id", "default-workspace")).strip() or "default-workspace",
        "client_id": str(scope.get("client_id", "default-client")).strip() or "default-client",
        "project_id": str(scope.get("project_id", "default-project")).strip() or "default-project",
        "tier": _normalize_tier(tier),
    }


def _scope_matches(active_scope: dict[str, Any], item_scope: dict[str, Any]) -> bool:
    ws = str(active_scope.get("workspace_id", "")).strip()
    client = str(active_scope.get("client_id", "")).strip()
    project = str(active_scope.get("project_id", "")).strip()
    item_ws = str(item_scope.get("workspace_id", "")).strip()
    item_client = str(item_scope.get("client_id", "")).strip()
    item_project = str(item_scope.get("project_id", "")).strip()
    tier = _normalize_tier(item_scope.get("tier", "work_item"))

    if item_ws and ws and item_ws != ws:
        return False

    if tier in {"run", "work_item", "project"}:
        if item_client and client and item_client != client:
            return False
        if item_project and project and item_project != project:
            return False
        return True

    if tier == "client":
        if item_client and client and item_client != client:
            return False
        return True

    # firm_pattern
    return True


def _normalize_applies_when(payload: Any) -> dict[str, Any]:
    if not isinstance(payload, dict):
        return {}
    return {
        "languages": _to_list_str(payload.get("languages", [])),
        "domains": _to_list_str(payload.get("domains", [])),
        "components": _to_list_str(payload.get("components", [])),
        "file_globs": _to_list_str(payload.get("file_globs", [])),
        "legacy_signals": _to_list_str(payload.get("legacy_signals", [])),
        "agent_stages": _to_list_str(payload.get("agent_stages", [])),
        "target_language": str(payload.get("target_language", "")).strip(),
        "use_case": str(payload.get("use_case", "")).strip(),
    }


def _normalize_enforcement(payload: Any) -> dict[str, Any]:
    row = payload if isinstance(payload, dict) else {}
    gate_level = str(row.get("gate_level", "warn")).strip().lower()
    if gate_level not in {"warn", "block"}:
        gate_level = "warn"
    return {
        "agent_stages": _to_list_str(row.get("agent_stages", [])),
        "gate_level": gate_level,
        "checks": _to_list_str(row.get("checks", [])),
    }


def _normalize_evidence_refs(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    rows: list[dict[str, Any]] = []
    for value in values:
        if isinstance(value, dict):
            ref_type = str(value.get("type", "")).strip() or "reference"
            ref = str(value.get("ref", "")).strip()
            if not ref:
                continue
            row = {"type": ref_type, "ref": ref}
            note = str(value.get("note", "")).strip()
            if note:
                row["note"] = note
            rows.append(row)
        elif isinstance(value, str) and value.strip():
            rows.append({"type": "reference", "ref": value.strip()})
    return rows


def _applicability_score(applies_when: dict[str, Any], fingerprint: dict[str, Any]) -> tuple[float, list[str]]:
    if not applies_when:
        return 0.0, []
    hints: list[str] = []
    score = 0.0
    evaluated = 0

    for key in ("languages", "domains", "components", "legacy_signals", "agent_stages"):
        expected = [str(x).strip().lower() for x in _to_list_str(applies_when.get(key, []))]
        if not expected:
            continue
        evaluated += 1
        actual = {str(x).strip().lower() for x in _to_list_str(fingerprint.get(key, []))}
        overlap = sorted(actual.intersection(set(expected)))
        if overlap:
            score += 1.0
            hints.append(f"{key}:{','.join(overlap[:3])}")

    target_expected = str(applies_when.get("target_language", "")).strip().lower()
    if target_expected:
        evaluated += 1
        target_actual = str(fingerprint.get("target_language", "")).strip().lower()
        if target_actual and target_actual == target_expected:
            score += 1.0
            hints.append(f"target_language:{target_actual}")

    use_case_expected = str(applies_when.get("use_case", "")).strip().lower()
    if use_case_expected:
        evaluated += 1
        use_case_actual = str(fingerprint.get("use_case", "")).strip().lower()
        if use_case_actual and use_case_actual == use_case_expected:
            score += 1.0
            hints.append(f"use_case:{use_case_actual}")

    if evaluated <= 0:
        return 0.0, hints
    return score / evaluated, hints


@dataclass
class TenantMemoryStore:
    root_dir: str

    def __post_init__(self) -> None:
        self.root = Path(self.root_dir)
        self.root.mkdir(parents=True, exist_ok=True)

    def _memory_path(self, scope: dict[str, Any]) -> Path:
        workspace = _safe_segment(str(scope.get("workspace_id", "")), "default-workspace")
        client = _safe_segment(str(scope.get("client_id", "")), "default-client")
        project = _safe_segment(str(scope.get("project_id", "")), "default-project")
        return self.root / workspace / client / project / "memory.json"

    def _load(self, scope: dict[str, Any]) -> dict[str, Any]:
        path = self._memory_path(scope)
        data = _safe_json_load(path)
        if not data:
            data = {
                "scope": {
                    "workspace_id": str(scope.get("workspace_id", "default-workspace")),
                    "client_id": str(scope.get("client_id", "default-client")),
                    "project_id": str(scope.get("project_id", "default-project")),
                },
                "updated_at": _utc_now(),
                "threads": {},
                "constraints": [],
                "memory_items": [],
                "review_queue": [],
                "audit_log": [],
            }
        if not isinstance(data.get("threads", {}), dict):
            data["threads"] = {}
        if not isinstance(data.get("constraints", []), list):
            data["constraints"] = []
        if not isinstance(data.get("memory_items", []), list):
            data["memory_items"] = []
        if not isinstance(data.get("review_queue", []), list):
            data["review_queue"] = []
        if not isinstance(data.get("audit_log", []), list):
            data["audit_log"] = []
        return data

    def _save(self, scope: dict[str, Any], data: dict[str, Any]) -> None:
        data["updated_at"] = _utc_now()
        path = self._memory_path(scope)
        _safe_json_write(path, data)

    def _append_audit(
        self,
        data: dict[str, Any],
        *,
        action: str,
        actor: str,
        ref_id: str,
        details: dict[str, Any] | None = None,
    ) -> None:
        audit = data.get("audit_log", [])
        if not isinstance(audit, list):
            audit = []
        row = {
            "id": f"audit-{uuid.uuid4().hex[:12]}",
            "timestamp": _utc_now(),
            "action": str(action).strip(),
            "actor": str(actor or "local-user").strip() or "local-user",
            "ref_id": str(ref_id).strip(),
            "details": details if isinstance(details, dict) else {},
        }
        audit.append(row)
        data["audit_log"] = audit[-3000:]

    def append_thread_message(
        self,
        scope: dict[str, Any],
        *,
        thread_id: str,
        agent_role: str,
        role: str,
        message: str,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        tid = str(thread_id or "").strip() or "default-thread"
        msg = str(message or "").strip()
        if not msg:
            raise ValueError("message is required")
        data = self._load(scope)
        threads = data.get("threads", {})
        thread = threads.get(tid, {})
        if not isinstance(thread, dict):
            thread = {}
        entries = thread.get("messages", [])
        if not isinstance(entries, list):
            entries = []
        row = {
            "id": f"memmsg-{uuid.uuid4().hex[:12]}",
            "timestamp": _utc_now(),
            "agent_role": str(agent_role or "").strip() or "analyst",
            "role": str(role or "").strip() or "user",
            "message": msg,
            "metadata": metadata if isinstance(metadata, dict) else {},
        }
        entries.append(row)
        thread["messages"] = entries[-300:]
        threads[tid] = thread
        data["threads"] = threads
        self._save(scope, data)
        return row

    def get_thread(self, scope: dict[str, Any], thread_id: str, limit: int = 40) -> list[dict[str, Any]]:
        tid = str(thread_id or "").strip() or "default-thread"
        data = self._load(scope)
        threads = data.get("threads", {})
        thread = threads.get(tid, {}) if isinstance(threads, dict) else {}
        messages = thread.get("messages", []) if isinstance(thread, dict) else []
        if not isinstance(messages, list):
            return []
        return [row for row in messages[-max(1, int(limit)):] if isinstance(row, dict)]

    def add_memory_item(
        self,
        scope: dict[str, Any],
        *,
        item_type: str,
        title: str,
        statement: str,
        created_by: str,
        source: str = "manual",
        tier: str = "work_item",
        status: str | None = None,
        applies_when: dict[str, Any] | None = None,
        enforcement: dict[str, Any] | None = None,
        evidence_refs: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
        approved_by: str = "",
        expires_at: str | None = None,
    ) -> dict[str, Any]:
        text = str(statement or "").strip()
        if not text:
            raise ValueError("memory item statement is required")
        tier_norm = _normalize_tier(tier, "work_item")
        default_status = _default_status_for_tier(tier_norm)
        status_norm = _normalize_status(status, default_status)
        item_scope = _normalize_scope(scope, tier_norm)
        row = {
            "id": f"mem-{uuid.uuid4().hex[:12]}",
            "type": _normalize_item_type(item_type, "constraint"),
            "scope": item_scope,
            "status": status_norm,
            "title": str(title or text[:96]).strip() or text[:96],
            "statement": text,
            "applies_when": _normalize_applies_when(applies_when or {}),
            "enforcement": _normalize_enforcement(enforcement or {}),
            "evidence_refs": _normalize_evidence_refs(evidence_refs or []),
            "source": str(source or "manual").strip() or "manual",
            "created_by": str(created_by or "local-user").strip() or "local-user",
            "approved_by": str(approved_by or "").strip(),
            "created_at": _utc_now(),
            "updated_at": _utc_now(),
            "expires_at": str(expires_at or "").strip() or None,
            "revision": 1,
            "metadata": metadata if isinstance(metadata, dict) else {},
        }
        data = self._load(scope)
        rows = data.get("memory_items", [])
        if not isinstance(rows, list):
            rows = []
        rows.insert(0, row)
        data["memory_items"] = rows[:4000]
        self._append_audit(
            data,
            action="memory_item_created",
            actor=row["created_by"],
            ref_id=row["id"],
            details={"status": row["status"], "tier": tier_norm, "type": row["type"]},
        )
        self._save(scope, data)
        return row

    def list_memory_items(
        self,
        scope: dict[str, Any],
        *,
        status: str = "",
        tier: str = "",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        data = self._load(scope)
        rows = data.get("memory_items", [])
        if not isinstance(rows, list):
            return []
        status_norm = str(status or "").strip().lower()
        tier_norm = str(tier or "").strip().lower()
        out: list[dict[str, Any]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if not _scope_matches(scope, row.get("scope", {})):
                continue
            if status_norm and str(row.get("status", "")).strip().lower() != status_norm:
                continue
            if tier_norm and _normalize_tier(row.get("scope", {}).get("tier", "")) != tier_norm:
                continue
            out.append(row)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def search_memory_items(
        self,
        scope: dict[str, Any],
        query: str,
        *,
        fingerprint: dict[str, Any] | None = None,
        limit: int = 8,
        statuses: list[str] | None = None,
        tiers: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        data = self._load(scope)
        rows = data.get("memory_items", [])
        if not isinstance(rows, list):
            return []
        tokens = _tokens(query)
        fp = fingerprint if isinstance(fingerprint, dict) else {}
        allowed_statuses = {
            _normalize_status(x, "approved")
            for x in (statuses if isinstance(statuses, list) and statuses else ["approved"])
        }
        allowed_tiers = {
            _normalize_tier(x, "work_item")
            for x in tiers
        } if isinstance(tiers, list) and tiers else None

        scored: list[tuple[float, dict[str, Any]]] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            if str(row.get("status", "")).strip().lower() not in allowed_statuses:
                continue
            if not _scope_matches(scope, row.get("scope", {})):
                continue
            item_tier = _normalize_tier(row.get("scope", {}).get("tier", "work_item"))
            if allowed_tiers is not None and item_tier not in allowed_tiers:
                continue

            text_blob = " ".join(
                [
                    str(row.get("title", "")),
                    str(row.get("statement", "")),
                    str(row.get("type", "")),
                    " ".join(_to_list_str(row.get("applies_when", {}).get("components", []))),
                    " ".join(_to_list_str(row.get("applies_when", {}).get("domains", []))),
                ]
            )
            row_tokens = _tokens(text_blob)
            overlap = len(tokens.intersection(row_tokens)) if tokens else 0
            text_score = (overlap / max(1, len(tokens))) if tokens else 0.45
            if tokens and overlap <= 0:
                # Keep strong applicability matches even if lexical overlap is low.
                text_score = 0.0

            applicability, hints = _applicability_score(
                row.get("applies_when", {}) if isinstance(row.get("applies_when", {}), dict) else {},
                fp,
            )
            score = text_score + (0.65 * applicability)
            if score <= 0:
                continue
            enriched = dict(row)
            enriched["match"] = {
                "score": round(score, 4),
                "text_overlap": round(text_score, 4),
                "applicability": round(applicability, 4),
                "hints": hints,
            }
            scored.append((score, enriched))

        scored.sort(key=lambda item: item[0], reverse=True)
        return [row for _, row in scored[: max(1, int(limit))]]

    def update_memory_item_status(
        self,
        scope: dict[str, Any],
        *,
        item_id: str,
        status: str,
        actor: str,
        approved_by: str = "",
    ) -> dict[str, Any] | None:
        wanted = _normalize_status(status, "approved")
        data = self._load(scope)
        rows = data.get("memory_items", [])
        if not isinstance(rows, list):
            return None
        for idx, row in enumerate(rows):
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).strip() != str(item_id).strip():
                continue
            row["status"] = wanted
            row["updated_at"] = _utc_now()
            row["revision"] = int(row.get("revision", 1) or 1) + 1
            if approved_by:
                row["approved_by"] = str(approved_by).strip()
            elif wanted == "approved":
                row["approved_by"] = str(actor or "local-user").strip() or "local-user"
            rows[idx] = row
            data["memory_items"] = rows
            self._append_audit(
                data,
                action="memory_item_status_updated",
                actor=actor,
                ref_id=str(item_id),
                details={"status": wanted},
            )
            self._save(scope, data)
            return row
        return None

    def add_review_candidate(
        self,
        scope: dict[str, Any],
        *,
        summary: str,
        source: str,
        created_by: str,
        proposed_item: dict[str, Any] | None = None,
        patch: list[dict[str, Any]] | None = None,
        evidence_refs: list[dict[str, Any]] | None = None,
        metadata: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        text = str(summary or "").strip()
        if not text:
            raise ValueError("review candidate summary is required")
        row = {
            "id": f"cand-{uuid.uuid4().hex[:12]}",
            "status": "pending",
            "summary": text,
            "source": str(source or "unknown").strip() or "unknown",
            "proposed_item": proposed_item if isinstance(proposed_item, dict) else {},
            "patch": [x for x in (patch or []) if isinstance(x, dict)],
            "evidence_refs": _normalize_evidence_refs(evidence_refs or []),
            "created_by": str(created_by or "local-user").strip() or "local-user",
            "created_at": _utc_now(),
            "resolved_at": "",
            "resolved_by": "",
            "resolution_note": "",
            "metadata": metadata if isinstance(metadata, dict) else {},
        }
        data = self._load(scope)
        queue = data.get("review_queue", [])
        if not isinstance(queue, list):
            queue = []
        queue.insert(0, row)
        data["review_queue"] = queue[:1200]
        self._append_audit(
            data,
            action="review_candidate_created",
            actor=row["created_by"],
            ref_id=row["id"],
            details={"source": row["source"]},
        )
        self._save(scope, data)
        return row

    def list_review_queue(
        self,
        scope: dict[str, Any],
        *,
        status: str = "pending",
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        status_norm = str(status or "").strip().lower()
        if status_norm not in REVIEW_STATUSES:
            status_norm = ""
        data = self._load(scope)
        queue = data.get("review_queue", [])
        if not isinstance(queue, list):
            return []
        out: list[dict[str, Any]] = []
        for row in queue:
            if not isinstance(row, dict):
                continue
            if status_norm and str(row.get("status", "")).strip().lower() != status_norm:
                continue
            out.append(row)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def resolve_review_candidate(
        self,
        scope: dict[str, Any],
        *,
        candidate_id: str,
        action: str,
        actor: str,
        promote_to: str = "",
        note: str = "",
    ) -> dict[str, Any]:
        action_norm = str(action or "").strip().lower()
        if action_norm not in {"approve", "reject"}:
            raise ValueError("action must be approve or reject")
        data = self._load(scope)
        queue = data.get("review_queue", [])
        if not isinstance(queue, list):
            raise ValueError("review queue unavailable")
        for idx, row in enumerate(queue):
            if not isinstance(row, dict):
                continue
            if str(row.get("id", "")).strip() != str(candidate_id).strip():
                continue
            if str(row.get("status", "")).strip().lower() != "pending":
                raise ValueError("candidate already resolved")
            row["status"] = "approved" if action_norm == "approve" else "rejected"
            row["resolved_at"] = _utc_now()
            row["resolved_by"] = str(actor or "local-user").strip() or "local-user"
            row["resolution_note"] = str(note or "").strip()
            queue[idx] = row
            data["review_queue"] = queue

            created_item: dict[str, Any] | None = None
            promote_tier = ""
            proposed: dict[str, Any] = {}
            if action_norm == "approve":
                proposed = row.get("proposed_item", {}) if isinstance(row.get("proposed_item", {}), dict) else {}
                promote_tier = _normalize_tier(promote_to or proposed.get("tier", ""), "work_item")
            self._append_audit(
                data,
                action="review_candidate_resolved",
                actor=str(actor or "local-user"),
                ref_id=str(candidate_id),
                details={"status": row["status"], "promote_tier": promote_tier},
            )
            self._save(scope, data)
            if action_norm == "approve":
                created_item = self.add_memory_item(
                    scope,
                    item_type=str(proposed.get("type", "pattern")),
                    title=str(proposed.get("title", row.get("summary", ""))),
                    statement=str(proposed.get("statement", row.get("summary", ""))),
                    created_by=str(row.get("created_by", actor)),
                    source=str(row.get("source", "review_queue")),
                    tier=promote_tier,
                    status=None,
                    applies_when=proposed.get("applies_when", {}),
                    enforcement=proposed.get("enforcement", {}),
                    evidence_refs=row.get("evidence_refs", []),
                    metadata={"candidate_id": row.get("id"), "resolution_note": row.get("resolution_note", "")},
                    approved_by=str(actor or "local-user"),
                )
                latest = self._load(scope)
                self._append_audit(
                    latest,
                    action="review_candidate_promoted",
                    actor=str(actor or "local-user"),
                    ref_id=str(candidate_id),
                    details={"memory_item_id": str(created_item.get("id", "")), "tier": promote_tier},
                )
                self._save(scope, latest)
            return {"candidate": row, "memory_item": created_item}
        raise ValueError("candidate not found")

    def get_audit_log(self, scope: dict[str, Any], limit: int = 200) -> list[dict[str, Any]]:
        data = self._load(scope)
        audit = data.get("audit_log", [])
        if not isinstance(audit, list):
            return []
        return [row for row in audit[-max(1, int(limit)):] if isinstance(row, dict)]

    def add_constraint(
        self,
        scope: dict[str, Any],
        *,
        text: str,
        source: str,
        created_by: str,
        priority: str = "medium",
        applies_to: str = "all",
        promote_to: str = "work_item",
        applies_when: dict[str, Any] | None = None,
        enforcement: dict[str, Any] | None = None,
        evidence_refs: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        body = str(text or "").strip()
        if not body:
            raise ValueError("constraint text is required")
        p = str(priority or "medium").strip().lower()
        if p not in {"low", "medium", "high", "critical"}:
            p = "medium"
        row = {
            "id": f"constraint-{uuid.uuid4().hex[:10]}",
            "text": body,
            "priority": p,
            "source": str(source or "manual"),
            "created_by": str(created_by or "local-user"),
            "created_at": _utc_now(),
            "status": "active",
            "applies_to": str(applies_to or "all"),
        }
        data = self._load(scope)
        constraints = data.get("constraints", [])
        if not isinstance(constraints, list):
            constraints = []
        constraints.insert(0, row)
        data["constraints"] = constraints[:800]
        self._append_audit(
            data,
            action="constraint_added",
            actor=row["created_by"],
            ref_id=row["id"],
            details={"priority": row["priority"], "applies_to": row["applies_to"]},
        )
        self._save(scope, data)

        tier = _normalize_tier(promote_to, "work_item")
        stage_hint = []
        applies_to_norm = str(applies_to or "").strip().lower()
        if applies_to_norm.startswith("stage_"):
            stage_num = applies_to_norm.split("_", 1)[-1]
            stage_hint = [stage_num]
        elif applies_to_norm.isdigit():
            stage_hint = [applies_to_norm]
        item_applies_when = dict(applies_when) if isinstance(applies_when, dict) else {}
        if stage_hint and not item_applies_when.get("agent_stages"):
            item_applies_when["agent_stages"] = stage_hint
        item = self.add_memory_item(
            scope,
            item_type="constraint",
            title=body[:100],
            statement=body,
            created_by=row["created_by"],
            source=row["source"],
            tier=tier,
            applies_when=item_applies_when,
            enforcement=enforcement or {"gate_level": "warn"},
            evidence_refs=evidence_refs or [],
            metadata={"legacy_constraint_id": row["id"], "priority": p, "applies_to": applies_to},
        )
        row["memory_item_id"] = item.get("id")
        return row

    def _memory_item_to_constraint(self, row: dict[str, Any]) -> dict[str, Any]:
        enforcement = row.get("enforcement", {}) if isinstance(row.get("enforcement", {}), dict) else {}
        gate = str(enforcement.get("gate_level", "warn")).strip().lower()
        priority = "high" if gate == "block" else "medium"
        return {
            "id": str(row.get("id", "")).strip(),
            "text": str(row.get("statement", "")).strip(),
            "priority": priority,
            "source": str(row.get("source", "memory_item")).strip() or "memory_item",
            "created_by": str(row.get("created_by", "local-user")).strip() or "local-user",
            "created_at": str(row.get("created_at", "")).strip() or _utc_now(),
            "status": "active" if str(row.get("status", "")).strip().lower() == "approved" else "inactive",
            "applies_to": str(row.get("scope", {}).get("tier", "work_item")),
            "memory_item_id": str(row.get("id", "")).strip(),
        }

    def search_constraints(self, scope: dict[str, Any], query: str, limit: int = 8) -> list[dict[str, Any]]:
        tokens = _tokens(query)
        data = self._load(scope)
        base_constraints = data.get("constraints", [])
        if not isinstance(base_constraints, list):
            base_constraints = []

        memory_rows = self.search_memory_items(
            scope,
            query=query,
            fingerprint={},
            limit=200,
            statuses=["approved"],
            tiers=None,
        )
        memory_constraints: list[dict[str, Any]] = []
        for row in memory_rows:
            if not isinstance(row, dict):
                continue
            item_type = _normalize_item_type(row.get("type", "constraint"), "constraint")
            if item_type not in {"constraint", "correction", "playbook_choice", "pattern"}:
                continue
            statement = str(row.get("statement", "")).strip()
            if statement:
                memory_constraints.append(self._memory_item_to_constraint(row))

        combined: list[dict[str, Any]] = []
        seen_text: set[str] = set()
        for row in [*base_constraints, *memory_constraints]:
            if not isinstance(row, dict):
                continue
            txt = str(row.get("text", "")).strip()
            if not txt:
                continue
            key = txt.lower()
            if key in seen_text:
                continue
            seen_text.add(key)
            combined.append(row)

        scored: list[tuple[float, dict[str, Any]]] = []
        for row in combined:
            if str(row.get("status", "active")).strip().lower() != "active":
                continue
            row_tokens = _tokens(str(row.get("text", "")))
            if not row_tokens:
                continue
            overlap = len(tokens.intersection(row_tokens)) if tokens else 0
            if tokens and overlap <= 0:
                continue
            score = overlap / max(1, len(tokens)) if tokens else 0.5
            scored.append((score, row))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [row for _, row in scored[: max(1, int(limit))]]
