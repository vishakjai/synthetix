"""
Typed proposal workflow for the run-scoped knowledge assistant.

This layer is review-only. It does not mutate authoritative artifacts.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from utils.interaction_intents import classify_interaction_intent
from utils.knowledge_queries import KnowledgeQueries


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _json_dump(data: Any) -> str:
    return json.dumps(data, ensure_ascii=True, indent=2, sort_keys=True)


def _doc_impact_for_type(proposal_type: str) -> list[str]:
    if proposal_type == "risk_change":
        return ["Analyst MD", "Tech Workbook", "BA Brief"]
    if proposal_type == "scope_change":
        return ["Analyst MD", "Tech Workbook", "BA Brief", "BRD"]
    if proposal_type == "sme_assignment":
        return ["Analyst MD", "BA Brief", "BRD"]
    return ["Analyst MD", "Tech Workbook", "BA Brief", "BRD"]


@dataclass
class ProposalTarget:
    node_id: str
    node_type: str
    name: str
    properties: dict[str, Any]
    provenance: list[dict[str, Any]]


class KnowledgeProposalStore:
    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def list(self) -> list[dict[str, Any]]:
        data = self._read()
        rows = data.get("proposals", [])
        return rows if isinstance(rows, list) else []

    def save_all(self, proposals: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload = {
            "updated_at": _utc_now(),
            "proposals": [row for row in proposals if isinstance(row, dict)],
        }
        self.path.write_text(_json_dump(payload), encoding="utf-8")
        return payload["proposals"]

    def create(self, proposal: dict[str, Any]) -> dict[str, Any]:
        proposals = self.list()
        proposals.append(proposal)
        self.save_all(proposals)
        return proposal

    def update_status(self, proposal_id: str, *, decision: str, rationale: str, actor: str) -> dict[str, Any]:
        proposals = self.list()
        target: dict[str, Any] | None = None
        for idx, row in enumerate(proposals):
            if not isinstance(row, dict):
                continue
            if _clean(row.get("id")) != proposal_id:
                continue
            target = dict(row)
            status = _clean(target.get("status")).lower() or "pending"
            if status != "pending":
                raise ValueError(f"proposal already {status}")
            normalized = "approved" if decision == "approve" else "rejected"
            target["status"] = normalized
            target["reviewed_at"] = _utc_now()
            target["reviewed_by"] = actor or "user"
            target["review_rationale"] = rationale
            proposals[idx] = target
            break
        if target is None:
            raise KeyError("proposal not found")
        self.save_all(proposals)
        return target

    def _read(self) -> dict[str, Any]:
        if not self.path.exists():
            return {"updated_at": "", "proposals": []}
        try:
            return json.loads(self.path.read_text(encoding="utf-8"))
        except Exception:
            return {"updated_at": "", "proposals": []}


class KnowledgeProposalService:
    def __init__(self, queries: KnowledgeQueries, store: KnowledgeProposalStore):
        self.queries = queries
        self.store = store

    def list_proposals(self) -> list[dict[str, Any]]:
        return self.store.list()

    def create_from_message(self, message: str, *, actor: str = "") -> dict[str, Any]:
        text = _clean(message)
        if not text:
            raise ValueError("message is required")
        routed = classify_interaction_intent(text)
        if _clean(routed.get("intent")) != "modification":
            raise ValueError("message does not describe a supported modification request")

        proposal_type, target = self._resolve_target(routed, text)
        before = self._before_state(target, proposal_type)
        after = self._after_state(text, proposal_type)
        impact = self._impact(target, proposal_type)

        proposal_id = f"kp_{uuid.uuid4().hex[:10]}"
        proposal = {
            "id": proposal_id,
            "status": "pending",
            "proposal_type": proposal_type,
            "title": self._title_for(target, proposal_type),
            "summary": self._summary_for(target, proposal_type, text),
            "request_message": text,
            "target": {
                "node_id": target.node_id,
                "node_type": target.node_type,
                "name": target.name,
            },
            "before": before,
            "after": after,
            "impact": impact,
            "provenance": target.provenance[:12],
            "confidence": round(float(target.properties.get("confidence", 0.0) or 0.7), 3),
            "created_at": _utc_now(),
            "created_by": actor or "user",
        }
        return self.store.create(proposal)

    def review(self, proposal_id: str, *, decision: str, rationale: str, actor: str = "") -> dict[str, Any]:
        normalized = _clean(decision).lower()
        if normalized not in {"approve", "reject"}:
            raise ValueError("decision must be approve or reject")
        return self.store.update_status(
            proposal_id,
            decision=normalized,
            rationale=_clean(rationale),
            actor=actor,
        )

    def _resolve_target(self, routed: dict[str, Any], message: str) -> tuple[str, ProposalTarget]:
        entity_name = _clean(routed.get("entity_name"))
        lower = message.lower()

        if entity_name.lower().startswith("br-") or "business rule" in lower or "rule " in lower:
            rule = self.queries.get_rule_context(entity_name) if entity_name else {}
            node = _as_dict(rule.get("rule"))
            if node:
                return "rule_update", ProposalTarget(
                    node_id=_clean(node.get("node_id")),
                    node_type=_clean(node.get("node_type")) or "BusinessRule",
                    name=_clean(node.get("name")),
                    properties=_as_dict(node.get("properties")),
                    provenance=_as_list(node.get("provenance_ref")),
                )

        if entity_name.lower().startswith("risk-") or "risk" in lower:
            target = self._search_first(message, ["RiskFlag"])
            if target:
                return "risk_change", target

        if "scope" in lower or "include " in lower or "exclude " in lower or "out of scope" in lower or "in scope" in lower:
            target = self._search_first(entity_name or message, ["Module"])
            if target:
                return "scope_change", target

        if "sme" in lower or "owner" in lower or "reviewer" in lower or "assign" in lower:
            target = self._search_first(entity_name or message, ["Module", "BusinessRule"])
            if target:
                return "sme_assignment", target

        target = self._search_first(entity_name or message, ["Module"])
        if target:
            return "module_description_change", target

        raise ValueError("could not resolve a proposal target from the request")

    def _search_first(self, query: str, node_types: list[str]) -> ProposalTarget | None:
        hits = _as_list(self.queries.search_concepts(query, node_types=node_types, limit=1).get("hits"))
        first = _as_dict(hits[0]) if hits else {}
        if not first:
            return None
        return ProposalTarget(
            node_id=_clean(first.get("node_id")),
            node_type=_clean(first.get("node_type")),
            name=_clean(first.get("name")),
            properties=_as_dict(first.get("properties")),
            provenance=_as_list(first.get("provenance_ref")),
        )

    def _before_state(self, target: ProposalTarget, proposal_type: str) -> dict[str, Any]:
        props = target.properties
        if proposal_type == "rule_update":
            return {
                "description": _clean(props.get("description")),
                "rule_type": _clean(props.get("rule_type")),
                "status": _clean(props.get("status")),
            }
        if proposal_type == "risk_change":
            return {
                "severity": _clean(props.get("severity")),
                "category": _clean(props.get("category")),
                "description": _clean(props.get("description")),
            }
        if proposal_type == "scope_change":
            return {
                "status": _clean(props.get("status") or "active"),
                "project": _clean(props.get("project")),
                "description": _clean(props.get("description")),
            }
        return {
            "description": _clean(props.get("description")),
            "project": _clean(props.get("project")),
            "status": _clean(props.get("status")),
        }

    def _after_state(self, message: str, proposal_type: str) -> dict[str, Any]:
        if proposal_type == "risk_change":
            return {"requested_change": message, "expected_outcome": "Risk classification/description reviewed"}
        if proposal_type == "sme_assignment":
            return {"requested_change": message, "expected_outcome": "SME ownership or review assignment reviewed"}
        if proposal_type == "scope_change":
            return {"requested_change": message, "expected_outcome": "Scope treatment reviewed"}
        if proposal_type == "rule_update":
            return {"requested_change": message, "expected_outcome": "Business rule wording reviewed"}
        return {"requested_change": message, "expected_outcome": "Module description reviewed"}

    def _impact(self, target: ProposalTarget, proposal_type: str) -> dict[str, Any]:
        docs = _doc_impact_for_type(proposal_type)
        impact: dict[str, Any] = {
            "impacted_documents": docs,
            "affected_node_ids": [target.node_id],
        }
        if target.node_type == "Module":
            blast = self.queries.get_dependency_blast_radius(target.name)
            impact["blast_radius"] = {
                "total_affected": int(blast.get("total_affected", 0) or 0),
                "affected_modules": _as_list(blast.get("affected_modules"))[:8],
            }
        elif target.node_type == "BusinessRule":
            rule_ctx = self.queries.get_rule_context(target.name)
            impact["implementers"] = [
                {
                    "node_id": _clean(row.get("node_id")),
                    "name": _clean(row.get("name")),
                    "node_type": _clean(row.get("node_type")),
                }
                for row in _as_list(rule_ctx.get("implementers"))[:8]
                if isinstance(row, dict)
            ]
            impact["affected_node_ids"].extend([_clean(row.get("node_id")) for row in _as_list(rule_ctx.get("implementers")) if _clean(_as_dict(row).get("node_id"))])
        return impact

    def _title_for(self, target: ProposalTarget, proposal_type: str) -> str:
        title_map = {
            "rule_update": f"Update business rule {target.name}",
            "risk_change": f"Reclassify risk {target.name}",
            "scope_change": f"Adjust scope for {target.name}",
            "sme_assignment": f"Assign SME review for {target.name}",
            "module_description_change": f"Revise module description for {target.name}",
        }
        return title_map.get(proposal_type, f"Update {target.name}")

    def _summary_for(self, target: ProposalTarget, proposal_type: str, message: str) -> str:
        prefix_map = {
            "rule_update": "Business rule text change requested",
            "risk_change": "Risk change requested",
            "scope_change": "Scope treatment change requested",
            "sme_assignment": "SME assignment change requested",
            "module_description_change": "Module description change requested",
        }
        prefix = prefix_map.get(proposal_type, "Change requested")
        return f"{prefix} for {target.name}: {message}"
