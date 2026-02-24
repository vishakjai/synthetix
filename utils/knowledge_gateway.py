"""
Knowledge Gateway for Analyst-as-a-Service.

Provides:
- graph-style deterministic capability dependency lookup
- vector-style semantic retrieval over domain packs + firm patterns
- regulation retrieval helpers
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from utils.domain_packs import retrieve_regulatory_constraints


def _tokens(text: str) -> set[str]:
    return {
        token
        for token in re.findall(r"[a-z0-9_]+", str(text or "").lower())
        if token
    }


def _score(query_tokens: set[str], content: str, tags: list[str] | None = None) -> float:
    body = _tokens(content)
    if tags:
        body.update(_tokens(" ".join([str(x) for x in tags if str(x).strip()])))
    if not query_tokens or not body:
        return 0.0
    overlap = len(query_tokens.intersection(body))
    return round(overlap / max(1, len(query_tokens)), 4)


def _safe_json_load(path: Path) -> dict[str, Any]:
    if not path.exists() or not path.is_file():
        return {}
    try:
        data = json.loads(path.read_text())
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


@dataclass
class KnowledgeGateway:
    root_dir: str

    def __post_init__(self) -> None:
        self.root = Path(self.root_dir)
        self.patterns_path = self.root / "knowledge_base" / "firm_patterns.json"

    def query_capability_dependencies(
        self,
        domain_pack: dict[str, Any],
        capability_ids: list[str],
    ) -> list[dict[str, Any]]:
        capabilities = (
            domain_pack.get("ontology", {}).get("capabilities", [])
            if isinstance(domain_pack.get("ontology", {}), dict)
            else []
        )
        cap_index: dict[str, dict[str, Any]] = {}
        for cap in capabilities if isinstance(capabilities, list) else []:
            if not isinstance(cap, dict):
                continue
            cap_id = str(cap.get("id", "")).strip()
            if cap_id:
                cap_index[cap_id] = cap

        edges: list[dict[str, Any]] = []
        for cap_id in capability_ids:
            source = str(cap_id or "").strip()
            if not source or source not in cap_index:
                continue
            deps = cap_index[source].get("dependencies", [])
            for dep in deps if isinstance(deps, list) else []:
                target = str(dep or "").strip()
                if not target:
                    continue
                edges.append(
                    {
                        "from": source,
                        "to": target,
                        "type": "depends_on",
                        "confidence": 0.95,
                        "evidence": f"domain_pack.ontology.capabilities[{source}].dependencies",
                    }
                )
        return edges

    def query_vector_context(
        self,
        *,
        query: str,
        domain_pack: dict[str, Any],
        top_k: int = 8,
    ) -> list[dict[str, Any]]:
        query_tokens = _tokens(query)
        corpus: list[dict[str, Any]] = []

        standards = domain_pack.get("standards", [])
        for item in standards if isinstance(standards, list) else []:
            if not isinstance(item, dict):
                continue
            corpus.append(
                {
                    "id": str(item.get("id", "")) or "standard",
                    "type": "standard",
                    "title": str(item.get("name", "Standard")).strip(),
                    "content": " ".join(
                        [str(item.get("name", ""))]
                        + [str(x) for x in (item.get("engineering_actions", []) if isinstance(item.get("engineering_actions", []), list) else [])]
                    ).strip(),
                    "tags": [str(x) for x in (item.get("applies_to", []) if isinstance(item.get("applies_to", []), list) else [])],
                    "source_class": "domain_pack",
                }
            )

        regulations = domain_pack.get("regulations", [])
        for item in regulations if isinstance(regulations, list) else []:
            if not isinstance(item, dict):
                continue
            corpus.append(
                {
                    "id": str(item.get("id", "")) or "regulation",
                    "type": "regulation",
                    "title": str(item.get("name", "Regulation")).strip(),
                    "content": " ".join(
                        [str(item.get("control_objective", ""))]
                        + [str(x) for x in (item.get("software_actions", []) if isinstance(item.get("software_actions", []), list) else [])]
                    ).strip(),
                    "tags": [str(x) for x in (item.get("tags", []) if isinstance(item.get("tags", []), list) else [])],
                    "source_class": "domain_pack",
                }
            )

        patterns = domain_pack.get("gold_patterns", [])
        for item in patterns if isinstance(patterns, list) else []:
            if not isinstance(item, dict):
                continue
            corpus.append(
                {
                    "id": str(item.get("id", "")) or "pattern",
                    "type": "pattern",
                    "title": str(item.get("title", "Pattern")).strip(),
                    "content": " ".join([str(item.get("title", ""))] + [str(x) for x in (item.get("guidance", []) if isinstance(item.get("guidance", []), list) else [])]).strip(),
                    "tags": [str(x) for x in (item.get("tags", []) if isinstance(item.get("tags", []), list) else [])],
                    "source_class": "domain_pack",
                }
            )

        pattern_file = _safe_json_load(self.patterns_path)
        docs = pattern_file.get("documents", []) if isinstance(pattern_file.get("documents", []), list) else []
        for doc in docs:
            if not isinstance(doc, dict):
                continue
            corpus.append(
                {
                    "id": str(doc.get("id", "")) or "firm-doc",
                    "type": "firm_pattern",
                    "title": str(doc.get("title", "Firm Pattern")).strip(),
                    "content": str(doc.get("content", "")).strip(),
                    "tags": [str(x) for x in (doc.get("tags", []) if isinstance(doc.get("tags", []), list) else [])],
                    "source_class": str(doc.get("source_class", "firm_pattern")),
                }
            )

        scored: list[dict[str, Any]] = []
        for row in corpus:
            row_score = _score(query_tokens, str(row.get("content", "")), tags=row.get("tags", []))
            if row_score <= 0:
                continue
            scored.append(
                {
                    "id": row.get("id"),
                    "type": row.get("type"),
                    "title": row.get("title"),
                    "snippet": str(row.get("content", ""))[:400],
                    "tags": row.get("tags", []),
                    "source_class": row.get("source_class", "domain_pack"),
                    "score": row_score,
                }
            )
        scored.sort(key=lambda item: float(item.get("score", 0.0)), reverse=True)
        return scored[: max(1, int(top_k))]

    def query_regulatory_constraints(
        self,
        *,
        domain_pack: dict[str, Any],
        capability_ids: list[str],
        jurisdiction: str,
        data_classes: list[str],
    ) -> list[dict[str, Any]]:
        return retrieve_regulatory_constraints(
            domain_pack,
            capability_ids=capability_ids,
            jurisdiction=jurisdiction,
            data_classes=data_classes,
        )
