"""
Canonical knowledge-layer contract for engagement-scoped graph projection.

The graph is a query-optimized projection of authoritative run artifacts.
"""

from __future__ import annotations

from typing import Any


NODE_TYPES: tuple[str, ...] = (
    "Module",
    "Function",
    "Route",
    "Template",
    "BusinessRule",
    "DataEntity",
    "ComplianceControl",
    "RiskFlag",
    "Document",
    "EvidenceBundle",
    "EvidenceArtifact",
    "EvidenceSpan",
    "Provider",
    "Decision",
)

EDGE_TYPES: tuple[str, ...] = (
    "CONTAINS",
    "CALLS",
    "IMPLEMENTS",
    "READS",
    "WRITES",
    "HAS_RISK",
    "VALIDATES",
    "DOCUMENTED_IN",
    "REFERENCES",
    "SUPPORTED_BY",
    "EXTRACTED_FROM",
    "PROVIDED_BY",
    "DEPENDS_ON",
    "HAS_DECISION",
)

REQUIRED_NODE_PROPERTIES: tuple[str, ...] = (
    "engagement_id",
    "source_artifact_id",
    "source_artifact_version",
    "confidence",
    "provenance_ref",
)


def valid_node_type(node_type: Any) -> bool:
    return str(node_type or "").strip() in NODE_TYPES


def valid_edge_type(edge_type: Any) -> bool:
    return str(edge_type or "").strip() in EDGE_TYPES


def provenance_ref(
    *,
    artifact_id: str,
    path: str = "",
    line: int = 0,
    note: str = "",
) -> dict[str, Any]:
    ref = {
        "artifact_id": str(artifact_id or "").strip(),
        "path": str(path or "").strip(),
        "line": int(line or 0),
        "note": str(note or "").strip(),
    }
    return {k: v for k, v in ref.items() if v not in {"", 0}}


def node_payload(
    *,
    node_id: str,
    node_type: str,
    name: str,
    engagement_id: str,
    source_artifact_id: str,
    source_artifact_version: str,
    confidence: float,
    provenance: list[dict[str, Any]] | None = None,
    properties: dict[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "node_id": str(node_id or "").strip(),
        "node_type": str(node_type or "").strip(),
        "name": str(name or "").strip(),
        "engagement_id": str(engagement_id or "").strip(),
        "source_artifact_id": str(source_artifact_id or "").strip(),
        "source_artifact_version": str(source_artifact_version or "").strip(),
        "confidence": float(confidence or 0.0),
        "provenance_ref": list(provenance or []),
        "properties": dict(properties or {}),
    }


def edge_payload(
    *,
    edge_type: str,
    source_node_id: str,
    target_node_id: str,
    properties: dict[str, Any] | None = None,
    confidence: float = 0.0,
) -> dict[str, Any]:
    return {
        "edge_type": str(edge_type or "").strip(),
        "source_node_id": str(source_node_id or "").strip(),
        "target_node_id": str(target_node_id or "").strip(),
        "properties": dict(properties or {}),
        "confidence": float(confidence or 0.0),
    }
