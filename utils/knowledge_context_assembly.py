"""
Context assembly for read-only knowledge interactions.
"""

from __future__ import annotations

from typing import Any

from utils.interaction_intents import classify_interaction_intent
from utils.knowledge_queries import KnowledgeQueries


def _clean(value: Any) -> str:
    return str(value or "").strip()


def assemble_knowledge_context(queries: KnowledgeQueries, message: str) -> dict[str, Any]:
    routed = classify_interaction_intent(message)
    topic = routed.get("topic")
    entity_name = _clean(routed.get("entity_name"))

    assembled: dict[str, Any] = {
        "intent": routed,
        "primary": {},
        "related": {},
    }

    if topic == "module" and entity_name:
        assembled["primary"] = queries.get_module_context(entity_name)
    elif topic == "dependency" and entity_name:
        assembled["primary"] = queries.get_dependency_blast_radius(entity_name)
    elif topic == "rule" and entity_name:
        assembled["primary"] = queries.get_rule_context(entity_name)
    elif topic == "compliance":
        assembled["primary"] = queries.get_compliance_gaps()
    elif topic == "traceability":
        assembled["primary"] = queries.get_traceability_gaps()
    elif topic == "metrics":
        assembled["primary"] = queries.get_estate_metrics()
    elif topic == "provenance" and entity_name:
        assembled["primary"] = queries.explain_provenance(entity_name)
    else:
        assembled["primary"] = queries.search_concepts(message, limit=8)

    if topic not in {"compliance", "traceability"}:
        assembled["related"]["compliance_gaps"] = queries.get_compliance_gaps()
        assembled["related"]["traceability_gaps"] = {
            "count": queries.get_traceability_gaps().get("count", 0)
        }
    return assembled
