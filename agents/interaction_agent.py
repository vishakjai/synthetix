"""
Read-only interaction agent over the engagement knowledge layer.
"""

from __future__ import annotations

from typing import Any

from utils.knowledge_context_assembly import assemble_knowledge_context
from utils.knowledge_queries import KnowledgeQueries


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _mode_for_confidence(confidence: float) -> str:
    if confidence >= 0.8:
        return "evidence-backed"
    if confidence >= 0.5:
        return "inferred"
    return "needs verification"


class InteractionAgent:
    def __init__(self, queries: KnowledgeQueries):
        self.queries = queries

    def respond(self, message: str) -> dict[str, Any]:
        assembled = assemble_knowledge_context(self.queries, message)
        routed = assembled.get("intent", {}) if isinstance(assembled.get("intent", {}), dict) else {}
        topic = _clean(routed.get("topic"))
        intent = _clean(routed.get("intent"))
        primary = assembled.get("primary", {}) if isinstance(assembled.get("primary", {}), dict) else {}
        answer = "No grounded answer was available from the knowledge layer."
        provenance: list[dict[str, Any]] = []
        confidence = 0.4

        if intent == "modification":
            answer = (
                "This request looks like a change proposal. Review the grounded context below and use the proposal "
                "workflow to create a typed change for approval before any artifacts are regenerated."
            )
            confidence = 0.8
        elif topic == "module" and isinstance(primary.get("module"), dict):
            module = primary["module"]
            props = module.get("properties", {}) if isinstance(module.get("properties", {}), dict) else {}
            desc = _clean(props.get("description")) or "No module description is available."
            answer = (
                f"{_clean(module.get('name'))} is a {_clean(props.get('module_kind')) or 'legacy'} module"
                f" in project {_clean(props.get('project')) or 'n/a'}. {desc} "
                f"Functions: {len(primary.get('functions', []))}. "
                f"Business rules: {len(primary.get('business_rules', []))}. "
                f"Risks: {len(primary.get('risk_flags', []))}."
            ).strip()
            provenance = list(module.get("provenance_ref", []))
            confidence = float(module.get("confidence", 0.0) or 0.0)
        elif topic == "dependency" and primary.get("module"):
            module = primary["module"]
            affected = primary.get("affected_modules", [])
            preview = ", ".join([_clean(row.get("name")) for row in affected[:6] if isinstance(row, dict) and _clean(row.get("name"))]) or "none"
            answer = (
                f"{_clean(module.get('name'))} affects {int(primary.get('total_affected', 0) or 0)} downstream module(s). "
                f"Nearest dependents: {preview}."
            )
            provenance = list(module.get("provenance_ref", []))
            confidence = float(module.get("confidence", 0.0) or 0.0)
        elif topic == "rule" and isinstance(primary.get("rule"), dict):
            rule = primary["rule"]
            props = rule.get("properties", {}) if isinstance(rule.get("properties", {}), dict) else {}
            implementers = primary.get("implementers", [])
            answer = (
                f"{_clean(rule.get('name'))}: {_clean(props.get('description')) or 'No rule description available.'} "
                f"Type: {_clean(props.get('rule_type')) or 'n/a'}. "
                f"Implemented by {len(implementers)} upstream node(s)."
            )
            provenance = list(rule.get("provenance_ref", []))
            confidence = float(rule.get("confidence", 0.0) or 0.0)
        elif topic == "compliance":
            controls = primary.get("controls", [])
            preview = ", ".join([_clean(row.get("name")) for row in controls[:6] if isinstance(row, dict) and _clean(row.get("name"))]) or "none"
            answer = f"There are {int(primary.get('count', 0) or 0)} unresolved compliance gap node(s). Sample: {preview}."
            confidence = 0.75
        elif topic == "traceability":
            modules = primary.get("modules", [])
            preview = ", ".join([_clean(row.get("name")) for row in modules[:6] if isinstance(row, dict) and _clean(row.get("name"))]) or "none"
            answer = f"There are {int(primary.get('count', 0) or 0)} module(s) with traceability gaps. Sample: {preview}."
            confidence = 0.75
        elif topic == "metrics":
            source_loc_total = int(primary.get("source_loc_total", 0) or 0)
            form_count = int(primary.get("form_count", 0) or 0)
            project_count = int(primary.get("project_count", 0) or 0)
            module_count = int(primary.get("module_count", 0) or 0)
            projects = primary.get("projects", [])
            project_preview = ", ".join([_clean(x) for x in projects[:6] if _clean(x)]) or "n/a"
            answer = (
                f"The legacy application contains {source_loc_total:,} lines of code. "
                f"I currently have {form_count} form/module node(s) across {project_count} project(s). "
                f"Projects: {project_preview}. "
                f"Projected module inventory size: {module_count}."
            )
            document = primary.get("document", {}) if isinstance(primary.get("document", {}), dict) else {}
            provenance = list(document.get("provenance_ref", []))
            confidence = float(document.get("confidence", 0.0) or 0.9)
        elif topic == "provenance" and isinstance(primary.get("node"), dict):
            node = primary["node"]
            refs = primary.get("provenance_ref", [])
            preview = ", ".join(
                [
                    _clean(ref.get("artifact_id")) + (f":{int(ref.get('line', 0) or 0)}" if int(ref.get("line", 0) or 0) else "")
                    for ref in refs[:6]
                    if isinstance(ref, dict) and _clean(ref.get("artifact_id"))
                ]
            ) or "none"
            answer = f"{_clean(node.get('name'))} is grounded in: {preview}."
            provenance = refs
            confidence = float(node.get("confidence", 0.0) or 0.0)
        else:
            hits = primary.get("hits", [])
            preview = ", ".join(
                [
                    f"{_clean(row.get('name'))} [{_clean(row.get('node_type'))}]"
                    for row in hits[:6]
                    if isinstance(row, dict) and _clean(row.get("name"))
                ]
            ) or "none"
            answer = f"I found {len(hits)} relevant knowledge node(s). Top matches: {preview}."
            if hits and isinstance(hits[0], dict):
                provenance = list(hits[0].get("provenance_ref", []))
                confidence = float(hits[0].get("confidence", 0.0) or 0.0)

        return {
            "intent": intent,
            "topic": topic,
            "answer": answer,
            "confidence": round(confidence, 3),
            "mode": _mode_for_confidence(confidence),
            "provenance": provenance[:12],
            "context": assembled,
        }
