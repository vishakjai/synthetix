"""
Deterministic first-pass intent routing for knowledge-layer interactions.
"""

from __future__ import annotations

from typing import Any


def _clean(value: Any) -> str:
    return str(value or "").strip()


def classify_interaction_intent(message: str) -> dict[str, Any]:
    text = _clean(message)
    lower = text.lower()

    modification_markers = (
        "update ",
        "change ",
        "modify ",
        "set ",
        "mark ",
        "rewrite ",
        "edit ",
        "commit ",
        "apply ",
    )
    generation_markers = (
        "generate ",
        "draft ",
        "summarize ",
        "write ",
        "create a briefing",
        "create briefing",
        "produce ",
    )
    analysis_markers = (
        "what if",
        "impact",
        "blast radius",
        "break if",
        "breaks if",
        "depends on",
        "dependency",
        "conflict",
        "gap",
        "compliance gap",
    )
    inventory_markers = (
        "list ",
        "show all",
        "show me all",
        "which forms",
        "what forms",
        "list forms",
        "list the forms",
        "list modules",
        "list the modules",
        "list rules",
        "list the rules",
        "list routes",
        "list the routes",
        "which routes",
        "what routes",
        "list controllers",
        "list the controllers",
        "which controllers",
        "what controllers",
        "list templates",
        "list the templates",
    )

    intent = "query"
    reason = "default_query"
    if any(token in lower for token in modification_markers):
        intent = "modification"
        reason = "mutation_verb"
    elif any(token in lower for token in generation_markers):
        intent = "generation"
        reason = "generation_verb"
    elif any(token in lower for token in analysis_markers):
        intent = "analysis"
        reason = "analysis_marker"
    elif any(token in lower for token in inventory_markers):
        intent = "query"
        reason = "inventory_marker"

    tokens = text.replace("?", " ").replace(",", " ").split()
    topic = "general"
    entity_name = ""
    inventory_kind = ""
    mentions_form_like = any(token.strip().lower().startswith(("frm", "mdi", "menu")) for token in tokens)
    if (
        "lines of code" in lower
        or "line of code" in lower
        or "loc" in lower
        or "how many forms" in lower
        or "how many modules" in lower
        or "how many files" in lower
        or "how many projects" in lower
        or "project count" in lower
        or "file count" in lower
        or "form count" in lower
    ):
        topic = "metrics"
    elif (
        "list" in lower
        or "show all" in lower
        or "which forms" in lower
        or "what forms" in lower
        or "what modules" in lower
        or "which modules" in lower
        or "what rules" in lower
        or "which rules" in lower
    ):
        topic = "inventory"
    elif "compliance" in lower:
        topic = "compliance"
    elif "traceability" in lower or "coverage" in lower:
        topic = "traceability"
    elif "rule" in lower or "business rule" in lower or "br-" in lower:
        topic = "rule"
    elif "depends on" in lower or "blast radius" in lower or "dependency" in lower:
        topic = "dependency"
    elif "module" in lower or "form" in lower or "screen" in lower or mentions_form_like:
        topic = "module"
    elif "provenance" in lower or "come from" in lower or "source of" in lower:
        topic = "provenance"

    for token in tokens:
        raw = token.strip()
        low = raw.lower()
        if low.startswith("br-") or low.startswith("dec-") or low.startswith("risk-"):
            entity_name = raw
            break
    if not entity_name and topic in {"module", "dependency", "provenance"}:
        for token in tokens:
            raw = token.strip()
            if raw.lower().startswith(("frm", "mdi", "menu")):
                entity_name = raw
                break

    if topic == "inventory":
        if "form" in lower or "screen" in lower:
            inventory_kind = "forms"
        elif "rule" in lower or "business rule" in lower or "br-" in lower:
            inventory_kind = "rules"
        elif "route" in lower:
            inventory_kind = "routes"
        elif "controller" in lower:
            inventory_kind = "controllers"
        elif "template" in lower or "view" in lower:
            inventory_kind = "templates"
        elif "project" in lower:
            inventory_kind = "projects"
        elif "function" in lower or "procedure" in lower:
            inventory_kind = "functions"
        else:
            inventory_kind = "modules"

    return {
        "intent": intent,
        "reason": reason,
        "topic": topic,
        "entity_name": entity_name,
        "inventory_kind": inventory_kind,
        "message": text,
    }
