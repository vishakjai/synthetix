"""
Read-only query facade over the knowledge projection store.
"""

from __future__ import annotations

from collections import deque
from typing import Any

from utils.knowledge_store import KnowledgeStore


def _clean(value: Any) -> str:
    return str(value or "").strip()


class KnowledgeQueries:
    def __init__(self, store: KnowledgeStore, engagement_id: str):
        self.store = store
        self.engagement_id = engagement_id

    def get_module_context(self, module_name: str) -> dict[str, Any]:
        modules = self.store.query_nodes(self.engagement_id, node_type="Module", name=module_name, limit=10)
        if not modules:
            return {"module": None, "functions": [], "business_rules": [], "risk_flags": [], "evidence": []}
        module = modules[0]
        neighborhood = self.store.get_neighbors(self.engagement_id, node_id=str(module.get("node_id")), direction="out")
        nodes_by_id = {str(row.get("node_id")): row for row in neighborhood.get("nodes", []) if isinstance(row, dict)}
        functions: list[dict[str, Any]] = []
        risks: list[dict[str, Any]] = []
        evidence: list[dict[str, Any]] = []
        rules: list[dict[str, Any]] = []
        for edge in neighborhood.get("edges", []):
            if not isinstance(edge, dict):
                continue
            target = nodes_by_id.get(str(edge.get("target_node_id")))
            if not isinstance(target, dict):
                continue
            edge_type = _clean(edge.get("edge_type"))
            node_type = _clean(target.get("node_type"))
            if edge_type == "CONTAINS" and node_type == "Function":
                functions.append(target)
            elif edge_type == "HAS_RISK" and node_type == "RiskFlag":
                risks.append(target)
            elif edge_type == "SUPPORTED_BY" and node_type == "EvidenceArtifact":
                evidence.append(target)
            elif edge_type == "IMPLEMENTS" and node_type == "BusinessRule":
                rules.append(target)
        for function in list(functions):
            fn_neighborhood = self.store.get_neighbors(self.engagement_id, node_id=str(function.get("node_id")), direction="out")
            fn_nodes = {str(row.get("node_id")): row for row in fn_neighborhood.get("nodes", []) if isinstance(row, dict)}
            for edge in fn_neighborhood.get("edges", []):
                if not isinstance(edge, dict) or _clean(edge.get("edge_type")) != "IMPLEMENTS":
                    continue
                target = fn_nodes.get(str(edge.get("target_node_id")))
                if isinstance(target, dict) and target not in rules:
                    rules.append(target)
        return {
            "module": module,
            "functions": functions,
            "business_rules": rules,
            "risk_flags": risks,
            "evidence": evidence,
            "projection": self.store.get_projection_metadata(self.engagement_id),
        }

    def get_rule_context(self, rule_id: str) -> dict[str, Any]:
        rules = self.store.query_nodes(self.engagement_id, node_type="BusinessRule", name=rule_id, limit=10)
        if not rules:
            return {"rule": None, "implementers": [], "evidence": []}
        rule = rules[0]
        neighborhood = self.store.get_neighbors(self.engagement_id, node_id=str(rule.get("node_id")), direction="in")
        nodes_by_id = {str(row.get("node_id")): row for row in neighborhood.get("nodes", []) if isinstance(row, dict)}
        implementers = []
        evidence = []
        for edge in neighborhood.get("edges", []):
            if not isinstance(edge, dict):
                continue
            source = nodes_by_id.get(str(edge.get("source_node_id")))
            if not isinstance(source, dict):
                continue
            if _clean(edge.get("edge_type")) == "IMPLEMENTS":
                implementers.append(source)
            elif _clean(edge.get("edge_type")) == "SUPPORTED_BY":
                evidence.append(source)
        return {"rule": rule, "implementers": implementers, "evidence": evidence}

    def get_dependency_blast_radius(self, module_name: str) -> dict[str, Any]:
        modules = self.store.query_nodes(self.engagement_id, node_type="Module", name=module_name, limit=10)
        if not modules:
            return {"module": None, "affected_modules": [], "total_affected": 0}
        root = modules[0]
        visited = {str(root.get("node_id"))}
        queue: deque[tuple[str, int]] = deque([(str(root.get("node_id")), 0)])
        results: list[dict[str, Any]] = []
        while queue:
            node_id, depth = queue.popleft()
            neighborhood = self.store.get_neighbors(
                self.engagement_id,
                node_id=node_id,
                direction="out",
                edge_types=["DEPENDS_ON"],
            )
            nodes_by_id = {str(row.get("node_id")): row for row in neighborhood.get("nodes", []) if isinstance(row, dict)}
            for edge in neighborhood.get("edges", []):
                if not isinstance(edge, dict):
                    continue
                target_id = str(edge.get("target_node_id"))
                if target_id in visited:
                    continue
                visited.add(target_id)
                target = nodes_by_id.get(target_id)
                if not isinstance(target, dict):
                    continue
                if _clean(target.get("node_type")) == "Module":
                    results.append(
                        {
                            "name": _clean(target.get("name")),
                            "project": _clean(target.get("properties", {}).get("project")),
                            "hops_away": depth + 1,
                            "module_id": target_id,
                        }
                    )
                    queue.append((target_id, depth + 1))
        results.sort(key=lambda row: (int(row.get("hops_away", 0) or 0), _clean(row.get("name")).lower()))
        return {"module": root, "affected_modules": results, "total_affected": len(results)}

    def get_compliance_gaps(self) -> dict[str, Any]:
        controls = self.store.query_nodes(self.engagement_id, node_type="ComplianceControl", limit=200)
        gaps = []
        for control in controls:
            status = _clean(control.get("properties", {}).get("status")).upper()
            if status not in {"CONFIRMED", "CARRIED_FORWARD", "RESOLVED", "CLOSED"}:
                gaps.append(control)
        return {"count": len(gaps), "controls": gaps}

    def get_traceability_gaps(self) -> dict[str, Any]:
        modules = self.store.query_nodes(self.engagement_id, node_type="Module", limit=500)
        gaps = []
        for module in modules:
            props = module.get("properties", {}) if isinstance(module.get("properties", {}), dict) else {}
            score = int(props.get("traceability_score", props.get("coverage_score", 0)) or 0)
            status = _clean(props.get("traceability_status")).lower()
            if score <= 0 or status in {"discovery gap", "trace_gap", "gap"}:
                gaps.append(module)
        return {"count": len(gaps), "modules": gaps}

    def get_estate_metrics(self) -> dict[str, Any]:
        docs = self.store.query_nodes(self.engagement_id, node_type="Document", name="Analyst Report v2", limit=5)
        summary = docs[0] if docs else None
        props = summary.get("properties", {}) if isinstance(summary, dict) and isinstance(summary.get("properties", {}), dict) else {}
        modules = self.store.query_nodes(self.engagement_id, node_type="Module", limit=1000)
        projects = sorted(
            {
                _clean(row.get("properties", {}).get("project"))
                for row in modules
                if isinstance(row, dict) and _clean(row.get("properties", {}).get("project"))
            }
        )
        total_form_loc = sum(
            int(_clean(_clean(row.get("properties", {}).get("loc")) or 0) or 0)
            for row in modules
            if isinstance(row, dict)
        )
        return {
            "document": summary,
            "source_loc_total": int(props.get("loc", 0) or 0),
            "project_count": int(props.get("projects", 0) or 0) or len(projects),
            "form_count": int(props.get("forms", 0) or 0) or len(modules),
            "module_count": len(modules),
            "projects": projects,
            "form_loc_subtotal": total_form_loc,
        }

    def search_concepts(self, query: str, *, node_types: list[str] | None = None, limit: int = 10) -> dict[str, Any]:
        hits = self.store.search_nodes(self.engagement_id, query=query, limit=limit * 3)
        if node_types:
            allowed = {str(item).strip() for item in node_types if str(item).strip()}
            hits = [row for row in hits if _clean(row.get("node_type")) in allowed]
        return {"query": query, "hits": hits[: max(1, min(limit, 50))]}

    def explain_provenance(self, node_id: str) -> dict[str, Any]:
        node = self.store.get_node(self.engagement_id, node_id)
        if not node:
            return {"node": None, "evidence": []}
        neighborhood = self.store.get_neighbors(
            self.engagement_id,
            node_id=node_id,
            direction="out",
            edge_types=["SUPPORTED_BY", "DOCUMENTED_IN", "REFERENCES"],
        )
        nodes_by_id = {str(row.get("node_id")): row for row in neighborhood.get("nodes", []) if isinstance(row, dict)}
        evidence = []
        for edge in neighborhood.get("edges", []):
            if not isinstance(edge, dict):
                continue
            target = nodes_by_id.get(str(edge.get("target_node_id")))
            if isinstance(target, dict):
                evidence.append({"edge_type": _clean(edge.get("edge_type")), "node": target})
        return {"node": node, "provenance_ref": node.get("provenance_ref", []), "evidence": evidence}
