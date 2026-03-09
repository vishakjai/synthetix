"""
Projection of analyst artifacts into the canonical knowledge graph shape.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from utils.knowledge_contract import edge_payload, node_payload, provenance_ref


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _slug(value: Any) -> str:
    token = "".join(ch.lower() if ch.isalnum() else "_" for ch in _clean(value))
    while "__" in token:
        token = token.replace("__", "_")
    return token.strip("_") or "item"


class _Builder:
    def __init__(self, engagement_id: str, artifact_version: str):
        self.engagement_id = engagement_id
        self.artifact_version = artifact_version
        self.nodes: dict[str, dict[str, Any]] = {}
        self.edges: dict[str, dict[str, Any]] = {}

    def add_node(
        self,
        *,
        node_id: str,
        node_type: str,
        name: str,
        source_artifact_id: str,
        confidence: float,
        properties: dict[str, Any] | None = None,
        provenance: list[dict[str, Any]] | None = None,
    ) -> str:
        payload = node_payload(
            node_id=node_id,
            node_type=node_type,
            name=name,
            engagement_id=self.engagement_id,
            source_artifact_id=source_artifact_id,
            source_artifact_version=self.artifact_version,
            confidence=confidence,
            properties=properties or {},
            provenance=provenance or [],
        )
        self.nodes[node_id] = payload
        return node_id

    def add_edge(
        self,
        *,
        edge_type: str,
        source_node_id: str,
        target_node_id: str,
        confidence: float = 0.0,
        properties: dict[str, Any] | None = None,
    ) -> None:
        if not source_node_id or not target_node_id:
            return
        key = f"{edge_type}|{source_node_id}|{target_node_id}"
        self.edges[key] = edge_payload(
            edge_type=edge_type,
            source_node_id=source_node_id,
            target_node_id=target_node_id,
            confidence=confidence,
            properties=properties or {},
        )


def build_knowledge_projection(
    *,
    run_id: str,
    analyst_output: dict[str, Any],
    run_context_bundle: dict[str, Any] | None = None,
) -> dict[str, Any]:
    output = _as_dict(analyst_output)
    run_ctx = _as_dict(run_context_bundle)
    report = _as_dict(output.get("analyst_report_v2"))
    raw = _as_dict(output.get("raw_artifacts"))
    context_ref = _as_dict(output.get("context_reference"))
    if not context_ref:
        context_ref = _as_dict(_as_dict(raw.get("legacy_inventory")).get("metadata")).get("context_reference", {})
        context_ref = _as_dict(context_ref)
    source_mode = (
        _clean(output.get("source_mode"))
        or _clean(_as_dict(run_ctx.get("integration_context")).get("source_mode"))
        or "repo_scan"
    )
    repo = _clean(context_ref.get("repo"))
    branch = _clean(context_ref.get("branch")) or "main"
    commit_sha = _clean(context_ref.get("commit_sha"))
    artifact_version = _clean(output.get("run_id")) or _clean(run_id) or _clean(context_ref.get("version_id")) or "latest"
    engagement_id = _clean(run_id) or artifact_version
    builder = _Builder(engagement_id, artifact_version)

    provider_id = "repo_scan"
    if source_mode in {"evidence", "imported_analysis"}:
        provider_id = _clean(_as_dict(_as_dict(run_ctx.get("integration_context")).get("evidence_mode")).get("provider_id")) or "imported_analysis"
        provider_node = builder.add_node(
            node_id=f"provider:{_slug(provider_id)}",
            node_type="Provider",
            name=provider_id,
            source_artifact_id="run_context_bundle",
            confidence=0.95,
            properties={"source_mode": source_mode},
            provenance=[provenance_ref(artifact_id="run_context_bundle", note="selected evidence provider")],
        )
        bundle_id = _clean(_as_dict(_as_dict(run_ctx.get("integration_context")).get("evidence_mode")).get("bundle_id"))
        if bundle_id:
            bundle_node = builder.add_node(
                node_id=f"bundle:{_slug(bundle_id)}",
                node_type="EvidenceBundle",
                name=bundle_id,
                source_artifact_id="run_context_bundle",
                confidence=0.95,
                properties={"bundle_id": bundle_id, "source_mode": source_mode},
                provenance=[provenance_ref(artifact_id="run_context_bundle", note="imported evidence bundle")],
            )
            builder.add_edge(edge_type="PROVIDED_BY", source_node_id=bundle_node, target_node_id=provider_node, confidence=0.95)

    artifact_nodes: dict[str, str] = {}
    for artifact_id, title in (
        ("legacy_inventory", "Legacy Inventory"),
        ("form_dossier", "Form Dossiers"),
        ("procedure_summary", "Procedure Summaries"),
        ("business_rule_catalog", "Business Rule Catalog"),
        ("risk_register", "Risk Register"),
        ("sql_catalog", "SQL Catalog"),
        ("dependency_inventory", "Dependency Inventory"),
        ("detector_findings", "Detector Findings"),
    ):
        artifact = _as_dict(raw.get(artifact_id))
        if not artifact:
            continue
        artifact_nodes[artifact_id] = builder.add_node(
            node_id=f"artifact:{artifact_id}",
            node_type="EvidenceArtifact",
            name=title,
            source_artifact_id=artifact_id,
            confidence=0.9,
            properties={"artifact_id": artifact_id},
            provenance=[provenance_ref(artifact_id=artifact_id, note="authoritative analyst artifact")],
        )

    legacy_inventory = _as_dict(raw.get("legacy_inventory"))
    legacy_counts = _as_dict(_as_dict(legacy_inventory.get("summary")).get("counts"))
    builder.add_node(
        node_id="document:analyst_report_v2",
        node_type="Document",
        name="Analyst Report v2",
        source_artifact_id="analyst_report_v2",
        confidence=0.95,
        properties={
            "repo": repo,
            "branch": branch,
            "projects": int(legacy_counts.get("projects", 0) or 0),
            "forms": int(legacy_counts.get("forms_or_screens", 0) or 0),
            "loc": int(legacy_counts.get("source_loc_total", 0) or 0),
        },
        provenance=[provenance_ref(artifact_id="analyst_report_v2", note="primary report summary")],
    )

    module_ids: dict[tuple[str, str], str] = {}
    dossiers = _as_list(_as_dict(raw.get("form_dossier")).get("dossiers"))
    if not dossiers:
        dossiers = _as_list(output.get("mapped_forms"))
    for idx, row in enumerate(dossiers, start=1):
        if not isinstance(row, dict):
            continue
        form_name = _clean(row.get("form_name") or row.get("form") or row.get("name"))
        if not form_name:
            continue
        project = _clean(row.get("project") or row.get("project_name") or "default")
        module_id = f"module:{_slug(project)}:{_slug(form_name)}"
        db_tables = [x for x in _as_list(row.get("db_tables")) if _clean(x)]
        description = _clean(row.get("purpose") or row.get("summary") or row.get("description"))
        coverage_score = row.get("coverage_score")
        module_ids[(project.lower(), form_name.lower())] = builder.add_node(
            node_id=module_id,
            node_type="Module",
            name=form_name,
            source_artifact_id="form_dossier",
            confidence=float(row.get("confidence", 0.6) or 0.6),
            properties={
                "project": project,
                "path": _clean(row.get("source_file")),
                "loc": int(row.get("loc", 0) or 0),
                "module_kind": _clean(row.get("type") or row.get("module_type")),
                "description": description,
                "coverage_score": int(coverage_score or 0) if str(coverage_score or "").strip() else None,
                "db_tables": [_clean(x) for x in db_tables if _clean(x)],
                "status": _clean(row.get("status") or "active"),
                "evidence_ref": _clean(row.get("evidence")),
            },
            provenance=[provenance_ref(artifact_id="form_dossier", path=_clean(row.get("source_file")), note=_clean(row.get("evidence")))],
        )
        if "form_dossier" in artifact_nodes:
            builder.add_edge(edge_type="SUPPORTED_BY", source_node_id=module_id, target_node_id=artifact_nodes["form_dossier"], confidence=0.8)
        for table in db_tables:
            entity_id = f"data:{_slug(table)}"
            builder.add_node(
                node_id=entity_id,
                node_type="DataEntity",
                name=_clean(table),
                source_artifact_id="form_dossier",
                confidence=0.7,
                properties={"entity_type": "TABLE"},
                provenance=[provenance_ref(artifact_id="form_dossier", note=f"{form_name} db_tables")],
            )
            builder.add_edge(edge_type="READS", source_node_id=module_id, target_node_id=entity_id, confidence=0.6)

    procedures = _as_list(_as_dict(raw.get("procedure_summary")).get("procedures"))
    function_ids: dict[str, str] = {}
    for idx, row in enumerate(procedures, start=1):
        if not isinstance(row, dict):
            continue
        name = _clean(row.get("procedure") or row.get("handler") or row.get("name"))
        if not name:
            continue
        form_name = _clean(row.get("form") or row.get("module") or row.get("component"))
        project = _clean(row.get("project") or "default")
        function_id = f"function:{_slug(project)}:{_slug(form_name or 'shared')}:{_slug(name)}"
        function_ids[f"{project.lower()}::{form_name.lower()}::{name.lower()}"] = function_id
        builder.add_node(
            node_id=function_id,
            node_type="Function",
            name=name,
            source_artifact_id="procedure_summary",
            confidence=float(row.get("confidence", 0.6) or 0.6),
            properties={
                "module_name": form_name,
                "project": project,
                "description": _clean(row.get("summary") or row.get("description")),
                "sql_ids": [_clean(x) for x in _as_list(row.get("sql_ids")) if _clean(x)],
                "start_line": int(row.get("line", 0) or 0),
            },
            provenance=[provenance_ref(artifact_id="procedure_summary", path=_clean(row.get("source_file")), line=int(row.get("line", 0) or 0), note=_clean(row.get("evidence")))],
        )
        module_id = module_ids.get((project.lower(), form_name.lower()))
        if module_id:
            builder.add_edge(edge_type="CONTAINS", source_node_id=module_id, target_node_id=function_id, confidence=0.85)
        if "procedure_summary" in artifact_nodes:
            builder.add_edge(edge_type="SUPPORTED_BY", source_node_id=function_id, target_node_id=artifact_nodes["procedure_summary"], confidence=0.8)

    rules = _as_list(_as_dict(raw.get("business_rule_catalog")).get("rules"))
    for idx, row in enumerate(rules, start=1):
        if not isinstance(row, dict):
            continue
        rule_id = _clean(row.get("rule_id") or row.get("id")) or f"BR-{idx:03d}"
        form_name = _clean(row.get("form") or _as_dict(row.get("scope")).get("form"))
        project = _clean(_as_dict(row.get("scope")).get("project") or "default")
        rule_node_id = f"rule:{_slug(rule_id)}"
        builder.add_node(
            node_id=rule_node_id,
            node_type="BusinessRule",
            name=rule_id,
            source_artifact_id="business_rule_catalog",
            confidence=float(row.get("confidence", 0.6) or 0.6),
            properties={
                "description": _clean(row.get("description")),
                "rule_type": _clean(row.get("type") or row.get("rule_type")),
                "domain": _clean(row.get("domain")),
                "status": _clean(row.get("status") or "EXTRACTED"),
                "form": form_name,
                "project": project,
            },
            provenance=[provenance_ref(artifact_id="business_rule_catalog", note=rule_id)],
        )
        module_id = module_ids.get((project.lower(), form_name.lower()))
        if module_id:
            builder.add_edge(edge_type="IMPLEMENTS", source_node_id=module_id, target_node_id=rule_node_id, confidence=0.7)
        if "business_rule_catalog" in artifact_nodes:
            builder.add_edge(edge_type="SUPPORTED_BY", source_node_id=rule_node_id, target_node_id=artifact_nodes["business_rule_catalog"], confidence=0.8)

    risks = _as_list(_as_dict(raw.get("risk_register")).get("risks"))
    for idx, row in enumerate(risks, start=1):
        if not isinstance(row, dict):
            continue
        risk_id = _clean(row.get("risk_id") or row.get("id")) or f"RISK-{idx:03d}"
        form_name = _clean(row.get("form") or row.get("component"))
        project = _clean(row.get("project") or "default")
        risk_node_id = f"risk:{_slug(risk_id)}"
        builder.add_node(
            node_id=risk_node_id,
            node_type="RiskFlag",
            name=risk_id,
            source_artifact_id="risk_register",
            confidence=0.85,
            properties={
                "severity": _clean(row.get("severity") or "medium").upper(),
                "category": _clean(row.get("category") or row.get("risk_type")),
                "description": _clean(row.get("description")),
                "recommended_action": _clean(row.get("recommended_action") or row.get("action")),
            },
            provenance=[provenance_ref(artifact_id="risk_register", note=risk_id)],
        )
        module_id = module_ids.get((project.lower(), form_name.lower()))
        if module_id:
            builder.add_edge(edge_type="HAS_RISK", source_node_id=module_id, target_node_id=risk_node_id, confidence=0.8)
        if "risk_register" in artifact_nodes:
            builder.add_edge(edge_type="SUPPORTED_BY", source_node_id=risk_node_id, target_node_id=artifact_nodes["risk_register"], confidence=0.8)

    statements = _as_list(_as_dict(raw.get("sql_catalog")).get("statements"))
    for row in statements:
        if not isinstance(row, dict):
            continue
        for table in _as_list(row.get("tables")):
            table_name = _clean(table)
            if not table_name:
                continue
            entity_id = f"data:{_slug(table_name)}"
            builder.add_node(
                node_id=entity_id,
                node_type="DataEntity",
                name=table_name,
                source_artifact_id="sql_catalog",
                confidence=0.8,
                properties={
                    "entity_type": "TABLE",
                    "sql_kind": _clean(row.get("kind")),
                },
                provenance=[provenance_ref(artifact_id="sql_catalog", note=_clean(row.get("sql_id")))],
            )

    sql_map_entries = _as_list(_as_dict(raw.get("sql_map")).get("entries"))
    for row in sql_map_entries:
        if not isinstance(row, dict):
            continue
        form_name = _clean(row.get("form") or row.get("component"))
        project = _clean(row.get("project") or "default")
        module_id = module_ids.get((project.lower(), form_name.lower()))
        if not module_id:
            continue
        for table in _as_list(row.get("tables")):
            table_name = _clean(table)
            if not table_name:
                continue
            entity_id = f"data:{_slug(table_name)}"
            if _clean(row.get("operation")).lower() in {"insert", "update", "delete"}:
                builder.add_edge(edge_type="WRITES", source_node_id=module_id, target_node_id=entity_id, confidence=0.75)
            else:
                builder.add_edge(edge_type="READS", source_node_id=module_id, target_node_id=entity_id, confidence=0.75)

    dependencies = _as_list(_as_dict(raw.get("dependency_inventory")).get("dependencies"))
    for row in dependencies:
        if not isinstance(row, dict):
            continue
        from_form = _clean(row.get("form") or row.get("from"))
        to_form = _clean(row.get("target_form") or row.get("to"))
        project = _clean(row.get("project") or "default")
        if not (from_form and to_form):
            continue
        source_id = module_ids.get((project.lower(), from_form.lower()))
        target_id = module_ids.get((project.lower(), to_form.lower()))
        if source_id and target_id:
            builder.add_edge(edge_type="DEPENDS_ON", source_node_id=source_id, target_node_id=target_id, confidence=0.65, properties={"kind": _clean(row.get("type"))})

    decisions = _as_list(_as_dict(report.get("decision_brief")).get("blocking_decisions"))
    if not decisions:
        decisions = _as_list(output.get("decisions_required"))
    for idx, row in enumerate(decisions, start=1):
        if not isinstance(row, dict):
            continue
        decision_id = _clean(row.get("id") or row.get("decision_id")) or f"DEC-{idx:03d}"
        topic = _clean(row.get("topic") or row.get("title") or decision_id)
        decision_node = builder.add_node(
            node_id=f"decision:{_slug(decision_id)}",
            node_type="Decision",
            name=decision_id,
            source_artifact_id="decision_brief",
            confidence=0.8,
            properties={
                "topic": topic,
                "status": _clean(row.get("status") or row.get("state") or "open"),
                "description": _clean(row.get("decision") or row.get("description")),
            },
            provenance=[provenance_ref(artifact_id="decision_brief", note=decision_id)],
        )
        if "topic" in topic.lower() or "compliance" in topic.lower():
            compliance_id = builder.add_node(
                node_id=f"control:{_slug(decision_id)}",
                node_type="ComplianceControl",
                name=topic,
                source_artifact_id="decision_brief",
                confidence=0.75,
                properties={
                    "framework": _clean(row.get("framework")),
                    "status": "AT_RISK" if _clean(row.get("status")).lower() not in {"closed", "resolved"} else "CONFIRMED",
                    "description": _clean(row.get("decision") or row.get("description")),
                },
                provenance=[provenance_ref(artifact_id="decision_brief", note=decision_id)],
            )
            builder.add_edge(edge_type="HAS_DECISION", source_node_id=compliance_id, target_node_id=decision_node, confidence=0.75)

    coverage_rows = _as_list(_as_dict(raw.get("traceability_coverage")).get("rows"))
    for row in coverage_rows:
        if not isinstance(row, dict):
            continue
        form_name = _clean(row.get("form"))
        project = _clean(row.get("project") or "default")
        module_id = module_ids.get((project.lower(), form_name.lower()))
        if not module_id:
            continue
        props = builder.nodes.get(module_id, {}).get("properties", {})
        if isinstance(props, dict):
            props["traceability_score"] = int(row.get("score", 0) or 0)
            props["traceability_status"] = _clean(row.get("status"))
            builder.nodes[module_id]["properties"] = props

    metadata = {
        "run_id": run_id,
        "source_mode": source_mode,
        "repo": repo,
        "branch": branch,
        "commit_sha": commit_sha,
        "provider_id": provider_id,
        "generated_at": _utc_now(),
    }
    return {
        "engagement_id": engagement_id,
        "metadata": metadata,
        "nodes": list(builder.nodes.values()),
        "edges": list(builder.edges.values()),
    }
