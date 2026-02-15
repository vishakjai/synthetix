"""
Context artifact contracts: canonicalization, validation, and persistence.

Builds immutable, schema-driven artifacts from SIL output:
- System Context Model (SCM)
- Convention Profile (CP)
- Health Assessment Bundle (HAB)
- Context Bundle (manifest for downstream agents)
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _new_id() -> str:
    return uuid.uuid4().hex


def _json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=True, sort_keys=True, separators=(",", ":"), default=str)


def _digest(payload: dict[str, Any]) -> str:
    return "sha256:" + hashlib.sha256(_json(payload).encode("utf-8")).hexdigest()


def _score(value: Any, default: float = 0.6) -> float:
    try:
        n = float(value)
    except Exception:
        return default
    return max(0.0, min(1.0, n))


def _confidence(value: Any, rationale: str = "") -> dict[str, Any]:
    if isinstance(value, dict):
        return {
            "score": _score(value.get("score", value.get("confidence", 0.6)), 0.6),
            "rationale": str(value.get("rationale", rationale or "")),
            "signals": list(value.get("signals", [])) if isinstance(value.get("signals", []), list) else [],
        }
    return {
        "score": _score(value, 0.6),
        "rationale": rationale,
        "signals": [],
    }


def _evidence_pointer(raw: dict[str, Any]) -> dict[str, Any]:
    kind = str(raw.get("kind", "")).strip().lower()
    if not kind:
        if raw.get("file"):
            kind = "file_span"
        elif raw.get("trace_id") or raw.get("span_id"):
            kind = "trace_span"
        elif raw.get("config_key"):
            kind = "config_key"
        else:
            kind = "manual"
    out = {"kind": kind}
    passthrough = [
        "file",
        "start_line",
        "end_line",
        "config_file",
        "config_key",
        "commit_sha",
        "author",
        "trace_id",
        "span_id",
        "query",
        "note",
        "ref",
    ]
    for key in passthrough:
        if key in raw and raw.get(key) not in (None, ""):
            out[key] = raw.get(key)
    return out


def _evidence_list(values: Any) -> list[dict[str, Any]]:
    if not isinstance(values, list):
        return []
    out: list[dict[str, Any]] = []
    for item in values:
        if isinstance(item, dict):
            out.append(_evidence_pointer(item))
        elif isinstance(item, str) and item.strip():
            out.append({"kind": "manual", "note": item.strip()})
    return out


_NODE_MAP = {
    "service": "service",
    "container": "container",
    "component": "component",
    "module": "module",
    "package": "package",
    "endpoint": "endpoint",
    "route": "endpoint",
    "messagetopic": "message_topic",
    "message_topic": "message_topic",
    "consumergroup": "component",
    "database": "database",
    "table": "table",
    "column": "component",
    "infraresource": "infra_resource",
    "infra_resource": "infra_resource",
    "externaldependency": "external_dependency",
    "external_dependency": "external_dependency",
    "system": "system",
    "boundary": "boundary",
}


_EDGE_MAP = {
    "depends_on": "depends_on",
    "dependson": "depends_on",
    "imports": "imports",
    "calls_http": "calls_http",
    "callshttp": "calls_http",
    "calls_grpc": "calls_grpc",
    "callsgrpc": "calls_grpc",
    "publishes": "publishes",
    "consumes": "consumes",
    "reads_table": "reads",
    "reads": "reads",
    "writes_table": "writes",
    "writes": "writes",
    "deploys_to": "deploys_to",
    "runson": "runs_on",
    "runs_on": "runs_on",
    "owns_resource": "owns",
    "owns": "owns",
    "authenticates_via": "authenticates_via",
    "uses_library": "depends_on",
}


def _norm_node_type(raw: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9_]+", "", str(raw or "").strip().lower())
    return _NODE_MAP.get(key, "component")


def _norm_edge_type(raw: str) -> str:
    key = re.sub(r"[^a-zA-Z0-9_]+", "", str(raw or "").strip().lower())
    return _EDGE_MAP.get(key, "depends_on")


def _severity_to_tier(sev: str) -> tuple[int, int, int, str]:
    s = str(sev or "").strip().lower()
    if s == "critical":
        return 5, 5, 25, "critical"
    if s == "high":
        return 4, 4, 16, "high"
    if s == "medium":
        return 3, 3, 9, "medium"
    if s == "low":
        return 2, 2, 4, "low"
    return 2, 2, 4, "low"


def _risk_from_severity(sev: str, rationale: str = "") -> dict[str, Any]:
    likelihood, impact, score, tier = _severity_to_tier(sev)
    return {
        "likelihood": likelihood,
        "impact": impact,
        "score": score,
        "tier": tier,
        "rationale": rationale,
    }


def _effort(raw: Any = "M", confidence: float = 0.6) -> dict[str, Any]:
    tshirt = str(raw or "M").strip().upper()
    if tshirt not in {"XS", "S", "M", "L", "XL"}:
        tshirt = "M"
    ranges = {
        "XS": [2, 6],
        "S": [6, 16],
        "M": [16, 40],
        "L": [40, 80],
        "XL": [80, 160],
    }
    return {
        "tshirt": tshirt,
        "hours_range": ranges[tshirt],
        "confidence": _confidence(confidence, "estimated from artifact metadata"),
    }


def _header(
    artifact_type: str,
    *,
    repo_ref: dict[str, Any],
    generated_by: dict[str, Any],
    parents: list[str] | None = None,
    labels: dict[str, str] | None = None,
) -> dict[str, Any]:
    return {
        "artifact_id": _new_id(),
        "artifact_type": artifact_type,
        "schema_version": "1.0.0",
        "created_at": _utc_now(),
        "repo_ref": repo_ref,
        "generated_by": generated_by,
        "parents": list(parents or []),
        "digest": "sha256:0000000000000000",
        "labels": labels or {},
    }


def _with_digest(artifact: dict[str, Any]) -> dict[str, Any]:
    out = copy.deepcopy(artifact)
    out["header"]["digest"] = "sha256:0000000000000000"
    out["header"]["digest"] = _digest(out)
    return out


def _repo_ref(context_ref: dict[str, Any]) -> dict[str, Any]:
    return {
        "provider": "other",
        "repo_url": str(context_ref.get("repo", "unknown")),
        "default_branch": str(context_ref.get("branch", "main") or "main"),
        "branch": str(context_ref.get("branch", "main") or "main"),
        "commit_sha": str(context_ref.get("commit_sha", "unknown") or "unknown"),
        "subpath": str(context_ref.get("subpath", "")),
    }


def _generated_by(context_ref: dict[str, Any], run_id: str, model: str = "") -> dict[str, Any]:
    return {
        "agent_name": "System Intelligence Layer",
        "agent_version": "1.0.0",
        "model": model,
        "tooling": ["repo_scan", "context_vault", "context_graph"],
        "run_id": run_id or _new_id(),
        "stage_id": "context_layer",
        "task_id": _new_id(),
    }


def _build_scm(sil: dict[str, Any], repo_ref: dict[str, Any], generated_by: dict[str, Any], labels: dict[str, str]) -> dict[str, Any]:
    scm_src = sil.get("system_context_model", {}) if isinstance(sil, dict) else {}
    src_nodes = scm_src.get("nodes", []) if isinstance(scm_src.get("nodes", []), list) else []
    src_edges = scm_src.get("edges", []) if isinstance(scm_src.get("edges", []), list) else []

    nodes: list[dict[str, Any]] = []
    node_ids: set[str] = set()
    for idx, raw in enumerate(src_nodes, start=1):
        if not isinstance(raw, dict):
            continue
        node_id = str(raw.get("id", "")).strip() or f"node:{idx}"
        while node_id in node_ids:
            node_id = f"{node_id}-{idx}"
        node_ids.add(node_id)

        metadata = raw.get("metadata", {}) if isinstance(raw.get("metadata", {}), dict) else {}
        repo_paths = metadata.get("repo_paths", []) if isinstance(metadata.get("repo_paths", []), list) else []
        if not repo_paths and metadata.get("path"):
            repo_paths = [str(metadata.get("path"))]
        evidence = _evidence_list(raw.get("provenance", []))
        if not repo_paths:
            # If explicit repo paths are missing, derive them from file-span evidence.
            inferred_paths = [
                str(e.get("file", "")).strip()
                for e in evidence
                if isinstance(e, dict) and str(e.get("kind", "")).strip() == "file_span" and str(e.get("file", "")).strip()
            ]
            if inferred_paths:
                # Preserve order while removing duplicates.
                repo_paths = list(dict.fromkeys(inferred_paths))

        nodes.append(
            {
                "id": node_id,
                "type": _norm_node_type(str(raw.get("type", "component"))),
                "name": str(raw.get("name", node_id)),
                "description": str(raw.get("description", "")),
                "labels": {"source": "sil"},
                "language": str(metadata.get("language", "")),
                "repo_paths": [str(x) for x in repo_paths],
                "runtime": metadata.get("runtime", {}) if isinstance(metadata.get("runtime", {}), dict) else {},
                "interfaces": metadata.get("interfaces", []) if isinstance(metadata.get("interfaces", []), list) else [],
                "data": metadata.get("data", {}) if isinstance(metadata.get("data", {}), dict) else {},
                "attributes": metadata,
                "evidence": evidence,
                "confidence": _confidence(raw.get("confidence", 0.6), "from SIL node extraction"),
            }
        )

    edges: list[dict[str, Any]] = []
    for idx, raw in enumerate(src_edges, start=1):
        if not isinstance(raw, dict):
            continue
        src = str(raw.get("from", "")).strip()
        dst = str(raw.get("to", "")).strip()
        if not src or not dst:
            continue
        edges.append(
            {
                "id": str(raw.get("id", "")).strip() or f"edge:{idx}",
                "type": _norm_edge_type(str(raw.get("type", "depends_on"))),
                "from": src,
                "to": dst,
                "labels": {"source": "sil"},
                "protocol": raw.get("protocol_metadata", {}) if isinstance(raw.get("protocol_metadata", {}), dict) else {},
                "evidence": _evidence_list(raw.get("evidence", [])),
                "confidence": _confidence(raw.get("confidence", 0.55), "from SIL edge extraction"),
            }
        )

    unknown_src = scm_src.get("unknowns", []) if isinstance(scm_src.get("unknowns", []), list) else []
    unknowns = []
    for u in unknown_src:
        if isinstance(u, dict):
            unknowns.append(
                {
                    "question": str(u.get("question", "Unknown system behavior")),
                    "scope": str(u.get("scope", "system")),
                    "suggested_validation": str(u.get("suggested_validation", "Trace and verify in staging")),
                    "confidence": _confidence(u.get("confidence", 0.4), "unknown requires verification"),
                }
            )
        else:
            unknowns.append(
                {
                    "question": str(u),
                    "scope": "system",
                    "suggested_validation": "Trace and verify in staging",
                    "confidence": _confidence(0.4, "unknown from SIL"),
                }
            )

    artifact = {
        "header": _header("system_context_model", repo_ref=repo_ref, generated_by=generated_by, labels=labels),
        "graph": {
            "nodes": nodes,
            "edges": edges,
            "statistics": {
                "node_count": len(nodes),
                "edge_count": len(edges),
                "languages": sorted({str(n.get("language", "")).strip() for n in nodes if str(n.get("language", "")).strip()}),
            },
        },
        "views": {"c4": {"context": [], "container": [], "component": [], "code": []}},
        "unknowns": unknowns,
    }
    return _with_digest(artifact)


def _map_cp_category(raw: str) -> str:
    text = str(raw or "").strip().lower()
    if text in {"naming", "error_handling", "testing", "api_design", "packaging_build", "infra_iac"}:
        return text
    if text in {"logging", "logging_observability", "observability"}:
        return "logging_observability"
    if text in {"auth", "authn", "authz", "authn_authz", "security"}:
        return "authn_authz"
    if "error" in text:
        return "error_handling"
    if "test" in text:
        return "testing"
    if "log" in text or "trace" in text:
        return "logging_observability"
    if "api" in text:
        return "api_design"
    return "naming"


def _build_cp(sil: dict[str, Any], repo_ref: dict[str, Any], generated_by: dict[str, Any], labels: dict[str, str]) -> dict[str, Any]:
    cp_src = sil.get("convention_profile", {}) if isinstance(sil, dict) else {}
    src_rules = cp_src.get("rules", []) if isinstance(cp_src.get("rules", []), list) else []
    src_templates = cp_src.get("scaffold_templates", []) if isinstance(cp_src.get("scaffold_templates", []), list) else []

    rules: list[dict[str, Any]] = []
    for idx, raw in enumerate(src_rules, start=1):
        if not isinstance(raw, dict):
            continue
        rule_id = str(raw.get("id", "")).strip() or f"CP-RULE-{idx:03d}"
        examples_raw = raw.get("examples", []) if isinstance(raw.get("examples", []), list) else []
        counter_raw = raw.get("counterexamples", []) if isinstance(raw.get("counterexamples", []), list) else []

        def _ex(val: Any) -> dict[str, Any]:
            if isinstance(val, dict):
                out = {"file": str(val.get("file", "unknown"))}
                if val.get("start_line"):
                    out["start_line"] = int(val.get("start_line"))
                if val.get("end_line"):
                    out["end_line"] = int(val.get("end_line"))
                if val.get("note"):
                    out["note"] = str(val.get("note"))
                return out
            return {"file": str(val)}

        rules.append(
            {
                "id": rule_id,
                "category": _map_cp_category(raw.get("category", "naming")),
                "title": str(raw.get("title", raw.get("rule", f"Convention Rule {idx}"))),
                "scope": {
                    "languages": list(raw.get("languages", [])) if isinstance(raw.get("languages", []), list) else [],
                    "file_globs": list(raw.get("file_globs", [])) if isinstance(raw.get("file_globs", []), list) else [],
                    "scm_node_ids": list(raw.get("scm_node_ids", [])) if isinstance(raw.get("scm_node_ids", []), list) else [],
                    "applies_to": str(raw.get("applies_to", "all")),
                },
                "statement": str(raw.get("statement", raw.get("rule", ""))),
                "detection": raw.get("detection", {}) if isinstance(raw.get("detection", {}), dict) else {},
                "enforcement": raw.get("enforcement", {}) if isinstance(raw.get("enforcement", {}), dict) else {},
                "examples": [_ex(x) for x in examples_raw],
                "counterexamples": [_ex(x) for x in counter_raw],
                "evidence": _evidence_list(raw.get("provenance", [])),
                "confidence": _confidence(raw.get("confidence", 0.6), "from convention extraction"),
                "exceptions": list(raw.get("exceptions", [])) if isinstance(raw.get("exceptions", []), list) else [],
            }
        )

    templates: list[dict[str, Any]] = []
    for idx, t in enumerate(src_templates, start=1):
        if isinstance(t, dict):
            templates.append(
                {
                    "id": str(t.get("id", "")).strip() or f"TPL-{idx:03d}",
                    "language": str(t.get("language", "generic")),
                    "name": str(t.get("name", f"Template {idx}")),
                    "description": str(t.get("description", "generated scaffold template")),
                    "file_path_template": str(t.get("file_path_template", "")),
                    "parameters": list(t.get("parameters", [])) if isinstance(t.get("parameters", []), list) else [],
                    "content_snippet": str(t.get("content_snippet", "")),
                }
            )
        elif isinstance(t, str) and t.strip():
            templates.append(
                {
                    "id": f"TPL-{idx:03d}",
                    "language": "generic",
                    "name": t.strip(),
                    "description": "generated scaffold template",
                    "file_path_template": "",
                    "parameters": [],
                    "content_snippet": "",
                }
            )

    artifact = {
        "header": _header("convention_profile", repo_ref=repo_ref, generated_by=generated_by, labels=labels),
        "rules": rules,
        "templates": templates,
        "overrides": [],
    }
    return _with_digest(artifact)


def _norm_path(value: str) -> str:
    out = str(value or "").strip().replace("\\", "/")
    while out.startswith("./"):
        out = out[2:]
    return out


def _resolve_scm_node_ids(scm: dict[str, Any], candidates: Any) -> list[str]:
    if not isinstance(candidates, list):
        return []

    nodes = scm.get("graph", {}).get("nodes", []) if isinstance(scm.get("graph", {}), dict) else []
    id_set: set[str] = set()
    name_map: dict[str, list[str]] = {}
    path_map: dict[str, list[str]] = {}

    for n in nodes:
        if not isinstance(n, dict):
            continue
        node_id = str(n.get("id", "")).strip()
        if not node_id:
            continue
        id_set.add(node_id)
        name_key = str(n.get("name", "")).strip().lower()
        if name_key:
            name_map.setdefault(name_key, []).append(node_id)
        repo_paths = n.get("repo_paths", []) if isinstance(n.get("repo_paths", []), list) else []
        for p in repo_paths:
            np = _norm_path(str(p))
            if not np:
                continue
            path_map.setdefault(np, []).append(node_id)
            base = np.rsplit("/", 1)[-1]
            if base:
                path_map.setdefault(base, []).append(node_id)

    resolved: list[str] = []
    seen: set[str] = set()
    for raw in candidates:
        token = str(raw or "").strip()
        if not token:
            continue
        matched: list[str] = []
        if token in id_set:
            matched = [token]
        else:
            low = token.lower()
            if low in name_map:
                matched = name_map[low]
            else:
                np = _norm_path(token)
                if np in path_map:
                    matched = path_map[np]
                else:
                    base = np.rsplit("/", 1)[-1]
                    if base and base in path_map:
                        matched = path_map[base]
        for node_id in matched:
            if node_id not in seen:
                seen.add(node_id)
                resolved.append(node_id)
    return resolved


def _build_hab(sil: dict[str, Any], scm: dict[str, Any], repo_ref: dict[str, Any], generated_by: dict[str, Any], labels: dict[str, str]) -> dict[str, Any]:
    ha_src = sil.get("health_assessment", {}) if isinstance(sil, dict) else {}
    rb_src = sil.get("remediation_backlog", []) if isinstance(sil.get("remediation_backlog", []), list) else []

    findings: list[dict[str, Any]] = []

    hotspots = ha_src.get("hotspots", []) if isinstance(ha_src.get("hotspots", []), list) else []
    for idx, h in enumerate(hotspots, start=1):
        if not isinstance(h, dict):
            continue
        sev = str(h.get("severity", "medium"))
        findings.append(
            {
                "id": f"F-HOT-{idx:03d}",
                "type": "hotspot",
                "severity": "high" if sev == "high" else ("low" if sev == "low" else "medium"),
                "title": f"Hotspot: {str(h.get('scope', 'unknown scope'))}",
                "description": str(h.get("reason", "Repository hotspot detected")),
                "risk": _risk_from_severity(sev, "hotspot severity"),
                "affected": {
                    "scm_node_ids": [str(h.get("scope", ""))] if str(h.get("scope", "")).strip() else [],
                    "paths": [str(h.get("scope", ""))] if str(h.get("scope", "")).strip() else [],
                },
                "evidence": _evidence_list(h.get("provenance", [])),
                "suggested_remediation": str(h.get("suggested_remediation", "Prioritize stabilization and targeted tests")),
                "effort": _effort("M", _score(h.get("confidence", 0.6), 0.6)),
                "confidence": _confidence(h.get("confidence", 0.6), "from hotspot analysis"),
            }
        )

    risks = ha_src.get("risks", []) if isinstance(ha_src.get("risks", []), list) else []
    for idx, r in enumerate(risks, start=1):
        if not isinstance(r, dict):
            continue
        sev = str(r.get("severity", "medium"))
        findings.append(
            {
                "id": f"F-RSK-{idx:03d}",
                "type": "operational_risk",
                "severity": "critical" if sev == "critical" else ("high" if sev == "high" else ("low" if sev == "low" else "medium")),
                "title": str(r.get("title", f"Risk {idx}")),
                "description": str(r.get("description", "Risk identified")),
                "risk": _risk_from_severity(sev, "risk item severity"),
                "affected": {"scm_node_ids": [], "paths": []},
                "evidence": _evidence_list(r.get("evidence", [])),
                "suggested_remediation": str(r.get("suggested_remediation", "Mitigate through targeted remediation plan")),
                "effort": _effort("S", _score(r.get("confidence", 0.6), 0.6)),
                "confidence": _confidence(r.get("confidence", 0.6), "from risk analysis"),
            }
        )

    backlog: list[dict[str, Any]] = []
    finding_ids = [f["id"] for f in findings]

    for idx, item in enumerate(rb_src, start=1):
        if not isinstance(item, dict):
            continue
        sev = str(item.get("severity", "medium"))
        linked = list(item.get("linked_findings", [])) if isinstance(item.get("linked_findings", []), list) else []
        if not linked and finding_ids:
            linked = [finding_ids[min(idx - 1, len(finding_ids) - 1)]]
        plan_raw = str(item.get("suggested_approach", "")).strip()
        plan = [p.strip() for p in re.split(r"[\n\.;]+", plan_raw) if p.strip()] or ["Implement remediation changes", "Add validation tests", "Verify results"]

        backlog.append(
            {
                "id": str(item.get("id", "")).strip() or f"B-{idx:03d}",
                "title": str(item.get("title", f"Remediation Item {idx}")),
                "priority": "P0" if sev == "critical" else ("P1" if sev == "high" else ("P2" if sev == "medium" else "P3")),
                "business_value": str(item.get("risk_if_unaddressed", "Reduce operational and modernization risk")),
                "risk": _risk_from_severity(sev, "derived from backlog severity"),
                "effort": _effort(item.get("effort", "M"), _score(item.get("confidence", 0.6), 0.6)),
                "linked_findings": linked,
                "scm_impact": {
                    "scm_node_ids": _resolve_scm_node_ids(
                        scm,
                        item.get("scope", []) if isinstance(item.get("scope", []), list) else [],
                    ),
                    "expected_edges_touched": list(item.get("expected_edges_touched", [])) if isinstance(item.get("expected_edges_touched", []), list) else [],
                },
                "dependencies": list(item.get("dependencies", [])) if isinstance(item.get("dependencies", []), list) else [],
                "plan": plan,
                "acceptance_criteria": list(item.get("success_criteria", [])) if isinstance(item.get("success_criteria", []), list) else ["All tests pass", "No regression in quality gates"],
                "owner_role": str(item.get("owner_role", "Tech Lead")),
                "suggested_agent_roles": list(item.get("suggested_agent_roles", [])) if isinstance(item.get("suggested_agent_roles", []), list) else ["developer", "tester"],
            }
        )

    if not backlog and findings:
        for idx, f in enumerate(findings[:3], start=1):
            backlog.append(
                {
                    "id": f"B-AUTO-{idx:03d}",
                    "title": f"Address {f['title']}",
                    "priority": "P1" if f["severity"] in {"critical", "high"} else "P2",
                    "business_value": "Reduce identified risk and improve delivery reliability",
                    "risk": f["risk"],
                    "effort": _effort("M", 0.6),
                    "linked_findings": [f["id"]],
                    "scm_impact": {"scm_node_ids": f.get("affected", {}).get("scm_node_ids", []), "expected_edges_touched": []},
                    "dependencies": [],
                    "plan": ["Implement change", "Add tests", "Validate in staging"],
                    "acceptance_criteria": ["Related failures resolved", "Regression tests pass"],
                    "owner_role": "Tech Lead",
                    "suggested_agent_roles": ["developer", "tester"],
                }
            )

    scores = ha_src.get("scores", {}) if isinstance(ha_src.get("scores", {}), dict) else {}
    raw_scores = [int(scores.get(k, 60)) for k in ["maintainability", "security", "reliability", "testability"]]
    avg = max(1, min(100, int(sum(raw_scores) / max(1, len(raw_scores)))))
    if avg >= 80:
        overall = _risk_from_severity("low", "strong system health")
    elif avg >= 65:
        overall = _risk_from_severity("medium", "moderate risk posture")
    elif avg >= 45:
        overall = _risk_from_severity("high", "elevated risk posture")
    else:
        overall = _risk_from_severity("critical", "critical risk posture")

    artifact = {
        "header": _header("health_assessment_bundle", repo_ref=repo_ref, generated_by=generated_by, labels=labels),
        "summary": {
            "overall_risk": overall,
            "key_themes": [str(ha_src.get("summary", "Health risks identified and prioritized"))],
            "recommendation": "Prioritize P0/P1 remediation items and enforce convention gates in CI",
            "notes": "Generated from SIL health assessment and remediation backlog",
        },
        "signals": {
            "scores": scores,
            "finding_count": len(findings),
            "backlog_count": len(backlog),
        },
        "findings": findings,
        "backlog": backlog,
        "roadmap_options": {
            "quick_wins": [b["id"] for b in backlog if b["priority"] in {"P0", "P1"}][:5],
            "stabilization": [b["id"] for b in backlog if b["priority"] in {"P1", "P2"}][:5],
            "modernization": [b["id"] for b in backlog][:5],
        },
    }
    return _with_digest(artifact)


def _build_context_bundle(
    repo_ref: dict[str, Any],
    *,
    scm: dict[str, Any],
    cp: dict[str, Any],
    hab: dict[str, Any],
) -> dict[str, Any]:
    return {
        "bundle_id": _new_id(),
        "created_at": _utc_now(),
        "repo_ref": repo_ref,
        "artifacts": [
            {
                "artifact_type": "system_context_model",
                "artifact_id": scm["header"]["artifact_id"],
                "digest": scm["header"]["digest"],
                "uri": "system_context_model.json",
            },
            {
                "artifact_type": "convention_profile",
                "artifact_id": cp["header"]["artifact_id"],
                "digest": cp["header"]["digest"],
                "uri": "convention_profile.json",
            },
            {
                "artifact_type": "health_assessment_bundle",
                "artifact_id": hab["header"]["artifact_id"],
                "digest": hab["header"]["digest"],
                "uri": "health_assessment_bundle.json",
            },
        ],
        "policy_pack": {
            "id": "brownfield-context-first",
            "version": "1.0",
            "labels": {
                "governance": "enabled",
                "hard_gates": "true",
            },
        },
        "constraints": {
            "require_context_bundle_for_downstream": True,
            "immutable_artifacts": True,
            "no_inplace_mutation": True,
        },
    }


def _semantic_validation(
    scm: dict[str, Any],
    cp: dict[str, Any],
    hab: dict[str, Any],
    *,
    repo_root: Path | None = None,
) -> list[str]:
    issues: list[str] = []

    nodes = scm.get("graph", {}).get("nodes", []) if isinstance(scm.get("graph", {}), dict) else []
    edges = scm.get("graph", {}).get("edges", []) if isinstance(scm.get("graph", {}), dict) else []
    node_ids = [str(n.get("id", "")) for n in nodes if isinstance(n, dict)]
    node_set = set(node_ids)

    if len(node_set) != len(node_ids):
        issues.append("SCM semantic gate: duplicate node IDs detected")

    for e in edges:
        if not isinstance(e, dict):
            continue
        if str(e.get("from", "")) not in node_set or str(e.get("to", "")) not in node_set:
            issues.append(f"SCM semantic gate: edge endpoints missing for edge `{e.get('id', 'unknown')}`")
        et = str(e.get("type", ""))
        conf = _score(e.get("confidence", {}).get("score", 0.0) if isinstance(e.get("confidence", {}), dict) else 0.0, 0.0)
        if et == "calls_http" and conf < 0.5:
            issues.append(f"SCM semantic gate: low-confidence calls_http edge `{e.get('id', 'unknown')}` should be marked as hypothesis/unknown")

    for n in nodes:
        if not isinstance(n, dict):
            continue
        if str(n.get("type", "")) == "service":
            has_signal = bool(n.get("repo_paths")) or bool(n.get("runtime")) or bool(n.get("interfaces"))
            if not has_signal:
                issues.append(f"SCM semantic gate: service node `{n.get('id', 'unknown')}` missing repo/runtime/interface signals")

    rules = cp.get("rules", []) if isinstance(cp.get("rules", []), list) else []
    for r in rules:
        if not isinstance(r, dict):
            continue
        if not isinstance(r.get("scope", {}), dict):
            issues.append(f"CP semantic gate: rule `{r.get('id', 'unknown')}` missing scope object")
        if not str(r.get("statement", "")).strip():
            issues.append(f"CP semantic gate: rule `{r.get('id', 'unknown')}` missing statement")
        conf = r.get("confidence", {}) if isinstance(r.get("confidence", {}), dict) else {}
        if "score" not in conf:
            issues.append(f"CP semantic gate: rule `{r.get('id', 'unknown')}` missing confidence.score")
        enf = r.get("enforcement", {}) if isinstance(r.get("enforcement", {}), dict) else {}
        if enf.get("lint_tool") and enf.get("config_path") and repo_root:
            cfg = (repo_root / str(enf.get("config_path"))).resolve()
            if not cfg.exists():
                issues.append(f"CP semantic gate: lint config path not found for rule `{r.get('id', 'unknown')}`: {cfg}")

    findings = hab.get("findings", []) if isinstance(hab.get("findings", []), list) else []
    finding_ids = {str(f.get("id", "")) for f in findings if isinstance(f, dict)}
    backlog = hab.get("backlog", []) if isinstance(hab.get("backlog", []), list) else []
    for b in backlog:
        if not isinstance(b, dict):
            continue
        linked = b.get("linked_findings", []) if isinstance(b.get("linked_findings", []), list) else []
        if not linked:
            issues.append(f"HAB semantic gate: backlog item `{b.get('id', 'unknown')}` missing linked_findings")
        for fid in linked:
            if str(fid) not in finding_ids:
                issues.append(f"HAB semantic gate: backlog item `{b.get('id', 'unknown')}` references unknown finding `{fid}`")
        if not isinstance(b.get("plan", []), list) or not b.get("plan"):
            issues.append(f"HAB semantic gate: backlog item `{b.get('id', 'unknown')}` missing plan")
        if not isinstance(b.get("acceptance_criteria", []), list) or not b.get("acceptance_criteria"):
            issues.append(f"HAB semantic gate: backlog item `{b.get('id', 'unknown')}` missing acceptance_criteria")
        impact_nodes = (
            b.get("scm_impact", {}).get("scm_node_ids", [])
            if isinstance(b.get("scm_impact", {}), dict)
            else []
        )
        for nid in impact_nodes:
            if str(nid) and str(nid) not in node_set:
                issues.append(f"HAB semantic gate: backlog item `{b.get('id', 'unknown')}` references unknown SCM node `{nid}`")

    return issues


def _schema_validation(
    schema_dir: Path,
    *,
    scm: dict[str, Any],
    cp: dict[str, Any],
    hab: dict[str, Any],
    bundle: dict[str, Any],
) -> list[str]:
    try:
        from jsonschema import Draft202012Validator, RefResolver
    except Exception:
        # jsonschema is optional; semantic validation still enforced.
        return ["Schema validation skipped: jsonschema package not installed"]

    schema_files = {
        "system_context_model": "system_context_model.schema.json",
        "convention_profile": "convention_profile.schema.json",
        "health_assessment_bundle": "health_assessment_bundle.schema.json",
        "context_bundle": "context_bundle.schema.json",
    }

    store: dict[str, Any] = {}
    loaded: dict[str, Any] = {}
    for name, rel in schema_files.items():
        path = schema_dir / rel
        if not path.exists():
            return [f"Schema validation failed: schema file missing `{path}`"]
        schema = json.loads(path.read_text())
        loaded[name] = schema
        store[rel] = schema
        if schema.get("$id"):
            store[str(schema["$id"])] = schema
        store[path.resolve().as_uri()] = schema

    common_path = schema_dir / "common.schema.json"
    if common_path.exists():
        common = json.loads(common_path.read_text())
        store["common.schema.json"] = common
        if common.get("$id"):
            store[str(common["$id"])] = common
        store[common_path.resolve().as_uri()] = common

    targets = [
        ("system_context_model", scm),
        ("convention_profile", cp),
        ("health_assessment_bundle", hab),
        ("context_bundle", bundle),
    ]

    errors: list[str] = []
    for key, payload in targets:
        schema = loaded[key]
        base_uri = (schema_dir / schema_files[key]).resolve().as_uri()
        resolver = RefResolver(base_uri=base_uri, referrer=schema, store=store)
        validator = Draft202012Validator(schema, resolver=resolver)
        for err in validator.iter_errors(payload):
            path = "/".join(str(x) for x in err.absolute_path)
            errors.append(f"Schema gate `{key}` failed at `{path or '$'}`: {err.message}")
    return errors


def build_context_contract_suite(
    sil_output: dict[str, Any],
    *,
    context_ref: dict[str, Any],
    run_id: str,
    schema_dir: Path,
    repo_root: Path,
    labels: dict[str, str] | None = None,
    model: str = "",
) -> dict[str, Any]:
    labels = labels or {}
    repo_ref = _repo_ref(context_ref)
    generated_by = _generated_by(context_ref, run_id, model=model)

    scm = _build_scm(sil_output, repo_ref, generated_by, labels)
    cp = _build_cp(sil_output, repo_ref, generated_by, labels)
    hab = _build_hab(sil_output, scm, repo_ref, generated_by, labels)
    bundle = _build_context_bundle(repo_ref, scm=scm, cp=cp, hab=hab)

    schema_issues = _schema_validation(schema_dir, scm=scm, cp=cp, hab=hab, bundle=bundle)
    semantic_issues = _semantic_validation(scm, cp, hab, repo_root=repo_root)

    return {
        "system_context_model": scm,
        "convention_profile": cp,
        "health_assessment_bundle": hab,
        "context_bundle": bundle,
        "validation_report": {
            "generated_at": _utc_now(),
            "schema_issues": schema_issues,
            "semantic_issues": semantic_issues,
            "is_valid": not any(i for i in schema_issues + semantic_issues if not i.startswith("Schema validation skipped")),
            "severity": "error" if (schema_issues or semantic_issues) else "ok",
        },
    }


def persist_context_contract_suite(suite: dict[str, Any], output_dir: Path) -> dict[str, str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    paths = {
        "system_context_model": output_dir / "system_context_model.json",
        "convention_profile": output_dir / "convention_profile.json",
        "health_assessment_bundle": output_dir / "health_assessment_bundle.json",
        "context_bundle": output_dir / "context_bundle.json",
        "validation_report": output_dir / "validation_report.json",
    }
    for key, path in paths.items():
        payload = suite.get(key, {})
        path.write_text(json.dumps(payload, indent=2, ensure_ascii=True, default=str))

    md = [
        "# Context Contract Bundle",
        "",
        f"- Generated at: {suite.get('validation_report', {}).get('generated_at', _utc_now())}",
        f"- Valid: {suite.get('validation_report', {}).get('is_valid', False)}",
        "",
        "## Artifacts",
        "- system_context_model.json",
        "- convention_profile.json",
        "- health_assessment_bundle.json",
        "- context_bundle.json",
        "- validation_report.json",
    ]
    (output_dir / "README.md").write_text("\n".join(md))
    return {k: str(v) for k, v in paths.items()}
