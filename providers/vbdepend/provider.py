from __future__ import annotations

import re
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from providers.base_provider import BaseEvidenceProvider
from providers.types import EvidenceBundle, EvidenceFileRef, ProbeResult
from utils.landscape_router import default_router_ruleset

try:
    from pypdf import PdfReader
except Exception:  # pragma: no cover
    PdfReader = None


KNOWN_CONTROL_TYPES = {
    "Frame", "CommandButton", "TextBox", "Label", "ComboBox", "OptionButton", "DTPicker", "ProgressBar",
    "Image", "Timer", "Shape", "ListView", "ListItem", "Control", "DateTime", "Strings", "Conversion",
    "Interaction", "Recordset", "Connection", "Command", "IListItems", "IListItem", "Er", "Form",
}


class VBDependProvider(BaseEvidenceProvider):
    provider_id = "vbdepend"
    provider_version = "1.0.0"

    def capabilities(self) -> dict[str, Any]:
        return {
            "provides_metrics": True,
            "provides_dependency_graph": True,
            "provides_sql_catalog": False,
            "provides_event_handler_inventory": False,
            "provides_db_schema": False,
            "provides_dead_code": True,
            "provides_trends": True,
        }

    def probe(self, bundle: EvidenceBundle) -> ProbeResult:
        matched: list[str] = []
        reasons: list[str] = []
        confidence = 0.0
        for ref in bundle.files:
            name = ref.file_name.lower()
            if "vbdepend" in name:
                matched.append(ref.file_id)
                confidence += 0.18
                reasons.append(f"filename matches VBDepend export: {ref.file_name}")
                continue
            text = self._read_file_text(ref)
            header = text[:1200].lower()
            if any(token in header for token in ["types dependencies", "projects metrics", "application statistics", "dead code", "trend charts"]):
                matched.append(ref.file_id)
                confidence += 0.25
                reasons.append(f"header matches VBDepend report: {ref.file_name}")
        confidence = min(0.99, confidence)
        return ProbeResult(
            provider_id=self.provider_id,
            provider_version=self.provider_version,
            tool_id="vbdepend",
            tool_vendor="VBDepend",
            tool_version="",
            confidence=confidence,
            matched_file_ids=matched,
            reasons=reasons or ["No VBDepend signatures detected."],
            capabilities=self.capabilities(),
        )

    def extract(self, bundle: EvidenceBundle) -> dict[str, Any]:
        datasets: dict[str, Any] = {
            "application_statistics": {},
            "project_metrics": {"rows": []},
            "type_metrics": {"rows": []},
            "type_dependencies": {"rows": []},
            "dead_code": {"types": [], "methods": [], "fields": []},
            "trend_snapshot": {},
            "trend_series": {"points": []},
        }
        provenance: dict[str, Any] = {}
        for ref in bundle.files:
            text = self._read_file_text(ref)
            if not text.strip():
                continue
            report_kind = self._report_kind(ref.file_name, text)
            if report_kind == "project_metrics":
                rows = self._parse_project_metrics(text)
                datasets["project_metrics"]["rows"].extend(rows)
                provenance[report_kind] = self._prov(ref, 1, "Projects Metrics")
            elif report_kind == "type_metrics":
                rows = self._parse_type_metrics(text)
                datasets["type_metrics"]["rows"].extend(rows)
                provenance[report_kind] = self._prov(ref, 1, "Types Metrics : Code Quality")
            elif report_kind == "type_dependencies":
                rows = self._parse_type_dependencies(text)
                datasets["type_dependencies"]["rows"].extend(rows)
                provenance[report_kind] = self._prov(ref, 1, "Types Dependencies")
            elif report_kind == "dead_code":
                dead = self._parse_dead_code(text)
                for key in ("types", "methods", "fields"):
                    datasets["dead_code"][key].extend(dead.get(key, []))
                provenance[report_kind] = self._prov(ref, 1, "Dead Code")
            elif report_kind == "trend":
                trend = self._parse_trends(text)
                datasets["trend_snapshot"] = trend.get("snapshot", {})
                datasets["trend_series"] = {"points": trend.get("points", [])}
                provenance[report_kind] = self._prov(ref, 1, "Trend Charts")
            elif report_kind == "application_statistics":
                stats = self._parse_application_statistics(text)
                datasets["application_statistics"] = stats
                provenance[report_kind] = self._prov(ref, 1, "Application Statistics")
        return {
            "meta": {
                "artifact_type": "tool_extraction_v1",
                "artifact_version": "1.0",
                "tool_id": "vbdepend",
                "provider_id": self.provider_id,
                "provider_version": self.provider_version,
                "extracted_at": datetime.now(timezone.utc).isoformat(),
            },
            "datasets": datasets,
            "provenance": provenance,
            "bundle_id": bundle.bundle_id,
        }

    def normalize(self, extraction: dict[str, Any]) -> dict[str, Any]:
        datasets = extraction.get("datasets", {}) if isinstance(extraction.get("datasets", {}), dict) else {}
        project_rows = datasets.get("project_metrics", {}).get("rows", []) if isinstance(datasets.get("project_metrics", {}), dict) else []
        type_rows = datasets.get("type_metrics", {}).get("rows", []) if isinstance(datasets.get("type_metrics", {}), dict) else []
        dep_rows = datasets.get("type_dependencies", {}).get("rows", []) if isinstance(datasets.get("type_dependencies", {}), dict) else []
        dead = datasets.get("dead_code", {}) if isinstance(datasets.get("dead_code", {}), dict) else {}
        app_stats = datasets.get("application_statistics", {}) if isinstance(datasets.get("application_statistics", {}), dict) else {}
        trend_snapshot = datasets.get("trend_snapshot", {}) if isinstance(datasets.get("trend_snapshot", {}), dict) else {}
        trend_series = datasets.get("trend_series", {}) if isinstance(datasets.get("trend_series", {}), dict) else {}
        provenance = extraction.get("provenance", {}) if isinstance(extraction.get("provenance", {}), dict) else {}

        known_types = {str(row.get("type_name", "")).strip() for row in type_rows if str(row.get("type_name", "")).strip()}
        type_edges: list[dict[str, Any]] = []
        runtime_edges: list[dict[str, Any]] = []
        runtime_counter: Counter[str] = Counter()
        runtime_forms: defaultdict[str, set[str]] = defaultdict(set)
        for row in dep_rows:
            source = str(row.get("source", "")).strip()
            for dep in row.get("depends_on", []):
                target = str(dep or "").strip()
                if not source or not target:
                    continue
                if target in known_types:
                    type_edges.append({
                        "source": source,
                        "target": target,
                        "kind": "type_dependency",
                        "confidence": 0.88,
                    })
                else:
                    kind = self._runtime_kind(target)
                    runtime_edges.append({
                        "source": source,
                        "target": target,
                        "kind": kind,
                        "confidence": 0.76,
                    })
                    runtime_counter[target] += 1
                    runtime_forms[target].add(source)

        total_loc = sum(int(row.get("loc", 0) or 0) for row in type_rows)
        project_count = len(project_rows)
        type_count = len(type_rows)
        risk_rows: list[dict[str, Any]] = []
        if project_count > 1:
            risk_rows.append({
                "signal_id": "SIG_VARIANTS_001",
                "severity": "high",
                "title": "Multiple VB projects detected in imported analysis",
                "description": "The imported evidence references multiple projects or variants that may need canonical selection.",
                "recommendation": "Confirm canonical project(s) before scope lock.",
            })
        if any(int(row.get("cyclomatic_complexity", 0) or 0) >= 40 for row in type_rows):
            risk_rows.append({
                "signal_id": "SIG_HOTSPOT_001",
                "severity": "medium",
                "title": "Hotspot types detected",
                "description": "Imported code-quality metrics show at least one highly complex type that should be prioritized in analysis and planning.",
                "recommendation": "Review hotspot types in Code Quality and sequence them early in the track plan.",
            })
        risk_rows.append({
            "signal_id": "SIG_EVIDENCE_DATA_001",
            "severity": "medium",
            "title": "Behavioral and data evidence are incomplete",
            "description": "VBDepend imports provide architecture and code-quality evidence, but do not provide SQL catalogs, DB schema, or event-handler parity evidence.",
            "recommendation": "Upload DB exports, query catalogs, or walkthrough evidence before code-generation commitments.",
        })

        components = []
        for row in project_rows or [{"project": "Imported VB6 system", "loc": total_loc, "type_count": type_count}]:
            project_name = str(row.get("project", "Imported VB6 system")).strip() or "Imported VB6 system"
            loc = int(row.get("loc", total_loc) or total_loc)
            components.append({
                "component_id": f"cmp_{self._slug(project_name)}",
                "name": project_name,
                "component_kind": "vb6_project",
                "root_paths": [f"evidence://{project_name}"],
                "project_files": [project_name],
                "language_mix": [{"language": "VB6", "percent_loc": 100.0}],
                "stats": {"files": 0, "loc": loc, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": loc * 4},
                "archetypes": ["desktop_forms_vb6"],
                "datastore_touch": [],
                "dependency_footprint": {"ocx": sum(1 for dep in runtime_counter if dep.lower().endswith("ocx")), "com": 0, "nuget": 0, "npm": 0},
                "risk_flags": ["external_evidence_mode"],
                "variant_candidate": project_count > 1,
                "shared_foundation_candidate": False,
                "confidence": 0.84,
                "recommended_skill_packs": ["vb6_forms_pack", "evidence_mode_pack"],
                "recommended_agents": ["VB6FormsAgent", "EvidenceQAGateAgent"],
                "routing_rules_fired": ["R_VB6_FORMS", "R_EVIDENCE_MODE"],
                "chunking_profile": "evidence_import",
                "evidence": [provenance.get("project_metrics", {})] if provenance.get("project_metrics") else [],
            })

        tracks = []
        for component in components:
            tracks.append({
                "track_id": f"TRK_{self._slug(component['component_id'])}_001",
                "title": f"{component['name']} evidence-backed modernization",
                "lane": "ui_modernization",
                "source_components": [component["component_id"]],
                "suggested_target": "Target UI/data strategy to be confirmed",
                "recommended_skill_packs": component["recommended_skill_packs"],
                "quality_gates": ["Evidence coverage reviewed", "Scope confirmed"],
                "confidence": component["confidence"],
                "why": "Derived from imported VBDepend architecture and code-quality evidence.",
                "risks": component["risk_flags"],
                "gating_questions": [
                    "Confirm canonical .vbp variant(s).",
                    "Confirm target UI strategy: Web vs WPF/WinUI.",
                    "Provide DB schema/query evidence for parity-sensitive workflows.",
                ],
            })

        ruleset = default_router_ruleset(repo=f"evidence-bundle:{extraction.get('bundle_id', '')}", branch="imported", commit_sha="evidence")
        coverage = self._coverage_report(type_rows=type_rows, dep_rows=dep_rows)
        followups = self.suggested_followups(coverage)
        evidence_banner = "Imported Analysis — Results are evidence-backed and may require verification before build."

        repo_landscape_v1 = {
            "meta": self._meta("repo_landscape", extraction),
            "landscape_mode": "brownfield",
            "source_mode": "imported_analysis",
            "source_banner": evidence_banner,
            "scan_summary": {
                "root_paths_scanned": ["evidence_bundle"],
                "excluded_paths": [],
                "included_paths": [],
                "total_files": 0,
                "binary_files": 0,
                "total_loc": total_loc,
                "estimated_tokens": total_loc * 4,
                "duration_ms": 0,
                "largest_files": [],
                "largest_directories": [],
                "notes": [
                    "Landscape derived from imported analysis outputs rather than direct repo scan.",
                    "Behavioral and data evidence may be incomplete unless additional artifacts are uploaded.",
                ],
            },
            "languages": [{"language": "VB6", "stats": {"files": 0, "loc": total_loc, "blank_loc": 0, "comment_loc": 0, "estimated_tokens": total_loc * 4}, "percent_loc": 100.0, "confidence": 0.9, "evidence": [provenance.get("type_metrics", {})] if provenance.get("type_metrics") else []}],
            "build_systems": [{"kind": "vb6_vbp", "paths": [str(row.get('project', 'Imported VB6 system')) for row in project_rows] or ["Imported VB6 system"], "confidence": 0.86, "evidence": [provenance.get("project_metrics", {})] if provenance.get("project_metrics") else []}],
            "archetypes": [{"archetype": "desktop_forms_vb6", "confidence": 0.9, "primary_evidence": [provenance.get("type_metrics", {})] if provenance.get("type_metrics") else [], "notes": ["Detected from VBDepend type metrics and dependencies."]}],
            "datastore_signals": [],
            "dependency_footprint": {
                "ocx_count": sum(1 for dep in runtime_counter if dep.lower().endswith("ocx")),
                "com_dll_count": sum(1 for dep in runtime_counter if dep.lower().endswith(("dll", "tlb"))),
                "nuget_package_count": 0,
                "npm_package_count": 0,
                "java_dependency_count": 0,
                "python_dependency_count": 0,
                "top_dependencies": [name for name, _ in runtime_counter.most_common(8)],
            },
            "high_risk_signals": risk_rows,
        }
        component_inventory_v1 = {
            "meta": self._meta("component_inventory", extraction),
            "graph_summary": {"component_count": len(components), "edge_count": 0, "cross_language_edges": 0, "shared_db_edges": 0, "notes": ["Built from imported analysis outputs."]},
            "components": components,
            "edges": [],
        }
        modernization_track_plan_v1 = {
            "meta": self._meta("modernization_track_plan", extraction),
            "tracks": tracks,
            "assumptions": [
                "Imported analysis is treated as upstream evidence, not as parity-complete behavior proof.",
                "Build output should remain evidence-backed until behavior/data gaps are resolved.",
            ],
            "open_questions": coverage.get("blockers", []) + followups,
        }
        project_metrics_artifact = {
            "meta": self._meta("project_metrics", extraction),
            "rows": project_rows,
            "provenance": provenance.get("project_metrics", {}),
        }
        type_metrics_artifact = {
            "meta": self._meta("type_metrics", extraction),
            "rows": type_rows,
            "provenance": provenance.get("type_metrics", {}),
        }
        dead_code_report = {
            "meta": self._meta("dead_code_report", extraction),
            "summary": {
                "dead_type_candidates": len(dead.get("types", [])),
                "dead_method_candidates": len(dead.get("methods", [])),
                "dead_field_candidates": len(dead.get("fields", [])),
            },
            "probable_dead_types": dead.get("types", []),
            "probable_dead_methods": dead.get("methods", []),
            "probable_dead_fields": dead.get("fields", []),
            "candidates": [*dead.get("types", []), *dead.get("methods", []), *dead.get("fields", [])],
            "provenance": provenance.get("dead_code", {}),
        }
        type_dependency_matrix = {
            "meta": self._meta("type_dependency_matrix", extraction),
            "edges": type_edges,
            "provenance": provenance.get("type_dependencies", {}),
        }
        runtime_dependency_matrix = {
            "meta": self._meta("runtime_dependency_matrix", extraction),
            "edges": runtime_edges,
            "provenance": provenance.get("type_dependencies", {}),
        }
        third_party_usage = {
            "meta": self._meta("third_party_usage", extraction),
            "rows": [
                {
                    "dependency": dep,
                    "kind": self._runtime_kind(dep),
                    "forms_using_count": len(runtime_forms.get(dep, set())),
                    "usage_intensity": count,
                }
                for dep, count in runtime_counter.most_common(24)
            ],
        }
        trend_snapshot_artifact = {
            "meta": self._meta("trend_snapshot", extraction),
            "snapshot": {
                "captured_at": trend_snapshot.get("captured_at", ""),
                "metrics": {
                    "loc_total": total_loc,
                    "avg_complexity": round(sum(float(row.get("cyclomatic_complexity", 0) or 0) for row in type_rows) / max(1, len(type_rows)), 2),
                    "max_complexity": max([int(row.get("cyclomatic_complexity", 0) or 0) for row in type_rows] or [0]),
                    "hotspot_count": len([row for row in type_rows if int(row.get("cyclomatic_complexity", 0) or 0) >= 20]),
                    "critical_rules_violated": int(trend_snapshot.get("critical_rules_violated", 0) or 0),
                    "rules_violated": int(trend_snapshot.get("rules_violated", 0) or 0),
                },
            },
        }
        trend_series_artifact = {
            "meta": self._meta("trend_series", extraction),
            "points": trend_series.get("points", []),
        }
        quality_violation_report = {
            "meta": self._meta("quality_violation_report", extraction),
            "summary": {
                "total_violations": int(trend_snapshot.get("rules_violated", 0) or 0),
                "critical_violations": int(trend_snapshot.get("critical_rules_violated", 0) or 0),
            },
            "violations": [
                {
                    "rule_id": "VBDEPEND-RULES",
                    "severity": "critical" if int(trend_snapshot.get("critical_rules_violated", 0) or 0) > 0 else "medium",
                    "subject": "Imported VBDepend trend metrics",
                    "detail": f"Rules violated={int(trend_snapshot.get('rules_violated', 0) or 0)}, critical={int(trend_snapshot.get('critical_rules_violated', 0) or 0)}.",
                }
            ] if trend_snapshot else [],
        }
        code_quality_rules = {
            "meta": self._meta("code_quality_rules", extraction),
            "rules": [
                {
                    "rule_id": "EVIDENCE-ARCH-001",
                    "rule_type": "evidence_gating",
                    "statement": "Imported analysis can guide architecture and planning, but build commitments require sufficient behavior and data coverage or explicit risk acceptance.",
                }
            ],
        }
        static_forensics_layer = {
            "meta": self._meta("static_forensics_layer", extraction),
            "summary": {
                "overall_status": coverage.get("proceed_state", "WARN").upper(),
                "projects": project_count,
                "types": type_count,
                "type_dependency_edges": len(type_edges),
                "runtime_dependency_edges": len(runtime_edges),
                "quality_violations": int(trend_snapshot.get("rules_violated", 0) or 0),
            },
            "checks": coverage.get("checks", []),
        }
        compatibility = self._build_compatibility_payload(
            extraction=extraction,
            project_rows=project_rows,
            type_rows=type_rows,
            runtime_counter=runtime_counter,
            dead=dead,
            coverage=coverage,
            evidence_banner=evidence_banner,
        )
        return {
            "repo_landscape_v1": repo_landscape_v1,
            "component_inventory_v1": component_inventory_v1,
            "modernization_track_plan_v1": modernization_track_plan_v1,
            "router_ruleset_v1": ruleset,
            "project_metrics": project_metrics_artifact,
            "type_metrics": type_metrics_artifact,
            "type_dependency_matrix": type_dependency_matrix,
            "runtime_dependency_matrix": runtime_dependency_matrix,
            "dead_code_report": dead_code_report,
            "third_party_usage": third_party_usage,
            "trend_snapshot": trend_snapshot_artifact,
            "trend_series": trend_series_artifact,
            "quality_violation_report": quality_violation_report,
            "code_quality_rules": code_quality_rules,
            "static_forensics_layer": static_forensics_layer,
            "evidence_coverage_report_v1": coverage,
            **compatibility,
        }

    def suggested_followups(self, coverage: dict[str, Any]) -> list[str]:
        blockers = coverage.get("blockers", []) if isinstance(coverage.get("blockers", []), list) else []
        items: list[str] = []
        if any("db schema" in str(item).lower() for item in blockers):
            items.append("Upload DB schema export or MDB/ACCDB artifacts to unlock data-confidence improvements.")
        if any("behavior" in str(item).lower() or "workflow" in str(item).lower() for item in blockers):
            items.append("Provide workflow walkthroughs, screenshots, or event inventories to improve behavioral coverage.")
        if not items:
            items.append("Review coverage and explicitly accept evidence risk before attempting build-oriented runs.")
        return items

    def _coverage_report(self, *, type_rows: list[dict[str, Any]], dep_rows: list[dict[str, Any]]) -> dict[str, Any]:
        architecture = 80 if type_rows else 20
        dependencies = 78 if dep_rows else 15
        code_quality = 72 if type_rows else 10
        data = 12
        behavior = 10
        ui = 28 if type_rows else 5
        reporting = 20
        security = 34 if type_rows else 5
        blockers = []
        if data < 60:
            blockers.append("DB schema export needed before target schema design can be trusted.")
        if behavior < 60:
            blockers.append("Behavior/workflow evidence is insufficient for functional parity commitments.")
        proceed_state = "allowed_with_risk" if architecture >= 50 else "blocked"
        if behavior >= 60 and data >= 60:
            proceed_state = "allowed"
        checks = [
            {"id": "architecture_coverage", "status": "PASS" if architecture >= 50 else "FAIL", "label": "Architecture evidence", "detail": f"score={architecture}"},
            {"id": "dependency_coverage", "status": "PASS" if dependencies >= 50 else "FAIL", "label": "Dependency evidence", "detail": f"score={dependencies}"},
            {"id": "data_coverage", "status": "FAIL" if data < 60 else "PASS", "label": "Data evidence", "detail": f"score={data}"},
            {"id": "behavior_coverage", "status": "FAIL" if behavior < 60 else "PASS", "label": "Behavior evidence", "detail": f"score={behavior}"},
        ]
        return {
            "meta": {
                "artifact_type": "evidence_coverage_report_v1",
                "artifact_version": "1.0",
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "provider_id": self.provider_id,
            },
            "source_mode": "imported_analysis",
            "dimensions": {
                "architecture": architecture,
                "dependencies": dependencies,
                "code_quality": code_quality,
                "data": data,
                "behavior": behavior,
                "ui": ui,
                "reporting": reporting,
                "security": security,
            },
            "checks": checks,
            "blockers": blockers,
            "proceed_state": proceed_state,
            "build_allowed": behavior >= 60 and data >= 60,
            "plan_allowed": architecture >= 50,
            "notes": [
                "Coverage is deterministic and based on evidence classes present in the imported bundle.",
                "VBDepend provides strong architecture/dependency insight but weak behavior/data coverage on its own.",
            ],
        }

    def _build_compatibility_payload(
        self,
        *,
        extraction: dict[str, Any],
        project_rows: list[dict[str, Any]],
        type_rows: list[dict[str, Any]],
        runtime_counter: Counter[str],
        dead: dict[str, Any],
        coverage: dict[str, Any],
        evidence_banner: str,
    ) -> dict[str, Any]:
        primary_project = str((project_rows[0] if project_rows else {}).get("project", "")).strip() or "Imported VB6 system"
        bundle_id = str(extraction.get("bundle_id", "")).strip()
        forms: list[dict[str, Any]] = []
        reports: list[str] = []
        module_names: list[str] = []
        member_files: list[str] = [f"{self._slug(primary_project)}.vbp"]
        source_loc_by_file: list[dict[str, Any]] = []
        form_loc_total = 0
        module_loc_total = 0
        class_loc_total = 0

        for row in type_rows:
            type_name = str(row.get("type_name", "")).strip()
            if not type_name:
                continue
            loc = int(row.get("loc", 0) or 0)
            kind = self._compat_member_kind(type_name)
            ext = ".frm" if kind == "form" else (".dsr" if kind == "report" else ".bas")
            pseudo_path = f"{self._slug(primary_project)}/{type_name}{ext}"
            member_files.append(pseudo_path)
            source_loc_by_file.append(
                {
                    "path": pseudo_path,
                    "loc": loc,
                    "project_name": primary_project,
                    "kind": kind,
                    "evidence": "Imported VBDepend type metrics",
                }
            )
            if kind == "form":
                form_loc_total += loc
                purpose = self._compat_form_purpose(type_name)
                forms.append(
                    {
                        "form_name": type_name,
                        "base_form_name": type_name,
                        "name": type_name,
                        "project_name": primary_project,
                        "project": primary_project,
                        "path": pseudo_path,
                        "source_path": pseudo_path,
                        "source_file": pseudo_path,
                        "source_loc": loc,
                        "purpose": purpose,
                        "display_name": type_name,
                        "controls": [],
                        "event_handlers": [],
                        "status": "mapped",
                    }
                )
            elif kind == "report":
                reports.append(type_name)
            else:
                module_loc_total += loc
                module_names.append(type_name)

        total_loc = sum(int(row.get("loc", 0) or 0) for row in type_rows)
        if not module_loc_total:
            module_loc_total = max(0, total_loc - form_loc_total - class_loc_total)
        ocx_dependencies = [name for name, _ in runtime_counter.most_common() if name.lower().endswith("ocx")]
        dll_dependencies = [name for name, _ in runtime_counter.most_common() if name.lower().endswith(("dll", "tlb"))]
        dead_types = dead.get("types", []) if isinstance(dead.get("types", []), list) else []
        pitfall_detectors: list[dict[str, Any]] = []
        if any(int(row.get("cyclomatic_complexity", 0) or 0) >= 40 for row in type_rows):
            hotspot = max(type_rows, key=lambda row: int(row.get("cyclomatic_complexity", 0) or 0))
            pitfall_detectors.append(
                {
                    "id": "EVID-HOTSPOT-001",
                    "severity": "medium",
                    "title": "High-complexity type identified in imported evidence",
                    "description": f"{str(hotspot.get('type_name', '')).strip() or 'A legacy type'} exceeds the hotspot threshold and should be prioritized for modernization planning.",
                    "recommended_action": "Sequence hotspot remediation early and validate behavior with SMEs.",
                    "evidence": "VBDepend type metrics",
                }
            )
        if dead_types:
            pitfall_detectors.append(
                {
                    "id": "EVID-DEADCODE-001",
                    "severity": "low",
                    "title": "Potential dead types detected",
                    "description": f"Imported dead-code analysis identified {len(dead_types)} probable dead type(s).",
                    "recommended_action": "Review dead code candidates before committing scope or parity assumptions.",
                    "evidence": "VBDepend dead code report",
                }
            )
        pitfall_detectors.append(
            {
                "id": "EVID-COVERAGE-001",
                "severity": "high",
                "title": "Behavior and data evidence remain incomplete",
                "description": "Imported analysis supports architecture and dependency understanding but does not prove SQL, DB schema, or event-handler parity.",
                "recommended_action": "Treat BRD and plan sections as evidence-backed with explicit verification gates before build commitments.",
                "evidence": "Evidence coverage report",
            }
        )

        readiness_score = round(
            (
                float(coverage.get("dimensions", {}).get("architecture", 0) or 0) * 0.45
                + float(coverage.get("dimensions", {}).get("dependencies", 0) or 0) * 0.2
                + float(coverage.get("dimensions", {}).get("code_quality", 0) or 0) * 0.15
                + float(coverage.get("dimensions", {}).get("behavior", 0) or 0) * 0.1
                + float(coverage.get("dimensions", {}).get("data", 0) or 0) * 0.1
            )
            / 100.0,
            4,
        )
        modernization_readiness = {
            "readiness_score": readiness_score,
            "summary": "Evidence-backed planning is viable, but behavior and data parity remain unverified.",
            "signals": [
                "Architecture and dependency coverage are strong enough for planning.",
                "Behavioral and data evidence require explicit SME validation before build commitments.",
            ],
        }

        compatibility_context = {
            "repo": "Imported analysis bundle",
            "repo_url": "",
            "branch": "imported",
            "commit_sha": bundle_id or "evidence-bundle",
            "version_id": bundle_id or "evidence-bundle",
        }
        vb6_projects = [
            {
                "project_name": primary_project,
                "startup_object": forms[0]["form_name"] if forms else "",
                "member_files": member_files[:4000],
                "forms": [row["form_name"] for row in forms][:400],
                "source_loc_total": total_loc,
                "source_loc_forms": form_loc_total,
                "source_loc_modules": module_loc_total,
                "source_loc_classes": class_loc_total,
                "members": len(member_files),
            }
        ]
        legacy_inventory = {
            "summary": {
                "counts": {
                    "vb6_projects": len(vb6_projects),
                    "forms": len(forms),
                    "modules": len(module_names),
                    "reports": len(reports),
                    "source_loc_total": total_loc,
                    "source_loc_forms": form_loc_total,
                    "source_loc_modules": module_loc_total,
                    "source_loc_classes": class_loc_total,
                    "source_files_scanned": len(source_loc_by_file),
                },
                "source_banner": evidence_banner,
                "notes": [
                    "Inventory synthesized from imported VBDepend outputs.",
                    "Behavior, SQL, and DB evidence remain partial until additional artifacts are uploaded.",
                ],
            },
            "vb6_projects": vb6_projects,
            "project_members": member_files[:4000],
            "forms": forms[:800],
            "controls": [],
            "event_handlers": [],
            "bas_module_summary": {
                "module_count": len(module_names),
                "modules": module_names[:1200],
                "procedure_count": 0,
                "note": "Module inventory inferred from type metrics; procedure-level evidence not available in imported mode.",
            },
            "database_tables": [],
            "sql_query_catalog": [],
            "connection_strings": [],
            "database_file_references": [],
            "connection_string_rows": [],
            "database_file_reference_rows": [],
            "module_global_declarations": [],
            "binary_companion_files": [],
            "ui_event_map": [],
            "pitfall_detectors": pitfall_detectors,
            "error_handling_profile": {},
            "modernization_readiness": modernization_readiness,
            "source_loc_by_file": source_loc_by_file[:5000],
            "source_loc_total": total_loc,
            "source_loc_forms": form_loc_total,
            "source_loc_modules": module_loc_total,
            "source_loc_classes": class_loc_total,
            "source_files_scanned": len(source_loc_by_file),
            "form_count_discovered_files": len(forms),
            "form_count_referenced": len(forms),
            "form_count_unmapped_files": 0,
            "ocx_dependencies": ocx_dependencies[:400],
            "dll_dependencies": dll_dependencies[:400],
        }
        vb6_analysis = {
            "project_count": len(vb6_projects),
            "projects": vb6_projects,
            "forms": [row["form_name"] for row in forms][:800],
            "controls": [],
            "activex_dependencies": ocx_dependencies[:400],
            "dependency_references": [
                {"source": dep.get("source"), "target": dep.get("target"), "kind": dep.get("kind")}
                for dep in []
            ],
            "event_handlers": [],
            "event_handler_keys": [],
            "event_handler_count_exact": 0,
            "project_members": member_files[:4000],
            "ui_event_map": [],
            "sql_query_catalog": [],
            "connection_strings": [],
            "database_file_references": [],
            "connection_string_rows": [],
            "database_file_reference_rows": [],
            "module_global_declarations": [],
            "com_surface_map": {
                "late_bound_progids": [],
                "call_by_name_sites": [],
                "createobject_getobject_sites": [],
                "references": ocx_dependencies[:120] + dll_dependencies[:120],
            },
            "win32_declares": [],
            "error_handling_profile": {},
            "pitfall_detectors": pitfall_detectors,
            "modernization_readiness": modernization_readiness,
            "bas_module_summary": legacy_inventory["bas_module_summary"],
            "binary_companion_files": [],
            "source_loc_by_file": source_loc_by_file[:5000],
            "source_loc_total": total_loc,
            "source_loc_forms": form_loc_total,
            "source_loc_modules": module_loc_total,
            "source_loc_classes": class_loc_total,
            "source_files_scanned": len(source_loc_by_file),
        }
        return {
            "project_name": primary_project,
            "analysis_walkthrough": {
                "business_objective_summary": "Imported analysis was used to establish architecture, dependencies, hotspots, and planning constraints without direct source-code access.",
            },
            "context_reference": compatibility_context,
            "legacy_skill_profile": {
                "selected_skill_id": "evidence_mode_pack",
                "version": self.provider_version,
                "confidence": 0.9,
            },
            "source_target_modernization_profile": {
                "source": {
                    "language": "VB6",
                    "framework": "Imported analysis evidence",
                    "repo": "Imported analysis bundle",
                },
                "target": {
                    "language": "TBD",
                    "framework": "TBD",
                },
            },
            "legacy_code_inventory": legacy_inventory,
            "vb6_analysis": vb6_analysis,
            "open_questions": coverage.get("blockers", []),
            "quality_gates": coverage.get("checks", []),
        }

    def _compat_member_kind(self, type_name: str) -> str:
        low = str(type_name or "").strip().lower()
        if low.startswith(("rpt", "report", "datareport")):
            return "report"
        if low in {"main", "menu", "mdi", "mdiform"} or low.startswith(("frm", "form", "mdi")):
            return "form"
        if any(token in low for token in ("login", "splash", "deposit", "withdraw", "customer", "account", "transaction", "search", "report")):
            return "form"
        return "module"

    def _compat_form_purpose(self, type_name: str) -> str:
        low = str(type_name or "").strip().lower()
        if "login" in low:
            return "Authentication and credential validation workflow."
        if "splash" in low:
            return "Startup readiness and application launch workflow."
        if "deposit" in low:
            return "Deposit capture and balance posting workflow."
        if "withdraw" in low:
            return "Withdrawal processing and balance deduction workflow."
        if "customer" in low:
            return "Customer onboarding and maintenance workflow."
        if "account" in low and "type" in low:
            return "Account type maintenance and configuration workflow."
        if "transaction" in low:
            return "Transaction ledger and posting workflow."
        if "search" in low:
            return "Record search and retrieval workflow."
        if low in {"main", "menu", "mdiform"}:
            return "Application navigation and workflow routing."
        if low.startswith(("rpt", "report")):
            return "Operational reporting workflow."
        return "Evidence-backed legacy workflow inferred from imported analysis."

    def _parse_project_metrics(self, text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line in lines:
            if line.startswith("Projects ") or line.startswith("Showing ") or line.startswith("Main "):
                continue
            m = re.match(r"^([A-Za-z0-9_. -]+?)\s+v?[0-9.]+\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)\s+([0-9,]+)$", line)
            if not m:
                continue
            rows.append({
                "project": m.group(1).strip(),
                "loc": int(m.group(2).replace(",", "")),
                "type_count": int(m.group(3).replace(",", "")),
                "abstract_type_count": int(m.group(4).replace(",", "")),
                "comment_loc": int(m.group(5).replace(",", "")),
                "comment_percent": int(m.group(6).replace(",", "")),
                "afferent_coupling": int(m.group(7).replace(",", "")),
                "efferent_coupling": int(m.group(8).replace(",", "")),
            })
        return rows

    def _parse_type_metrics(self, text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        seen: set[str] = set()
        for line in lines:
            if line.startswith(("Types Metrics", "Main ", "Type Name ", "Showing ", "menu")):
                continue
            m = re.match(r"^([A-Za-z0-9_]+)\s+([0-9.]+)\s+([0-9-]+)\s+([0-9-]+)\s+([0-9-]+)\s+([0-9.\-]+)\s+([0-9-]+)$", line)
            if not m:
                continue
            type_name = m.group(1).strip()
            if type_name in seen:
                continue
            seen.add(type_name)
            def _int(raw: str) -> int:
                try:
                    return int(raw.replace(",", "")) if raw not in {"-", ""} else 0
                except Exception:
                    return 0
            def _float(raw: str) -> float:
                try:
                    return float(raw) if raw not in {"-", ""} else 0.0
                except Exception:
                    return 0.0
            rows.append({
                "project": "Imported VB6 system",
                "type_name": type_name,
                "type_rank": _float(m.group(2)),
                "loc": _int(m.group(3)),
                "comment_loc": _int(m.group(4)),
                "comment_percent": _float(m.group(5)),
                "cyclomatic_complexity": _int(m.group(6)),
                "afferent_coupling": _int(m.group(7)),
                "efferent_coupling": 0,
            })
        return rows

    def _parse_type_dependencies(self, text: str) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        for line in lines:
            if line.startswith(("Types Dependencies", "Type Depends on", "Showing ", "Main ", "menu")):
                continue
            m = re.match(r"^([A-Za-z0-9_]+)\s+(.+)$", line)
            if not m:
                continue
            source = m.group(1).strip()
            tail = m.group(2).strip().rstrip(";")
            deps = [part.strip() for part in tail.split(";") if part.strip()]
            if not deps:
                continue
            rows.append({"source": source, "depends_on": deps})
        return rows

    def _parse_dead_code(self, text: str) -> dict[str, Any]:
        out = {"types": [], "methods": [], "fields": []}
        lines = [line.strip() for line in text.splitlines() if line.strip()]
        in_types = False
        for line in lines:
            if line.startswith("types Full Name"):
                in_types = True
                continue
            if in_types:
                if line.startswith("Showing ") or line.startswith("Stat") or line.startswith("methods "):
                    in_types = False
                    continue
                parts = line.split()
                if parts:
                    name = parts[0].strip()
                    if name and name.lower() not in {"types", "dead", "potentially"}:
                        out["types"].append({"kind": "type", "name": name, "confidence": 0.84})
        method_matches = re.search(r"(\d+) methods matched", text, re.I)
        if method_matches:
            out["methods"].append({"kind": "method", "name": f"{method_matches.group(1)} potential methods", "confidence": 0.55})
        field_matches = re.search(r"No fields matched", text, re.I)
        if field_matches:
            out["fields"] = []
        return out

    def _parse_trends(self, text: str) -> dict[str, Any]:
        dates = re.findall(r"\b\d{1,2}/\d{1,2}/\d{4}\b", text)
        captured = dates[0] if dates else ""
        return {
            "snapshot": {
                "captured_at": captured,
                "rules_violated": 0,
                "critical_rules_violated": 0,
            },
            "points": [{"at": captured, "loc_total": 0, "avg_complexity": 0, "max_complexity": 0}] if captured else [],
        }

    def _parse_application_statistics(self, text: str) -> dict[str, Any]:
        rows = []
        for line in [line.strip() for line in text.splitlines() if line.strip()]:
            m = re.match(r"^(.+?)\s+(\d+)\s+(\d+)\s+(-?\d+)\s+(.+)$", line)
            if m and "Application Statistics" not in line and "Main" not in line:
                rows.append({
                    "stat": m.group(1).strip(),
                    "occurrences": int(m.group(2)),
                    "average": int(m.group(3)),
                    "stddev": int(m.group(4)),
                    "sample": m.group(5).strip(),
                })
        return {"rows": rows[:20]}

    def _prov(self, ref: EvidenceFileRef, page: int, snippet: str) -> dict[str, Any]:
        return {
            "source_file": ref.file_name,
            "file_id": ref.file_id,
            "page": page,
            "extracted_text_span": snippet,
            "confidence": "high",
            "storage_path": ref.storage_path,
        }

    def _meta(self, artifact_type: str, extraction: dict[str, Any]) -> dict[str, Any]:
        return {
            "artifact_type": artifact_type,
            "artifact_version": "1.0",
            "artifact_id": f"{artifact_type}_{self._slug(str(extraction.get('bundle_id', 'bundle')))}",
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "provider_id": self.provider_id,
            "provider_version": self.provider_version,
            "bundle_id": extraction.get("bundle_id", ""),
        }

    def _read_file_text(self, ref: EvidenceFileRef) -> str:
        path = Path(ref.storage_path)
        suffix = path.suffix.lower()
        if suffix == ".pdf":
            if PdfReader is None:
                raise RuntimeError("pypdf is required for Evidence Mode PDF parsing")
            reader = PdfReader(str(path))
            return "\n".join((page.extract_text() or "") for page in reader.pages)
        return path.read_text(encoding="utf-8", errors="ignore")

    def _report_kind(self, filename: str, text: str) -> str:
        lower = f"{filename}\n{text[:2000]}".lower()
        if "projectmatrix" in lower or "projects metrics" in lower:
            return "project_metrics"
        if "typematrix" in lower or "types metrics : code quality" in lower:
            return "type_metrics"
        if "types dependencies" in lower:
            return "type_dependencies"
        if "dead code" in lower:
            return "dead_code"
        if "trendcharts" in lower or "trend charts" in lower:
            return "trend"
        if "application statistics" in lower:
            return "application_statistics"
        return "unknown"

    def _runtime_kind(self, target: str) -> str:
        lower = str(target or "").lower()
        if lower in {x.lower() for x in KNOWN_CONTROL_TYPES}:
            return "framework_or_control"
        if lower.endswith("ocx"):
            return "ocx_dependency"
        if lower.endswith(("dll", "tlb")):
            return "runtime_library"
        return "runtime_dependency"

    def _slug(self, value: str) -> str:
        return re.sub(r"[^a-z0-9]+", "_", str(value or "").lower()).strip("_") or "item"
