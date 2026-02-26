from __future__ import annotations

import html
import io
import os
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from zipfile import ZIP_DEFLATED, ZipFile

try:
    from docx import Document  # type: ignore
    from docx.enum.table import WD_TABLE_ALIGNMENT  # type: ignore
    from docx.enum.text import WD_ALIGN_PARAGRAPH  # type: ignore
    from docx.oxml import OxmlElement  # type: ignore
    from docx.oxml.ns import qn  # type: ignore
    from docx.shared import Inches, Pt, RGBColor  # type: ignore
except Exception:  # pragma: no cover - optional dependency
    Document = None  # type: ignore
    WD_TABLE_ALIGNMENT = None  # type: ignore
    WD_ALIGN_PARAGRAPH = None  # type: ignore
    OxmlElement = None  # type: ignore
    qn = None  # type: ignore
    Inches = None  # type: ignore
    Pt = None  # type: ignore
    RGBColor = None  # type: ignore


COLOR_PRIMARY = "1F3864"
COLOR_SECONDARY = "2E75B6"
COLOR_SECTION_ACCENT = "1F6B75"
COLOR_TEXT = "111827"
COLOR_MUTED = "6B7280"
COLOR_WHITE = "FFFFFF"
COLOR_ROW_ALT = "F3F4F6"
COLOR_CARD_BG = "D6E4F0"
COLOR_ACCENT_BG = "D6EEF2"
COLOR_RISK_HIGH_BG = "FEE2E2"
COLOR_RISK_MED_BG = "FEF3C7"
COLOR_RISK_LOW_BG = "DCFCE7"
COLOR_GATE_PASS_BG = "DCFCE7"
COLOR_GATE_WARN_BG = "FEF3C7"
COLOR_GATE_FAIL_BG = "FEE2E2"
TEMPLATE_ENV_VAR = "ANALYST_DOCX_TEMPLATE_PATH"
_STRICT_TEMPLATE_ACTIVE = False


def _as_dict(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _as_list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _clean(value: Any) -> str:
    return str(value or "").strip()


def _to_int(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except Exception:
        return default


def _extract_report_and_raw(payload: dict[str, Any]) -> tuple[dict[str, Any], dict[str, Any]]:
    safe = _as_dict(payload)
    if _clean(safe.get("artifact_type")) == "analyst_report":
        return safe, _as_dict(safe.get("raw_artifacts"))
    report = _as_dict(safe.get("analyst_report_v2"))
    if _clean(report.get("artifact_type")) == "analyst_report":
        return report, _as_dict(safe.get("raw_artifacts"))
    return safe, _as_dict(safe.get("raw_artifacts"))


def _collect_report_data(report: dict[str, Any], raw_artifacts: dict[str, Any] | None = None) -> dict[str, Any]:
    metadata = _as_dict(report.get("metadata"))
    project = _as_dict(metadata.get("project"))
    ctx = _as_dict(metadata.get("context_reference"))
    brief = _as_dict(report.get("decision_brief"))
    glance = _as_dict(brief.get("at_a_glance"))
    inventory = _as_dict(glance.get("inventory_summary"))
    strategy = _as_dict(brief.get("recommended_strategy"))
    decisions = _as_dict(brief.get("decisions_required"))
    top_risks = _as_list(brief.get("top_risks"))
    next_steps = _as_list(brief.get("next_steps"))
    delivery = _as_dict(report.get("delivery_spec"))
    backlog = _as_list(_as_dict(delivery.get("backlog")).get("items"))
    testing = _as_dict(delivery.get("testing_and_evidence"))
    quality_gates = _as_list(testing.get("quality_gates"))
    open_questions = _as_list(delivery.get("open_questions"))
    appendix = _as_dict(report.get("appendix"))
    artifact_refs = _as_dict(appendix.get("artifact_refs"))
    raw = _as_dict(raw_artifacts)
    return {
        "metadata": metadata,
        "project": project,
        "context": ctx,
        "brief": brief,
        "glance": glance,
        "inventory": inventory,
        "strategy": strategy,
        "decisions": decisions,
        "top_risks": top_risks,
        "next_steps": next_steps,
        "delivery": delivery,
        "backlog": backlog,
        "testing": testing,
        "quality_gates": quality_gates,
        "open_questions": open_questions,
        "artifact_refs": artifact_refs,
        "raw_artifacts": raw,
    }


def _set_run_style(run: Any, *, size: int = 11, bold: bool = False, color: str = COLOR_TEXT) -> None:
    if run is None or Pt is None or RGBColor is None:
        return
    run.bold = bool(bold)
    # In strict-template mode, keep the template's font family but still
    # apply emphasis/size/color so generated content remains visually rich.
    if not _STRICT_TEMPLATE_ACTIVE:
        run.font.name = "Arial"
    run.font.size = Pt(size)
    try:
        run.font.color.rgb = RGBColor.from_string(color)
    except Exception:
        pass


def _apply_heading_style(doc: Any) -> None:
    if doc is None or Pt is None or RGBColor is None:
        return
    try:
        normal = doc.styles["Normal"]
        normal.font.name = "Arial"
        normal.font.size = Pt(11)
        normal.font.color.rgb = RGBColor.from_string(COLOR_TEXT)
    except Exception:
        pass
    for style_name, size, color in [
        ("Title", 28, COLOR_PRIMARY),
        ("Heading 1", 16, COLOR_PRIMARY),
        ("Heading 2", 13, COLOR_SECONDARY),
        ("Heading 3", 12, COLOR_PRIMARY),
    ]:
        try:
            st = doc.styles[style_name]
            st.font.name = "Arial"
            st.font.bold = True
            st.font.size = Pt(size)
            st.font.color.rgb = RGBColor.from_string(color)
        except Exception:
            continue


def _set_cell_shading(cell: Any, fill_hex: str) -> None:
    if cell is None or OxmlElement is None or qn is None:
        return
    tc_pr = cell._tc.get_or_add_tcPr()
    shd = OxmlElement("w:shd")
    shd.set(qn("w:val"), "clear")
    shd.set(qn("w:color"), "auto")
    shd.set(qn("w:fill"), str(fill_hex or "").replace("#", ""))
    tc_pr.append(shd)


def _set_cell_text(cell: Any, text: str, *, bold: bool = False, size: int = 10, color: str = COLOR_TEXT) -> None:
    if cell is None:
        return
    cell.text = ""
    paragraph = cell.paragraphs[0]
    run = paragraph.add_run(str(text or ""))
    _set_run_style(run, size=size, bold=bold, color=color)


def _add_labeled_paragraph(doc: Any, label: str, value: str, *, muted: bool = False) -> None:
    p = doc.add_paragraph()
    r1 = p.add_run(f"{label}: ")
    _set_run_style(r1, size=10, bold=True, color=COLOR_SECONDARY)
    r2 = p.add_run(value or "n/a")
    _set_run_style(r2, size=10, bold=False, color=COLOR_MUTED if muted else COLOR_TEXT)


def _severity_fill(sev: str) -> str:
    s = str(sev or "").strip().lower()
    if s in {"high", "critical", "blocker"}:
        return COLOR_RISK_HIGH_BG
    if s in {"medium", "warn"}:
        return COLOR_RISK_MED_BG
    return COLOR_RISK_LOW_BG


def _gate_fill(result: str) -> str:
    s = str(result or "").strip().lower()
    if s == "pass":
        return COLOR_GATE_PASS_BG
    if s == "fail":
        return COLOR_GATE_FAIL_BG
    return COLOR_GATE_WARN_BG


def _base_form_name(value: Any) -> str:
    text = _clean(value)
    if not text:
        return ""
    if "::" in text:
        text = text.split("::")[-1]
    text = text.split(":")[0]
    text = text.rsplit("/", 1)[-1]
    if text.lower().endswith(".frm"):
        text = text[:-4]
    return text.lower()


def _extract_forms_from_text(value: Any) -> list[str]:
    text = _clean(value)
    if not text:
        return []
    forms = re.findall(r"([A-Za-z0-9_]+(?:\.frm)?)", text)
    out: list[str] = []
    for form in forms:
        low = form.lower()
        if "frm" not in low and not low.startswith("form"):
            continue
        normalized = form[:-4] if low.endswith(".frm") else form
        if normalized not in out:
            out.append(normalized)
    return out


def _project_label(name: str, path_map: dict[str, str]) -> str:
    key = _clean(name)
    if not key:
        return "n/a"
    path = _clean(path_map.get(key))
    return f"{key} [{path}]" if path else key


def _resolve_paragraph_style(doc: Any, role: str) -> Any:
    aliases = {
        "title": ["Title"],
        "heading1": ["Heading 1"],
        "heading2": ["Heading 2"],
        "heading3": ["Heading 3"],
        "list": ["List Paragraph"],
        "body": ["Normal"],
    }
    role_key = str(role or "").strip().lower()
    names = aliases.get(role_key, [])
    paragraph_styles = [
        style
        for style in getattr(doc, "styles", [])
        if str(getattr(style, "type", "")).endswith("PARAGRAPH (1)")
    ]
    for name in names:
        for style in paragraph_styles:
            if _clean(getattr(style, "name", "")) == name:
                return style
    for style in paragraph_styles:
        name = _clean(getattr(style, "name", ""))
        style_id = _clean(getattr(style, "style_id", ""))
        if role_key == "heading1" and style_id.lower() == "heading1":
            return style
        if role_key == "heading2" and style_id.lower() == "heading2":
            return style
        if role_key == "list" and style_id.lower() == "listparagraph":
            return style
        if role_key == "title" and style_id.lower() == "title":
            return style
        if role_key == "body" and (name.lower() == "normal" or style_id.lower() == "normal"):
            return style
    return None


def _add_paragraph_with_role(doc: Any, text: str = "", *, role: str = "body") -> Any:
    style_obj = _resolve_paragraph_style(doc, role)
    if style_obj is not None:
        return doc.add_paragraph(text, style=style_obj)
    return doc.add_paragraph(text)


def _resolve_table_style(doc: Any) -> Any:
    preferred = ["Table Grid"]
    for name in preferred:
        try:
            return doc.styles[name]
        except Exception:
            continue
    for style in getattr(doc, "styles", []):
        if str(getattr(style, "type", "")).endswith("TABLE (3)"):
            return style
    return None


def _build_table(doc: Any, headers: list[str], *, col_count: int | None = None) -> Any:
    cols = int(col_count or len(headers) or 1)
    table = doc.add_table(rows=1, cols=cols)
    resolved_style = _resolve_table_style(doc)
    if resolved_style is not None:
        try:
            table.style = resolved_style
        except Exception:
            pass
    if WD_TABLE_ALIGNMENT:
        table.alignment = WD_TABLE_ALIGNMENT.LEFT
    for idx, head in enumerate(headers[:cols]):
        cell = table.rows[0].cells[idx]
        _set_cell_shading(cell, COLOR_PRIMARY)
        _set_cell_text(cell, head, bold=True, color=COLOR_WHITE, size=9)
    return table


def _add_table_row(table: Any, values: list[Any], *, alt: bool = False, size: int = 9) -> None:
    row_cells = table.add_row().cells
    for idx, value in enumerate(values[: len(row_cells)]):
        text = _clean(value) or "n/a"
        _set_cell_text(row_cells[idx], text, size=size, color=COLOR_TEXT)
        if alt:
            _set_cell_shading(row_cells[idx], COLOR_ROW_ALT)


def _normalize_llm_doc_plan(value: Any) -> dict[str, Any]:
    plan = _as_dict(value)
    section_intros = _as_dict(plan.get("section_intros"))
    callouts = _as_list(plan.get("callouts"))
    bullets = _as_list(plan.get("executive_bullets"))
    return {
        "title": _clean(plan.get("title"))[:140],
        "subtitle": _clean(plan.get("subtitle"))[:220],
        "narrative": _clean(plan.get("narrative"))[:2200],
        "executive_bullets": [_clean(x)[:260] for x in bullets if _clean(x)][:8],
        "callouts": [
            {
                "label": _clean(_as_dict(x).get("label"))[:64],
                "message": _clean(_as_dict(x).get("message"))[:260],
                "severity": _clean(_as_dict(x).get("severity") or "info").lower(),
            }
            for x in callouts[:8]
            if isinstance(x, dict) and (_clean(_as_dict(x).get("label")) or _clean(_as_dict(x).get("message")))
        ],
        "section_intros": {
            "executive_snapshot": _clean(section_intros.get("executive_snapshot"))[:320],
            "dependency_map": _clean(section_intros.get("dependency_map"))[:320],
            "form_dossiers": _clean(section_intros.get("form_dossiers"))[:320],
            "flow_traces": _clean(section_intros.get("flow_traces"))[:320],
            "traceability": _clean(section_intros.get("traceability"))[:320],
            "sprints": _clean(section_intros.get("sprints"))[:320],
            "risks": _clean(section_intros.get("risks"))[:320],
        },
    }


def _add_section_intro(doc: Any, text: str) -> None:
    if not _clean(text):
        return
    para = doc.add_paragraph()
    run = para.add_run(_clean(text))
    _set_run_style(run, size=9, color=COLOR_MUTED)


def _resolve_docx_template_path(template_path: str | None = None) -> str:
    requested = _clean(template_path)
    if requested and Path(requested).exists():
        return requested

    env_path = _clean(os.getenv(TEMPLATE_ENV_VAR, ""))
    if env_path and Path(env_path).exists():
        return env_path

    module_root = Path(__file__).resolve().parents[1]
    candidates = [
        module_root / "assets" / "docx_templates" / "ba_workbook_v3.docx",
        Path("/Users/vishak/Downloads/ba_workbook_v3.docx"),
    ]
    for candidate in candidates:
        if candidate.exists():
            return str(candidate)
    return ""


def _clear_document_body(doc: Any) -> None:
    body = getattr(getattr(doc, "_element", None), "body", None)
    if body is None:
        return
    children = list(body)
    for child in children:
        # Keep final section properties so page config/header/footer links remain valid.
        if child.tag.endswith("}sectPr"):
            continue
        body.remove(child)


def _build_business_docx_bytes_rich(
    payload: dict[str, Any],
    *,
    run_id: str = "",
    llm_doc_plan: dict[str, Any] | None = None,
    template_path: str | None = None,
    strict_template: bool = False,
) -> bytes:
    if Document is None:
        raise RuntimeError("python-docx is not available")
    global _STRICT_TEMPLATE_ACTIVE
    _STRICT_TEMPLATE_ACTIVE = bool(strict_template)
    report, raw_artifacts = _extract_report_and_raw(payload)
    resolved_template = _resolve_docx_template_path(template_path) if strict_template else _clean(template_path)
    if resolved_template:
        doc = Document(resolved_template)
        _clear_document_body(doc)
    else:
        doc = Document()
    if not strict_template:
        _apply_heading_style(doc)
    data = _collect_report_data(report, raw_artifacts)
    raw = _as_dict(data.get("raw_artifacts"))

    section = doc.sections[0]
    if Inches is not None and not strict_template:
        section.left_margin = Inches(0.6)
        section.right_margin = Inches(0.6)
        section.top_margin = Inches(0.55)
        section.bottom_margin = Inches(0.55)

    if not strict_template:
        header = section.header.paragraphs[0]
        header.clear()
        hrun = header.add_run("VB6 Legacy Analyst Workbook")
        _set_run_style(hrun, size=10, bold=True, color=COLOR_SECTION_ACCENT)
        header.alignment = WD_ALIGN_PARAGRAPH.LEFT if WD_ALIGN_PARAGRAPH else 0

        footer = section.footer.paragraphs[0]
        footer.clear()
        frun = footer.add_run("Synthetix Platform | Analyst Evidence Workbook")
        _set_run_style(frun, size=9, bold=False, color=COLOR_MUTED)
        footer.alignment = WD_ALIGN_PARAGRAPH.LEFT if WD_ALIGN_PARAGRAPH else 0

    banner = _build_table(doc, ["VB6 Modernization - Business Analyst Workbook"], col_count=1)
    _set_cell_shading(banner.rows[0].cells[0], COLOR_PRIMARY)
    plan = _normalize_llm_doc_plan(llm_doc_plan)
    workbook_title = plan.get("title") or "VB6 Modernization - Business Analyst Workbook"
    _set_cell_text(banner.rows[0].cells[0], workbook_title, bold=True, color=COLOR_WHITE, size=12)

    project_name = _clean(data["project"].get("name")) or "Untitled Project"
    subtitle = doc.add_paragraph()
    _set_run_style(subtitle.add_run(project_name), size=13, bold=True, color=COLOR_SECTION_ACCENT)
    if _clean(plan.get("subtitle")):
        subtitle2 = doc.add_paragraph()
        _set_run_style(subtitle2.add_run(_clean(plan.get("subtitle"))), size=10, color=COLOR_TEXT)
    stamp = _clean(data["metadata"].get("generated_at")) or datetime.now(timezone.utc).isoformat()
    meta = doc.add_paragraph()
    _set_run_style(
        meta.add_run(
            f"Run {run_id or 'n/a'}  ·  Generated {stamp}  ·  Repo {_clean(data['context'].get('repo')) or 'n/a'}"
        ),
        size=9,
        color=COLOR_MUTED,
    )

    doc.add_paragraph()
    _add_paragraph_with_role(doc, "Executive Snapshot", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("executive_snapshot", ""))
    objective = _clean(data["project"].get("objective"))
    if objective:
        _add_labeled_paragraph(doc, "Objective", objective)
    headline = _clean(data["glance"].get("headline"))
    if headline:
        _add_labeled_paragraph(doc, "Summary", headline)
    narrative = _clean(plan.get("narrative"))
    if narrative:
        n = doc.add_paragraph()
        _set_run_style(n.add_run(narrative), size=10, color=COLOR_TEXT)
    bullets = _as_list(plan.get("executive_bullets"))
    for bullet in bullets:
        bp = _add_paragraph_with_role(doc, role="list")
        _set_run_style(bp.add_run(f"- {_clean(bullet)}"), size=9, color=COLOR_TEXT)

    inv = data["inventory"]
    tables_touched = [str(x).strip() for x in _as_list(inv.get("tables_touched")) if str(x).strip()]
    summary_table = _build_table(doc, ["Category", "Summary"])
    summary_rows = [
        ("Readiness", f"{_clean(data['glance'].get('readiness_score')) or 'n/a'}/100"),
        ("Risk tier", _clean(data["glance"].get("risk_tier")) or "n/a"),
        (
            "Inventory",
            f"projects={_to_int(inv.get('projects'))}, forms={_to_int(inv.get('forms'))}, dependencies={_to_int(inv.get('dependencies'))}",
        ),
        ("Data touchpoints", ", ".join(tables_touched[:16]) if tables_touched else "n/a"),
    ]
    for idx, row in enumerate(summary_rows):
        _add_table_row(summary_table, [row[0], row[1]], alt=(idx % 2 == 1))
    for idx, callout in enumerate(_as_list(plan.get("callouts"))[:4]):
        row = _as_dict(callout)
        label = _clean(row.get("label")) or f"Callout {idx + 1}"
        message = _clean(row.get("message")) or "n/a"
        sev = _clean(row.get("severity") or "info")
        c = doc.add_paragraph()
        _set_run_style(c.add_run(f"{label}: "), size=9, bold=True, color=COLOR_SECONDARY)
        _set_run_style(c.add_run(message), size=9, color=COLOR_TEXT)
        if sev in {"high", "critical", "blocker"}:
            c_format = c.paragraph_format
            c_format.left_indent = Inches(0.08) if Inches else None

    strategy = data["strategy"]
    _add_paragraph_with_role(doc, "Recommended Modernization Strategy", role="heading2")
    p = doc.add_paragraph()
    _set_run_style(p.add_run(f"{_clean(strategy.get('name')) or 'Phased migration'}: "), size=10, bold=True, color=COLOR_SECTION_ACCENT)
    _set_run_style(p.add_run(_clean(strategy.get("rationale")) or "Rationale not provided."), size=10, color=COLOR_TEXT)
    for phase in _as_list(strategy.get("phases"))[:8]:
        if not isinstance(phase, dict):
            continue
        item = _add_paragraph_with_role(doc, role="list")
        _set_run_style(item.add_run(f"- {_clean(phase.get('id'))} {_clean(phase.get('title'))}: "), size=9, bold=True, color=COLOR_SECONDARY)
        _set_run_style(item.add_run(_clean(phase.get("outcome"))), size=9, color=COLOR_TEXT)

    decisions = _as_dict(data.get("decisions"))
    decision_rows = _as_list(decisions.get("blocking"))[:12] + _as_list(decisions.get("non_blocking"))[:8]
    _add_paragraph_with_role(doc, "Decisions Required", role="heading2")
    dec_table = _build_table(doc, ["ID", "Question", "Recommendation"])
    if not decision_rows:
        decision_rows = [{"id": "DEC-NA", "question": "No explicit decisions listed.", "default_recommendation": ""}]
    for idx, row in enumerate(decision_rows):
        r = _as_dict(row)
        _add_table_row(
            dec_table,
            [
                _clean(r.get("id")) or "DEC",
                _clean(r.get("question")) or "n/a",
                _clean(r.get("default_recommendation")) or "n/a",
            ],
            alt=(idx % 2 == 1),
        )

    quality_gates = _as_list(data.get("quality_gates"))
    _add_paragraph_with_role(doc, "Quality Gates", role="heading2")
    gate_table = _build_table(doc, ["Gate ID", "Result", "Description"])
    if not quality_gates:
        quality_gates = [{"id": "gate_na", "result": "warn", "description": "No quality gates listed."}]
    for idx, row in enumerate(quality_gates[:24]):
        r = _as_dict(row)
        result = _clean(r.get("result")) or "warn"
        _add_table_row(
            gate_table,
            [
                _clean(r.get("id")) or "gate",
                result.upper(),
                _clean(r.get("description")) or "n/a",
            ],
            alt=(idx % 2 == 1),
        )
        _set_cell_shading(gate_table.rows[idx + 1].cells[1], _gate_fill(result))

    # Raw artifacts for detailed BA sections
    raw_event_map = _as_list(_as_dict(raw.get("event_map")).get("entries"))
    raw_sql_map = _as_list(_as_dict(raw.get("sql_map")).get("entries"))
    raw_procedures = _as_list(_as_dict(raw.get("procedure_summary")).get("procedures"))
    raw_form_dossiers = _as_list(_as_dict(raw.get("form_dossier")).get("dossiers"))
    raw_rules = _as_list(_as_dict(raw.get("business_rule_catalog")).get("rules"))
    raw_risks = _as_list(_as_dict(raw.get("risk_register")).get("risks"))
    raw_landscape = _as_list(_as_dict(raw.get("repo_landscape")).get("projects"))
    raw_variant_diff = _as_dict(raw.get("variant_diff_report"))
    raw_deps = _as_list(_as_dict(raw.get("dependency_inventory")).get("dependencies"))

    project_path_by_name: dict[str, str] = {}
    project_dependencies_by_name: dict[str, set[str]] = {}
    for row in raw_landscape:
        r = _as_dict(row)
        raw_id = _clean(r.get("id"))
        left = raw_id.split("|", 1)[0] if "|" in raw_id else raw_id
        path = _clean(r.get("path"))
        deps = {_clean(x) for x in _as_list(r.get("dependencies")) if _clean(x)}
        for key in [left, _clean(r.get("name")), raw_id]:
            key_clean = _clean(key)
            if not key_clean:
                continue
            if key_clean not in project_path_by_name and path:
                project_path_by_name[key_clean] = path
            project_dependencies_by_name.setdefault(key_clean, set()).update(deps)

    form_sql_rows: dict[str, list[dict[str, Any]]] = {}
    form_db_tables: dict[str, set[str]] = {}
    for row in raw_sql_map:
        r = _as_dict(row)
        tables = {_clean(x) for x in _as_list(r.get("tables")) if _clean(x)}
        for form_key in {
            _base_form_name(r.get("form")),
            _base_form_name(r.get("form_base")),
            _base_form_name(r.get("variant")),
        }:
            if not form_key:
                continue
            form_sql_rows.setdefault(form_key, []).append(r)
            form_db_tables.setdefault(form_key, set()).update(tables)

    form_event_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_event_map:
        r = _as_dict(row)
        for form_key in {
            _base_form_name(r.get("container")),
            _base_form_name(r.get("name")),
            _base_form_name(_as_dict(r.get("handler")).get("symbol")),
        }:
            if not form_key:
                continue
            form_event_rows.setdefault(form_key, []).append(r)

    form_proc_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_procedures:
        r = _as_dict(row)
        key = _base_form_name(r.get("form"))
        if key:
            form_proc_rows.setdefault(key, []).append(r)

    form_rule_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_rules:
        r = _as_dict(row)
        texts = [
            _clean(_as_dict(r.get("scope")).get("component_id")),
            _clean(r.get("statement")),
        ]
        for ev in _as_list(r.get("evidence")):
            e = _as_dict(ev)
            texts.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
            texts.append(_clean(_as_dict(e.get("file_span")).get("path")))
        forms: set[str] = set()
        for text in texts:
            for form in _extract_forms_from_text(text):
                forms.add(_base_form_name(form))
        for form_key in [x for x in forms if x]:
            form_rule_rows.setdefault(form_key, []).append(r)

    form_risk_rows: dict[str, list[dict[str, Any]]] = {}
    for row in raw_risks:
        r = _as_dict(row)
        texts = [_clean(r.get("description")), _clean(r.get("recommended_action"))]
        for ev in _as_list(r.get("evidence")):
            e = _as_dict(ev)
            texts.append(_clean(_as_dict(e.get("external_ref")).get("ref")))
            texts.append(_clean(_as_dict(e.get("file_span")).get("path")))
        forms: set[str] = set()
        for text in texts:
            for form in _extract_forms_from_text(text):
                forms.add(_base_form_name(form))
        for form_key in [x for x in forms if x]:
            form_risk_rows.setdefault(form_key, []).append(r)

    # Section O
    doc.add_page_break()
    _add_paragraph_with_role(doc, "Section O - Project Dependency Map", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("dependency_map", ""))
    project_table = _build_table(doc, ["Project", "Path", "Forms", "Dependencies", "DB touchpoints"])
    for idx, row in enumerate(raw_landscape[:80]):
        r = _as_dict(row)
        raw_id = _clean(r.get("id"))
        project_name = raw_id.split("|", 1)[0] if "|" in raw_id else raw_id
        _add_table_row(
            project_table,
            [
                project_name or "variant",
                _clean(r.get("path")) or "n/a",
                str(_to_int(_as_dict(r.get("counts")).get("forms"))),
                ", ".join(_as_list(r.get("dependencies"))[:6]) or "n/a",
                ", ".join(_as_list(r.get("db_touchpoints"))[:8]) or "n/a",
            ],
            alt=(idx % 2 == 1),
        )

    dep_rows: list[dict[str, str]] = []
    dep_seen: set[tuple[str, str, str, str]] = set()
    shared_module_procs = {
        _clean(_as_dict(proc).get("procedure_name"))
        for proc in raw_procedures
        if _base_form_name(_as_dict(proc).get("form")) == "shared_module"
    }
    for entry in raw_event_map:
        e = _as_dict(entry)
        source = _clean(e.get("container") or e.get("form") or e.get("name")) or "n/a"
        trigger_control = _clean(_as_dict(e.get("trigger")).get("control")).lower()
        for call in [_clean(x) for x in _as_list(e.get("calls")) if _clean(x)]:
            dep_type = ""
            if call in shared_module_procs:
                dep_type = "shared_module_call"
            elif (
                "main" in source.lower()
                or "toolbar" in source.lower()
                or "toolbar" in trigger_control
            ) and call.lower().startswith(("frm", "form", "rpt", "datareport")):
                dep_type = "mdi_navigation"
            if not dep_type:
                continue
            evidence = _clean(e.get("entry_id") or e.get("name"))
            key = (source, call, dep_type, evidence)
            if key in dep_seen:
                continue
            dep_seen.add(key)
            dep_rows.append({"from": source, "to": call, "type": dep_type, "evidence": evidence})

    schema = _as_dict(raw_variant_diff.get("schema_divergence"))
    for pair in _as_list(schema.get("blocking_pairs") or schema.get("pairs")):
        p = _as_dict(pair)
        left = _clean(p.get("left_project"))
        right = _clean(p.get("right_project"))
        if not left or not right:
            continue
        evidence = (
            f"alias_mismatches={len(_as_list(p.get('alias_mismatches')))}, "
            f"near_miss={len(_as_list(p.get('near_miss_names')))}, "
            f"transaction_conflict={'yes' if bool(p.get('transaction_schema_conflict')) else 'no'}"
        )
        key = (left, right, "cross_variant_schema_conflict", evidence)
        if key in dep_seen:
            continue
        dep_seen.add(key)
        dep_rows.append({"from": left, "to": right, "type": "cross_variant_schema_conflict", "evidence": evidence})

    dep_table = _build_table(doc, ["From", "To", "Type", "Evidence"])
    if not dep_rows:
        dep_rows = [{"from": "n/a", "to": "n/a", "type": "n/a", "evidence": "No project dependencies detected"}]
    for idx, row in enumerate(dep_rows[:600]):
        _add_table_row(dep_table, [row.get("from"), row.get("to"), row.get("type"), row.get("evidence")], alt=(idx % 2 == 1))

    # Section Q2/Q3 style form dossiers
    doc.add_page_break()
    _add_paragraph_with_role(doc, "Section K - Form Dossiers (Extended)", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("form_dossiers", ""))
    form_table = _build_table(doc, ["Form", "Project", "Purpose", "Inputs", "Outputs", "ActiveX used", "DB tables", "Rules", "Risks"])
    for idx, row in enumerate(raw_form_dossiers[:280]):
        r = _as_dict(row)
        form_name = _clean(r.get("form_name")) or "n/a"
        form_key = _base_form_name(form_name)
        project_name = _clean(r.get("project_name"))
        proc_rows = form_proc_rows.get(form_key, [])
        inputs: set[str] = set()
        outputs: set[str] = set()
        for proc in proc_rows:
            p = _as_dict(proc)
            inputs.update(_clean(x) for x in _as_list(p.get("inputs")) if _clean(x))
            outputs.update(_clean(x) for x in _as_list(p.get("tables_touched")) if _clean(x))
            outputs.update(_clean(x) for x in _as_list(p.get("data_mutations")) if _clean(x))
            outputs.update(_clean(x) for x in _as_list(p.get("navigation_side_effects")) if _clean(x))

        activex: set[str] = set()
        for ctl in _as_list(r.get("controls")):
            ctl_name = _clean(ctl)
            if not ctl_name:
                continue
            prefix = ctl_name.split(":", 1)[0]
            if prefix and prefix.upper() != "VB":
                activex.add(prefix)
        for dep in project_dependencies_by_name.get(project_name, set()):
            dep_name = _clean(dep)
            if dep_name.lower().endswith((".ocx", ".dll")) or "MSCOM" in dep_name.upper() or "MSFLEX" in dep_name.upper():
                activex.add(dep_name)

        _add_table_row(
            form_table,
            [
                form_name,
                _project_label(project_name, project_path_by_name),
                _clean(r.get("purpose")) or "n/a",
                ", ".join(sorted(inputs)[:6]) or "n/a",
                ", ".join(sorted(outputs)[:6]) or "n/a",
                ", ".join(sorted(activex)[:6]) or "n/a",
                ", ".join(sorted(form_db_tables.get(form_key, set()))[:8]) or "n/a",
                str(len(form_rule_rows.get(form_key, []))),
                str(len(form_risk_rows.get(form_key, []))),
            ],
            alt=(idx % 2 == 1),
            size=8,
        )

    _add_paragraph_with_role(doc, "Business Rules by Form", role="heading2")
    rule_table = _build_table(doc, ["Rule ID", "Form", "Category", "Statement"])
    rules_rendered = 0
    for row in raw_rules[:900]:
        r = _as_dict(row)
        evidence = ", ".join(
            [
                _clean(_as_dict(ev).get("external_ref", {}).get("ref") or _as_dict(ev).get("file_span", {}).get("path"))
                for ev in _as_list(r.get("evidence"))
            ]
        )
        forms: list[str] = []
        for source in [
            _clean(_as_dict(r.get("scope")).get("component_id")),
            _clean(r.get("statement")),
            evidence,
        ]:
            for f in _extract_forms_from_text(source):
                if f not in forms:
                    forms.append(f)
        form_value = ", ".join(forms[:3]) or _clean(_as_dict(r.get("scope")).get("component_id")) or "n/a"
        _add_table_row(
            rule_table,
            [
                _clean(r.get("rule_id") or r.get("id")) or "BR",
                form_value,
                _clean(r.get("category") or "other"),
                _clean(r.get("statement")) or "n/a",
            ],
            alt=(rules_rendered % 2 == 1),
            size=8,
        )
        rules_rendered += 1
        if rules_rendered >= 220:
            break
    if rules_rendered == 0:
        _add_table_row(rule_table, ["BR-NA", "n/a", "n/a", "No business rules available"], alt=False)

    # Section P
    doc.add_page_break()
    _add_paragraph_with_role(doc, "Section P - Form Flow Traces", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("flow_traces", ""))
    for form_idx, row in enumerate(raw_form_dossiers[:60], start=1):
        r = _as_dict(row)
        form_name = _clean(r.get("form_name")) or f"Form-{form_idx}"
        form_key = _base_form_name(form_name)
        project_name = _clean(r.get("project_name"))
        _add_paragraph_with_role(
            doc,
            f"{form_name} ({_project_label(project_name, project_path_by_name)})",
            role="heading2",
        )

        activex: set[str] = set()
        for ctl in _as_list(r.get("controls")):
            ctl_name = _clean(ctl)
            if not ctl_name:
                continue
            prefix = ctl_name.split(":", 1)[0]
            if prefix and prefix.upper() != "VB":
                activex.add(prefix)
        for dep in project_dependencies_by_name.get(project_name, set()):
            dep_name = _clean(dep)
            if dep_name.lower().endswith((".ocx", ".dll")) or "MSCOM" in dep_name.upper() or "MSFLEX" in dep_name.upper():
                activex.add(dep_name)

        events = form_event_rows.get(form_key, [])
        procedures = form_proc_rows.get(form_key, [])
        sql_entries = form_sql_rows.get(form_key, [])
        procedure_names: set[str] = set()
        for proc in procedures:
            name = _clean(_as_dict(proc).get("procedure_name"))
            if name:
                procedure_names.add(name)
        for sql_entry in sql_entries:
            name = _clean(_as_dict(sql_entry).get("procedure"))
            if name:
                procedure_names.add(name)

        trace_table = _build_table(doc, ["Procedure", "Event", "ActiveX", "SQL IDs", "Tables", "Trace status"])
        if not procedure_names:
            _add_table_row(
                trace_table,
                [
                    "n/a",
                    "n/a",
                    ", ".join(sorted(activex)[:5]) or "n/a",
                    "n/a",
                    ", ".join(sorted(form_db_tables.get(form_key, set()))[:8]) or "n/a",
                    "TRACE_GAP",
                ],
                alt=False,
            )
            _set_cell_shading(trace_table.rows[1].cells[5], COLOR_RISK_MED_BG)
            continue

        for idx, proc_name in enumerate(sorted(procedure_names)[:80]):
            related_events = [
                _as_dict(e)
                for e in events
                if proc_name in _clean(_as_dict(e).get("handler", {}).get("symbol"))
            ]
            related_sql = [
                _as_dict(s)
                for s in sql_entries
                if _clean(_as_dict(s).get("procedure")) == proc_name
            ]
            sql_ids = sorted({_clean(x.get("sql_id")) for x in related_sql if _clean(x.get("sql_id"))})
            tables = sorted({_clean(t) for x in related_sql for t in _as_list(_as_dict(x).get("tables")) if _clean(t)})
            trace_ok = bool(related_events) and bool(related_sql) and bool(activex)
            _add_table_row(
                trace_table,
                [
                    proc_name,
                    ", ".join(_clean(e.get("entry_id")) for e in related_events[:3]) or "n/a",
                    ", ".join(sorted(activex)[:5]) or "n/a",
                    ", ".join(sql_ids[:6]) or "n/a",
                    ", ".join(tables[:8]) or "n/a",
                    "OK" if trace_ok else "TRACE_GAP",
                ],
                alt=(idx % 2 == 1),
                size=8,
            )
            _set_cell_shading(trace_table.rows[idx + 1].cells[5], COLOR_RISK_LOW_BG if trace_ok else COLOR_RISK_MED_BG)

    # Section Q
    doc.add_page_break()
    _add_paragraph_with_role(doc, "Section Q - Form Traceability Matrix", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("traceability", ""))
    trace_table = _build_table(
        doc,
        ["Form", "Project", "has_event_map", "has_sql_map", "has_business_rules", "has_risk_entry", "missing_links"],
    )
    traceability_rows: list[dict[str, Any]] = []
    for idx, row in enumerate(raw_form_dossiers[:300]):
        r = _as_dict(row)
        form_name = _clean(r.get("form_name")) or "n/a"
        project_name = _clean(r.get("project_name"))
        form_key = _base_form_name(form_name)
        has_event = bool(form_event_rows.get(form_key))
        has_sql = bool(form_sql_rows.get(form_key))
        has_rules = bool(form_rule_rows.get(form_key))
        has_risk = bool(form_risk_rows.get(form_key))
        missing: list[str] = []
        if not has_event:
            missing.append("event_map")
        if not has_sql:
            missing.append("sql_map")
        if not has_rules:
            missing.append("business_rules")
        if not has_risk:
            missing.append("risk_register")
        _add_table_row(
            trace_table,
            [
                form_name,
                _project_label(project_name, project_path_by_name),
                "yes" if has_event else "no",
                "yes" if has_sql else "no",
                "yes" if has_rules else "no",
                "yes" if has_risk else "no",
                ", ".join(missing) or "none",
            ],
            alt=(idx % 2 == 1),
            size=8,
        )
        traceability_rows.append(
            {
                "form_name": form_name,
                "missing": missing,
                "risk_ids": [
                    _clean(_as_dict(rr).get("risk_id"))
                    for rr in form_risk_rows.get(form_key, [])[:4]
                    if _clean(_as_dict(rr).get("risk_id"))
                ],
            }
        )

    # Section R
    _add_paragraph_with_role(doc, "Section R - Sprint Dependency Map", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("sprints", ""))
    sprint_table = _build_table(doc, ["Form", "Suggested sprint", "Depends on", "Rationale"])
    variant_gate_needed = bool(raw_variant_diff.get("decision_required"))
    sorted_rows = sorted(
        traceability_rows,
        key=lambda row: (len(_as_list(row.get("missing"))), _clean(row.get("form_name"))),
        reverse=True,
    )
    if not sorted_rows:
        sorted_rows = [{"form_name": "n/a", "missing": ["event_map"], "risk_ids": []}]
    for idx, row in enumerate(sorted_rows[:300]):
        missing = _as_list(row.get("missing"))
        risk_ids = _as_list(row.get("risk_ids"))
        deps: list[str] = []
        if variant_gate_needed:
            deps.append("DEC-VARIANT-001")
        if "sql_map" in missing:
            deps.append("Q.sql_map")
        if "event_map" in missing:
            deps.append("Q.event_map")
        if "business_rules" in missing:
            deps.append("Q.business_rules")
        deps.extend([_clean(x) for x in risk_ids[:2] if _clean(x)])

        if "event_map" in missing or "sql_map" in missing:
            sprint = "Sprint 0 (Discovery closure)"
            rationale = "Close traceability gaps before modernization delivery starts."
        elif risk_ids:
            sprint = "Sprint 1 (Risk-first modernization)"
            rationale = "Prioritize high-risk remediations before parity rollout."
        else:
            sprint = "Sprint 2 (Parity hardening)"
            rationale = "Traceability is present; proceed with parity implementation/testing."

        _add_table_row(
            sprint_table,
            [
                _clean(row.get("form_name")) or "n/a",
                sprint,
                ", ".join(deps) or "none",
                rationale,
            ],
            alt=(idx % 2 == 1),
            size=8,
        )

    # Risks and references
    _add_paragraph_with_role(doc, "Risks Register Snapshot", role="heading1")
    _add_section_intro(doc, _as_dict(plan.get("section_intros")).get("risks", ""))
    risk_table = _build_table(doc, ["Risk ID", "Severity", "Description", "Recommended action"])
    risks_to_render = raw_risks[:220] if raw_risks else _as_list(data.get("top_risks"))
    if not risks_to_render:
        risks_to_render = [{"risk_id": "RISK-NA", "severity": "low", "description": "No explicit risks listed.", "recommended_action": ""}]
    for idx, row in enumerate(risks_to_render):
        r = _as_dict(row)
        sev = _clean(r.get("severity")) or "medium"
        _add_table_row(
            risk_table,
            [
                _clean(r.get("risk_id") or r.get("id")) or "RISK",
                sev.upper(),
                _clean(r.get("description")) or "n/a",
                _clean(r.get("recommended_action") or r.get("mitigation")) or "n/a",
            ],
            alt=(idx % 2 == 1),
            size=8,
        )
        _set_cell_shading(risk_table.rows[idx + 1].cells[1], _severity_fill(sev))

    _add_paragraph_with_role(doc, "Evidence References", role="heading2")
    refs = _as_dict(data.get("artifact_refs"))
    ref_table = _build_table(doc, ["Artifact", "Reference"])
    if not refs:
        _add_table_row(ref_table, ["n/a", "No artifact references listed."], alt=False)
    for idx, (key, value) in enumerate(list(refs.items())[:120]):
        _add_table_row(ref_table, [key, _clean(value) or "n/a"], alt=(idx % 2 == 1), size=8)

    disclaimer = doc.add_paragraph()
    _set_run_style(
        disclaimer.add_run(
            "Generated from analyst structured artifacts. This workbook is intended for BA/business review "
            "and should be paired with technical JSON/markdown exports for engineering implementation."
        ),
        size=8,
        color=COLOR_MUTED,
    )

    buffer = io.BytesIO()
    doc.save(buffer)
    return buffer.getvalue()


def _xml_text(value: str) -> str:
    return html.escape(str(value or ""), quote=False)


def _paragraph_xml(text: str, *, bold: bool = False) -> str:
    if not text:
        return "<w:p/>"
    escaped = _xml_text(text)
    if bold:
        return (
            "<w:p><w:r><w:rPr><w:b/></w:rPr>"
            f"<w:t xml:space=\"preserve\">{escaped}</w:t>"
            "</w:r></w:p>"
        )
    return "<w:p><w:r>" f"<w:t xml:space=\"preserve\">{escaped}</w:t>" "</w:r></w:p>"


def _document_xml(paragraphs: list[tuple[str, bool]]) -> str:
    body = "".join([_paragraph_xml(text, bold=bold) for text, bold in paragraphs])
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<w:document "
        "xmlns:wpc=\"http://schemas.microsoft.com/office/word/2010/wordprocessingCanvas\" "
        "xmlns:mc=\"http://schemas.openxmlformats.org/markup-compatibility/2006\" "
        "xmlns:o=\"urn:schemas-microsoft-com:office:office\" "
        "xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\" "
        "xmlns:m=\"http://schemas.openxmlformats.org/officeDocument/2006/math\" "
        "xmlns:v=\"urn:schemas-microsoft-com:vml\" "
        "xmlns:wp14=\"http://schemas.microsoft.com/office/word/2010/wordprocessingDrawing\" "
        "xmlns:wp=\"http://schemas.openxmlformats.org/drawingml/2006/wordprocessingDrawing\" "
        "xmlns:w10=\"urn:schemas-microsoft-com:office:word\" "
        "xmlns:w=\"http://schemas.openxmlformats.org/wordprocessingml/2006/main\" "
        "xmlns:w14=\"http://schemas.microsoft.com/office/word/2010/wordml\" "
        "xmlns:wpg=\"http://schemas.microsoft.com/office/word/2010/wordprocessingGroup\" "
        "xmlns:wpi=\"http://schemas.microsoft.com/office/word/2010/wordprocessingInk\" "
        "xmlns:wne=\"http://schemas.microsoft.com/office/word/2006/wordml\" "
        "xmlns:wps=\"http://schemas.microsoft.com/office/word/2010/wordprocessingShape\" "
        "mc:Ignorable=\"w14 wp14\">"
        f"<w:body>{body}<w:sectPr><w:pgSz w:w=\"12240\" w:h=\"15840\"/>"
        "<w:pgMar w:top=\"1440\" w:right=\"1440\" w:bottom=\"1440\" w:left=\"1440\" w:header=\"708\" w:footer=\"708\" w:gutter=\"0\"/>"
        "<w:cols w:space=\"708\"/><w:docGrid w:linePitch=\"360\"/></w:sectPr></w:body></w:document>"
    )


def _content_types_xml() -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">"
        "<Default Extension=\"rels\" ContentType=\"application/vnd.openxmlformats-package.relationships+xml\"/>"
        "<Default Extension=\"xml\" ContentType=\"application/xml\"/>"
        "<Override PartName=\"/word/document.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.wordprocessingml.document.main+xml\"/>"
        "<Override PartName=\"/docProps/core.xml\" "
        "ContentType=\"application/vnd.openxmlformats-package.core-properties+xml\"/>"
        "<Override PartName=\"/docProps/app.xml\" "
        "ContentType=\"application/vnd.openxmlformats-officedocument.extended-properties+xml\"/>"
        "</Types>"
    )


def _package_rels_xml() -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\">"
        "<Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument\" Target=\"word/document.xml\"/>"
        "<Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/package/2006/relationships/metadata/core-properties\" Target=\"docProps/core.xml\"/>"
        "<Relationship Id=\"rId3\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/extended-properties\" Target=\"docProps/app.xml\"/>"
        "</Relationships>"
    )


def _core_props_xml() -> str:
    stamp = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<cp:coreProperties xmlns:cp=\"http://schemas.openxmlformats.org/package/2006/metadata/core-properties\" "
        "xmlns:dc=\"http://purl.org/dc/elements/1.1/\" xmlns:dcterms=\"http://purl.org/dc/terms/\" "
        "xmlns:dcmitype=\"http://purl.org/dc/dcmitype/\" xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\">"
        "<dc:title>Synthetix Analyst Business Brief</dc:title>"
        "<dc:creator>Synthetix Analyst Agent</dc:creator>"
        "<cp:lastModifiedBy>Synthetix Analyst Agent</cp:lastModifiedBy>"
        f"<dcterms:created xsi:type=\"dcterms:W3CDTF\">{stamp}</dcterms:created>"
        f"<dcterms:modified xsi:type=\"dcterms:W3CDTF\">{stamp}</dcterms:modified>"
        "</cp:coreProperties>"
    )


def _app_props_xml() -> str:
    return (
        "<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>"
        "<Properties xmlns=\"http://schemas.openxmlformats.org/officeDocument/2006/extended-properties\" "
        "xmlns:vt=\"http://schemas.openxmlformats.org/officeDocument/2006/docPropsVTypes\">"
        "<Application>Synthetix</Application>"
        "</Properties>"
    )


def _build_business_paragraphs(payload: dict[str, Any], run_id: str) -> list[tuple[str, bool]]:
    report, raw_artifacts = _extract_report_and_raw(payload)
    data = _collect_report_data(report, raw_artifacts)
    lines: list[tuple[str, bool]] = []
    lines.append(("Modernization Business Brief", True))
    lines.append((f"Project: {_clean(data['project'].get('name')) or 'Untitled project'}", False))
    if run_id:
        lines.append((f"Run ID: {run_id}", False))
    lines.append((f"Generated: {_clean(data['metadata'].get('generated_at')) or datetime.now(timezone.utc).isoformat()}", False))
    lines.append((f"Source: {_clean(data['context'].get('repo')) or 'n/a'} @ {_clean(data['context'].get('branch')) or 'main'}", False))
    lines.append(("", False))
    lines.append(("Executive Summary", True))
    objective = _clean(data["project"].get("objective"))
    if objective:
        lines.append((f"Objective: {objective}", False))
    lines.append((f"Readiness Score: {_clean(data['glance'].get('readiness_score')) or 'n/a'}/100", False))
    lines.append((f"Risk Tier: {_clean(data['glance'].get('risk_tier')) or 'n/a'}", False))
    lines.append(("Generated by Synthetix Analyst Agent", False))
    return lines


def _build_business_docx_bytes_fallback(payload: dict[str, Any], *, run_id: str = "") -> bytes:
    paragraphs = _build_business_paragraphs(_as_dict(payload), run_id)
    package = io.BytesIO()
    with ZipFile(package, mode="w", compression=ZIP_DEFLATED) as zf:
        zf.writestr("[Content_Types].xml", _content_types_xml())
        zf.writestr("_rels/.rels", _package_rels_xml())
        zf.writestr("docProps/core.xml", _core_props_xml())
        zf.writestr("docProps/app.xml", _app_props_xml())
        zf.writestr("word/document.xml", _document_xml(paragraphs))
    return package.getvalue()


def build_business_docx_bytes(
    payload: dict[str, Any],
    *,
    run_id: str = "",
    render_mode: str = "deterministic",
    llm_doc_plan: dict[str, Any] | None = None,
    template_path: str | None = None,
    strict_template: bool = False,
) -> bytes:
    safe = _as_dict(payload)
    try:
        use_plan = llm_doc_plan if str(render_mode).strip().lower() == "llm_rich" else None
        return _build_business_docx_bytes_rich(
            safe,
            run_id=run_id,
            llm_doc_plan=use_plan,
            template_path=template_path,
            strict_template=bool(strict_template),
        )
    except Exception:
        return _build_business_docx_bytes_fallback(safe, run_id=run_id)
