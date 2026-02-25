from __future__ import annotations

from dataclasses import dataclass
from typing import Any


@dataclass(frozen=True)
class LegacySkillSpec:
    id: str
    name: str
    description: str
    extensions: tuple[str, ...]
    content_tokens: tuple[str, ...]
    analysis_focus: tuple[str, ...]
    maturity: str = "beta"


LEGACY_SKILLS: tuple[LegacySkillSpec, ...] = (
    LegacySkillSpec(
        id="vb6_legacy",
        name="VB6 Legacy Skill",
        description="Deep analysis for VB6 projects (.vbp/.vbg/.frm/.frx/.bas/.cls/.ctl/.ctx/.res), ActiveX/COM dependencies (.ocx), and data definition files (.dcx/.dca).",
        extensions=(".vbp", ".vbg", ".frm", ".frx", ".ctl", ".ctx", ".bas", ".cls", ".res", ".ocx", ".dcx", ".dca", ".vb"),
        content_tokens=(
            "attribute vb_name",
            "begin vb.form",
            "begin vb.mdiform",
            "begin vb.usercontrol",
            "private sub",
            "public sub",
            "mscomctl",
            ".ocx",
            ".dcx",
            ".dca",
            ".dll",
        ),
        analysis_focus=(
            "form inventory and business use mapping",
            "event handlers and control wiring",
            "activex/dll/ocx dependency footprint",
            "project member composition",
            "function-level IO and side effects",
        ),
        maturity="ga",
    ),
    LegacySkillSpec(
        id="asp_classic_legacy",
        name="Classic ASP Legacy Skill",
        description="Analysis for ASP/VBScript pages with Request/Response patterns, server-side rendering, and COM integration.",
        extensions=(".asp", ".asa", ".inc", ".vbs"),
        content_tokens=("request.querystring", "request.form", "response.write", "<%", "server.createobject"),
        analysis_focus=(
            "input/output contract extraction from Request/Response",
            "database query and table usage hints",
            "COM object invocation and side effects",
        ),
    ),
    LegacySkillSpec(
        id="dotnet_webforms_legacy",
        name=".NET WebForms Legacy Skill",
        description="Analysis for ASP.NET WebForms projects with code-behind and control lifecycle patterns.",
        extensions=(".aspx", ".ascx", ".master", ".cs", ".vb", ".config"),
        content_tokens=("runat=\"server\"", "codebehind", "autoeventwireup", "viewstate", "page_load"),
        analysis_focus=(
            "page/control lifecycle behavior",
            "event-driven UI server contract mapping",
            "code-behind dependency tracing",
        ),
    ),
    LegacySkillSpec(
        id="cobol_legacy",
        name="COBOL Legacy Skill",
        description="Analysis for COBOL programs and batch-style processing patterns.",
        extensions=(".cbl", ".cob", ".cpy", ".jcl"),
        content_tokens=("identification division", "procedure division", "working-storage", "perform", "copy "),
        analysis_focus=(
            "copybook and record layout mapping",
            "batch transaction flow extraction",
            "file/database IO contract tracing",
        ),
    ),
    LegacySkillSpec(
        id="powerbuilder_legacy",
        name="PowerBuilder Legacy Skill",
        description="Analysis for PowerBuilder applications with DataWindow-centric behavior.",
        extensions=(".pbl", ".pbt", ".srw", ".sru", ".srf"),
        content_tokens=("datawindow", "openwithparm", "event ", "setitem", "retrieve"),
        analysis_focus=(
            "datawindow usage and query mapping",
            "window/event workflow extraction",
            "database interaction profiling",
        ),
    ),
    LegacySkillSpec(
        id="generic_legacy",
        name="Generic Legacy Skill",
        description="Fallback analyzer when no specific legacy language signature dominates.",
        extensions=(),
        content_tokens=(),
        analysis_focus=(
            "function and module inventory",
            "input/output and side-effect extraction",
            "dependency and data-hint summarization",
        ),
        maturity="ga",
    ),
)

VB6_KEYWORDS = {
    "if",
    "then",
    "else",
    "elseif",
    "end",
    "sub",
    "function",
    "property",
    "call",
    "set",
    "let",
    "get",
    "for",
    "next",
    "while",
    "wend",
    "do",
    "loop",
    "select",
    "case",
    "on",
    "error",
    "resume",
    "goto",
    "dim",
    "as",
    "private",
    "public",
    "friend",
    "protected",
    "const",
    "option",
    "byval",
    "byref",
    "new",
    "not",
    "and",
    "or",
    "xor",
    "mod",
    "with",
    "me",
    "true",
    "false",
}

VB6_DETECTOR_POLICY: tuple[dict[str, Any], ...] = (
    {
        "id": "VB6-ERR-001",
        "pattern": "On Error Resume Next",
        "severity": "high",
        "requires": ["error_model_plan"],
    },
    {
        "id": "VB6-UI-002",
        "pattern": "Control arrays (Index-based controls)",
        "severity": "medium",
        "requires": ["ui_migration_strategy"],
    },
    {
        "id": "VB6-COM-003",
        "pattern": "CreateObject/GetObject/CallByName late binding",
        "severity": "high",
        "requires": ["com_dependency_plan"],
    },
    {
        "id": "VB6-API-004",
        "pattern": "Win32 API Declare usage",
        "severity": "high",
        "requires": ["interop_risk_assessment"],
    },
    {
        "id": "VB6-CONC-005",
        "pattern": "DoEvents / Timer-driven flow",
        "severity": "medium",
        "requires": ["event_loop_plan"],
    },
    {
        "id": "VB6-TYPE-006",
        "pattern": "Variant-heavy typing",
        "severity": "medium",
        "requires": ["type_normalization_plan"],
    },
    {
        "id": "VB6-OOP-007",
        "pattern": "Default instance usage",
        "severity": "medium",
        "requires": ["default_instance_refactor_plan"],
    },
)
