"""
Domain Pack utilities for deterministic analyst orchestration.

A Domain Pack is a versioned, structured bundle that guides requirement
compilation for specific industries (for example, banking).
"""

from __future__ import annotations

import re
from copy import deepcopy
from typing import Any


def _keywords(text: str) -> set[str]:
    return {token for token in re.findall(r"[a-z0-9_]+", str(text or "").lower()) if token}


def _score_overlap(haystack: set[str], needles: list[str]) -> float:
    if not needles:
        return 0.0
    matches = sum(1 for needle in needles if needle.lower() in haystack)
    return round(matches / max(1, len(needles)), 3)


def _banking_domain_pack() -> dict[str, Any]:
    return {
        "id": "banking-core-v1",
        "name": "Banking Core Domain Pack",
        "version": "1.0.0",
        "ontology": {
            "framework": "BIAN",
            "capabilities": [
                {
                    "id": "payments_execution",
                    "service_domain": "Payments Execution",
                    "business_capability": "Payments Processing",
                    "keywords": ["payment", "transfer", "pacs", "iso20022", "settlement", "ach", "wire", "rtp"],
                    "dependencies": ["customer_profile", "fraud_detection", "ledger_management"],
                },
                {
                    "id": "customer_profile",
                    "service_domain": "Customer Profile",
                    "business_capability": "Customer Identity & Servicing",
                    "keywords": ["customer", "profile", "kyc", "onboarding", "identity", "account holder"],
                    "dependencies": ["access_control", "audit_trail"],
                },
                {
                    "id": "ledger_management",
                    "service_domain": "Ledger",
                    "business_capability": "Accounting & Balances",
                    "keywords": ["ledger", "balance", "posting", "reversal", "transaction", "journal"],
                    "dependencies": ["audit_trail"],
                },
                {
                    "id": "loan_servicing",
                    "service_domain": "Consumer Loan",
                    "business_capability": "Lending",
                    "keywords": ["loan", "disbursement", "repayment", "interest", "lending", "amortization"],
                    "dependencies": ["customer_profile", "risk_assessment", "ledger_management"],
                },
                {
                    "id": "risk_assessment",
                    "service_domain": "Credit Risk",
                    "business_capability": "Risk Management",
                    "keywords": ["risk", "scoring", "exposure", "limit", "stress", "risk tier"],
                    "dependencies": ["audit_trail"],
                },
                {
                    "id": "fraud_detection",
                    "service_domain": "Fraud",
                    "business_capability": "Fraud & AML",
                    "keywords": ["fraud", "aml", "suspicious", "monitoring", "sanctions", "screening"],
                    "dependencies": ["customer_profile", "audit_trail"],
                },
                {
                    "id": "regulatory_reporting",
                    "service_domain": "Regulatory Reporting",
                    "business_capability": "Compliance Reporting",
                    "keywords": ["regulatory", "reporting", "audit", "sox", "occ", "compliance report"],
                    "dependencies": ["audit_trail", "ledger_management"],
                },
                {
                    "id": "access_control",
                    "service_domain": "Authentication",
                    "business_capability": "Identity and Access",
                    "keywords": ["auth", "authentication", "authorization", "rbac", "abac", "token"],
                    "dependencies": ["audit_trail"],
                },
                {
                    "id": "audit_trail",
                    "service_domain": "Audit Trail",
                    "business_capability": "Auditability",
                    "keywords": ["audit", "trace", "correlation", "immutability", "forensic", "evidence"],
                    "dependencies": [],
                },
            ],
        },
        "standards": [
            {
                "id": "iso20022-core",
                "name": "ISO 20022 Core Messages",
                "applies_to": ["payments_execution", "regulatory_reporting"],
                "engineering_actions": [
                    "Validate payment payload structure against canonical message schema.",
                    "Use deterministic field mapping and explicit validation errors.",
                    "Retain original inbound/outbound message payloads for audit evidence.",
                ],
            },
            {
                "id": "fix-protocol",
                "name": "FIX Protocol Baseline",
                "applies_to": ["risk_assessment", "regulatory_reporting"],
                "engineering_actions": [
                    "Enforce message sequencing and replay protection.",
                    "Capture deterministic routing/acknowledgement states in logs.",
                ],
            },
        ],
        "regulations": [
            {
                "id": "pci_dss",
                "name": "PCI-DSS",
                "jurisdictions": ["US", "EU", "UK", "GLOBAL"],
                "tags": ["payments_execution", "customer_profile"],
                "control_objective": "Protect cardholder data and payment processing flows.",
                "software_actions": [
                    "Mask PAN and sensitive auth data in logs and responses.",
                    "Enforce encryption in transit and at rest for payment data.",
                    "Implement strict RBAC for card-data-adjacent endpoints.",
                ],
                "evidence_required": [
                    "Masked log samples",
                    "Encryption configuration references",
                    "Access control policy coverage report",
                ],
                "effective_date": "2024-01-01",
            },
            {
                "id": "sox",
                "name": "Sarbanes-Oxley",
                "jurisdictions": ["US"],
                "tags": ["ledger_management", "regulatory_reporting", "audit_trail"],
                "control_objective": "Ensure financial reporting integrity and change traceability.",
                "software_actions": [
                    "Record immutable audit events for financial state changes.",
                    "Require dual-control or approval workflows for sensitive operations.",
                    "Link business decisions to technical change evidence.",
                ],
                "evidence_required": [
                    "Audit event model",
                    "Approval workflow traces",
                    "Traceability matrix (requirement -> test -> release)",
                ],
                "effective_date": "2002-07-30",
            },
            {
                "id": "gdpr",
                "name": "GDPR",
                "jurisdictions": ["EU", "UK"],
                "tags": ["customer_profile", "access_control", "audit_trail"],
                "control_objective": "Protect personal data and enforce data subject rights.",
                "software_actions": [
                    "Implement data minimization and purpose tagging for PII fields.",
                    "Support deletion/rectification workflows with audit traces.",
                    "Restrict PII in logs and analytical copies.",
                ],
                "evidence_required": [
                    "PII field inventory",
                    "Data retention/deletion procedure",
                    "PII log redaction tests",
                ],
                "effective_date": "2018-05-25",
            },
            {
                "id": "occ_guidance",
                "name": "OCC Operational Risk Guidance",
                "jurisdictions": ["US"],
                "tags": ["risk_assessment", "fraud_detection", "regulatory_reporting"],
                "control_objective": "Maintain resilient controls for operational risk and incident response.",
                "software_actions": [
                    "Define operational risk thresholds and alerting paths.",
                    "Document fallback/rollback strategies for critical transactions.",
                    "Capture risk decision rationale in machine-readable evidence.",
                ],
                "evidence_required": [
                    "Alert threshold config",
                    "Rollback plan",
                    "Risk acceptance records",
                ],
                "effective_date": "2011-06-01",
            },
        ],
        "gold_patterns": [
            {
                "id": "idempotent-payments-api",
                "title": "Idempotent Payments Endpoint",
                "tags": ["payments_execution", "api", "resilience"],
                "guidance": [
                    "Require Idempotency-Key header for payment creation.",
                    "Persist key + payload hash + response tuple with TTL.",
                    "Return 409 for same key with divergent payload.",
                ],
            },
            {
                "id": "audit-evidence-spine",
                "title": "Audit Evidence Spine",
                "tags": ["audit_trail", "sox", "regulatory_reporting"],
                "guidance": [
                    "Attach stable requirement IDs to audit events and tests.",
                    "Emit correlation_id, trace_id, actor_id on all state changes.",
                    "Persist tamper-evident release evidence pack manifests.",
                ],
            },
            {
                "id": "pii-redaction-policy",
                "title": "PII Redaction and Access Policy",
                "tags": ["gdpr", "customer_profile", "security"],
                "guidance": [
                    "Centralize redaction middleware for logs and errors.",
                    "Define allowlist fields for support and analytics views.",
                    "Block production debug endpoints exposing raw payloads.",
                ],
            },
        ],
        "rules": {
            "non_negotiables": [
                "Audit logging is required for privileged and financial state changes.",
                "Idempotency is required for externally retriable financial write operations.",
                "PII and regulated payment fields must be masked in logs.",
                "Ledger-impacting operations must define consistency and reversal behavior.",
            ],
            "completeness_checklist": [
                "Capability mapping present with confidence and alternatives.",
                "Regulatory constraints mapped to explicit software actions.",
                "BDD scenarios include Given/When/Then and stable IDs.",
                "Acceptance criteria mapped to test strategy.",
                "Open questions and assumptions explicitly listed.",
            ],
        },
        "evaluation_harness": {
            "minimum_functional_requirements": 8,
            "minimum_non_functional_requirements": 5,
            "minimum_bdd_scenarios": 5,
            "required_quality_gates": [
                "gherkin_syntax",
                "requirements_completeness",
                "compliance_constraints_applied",
            ],
        },
    }


def _general_software_pack() -> dict[str, Any]:
    return {
        "id": "software-general-v1",
        "name": "General Software Domain Pack",
        "version": "1.0.0",
        "ontology": {
            "framework": "Capability Taxonomy",
            "capabilities": [
                {
                    "id": "api_platform",
                    "service_domain": "API Platform",
                    "business_capability": "Service Delivery",
                    "keywords": ["api", "endpoint", "service", "backend", "integration"],
                    "dependencies": ["identity_access", "observability"],
                },
                {
                    "id": "identity_access",
                    "service_domain": "Identity",
                    "business_capability": "Authentication & Authorization",
                    "keywords": ["auth", "oauth", "rbac", "session", "token", "login"],
                    "dependencies": ["observability"],
                },
                {
                    "id": "data_management",
                    "service_domain": "Data",
                    "business_capability": "Data Storage & Migration",
                    "keywords": ["database", "schema", "migration", "data", "table", "query"],
                    "dependencies": ["observability"],
                },
                {
                    "id": "observability",
                    "service_domain": "Observability",
                    "business_capability": "Logging & Monitoring",
                    "keywords": ["log", "trace", "metrics", "alert", "monitoring", "reliability"],
                    "dependencies": [],
                },
            ],
        },
        "standards": [],
        "regulations": [],
        "gold_patterns": [],
        "rules": {
            "non_negotiables": [
                "Define explicit error handling and observability requirements.",
                "Include measurable non-functional requirements.",
                "Map acceptance criteria to executable test types.",
            ],
            "completeness_checklist": [
                "Capability mapping present.",
                "BDD scenarios present and linted.",
                "Open questions and assumptions captured.",
            ],
        },
        "evaluation_harness": {
            "minimum_functional_requirements": 6,
            "minimum_non_functional_requirements": 4,
            "minimum_bdd_scenarios": 4,
            "required_quality_gates": [
                "gherkin_syntax",
                "requirements_completeness",
            ],
        },
    }


DOMAIN_PACKS: dict[str, dict[str, Any]] = {
    "banking-core-v1": _banking_domain_pack(),
    "software-general-v1": _general_software_pack(),
}


def list_domain_packs() -> list[dict[str, Any]]:
    return [deepcopy(pack) for pack in DOMAIN_PACKS.values()]


def get_domain_pack(pack_id: str) -> dict[str, Any]:
    key = str(pack_id or "").strip().lower()
    if key in DOMAIN_PACKS:
        return deepcopy(DOMAIN_PACKS[key])
    return deepcopy(DOMAIN_PACKS["software-general-v1"])


def infer_domain_pack_id(objectives: str, explicit_pack_id: str = "") -> str:
    explicit = str(explicit_pack_id or "").strip().lower()
    if explicit in DOMAIN_PACKS:
        return explicit
    kws = _keywords(objectives)
    banking_markers = {
        "bank", "banking", "payment", "payments", "loan", "lending", "ledger", "kyc", "aml",
        "sox", "pci", "iso20022", "swift", "fraud", "settlement", "accounting",
    }
    if any(token in kws for token in banking_markers):
        return "banking-core-v1"
    return "software-general-v1"


def infer_jurisdiction(objectives: str, integration_context: dict[str, Any] | None = None) -> str:
    ctx = integration_context if isinstance(integration_context, dict) else {}
    for key in ("jurisdiction", "regulatory_region", "compliance_region"):
        val = str(ctx.get(key, "")).strip().upper()
        if val:
            return val
    text = str(objectives or "").lower()
    if any(x in text for x in ["gdpr", "europe", "eu ", "european"]):
        return "EU"
    if any(x in text for x in ["fca", "uk ", "united kingdom"]):
        return "UK"
    if any(x in text for x in ["occ", "sox", "us ", "united states"]):
        return "US"
    return "GLOBAL"


def infer_data_classification(objectives: str, integration_context: dict[str, Any] | None = None) -> list[str]:
    ctx = integration_context if isinstance(integration_context, dict) else {}
    declared = ctx.get("data_classification", [])
    if isinstance(declared, list):
        cleaned = [str(item).strip().upper() for item in declared if str(item).strip()]
        if cleaned:
            return sorted(set(cleaned))
    text = str(objectives or "").lower()
    classes: set[str] = set()
    if any(x in text for x in ["pii", "personal data", "customer profile", "user data"]):
        classes.add("PII")
    if any(x in text for x in ["pci", "card", "payment", "pan"]):
        classes.add("PCI")
    if any(x in text for x in ["phi", "health"]):
        classes.add("PHI")
    if not classes:
        classes.add("INTERNAL")
    return sorted(classes)


def normalize_requirement(objectives: str, use_case: str = "business_objectives") -> dict[str, Any]:
    text = str(objectives or "").strip()
    lower = text.lower()
    actors: list[str] = []
    for candidate in ["customer", "banker", "analyst", "admin", "operator", "merchant", "partner", "auditor"]:
        if candidate in lower:
            actors.append(candidate)
    actions: list[str] = []
    for candidate in [
        "create", "update", "delete", "approve", "settle", "transfer", "authenticate", "authorize", "report",
        "modernize", "migrate", "deploy", "validate",
    ]:
        if candidate in lower:
            actions.append(candidate)
    objects: list[str] = []
    for candidate in [
        "payment", "account", "ledger", "transaction", "loan", "customer", "profile", "schema", "api", "service",
    ]:
        if candidate in lower:
            objects.append(candidate)
    constraints: list[str] = []
    for marker in ["latency", "availability", "uptime", "compliance", "audit", "security", "scalability"]:
        if marker in lower:
            constraints.append(marker)
    return {
        "use_case": str(use_case or "business_objectives"),
        "raw_requirement": text,
        "actors": sorted(set(actors)),
        "actions": sorted(set(actions)),
        "objects": sorted(set(objects)),
        "constraints": sorted(set(constraints)),
    }


def map_to_capabilities(domain_pack: dict[str, Any], normalized: dict[str, Any]) -> dict[str, Any]:
    objective_tokens = _keywords(
        " ".join(
            [
                str(normalized.get("raw_requirement", "")),
                " ".join(normalized.get("actions", [])),
                " ".join(normalized.get("objects", [])),
                " ".join(normalized.get("constraints", [])),
            ]
        )
    )
    capabilities = domain_pack.get("ontology", {}).get("capabilities", [])
    scored: list[dict[str, Any]] = []
    for cap in capabilities if isinstance(capabilities, list) else []:
        if not isinstance(cap, dict):
            continue
        score = _score_overlap(objective_tokens, [str(x) for x in cap.get("keywords", []) if str(x).strip()])
        if score <= 0:
            continue
        scored.append(
            {
                "id": str(cap.get("id", "")),
                "service_domain": str(cap.get("service_domain", "")),
                "business_capability": str(cap.get("business_capability", "")),
                "confidence": round(min(0.95, 0.45 + score * 0.5), 3),
                "dependencies": list(cap.get("dependencies", [])) if isinstance(cap.get("dependencies", []), list) else [],
            }
        )
    scored.sort(key=lambda item: float(item.get("confidence", 0.0)), reverse=True)
    primary = scored[:4]
    alternatives = scored[4:8]
    return {
        "framework": str(domain_pack.get("ontology", {}).get("framework", "")),
        "primary_capabilities": primary,
        "alternative_capabilities": alternatives,
        "confidence": round(sum(float(x.get("confidence", 0.0)) for x in primary) / max(1, len(primary)), 3),
    }


def retrieve_regulatory_constraints(
    domain_pack: dict[str, Any],
    capability_ids: list[str],
    jurisdiction: str,
    data_classes: list[str],
) -> list[dict[str, Any]]:
    regs = domain_pack.get("regulations", [])
    selected: list[dict[str, Any]] = []
    capability_set = {str(cid or "").strip() for cid in capability_ids if str(cid or "").strip()}
    juris = str(jurisdiction or "GLOBAL").upper()
    classes = {str(c).upper() for c in (data_classes or [])}

    for reg in regs if isinstance(regs, list) else []:
        if not isinstance(reg, dict):
            continue
        jurisdictions = [str(x).upper() for x in reg.get("jurisdictions", []) if str(x).strip()]
        if jurisdictions and ("GLOBAL" not in jurisdictions) and (juris not in jurisdictions):
            continue
        tags = {str(tag).strip() for tag in reg.get("tags", []) if str(tag).strip()}
        direct_match = bool(capability_set.intersection(tags))
        class_match = "PCI" in classes and str(reg.get("id", "")).lower() == "pci_dss"
        if not direct_match and not class_match:
            continue
        selected.append(
            {
                "id": str(reg.get("id", "")),
                "name": str(reg.get("name", "")),
                "control_objective": str(reg.get("control_objective", "")),
                "software_actions": list(reg.get("software_actions", [])) if isinstance(reg.get("software_actions", []), list) else [],
                "evidence_required": list(reg.get("evidence_required", [])) if isinstance(reg.get("evidence_required", []), list) else [],
                "effective_date": str(reg.get("effective_date", "")),
                "jurisdictions": jurisdictions,
            }
        )
    return selected


def retrieve_gold_patterns(domain_pack: dict[str, Any], capability_ids: list[str]) -> list[dict[str, Any]]:
    patterns = domain_pack.get("gold_patterns", [])
    capability_set = {str(cid or "").strip() for cid in capability_ids if str(cid or "").strip()}
    selected: list[dict[str, Any]] = []
    for pattern in patterns if isinstance(patterns, list) else []:
        if not isinstance(pattern, dict):
            continue
        tags = {str(tag).strip() for tag in pattern.get("tags", []) if str(tag).strip()}
        if not tags.intersection(capability_set) and capability_set:
            continue
        selected.append(
            {
                "id": str(pattern.get("id", "")),
                "title": str(pattern.get("title", "")),
                "guidance": list(pattern.get("guidance", [])) if isinstance(pattern.get("guidance", []), list) else [],
            }
        )
    return selected[:6]


def build_open_questions(
    normalized: dict[str, Any],
    capability_map: dict[str, Any],
    regulatory_constraints: list[dict[str, Any]],
) -> list[str]:
    questions: list[str] = []
    primary = capability_map.get("primary_capabilities", [])
    capability_ids = [str(item.get("id", "")) for item in primary if isinstance(item, dict)]
    if "ledger_management" in capability_ids:
        questions.append("What is the authoritative ledger of record for financial postings?")
        questions.append("Which operations require strong consistency vs eventual consistency?")
    if "payments_execution" in capability_ids:
        questions.append("What is the idempotency window and replay policy for payment requests?")
        questions.append("Are there jurisdiction-specific settlement cut-off rules?")
    if any("gdpr" == str(x.get("id", "")).lower() for x in regulatory_constraints if isinstance(x, dict)):
        questions.append("What are approved retention and deletion SLAs for personal data?")
    if not normalized.get("constraints"):
        questions.append("What are target latency, throughput, and availability SLOs?")
    if not questions:
        questions.append("Are there existing operational constraints or integration dependencies not listed?")
    return questions[:8]

