"""
Persistent team + persona store for Synthetix.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any


PREMADE_AGENTS: list[dict[str, Any]] = [
    {
        "id": "analyst-strategist",
        "stage": 1,
        "role": "Analyst Agent",
        "display_name": "Strategic Analyst",
        "persona": "Translate business ambiguity into structured, testable requirements with explicit acceptance criteria.",
        "requirements_pack_profile": "requirements-pack-v2-general",
        "is_custom": False,
    },
    {
        "id": "analyst-modernization",
        "stage": 1,
        "role": "Analyst Agent",
        "display_name": "Legacy Modernization Analyst",
        "persona": "Prioritize legacy parity, interface contracts, backward compatibility, and modernization risk documentation.",
        "requirements_pack_profile": "requirements-pack-v2-general",
        "is_custom": False,
    },
    {
        "id": "architect-scalable",
        "stage": 2,
        "role": "Architect Agent",
        "display_name": "Scalable Systems Architect",
        "persona": "Design low-latency, secure, scalable architectures with concrete trade-offs and deployment pragmatism.",
        "is_custom": False,
    },
    {
        "id": "architect-modernization",
        "stage": 2,
        "role": "Architect Agent",
        "display_name": "Modernization Architect",
        "persona": "Map legacy flows to modern services while minimizing migration risk and preserving business behavior.",
        "is_custom": False,
    },
    {
        "id": "developer-delivery",
        "stage": 3,
        "role": "Developer Agent",
        "display_name": "Delivery-Focused Developer Lead",
        "persona": "Generate minimal, buildable code quickly with clear boundaries, robust health endpoints, and reliable packaging.",
        "is_custom": False,
    },
    {
        "id": "developer-modernization",
        "stage": 3,
        "role": "Developer Agent",
        "display_name": "Legacy Refactor Specialist",
        "persona": "Rewrite legacy logic with functional parity first, then improve modularity without changing behavior.",
        "is_custom": False,
    },
    {
        "id": "db-engineer-migration",
        "stage": 4,
        "role": "Database Engineer Agent",
        "display_name": "Migration Database Engineer",
        "persona": "Design low-risk schema/data migration scripts with rollback plans and validation checkpoints.",
        "is_custom": False,
    },
    {
        "id": "db-engineer-optimizer",
        "stage": 4,
        "role": "Database Engineer Agent",
        "display_name": "Performance Database Engineer",
        "persona": "Optimize indexing, query patterns, and migration performance for large datasets.",
        "is_custom": False,
    },
    {
        "id": "security-engineer-principal",
        "stage": 5,
        "role": "Security Engineer Agent",
        "display_name": "Principal Security Engineer",
        "persona": "Produce threat models, prioritized security controls, and release-gating recommendations.",
        "is_custom": False,
    },
    {
        "id": "security-engineer-compliance",
        "stage": 5,
        "role": "Security Engineer Agent",
        "display_name": "Compliance Security Engineer",
        "persona": "Prioritize compliance, auditability, and control evidence with practical remediation paths.",
        "is_custom": False,
    },
    {
        "id": "tester-pragmatic",
        "stage": 6,
        "role": "Tester Agent",
        "display_name": "Pragmatic QA Engineer",
        "persona": "Run actionable checks, separate environment issues from code defects, and produce clear remediation guidance.",
        "is_custom": False,
    },
    {
        "id": "tester-security",
        "stage": 6,
        "role": "Tester Agent",
        "display_name": "Security QA Engineer",
        "persona": "Bias toward security verification and classify findings by severity and exploitability.",
        "is_custom": False,
    },
    {
        "id": "validator-business",
        "stage": 7,
        "role": "Analyst (Validation)",
        "display_name": "Business Validation Analyst",
        "persona": "Validate requirement coverage clearly for business stakeholders with traceable evidence.",
        "is_custom": False,
    },
    {
        "id": "validator-audit",
        "stage": 7,
        "role": "Analyst (Validation)",
        "display_name": "Audit Validation Analyst",
        "persona": "Produce strict compliance-style validation with explicit gap severity and sign-off criteria.",
        "is_custom": False,
    },
    {
        "id": "deployer-local",
        "stage": 8,
        "role": "Deployment Agent",
        "display_name": "Local-First Deployer",
        "persona": "Prefer local Docker deployability, deterministic health checks, and clear run instructions.",
        "is_custom": False,
    },
    {
        "id": "deployer-cloud",
        "stage": 8,
        "role": "Deployment Agent",
        "display_name": "Cloud Deployment Engineer",
        "persona": "Prioritize cloud readiness, operational safety, and explicit platform-specific deployment steps.",
        "is_custom": False,
    },
]


DEFAULT_STAGE_AGENT_IDS_BALANCED: dict[str, str] = {
    "1": "analyst-strategist",
    "2": "architect-scalable",
    "3": "developer-delivery",
    "4": "db-engineer-migration",
    "5": "security-engineer-principal",
    "6": "tester-pragmatic",
    "7": "validator-business",
    "8": "deployer-local",
}

DEFAULT_STAGE_AGENT_IDS_MODERNIZATION: dict[str, str] = {
    "1": "analyst-modernization",
    "2": "architect-modernization",
    "3": "developer-modernization",
    "4": "db-engineer-migration",
    "5": "security-engineer-principal",
    "6": "tester-pragmatic",
    "7": "validator-business",
    "8": "deployer-local",
}


DEFAULT_TEAMS: list[dict[str, Any]] = [
    {
        "id": "team-synthetix-balanced",
        "name": "Synthetix Balanced Team",
        "description": "General-purpose delivery team for most product tasks.",
        "stage_agent_ids": DEFAULT_STAGE_AGENT_IDS_BALANCED,
        "is_custom": False,
    },
    {
        "id": "team-synthetix-modernization",
        "name": "Synthetix Modernization Team",
        "description": "Legacy modernization team focused on functional parity and migration safety.",
        "stage_agent_ids": DEFAULT_STAGE_AGENT_IDS_MODERNIZATION,
        "is_custom": False,
    },
]


def _safe_json_load(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text())
    except json.JSONDecodeError:
        return default


def _safe_json_write(path: Path, payload: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, indent=2, ensure_ascii=True))
    tmp.replace(path)


@dataclass
class TeamStore:
    root_dir: str

    def __post_init__(self) -> None:
        self.root = Path(self.root_dir)
        self.root.mkdir(parents=True, exist_ok=True)
        self.custom_agents_path = self.root / "custom_agents.json"
        self.custom_teams_path = self.root / "custom_teams.json"
        if not self.custom_agents_path.exists():
            _safe_json_write(self.custom_agents_path, [])
        if not self.custom_teams_path.exists():
            _safe_json_write(self.custom_teams_path, [])

    def list_agents(self) -> dict[str, Any]:
        custom = _safe_json_load(self.custom_agents_path, [])
        custom_list = custom if isinstance(custom, list) else []
        all_agents = PREMADE_AGENTS + custom_list
        return {"premade": PREMADE_AGENTS, "custom": custom_list, "all": all_agents}

    def get_agent(self, agent_id: str) -> dict[str, Any] | None:
        target = str(agent_id).strip()
        if not target:
            return None
        for agent in self.list_agents()["all"]:
            if str(agent.get("id", "")) == target:
                return agent
        return None

    def clone_agent(
        self,
        base_agent_id: str,
        display_name: str,
        persona: str,
        requirements_pack_profile: str = "",
        requirements_pack_template: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        base = self.get_agent(base_agent_id)
        if not base:
            raise ValueError("base agent not found")
        cloned = dict(base)
        cloned["id"] = f"custom-agent-{uuid.uuid4().hex[:10]}"
        cloned["display_name"] = display_name.strip() or f"{base.get('display_name', 'Agent')} (Clone)"
        cloned["persona"] = persona.strip() or str(base.get("persona", ""))
        cloned["is_custom"] = True
        cloned["based_on"] = base_agent_id
        if int(cloned.get("stage", 0) or 0) == 1:
            profile = requirements_pack_profile.strip() or str(base.get("requirements_pack_profile", "")).strip()
            if profile:
                cloned["requirements_pack_profile"] = profile
            template = requirements_pack_template if isinstance(requirements_pack_template, dict) else {}
            if template:
                cloned["requirements_pack_template"] = template

        existing = _safe_json_load(self.custom_agents_path, [])
        custom_agents = existing if isinstance(existing, list) else []
        custom_agents.append(cloned)
        _safe_json_write(self.custom_agents_path, custom_agents)
        return cloned

    def list_teams(self) -> list[dict[str, Any]]:
        custom = _safe_json_load(self.custom_teams_path, [])
        custom_list = custom if isinstance(custom, list) else []
        return DEFAULT_TEAMS + custom_list

    def get_team(self, team_id: str) -> dict[str, Any] | None:
        target = str(team_id).strip()
        if not target:
            return None
        for team in self.list_teams():
            if str(team.get("id", "")) == target:
                return team
        return None

    def save_team(
        self,
        name: str,
        stage_agent_ids: dict[str, Any],
        description: str = "",
        team_id: str = "",
    ) -> dict[str, Any]:
        normalized_stage_ids: dict[str, str] = {}
        for stage in ["1", "2", "3", "4", "5", "6", "7", "8"]:
            candidate = str(stage_agent_ids.get(stage, "")).strip()
            if not candidate or not self.get_agent(candidate):
                candidate = DEFAULT_STAGE_AGENT_IDS_BALANCED[stage]
            normalized_stage_ids[stage] = candidate

        payload = {
            "id": team_id.strip() or f"custom-team-{uuid.uuid4().hex[:10]}",
            "name": name.strip() or "Untitled Team",
            "description": description.strip(),
            "stage_agent_ids": normalized_stage_ids,
            "is_custom": True,
        }

        existing = _safe_json_load(self.custom_teams_path, [])
        custom_teams = existing if isinstance(existing, list) else []
        idx = next((i for i, t in enumerate(custom_teams) if str(t.get("id", "")) == payload["id"]), -1)
        if idx >= 0:
            custom_teams[idx] = payload
        else:
            custom_teams.append(payload)
        _safe_json_write(self.custom_teams_path, custom_teams)
        return payload

    def suggest_team(self, challenge_text: str) -> dict[str, Any]:
        text = str(challenge_text or "").lower()
        modernization_keywords = ["legacy", "moderniz", "migration", "asp", "refactor"]
        db_keywords = ["database", "schema", "sql", "postgres", "mysql", "oracle", "migrate"]
        modernization = any(k in text for k in modernization_keywords)
        db_migration = any(k in text for k in db_keywords)
        if modernization or db_migration:
            team = self.get_team("team-synthetix-modernization") or DEFAULT_TEAMS[1]
            reason = (
                "Detected modernization/database-migration intent from challenge keywords."
            )
        else:
            team = self.get_team("team-synthetix-balanced") or DEFAULT_TEAMS[0]
            reason = "Defaulted to balanced team for general product delivery."
        return {
            "team_id": team.get("id"),
            "team_name": team.get("name"),
            "stage_agent_ids": team.get("stage_agent_ids", {}),
            "reason": reason,
        }

    def resolve_personas(
        self,
        team_id: str = "",
        stage_agent_ids: dict[str, Any] | None = None,
    ) -> tuple[dict[str, dict[str, str]], dict[str, Any]]:
        selected: dict[str, str] = {}
        team_meta: dict[str, Any] = {}

        if team_id:
            team = self.get_team(team_id)
            if team:
                team_meta = team
                source_map = team.get("stage_agent_ids", {})
                if isinstance(source_map, dict):
                    for k, v in source_map.items():
                        selected[str(k)] = str(v)

        if isinstance(stage_agent_ids, dict):
            for k, v in stage_agent_ids.items():
                if str(v).strip():
                    selected[str(k)] = str(v).strip()

        personas: dict[str, dict[str, str]] = {}
        for stage in ["1", "2", "3", "4", "5", "6", "7", "8"]:
            agent_id = selected.get(stage, DEFAULT_STAGE_AGENT_IDS_BALANCED[stage])
            agent = self.get_agent(agent_id) or self.get_agent(DEFAULT_STAGE_AGENT_IDS_BALANCED[stage])
            personas[stage] = {
                "agent_id": str(agent.get("id", "")) if agent else "",
                "display_name": str(agent.get("display_name", "")) if agent else "",
                "persona": str(agent.get("persona", "")) if agent else "",
                "requirements_pack_profile": str(agent.get("requirements_pack_profile", "")).strip() if agent else "",
                "requirements_pack_template": (
                    agent.get("requirements_pack_template", {})
                    if isinstance(agent.get("requirements_pack_template", {}), dict)
                    else {}
                ) if agent else {},
            }
            selected[stage] = personas[stage]["agent_id"]

        if not team_meta:
            team_meta = {
                "id": team_id or "ad-hoc-team",
                "name": "Ad-hoc Team",
                "description": "Resolved from manual stage selections.",
                "stage_agent_ids": selected,
                "is_custom": True,
            }
        return personas, team_meta
