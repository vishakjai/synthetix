"""
LangGraph-based orchestration pipeline for the 8-agent software development workflow.

Pipeline flow:
  1. Analyst Agent → structured requirements
  2. Architect Agent → system architecture
  3. Developer Agent → parallel sub-agent code generation
  4. Database Engineer Agent → migration scripts and schema plans
  5. Security Engineer Agent → threat model and security controls
  6. Tester Agent → comprehensive test suites
  7. Validator Agent → requirements verification
  8. Deployment Agent → containerized deployment
"""

from __future__ import annotations

import time
from typing import Any, TypedDict, Annotated, Callable

from langgraph.graph import StateGraph, END

from config import PipelineConfig
from utils.llm import LLMClient
from agents.analyst import AnalystAgent
from agents.architect import ArchitectAgent
from agents.developer import DeveloperAgent
from agents.database_engineer import DatabaseEngineerAgent
from agents.security_engineer import SecurityEngineerAgent
from agents.tester import TesterAgent
from agents.validator import ValidatorAgent
from agents.deployer import DeployerAgent
from agents.base import AgentResult
from utils.context_vault import context_gate_issues


class PipelineState(TypedDict, total=False):
    """Typed state that flows through the LangGraph pipeline."""
    # Input
    business_objectives: str
    run_id: str
    legacy_code: str
    modernization_language: str
    database_source: str
    database_target: str
    database_schema: str
    use_case: str
    deployment_target: str
    cloud_config: dict[str, Any]
    human_approval: bool
    strict_security_mode: bool
    sil_ready: bool
    sil_output: dict[str, Any]
    system_context_model: dict[str, Any]
    convention_profile: dict[str, Any]
    health_assessment: dict[str, Any]
    remediation_backlog: list[dict[str, Any]]
    context_vault_ref: dict[str, Any]

    # Agent outputs (populated as pipeline executes)
    analyst_output: dict[str, Any]
    architect_output: dict[str, Any]
    developer_output: dict[str, Any]
    database_engineer_output: dict[str, Any]
    security_engineer_output: dict[str, Any]
    tester_output: dict[str, Any]
    validator_output: dict[str, Any]
    deployer_output: dict[str, Any]
    analyst_answers: list[dict[str, str]]
    tester_feedback: dict[str, Any]

    # Agent results (full AgentResult as dicts for serialization)
    agent_results: list[dict[str, Any]]

    # Pipeline metadata
    current_stage: int
    pipeline_status: str  # "running", "completed", "failed"
    total_tokens: int
    total_latency_ms: float
    logs: list[str]


def _context_reference_from_state(state: dict[str, Any]) -> dict[str, Any]:
    ref = state.get("context_vault_ref", {}) if isinstance(state, dict) else {}
    scm = state.get("system_context_model", {}) if isinstance(state, dict) else {}
    cp = state.get("convention_profile", {}) if isinstance(state, dict) else {}
    ha = state.get("health_assessment", {}) if isinstance(state, dict) else {}
    return {
        "version_id": str(ref.get("version_id", "")),
        "repo": str(ref.get("repo", "")),
        "branch": str(ref.get("branch", "")),
        "commit_sha": str(ref.get("commit_sha", "")),
        "scm_version": str(scm.get("version", "scm-v1")),
        "cp_version": str(cp.get("version", "cp-v1")),
        "ha_version": str(ha.get("version", "ha-v1")),
    }


def _make_node(
    agent_class: type,
    output_key: str,
    config: PipelineConfig,
    on_progress: Callable[[int, str, list[str]], None] | None = None,
):
    """Factory to create a LangGraph node function from an agent class."""
    llm = LLMClient(config)

    # DeployerAgent accepts extra kwargs for live deployment
    if agent_class.__name__ == "DeployerAgent":
        agent = agent_class(
            llm,
            live_deploy=config.live_deploy,
            deploy_output_dir=config.deploy_output_dir,
            cluster_name=config.cluster_name,
            namespace=config.namespace,
        )
    else:
        agent = agent_class(llm)

    def node_fn(state: PipelineState) -> dict[str, Any]:
        if agent.stage >= 2:
            gate_issues = context_gate_issues(state if isinstance(state, dict) else {})
            if gate_issues:
                return {
                    "pipeline_status": "failed",
                    "current_stage": agent.stage,
                    "logs": list(state.get("logs", [])) + [f"[Context Gate] {x}" for x in gate_issues],
                    "agent_results": list(state.get("agent_results", [])) + [{
                        "agent_name": agent.name,
                        "stage": agent.stage,
                        "status": "error",
                        "summary": "Context gate failed: " + "; ".join(gate_issues),
                        "output": {"context_gate_issues": gate_issues},
                        "tokens_used": 0,
                        "latency_ms": 0,
                        "logs": [f"[Context Gate] {x}" for x in gate_issues],
                    }],
                }

        if on_progress:
            on_progress(agent.stage, f"Running {agent.name}...", [])

        result: AgentResult = agent.run(state)
        output_payload = result.output
        if agent.stage >= 2 and isinstance(output_payload, dict):
            output_payload = dict(output_payload)
            output_payload["context_reference"] = _context_reference_from_state(state if isinstance(state, dict) else {})

        if on_progress:
            on_progress(
                agent.stage,
                result.summary,
                result.logs,
            )

        # Build state updates
        existing_results = list(state.get("agent_results", []))
        existing_results.append({
            "agent_name": result.agent_name,
            "stage": result.stage,
            "status": result.status,
            "summary": result.summary,
            "output": output_payload,
            "raw_response": result.raw_response,
            "tokens_used": result.tokens_used,
            "latency_ms": result.latency_ms,
            "logs": result.logs,
        })

        existing_logs = list(state.get("logs", []))
        existing_logs.extend(result.logs)

        updates = {
            output_key: output_payload,
            "agent_results": existing_results,
            "current_stage": agent.stage,
            "total_tokens": state.get("total_tokens", 0) + result.tokens_used,
            "total_latency_ms": state.get("total_latency_ms", 0) + result.latency_ms,
            "logs": existing_logs,
        }

        # Check for failure
        if result.status == "error":
            updates["pipeline_status"] = "failed"

        return updates

    return node_fn


def _should_continue(state: PipelineState) -> str:
    """Conditional edge: stop pipeline if any agent failed."""
    if state.get("pipeline_status") == "failed":
        return "end"
    return "continue"


def build_pipeline(
    config: PipelineConfig,
    on_progress: Callable[[int, str, list[str]], None] | None = None,
) -> StateGraph:
    """
    Construct the LangGraph state graph for the 8-agent pipeline.

    Args:
        config: Pipeline configuration (API keys, model, etc.)
        on_progress: Optional callback(stage, message, logs) for UI updates.

    Returns:
        A compiled LangGraph that accepts PipelineState and returns final state.
    """
    graph = StateGraph(PipelineState)

    # Add nodes
    graph.add_node("analyst", _make_node(AnalystAgent, "analyst_output", config, on_progress))
    graph.add_node("architect", _make_node(ArchitectAgent, "architect_output", config, on_progress))
    graph.add_node("developer", _make_node(DeveloperAgent, "developer_output", config, on_progress))
    graph.add_node("database_engineer", _make_node(DatabaseEngineerAgent, "database_engineer_output", config, on_progress))
    graph.add_node("security_engineer", _make_node(SecurityEngineerAgent, "security_engineer_output", config, on_progress))
    graph.add_node("tester", _make_node(TesterAgent, "tester_output", config, on_progress))
    graph.add_node("validator", _make_node(ValidatorAgent, "validator_output", config, on_progress))
    graph.add_node("deployer", _make_node(DeployerAgent, "deployer_output", config, on_progress))

    # Set entry point
    graph.set_entry_point("analyst")

    # Add edges with conditional failure checks
    graph.add_conditional_edges(
        "analyst",
        _should_continue,
        {"continue": "architect", "end": END},
    )
    graph.add_conditional_edges(
        "architect",
        _should_continue,
        {"continue": "developer", "end": END},
    )
    graph.add_conditional_edges(
        "developer",
        _should_continue,
        {"continue": "database_engineer", "end": END},
    )
    graph.add_conditional_edges(
        "database_engineer",
        _should_continue,
        {"continue": "security_engineer", "end": END},
    )
    graph.add_conditional_edges(
        "security_engineer",
        _should_continue,
        {"continue": "tester", "end": END},
    )
    graph.add_conditional_edges(
        "tester",
        _should_continue,
        {"continue": "validator", "end": END},
    )
    graph.add_conditional_edges(
        "validator",
        _should_continue,
        {"continue": "deployer", "end": END},
    )
    graph.add_edge("deployer", END)

    return graph.compile()


def run_pipeline(
    config: PipelineConfig,
    business_objectives: str,
    on_progress: Callable[[int, str, list[str]], None] | None = None,
) -> PipelineState:
    """
    Execute the full 8-agent pipeline end-to-end (blocking).

    Args:
        config: Pipeline configuration.
        business_objectives: The input business requirements text.
        on_progress: Optional callback for progress updates.

    Returns:
        The final PipelineState with all agent outputs.
    """
    pipeline = build_pipeline(config, on_progress)

    initial_state: PipelineState = {
        "business_objectives": business_objectives,
        "agent_results": [],
        "current_stage": 0,
        "pipeline_status": "running",
        "total_tokens": 0,
        "total_latency_ms": 0,
        "logs": [],
    }

    final_state = pipeline.invoke(initial_state)

    # Mark as completed if not already failed
    if final_state.get("pipeline_status") != "failed":
        final_state["pipeline_status"] = "completed"

    return final_state


# ─── Step-by-step runner for Streamlit compatibility ────────────────────────

# Ordered list of (agent_class, output_key) for stepwise execution
AGENT_SEQUENCE = [
    (AnalystAgent, "analyst_output"),
    (ArchitectAgent, "architect_output"),
    (DeveloperAgent, "developer_output"),
    (DatabaseEngineerAgent, "database_engineer_output"),
    (SecurityEngineerAgent, "security_engineer_output"),
    (TesterAgent, "tester_output"),
    (ValidatorAgent, "validator_output"),
    (DeployerAgent, "deployer_output"),
]


def run_single_stage(
    config: PipelineConfig,
    state: dict[str, Any],
    stage_index: int,
) -> dict[str, Any]:
    """
    Run a single pipeline stage and return the updated state.

    This is designed for Streamlit's rerun-based execution model:
    call this once per stage, store the returned state in session_state,
    then st.rerun() to render progress before calling the next stage.

    Args:
        config: Pipeline configuration.
        state: Current pipeline state dict (mutable copy).
        stage_index: 0-based index into AGENT_SEQUENCE.

    Returns:
        Updated state dict with the agent's output merged in.
    """
    if stage_index < 0 or stage_index >= len(AGENT_SEQUENCE):
        raise ValueError(f"Invalid stage_index: {stage_index}")

    agent_class, output_key = AGENT_SEQUENCE[stage_index]
    stage_num = stage_index + 1

    # Enforce Context Layer gate for downstream stages (Architect onwards).
    if stage_num >= 2:
        gate_issues = context_gate_issues(state if isinstance(state, dict) else {})
        if gate_issues:
            state = dict(state)
            existing_results = list(state.get("agent_results", []))
            existing_results.append({
                "agent_name": agent_class.__name__,
                "stage": stage_num,
                "status": "error",
                "summary": "Context gate failed: " + "; ".join(gate_issues),
                "output": {"context_gate_issues": gate_issues},
                "tokens_used": 0,
                "latency_ms": 0,
                "logs": [f"[Context Gate] {x}" for x in gate_issues],
            })
            state["agent_results"] = existing_results
            state["pipeline_status"] = "failed"
            state["current_stage"] = stage_num
            return state
    llm = LLMClient(config)

    # Instantiate agent (DeployerAgent needs extra kwargs)
    if agent_class.__name__ == "DeployerAgent":
        agent = agent_class(
            llm,
            live_deploy=config.live_deploy,
            deploy_output_dir=config.deploy_output_dir,
            cluster_name=config.cluster_name,
            namespace=config.namespace,
        )
    else:
        agent = agent_class(llm)

    # Run the agent
    result: AgentResult = agent.run(state)

    # Merge result into state
    state = dict(state)  # shallow copy to avoid mutation issues
    output_payload = result.output
    if stage_num >= 2 and isinstance(output_payload, dict):
        output_payload = dict(output_payload)
        output_payload["context_reference"] = _context_reference_from_state(state)
    state[output_key] = output_payload
    if stage_num == 2 and isinstance(output_payload, dict):
        handoff = (
            output_payload.get("architect_handoff_package", {})
            if isinstance(output_payload.get("architect_handoff_package", {}), dict)
            else {}
        )
        if handoff:
            state["architect_handoff_package"] = handoff

    existing_results = list(state.get("agent_results", []))
    existing_results.append({
        "agent_name": result.agent_name,
        "stage": result.stage,
        "status": result.status,
        "summary": result.summary,
        "output": output_payload,
        "tokens_used": result.tokens_used,
        "latency_ms": result.latency_ms,
        "logs": result.logs,
    })
    state["agent_results"] = existing_results

    existing_logs = list(state.get("logs", []))
    existing_logs.extend(result.logs)
    state["logs"] = existing_logs

    state["current_stage"] = result.stage
    state["total_tokens"] = state.get("total_tokens", 0) + result.tokens_used
    state["total_latency_ms"] = state.get("total_latency_ms", 0) + result.latency_ms

    if result.status == "error":
        state["pipeline_status"] = "failed"

    return state


def make_initial_state(business_objectives: str) -> dict[str, Any]:
    """Create a fresh pipeline state for stepwise execution."""
    return {
        "business_objectives": business_objectives,
        "legacy_code": "",
        "modernization_language": "",
        "database_source": "",
        "database_target": "",
        "database_schema": "",
        "use_case": "business_objectives",
        "deployment_target": "local",
        "cloud_config": {},
        "human_approval": False,
        "strict_security_mode": False,
        "sil_ready": False,
        "sil_output": {},
        "system_context_model": {},
        "convention_profile": {},
        "health_assessment": {},
        "remediation_backlog": [],
        "context_vault_ref": {},
        "agent_results": [],
        "current_stage": 0,
        "pipeline_status": "running",
        "total_tokens": 0,
        "total_latency_ms": 0,
        "logs": [],
    }
