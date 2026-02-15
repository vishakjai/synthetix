"""
Configuration for the 6-Agent Software Development Pipeline.
Supports both Anthropic (Claude) and OpenAI (GPT-4) as LLM providers.
"""

import os
from dataclasses import dataclass, field
from enum import Enum


class LLMProvider(str, Enum):
    ANTHROPIC = "anthropic"
    OPENAI = "openai"


@dataclass
class PipelineConfig:
    """Global pipeline configuration."""

    provider: LLMProvider = LLMProvider.ANTHROPIC

    # Anthropic settings
    anthropic_api_key: str = ""
    anthropic_model: str = "claude-sonnet-4-20250514"

    # OpenAI settings
    openai_api_key: str = ""
    openai_model: str = "gpt-4o"

    # Pipeline settings
    max_retries: int = 2
    developer_parallel_agents: int = 5
    temperature: float = 0.3
    max_output_tokens: int = 6000

    # Deployment settings
    live_deploy: bool = False
    deploy_output_dir: str = "./deploy_output"
    cluster_name: str = "agent-pipeline"
    namespace: str = "agent-app"

    def get_api_key(self) -> str:
        if self.provider == LLMProvider.ANTHROPIC:
            return self.anthropic_api_key or os.getenv("ANTHROPIC_API_KEY", "")
        return self.openai_api_key or os.getenv("OPENAI_API_KEY", "")

    def get_model(self) -> str:
        if self.provider == LLMProvider.ANTHROPIC:
            return self.anthropic_model
        return self.openai_model


# ─── Sample objectives ────────────────────────────────────────────────────────

# Focused samples: narrow scope → the Developer Agent can generate complete,
# buildable code that Docker can compile and K8s can run end-to-end.
SAMPLE_OBJECTIVES_FOCUSED = {
    "Legacy ASP Modernization (to Python/FastAPI)": (
        "Modernize the provided legacy ASP business logic into Python FastAPI while preserving "
        "the same request/response behavior. Capture functional requirements, document input/output "
        "contracts, generate clean modular code, run executable QA checks, and deploy as a Docker "
        "container with /health and /ready endpoints."
    ),
    "Todo REST API (Python/FastAPI)": (
        "Build a single REST API service for managing a todo list. "
        "Use Python with FastAPI. Store todos in an in-memory list (no database). "
        "Endpoints: GET /todos, POST /todos, PUT /todos/{id}, DELETE /todos/{id}. "
        "Include input validation with Pydantic models. "
        "Expose /health and /ready endpoints. Listen on port 8080."
    ),
    "URL Shortener (Python/Flask)": (
        "Build a URL shortener microservice using Python and Flask. "
        "Store shortened URLs in an in-memory dictionary (no database needed). "
        "Endpoints: POST /shorten (accepts {\"url\": \"...\"}, returns short code), "
        "GET /<code> (redirects to original URL), GET /stats/<code> (returns click count). "
        "Expose /health and /ready endpoints. Listen on port 8080."
    ),
    "Weather Proxy API (Node.js/Express)": (
        "Build a weather API proxy service using Node.js with Express. "
        "It should accept GET /weather?city=<name> and return mock weather data "
        "(temperature, humidity, description) — no real external API needed, use hardcoded sample data. "
        "Include GET /cities to list available mock cities. "
        "Expose /health and /ready endpoints. Listen on port 8080."
    ),
    "Markdown Note Service (Python)": (
        "Build a simple note-taking REST API in Python using the built-in http.server module "
        "(no external frameworks). Store notes as in-memory dicts with id, title, content (markdown), "
        "and created_at fields. Endpoints: GET /notes, POST /notes, GET /notes/{id}, DELETE /notes/{id}. "
        "Expose /health and /ready endpoints. Listen on port 8080. Zero external dependencies."
    ),
    "Key-Value Store (Go)": (
        "Build a simple in-memory key-value store REST API in Go using only the standard library. "
        "Endpoints: PUT /kv/{key} (body is the value), GET /kv/{key}, DELETE /kv/{key}, GET /kv (list all keys). "
        "Thread-safe with sync.RWMutex. Expose /health and /ready endpoints. Listen on port 8080."
    ),
}

# Broad samples: larger scope for showcasing the full pipeline's planning
# capabilities (architecture, testing, deployment). Docker builds will likely
# need the stub fallback since the generated code is structural scaffolding.
SAMPLE_OBJECTIVES_BROAD = {
    "E-commerce Platform": (
        "Build a modern e-commerce platform that supports product catalog browsing, "
        "user authentication with OAuth2, shopping cart management, Stripe payment processing, "
        "and real-time order tracking. The system must handle 10,000 concurrent users with "
        "sub-200ms API response times and achieve 99.9% uptime."
    ),
    "Real-time Chat App": (
        "Create a real-time messaging application with WebSocket-based communication, "
        "end-to-end encryption, file sharing up to 100MB, typing indicators, read receipts, "
        "and group chat support for up to 500 members. Must support 50,000 concurrent connections "
        "with message delivery latency under 100ms."
    ),
    "ML Model Serving API": (
        "Design and deploy a machine learning model serving platform that supports A/B testing, "
        "canary deployments, auto-scaling based on inference latency, model versioning with rollback, "
        "batch and real-time prediction endpoints, and comprehensive monitoring dashboards. "
        "Target p99 inference latency of 50ms for real-time predictions."
    ),
    "Task Management SaaS": (
        "Build a project management SaaS application with Kanban boards, Gantt charts, "
        "team collaboration features, role-based access control, GitHub/GitLab integration, "
        "automated notifications via email and Slack, and a REST + GraphQL API. "
        "Support multi-tenancy with data isolation and SOC2 compliance requirements."
    ),
}

# Combined for backward compatibility
SAMPLE_OBJECTIVES = {**SAMPLE_OBJECTIVES_FOCUSED, **SAMPLE_OBJECTIVES_BROAD}
