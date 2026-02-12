# 6-Agent Software Development Pipeline

A full-stack multi-agent orchestration system powered by **LangGraph** with a **Tailwind CSS + Starlette** web UI, with configurable support for both **Anthropic (Claude)** and **OpenAI (GPT-4)**.

This version supports a concrete modernization workflow:
- Paste legacy ASP code
- Upload business objectives and legacy code files
- Choose a target modernization language
- Generate requirements + architecture + rewritten implementation
- Execute QA checks against generated artifacts
- Deploy the modernized service as a live Docker container

## Pipeline Stages

| Stage | Agent | Role |
|-------|-------|------|
| 1 | Analyst Agent | Parses business objectives → structured requirements with acceptance criteria |
| 2 | Architect Agent | Designs architecture optimized for latency, security, scalability |
| 3 | Developer Agent | Decomposes into components, spawns parallel sub-agents for code generation |
| 4 | Tester Agent | Generates unit, integration, load, security, and E2E test suites |
| 5 | Analyst (Validation) | Re-verifies functional requirements against acceptance criteria |
| 6 | Deployment Agent | Containerizes and deploys to Docker with health checks |

## Quick Start

```bash
# 1. Create an isolated env (recommended to avoid FastAPI/Gradio conflicts)
python3 -m venv .venv
source .venv/bin/activate
python -m pip install --upgrade pip

# 2. Install dependencies
pip install -r requirements.txt

# 3. Set your API key
export ANTHROPIC_API_KEY="sk-ant-..."
# or
export OPENAI_API_KEY="sk-..."

# 4. Run the app
cd agent_pipeline
python web/server.py
# Open http://127.0.0.1:8788
```

In the UI:
- Paste legacy ASP source in **Legacy ASP Code**
- Set **Target Modernization Language**
- Run pipeline to produce artifacts + Docker deployment endpoint

## Architecture

```
agent_pipeline/
├── config.py                # Configuration & sample objectives
├── requirements.txt
├── web/
│   ├── server.py            # Starlette API + static file server
│   └── static/
│       ├── index.html       # Tailwind frontend
│       └── app.js           # Frontend orchestration + SSE streaming
├── agents/
│   ├── base.py              # Base agent class (LLM interaction patterns)
│   ├── analyst.py           # Stage 1: Requirements analysis
│   ├── architect.py         # Stage 2: Architecture design
│   ├── developer.py         # Stage 3: Code generation (parallel sub-agents)
│   ├── tester.py            # Stage 4: Test suite generation
│   ├── validator.py         # Stage 5: Requirements validation
│   └── deployer.py          # Stage 6: K8s deployment
├── orchestrator/
│   └── pipeline.py          # LangGraph state graph definition
└── utils/
    ├── llm.py               # Unified Anthropic/OpenAI client (+ tool calling)
    └── run_store.py         # Persistent run storage
```

## Features

- **LangGraph orchestration** with typed state, conditional edges, and failure handling
- **Tool-calling Developer planner** that selects sub-agent work, then executes in parallel
- **Tailwind web UI** with animated progress, SSE live log stream, current-agent focus, tabs, run history, and artifact browser
- **Legacy modernization inputs** — legacy ASP code + target language
- **Use-case selector** — business-objective flow vs code-modernization flow
- **Human approval gates** at every stage (optional), plus mandatory Developer planning approval
- **Developer planning checkpoint** with selectable microservice count, split strategy, target language/platform
- **Executable QA** against generated artifacts (not synthetic-only reports)
- **Deployment target choice**: local Docker execution or cloud execution adapters (AWS/Azure/GCP) with required details
- **Live Docker deployment** with endpoint + health-check evidence (local target)
- **Dual provider support** — switch between Claude and GPT-4 from the sidebar
- **Persistent run history** — stage outputs/logs are saved to disk and can be reloaded
- **Export** full pipeline results as JSON
- **Sample objectives** for quick demos (e-commerce, chat app, ML platform, SaaS)

## Runtime Prerequisites

- Python 3.11+
- Docker CLI + Docker daemon running (for live deployment stage)
- Optional for richer QA:
  - `pytest` for Python test execution
  - `node`/`npm` for Node test execution
  - `go` for Go test execution
