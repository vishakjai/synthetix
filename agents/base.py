"""
Base agent class providing common LLM interaction patterns for all pipeline agents.
"""

from __future__ import annotations

import json
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Any, Generator

from utils.llm import LLMClient, LLMResponse


@dataclass
class AgentResult:
    """Standardized output from any agent."""
    agent_name: str
    stage: int
    status: str  # "success", "warning", "error"
    summary: str
    output: dict[str, Any]
    raw_response: str
    tokens_used: int = 0
    latency_ms: float = 0
    logs: list[str] = field(default_factory=list)


class BaseAgent(ABC):
    """Abstract base class for all pipeline agents."""

    def __init__(self, llm: LLMClient):
        self.llm = llm
        self._logs: list[str] = []

    @property
    @abstractmethod
    def name(self) -> str:
        ...

    @property
    @abstractmethod
    def stage(self) -> int:
        ...

    @property
    @abstractmethod
    def system_prompt(self) -> str:
        ...

    def log(self, message: str):
        self._logs.append(message)

    def _persona_instruction(self, state: dict[str, Any]) -> str:
        """
        Resolve an optional persona override for this agent stage.

        Expected state shape:
          state["agent_personas"][str(stage)] = {
            "display_name": "...",
            "persona": "..."
          }
        """
        personas = state.get("agent_personas", {})
        if not isinstance(personas, dict):
            return ""
        stage_payload = personas.get(str(self.stage), {})
        if not isinstance(stage_payload, dict):
            return ""
        persona = str(stage_payload.get("persona", "")).strip()
        if not persona:
            return ""
        display_name = str(stage_payload.get("display_name", "")).strip() or self.name
        return (
            "PERSONA OVERRIDE (HIGH PRIORITY):\n"
            f'You are operating as "{display_name}".\n'
            f"Persona guidance: {persona}\n"
            "Keep your output format contract unchanged."
        )

    def effective_system_prompt(self, state: dict[str, Any], base_prompt: str | None = None) -> str:
        prompt = base_prompt if isinstance(base_prompt, str) else self.system_prompt
        persona = self._persona_instruction(state)
        if not persona:
            return prompt
        return f"{prompt}\n\n{persona}"

    @abstractmethod
    def build_user_message(self, state: dict[str, Any]) -> str:
        """Construct the user message from the current pipeline state."""
        ...

    @abstractmethod
    def parse_output(self, raw: str) -> dict[str, Any]:
        """Parse the LLM's raw text into structured output."""
        ...

    def run(self, state: dict[str, Any]) -> AgentResult:
        """Execute this agent: call LLM, parse output, return result."""
        self._logs = []
        self.log(f"[{self.name}] Starting execution...")

        user_msg = self.build_user_message(state)
        self.log(f"[{self.name}] Sending request to LLM ({self.llm.config.get_model()})...")

        try:
            response = self.llm.invoke(self.effective_system_prompt(state), user_msg)
            self.log(f"[{self.name}] Received response ({response.output_tokens} tokens, {response.latency_ms:.0f}ms)")

            parsed = self.parse_output(response.content)
            self.log(f"[{self.name}] Output parsed successfully")

            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="success",
                summary=self._build_summary(parsed),
                output=parsed,
                raw_response=response.content,
                tokens_used=response.input_tokens + response.output_tokens,
                latency_ms=response.latency_ms,
                logs=self._logs.copy(),
            )
        except Exception as e:
            self.log(f"[{self.name}] ERROR: {e}")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary=f"Agent failed: {e}",
                output={"error": str(e)},
                raw_response="",
                logs=self._logs.copy(),
            )

    def stream_run(self, state: dict[str, Any]) -> Generator[str, None, AgentResult]:
        """Stream the agent's LLM response token by token, then return the parsed result."""
        self._logs = []
        self.log(f"[{self.name}] Starting streamed execution...")

        user_msg = self.build_user_message(state)
        self.log(f"[{self.name}] Streaming from LLM ({self.llm.config.get_model()})...")

        full_response = ""
        try:
            for token in self.llm.stream(self.effective_system_prompt(state), user_msg):
                full_response += token
                yield token

            self.log(f"[{self.name}] Stream complete, parsing output...")
            parsed = self.parse_output(full_response)
            self.log(f"[{self.name}] Output parsed successfully")

            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="success",
                summary=self._build_summary(parsed),
                output=parsed,
                raw_response=full_response,
                logs=self._logs.copy(),
            )
        except Exception as e:
            self.log(f"[{self.name}] ERROR: {e}")
            return AgentResult(
                agent_name=self.name,
                stage=self.stage,
                status="error",
                summary=f"Agent failed: {e}",
                output={"error": str(e)},
                raw_response=full_response,
                logs=self._logs.copy(),
            )

    def _build_summary(self, parsed: dict[str, Any]) -> str:
        """Build a human-readable summary from parsed output. Override in subclasses."""
        return f"{self.name} completed successfully"

    @staticmethod
    def _sanitize_json(text: str) -> str:
        """
        Clean up common LLM JSON mistakes so json.loads() succeeds.

        Handles:
          - JavaScript-style // line comments and /* block comments */
          - Trailing commas before } or ]
          - Single-quoted strings (converted to double-quoted)
        """
        # Remove // line comments (but not inside strings — best effort)
        text = re.sub(r'(?<!["\'])//[^\n]*', '', text)
        # Remove /* ... */ block comments
        text = re.sub(r'/\*.*?\*/', '', text, flags=re.DOTALL)
        # Remove trailing commas before } or ]  (with optional whitespace)
        text = re.sub(r',\s*([}\]])', r'\1', text)
        return text

    @staticmethod
    def extract_json(text: str) -> dict[str, Any]:
        """Extract JSON from LLM response, handling markdown code blocks and common mistakes."""

        def _try_parse(candidate: str) -> dict[str, Any] | None:
            """Attempt strict parse first, then sanitized parse."""
            try:
                return json.loads(candidate)
            except json.JSONDecodeError:
                pass
            try:
                return json.loads(BaseAgent._sanitize_json(candidate))
            except json.JSONDecodeError:
                return None

        # 1. Try JSON in ```json ... ``` code blocks
        json_match = re.search(r"```(?:json)?\s*\n?(.*?)\n?```", text, re.DOTALL)
        if json_match:
            result = _try_parse(json_match.group(1).strip())
            if result is not None:
                return result

        # 2. Try raw JSON object (outermost braces)
        brace_match = re.search(r"\{.*\}", text, re.DOTALL)
        if brace_match:
            result = _try_parse(brace_match.group(0))
            if result is not None:
                return result

        # 3. Last resort: try the full text
        result = _try_parse(text)
        if result is not None:
            return result

        # Nothing worked — raise with a helpful message
        raise json.JSONDecodeError(
            "Could not extract valid JSON from LLM response (tried code blocks, "
            "brace extraction, and sanitization)",
            text[:200],
            0,
        )
