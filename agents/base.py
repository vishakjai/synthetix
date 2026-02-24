"""
Base agent class providing common LLM interaction patterns for all pipeline agents.
"""

from __future__ import annotations

import json
import os
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

    def _context_contract_instruction(self, state: dict[str, Any]) -> str:
        """
        Inject Context Layer contract instructions for downstream stages.
        """
        if int(self.stage) < 2:
            return ""
        ctx = state.get("context_vault_ref", {})
        scm = state.get("system_context_model", {})
        cp = state.get("convention_profile", {})
        ha = state.get("health_assessment", {})
        rb = state.get("remediation_backlog", [])
        if not isinstance(ctx, dict) or not ctx.get("version_id"):
            return ""
        if not isinstance(scm, dict) or not isinstance(cp, dict) or not isinstance(ha, dict) or not isinstance(rb, list):
            return ""
        return (
            "CONTEXT LAYER CONTRACT (MANDATORY):\n"
            f"- Context vault version_id: {ctx.get('version_id')}\n"
            f"- SCM version: {scm.get('version', 'scm-v1')}\n"
            f"- CP version: {cp.get('version', 'cp-v1')}\n"
            f"- HA version: {ha.get('version', 'ha-v1')}\n"
            "You must align your decisions with these artifacts and avoid violating conventions/topology.\n"
            "If your output supports metadata, include a `context_reference` object that echoes these versions."
        )

    def effective_system_prompt(self, state: dict[str, Any], base_prompt: str | None = None) -> str:
        prompt = base_prompt if isinstance(base_prompt, str) else self.system_prompt
        persona = self._persona_instruction(state)
        context_contract = self._context_contract_instruction(state)
        additions = [x for x in [persona, context_contract] if x]
        if not additions:
            return prompt
        return f"{prompt}\n\n" + "\n\n".join(additions)

    @staticmethod
    def _truncate_text(text: Any, max_chars: int = 6000) -> str:
        raw = str(text or "")
        if len(raw) <= max_chars:
            return raw
        keep_head = int(max_chars * 0.7)
        keep_tail = max_chars - keep_head
        return raw[:keep_head] + "\n...[truncated]...\n" + raw[-keep_tail:]

    @staticmethod
    def _compact_for_prompt(
        value: Any,
        *,
        max_depth: int = 3,
        max_items: int = 10,
        max_str: int = 320,
    ) -> Any:
        def _walk(obj: Any, depth: int) -> Any:
            if depth > max_depth:
                if isinstance(obj, dict):
                    return {"_truncated": f"dict({len(obj)})"}
                if isinstance(obj, list):
                    return [f"... {len(obj)} items ..."]
                return BaseAgent._truncate_text(obj, max_str)
            if isinstance(obj, dict):
                out: dict[str, Any] = {}
                keys = list(obj.keys())
                for key in keys[:max_items]:
                    out[str(key)] = _walk(obj.get(key), depth + 1)
                if len(keys) > max_items:
                    out["_truncated_keys"] = len(keys) - max_items
                return out
            if isinstance(obj, list):
                rows = [_walk(item, depth + 1) for item in obj[:max_items]]
                if len(obj) > max_items:
                    rows.append(f"... {len(obj) - max_items} more items ...")
                return rows
            if isinstance(obj, tuple):
                rows = [_walk(item, depth + 1) for item in list(obj)[:max_items]]
                if len(obj) > max_items:
                    rows.append(f"... {len(obj) - max_items} more items ...")
                return rows
            if isinstance(obj, str):
                return BaseAgent._truncate_text(obj, max_str)
            return obj

        return _walk(value, 0)

    @staticmethod
    def _json_for_prompt(
        value: Any,
        *,
        max_chars: int = 7000,
        max_depth: int = 3,
        max_items: int = 10,
        max_str: int = 320,
    ) -> str:
        compact = BaseAgent._compact_for_prompt(
            value,
            max_depth=max_depth,
            max_items=max_items,
            max_str=max_str,
        )
        format_pref = str(os.getenv("SYNTHETIX_PROMPT_PAYLOAD_FORMAT", "TOON")).strip().upper()
        if format_pref == "JSON":
            try:
                text = json.dumps(compact, ensure_ascii=True, separators=(",", ":"), default=str)
            except Exception:
                text = str(compact)
        else:
            text = BaseAgent._toon_for_prompt(
                compact,
                max_chars=max_chars,
                max_depth=max_depth,
                max_items=max_items,
                max_str=max_str,
            )
        return BaseAgent._truncate_text(text, max_chars=max_chars)

    @staticmethod
    def _toon_atom(value: Any, *, max_str: int = 320) -> str:
        if value is None:
            return "null"
        if isinstance(value, bool):
            return "true" if value else "false"
        if isinstance(value, (int, float)):
            return str(value)
        text = BaseAgent._truncate_text(value, max_chars=max_str).replace("\n", "\\n")
        # Emit unquoted tokens when safe to reduce token load.
        if re.fullmatch(r"[A-Za-z0-9_.:/-]{1,64}", text):
            return text
        return json.dumps(text, ensure_ascii=True)

    @staticmethod
    def _toon_key(key: Any) -> str:
        raw = str(key or "").strip()
        if not raw:
            return "key"
        cleaned = re.sub(r"[^a-zA-Z0-9_]+", "_", raw).strip("_")
        return cleaned or "key"

    @staticmethod
    def _toon_for_prompt(
        value: Any,
        *,
        max_chars: int = 7000,
        max_depth: int = 3,
        max_items: int = 10,
        max_str: int = 320,
    ) -> str:
        """
        TOON (Token-Optimized Object Notation):
        - Flattened dotted paths with scalar values
        - Minimal punctuation and no structural braces
        - Deterministic truncation markers
        """
        lines: list[str] = []

        def inline_dict(obj: dict[str, Any]) -> str:
            parts: list[str] = []
            for idx, (k, v) in enumerate(list(obj.items())[:max_items]):
                key = BaseAgent._toon_key(k)
                if isinstance(v, list):
                    if all(not isinstance(item, (dict, list, tuple)) for item in v[:6]) and len(v) <= 6:
                        joined = ",".join(BaseAgent._toon_atom(item, max_str=max_str) for item in v)
                        parts.append(f"{key}=[{joined}]")
                    else:
                        parts.append(f"{key}=<nested_list>")
                elif isinstance(v, dict):
                    sub_items = list(v.items())
                    scalar_small = (
                        len(sub_items) <= 4
                        and all(not isinstance(val, (dict, list, tuple)) for _, val in sub_items)
                    )
                    if scalar_small:
                        sub = ",".join(
                            f"{BaseAgent._toon_key(sk)}:{BaseAgent._toon_atom(sv, max_str=max_str)}"
                            for sk, sv in sub_items
                        )
                        parts.append(f"{key}={{{sub}}}")
                    else:
                        parts.append(f"{key}=<nested_obj>")
                elif isinstance(v, tuple):
                    parts.append(f"{key}=<nested_tuple>")
                else:
                    parts.append(f"{key}={BaseAgent._toon_atom(v, max_str=max_str)}")
                if idx + 1 >= max_items:
                    break
            if len(obj) > max_items:
                parts.append(f"_truncated_keys={len(obj) - max_items}")
            return "; ".join(parts)

        def emit(obj: Any, depth: int, key: str | None = None) -> None:
            if len("\n".join(lines)) > max_chars:
                return
            indent = "  " * depth
            kprefix = f"{BaseAgent._toon_key(key)}" if key else ""

            if depth > max_depth:
                if kprefix:
                    lines.append(f"{indent}{kprefix}=<truncated_depth>")
                else:
                    lines.append(f"{indent}<truncated_depth>")
                return

            if isinstance(obj, dict):
                items = list(obj.items())
                header = f"{indent}{kprefix}:" if kprefix else None
                if header:
                    lines.append(header)
                if not items:
                    lines.append(f"{indent}  {{}}")
                    return
                for k, v in items[:max_items]:
                    emit(v, depth + (1 if kprefix else 0), str(k))
                    if len("\n".join(lines)) > max_chars:
                        return
                if len(items) > max_items:
                    lines.append(
                        f"{indent}{'  ' if kprefix else ''}_truncated_keys={len(items) - max_items}"
                    )
                return

            if isinstance(obj, list):
                header = f"{indent}{kprefix}:" if kprefix else None
                if header:
                    lines.append(header)
                if not obj:
                    lines.append(f"{indent}  []")
                    return
                scalars = all(not isinstance(item, (dict, list, tuple)) for item in obj[:max_items])
                if scalars and len(obj) <= max_items:
                    joined = ",".join(BaseAgent._toon_atom(item, max_str=max_str) for item in obj)
                    lines.append(f"{indent}{'  ' if kprefix else ''}[{joined}]")
                    return
                for item in obj[:max_items]:
                    if isinstance(item, dict):
                        lines.append(f"{indent}{'  ' if kprefix else ''}- {inline_dict(item)}")
                    elif isinstance(item, list):
                        joined = ",".join(
                            BaseAgent._toon_atom(x, max_str=max_str)
                            for x in item[:max_items]
                            if not isinstance(x, (dict, list, tuple))
                        )
                        lines.append(f"{indent}{'  ' if kprefix else ''}- [{joined}]")
                    else:
                        lines.append(
                            f"{indent}{'  ' if kprefix else ''}- {BaseAgent._toon_atom(item, max_str=max_str)}"
                        )
                    if len("\n".join(lines)) > max_chars:
                        return
                if len(obj) > max_items:
                    lines.append(
                        f"{indent}{'  ' if kprefix else ''}_truncated_items={len(obj) - max_items}"
                    )
                return

            if kprefix:
                lines.append(f"{indent}{kprefix}={BaseAgent._toon_atom(obj, max_str=max_str)}")
            else:
                lines.append(f"{indent}{BaseAgent._toon_atom(obj, max_str=max_str)}")

        emit(value, 0, None)
        text = "\n".join(lines)
        if not text.strip():
            text = "root=<empty>"
        return BaseAgent._truncate_text(text, max_chars=max_chars)

    @staticmethod
    def _chunk_text_for_prompt(
        text: Any,
        *,
        chunk_chars: int = 2200,
        max_chunks: int = 4,
        file_marker_pattern: str = r"(?im)(?=^### FILE:\s+)",
    ) -> list[str]:
        raw = str(text or "").strip()
        if not raw:
            return []
        parts = [p.strip() for p in re.split(file_marker_pattern, raw) if p and p.strip()]
        if len(parts) <= 1:
            return [
                raw[i : i + chunk_chars]
                for i in range(0, min(len(raw), chunk_chars * max_chunks), chunk_chars)
            ]
        chunks: list[str] = []
        buf = ""
        for part in parts:
            block = part + "\n"
            if len(block) > chunk_chars:
                if buf.strip():
                    chunks.append(buf.strip())
                    buf = ""
                for i in range(0, len(block), chunk_chars):
                    piece = block[i : i + chunk_chars].strip()
                    if piece:
                        chunks.append(piece)
                    if len(chunks) >= max_chunks:
                        return chunks
                continue
            if len(buf) + len(block) > chunk_chars:
                if buf.strip():
                    chunks.append(buf.strip())
                buf = block
            else:
                buf += block
            if len(chunks) >= max_chunks:
                return chunks
        if buf.strip() and len(chunks) < max_chunks:
            chunks.append(buf.strip())
        return chunks

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
