"""
Base agent class providing common LLM interaction patterns for all pipeline agents.
"""

from __future__ import annotations

import json
import os
import re
import ast
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

    def _delivery_constitution_instruction(self, state: dict[str, Any]) -> str:
        """
        Inject the always-on Delivery Constitution from run context.
        """
        run_ctx = state.get("run_context_bundle", {})
        if not isinstance(run_ctx, dict):
            return ""
        constitution = run_ctx.get("delivery_constitution", {})
        if not isinstance(constitution, dict):
            return ""
        constitution_id = str(constitution.get("constitution_id", "")).strip()
        non_negotiables = constitution.get("non_negotiables", [])
        if not isinstance(non_negotiables, list):
            non_negotiables = []
        objective = str(constitution.get("modernization_objective", "")).strip()
        knowledge_snapshot = constitution.get("knowledge_snapshot", {})
        if not isinstance(knowledge_snapshot, dict):
            knowledge_snapshot = {}
        snapshot_id = str(knowledge_snapshot.get("snapshot_id", "")).strip()
        hints = constitution.get("orchestration_hints", [])
        if not isinstance(hints, list):
            hints = []
        routing = run_ctx.get("specialist_routing", {})
        if not isinstance(routing, dict):
            routing = {}
        selected_specialists = routing.get("selected", [])
        if not isinstance(selected_specialists, list):
            selected_specialists = []
        pre_change = constitution.get("checklists", {})
        if not isinstance(pre_change, dict):
            pre_change = {}
        pre_steps = pre_change.get("pre_change", [])
        if not isinstance(pre_steps, list):
            pre_steps = []

        rule_lines = [f"- {str(item).strip()}" for item in non_negotiables[:8] if str(item).strip()]
        hint_lines = []
        for row in hints[:4]:
            if not isinstance(row, dict):
                continue
            when = str(row.get("when", "")).strip()
            route_to = str(row.get("route_to", "")).strip()
            reason = str(row.get("reason", "")).strip()
            if when or route_to or reason:
                hint_lines.append(f"- When={when or 'n/a'} | Route={route_to or 'n/a'} | Why={reason or 'n/a'}")
        route_lines = []
        for row in selected_specialists[:4]:
            if not isinstance(row, dict):
                continue
            name = str(row.get("name", "")).strip()
            agent_key = str(row.get("route_target_agent_key", "")).strip() or str(row.get("linked_agent_key", "")).strip()
            reason = []
            intents = row.get("matched_intents", [])
            if isinstance(intents, list) and intents:
                reason.append(f"intent={','.join([str(x) for x in intents[:2]])}")
            files = row.get("matched_files", [])
            if isinstance(files, list) and files:
                reason.append(f"file={','.join([str(x) for x in files[:2]])}")
            arts = row.get("matched_artifacts", [])
            if isinstance(arts, list) and arts:
                reason.append(f"artifact={','.join([str(x) for x in arts[:2]])}")
            score = str(row.get("score", "")).strip()
            if name or agent_key:
                route_lines.append(
                    f"- {name or 'specialist'} -> {agent_key or 'unbound'}"
                    + (f" | score={score}" if score else "")
                    + (f" | {'; '.join(reason)}" if reason else "")
                )
        pre_lines = [f"- {str(item).strip()}" for item in pre_steps[:4] if str(item).strip()]
        if not rule_lines and not hint_lines and not pre_lines and not route_lines and not objective:
            return ""

        return (
            "DELIVERY CONSTITUTION (ALWAYS-ON):\n"
            f"- Constitution ID: {constitution_id or 'n/a'}\n"
            f"- Knowledge snapshot: {snapshot_id or 'n/a'}\n"
            f"- Objective: {objective or 'n/a'}\n"
            "Non-negotiables:\n"
            + ("\n".join(rule_lines) if rule_lines else "- n/a")
            + "\nRouting hints:\n"
            + ("\n".join(hint_lines) if hint_lines else "- n/a")
            + "\nSpecialist routes selected for this run:\n"
            + ("\n".join(route_lines) if route_lines else "- none")
            + "\nPre-change checklist:\n"
            + ("\n".join(pre_lines) if pre_lines else "- n/a")
        )

    def effective_system_prompt(self, state: dict[str, Any], base_prompt: str | None = None) -> str:
        prompt = base_prompt if isinstance(base_prompt, str) else self.system_prompt
        persona = self._persona_instruction(state)
        context_contract = self._context_contract_instruction(state)
        constitution = self._delivery_constitution_instruction(state)
        additions = [x for x in [persona, constitution, context_contract] if x]
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
    def _normalize_json_text(text: str) -> str:
        replacements = {
            "\u201c": '"',
            "\u201d": '"',
            "\u2018": "'",
            "\u2019": "'",
            "\u00a0": " ",
            "\ufeff": "",
        }
        normalized = str(text or "")
        for src, dst in replacements.items():
            normalized = normalized.replace(src, dst)
        return normalized.strip()

    @staticmethod
    def _quote_unquoted_keys(text: str) -> str:
        return re.sub(r'([{\[,]\s*)([A-Za-z_][A-Za-z0-9_\-]*)(\s*:)', r'\1"\2"\3', text)

    @staticmethod
    def _balanced_json_candidates(text: str) -> list[str]:
        candidates: list[str] = []
        stack: list[tuple[str, int]] = []
        in_string = False
        escape = False
        string_char = ""
        for idx, ch in enumerate(text):
            if in_string:
                if escape:
                    escape = False
                elif ch == "\\":
                    escape = True
                elif ch == string_char:
                    in_string = False
                continue
            if ch in ('"', "'"):
                in_string = True
                string_char = ch
                continue
            if ch in "{[":
                stack.append((ch, idx))
                continue
            if ch in "}]":
                if not stack:
                    continue
                open_ch, start = stack[-1]
                if (open_ch == "{" and ch == "}") or (open_ch == "[" and ch == "]"):
                    stack.pop()
                    if not stack:
                        candidate = text[start : idx + 1].strip()
                        if candidate:
                            candidates.append(candidate)
                else:
                    stack.clear()
        return sorted(set(candidates), key=len, reverse=True)

    @staticmethod
    def _coerce_json_compatible(value: Any) -> Any:
        if isinstance(value, dict):
            return {str(k): BaseAgent._coerce_json_compatible(v) for k, v in value.items()}
        if isinstance(value, list):
            return [BaseAgent._coerce_json_compatible(v) for v in value]
        if isinstance(value, tuple):
            return [BaseAgent._coerce_json_compatible(v) for v in value]
        return value

    @staticmethod
    def extract_json(text: str) -> dict[str, Any]:
        """Extract JSON from LLM response, handling markdown code blocks and common mistakes."""

        def _try_parse(candidate: str) -> dict[str, Any] | None:
            """Attempt increasingly tolerant parse strategies while staying deterministic."""
            candidate = BaseAgent._normalize_json_text(candidate)
            if not candidate:
                return None
            try:
                parsed = json.loads(candidate)
                if isinstance(parsed, str):
                    return _try_parse(parsed)
                return parsed if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                pass
            sanitized = BaseAgent._sanitize_json(candidate)
            try:
                parsed = json.loads(sanitized)
                if isinstance(parsed, str):
                    return _try_parse(parsed)
                return parsed if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                pass
            quoted = BaseAgent._quote_unquoted_keys(sanitized)
            try:
                parsed = json.loads(quoted)
                if isinstance(parsed, str):
                    return _try_parse(parsed)
                return parsed if isinstance(parsed, dict) else None
            except json.JSONDecodeError:
                pass
            try:
                literal = ast.literal_eval(quoted)
            except Exception:
                literal = None
            if isinstance(literal, str):
                return _try_parse(literal)
            if isinstance(literal, dict):
                coerced = BaseAgent._coerce_json_compatible(literal)
                return coerced if isinstance(coerced, dict) else None
            return None

        text = BaseAgent._normalize_json_text(text)

        # 1. Try JSON in fenced code blocks
        for candidate in re.findall(r"```(?:json|javascript|js|python)?\s*\n?(.*?)\n?```", text, re.DOTALL | re.IGNORECASE):
            result = _try_parse(candidate.strip())
            if result is not None:
                return result

        # 2. Try balanced JSON-like segments instead of a greedy regex slice
        for candidate in BaseAgent._balanced_json_candidates(text):
            result = _try_parse(candidate)
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
