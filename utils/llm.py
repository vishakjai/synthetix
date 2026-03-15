"""
Unified LLM client that supports both Anthropic (Claude) and OpenAI (GPT-4).
Provides a consistent interface for all agents regardless of provider.
"""

from __future__ import annotations

import json
import os
import time
from dataclasses import dataclass, field
from typing import Any, Generator

from config import LLMProvider, PipelineConfig


@dataclass
class LLMResponse:
    """Structured response from the LLM."""
    content: str
    model: str
    provider: str
    input_tokens: int
    output_tokens: int
    latency_ms: float
    tool_calls: list[dict[str, Any]] = field(default_factory=list)


class LLMClient:
    """Unified client for Anthropic and OpenAI APIs."""

    def __init__(self, config: PipelineConfig):
        self.config = config
        self._anthropic_client = None
        self._openai_client = None
        self._request_timeout_sec = max(
            30.0,
            float(os.getenv("LLM_REQUEST_TIMEOUT_SEC", "240") or 240),
        )

    def _get_anthropic_client(self):
        if self._anthropic_client is None:
            try:
                from anthropic import Anthropic
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "Anthropic provider selected but dependency is missing. "
                    "Install with: uv pip install --python '/Users/vishak/Projects/Codex Projects/.venv/bin/python' anthropic"
                ) from exc
            try:
                self._anthropic_client = Anthropic(
                    api_key=self.config.get_api_key(),
                    timeout=self._request_timeout_sec,
                )
            except TypeError:
                # Backward compatibility for older anthropic client versions.
                self._anthropic_client = Anthropic(api_key=self.config.get_api_key())
        return self._anthropic_client

    def _get_openai_client(self):
        if self._openai_client is None:
            try:
                from openai import OpenAI
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "OpenAI provider selected but dependency is missing. "
                    "Install with: uv pip install --python '/Users/vishak/Projects/Codex Projects/.venv/bin/python' openai"
                ) from exc
            self._openai_client = OpenAI(
                api_key=self.config.get_api_key(),
                timeout=self._request_timeout_sec,
                max_retries=2,
            )
        return self._openai_client

    def _openai_model_supports_max_completion_tokens(self, model_override: str = "") -> bool:
        model = str(model_override or self.config.get_model() or "").strip().lower()
        if not model:
            return False
        return model.startswith("gpt-5") or model.startswith("o1") or model.startswith("o3") or model.startswith("o4")

    def _openai_token_limit_kwargs(self, model_override: str = "") -> dict[str, Any]:
        token_limit = int(self.config.max_output_tokens or 0)
        if token_limit <= 0:
            return {}
        if self._openai_model_supports_max_completion_tokens(model_override):
            return {"max_completion_tokens": token_limit}
        return {"max_tokens": token_limit}

    def _openai_sampling_kwargs(self, model_override: str = "") -> dict[str, Any]:
        temperature = float(self.config.temperature)
        if self._openai_model_supports_max_completion_tokens(model_override):
            if abs(temperature - 1.0) < 1e-9:
                return {"temperature": 1.0}
            return {}
        return {"temperature": temperature}

    @staticmethod
    def _openai_message_text(message: Any) -> str:
        content = getattr(message, "content", "")
        if isinstance(content, str) and content.strip():
            return content
        if isinstance(content, list):
            parts: list[str] = []
            for block in content:
                if isinstance(block, str) and block.strip():
                    parts.append(block)
                    continue
                if isinstance(block, dict):
                    text_value = block.get("text", "")
                    if isinstance(text_value, str) and text_value.strip():
                        parts.append(text_value)
                        continue
                    nested = block.get("content", "")
                    if isinstance(nested, str) and nested.strip():
                        parts.append(nested)
                        continue
                text_attr = getattr(block, "text", "")
                if isinstance(text_attr, str) and text_attr.strip():
                    parts.append(text_attr)
            if parts:
                return "\n".join(parts)
        refusal = getattr(message, "refusal", "")
        if isinstance(refusal, str) and refusal.strip():
            return refusal
        return ""

    @staticmethod
    def _is_anthropic_billing_error(exc: Exception) -> bool:
        text = str(exc or "").lower()
        return (
            "credit balance is too low" in text
            or "plans & billing" in text
            or "purchase credits" in text
            or "billing" in text and "anthropic" in text
        )

    def _can_fallback_to_openai(self) -> bool:
        key = str(self.config.openai_api_key or os.getenv("OPENAI_API_KEY", "")).strip()
        model = str(self.config.openai_model or "").strip()
        return bool(key and model)

    def _fallback_invoke_openai(self, system_prompt: str, user_message: str) -> LLMResponse:
        client = self._get_openai_client()
        model = self.config.openai_model
        response = client.chat.completions.create(
            model=model,
            **self._openai_sampling_kwargs(model),
            **self._openai_token_limit_kwargs(model),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        return LLMResponse(
            content=response.choices[0].message.content,
            model=model,
            provider="openai",
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            latency_ms=0,
        )

    def _fallback_invoke_openai_with_tools(
        self,
        system_prompt: str,
        user_message: str,
        tools: list[dict[str, Any]],
        tool_choice: str,
    ) -> LLMResponse:
        client = self._get_openai_client()
        model = self.config.openai_model
        response = client.chat.completions.create(
            model=model,
            **self._openai_sampling_kwargs(model),
            **self._openai_token_limit_kwargs(model),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            tools=tools,
            tool_choice=tool_choice,
        )
        message = response.choices[0].message
        tool_calls: list[dict[str, Any]] = []
        for call in message.tool_calls or []:
            arguments = call.function.arguments
            parsed_arguments: Any
            try:
                parsed_arguments = json.loads(arguments) if isinstance(arguments, str) else arguments
            except Exception:
                parsed_arguments = arguments
            tool_calls.append(
                {
                    "id": call.id,
                    "name": call.function.name,
                    "arguments": parsed_arguments or {},
                }
            )
        return LLMResponse(
            content=self._openai_message_text(message),
            model=model,
            provider="openai",
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            latency_ms=0,
            tool_calls=tool_calls,
        )

    def invoke(self, system_prompt: str, user_message: str) -> LLMResponse:
        """Send a message to the configured LLM and return structured response."""
        start_time = time.time()
        try:
            if self.config.provider == LLMProvider.ANTHROPIC:
                response = self._invoke_anthropic(system_prompt, user_message)
            else:
                response = self._invoke_openai(system_prompt, user_message)
        except Exception as exc:
            if (
                self.config.provider == LLMProvider.ANTHROPIC
                and self._is_anthropic_billing_error(exc)
                and self._can_fallback_to_openai()
            ):
                response = self._fallback_invoke_openai(system_prompt, user_message)
            else:
                raise

        response.latency_ms = (time.time() - start_time) * 1000
        return response

    def stream(self, system_prompt: str, user_message: str) -> Generator[str, None, None]:
        """Stream response tokens from the configured LLM."""
        if self.config.provider == LLMProvider.ANTHROPIC:
            yield from self._stream_anthropic(system_prompt, user_message)
        else:
            yield from self._stream_openai(system_prompt, user_message)

    def invoke_with_tools(
        self,
        system_prompt: str,
        user_message: str,
        tools: list[dict[str, Any]],
        tool_choice: str = "auto",
    ) -> LLMResponse:
        """
        Send a message with tool definitions and return tool calls requested by the LLM.

        The `tools` argument should follow OpenAI's function-tool schema:
          [{"type": "function", "function": {...}}]
        """
        start_time = time.time()
        try:
            if self.config.provider == LLMProvider.ANTHROPIC:
                response = self._invoke_anthropic_with_tools(
                    system_prompt,
                    user_message,
                    tools,
                    tool_choice,
                )
            else:
                response = self._invoke_openai_with_tools(
                    system_prompt,
                    user_message,
                    tools,
                    tool_choice,
                )
        except Exception as exc:
            if (
                self.config.provider == LLMProvider.ANTHROPIC
                and self._is_anthropic_billing_error(exc)
                and self._can_fallback_to_openai()
            ):
                response = self._fallback_invoke_openai_with_tools(
                    system_prompt,
                    user_message,
                    tools,
                    tool_choice,
                )
            else:
                raise
        response.latency_ms = (time.time() - start_time) * 1000
        return response

    def _invoke_anthropic(self, system_prompt: str, user_message: str) -> LLMResponse:
        client = self._get_anthropic_client()
        response = client.messages.create(
            model=self.config.get_model(),
            max_tokens=self.config.max_output_tokens,
            temperature=self.config.temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )
        return LLMResponse(
            content=response.content[0].text,
            model=self.config.get_model(),
            provider="anthropic",
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            latency_ms=0,
        )

    def _invoke_anthropic_with_tools(
        self,
        system_prompt: str,
        user_message: str,
        tools: list[dict[str, Any]],
        tool_choice: str,
    ) -> LLMResponse:
        client = self._get_anthropic_client()
        anthropic_tools = []
        for tool in tools:
            fn = tool.get("function", {})
            anthropic_tools.append(
                {
                    "name": fn.get("name", "tool"),
                    "description": fn.get("description", ""),
                    "input_schema": fn.get(
                        "parameters",
                        {"type": "object", "properties": {}},
                    ),
                }
            )

        response = client.messages.create(
            model=self.config.get_model(),
            max_tokens=self.config.max_output_tokens,
            temperature=self.config.temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
            tools=anthropic_tools,
            tool_choice={"type": tool_choice},
        )

        content_parts: list[str] = []
        tool_calls: list[dict[str, Any]] = []
        for block in response.content:
            block_type = getattr(block, "type", "")
            if block_type == "text":
                content_parts.append(getattr(block, "text", ""))
            elif block_type == "tool_use":
                tool_calls.append(
                    {
                        "id": getattr(block, "id", ""),
                        "name": getattr(block, "name", ""),
                        "arguments": getattr(block, "input", {}) or {},
                    }
                )

        return LLMResponse(
            content="\n".join(p for p in content_parts if p),
            model=self.config.get_model(),
            provider="anthropic",
            input_tokens=response.usage.input_tokens,
            output_tokens=response.usage.output_tokens,
            latency_ms=0,
            tool_calls=tool_calls,
        )

    def _invoke_openai(self, system_prompt: str, user_message: str) -> LLMResponse:
        client = self._get_openai_client()
        response = client.chat.completions.create(
            model=self.config.get_model(),
            **self._openai_sampling_kwargs(),
            **self._openai_token_limit_kwargs(),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        return LLMResponse(
            content=self._openai_message_text(response.choices[0].message),
            model=self.config.get_model(),
            provider="openai",
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            latency_ms=0,
        )

    def _invoke_openai_with_tools(
        self,
        system_prompt: str,
        user_message: str,
        tools: list[dict[str, Any]],
        tool_choice: str,
    ) -> LLMResponse:
        client = self._get_openai_client()
        response = client.chat.completions.create(
            model=self.config.get_model(),
            **self._openai_sampling_kwargs(),
            **self._openai_token_limit_kwargs(),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
            tools=tools,
            tool_choice=tool_choice,
        )

        message = response.choices[0].message
        tool_calls = []
        for call in (message.tool_calls or []):
            raw_args = call.function.arguments or "{}"
            try:
                parsed_args = json.loads(raw_args)
            except json.JSONDecodeError:
                parsed_args = {"_raw": raw_args}
            tool_calls.append(
                {
                    "id": call.id,
                    "name": call.function.name,
                    "arguments": parsed_args,
                }
            )

        return LLMResponse(
            content=self._openai_message_text(message),
            model=self.config.get_model(),
            provider="openai",
            input_tokens=response.usage.prompt_tokens,
            output_tokens=response.usage.completion_tokens,
            latency_ms=0,
            tool_calls=tool_calls,
        )

    def _stream_anthropic(self, system_prompt: str, user_message: str) -> Generator[str, None, None]:
        client = self._get_anthropic_client()
        with client.messages.stream(
            model=self.config.get_model(),
            max_tokens=self.config.max_output_tokens,
            temperature=self.config.temperature,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        ) as stream:
            for text in stream.text_stream:
                yield text

    def _stream_openai(self, system_prompt: str, user_message: str) -> Generator[str, None, None]:
        client = self._get_openai_client()
        stream = client.chat.completions.create(
            model=self.config.get_model(),
            **self._openai_sampling_kwargs(),
            **self._openai_token_limit_kwargs(),
            stream=True,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
