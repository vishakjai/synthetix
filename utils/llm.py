"""
Unified LLM client that supports both Anthropic (Claude) and OpenAI (GPT-4).
Provides a consistent interface for all agents regardless of provider.
"""

from __future__ import annotations

import json
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

    def _get_anthropic_client(self):
        if self._anthropic_client is None:
            try:
                from anthropic import Anthropic
            except ModuleNotFoundError as exc:
                raise RuntimeError(
                    "Anthropic provider selected but dependency is missing. "
                    "Install with: uv pip install --python '/Users/vishak/Projects/Codex Projects/.venv/bin/python' anthropic"
                ) from exc
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
            self._openai_client = OpenAI(api_key=self.config.get_api_key())
        return self._openai_client

    def invoke(self, system_prompt: str, user_message: str) -> LLMResponse:
        """Send a message to the configured LLM and return structured response."""
        start_time = time.time()

        if self.config.provider == LLMProvider.ANTHROPIC:
            response = self._invoke_anthropic(system_prompt, user_message)
        else:
            response = self._invoke_openai(system_prompt, user_message)

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
            temperature=self.config.temperature,
            max_tokens=self.config.max_output_tokens,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        return LLMResponse(
            content=response.choices[0].message.content,
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
            temperature=self.config.temperature,
            max_tokens=self.config.max_output_tokens,
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
            content=message.content or "",
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
            temperature=self.config.temperature,
            max_tokens=self.config.max_output_tokens,
            stream=True,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_message},
            ],
        )
        for chunk in stream:
            if chunk.choices[0].delta.content:
                yield chunk.choices[0].delta.content
