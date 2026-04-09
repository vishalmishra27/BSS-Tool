"""
LLM client abstraction.

Thin wrapper over the OpenAI Python SDK that centralises model selection,
error handling, and call patterns used by the agent loop, planner, and
reflector.

NOTE: gpt-5.2 only supports temperature=1 (the default). We omit the
temperature parameter entirely so the API uses its default.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from openai import AzureOpenAI

logger = logging.getLogger("agent.llm")


class LLMClient:
    """OpenAI chat-completion client used by the agent."""

    def __init__(
        self,
        api_key: str,
        model: str = "gpt-5.2-chat",
        azure_endpoint: str = "",
        azure_api_version: str = "2024-12-01-preview",
    ):
        self._client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=azure_endpoint,
            api_version=azure_api_version,
        )
        self._model = model

    # -- Simple completion (planner, reflector) ----------------------------

    def complete(
        self,
        system: str,
        user: str,
        max_tokens: int = 4096,
        response_format: Optional[Dict[str, Any]] = None,
    ) -> str:
        """System + user prompt → string response."""
        kwargs: Dict[str, Any] = {
            "model": self._model,
            "messages": [
                {"role": "system", "content": system},
                {"role": "user", "content": user},
            ],
            "max_completion_tokens": max_tokens,
        }
        if response_format:
            kwargs["response_format"] = response_format

        response = self._client.chat.completions.create(**kwargs)
        return response.choices[0].message.content or ""

    # -- Chat with function-calling (agent loop) ---------------------------

    def chat_with_tools(
        self,
        system: str,
        messages: List[Dict[str, Any]],
        tools: List[Dict[str, Any]],
        max_tokens: int = 16384,
    ) -> Any:
        """
        Chat completion with OpenAI function-calling.

        Returns the raw ``ChatCompletion`` object so the caller can inspect
        ``choices[0].message.tool_calls``.
        """
        full_messages = [{"role": "system", "content": system}, *messages]
        return self._client.chat.completions.create(
            model=self._model,
            messages=full_messages,
            tools=tools,
            tool_choice="auto",
            max_completion_tokens=max_tokens,
        )
