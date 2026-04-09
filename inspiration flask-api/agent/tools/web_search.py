"""Web search tool using DuckDuckGo + LLM knowledge fallback."""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from .base import Tool
from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from ..config import get_config

logger = logging.getLogger("agent.tools.web_search")


class WebSearchTool(Tool):
    """Web search tool — DuckDuckGo with LLM knowledge fallback."""

    def __init__(self) -> None:
        # Lazy-initialised; created once per tool instance (startup), not per call.
        self._llm_client = None

    def _get_llm_client(self):
        """Return a cached AzureOpenAI client, creating it on first use."""
        if self._llm_client is None:
            from openai import AzureOpenAI
            config = get_config()
            self._llm_client = AzureOpenAI(
                api_key=config.openai_api_key,
                azure_endpoint=config.azure_openai_endpoint,
                api_version=config.azure_openai_api_version,
            )
        return self._llm_client

    @property
    def name(self) -> str:
        return "web_search"

    @property
    def description(self) -> str:
        return (
            "Search the web for compliance frameworks (SOX, IFC, ICOFR), PCAOB standards, COSO framework, "
            "audit procedures, and general knowledge. Uses DuckDuckGo + LLM knowledge."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter("query", "string", "Search query"),
            ToolParameter("num_results", "integer", "Max results (default 5, max 10)",
                          required=False),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        import requests as req

        query = args.get("query", "").strip()
        num_results = min(args.get("num_results", 5), 10)

        if not query:
            return ToolResult(success=False, data={}, error="No query provided.")

        logger.info("Web search: %s", query)
        results: List[Dict[str, str]] = []
        ddg_summary = ""

        # DuckDuckGo Instant Answer API
        try:
            resp = req.get(
                "https://api.duckduckgo.com/",
                params={"q": query, "format": "json", "no_html": "1", "skip_disambig": "1"},
                timeout=10,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data.get("AbstractText"):
                    ddg_summary = data["AbstractText"]
                    results.append({
                        "title": data.get("AbstractSource", "DuckDuckGo"),
                        "snippet": data["AbstractText"],
                        "url": data.get("AbstractURL", ""),
                    })
                for topic in data.get("RelatedTopics", [])[:num_results]:
                    if isinstance(topic, dict) and topic.get("Text"):
                        results.append({
                            "title": topic.get("Text", "")[:80],
                            "snippet": topic.get("Text", ""),
                            "url": topic.get("FirstURL", ""),
                        })
        except Exception as exc:
            logger.warning("DuckDuckGo API error: %s", exc)

        # LLM knowledge fallback
        llm_answer = ""
        if len(results) < 2:
            try:
                config = get_config()
                client = self._get_llm_client()
                llm_resp = client.chat.completions.create(
                    model=config.openai_model,
                    messages=[
                        {"role": "system",
                         "content": (
                             "You are a compliance and audit expert. Give clear, "
                             "straightforward answers in plain English. Use short sentences. "
                             "Cover SOX, IFC, ICOFR, PCAOB standards, COSO framework, COBIT, "
                             "audit procedures, and internal controls. "
                             "Cite specific standard numbers where possible. If unsure, say so."
                         )},
                        {"role": "user", "content": query},
                    ],
                    max_completion_tokens=1000,
                )
                llm_answer = llm_resp.choices[0].message.content or ""
            except Exception as exc:
                logger.warning("LLM knowledge call error: %s", exc)

        return ToolResult(
            success=True,
            data={
                "query": query,
                "web_results": results[:num_results],
                "llm_knowledge": llm_answer if llm_answer else None,
                "source_note": (
                    "Web results from DuckDuckGo. LLM knowledge from model training data "
                    "— verify critical regulatory details against official sources."
                ),
            },
            summary=f"{len(results)} results for '{query}'",
        )
