"""
Abstract base class for all agent tools.

Every tool in ``agent/tools/*.py`` subclasses ``Tool``. The registry
auto-discovers these subclasses at startup — no manual registration.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, List, Optional

from ..types import AgentState, ToolCategory, ToolParameter, ToolResult


class Tool(ABC):
    """
    Base class every agent tool must inherit from.

    Subclasses define:
      * ``name`` / ``description`` / ``category`` / ``parameters``
        — metadata the LLM sees via the OpenAI function-calling schema.
      * ``execute()`` — the actual work.
      * ``preconditions()`` (optional) — fast guard that runs *before*
        execution; returns an error string or ``None``.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Unique tool name used in function calls (e.g. 'load_rcm')."""
        ...

    @property
    @abstractmethod
    def description(self) -> str:
        """What this tool does — shown to the LLM in the function schema."""
        ...

    @property
    def category(self) -> ToolCategory:
        """Logical grouping for the capability summary."""
        return ToolCategory.UTILITY

    @property
    @abstractmethod
    def parameters(self) -> List[ToolParameter]:
        """Ordered list of parameters the tool accepts."""
        ...

    # -- Optional guard ----------------------------------------------------

    def preconditions(self, state: AgentState) -> Optional[str]:
        """
        Return ``None`` if the tool can run, or an error message if it
        cannot (e.g. "No RCM loaded").  Called *before* ``execute()``.
        """
        return None

    # -- Core execution ----------------------------------------------------

    @abstractmethod
    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        """Run the tool and return a structured ``ToolResult``."""
        ...

    # -- Schema generation -------------------------------------------------

    def to_openai_schema(self) -> Dict[str, Any]:
        """Auto-generate the OpenAI function-calling JSON schema."""
        properties: Dict[str, Any] = {}
        required: List[str] = []

        for p in self.parameters:
            prop: Dict[str, Any] = {"type": p.type, "description": p.description}
            if p.enum:
                prop["enum"] = p.enum
            if p.items:
                prop["items"] = p.items
            properties[p.name] = prop
            if p.required:
                required.append(p.name)

        return {
            "type": "function",
            "function": {
                "name": self.name,
                "description": self.description,
                "parameters": {
                    "type": "object",
                    "properties": properties,
                    "required": required,
                },
            },
        }
