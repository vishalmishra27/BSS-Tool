"""
Tool auto-discovery package.

On import, this module scans every ``.py`` file in ``agent/tools/`` for
classes that inherit from ``Tool``.  The results are available via
:func:`discover_tools`.
"""

from __future__ import annotations

import importlib
import logging
import pkgutil
from pathlib import Path
from typing import Dict, List

from .base import Tool

logger = logging.getLogger("agent.tools")


def discover_tools() -> Dict[str, Tool]:
    """
    Scan ``agent/tools/`` for ``Tool`` subclasses and instantiate them.

    Returns a ``{name: instance}`` mapping.
    """
    tools: Dict[str, Tool] = {}
    package_dir = Path(__file__).parent

    for finder, module_name, is_pkg in pkgutil.iter_modules([str(package_dir)]):
        if module_name.startswith("_") or module_name == "base":
            continue
        try:
            module = importlib.import_module(f"agent.tools.{module_name}")
            for attr_name in dir(module):
                attr = getattr(module, attr_name)
                if (
                    isinstance(attr, type)
                    and issubclass(attr, Tool)
                    and attr is not Tool
                ):
                    instance = attr()
                    tools[instance.name] = instance
                    logger.info("Registered tool: %s  (%s.py)", instance.name, module_name)
        except Exception:
            logger.exception("Failed to load tool module %s", module_name)

    return tools
