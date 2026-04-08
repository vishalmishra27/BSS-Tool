"""
Database factory — selects Cosmos DB or PostgreSQL backend based on COSMOS env var.

    COSMOS=ON  (default) → databaseCosmos.Database  (Azure Cosmos DB)
    COSMOS=OFF           → databasePostgres.Database (local PostgreSQL)

All existing imports (``from .database import Database``) continue to work.
"""

import os

_cosmos_mode = os.environ.get("COSMOS", "ON").strip().upper()

if _cosmos_mode == "OFF":
    from .databasePostgres import Database  # noqa: F401
else:
    from .databaseCosmos import Database  # noqa: F401
