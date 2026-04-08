"""
Central Configuration — Edit ONCE, used everywhere.
=====================================================
ALL engine files, the agent framework, sox_agent, api.py, and the scoping
engine import credentials from THIS file.  Change your keys here and every
component picks them up automatically.

Just fill in your keys below and everything works.
"""

import os

# ═══════════════════════════════════════════════════════════════════════════════
#  AZURE DOCUMENT INTELLIGENCE (for parsing PDFs, images, Office docs)
# ═══════════════════════════════════════════════════════════════════════════════

DI_KEY      = "ff16a8bd28d94d86b9f3b758e044bca1"
DI_ENDPOINT = "https://entdocintuat.cognitiveservices.azure.com/"


# ═══════════════════════════════════════════════════════════════════════════════
#  AZURE OPENAI (for evaluation, schema generation, LLM refinement, embeddings)
# ═══════════════════════════════════════════════════════════════════════════════

AZURE_OPENAI_ENDPOINT    = "https://entgptaiuat.openai.azure.com"
AZURE_OPENAI_API_KEY     = "808cf0ccab8445b39c6d8767a7e2c433"
AZURE_OPENAI_DEPLOYMENT  = "gpt-5.2-chat"
AZURE_OPENAI_API_VERSION = "2024-12-01-preview"

# Embedding model
AZURE_OPENAI_EMBEDDING_DEPLOYMENT = "text-embedding-ada-002"

# Convenience aliases (used by some engines that expect OPENAI_* names)
OPENAI_API_KEY = AZURE_OPENAI_API_KEY
OPENAI_MODEL   = AZURE_OPENAI_DEPLOYMENT


# ═══════════════════════════════════════════════════════════════════════════════
#  FEATURE TOGGLES
# ═══════════════════════════════════════════════════════════════════════════════

LLM_EVIDENCE_PARSING = True     # LLM post-processing of extracted text
EVIDENCE_EMBEDDINGS  = True     # embedding-based chunk selection for prompts
EVIDENCE_TOP_K       = 24       # top-K chunks to keep after embedding
EVIDENCE_PARSE_WORKERS = 8      # parallel workers for Document Intelligence extraction

# Attribute Training Library — global RAG library of user-approved control attributes.
# When enabled, approved attributes are stored in Cosmos DB and retrieved during
# schema generation to improve attribute quality over time.
# Requires an active Cosmos DB subscription. Set to True when available.
ENABLE_ATTRIBUTE_LIBRARY = False


# ═══════════════════════════════════════════════════════════════════════════════
#  AUTO-APPLY — pushes everything into env vars on import
#  (ensures os.getenv() calls in third-party code also pick up these values)
# ═══════════════════════════════════════════════════════════════════════════════

def _apply():
    _map = {
        "AZURE_DOC_INTELLIGENCE_KEY":           DI_KEY,
        "AZURE_DOC_INTELLIGENCE_ENDPOINT":      DI_ENDPOINT,
        "AZURE_DOCUMENT_INTELLIGENCE_KEY":      DI_KEY,
        "AZURE_DOCUMENT_INTELLIGENCE_ENDPOINT": DI_ENDPOINT,
        "AZURE_OPENAI_ENDPOINT":                AZURE_OPENAI_ENDPOINT,
        "AZURE_OPENAI_API_KEY":                 AZURE_OPENAI_API_KEY,
        "AZURE_OPENAI_DEPLOYMENT_NAME":         AZURE_OPENAI_DEPLOYMENT,
        "AZURE_OPENAI_API_VERSION":             AZURE_OPENAI_API_VERSION,
        "OPENAI_API_KEY":                       AZURE_OPENAI_API_KEY,
        "OPENAI_MODEL":                         AZURE_OPENAI_DEPLOYMENT,
        "AZURE_OPENAI_EMBEDDING_DEPLOYMENT":    AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
        "LLM_EVIDENCE_PARSING":                 "1" if LLM_EVIDENCE_PARSING else "0",
        "EVIDENCE_EMBEDDINGS":                  "1" if EVIDENCE_EMBEDDINGS else "0",
        "EVIDENCE_TOP_K":                       str(EVIDENCE_TOP_K),
        "EVIDENCE_PARSE_WORKERS":               str(EVIDENCE_PARSE_WORKERS),
        "ENABLE_ATTRIBUTE_LIBRARY":             "1" if ENABLE_ATTRIBUTE_LIBRARY else "0",
    }
    for k, v in _map.items():
        if str(v).strip():
            os.environ[k] = str(v).strip()

_apply()
