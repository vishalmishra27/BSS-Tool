"""
RAG Tools — index_handbook and ask_handbook.

Two tools that let the agent ingest reference documents (handbooks, manuals,
guides) and answer questions from them using Retrieval-Augmented Generation.

The underlying RAG engine uses:
  - Document_Intelligence for PDF/DOCX parsing
  - Azure OpenAI text-embedding-ada-002 for embeddings
  - Cosmos DB Embeddings container for persistent vector storage
  - GPT for grounded answer generation with page citations
"""

from __future__ import annotations

import logging
import time
from typing import Any, Dict, List, Optional

from ..types import AgentState, ToolCategory, ToolParameter, ToolResult
from .base import Tool

logger = logging.getLogger("agent.tools.rag_query")


# ---------------------------------------------------------------------------
# Tool 1: index_handbook
# ---------------------------------------------------------------------------

class IndexHandbookTool(Tool):
    """Index a handbook or reference document for RAG queries."""

    @property
    def name(self) -> str:
        return "index_handbook"

    @property
    def description(self) -> str:
        return (
            "Index a handbook, manual, or reference document so you can answer "
            "questions from it later using ask_handbook. Parses the document, "
            "splits it into chunks, creates embeddings, and stores them persistently. "
            "This is a one-time operation — once indexed, the document remains "
            "available across all chat sessions. Supports PDF, DOCX, and other "
            "document formats."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.UTILITY

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="file_path",
                type="string",
                description="Path to the document file to index (PDF, DOCX, etc.)",
                required=True,
            ),
            ToolParameter(
                name="force_reindex",
                type="boolean",
                description="Force re-indexing even if the document was already indexed. Default: false.",
                required=False,
                default=False,
            ),
        ]

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        start = time.time()
        from server.cosmos_store import is_cosmos_enabled
        if not is_cosmos_enabled():
            return ToolResult(success=False, data={},
                              error="RAG is disabled (COSMOS=OFF). Indexing requires Azure Cosmos DB.",
                              duration_seconds=time.time() - start)
        file_path = args.get("file_path", "")
        force_reindex = args.get("force_reindex", False)

        if not file_path:
            return ToolResult(
                success=False,
                data={},
                error="file_path is required.",
                duration_seconds=time.time() - start,
            )

        # Get project_id from state or use a default
        project_id = getattr(state, "rag_project_id", None) or "icofar-handbook"

        try:
            from engines.RAGEngine import RAGEngine
            engine = RAGEngine()
            result = engine.ingest(
                file_path=file_path,
                project_id=project_id,
                force_reindex=force_reindex,
            )
        except Exception as exc:
            logger.error("index_handbook failed: %s", exc)
            return ToolResult(
                success=False,
                data={},
                error=f"Indexing failed: {exc}",
                duration_seconds=time.time() - start,
            )

        if not result.success:
            return ToolResult(
                success=False,
                data={
                    "document_name": result.document_name,
                    "error": result.error,
                },
                error=result.error,
                duration_seconds=result.duration_seconds,
            )

        # Update state
        state.rag_project_id = project_id
        state.rag_document_name = result.document_name
        state.rag_chunk_count = result.chunks_stored or result.total_chunks

        # Persist RAG state to Cosmos Agent-memory so it survives Flask restarts
        try:
            import json
            from server.cosmos_store import get_cosmos_store
            store = get_cosmos_store()
            if store and store.available:
                session_id = getattr(state, '_session_id', 'default')
                store.save_tool_result(
                    session_id=session_id,
                    tool_key="rag_state",
                    result_json=json.dumps({
                        "rag_project_id": project_id,
                        "rag_document_name": result.document_name,
                        "rag_chunk_count": result.chunks_stored or result.total_chunks,
                    }),
                )
                logger.info("Persisted RAG state to Agent-memory for session=%s", session_id)
        except Exception as e:
            logger.warning("Failed to persist RAG state: %s", e)

        # Check if it was already indexed (no new chunks stored)
        already_indexed = result.chunks_stored == 0 and result.error and "Already indexed" in result.error

        summary = (
            f"'{result.document_name}' was already indexed ({result.total_pages} pages). "
            f"Ready for questions."
            if already_indexed
            else f"Indexed '{result.document_name}': {result.total_pages} pages, "
                 f"{result.total_chunks} chunks stored in {result.duration_seconds:.1f}s. "
                 f"You can now ask questions about it."
        )

        return ToolResult(
            success=True,
            data={
                "document_name": result.document_name,
                "total_pages": result.total_pages,
                "total_chunks": result.total_chunks,
                "chunks_stored": result.chunks_stored,
                "already_indexed": already_indexed,
                "duration_seconds": round(result.duration_seconds, 1),
            },
            summary=summary,
            duration_seconds=result.duration_seconds,
        )


# ---------------------------------------------------------------------------
# Tool 2: ask_handbook
# ---------------------------------------------------------------------------

class AskHandbookTool(Tool):
    """Answer questions from indexed handbook documents using RAG."""

    @property
    def name(self) -> str:
        return "ask_handbook"

    @property
    def description(self) -> str:
        return (
            "Answer a question by searching the user's indexed handbook or "
            "reference documents. Uses semantic search to find relevant sections "
            "and generates a grounded answer with page citations. Use this when "
            "the user asks about content in THEIR uploaded/indexed documents "
            "(handbooks, manuals, policies, guides) — not for general knowledge "
            "questions. Requires a document to be indexed first via index_handbook."
        )

    @property
    def category(self) -> ToolCategory:
        return ToolCategory.ANALYSIS

    @property
    def parameters(self) -> List[ToolParameter]:
        return [
            ToolParameter(
                name="question",
                type="string",
                description="The question to answer from the indexed documents.",
                required=True,
            ),
            ToolParameter(
                name="top_k",
                type="integer",
                description="Number of relevant chunks to retrieve (default: 10). "
                            "Increase for broader context, decrease for more focused answers.",
                required=False,
                default=10,
            ),
        ]

    def preconditions(self, state: AgentState) -> Optional[str]:
        """Check if any documents are indexed."""
        # The ICOFAR handbook is auto-seeded on startup, so this tool
        # is always available even without explicit indexing.
        return None

    def execute(self, args: Dict[str, Any], state: AgentState) -> ToolResult:
        start = time.time()
        from server.cosmos_store import is_cosmos_enabled
        if not is_cosmos_enabled():
            return ToolResult(success=False, data={},
                              error="RAG is disabled (COSMOS=OFF). Querying requires Azure Cosmos DB.",
                              duration_seconds=time.time() - start)
        question = args.get("question", "")
        top_k = args.get("top_k", 10)

        if not question.strip():
            return ToolResult(
                success=False,
                data={},
                error="question is required.",
                duration_seconds=time.time() - start,
            )

        project_id = getattr(state, "rag_project_id", None) or "icofar-handbook"

        try:
            from engines.RAGEngine import RAGEngine
            engine = RAGEngine()
            result = engine.query(
                question=question,
                project_id=project_id,
                top_k=top_k,
            )
        except Exception as exc:
            logger.error("ask_handbook failed: %s", exc)
            return ToolResult(
                success=False,
                data={},
                error=f"Query failed: {exc}",
                duration_seconds=time.time() - start,
            )

        if not result.success:
            return ToolResult(
                success=False,
                data={"error": result.error},
                error=result.error,
                duration_seconds=result.duration_seconds,
            )

        # Build source citations for the response
        source_citations = []
        for src in result.sources:
            citation = {"snippet": src.text}
            if src.page:
                citation["page"] = src.page
            citation["similarity"] = round(src.similarity_score, 3)
            source_citations.append(citation)

        doc_name = getattr(state, "rag_document_name", "indexed document")

        return ToolResult(
            success=True,
            data={
                "answer": result.answer,
                "sources": source_citations,
                "chunks_retrieved": result.chunks_retrieved,
                "document_name": doc_name,
                "duration_seconds": round(result.duration_seconds, 1),
            },
            summary=f"Answered from '{doc_name}' using {result.chunks_retrieved} relevant sections.",
            duration_seconds=result.duration_seconds,
        )
