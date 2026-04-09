"""
RAG Engine — Retrieval-Augmented Generation for handbook / reference documents.

Ingests large documents (PDFs, DOCX, etc.) by parsing, chunking, embedding,
and storing vectors in Cosmos DB.  At query time, embeds the question, performs
vector search, and generates a grounded answer with page citations.

Usage:
    from engines.RAGEngine import RAGEngine

    engine = RAGEngine()

    # One-time ingestion
    result = engine.ingest(file_path="/path/to/handbook.pdf", project_id="proj-123")

    # Query
    answer = engine.query(question="What are the key controls?", project_id="proj-123")
"""

from __future__ import annotations

import hashlib
import logging
import os
import time
import uuid
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np

from engines.config import (
    AZURE_OPENAI_API_KEY,
    AZURE_OPENAI_API_VERSION,
    AZURE_OPENAI_DEPLOYMENT,
    AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
    AZURE_OPENAI_ENDPOINT,
)

logger = logging.getLogger("engines.RAGEngine")

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

CHUNK_SIZE_TOKENS = 512          # Target chunk size in approximate tokens
CHUNK_OVERLAP_TOKENS = 100      # Overlap between consecutive chunks
BATCH_SIZE = 16                 # Azure embedding API batch limit
DEFAULT_TOP_K = 10              # Default number of chunks to retrieve
MAX_CONTEXT_CHARS = 12_000      # Max chars of context sent to LLM for answer
EMBED_WORKERS = 4               # Parallel workers for embedding batches


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class Chunk:
    """One chunk of text from a parsed document."""
    text: str
    page: Optional[int] = None
    section_type: Optional[str] = None
    chunk_index: int = 0


@dataclass
class IngestResult:
    """Result of document ingestion."""
    success: bool
    document_name: str
    total_pages: int = 0
    total_chunks: int = 0
    chunks_stored: int = 0
    duration_seconds: float = 0.0
    error: Optional[str] = None


@dataclass
class SourceReference:
    """A source chunk used to answer a question."""
    text: str
    page: Optional[int] = None
    similarity_score: float = 0.0


@dataclass
class QueryResult:
    """Result of a RAG query."""
    success: bool
    answer: str = ""
    sources: List[SourceReference] = field(default_factory=list)
    chunks_retrieved: int = 0
    duration_seconds: float = 0.0
    error: Optional[str] = None


# ---------------------------------------------------------------------------
# Chunking
# ---------------------------------------------------------------------------

def _approx_token_count(text: str) -> int:
    """Rough token estimate: ~4 chars per token for English text."""
    return max(1, len(text) // 4)


def _chunk_text(full_text: str, sections: Optional[List] = None,
                chunk_size: int = CHUNK_SIZE_TOKENS,
                overlap: int = CHUNK_OVERLAP_TOKENS) -> List[Chunk]:
    """Split document text into overlapping chunks.

    If sections (from Document_Intelligence) are provided, uses section
    boundaries as natural break points.  Otherwise falls back to sliding
    window on the full text.
    """
    chunks: List[Chunk] = []

    if sections:
        # Try section-aware chunking first
        chunks = _chunk_by_sections(sections, chunk_size, overlap)
        if chunks:
            return chunks

    # Fallback: sliding window on full text
    chars_per_chunk = chunk_size * 4
    chars_overlap = overlap * 4
    step = max(chars_per_chunk - chars_overlap, 200)

    for i in range(0, len(full_text), step):
        segment = full_text[i:i + chars_per_chunk]
        if not segment.strip():
            continue
        # Estimate page from position (rough: ~3000 chars per page)
        estimated_page = (i // 3000) + 1
        chunks.append(Chunk(
            text=segment.strip(),
            page=estimated_page,
            section_type="text",
            chunk_index=len(chunks),
        ))

    return chunks


def _chunk_by_sections(sections: List, chunk_size: int,
                       overlap: int) -> List[Chunk]:
    """Chunk using Document_Intelligence Section objects as natural boundaries."""
    chunks: List[Chunk] = []
    buffer_text = ""
    buffer_page: Optional[int] = None
    buffer_type: Optional[str] = None
    max_chars = chunk_size * 4
    overlap_chars = overlap * 4

    for section in sections:
        sec_text = getattr(section, "text", "") or ""
        sec_page = getattr(section, "page", None)
        sec_type = getattr(section, "type", None)
        if hasattr(sec_type, "value"):
            sec_type = sec_type.value

        if not sec_text.strip():
            continue

        # If adding this section would exceed chunk size, flush buffer
        if buffer_text and len(buffer_text) + len(sec_text) > max_chars:
            chunks.append(Chunk(
                text=buffer_text.strip(),
                page=buffer_page,
                section_type=buffer_type,
                chunk_index=len(chunks),
            ))
            # Keep overlap from end of buffer
            buffer_text = buffer_text[-overlap_chars:] if len(buffer_text) > overlap_chars else buffer_text
            buffer_page = sec_page
            buffer_type = sec_type

        if not buffer_text:
            buffer_page = sec_page
            buffer_type = sec_type

        buffer_text += "\n" + sec_text

    # Flush remaining buffer
    if buffer_text.strip():
        chunks.append(Chunk(
            text=buffer_text.strip(),
            page=buffer_page,
            section_type=buffer_type,
            chunk_index=len(chunks),
        ))

    return chunks


# ---------------------------------------------------------------------------
# Embedding helpers
# ---------------------------------------------------------------------------

def _get_embedding_client():
    """Create an Azure OpenAI client for embeddings."""
    from openai import AzureOpenAI
    return AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )


def _embed_batch(texts: List[str], client=None) -> List[List[float]]:
    """Embed a batch of texts (max BATCH_SIZE)."""
    if client is None:
        client = _get_embedding_client()

    # Azure text-embedding-ada-002 supports up to 8191 tokens (~32K chars).
    # text-embedding-3-small/large support 8191 tokens as well.
    # Truncate only beyond the actual model limit, not an arbitrary low threshold.
    truncated = [t[:32000] if len(t) > 32000 else t for t in texts]

    try:
        resp = client.embeddings.create(
            model=AZURE_OPENAI_EMBEDDING_DEPLOYMENT,
            input=truncated,
        )
        return [d.embedding for d in resp.data]
    except Exception as exc:
        logger.error("Embedding batch failed: %s", exc)
        return [[0.0] * 1536 for _ in texts]


def _embed_texts(texts: List[str]) -> List[List[float]]:
    """Embed a list of texts, batching as needed."""
    client = _get_embedding_client()
    all_embeddings: List[List[float]] = []

    for i in range(0, len(texts), BATCH_SIZE):
        batch = texts[i:i + BATCH_SIZE]
        embeddings = _embed_batch(batch, client)
        all_embeddings.extend(embeddings)

    return all_embeddings


def _embed_single(text: str) -> List[float]:
    """Embed a single text string."""
    results = _embed_texts([text])
    return results[0] if results else [0.0] * 1536


# ---------------------------------------------------------------------------
# Cosmos DB helpers
# ---------------------------------------------------------------------------

def _get_cosmos_store():
    """Get the CosmosStore singleton."""
    from server.cosmos_store import get_cosmos_store
    return get_cosmos_store()


def _store_chunks_cosmos(
    chunks: List[Chunk],
    embeddings: List[List[float]],
    project_id: str,
    document_name: str,
    source_hash: str,
) -> int:
    """Store chunks + embeddings in Cosmos DB Embeddings container.

    Returns the number of chunks successfully stored.
    """
    store = _get_cosmos_store()
    stored = 0

    for i, (chunk, embedding) in enumerate(zip(chunks, embeddings)):
        metadata = {
            "document_name": document_name,
            "source_hash": source_hash,
            "chunk_index": chunk.chunk_index,
            "page": chunk.page,
            "section_type": chunk.section_type,
            "type": "rag_chunk",           # Distinguish from other embeddings
        }
        doc = store.save_embedding(
            project_id=project_id,
            text=chunk.text,
            embedding=embedding,
            source=f"rag:{document_name}",
            metadata=metadata,
        )
        if doc:
            stored += 1

    return stored


def _search_chunks_cosmos(
    query_vector: List[float],
    project_id: str,
    top_k: int = DEFAULT_TOP_K,
) -> List[Dict]:
    """Search for similar RAG chunks in Cosmos DB.

    Passes metadata_type='rag_chunk' so non-RAG embeddings are filtered
    at the database level rather than in application code.
    """
    store = _get_cosmos_store()
    return store.vector_search(
        project_id=project_id,
        query_vector=query_vector,
        top_k=top_k,
        metadata_type="rag_chunk",
    )


def _check_document_indexed(project_id: str, source_hash: str) -> bool:
    """Check if a document with this hash is already indexed.

    Uses a targeted Cosmos SQL query filtering on metadata.source_hash and
    metadata.type instead of scanning a limited number of embeddings, which
    could miss documents when the project contains many embeddings.
    """
    store = _get_cosmos_store()
    if not store.available or not store._embeddings:
        return False
    try:
        items = list(
            store._embeddings.query_items(
                query=(
                    "SELECT VALUE COUNT(1) FROM c "
                    "WHERE c.projectId = @pid "
                    "AND c.metadata.source_hash = @hash "
                    "AND c.metadata.type = 'rag_chunk'"
                ),
                parameters=[
                    {"name": "@pid", "value": project_id},
                    {"name": "@hash", "value": source_hash},
                ],
                partition_key=project_id,
            )
        )
        count = items[0] if items else 0
        return count > 0
    except Exception as exc:
        logger.warning("_check_document_indexed query failed, falling back to scan: %s", exc)
        # Fallback: scan a reasonable number of embeddings
        embeddings = store.get_embeddings(project_id, limit=100)
        for emb in embeddings:
            meta = emb.get("metadata", {})
            if meta.get("source_hash") == source_hash and meta.get("type") == "rag_chunk":
                return True
        return False


def _delete_document_chunks(project_id: str, document_name: str) -> int:
    """Delete all chunks for a specific document. Returns count deleted."""
    store = _get_cosmos_store()
    embeddings = store.get_embeddings(project_id, limit=5000)
    deleted = 0
    for emb in embeddings:
        meta = emb.get("metadata", {})
        if meta.get("document_name") == document_name and meta.get("type") == "rag_chunk":
            if store.delete_embedding(project_id, emb["id"]):
                deleted += 1
    return deleted


# ---------------------------------------------------------------------------
# LLM answer generation
# ---------------------------------------------------------------------------

def _generate_answer(question: str, context_chunks: List[Dict]) -> str:
    """Generate a grounded answer from retrieved chunks using GPT."""
    from openai import AzureOpenAI

    client = AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        azure_endpoint=AZURE_OPENAI_ENDPOINT,
        api_version=AZURE_OPENAI_API_VERSION,
    )

    # Build context from retrieved chunks
    context_parts = []
    total_chars = 0
    for chunk in context_chunks:
        text = chunk.get("text", "")
        page = chunk.get("metadata", {}).get("page")
        page_label = f" [Page {page}]" if page else ""
        entry = f"---{page_label}\n{text}\n"

        if total_chars + len(entry) > MAX_CONTEXT_CHARS:
            break
        context_parts.append(entry)
        total_chars += len(entry)

    context_str = "\n".join(context_parts)

    system_prompt = (
        "You are a helpful assistant that answers questions based ONLY on the provided "
        "document excerpts. Follow these rules strictly:\n"
        "1. Answer ONLY from the provided excerpts. Do NOT use outside knowledge.\n"
        "2. Cite page numbers when available (e.g., 'According to Page 45...').\n"
        "3. If the excerpts do not contain enough information to answer, say: "
        "'The handbook does not appear to cover this topic in the sections I found.'\n"
        "4. Be concise and direct. Use bullet points for lists.\n"
        "5. If multiple excerpts are relevant, synthesize them into a coherent answer.\n"
    )

    user_prompt = (
        f"DOCUMENT EXCERPTS:\n{context_str}\n\n"
        f"QUESTION: {question}\n\n"
        "Answer based on the excerpts above. Cite page numbers where possible."
    )

    try:
        resp = client.chat.completions.create(
            model=AZURE_OPENAI_DEPLOYMENT,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
            temperature=0.3,
            max_completion_tokens=1500,
        )
        return resp.choices[0].message.content or "No answer generated."
    except Exception as exc:
        logger.error("LLM answer generation failed: %s", exc)
        return f"Error generating answer: {exc}"


# ---------------------------------------------------------------------------
# Main engine class
# ---------------------------------------------------------------------------

def _is_rag_disabled() -> bool:
    """Return True when COSMOS=OFF — RAG requires Cosmos Embeddings container."""
    from server.cosmos_store import is_cosmos_enabled
    return not is_cosmos_enabled()


class RAGEngine:
    """Retrieval-Augmented Generation engine for handbook documents.

    All methods return an error result when COSMOS=OFF (RAG disabled).
    """

    def ingest(
        self,
        file_path: str,
        project_id: str,
        force_reindex: bool = False,
    ) -> IngestResult:
        """Parse, chunk, embed, and store a document.

        Parameters
        ----------
        file_path : str
            Path to the document (PDF, DOCX, etc.)
        project_id : str
            Cosmos DB partition key — typically the project ID.
        force_reindex : bool
            If True, re-index even if the document hash matches.

        Returns
        -------
        IngestResult
        """
        start = time.time()

        if _is_rag_disabled():
            return IngestResult(success=False, document_name="",
                                error="RAG is disabled (COSMOS=OFF). Embeddings require Azure Cosmos DB.",
                                duration_seconds=time.time() - start)

        path = Path(file_path)

        if not path.exists():
            return IngestResult(
                success=False,
                document_name=path.name,
                error=f"File not found: {file_path}",
            )

        document_name = path.name

        # Compute file hash for dedup
        file_hash = hashlib.md5(path.read_bytes()).hexdigest()

        # Check if already indexed
        if not force_reindex and _check_document_indexed(project_id, file_hash):
            logger.info("Document '%s' already indexed (hash=%s), skipping.", document_name, file_hash)
            return IngestResult(
                success=True,
                document_name=document_name,
                total_chunks=0,
                chunks_stored=0,
                duration_seconds=time.time() - start,
                error="Already indexed — use force_reindex=True to re-index.",
            )

        # 1. Parse document
        logger.info("Parsing document: %s", file_path)
        try:
            import sys as _sys
            _di_dir = str(Path(__file__).resolve().parent.parent)
            if _di_dir not in _sys.path:
                _sys.path.insert(0, _di_dir)
            from Document_Intelligence import parse_document
            parsed = parse_document(file_path)
        except Exception as exc:
            logger.error("Failed to parse document: %s", exc)
            return IngestResult(
                success=False,
                document_name=document_name,
                error=f"Parse error: {exc}",
                duration_seconds=time.time() - start,
            )

        full_text = parsed.full_text or ""
        total_pages = parsed.metadata.page_count or 0

        if not full_text.strip():
            return IngestResult(
                success=False,
                document_name=document_name,
                total_pages=total_pages,
                error="No text extracted from document.",
                duration_seconds=time.time() - start,
            )

        # 2. Chunk
        logger.info("Chunking document (%d pages, %d chars)...", total_pages, len(full_text))
        chunks = _chunk_text(full_text, sections=parsed.sections)
        logger.info("Created %d chunks", len(chunks))

        if not chunks:
            return IngestResult(
                success=False,
                document_name=document_name,
                total_pages=total_pages,
                error="No chunks created from document.",
                duration_seconds=time.time() - start,
            )

        # 3. If force re-indexing, delete old chunks first
        if force_reindex:
            deleted = _delete_document_chunks(project_id, document_name)
            if deleted:
                logger.info("Deleted %d old chunks for re-indexing", deleted)

        # 4. Embed all chunks
        logger.info("Embedding %d chunks...", len(chunks))
        chunk_texts = [c.text for c in chunks]
        embeddings = _embed_texts(chunk_texts)

        # 5. Store in Cosmos DB
        logger.info("Storing %d chunks in Cosmos DB...", len(chunks))
        stored = _store_chunks_cosmos(chunks, embeddings, project_id, document_name, file_hash)

        duration = time.time() - start
        logger.info(
            "Ingestion complete: %s — %d pages, %d chunks, %d stored (%.1fs)",
            document_name, total_pages, len(chunks), stored, duration,
        )

        return IngestResult(
            success=True,
            document_name=document_name,
            total_pages=total_pages,
            total_chunks=len(chunks),
            chunks_stored=stored,
            duration_seconds=duration,
        )

    def query(
        self,
        question: str,
        project_id: str,
        top_k: int = DEFAULT_TOP_K,
    ) -> QueryResult:
        """Answer a question using RAG over indexed documents.

        Parameters
        ----------
        question : str
            The user's question.
        project_id : str
            Cosmos DB partition key to search within.
        top_k : int
            Number of chunks to retrieve.

        Returns
        -------
        QueryResult
        """
        start = time.time()

        if _is_rag_disabled():
            return QueryResult(success=False, error="RAG is disabled (COSMOS=OFF). Embeddings require Azure Cosmos DB.",
                               duration_seconds=time.time() - start)

        if not question.strip():
            return QueryResult(
                success=False,
                error="Empty question.",
                duration_seconds=time.time() - start,
            )

        # 1. Embed the question
        logger.info("Embedding question: %s", question[:80])
        query_vector = _embed_single(question)

        # 2. Vector search
        logger.info("Searching for top-%d chunks...", top_k)
        results = _search_chunks_cosmos(query_vector, project_id, top_k)

        # Filter to only RAG chunks (not other embeddings in the project)
        rag_results = [
            r for r in results
            if r.get("metadata", {}).get("type") == "rag_chunk"
        ]

        if not rag_results:
            return QueryResult(
                success=True,
                answer="No relevant information found in the indexed documents. "
                       "Make sure a document has been indexed first.",
                chunks_retrieved=0,
                duration_seconds=time.time() - start,
            )

        # 3. Generate answer
        logger.info("Generating answer from %d chunks...", len(rag_results))
        answer = _generate_answer(question, rag_results)

        # 4. Build source references
        sources = []
        for r in rag_results:
            sources.append(SourceReference(
                text=r.get("text", "")[:200] + "..." if len(r.get("text", "")) > 200 else r.get("text", ""),
                page=r.get("metadata", {}).get("page"),
                similarity_score=r.get("similarityScore", 0.0),
            ))

        duration = time.time() - start
        logger.info("RAG query complete (%.1fs): %d chunks retrieved", duration, len(rag_results))

        return QueryResult(
            success=True,
            answer=answer,
            sources=sources,
            chunks_retrieved=len(rag_results),
            duration_seconds=duration,
        )

    def is_indexed(self, project_id: str) -> Dict[str, Any]:
        """Check if any documents are indexed for this project."""
        if _is_rag_disabled():
            return {"indexed": False, "document_names": [], "chunk_count": 0}
        store = _get_cosmos_store()
        embeddings = store.get_embeddings(project_id, limit=5000)

        doc_names = set()
        chunk_count = 0
        for emb in embeddings:
            meta = emb.get("metadata", {})
            if meta.get("type") == "rag_chunk":
                chunk_count += 1
                name = meta.get("document_name")
                if name:
                    doc_names.add(name)

        return {
            "indexed": chunk_count > 0,
            "document_names": sorted(doc_names),
            "chunk_count": chunk_count,
        }

    def delete_index(self, project_id: str, document_name: Optional[str] = None) -> Dict[str, Any]:
        """Delete indexed chunks. If document_name is provided, only delete that document.
        Otherwise delete all RAG chunks for the project.

        Returns
        -------
        dict with keys: deleted_count (int)
        """
        if _is_rag_disabled():
            return {"deleted_count": 0}
        if document_name:
            deleted = _delete_document_chunks(project_id, document_name)
        else:
            store = _get_cosmos_store()
            embeddings = store.get_embeddings(project_id, limit=5000)
            deleted = 0
            for emb in embeddings:
                meta = emb.get("metadata", {})
                if meta.get("type") == "rag_chunk":
                    if store.delete_embedding(project_id, emb["id"]):
                        deleted += 1

        return {"deleted_count": deleted}
