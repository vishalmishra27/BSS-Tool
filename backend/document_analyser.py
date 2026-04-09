"""
Document Analyser — mirrors the GraphRAGEngine API from AAA 2 / graph_rag.py,
but uses Groq (llama-3.3-70b) instead of Azure OpenAI.

Pipeline (identical to AAA 2):
  PDF → pdfplumber extraction → section-aware chunking (pdf_parser.py)
      → keyword-ranked chunk retrieval → LLM answer via Groq

Public API matches GraphRAGEngine so ocr_endpoints.py can swap implementations
later (e.g. switch to Azure OpenAI by swapping this module).
"""

import os
import re
import json
import logging
from openai import AzureOpenAI
from dotenv import load_dotenv

from pdf_parser import process_pdf   # identical copy from AAA 2

load_dotenv()
logger = logging.getLogger(__name__)

MODEL = os.getenv("AZURE_OPENAI_DEPLOYMENT", "gpt-4o")
_client = None

# ── Prompts (same intent as AAA 2 graph_rag.py) ────────────────────────────────

QUERY_PROMPT = """You are a document analysis assistant.
You have been given the full text and relevant sections from one or more documents.

DOCUMENT CONTENT:
{text_context}

USER QUESTION:
{question}

Instructions:
- Answer based ONLY on the provided document content.
- Be COMPREHENSIVE — include ALL relevant details, sub-clauses, conditions, exceptions, notice periods, amounts, dates.
- Do NOT summarise away important details. If a section has 10 points, list all 10.
- If the user asks for a table, return a proper markdown table with headers and rows.
- If information is genuinely not found, say so clearly.
- Be precise and cite specific sections or document names when possible.
- For monetary values, include exact figures and currency.
- For dates, include exact dates.

Answer:"""

BATCH_EXTRACT_PROMPT = """You are a document analyst. Extract the following specific items from this document.

ITEMS TO EXTRACT:
{items}

DOCUMENT TEXT:
{text}

Instructions:
- For each item, find the specific value/answer from the document.
- If an item is not found, write "Not Found".
- Be precise: use exact figures, dates, names as stated in the document.
- Return ONLY valid JSON in this format:
{{
  "document_name": "{doc_name}",
  "extractions": {{
    "item_name": "extracted value"
  }}
}}

Answer:"""


# ── Keyword retrieval (replaces embedding search from AAA 2) ──────────────────

def _keyword_score(chunk_text: str, query: str) -> float:
    """
    Simple keyword relevance score for a chunk vs a query.
    Same purpose as cosine-similarity in AAA 2 — surfaces the most relevant chunks.
    """
    query_words = set(re.findall(r'\b\w{3,}\b', query.lower()))
    chunk_lower = chunk_text.lower()
    hits = sum(1 for w in query_words if w in chunk_lower)
    return hits / max(len(query_words), 1)


def _retrieve_chunks(chunks: list[dict], query: str, top_k: int = 12) -> list[dict]:
    """Return the top_k most relevant chunks for a query (keyword-scored)."""
    scored = [(c, _keyword_score(c["text"], query)) for c in chunks]
    scored.sort(key=lambda x: x[1], reverse=True)
    return [c for c, _ in scored[:top_k]]


# ── LLM call ─────────────────────────────────────────────────────────────────

def _call_llm(prompt: str, max_tokens: int = 4096, json_mode: bool = False) -> str:
    global _client
    if _client is None:
        api_key = os.getenv("AZURE_OPENAI_KEY")
        endpoint = os.getenv("AZURE_OPENAI_ENDPOINT")
        if not api_key or not endpoint:
            raise RuntimeError(
                "AZURE_OPENAI_KEY and AZURE_OPENAI_ENDPOINT are not configured. "
                "Add them to your .env file to enable document analysis."
            )
        _client = AzureOpenAI(
            api_key=api_key,
            azure_endpoint=endpoint,
            api_version=os.getenv("AZURE_OPENAI_API_VERSION", "2024-12-01-preview"),
        )

    kwargs = dict(
        model=MODEL,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=max_tokens,
        temperature=0,
    )
    if json_mode:
        kwargs["response_format"] = {"type": "json_object"}
    resp = _client.chat.completions.create(**kwargs)
    return resp.choices[0].message.content.strip()


# ── DocumentAnalyser class ────────────────────────────────────────────────────

class DocumentAnalyser:
    """
    Stateful document analyser.
    Mirrors GraphRAGEngine's public interface from AAA 2/graph_rag.py.
    """

    def __init__(self):
        self.full_text_by_doc: dict[str, str] = {}   # doc_name → full text
        self.chunks_by_doc: dict[str, list[dict]] = {}  # doc_name → chunks

    # ── Ingest ────────────────────────────────────────────────────────────────

    def add_document(self, doc_name: str, pdf_path: str) -> tuple[str, list[dict]]:
        """
        Parse a readable (text-layer) PDF and store its text + chunks.
        Uses pdfplumber extraction — call this only for PDFs with a text layer.
        Returns (full_text, chunks).
        """
        full_text, chunks = process_pdf(pdf_path, chunk_size=3000)
        self.full_text_by_doc[doc_name] = full_text
        self.chunks_by_doc[doc_name] = chunks
        logger.info(f"[DocumentAnalyser] '{doc_name}' — {len(full_text)} chars, {len(chunks)} chunks")
        return full_text, chunks

    def add_document_from_text(self, doc_name: str, full_text: str) -> tuple[str, list[dict]]:
        """
        Ingest pre-extracted text (e.g. from Azure Document Intelligence) and
        store it alongside its chunks.  Use this for scanned PDFs, images,
        DOCX, XLSX, PPTX, and any other format processed by document_intelligence.py.
        Returns (full_text, chunks).
        """
        from pdf_parser import chunk_text, clean_text
        cleaned = clean_text(full_text)
        chunks = chunk_text(cleaned, chunk_size=3000)
        self.full_text_by_doc[doc_name] = cleaned
        self.chunks_by_doc[doc_name] = chunks
        logger.info(
            f"[DocumentAnalyser] '{doc_name}' (DI) — {len(cleaned)} chars, {len(chunks)} chunks"
        )
        return cleaned, chunks

    def document_loaded(self, doc_name: str) -> bool:
        return doc_name in self.full_text_by_doc

    # ── Query ─────────────────────────────────────────────────────────────────

    def query(self, question: str) -> str:
        """
        Answer a question across all loaded documents.
        Mirrors GraphRAGEngine.query() from AAA 2.
        """
        if not self.full_text_by_doc:
            return "No documents have been loaded yet."

        context_parts = []
        for doc_name, chunks in self.chunks_by_doc.items():
            relevant = _retrieve_chunks(chunks, question, top_k=12)
            section_texts = "\n\n".join(c["text"] for c in relevant)
            context_parts.append(f"=== {doc_name} ===\n{section_texts}")

        combined = "\n\n".join(context_parts)

        # Truncate to ~120k chars to stay within context limits
        MAX_CHARS = 120_000
        if len(combined) > MAX_CHARS:
            combined = combined[:MAX_CHARS] + "\n\n[content truncated]"

        prompt = QUERY_PROMPT.format(text_context=combined, question=question)
        return _call_llm(prompt, max_tokens=4096)

    # ── Batch extraction ──────────────────────────────────────────────────────

    def batch_extract(self, items: list[str]) -> list[dict]:
        """
        Extract specific items from every loaded document.
        Mirrors GraphRAGEngine.batch_extract() from AAA 2.
        Returns list of {document_name, extractions: {item: value}}.
        """
        items_str = "\n".join(f"- {item}" for item in items)
        results = []

        for doc_name, full_text in self.full_text_by_doc.items():
            # Use full text for batch (same as AAA 2's approach)
            text_sample = full_text[:60_000]  # stay within context
            prompt = BATCH_EXTRACT_PROMPT.format(
                items=items_str,
                text=text_sample,
                doc_name=doc_name,
            )
            try:
                raw = _call_llm(prompt, max_tokens=2048, json_mode=True)
                result = json.loads(raw)
                result.setdefault("document_name", doc_name)
                results.append(result)
            except Exception as e:
                logger.warning(f"batch_extract failed for '{doc_name}': {e}")
                results.append({
                    "document_name": doc_name,
                    "extractions": {item: "Extraction failed" for item in items},
                })

        return results

    # ── Stats ─────────────────────────────────────────────────────────────────

    def get_stats(self) -> dict:
        """Mirrors GraphRAGEngine.get_graph_stats()."""
        total_chunks = sum(len(c) for c in self.chunks_by_doc.values())
        return {
            "documents_loaded": list(self.full_text_by_doc.keys()),
            "total_chunks": total_chunks,
            "total_chars": sum(len(t) for t in self.full_text_by_doc.values()),
        }

    def reset(self):
        self.full_text_by_doc.clear()
        self.chunks_by_doc.clear()
