"""
PDF Parser: Extracts text from PDFs, handles large documents (50-100+ pages).
Splits into section-aware semantic chunks for GraphRAG processing.
"""

import pdfplumber
import re
import hashlib

# Pattern to detect section headers in documents
SECTION_HEADER_PATTERN = re.compile(
    r'^\s*'
    r'(?:'
    r'(?:ARTICLE|SECTION|CLAUSE|SCHEDULE|ANNEXURE|EXHIBIT|APPENDIX|PART)\s*[\d\.IVXLC]+'
    r'|\d+\.\d*\s+[A-Z]'
    r'|[A-Z][A-Z\s]{4,}$'
    r')',
    re.MULTILINE
)


def extract_text_from_pdf(pdf_path: str) -> str:
    """Extract all text from a PDF file."""
    full_text = ""
    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if text:
                full_text += text + "\n\n"
    return full_text.strip()


def clean_text(text: str) -> str:
    """Clean extracted text: fix whitespace, remove artifacts."""
    text = re.sub(r'\n{3,}', '\n\n', text)
    text = re.sub(r'[ \t]+', ' ', text)
    text = re.sub(r'(\w)-\n(\w)', r'\1\2', text)  # fix hyphenation
    return text.strip()


def _is_section_header(line: str) -> bool:
    """Check if a line looks like a section header."""
    line = line.strip()
    if not line or len(line) > 200:
        return False
    if SECTION_HEADER_PATTERN.match(line):
        return True
    # Numbered sections like "1.", "1.1", "1.1.1"
    if re.match(r'^\d+(\.\d+)*\.?\s+\S', line):
        return True
    # All caps short lines (likely headers)
    if line.isupper() and 3 < len(line) < 100:
        return True
    return False


def chunk_text(text: str, chunk_size: int = 3000, overlap: int = 500) -> list[dict]:
    """
    Split text into overlapping, section-aware chunks.
    Tries to keep sections together. Uses larger chunks (3000 chars)
    with bigger overlap (500 chars) so no information is lost.
    """
    paragraphs = text.split('\n\n')
    chunks = []
    current_chunk = ""
    current_section = ""

    for para in paragraphs:
        para = para.strip()
        if not para:
            continue

        # Detect section header
        first_line = para.split('\n')[0].strip()
        is_header = _is_section_header(first_line)

        # If this is a new section header and current chunk is big enough,
        # flush the current chunk to start a new one at section boundary
        if is_header and len(current_chunk) > chunk_size // 3:
            chunk_id = hashlib.md5(current_chunk[:100].encode()).hexdigest()[:8]
            chunks.append({
                "id": f"chunk_{len(chunks)}_{chunk_id}",
                "text": current_chunk.strip(),
                "section": current_section,
                "index": len(chunks),
            })
            # Overlap: keep last portion for continuity
            overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
            current_chunk = overlap_text + "\n\n" + para
            current_section = first_line
        elif len(current_chunk) + len(para) + 2 > chunk_size and current_chunk:
            # Chunk is full, flush it
            chunk_id = hashlib.md5(current_chunk[:100].encode()).hexdigest()[:8]
            chunks.append({
                "id": f"chunk_{len(chunks)}_{chunk_id}",
                "text": current_chunk.strip(),
                "section": current_section,
                "index": len(chunks),
            })
            overlap_text = current_chunk[-overlap:] if len(current_chunk) > overlap else current_chunk
            current_chunk = overlap_text + "\n\n" + para
        else:
            if is_header and not current_section:
                current_section = first_line
            current_chunk += ("\n\n" if current_chunk else "") + para

    if current_chunk.strip():
        chunk_id = hashlib.md5(current_chunk[:100].encode()).hexdigest()[:8]
        chunks.append({
            "id": f"chunk_{len(chunks)}_{chunk_id}",
            "text": current_chunk.strip(),
            "section": current_section,
            "index": len(chunks),
        })

    return chunks


def process_pdf(pdf_path: str, chunk_size: int = 3000) -> tuple[str, list[dict]]:
    """Full pipeline: extract → clean → chunk."""
    raw_text = extract_text_from_pdf(pdf_path)
    cleaned = clean_text(raw_text)
    chunks = chunk_text(cleaned, chunk_size=chunk_size)
    return cleaned, chunks
