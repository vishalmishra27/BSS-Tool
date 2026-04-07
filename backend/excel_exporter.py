"""
Excel Exporter: Converts query results and batch extractions to downloadable Excel files.
"""

import pandas as pd
import re
import io
import os


def _strip_markdown(text: str) -> str:
    """Remove markdown formatting from text for clean Excel output."""
    if not isinstance(text, str):
        return text
    text = re.sub(r'\*\*\*(.+?)\*\*\*', r'\1', text)  # ***bold italic***
    text = re.sub(r'\*\*(.+?)\*\*', r'\1', text)       # **bold**
    text = re.sub(r'\*(.+?)\*', r'\1', text)            # *italic*
    text = re.sub(r'__(.+?)__', r'\1', text)            # __bold__
    text = re.sub(r'_(.+?)_', r'\1', text)              # _italic_
    text = re.sub(r'~~(.+?)~~', r'\1', text)            # ~~strikethrough~~
    text = re.sub(r'`(.+?)`', r'\1', text)              # `code`
    return text.strip()


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Strip markdown from all cells and headers in a DataFrame."""
    df.columns = [_strip_markdown(col) for col in df.columns]
    return df.map(_strip_markdown)


def markdown_table_to_dataframe(markdown_text: str) -> pd.DataFrame | None:
    """Parse a markdown table from LLM response into a DataFrame."""
    lines = markdown_text.strip().split('\n')

    table_lines = [l for l in lines if '|' in l]
    if len(table_lines) < 2:
        return None

    header_line = table_lines[0]
    headers = [h.strip() for h in header_line.split('|') if h.strip()]

    data_lines = []
    for line in table_lines[1:]:
        if re.match(r'^[\s|:-]+$', line):
            continue
        cells = [c.strip() for c in line.split('|') if c.strip()]
        if cells:
            data_lines.append(cells)

    if not data_lines:
        return None

    max_cols = len(headers)
    aligned_data = []
    for row in data_lines:
        if len(row) < max_cols:
            row.extend([''] * (max_cols - len(row)))
        elif len(row) > max_cols:
            row = row[:max_cols]
        aligned_data.append(row)

    return pd.DataFrame(aligned_data, columns=headers)


def batch_results_to_dataframe(results: list[dict], items: list[str]) -> pd.DataFrame:
    """Convert batch extraction results to a single DataFrame."""
    rows = []
    for result in results:
        doc_name = result.get("document_name", result.get("contract_name", "Unknown"))
        extractions = result.get("extractions", {})
        row = {"Document": doc_name}
        for item in items:
            # Try exact match first, then fuzzy
            value = extractions.get(item, "")
            if not value:
                # Try case-insensitive match
                for k, v in extractions.items():
                    if k.lower().strip() == item.lower().strip():
                        value = v
                        break
            if not value:
                # Try partial match
                for k, v in extractions.items():
                    if item.lower() in k.lower() or k.lower() in item.lower():
                        value = v
                        break
            row[item] = value if value else "Not Found"
        rows.append(row)

    return pd.DataFrame(rows)


def dataframe_to_excel_bytes(df: pd.DataFrame, sheet_name: str = "Results") -> bytes:
    """Convert DataFrame to Excel file bytes."""
    df = _clean_dataframe(df.copy())
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
        worksheet = writer.sheets[sheet_name]
        for column_cells in worksheet.columns:
            max_length = 0
            column_letter = column_cells[0].column_letter
            for cell in column_cells:
                try:
                    if cell.value:
                        max_length = max(max_length, len(str(cell.value)))
                except (TypeError, AttributeError):
                    pass
            adjusted_width = min(max_length + 4, 60)
            worksheet.column_dimensions[column_letter].width = adjusted_width

    return output.getvalue()


def response_to_dataframe(response_text: str) -> pd.DataFrame | None:
    """Try to extract a table from the LLM response text."""
    df = markdown_table_to_dataframe(response_text)
    if df is not None and not df.empty:
        return df

    lines = response_text.strip().split('\n')
    kv_pairs = {}
    for line in lines:
        if ':' in line and not line.startswith('|'):
            parts = line.split(':', 1)
            key = parts[0].strip().strip('-* ')
            value = parts[1].strip()
            if key and value:
                kv_pairs[key] = value

    if kv_pairs:
        return pd.DataFrame([kv_pairs])

    return None


def save_excel(df: pd.DataFrame, path: str, sheet_name: str = "Results"):
    """Save DataFrame to an Excel file on disk."""
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    with pd.ExcelWriter(path, engine='openpyxl') as writer:
        df.to_excel(writer, index=False, sheet_name=sheet_name)
