import email
import quopri
import re
import os
import sys


def parse_eml(eml_path):
    """Parse .eml file and extract files from email thread.

    Each email in the thread has a filename as the first line followed by code.
    The oldest email (app.py) has no filename header - just code directly.
    Emails are quoted with '>' prefixes, deeper = older.
    """
    with open(eml_path, 'rb') as f:
        msg = email.message_from_bytes(f.read())

    # Get the text/plain part
    plain_text = None
    for part in msg.walk():
        if part.get_content_type() == 'text/plain':
            payload = part.get_payload(decode=True)
            plain_text = payload.decode('utf-8', errors='replace')
            break

    if not plain_text:
        print("No text/plain part found in the email.")
        sys.exit(1)

    lines = plain_text.split('\n')

    # Parse the email thread into separate messages
    # Each message boundary is marked by "On ... wrote:" lines
    # We need to group content by quote depth

    # Strategy: walk through lines, track quote depth changes
    # When we hit "On ... wrote:" we know the next quoted block is a separate message

    messages = []  # list of (quote_depth, lines)

    # Split by "wrote:" boundaries
    # First, collect all segments between "wrote:" markers
    segments = []
    current_segment = []

    for line in lines:
        # Check if this line contains "wrote:" (email thread boundary)
        stripped = line.lstrip('> ')
        if re.search(r'wrote:\s*$', stripped):
            if current_segment:
                segments.append(current_segment)
            current_segment = []
        else:
            current_segment.append(line)

    if current_segment:
        segments.append(current_segment)

    # Each segment contains the content of one email reply
    # The first segment is the newest message (no '>' prefix)
    # Process each segment to extract filename and code

    files = {}

    for seg_idx, segment in enumerate(segments):
        # Remove quote prefixes and find the actual content
        content_lines = []
        for line in segment:
            # Strip leading '>' characters and one space after them
            cleaned = re.sub(r'^(>\s*)+', '', line)
            content_lines.append(cleaned)

        # Remove leading/trailing empty lines
        while content_lines and not content_lines[0].strip():
            content_lines.pop(0)
        while content_lines and not content_lines[-1].strip():
            content_lines.pop()

        if not content_lines:
            continue

        # Check if first non-empty line looks like a filename (single word, no spaces or code-like syntax)
        first_line = content_lines[0].strip()

        # Heuristic: filename headers are short, single-word identifiers (like "ContractModal", "ReconciliationDashboardPage")
        # Code lines typically have imports, brackets, operators, etc.
        is_filename_header = (
            len(first_line.split()) == 1 and
            not first_line.startswith('import ') and
            not first_line.startswith('from ') and
            not first_line.startswith('//') and
            not first_line.startswith('/*') and
            not first_line.startswith('<') and
            not first_line.startswith('--') and
            not first_line.startswith('def ') and
            not first_line.startswith('class ') and
            not any(c in first_line for c in ['(', '{', '=', ';', ':']) and
            len(first_line) > 0
        )

        if is_filename_header:
            filename = first_line
            code_lines = content_lines[1:]
            # Remove leading empty lines from code
            while code_lines and not code_lines[0].strip():
                code_lines.pop(0)
        else:
            # This is the app.py case - no filename header, it should be the oldest (last segment)
            filename = 'app.py'
            code_lines = content_lines

        code = '\n'.join(code_lines)

        if code.strip():
            # Determine file extension based on content
            if filename == 'app.py':
                ext = ''  # already has extension
            elif any(kw in code for kw in ['import React', 'from \'react\'', 'from "react"', 'const ', 'export default']):
                ext = '.jsx'
            elif code.strip().startswith('--') or 'SELECT' in code.upper() or 'INSERT INTO' in code.upper() or 'CREATE TABLE' in code.upper():
                ext = '.sql'
            elif code.strip().startswith('<!DOCTYPE') or code.strip().startswith('<html'):
                ext = '.html'
            else:
                ext = '.jsx'

            if '.' not in filename:
                filename = filename + ext

            # Don't overwrite - keep first occurrence (newest version)
            if filename not in files:
                files[filename] = code

    return files


def main():
    eml_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'app.py.eml')

    if not os.path.exists(eml_path):
        print(f"File not found: {eml_path}")
        sys.exit(1)

    print(f"Parsing: {eml_path}")
    files = parse_eml(eml_path)

    # Create output directory
    output_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'extracted_files')
    os.makedirs(output_dir, exist_ok=True)

    print(f"\nExtracted {len(files)} files:")
    for filename, code in files.items():
        filepath = os.path.join(output_dir, filename)
        with open(filepath, 'w') as f:
            f.write(code)
        line_count = len(code.split('\n'))
        print(f"  {filename} ({line_count} lines)")

    print(f"\nFiles saved to: {output_dir}")


if __name__ == '__main__':
    main()
