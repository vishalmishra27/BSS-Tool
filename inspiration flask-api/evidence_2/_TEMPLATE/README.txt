EVIDENCE FOLDER TEMPLATE
========================

How to set up evidence for a control:

1. Create a folder named after the Control ID (e.g., C-P2P-021/)

2. Add an evidence.txt file with your walkthrough narrative.
   This is just PLAIN TEXT — no special markers or formatting needed.

   Optionally, add metadata at the top separated by a blank line:

       Sample ID: S1
       Source Document: PR-20240315 / PO-88431
       Test Date: 2024-06-10
       Tested By: Auditor - Priya Nair

       Purchase Requisition PR-20240315 for $12,500...

   If you skip the metadata, defaults are used (Sample ID: S1, etc.).
   You can also skip metadata entirely and just write the walkthrough:

       Purchase Requisition PR-20240315 for $12,500...

3. Drop any supporting documents into the same folder:
   - Word docs (.docx) — SOPs, policies, procedures
   - Excel files (.xlsx) — config exports, data dumps, checklists
   - PDFs (.pdf) — signed forms, reports, screenshots
   - PowerPoint (.pptx) — training materials, process decks
   - Images (.png, .jpg) — screenshots, scanned documents (OCR)
   - Text files (.txt, .csv) — configs, logs, exports

   The system auto-extracts text from all formats.

4. That's it. Run the script and the system handles the rest.

EXAMPLE FOLDER:
   C-P2P-021/
     evidence.txt                          <- your walkthrough
     Procurement_Policy_v3.pdf             <- supporting
     SAP_Config_Export.xlsx                 <- supporting
     Approval_Screenshot.png               <- supporting (OCR'd)
