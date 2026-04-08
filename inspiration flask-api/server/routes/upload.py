"""
File upload endpoint — accept Excel/CSV/PDF files for agent processing.
Also supports folder uploads with nested structure preservation.

Files are uploaded exclusively to Azure Blob Storage. Local temp cache is
only used transiently when engines need real filesystem paths.
"""

from __future__ import annotations

import os
import uuid
import logging
from datetime import datetime
from typing import List

from fastapi import APIRouter, File, UploadFile, HTTPException, Query, Form
from fastapi.responses import FileResponse, StreamingResponse
import io

logger = logging.getLogger("server.routes.upload")

router = APIRouter(prefix="/api", tags=["upload"])

ALLOWED_EXTENSIONS = {".xlsx", ".xls", ".csv", ".pdf", ".txt", ".json", ".doc", ".docx"}
MAX_FILE_SIZE = 50 * 1024 * 1024  # 50 MB


@router.post("/upload")
async def upload_file(
    file: UploadFile = File(...),
    projectId: str = Form(None),
    uploadedBy: str = Form(None),
):
    """Upload a file for agent processing.

    Uploads exclusively to Azure Blob Storage. If projectId is provided,
    also creates a document metadata record in Cosmos.
    """
    _, ext = os.path.splitext(file.filename or "")
    ext = ext.lower()
    if ext not in ALLOWED_EXTENSIONS:
        raise HTTPException(
            status_code=400,
            detail=f"File type '{ext}' not allowed. Accepted: {', '.join(ALLOWED_EXTENSIONS)}",
        )

    content = await file.read()
    if len(content) > MAX_FILE_SIZE:
        raise HTTPException(status_code=400, detail="File too large (max 50 MB)")

    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_id = uuid.uuid4().hex[:8]
    safe_name = f"{ts}_{session_id}{ext}"

    # Upload to Azure Blob Storage (exclusively)
    from ..blob_store import get_blob_store
    blob_store = get_blob_store()

    if not blob_store.available:
        raise HTTPException(
            status_code=503,
            detail="Azure Blob Storage is not available. Cannot upload files.",
        )

    blob_path = f"uploads/{session_id}/{file.filename or safe_name}"
    result = blob_store.upload_bytes(content, blob_path)
    if not result:
        raise HTTPException(
            status_code=500,
            detail="Failed to upload file to Azure Blob Storage.",
        )

    blob_url = blob_store.get_blob_url(blob_path)
    logger.info("Uploaded to blob: %s -> %s (%d bytes)", file.filename, blob_path, len(content))

    # If projectId is provided, create document metadata in Cosmos
    document_id = None
    if projectId:
        try:
            from ..cosmos_store import get_cosmos_store
            cosmos = get_cosmos_store()
            if cosmos.available:
                doc_id = uuid.uuid4().hex
                cosmos.create_document({
                    "id": doc_id,
                    "projectId": projectId,
                    "filename": file.filename,
                    "blobPath": blob_path,
                    "blobUrl": blob_url,
                    "size": len(content),
                    "type": ext,
                    "uploadedBy": uploadedBy,
                    "uploadedAt": datetime.utcnow().isoformat(),
                })
                document_id = doc_id
                logger.info("Created Cosmos document metadata: %s for project %s", doc_id, projectId)
        except Exception as exc:
            logger.warning("Failed to create Cosmos document metadata (non-fatal): %s", exc)

    return {
        "filename": file.filename,
        "path": blob_path,
        "blob_path": blob_path,
        "blob_url": blob_url,
        "size": len(content),
        "type": ext,
        "document_id": document_id,
        "projectId": projectId,
    }


@router.post("/upload-folder")
async def upload_folder(
    files: List[UploadFile] = File(...),
    relativePaths: List[str] = Form(...),
    projectId: str = Form(None),
    uploadedBy: str = Form(None),
):
    """
    Upload an entire evidence folder with nested structure preserved.

    Uploads exclusively to Azure Blob Storage.
    """
    if not files:
        raise HTTPException(status_code=400, detail="No files uploaded")

    if len(relativePaths) != len(files):
        raise HTTPException(
            status_code=400,
            detail=f"Mismatch: {len(files)} files but {len(relativePaths)} relative paths",
        )

    from ..blob_store import get_blob_store
    blob_store = get_blob_store()

    if not blob_store.available:
        raise HTTPException(
            status_code=503,
            detail="Azure Blob Storage is not available. Cannot upload files.",
        )

    # Create a unique blob prefix for this upload session
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    session_id = uuid.uuid4().hex[:8]
    blob_prefix = f"evidence/{session_id}/{ts}"

    # Determine the common root folder name from relative paths
    roots = set()
    for rp in relativePaths:
        parts = rp.replace("\\", "/").split("/")
        if len(parts) > 1:
            roots.add(parts[0])

    total_size = 0
    file_count = 0
    blob_upload_failures = 0

    for i, (file, rel_path) in enumerate(zip(files, relativePaths)):
        content = await file.read()
        total_size += len(content)

        # Sanitize relative path to prevent path traversal
        safe_rel = rel_path.replace("\\", "/")
        parts = [p for p in safe_rel.split("/") if p and p != ".."]
        if not parts:
            parts = [file.filename or f"file_{i}"]

        # Upload to Azure Blob Storage
        blob_file_path = f"{blob_prefix}/{'/'.join(parts)}"
        result = blob_store.upload_bytes(content, blob_file_path)
        if not result:
            blob_upload_failures += 1

        file_count += 1

    # Determine the blob folder path
    if len(roots) == 1:
        blob_folder_path = f"{blob_prefix}/{list(roots)[0]}"
    else:
        blob_folder_path = blob_prefix

    logger.info(
        "Folder uploaded: %d files, %d bytes total -> blob=%s",
        file_count, total_size, blob_folder_path,
    )

    # List the subfolder structure from blob storage
    subfolders = []
    folder_blobs = blob_store.list_blobs(blob_folder_path + "/", delimiter="/")
    for item in folder_blobs:
        if item.get("is_prefix"):
            prefix_name = item["name"].rstrip("/").split("/")[-1]
            # Count files under this subfolder
            sub_blobs = blob_store.list_blobs(item["name"])
            sub_file_count = sum(1 for b in sub_blobs if not b.get("is_prefix"))
            subfolders.append({"name": prefix_name, "files": sub_file_count})

    logger.info("Folder structure: %s", subfolders)

    if blob_upload_failures:
        logger.warning(
            "Folder upload: %d/%d files failed to upload to blob storage",
            blob_upload_failures, file_count,
        )

    # If projectId is provided, create document metadata in Cosmos
    document_id = None
    if projectId:
        try:
            from ..cosmos_store import get_cosmos_store
            cosmos = get_cosmos_store()
            if cosmos.available:
                doc_id = uuid.uuid4().hex
                cosmos.create_document({
                    "id": doc_id,
                    "projectId": projectId,
                    "blobPath": blob_folder_path,
                    "fileCount": file_count,
                    "totalSize": total_size,
                    "uploadedBy": uploadedBy,
                    "uploadedAt": datetime.utcnow().isoformat(),
                })
                document_id = doc_id
                logger.info("Created Cosmos document metadata: %s for project %s", doc_id, projectId)
        except Exception as exc:
            logger.warning("Failed to create Cosmos document metadata (non-fatal): %s", exc)

    return {
        "folder_path": blob_folder_path,
        "blob_path": blob_folder_path,
        "file_count": file_count,
        "total_size": total_size,
        "subfolders": subfolders,
        "blob_upload_failures": blob_upload_failures if blob_upload_failures else None,
        "document_id": document_id,
        "projectId": projectId,
    }


@router.get("/download")
async def download_file(path: str = Query(..., description="Blob path or filename to download")):
    """
    Download a generated artifact file from Azure Blob Storage,
    with local filesystem fallback when blob is unavailable.
    """
    import os
    from ..blob_store import get_blob_store, BlobStore

    store = get_blob_store()

    # ── Try blob storage first (if available) ──
    if store.available:
        # If it looks like a blob path, download directly
        if BlobStore.is_blob_path(path):
            data = store.download_bytes(path)
            if data:
                filename = path.split("/")[-1]
                return StreamingResponse(
                    io.BytesIO(data),
                    media_type="application/octet-stream",
                    headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                )
            # Blob path not found — fall through to local file check

        # For non-blob paths, try to find in blob by filename search
        filename = path.split("/")[-1] if "/" in path else path
        for prefix in ["artifacts/", "uploads/", "evidence/"]:
            blobs = store.list_blobs(prefix)
            for blob in blobs:
                if blob["name"].endswith(f"/{filename}"):
                    data = store.download_bytes(blob["name"])
                    if data:
                        return StreamingResponse(
                            io.BytesIO(data),
                            media_type="application/octet-stream",
                            headers={"Content-Disposition": f'attachment; filename="{filename}"'},
                        )

    # ── Fallback: serve from local filesystem ──
    # When blob storage is unavailable (COSMOS=OFF, no Azure credentials),
    # tools store artifacts locally and return an absolute file path.
    local_path = path
    if os.path.isfile(local_path):
        filename = os.path.basename(local_path)
        return FileResponse(
            local_path,
            media_type="application/octet-stream",
            filename=filename,
        )

    raise HTTPException(status_code=404, detail=f"File not found: {path}")
