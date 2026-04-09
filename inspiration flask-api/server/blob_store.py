"""
Azure Blob Storage service for the Control Testing Agent.

Centralised helper that all routes and tools use to upload/download/list
files in Azure Blob Storage instead of local disk.

Blob path convention:
    uploads/{session_id}/{filename}        — user-uploaded files
    evidence/{session_id}/{rel_path}       — evidence folder trees
    artifacts/{session_id}/{filename}      — engine-generated output (Excel, PDF)

This is the ONLY persistent file storage — there is no local filesystem fallback.
A local temp-cache directory mirrors downloaded blobs so pandas / engines
that require real file paths keep working, but the temp cache is transient
and not a storage destination.
"""

from __future__ import annotations

import logging
import os
import tempfile
import time
import threading
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("server.blob_store")

AZURE_STORAGE_CONNECTION_STRING = os.environ.get("AZURE_STORAGE_CONNECTION_STRING", "")
AZURE_STORAGE_CONTAINER_NAME = os.environ.get("AZURE_STORAGE_CONTAINER_NAME", "fileupload-aashu")

# Local temp cache root — blobs are downloaded here for tools that need file paths
_TEMP_CACHE_ROOT = os.path.join(tempfile.gettempdir(), "sox_blob_cache")
os.makedirs(_TEMP_CACHE_ROOT, exist_ok=True)

# TTL for cached blobs (seconds). Files older than this are eligible for cleanup.
# Matches Context-cache TTL so cached blobs expire alongside their Cosmos references.
BLOB_CACHE_TTL = int(os.environ.get("BLOB_CACHE_TTL", "3600"))  # default 1 hour

# How often the cleanup daemon runs (seconds)
_CACHE_CLEANUP_INTERVAL = int(os.environ.get("BLOB_CACHE_CLEANUP_INTERVAL", "600"))  # default 10 min


class BlobStore:
    """
    Thread-safe Azure Blob Storage wrapper.

    This is the sole persistent file storage for the Control Testing Agent.
    Blob Storage must be available for file uploads and artifact storage to function.
    """

    def __init__(self) -> None:
        if not AZURE_STORAGE_CONNECTION_STRING:
            logger.warning(
                "BlobStore: AZURE_STORAGE_CONNECTION_STRING not set — "
                "Blob Storage persistence disabled."
            )
            self._available = False
            return

        try:
            from azure.storage.blob import BlobServiceClient

            self._service_client = BlobServiceClient.from_connection_string(
                AZURE_STORAGE_CONNECTION_STRING
            )
            self._container_client = self._service_client.get_container_client(
                AZURE_STORAGE_CONTAINER_NAME
            )
            # Verify container exists (don't try to create — it likely exists)
            try:
                self._container_client.get_container_properties()
            except Exception:
                # Container might not exist — try to create it
                try:
                    self._container_client.create_container()
                    logger.info("BlobStore: created container '%s'", AZURE_STORAGE_CONTAINER_NAME)
                except Exception:
                    # Container probably exists but we can't read properties
                    # (possible permission issue). Continue anyway — actual
                    # blob operations will fail gracefully if needed.
                    logger.debug("BlobStore: container check/create skipped — will attempt operations directly")

            self._available = True
            logger.info(
                "BlobStore initialised: container=%s",
                AZURE_STORAGE_CONTAINER_NAME,
            )
        except Exception as exc:
            logger.error("BlobStore init failed: %s", exc)
            self._available = False

    @property
    def available(self) -> bool:
        return self._available

    # ── Upload ────────────────────────────────────────────────────────────────

    def upload_file(
        self,
        local_path: str,
        blob_path: str,
        content_type: Optional[str] = None,
    ) -> Optional[str]:
        """
        Upload a local file to blob storage.

        Returns the blob_path on success, None on failure.
        """
        if not self._available:
            return None
        try:
            from azure.storage.blob import ContentSettings

            blob_client = self._container_client.get_blob_client(blob_path)
            with open(local_path, "rb") as f:
                kwargs = {"overwrite": True}
                if content_type:
                    kwargs["content_settings"] = ContentSettings(content_type=content_type)
                blob_client.upload_blob(f, **kwargs)
            logger.info("BlobStore.upload_file: %s → %s", local_path, blob_path)
            return blob_path
        except Exception as exc:
            logger.warning("BlobStore.upload_file failed: %s → %s error=%s", local_path, blob_path, exc)
            return None

    def upload_bytes(
        self,
        data: bytes,
        blob_path: str,
        content_type: Optional[str] = None,
    ) -> Optional[str]:
        """Upload raw bytes to blob storage. Returns blob_path on success."""
        if not self._available:
            return None
        try:
            from azure.storage.blob import ContentSettings

            blob_client = self._container_client.get_blob_client(blob_path)
            kwargs = {"overwrite": True}
            if content_type:
                kwargs["content_settings"] = ContentSettings(content_type=content_type)
            blob_client.upload_blob(data, **kwargs)
            logger.info("BlobStore.upload_bytes: %s (%d bytes)", blob_path, len(data))
            return blob_path
        except Exception as exc:
            logger.warning("BlobStore.upload_bytes failed: %s error=%s", blob_path, exc)
            return None

    def upload_directory(
        self,
        local_dir: str,
        blob_prefix: str,
    ) -> List[str]:
        """
        Recursively upload an entire local directory to blob storage.

        Returns list of blob paths uploaded.
        """
        if not self._available:
            return []
        uploaded = []
        try:
            local_root = Path(local_dir)
            for file_path in local_root.rglob("*"):
                if file_path.is_file():
                    rel = file_path.relative_to(local_root)
                    bp = f"{blob_prefix}/{rel}".replace("\\", "/")
                    result = self.upload_file(str(file_path), bp)
                    if result:
                        uploaded.append(result)
        except Exception as exc:
            logger.warning("BlobStore.upload_directory failed: %s error=%s", local_dir, exc)
        return uploaded

    # ── Download ──────────────────────────────────────────────────────────────

    def download_to_file(self, blob_path: str, local_path: str) -> Optional[str]:
        """
        Download a blob to a specific local path.

        Returns the local_path on success, None on failure.
        """
        if not self._available:
            return None
        try:
            blob_client = self._container_client.get_blob_client(blob_path)
            os.makedirs(os.path.dirname(local_path), exist_ok=True)
            with open(local_path, "wb") as f:
                stream = blob_client.download_blob()
                stream.readinto(f)
            logger.debug("BlobStore.download_to_file: %s → %s", blob_path, local_path)
            return local_path
        except Exception as exc:
            logger.warning("BlobStore.download_to_file failed: %s error=%s", blob_path, exc)
            return None

    def download_to_temp(self, blob_path: str) -> Optional[str]:
        """
        Download a blob to the local temp cache, preserving its relative path.

        Returns the local temp path on success, None on failure.
        The file is cached — repeated calls return the cached path.
        """
        if not self._available:
            return None
        local_path = os.path.join(_TEMP_CACHE_ROOT, blob_path)
        # Return cached copy if it exists, refreshing mtime to extend TTL
        if os.path.exists(local_path):
            try:
                os.utime(local_path)  # touch — resets mtime to now
            except OSError:
                pass
            logger.debug("BlobStore.download_to_temp: cache hit %s", local_path)
            return local_path
        return self.download_to_file(blob_path, local_path)

    def download_bytes(self, blob_path: str) -> Optional[bytes]:
        """Download a blob as bytes. Returns None on failure."""
        if not self._available:
            return None
        try:
            blob_client = self._container_client.get_blob_client(blob_path)
            stream = blob_client.download_blob()
            data = stream.readall()
            logger.debug("BlobStore.download_bytes: %s (%d bytes)", blob_path, len(data))
            return data
        except Exception as exc:
            logger.warning("BlobStore.download_bytes failed: %s error=%s", blob_path, exc)
            return None

    def download_directory(self, blob_prefix: str, local_dir: str) -> Optional[str]:
        """
        Download all blobs under a prefix to a local directory.

        Returns the local_dir on success, None on failure.
        """
        if not self._available:
            return None
        try:
            blobs = self._container_client.list_blobs(name_starts_with=blob_prefix)
            count = 0
            for blob in blobs:
                rel = blob.name[len(blob_prefix):].lstrip("/")
                if not rel:
                    continue
                local_path = os.path.join(local_dir, rel)
                self.download_to_file(blob.name, local_path)
                count += 1
            if count > 0:
                logger.info(
                    "BlobStore.download_directory: %s → %s (%d files)",
                    blob_prefix, local_dir, count,
                )
                return local_dir
            else:
                logger.debug("BlobStore.download_directory: no blobs under %s", blob_prefix)
                return None
        except Exception as exc:
            logger.warning("BlobStore.download_directory failed: %s error=%s", blob_prefix, exc)
            return None

    # ── List ──────────────────────────────────────────────────────────────────

    def list_blobs(
        self,
        prefix: str,
        delimiter: Optional[str] = None,
    ) -> List[Dict]:
        """
        List blobs under a prefix.

        If delimiter is set (e.g. '/'), returns virtual "directories" too.
        Returns list of dicts with keys: name, size, is_prefix (for virtual dirs).
        """
        if not self._available:
            return []
        try:
            items = []
            if delimiter:
                # Walk by hierarchy to get virtual folders
                for item in self._container_client.walk_blobs(
                    name_starts_with=prefix, delimiter=delimiter
                ):
                    if hasattr(item, "prefix"):
                        # Virtual directory
                        items.append({
                            "name": item.prefix,
                            "size": 0,
                            "is_prefix": True,
                        })
                    else:
                        items.append({
                            "name": item.name,
                            "size": item.size,
                            "is_prefix": False,
                        })
            else:
                for blob in self._container_client.list_blobs(name_starts_with=prefix):
                    items.append({
                        "name": blob.name,
                        "size": blob.size,
                        "is_prefix": False,
                    })
            return items
        except Exception as exc:
            logger.warning("BlobStore.list_blobs failed: prefix=%s error=%s", prefix, exc)
            return []

    def blob_exists(self, blob_path: str) -> bool:
        """Check if a blob exists."""
        if not self._available:
            return False
        try:
            blob_client = self._container_client.get_blob_client(blob_path)
            blob_client.get_blob_properties()
            return True
        except Exception:
            return False

    # ── Delete ────────────────────────────────────────────────────────────────

    def delete_blob(self, blob_path: str) -> bool:
        """Delete a single blob. Returns True on success."""
        if not self._available:
            return False
        try:
            blob_client = self._container_client.get_blob_client(blob_path)
            blob_client.delete_blob()
            logger.info("BlobStore.delete_blob: %s", blob_path)
            return True
        except Exception as exc:
            logger.warning("BlobStore.delete_blob failed: %s error=%s", blob_path, exc)
            return False

    # ── Helpers ───────────────────────────────────────────────────────────────

    def get_blob_url(self, blob_path: str) -> Optional[str]:
        """Get the full URL for a blob (without SAS token)."""
        if not self._available:
            return None
        try:
            blob_client = self._container_client.get_blob_client(blob_path)
            return blob_client.url
        except Exception:
            return None

    @staticmethod
    def make_upload_path(session_id: str, filename: str) -> str:
        """Generate a blob path for an uploaded file."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        safe_name = f"{ts}_{uuid.uuid4().hex[:8]}_{filename}"
        return f"uploads/{session_id}/{safe_name}"

    @staticmethod
    def make_evidence_path(session_id: str) -> str:
        """Generate a blob prefix for an evidence folder upload."""
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        return f"evidence/{session_id}/{ts}"

    @staticmethod
    def make_artifact_path(session_id: str, filename: str) -> str:
        """Generate a blob path for an engine-generated artifact."""
        return f"artifacts/{session_id}/{filename}"

    @staticmethod
    def is_blob_path(path: str) -> bool:
        """Check if a path looks like a blob path (not a local filesystem path).

        Blob paths are relative (e.g. 'uploads/session/file.xlsx').
        Absolute paths (starting with / or drive letter) are always local.
        """
        if not path or os.path.isabs(path):
            return False
        return (
            path.startswith("uploads/")
            or path.startswith("evidence/")
            or path.startswith("artifacts/")
            or path.startswith("blob://")
        )

    def ensure_local(self, path: str) -> Optional[str]:
        """
        If path is a blob path, download to temp cache and return local path.
        If path is already a local path, return it as-is.
        """
        if self.is_blob_path(path):
            clean = path.removeprefix("blob://")
            return self.download_to_temp(clean)
        logger.warning("ensure_local called with non-blob path: %s — expected blob path", path)
        return path

    def ensure_local_directory(self, path: str) -> Optional[str]:
        """
        If path is a blob prefix, download all blobs under it to temp cache
        and return the local directory path.
        If path is already a local directory, return it as-is.
        """
        if self.is_blob_path(path):
            clean = path.removeprefix("blob://")
            local_dir = os.path.join(_TEMP_CACHE_ROOT, clean)
            if os.path.isdir(local_dir) and os.listdir(local_dir):
                # Touch all files to extend their TTL
                try:
                    for root, _dirs, files in os.walk(local_dir):
                        for fname in files:
                            try:
                                os.utime(os.path.join(root, fname))
                            except OSError:
                                pass
                except OSError:
                    pass
                logger.debug("BlobStore.ensure_local_directory: cache hit %s", local_dir)
                return local_dir
            return self.download_directory(clean, local_dir)
        logger.warning("ensure_local_directory called with non-blob path: %s — expected blob path", path)
        return path

    # ── Temp cache cleanup (TTL-based) ────────────────────────────────────────

    @staticmethod
    def cleanup_temp_cache(ttl_seconds: Optional[int] = None) -> Dict[str, int]:
        """
        Remove cached blob files older than *ttl_seconds* from /tmp/sox_blob_cache.

        Uses file modification time (mtime) to determine age. Empty directories
        left behind after file removal are also pruned.

        Returns: {files_removed, dirs_removed, errors, bytes_freed}.
        """
        ttl = ttl_seconds if ttl_seconds is not None else BLOB_CACHE_TTL
        cutoff = time.time() - ttl
        stats = {"files_removed": 0, "dirs_removed": 0, "errors": 0, "bytes_freed": 0}

        if not os.path.isdir(_TEMP_CACHE_ROOT):
            return stats

        # Pass 1: remove expired files
        for dirpath, _dirnames, filenames in os.walk(_TEMP_CACHE_ROOT, topdown=False):
            for fname in filenames:
                fpath = os.path.join(dirpath, fname)
                try:
                    st = os.stat(fpath)
                    if st.st_mtime < cutoff:
                        size = st.st_size
                        os.remove(fpath)
                        stats["files_removed"] += 1
                        stats["bytes_freed"] += size
                except Exception as exc:
                    logger.debug("cleanup_temp_cache: could not remove %s: %s", fpath, exc)
                    stats["errors"] += 1

        # Pass 2: remove empty directories (bottom-up so children go first)
        for dirpath, dirnames, filenames in os.walk(_TEMP_CACHE_ROOT, topdown=False):
            if dirpath == _TEMP_CACHE_ROOT:
                continue  # never remove the root itself
            try:
                if not os.listdir(dirpath):
                    os.rmdir(dirpath)
                    stats["dirs_removed"] += 1
            except Exception:
                pass

        if stats["files_removed"] > 0:
            logger.info(
                "cleanup_temp_cache: removed %d file(s) (%d bytes), "
                "%d empty dir(s), %d error(s) — ttl=%ds",
                stats["files_removed"], stats["bytes_freed"],
                stats["dirs_removed"], stats["errors"], ttl,
            )
        else:
            logger.debug("cleanup_temp_cache: nothing to clean (ttl=%ds)", ttl)

        return stats

    @staticmethod
    def start_cache_cleanup_daemon(
        ttl_seconds: Optional[int] = None,
        interval_seconds: Optional[int] = None,
    ) -> threading.Thread:
        """
        Start a background daemon thread that periodically cleans expired
        cached blobs from /tmp/sox_blob_cache.

        Returns the Thread object. Thread is daemon=True so it dies with the process.
        """
        ttl = ttl_seconds if ttl_seconds is not None else BLOB_CACHE_TTL
        interval = interval_seconds if interval_seconds is not None else _CACHE_CLEANUP_INTERVAL

        def _run():
            while True:
                try:
                    BlobStore.cleanup_temp_cache(ttl)
                except Exception as exc:
                    logger.warning("cache cleanup daemon error: %s", exc)
                time.sleep(interval)

        thread = threading.Thread(
            target=_run,
            name="blob-cache-cleanup",
            daemon=True,
        )
        thread.start()
        logger.info(
            "Blob cache cleanup daemon started: ttl=%ds interval=%ds cache_root=%s",
            ttl, interval, _TEMP_CACHE_ROOT,
        )
        return thread


# ── Module-level singleton ────────────────────────────────────────────────────
_store: Optional[BlobStore] = None


def get_blob_store() -> BlobStore:
    """Return the module-level singleton BlobStore, initialising it on first call."""
    global _store
    if _store is None:
        logger.info("BlobStore: initialising singleton")
        _store = BlobStore()
    return _store
