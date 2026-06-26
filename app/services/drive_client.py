"""Thin async Google Drive client (REST v3, via httpx).

Used by the "export a version's files to Drive" feature. Deliberately
avoids the heavyweight ``google-api-python-client`` / ``google-auth``
stack — we already depend on httpx, the OAuth dance already lives in
``app.api.oauth``, and we only need three operations:

  * mint a short-lived access token from the admin's stored refresh token
  * validate that the pasted folder exists and we can add children to it
  * upload a (potentially large) file with a resumable upload

Resumable upload is required because scraper ZIP parts routinely exceed
the 5 MB simple-upload ceiling; it also lets us stream the file from disk
in fixed chunks (constant memory) and survive transient network blips.
"""
import asyncio
import json
import logging
import mimetypes
import os
import re

import httpx

from app.config import settings

logger = logging.getLogger(__name__)

GOOGLE_TOKEN_URL = "https://oauth2.googleapis.com/token"
DRIVE_FILES_URL = "https://www.googleapis.com/drive/v3/files"
DRIVE_UPLOAD_URL = "https://www.googleapis.com/upload/drive/v3/files"

# Full Drive scope — needed to write into an arbitrary existing folder the
# admin pastes (the narrower drive.file scope only reaches app-created or
# user-picked files).
DRIVE_SCOPE = "https://www.googleapis.com/auth/drive"

# Resumable upload chunk size. Must be a multiple of 256 KB per the Drive
# protocol. 8 MB balances request overhead against memory.
_CHUNK = 8 * 1024 * 1024
# Files at or below this go up in a single multipart request (one round-trip
# instead of resumable's two+); above it we resume from disk. Tuned for the
# thousands of small documents extracted from the ZIPs.
SMALL_FILE_LIMIT = 4 * 1024 * 1024
# Transient-error retry schedule (seconds). Covers Drive rate limits (429 /
# 403 rateLimitExceeded) and 5xx blips on a long, many-file export.
_RETRY_BACKOFF = [2, 5, 15, 45]


class DriveError(RuntimeError):
    """Raised for Drive API failures the caller should surface to the user."""


def _is_rate_limited(resp: httpx.Response) -> bool:
    """True if a 403 is actually a rate-limit (retryable), not a hard auth /
    service-disabled 403."""
    if resp.status_code != 403:
        return False
    body = resp.text.lower()
    return "ratelimitexceeded" in body or "userratelimitexceeded" in body


def extract_folder_id(url_or_id: str) -> str | None:
    """Pull a Drive folder id out of a pasted URL or accept a bare id.

    Handles ``…/folders/<id>``, ``…?id=<id>`` and a raw id token. Returns
    None if nothing folder-id-shaped is found.
    """
    s = (url_or_id or "").strip()
    if not s:
        return None
    m = re.search(r"/folders/([A-Za-z0-9_-]+)", s)
    if m:
        return m.group(1)
    m = re.search(r"[?&]id=([A-Za-z0-9_-]+)", s)
    if m:
        return m.group(1)
    if re.fullmatch(r"[A-Za-z0-9_-]{10,}", s):
        return s
    return None


async def get_access_token(refresh_token: str) -> str:
    """Exchange a stored refresh token for a fresh access token."""
    if not settings.google_client_id or not settings.google_client_secret:
        raise DriveError("Google OAuth is not configured on the server")
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.post(
            GOOGLE_TOKEN_URL,
            data={
                "client_id": settings.google_client_id,
                "client_secret": settings.google_client_secret,
                "refresh_token": refresh_token,
                "grant_type": "refresh_token",
            },
        )
    if resp.status_code != 200:
        # invalid_grant ⇒ the refresh token was revoked / expired; the admin
        # must reconnect Drive.
        raise DriveError(
            f"Could not refresh Google access token ({resp.status_code}). "
            "Reconnect Drive and try again."
        )
    return resp.json()["access_token"]


def _drive_error_detail(resp: httpx.Response) -> str:
    """Extract Google's human reason from an error response, so a 403 says
    *why* (API disabled / insufficient scope / rate limit) instead of a bare
    code."""
    try:
        err = (resp.json() or {}).get("error", {})
        if isinstance(err, dict):
            msg = err.get("message")
            if msg:
                return msg
        if isinstance(err, str):
            return err
    except Exception:
        pass
    return (resp.text or "")[:300]


async def validate_folder(access_token: str, folder_id: str) -> str:
    """Confirm the folder exists, is a folder, and we can add files to it.
    Returns the folder name. Raises DriveError otherwise."""
    async with httpx.AsyncClient(timeout=20.0) as client:
        resp = await client.get(
            f"{DRIVE_FILES_URL}/{folder_id}",
            params={
                "fields": "id,name,mimeType,capabilities(canAddChildren)",
                "supportsAllDrives": "true",
            },
            headers={"Authorization": f"Bearer {access_token}"},
        )
    if resp.status_code == 404:
        raise DriveError("Drive folder not found, or it isn't shared with this Google account")
    if resp.status_code == 403:
        # Most common cause: the Drive API isn't enabled in the Cloud project,
        # or the granted token lacks the drive scope. Surface Google's message.
        raise DriveError(f"Drive access denied (403): {_drive_error_detail(resp)}")
    if resp.status_code != 200:
        raise DriveError(f"Drive folder check failed ({resp.status_code}): {_drive_error_detail(resp)}")
    data = resp.json()
    if data.get("mimeType") != "application/vnd.google-apps.folder":
        raise DriveError("That link doesn't point to a Drive folder")
    if not (data.get("capabilities") or {}).get("canAddChildren", False):
        raise DriveError("No permission to add files to that Drive folder")
    return data.get("name") or folder_id


async def upload_bytes(
    access_token: str,
    folder_id: str,
    filename: str,
    data: bytes,
) -> str:
    """Single-request multipart upload of in-memory bytes. For the many small
    documents extracted from the ZIPs — one round-trip each. Retries transient
    Drive errors (rate limit / 5xx)."""
    mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    boundary = "over_drive_boundary_7e1c"
    meta = json.dumps({"name": filename, "parents": [folder_id]}, ensure_ascii=False)
    body = (
        f"--{boundary}\r\n"
        "Content-Type: application/json; charset=UTF-8\r\n\r\n"
        f"{meta}\r\n"
        f"--{boundary}\r\n"
        f"Content-Type: {mime}\r\n\r\n"
    ).encode("utf-8") + data + f"\r\n--{boundary}--\r\n".encode("utf-8")

    async def _attempt(client: httpx.AsyncClient) -> httpx.Response:
        return await client.post(
            DRIVE_UPLOAD_URL,
            params={"uploadType": "multipart", "supportsAllDrives": "true"},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": f"multipart/related; boundary={boundary}",
            },
            content=body,
        )

    async with httpx.AsyncClient(timeout=httpx.Timeout(120.0, connect=20.0)) as client:
        resp = await _with_retry(_attempt, client, filename)
    return resp.json().get("id", "")


async def upload_file(
    access_token: str,
    folder_id: str,
    filename: str,
    file_path: str,
) -> str:
    """Upload a local file into ``folder_id``. Small files go multipart;
    larger ones stream via a resumable upload (constant memory). Returns the
    new Drive file id."""
    size = os.path.getsize(file_path)
    if size <= SMALL_FILE_LIMIT:
        with open(file_path, "rb") as fh:
            return await upload_bytes(access_token, folder_id, filename, fh.read())

    mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"
    async with httpx.AsyncClient(timeout=httpx.Timeout(600.0, connect=20.0)) as client:
        init = await client.post(
            DRIVE_UPLOAD_URL,
            params={"uploadType": "resumable", "supportsAllDrives": "true"},
            headers={
                "Authorization": f"Bearer {access_token}",
                "Content-Type": "application/json; charset=UTF-8",
                "X-Upload-Content-Type": mime,
                "X-Upload-Content-Length": str(size),
            },
            json={"name": filename, "parents": [folder_id]},
        )
        if init.status_code not in (200, 201):
            raise DriveError(
                f"Drive upload init failed for {filename} ({init.status_code}): "
                f"{_drive_error_detail(init)}"
            )
        session_url = init.headers.get("Location")
        if not session_url:
            raise DriveError(f"Drive upload init returned no session URL for {filename}")

        offset = 0
        with open(file_path, "rb") as fh:
            while offset < size:
                chunk = fh.read(_CHUNK)
                end = offset + len(chunk) - 1
                resp = await client.put(
                    session_url,
                    headers={
                        "Content-Length": str(len(chunk)),
                        "Content-Range": f"bytes {offset}-{end}/{size}",
                    },
                    content=chunk,
                )
                if resp.status_code in (200, 201):
                    return resp.json().get("id", "")
                if resp.status_code == 308:  # Resume Incomplete — continue.
                    offset = end + 1
                    continue
                raise DriveError(
                    f"Drive chunk upload failed for {filename} "
                    f"({resp.status_code}): {_drive_error_detail(resp)}"
                )
    raise DriveError(f"Drive upload did not complete for {filename}")


async def _with_retry(attempt, client: httpx.AsyncClient, label: str) -> httpx.Response:
    """Run ``attempt(client)``; retry on transient Drive errors (429, 5xx,
    403 rate-limit) with backoff. Raises DriveError on a hard failure or after
    the schedule is exhausted."""
    last: httpx.Response | None = None
    for i in range(len(_RETRY_BACKOFF) + 1):
        resp = await attempt(client)
        if resp.status_code in (200, 201):
            return resp
        last = resp
        retryable = resp.status_code == 429 or resp.status_code >= 500 or _is_rate_limited(resp)
        if not retryable or i == len(_RETRY_BACKOFF):
            break
        await asyncio.sleep(_RETRY_BACKOFF[i])
    detail = _drive_error_detail(last) if last is not None else "no response"
    code = last.status_code if last is not None else "?"
    raise DriveError(f"Drive upload failed for {label} ({code}): {detail}")
