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


class DriveError(RuntimeError):
    """Raised for Drive API failures the caller should surface to the user."""


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
    if resp.status_code != 200:
        raise DriveError(f"Drive folder check failed ({resp.status_code})")
    data = resp.json()
    if data.get("mimeType") != "application/vnd.google-apps.folder":
        raise DriveError("That link doesn't point to a Drive folder")
    if not (data.get("capabilities") or {}).get("canAddChildren", False):
        raise DriveError("No permission to add files to that Drive folder")
    return data.get("name") or folder_id


async def upload_file(
    access_token: str,
    folder_id: str,
    filename: str,
    file_path: str,
) -> str:
    """Resumable-upload a local file into ``folder_id``. Returns the new
    Drive file id. Reads the file in fixed chunks (constant memory)."""
    size = os.path.getsize(file_path)
    mime = mimetypes.guess_type(filename)[0] or "application/octet-stream"

    async with httpx.AsyncClient(timeout=httpx.Timeout(300.0, connect=20.0)) as client:
        # 1. Initiate the resumable session.
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
                f"{init.text[:300]}"
            )
        session_url = init.headers.get("Location")
        if not session_url:
            raise DriveError(f"Drive upload init returned no session URL for {filename}")

        # 2. Upload the bytes. Empty files get a single zero-length PUT.
        if size == 0:
            resp = await client.put(
                session_url,
                headers={"Content-Range": "bytes */0"},
                content=b"",
            )
            if resp.status_code not in (200, 201):
                raise DriveError(f"Drive empty-file upload failed for {filename} ({resp.status_code})")
            return resp.json().get("id", "")

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
                    f"({resp.status_code}): {resp.text[:300]}"
                )
    # Loop exhausted without a terminal 200/201 (shouldn't happen).
    raise DriveError(f"Drive upload did not complete for {filename}")
