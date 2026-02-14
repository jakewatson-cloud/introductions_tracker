"""
Email Archiver
==============
Downloads full email content and attachments from Gmail,
saves them to the Investment Introductions archive folder.

Archive folder structure (grouped by property):
    .../1) Automated Introductions/
        [Town, Asset Name]/                      ← property folder
            [YYYY-MM-DD - Agent Name]/           ← dated subfolder (files live here)
                email_body.txt
                metadata.json
                [attachment1.pdf]
                [attachment2.xlsx]

Matching rules:
    - Same property within 90 days → new dated subfolder in existing property folder
    - Same property after 90 days  → new property folder with quarter suffix
    - New property                 → new property folder + dated subfolder

Property matching priority:
    1. Exact folder name match
    2. Postcode match (scans existing metadata.json files)
    3. Fuzzy name match (same town prefix + ≥70% similarity)
"""

import base64
import difflib
import json
import logging
import re
from datetime import datetime
from pathlib import Path
from typing import Optional

from email_pipeline.models import DealExtraction

logger = logging.getLogger(__name__)

# Regex for UK postcodes (e.g. "DY4 0PY", "SW1A 1AA", "M1 1AA")
_UK_POSTCODE_RE = re.compile(
    r"[A-Z]{1,2}\d[A-Z\d]?\s*\d[A-Z]{2}", re.IGNORECASE
)

# Regex for dated subfolder names: "YYYY-MM-DD - ..."
_DATED_SUBFOLDER_RE = re.compile(r"^(\d{4}-\d{2}-\d{2})\s*-\s*")

# Regex for quarter-suffix folders: "Name (YYYY QN)"
_QUARTER_SUFFIX_RE_TEMPLATE = r"^{base_name}\s*\(\d{{4}}\s+Q[1-4]\)$"

# Days within which emails are grouped into the same property folder
_GROUPING_WINDOW_DAYS = 90


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def build_archive_folder_name(deal: DealExtraction) -> str:
    """Build the archive folder name from deal data.

    Follows the existing convention: "[Town], [Asset Name]"
    e.g. "Birmingham, Erdington Industrial Estate"

    Parameters
    ----------
    deal : DealExtraction
        Extracted deal data.

    Returns
    -------
    str
        Folder name string.
    """
    town = deal.town.strip() if deal.town else ""
    asset = deal.asset_name.strip() if deal.asset_name else ""

    if town and asset:
        folder_name = f"{town}, {asset}"
    elif asset:
        folder_name = asset
    elif town:
        folder_name = town
    else:
        # Fallback to date and agent
        folder_name = f"Unknown - {deal.agent or 'No Agent'} - {deal.date or 'No Date'}"

    return _sanitise_folder_name(folder_name)


def archive_email(
    service,
    gmail_message_id: str,
    deal: DealExtraction,
    archive_root: Path,
    email_subject: str = "",
    email_sender: str = "",
    email_date: str = "",
) -> Optional[Path]:
    """Download and archive an email with attachments.

    Creates a property-grouped folder structure with dated subfolders.

    Parameters
    ----------
    service : googleapiclient.discovery.Resource
        Authenticated Gmail API service.
    gmail_message_id : str
        Gmail message ID to download.
    deal : DealExtraction
        Extracted deal data (used for folder naming).
    archive_root : Path
        Root path to the archive folder.
    email_subject, email_sender, email_date : str
        Email metadata for the metadata.json file.

    Returns
    -------
    Path or None
        Path to the created dated subfolder (where files live), or None if failed.
    """
    try:
        folder_name = build_archive_folder_name(deal)
        postcode = _normalise_postcode(deal.postcode) if deal.postcode else ""

        # Parse the new email's date for 90-day comparison
        new_email_dt = _parse_date(email_date)

        # Build the dated subfolder name
        subfolder_name = _build_dated_subfolder_name(email_date, deal.agent)

        # --- Find or create the property folder ---
        property_folder = _find_matching_property_folder(
            archive_root, folder_name, postcode
        )

        if property_folder is not None:
            # Property folder exists — migrate legacy flat structure if needed
            if _is_legacy_flat_archive(property_folder):
                logger.info("Migrating legacy flat archive: %s", property_folder.name)
                _migrate_legacy_folder(property_folder)

            # Check for duplicate: scan subfolders for matching gmail_message_id
            existing_subfolder = _find_archived_subfolder(
                property_folder, gmail_message_id
            )
            if existing_subfolder is not None:
                logger.info("Email already archived in %s", existing_subfolder)
                return existing_subfolder

            # Check the 90-day window
            most_recent_date = _get_most_recent_subfolder_date(property_folder)
            if most_recent_date is not None:
                gap = abs((new_email_dt - most_recent_date).days)
                if gap > _GROUPING_WINDOW_DAYS:
                    # Too old — create a new property folder with quarter suffix
                    suffix = _quarter_suffix(new_email_dt)
                    new_name = f"{folder_name} ({suffix})"
                    property_folder = archive_root / _sanitise_folder_name(new_name)
                    logger.info(
                        "90-day gap exceeded (%d days). New folder: %s",
                        gap,
                        property_folder.name,
                    )
        else:
            # No matching folder — create new property folder
            property_folder = archive_root / folder_name

        # --- Create the dated subfolder ---
        subfolder_path = property_folder / subfolder_name

        # Deduplicate subfolder name if it already exists
        if subfolder_path.exists():
            counter = 2
            while subfolder_path.exists():
                subfolder_path = property_folder / f"{subfolder_name} ({counter})"
                counter += 1

        subfolder_path.mkdir(parents=True, exist_ok=True)
        logger.info("Archiving to: %s", subfolder_path)

        # --- Fetch full email and save files ---
        msg = service.users().messages().get(
            userId="me", id=gmail_message_id, format="full"
        ).execute()
        payload = msg.get("payload", {})

        # Save email body
        body_text = _extract_full_body(payload)
        body_path = subfolder_path / "email_body.txt"
        body_path.write_text(body_text, encoding="utf-8")
        logger.info("  Saved email body (%d chars)", len(body_text))

        # Save attachments
        attachment_files = _download_attachments(
            service, gmail_message_id, payload, subfolder_path
        )
        logger.info("  Saved %d attachments", len(attachment_files))

        # Save metadata
        metadata = {
            "gmail_message_id": gmail_message_id,
            "subject": email_subject,
            "sender": email_sender,
            "date": email_date,
            "archived_at": datetime.now().isoformat(),
            "property_folder": property_folder.name,
            "deal_extraction": {
                "asset_name": deal.asset_name,
                "town": deal.town,
                "postcode": deal.postcode,
                "agent": deal.agent,
                "classification": deal.classification,
                "asking_price": deal.asking_price,
                "net_yield": deal.net_yield,
                "area_sqft": deal.area_sqft,
                "rent_pa": deal.rent_pa,
                "confidence": deal.confidence,
            },
            "attachments": attachment_files,
        }
        metadata_path = subfolder_path / "metadata.json"
        metadata_path.write_text(
            json.dumps(metadata, indent=2, default=str),
            encoding="utf-8",
        )

        return subfolder_path

    except Exception as e:
        logger.error("Failed to archive email %s: %s", gmail_message_id, e)
        return None


def get_attachment_paths(archive_folder: Path) -> list[Path]:
    """Get paths to brochure-type attachments in an archive folder.

    Filters for PDF and Excel files that are likely brochures.

    Parameters
    ----------
    archive_folder : Path
        Path to the archive folder (dated subfolder).

    Returns
    -------
    list[Path]
        Paths to brochure attachments.
    """
    brochure_extensions = {".pdf", ".xlsx", ".xls", ".pptx", ".ppt"}
    exclude_patterns = {"metadata.json", "email_body.txt"}

    paths = []
    for f in archive_folder.iterdir():
        if f.is_file() and f.suffix.lower() in brochure_extensions:
            if f.name not in exclude_patterns:
                paths.append(f)

    return sorted(paths)


# ---------------------------------------------------------------------------
# Property folder matching
# ---------------------------------------------------------------------------

def _find_matching_property_folder(
    archive_root: Path,
    folder_name: str,
    postcode: str,
) -> Optional[Path]:
    """Find an existing property folder matching the incoming deal.

    Checks in priority order:
    1. Exact folder name match
    2. Postcode match (scans metadata.json in subfolders)
    3. Fuzzy name match (same town, ≥70% similarity)
    4. Quarter-suffix variant match (e.g. "Name (2026 Q1)")

    Parameters
    ----------
    archive_root : Path
        Root archive directory.
    folder_name : str
        Base property folder name (e.g. "Tipton, Apex II").
    postcode : str
        Normalised UK postcode (e.g. "DY4 0PY"), or empty string.

    Returns
    -------
    Path or None
        Path to the matching property folder, or None.
    """
    if not archive_root.exists():
        return None

    # Check 1 — exact name
    exact = archive_root / folder_name
    if exact.exists() and exact.is_dir():
        return exact

    # Gather all property-level directories (skip non-directories)
    property_dirs = [d for d in archive_root.iterdir() if d.is_dir()]

    # Check 2 — postcode match
    if postcode:
        for prop_dir in property_dirs:
            if _folder_has_postcode(prop_dir, postcode):
                logger.info(
                    "Postcode match: %s → %s", postcode, prop_dir.name
                )
                return prop_dir

    # Check 3 — fuzzy name match (only within same town)
    town_prefix = folder_name.split(",")[0].strip().lower() if "," in folder_name else ""
    if town_prefix:
        best_match = None
        best_ratio = 0.0
        for prop_dir in property_dirs:
            dir_town = prop_dir.name.split(",")[0].strip().lower() if "," in prop_dir.name else ""
            if dir_town != town_prefix:
                continue
            # Strip any quarter suffix before comparing
            clean_name = _strip_quarter_suffix(prop_dir.name)
            ratio = difflib.SequenceMatcher(
                None, folder_name.lower(), clean_name.lower()
            ).ratio()
            if ratio >= 0.7 and ratio > best_ratio:
                best_ratio = ratio
                best_match = prop_dir

        if best_match is not None:
            logger.info(
                "Fuzzy match (%.0f%%): '%s' → '%s'",
                best_ratio * 100,
                folder_name,
                best_match.name,
            )
            return best_match

    # Check 4 — quarter-suffix variants
    suffix_pattern = re.compile(
        r"^" + re.escape(folder_name) + r"\s*\(\d{4}\s+Q[1-4]\)$"
    )
    matches = [d for d in property_dirs if suffix_pattern.match(d.name)]
    if matches:
        # Return the one with the most recent subfolder date
        best = None
        best_date = None
        for m in matches:
            dt = _get_most_recent_subfolder_date(m)
            if dt is not None and (best_date is None or dt > best_date):
                best = m
                best_date = dt
        return best or sorted(matches)[-1]

    return None


def _folder_has_postcode(property_folder: Path, target_postcode: str) -> bool:
    """Check if any metadata.json in a property folder contains the target postcode.

    Parameters
    ----------
    property_folder : Path
        Property-level folder to scan.
    target_postcode : str
        Normalised postcode to search for.

    Returns
    -------
    bool
        True if any subfolder's metadata contains this postcode.
    """
    target_norm = _normalise_postcode(target_postcode)
    if not target_norm:
        return False

    for item in property_folder.iterdir():
        if not item.is_dir():
            continue
        metadata_path = item / "metadata.json"
        if not metadata_path.exists():
            continue
        try:
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
            existing_pc = data.get("deal_extraction", {}).get("postcode", "")
            if existing_pc and _normalise_postcode(existing_pc) == target_norm:
                return True
        except (json.JSONDecodeError, OSError):
            continue

    return False


# ---------------------------------------------------------------------------
# Dated subfolder helpers
# ---------------------------------------------------------------------------

def _build_dated_subfolder_name(email_date: str, agent: str) -> str:
    """Build a dated subfolder name: 'YYYY-MM-DD - Agent Name'.

    Parameters
    ----------
    email_date : str
        Email date (ISO 8601 from Gmail, or DD/MM/YYYY).
    agent : str
        Agent name from the deal extraction.

    Returns
    -------
    str
        Sanitised subfolder name.
    """
    dt = _parse_date(email_date)
    date_str = dt.strftime("%Y-%m-%d")

    agent_clean = _sanitise_folder_name(agent.strip()) if agent else "Unknown Agent"
    if not agent_clean:
        agent_clean = "Unknown Agent"

    return _sanitise_folder_name(f"{date_str} - {agent_clean}")


def _get_most_recent_subfolder_date(property_folder: Path) -> Optional[datetime]:
    """Find the most recent date from dated subfolders in a property folder.

    Parameters
    ----------
    property_folder : Path
        Property-level folder.

    Returns
    -------
    datetime or None
        Most recent date, or None if no dated subfolders exist.
    """
    most_recent = None

    for item in property_folder.iterdir():
        if not item.is_dir():
            continue
        match = _DATED_SUBFOLDER_RE.match(item.name)
        if match:
            try:
                dt = datetime.strptime(match.group(1), "%Y-%m-%d")
                if most_recent is None or dt > most_recent:
                    most_recent = dt
            except ValueError:
                continue

    return most_recent


# ---------------------------------------------------------------------------
# Duplicate detection
# ---------------------------------------------------------------------------

def _find_archived_subfolder(
    property_folder: Path, gmail_message_id: str
) -> Optional[Path]:
    """Find the subfolder where a specific email is already archived.

    Parameters
    ----------
    property_folder : Path
        Property-level folder.
    gmail_message_id : str
        Gmail message ID to find.

    Returns
    -------
    Path or None
        Subfolder path if found, None otherwise.
    """
    for subfolder in property_folder.iterdir():
        if not subfolder.is_dir():
            continue
        metadata_path = subfolder / "metadata.json"
        if not metadata_path.exists():
            continue
        try:
            data = json.loads(metadata_path.read_text(encoding="utf-8"))
            if data.get("gmail_message_id") == gmail_message_id:
                return subfolder
        except (json.JSONDecodeError, OSError):
            continue

    return None


# ---------------------------------------------------------------------------
# Legacy flat archive migration
# ---------------------------------------------------------------------------

def _is_legacy_flat_archive(property_folder: Path) -> bool:
    """Check if a property folder is a legacy flat archive.

    A legacy folder has metadata.json directly in the property folder,
    rather than inside a dated subfolder.

    Parameters
    ----------
    property_folder : Path
        Property-level folder.

    Returns
    -------
    bool
        True if this is a legacy flat archive.
    """
    return (property_folder / "metadata.json").exists()


def _migrate_legacy_folder(property_folder: Path) -> Optional[Path]:
    """Migrate a legacy flat archive into the new grouped structure.

    Reads metadata.json to determine the date and agent, creates a dated
    subfolder, and moves all files into it.

    Parameters
    ----------
    property_folder : Path
        Path to the legacy flat archive folder.

    Returns
    -------
    Path or None
        Path to the created dated subfolder, or None if migration failed.
    """
    metadata_path = property_folder / "metadata.json"
    try:
        metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
    except (json.JSONDecodeError, OSError) as e:
        logger.warning("Cannot read metadata for migration: %s", e)
        return None

    email_date = metadata.get("date", "")
    agent = metadata.get("deal_extraction", {}).get("agent", "")
    subfolder_name = _build_dated_subfolder_name(email_date, agent)
    subfolder = property_folder / subfolder_name

    # Deduplicate subfolder name if needed
    if subfolder.exists():
        counter = 2
        while subfolder.exists():
            subfolder = property_folder / f"{subfolder_name} ({counter})"
            counter += 1

    subfolder.mkdir(parents=True, exist_ok=True)

    # Move all files (not directories) from property folder into subfolder
    for item in property_folder.iterdir():
        if item.is_file():
            item.rename(subfolder / item.name)

    logger.info("Migrated legacy archive to: %s", subfolder)
    return subfolder


# ---------------------------------------------------------------------------
# Utility helpers
# ---------------------------------------------------------------------------

def _sanitise_folder_name(name: str) -> str:
    """Sanitise a string for use as a folder name on Windows."""
    # Replace characters that are invalid in Windows paths
    invalid_chars = r'<>:"/\|?*'
    for ch in invalid_chars:
        name = name.replace(ch, "")

    # Collapse whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # Remove trailing dots/spaces (Windows doesn't like them)
    name = name.rstrip(". ")

    # Truncate to reasonable length
    if len(name) > 100:
        name = name[:100].rstrip()

    return name


def _sanitise_filename(filename: str) -> str:
    """Sanitise a filename for Windows."""
    invalid_chars = r'<>:"/\|?*'
    for ch in invalid_chars:
        filename = filename.replace(ch, "_")

    filename = filename.strip(". ")

    if len(filename) > 200:
        stem = Path(filename).stem[:190]
        suffix = Path(filename).suffix
        filename = f"{stem}{suffix}"

    return filename


def _parse_date(date_str: str) -> datetime:
    """Parse a date string to datetime.

    Tries ISO 8601 first, then DD/MM/YYYY, falls back to now().

    Parameters
    ----------
    date_str : str
        Date string to parse.

    Returns
    -------
    datetime
    """
    if not date_str:
        return datetime.now()

    # Try ISO 8601 (from Gmail headers)
    try:
        return datetime.fromisoformat(
            date_str.replace("Z", "+00:00")
        ).replace(tzinfo=None)
    except (ValueError, AttributeError):
        pass

    # Try DD/MM/YYYY (deal.date format)
    try:
        return datetime.strptime(date_str, "%d/%m/%Y")
    except (ValueError, AttributeError):
        pass

    return datetime.now()


def _normalise_postcode(postcode: str) -> str:
    """Normalise a UK postcode to uppercase with single space.

    Parameters
    ----------
    postcode : str
        Raw postcode string.

    Returns
    -------
    str
        Normalised postcode (e.g. "DY4 0PY") or empty string.
    """
    if not postcode:
        return ""

    cleaned = postcode.strip().upper()

    # Extract a valid UK postcode using regex
    match = _UK_POSTCODE_RE.search(cleaned)
    if not match:
        return ""

    pc = match.group(0)

    # Normalise spacing: ensure single space before last 3 chars
    pc = re.sub(r"\s+", "", pc)  # Remove all spaces
    if len(pc) >= 4:
        pc = pc[:-3] + " " + pc[-3:]

    return pc


def _quarter_suffix(dt: datetime) -> str:
    """Return a quarter suffix string like '2026 Q1' for a datetime."""
    quarter = (dt.month - 1) // 3 + 1
    return f"{dt.year} Q{quarter}"


def _strip_quarter_suffix(name: str) -> str:
    """Remove a quarter suffix from a folder name.

    e.g. "Tipton, Apex II (2026 Q1)" → "Tipton, Apex II"
    """
    return re.sub(r"\s*\(\d{4}\s+Q[1-4]\)$", "", name).strip()


# ---------------------------------------------------------------------------
# Email body extraction
# ---------------------------------------------------------------------------

def _extract_full_body(payload: dict) -> str:
    """Extract the full text body from a Gmail message payload.

    Tries plain text first, falls back to HTML with tag stripping.
    """
    mime_type = payload.get("mimeType", "")

    # Simple text message
    if mime_type == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        return ""

    # Multipart — collect all text parts
    parts = payload.get("parts", [])
    text_parts = []
    html_parts = []

    def _walk(parts_list):
        for part in parts_list:
            part_mime = part.get("mimeType", "")
            if part_mime == "text/plain":
                data = part.get("body", {}).get("data", "")
                if data:
                    text_parts.append(
                        base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    )
            elif part_mime == "text/html":
                data = part.get("body", {}).get("data", "")
                if data:
                    html_parts.append(
                        base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                    )
            elif part_mime.startswith("multipart/"):
                _walk(part.get("parts", []))

    _walk(parts)

    if text_parts:
        return "\n\n".join(text_parts)

    # Fallback: strip HTML tags
    if html_parts:
        html = "\n\n".join(html_parts)
        text = re.sub(r"<br\s*/?>", "\n", html, flags=re.IGNORECASE)
        text = re.sub(r"<p[^>]*>", "\n\n", text, flags=re.IGNORECASE)
        text = re.sub(r"<[^>]+>", "", text)
        text = re.sub(r"&nbsp;", " ", text)
        text = re.sub(r"&amp;", "&", text)
        text = re.sub(r"&lt;", "<", text)
        text = re.sub(r"&gt;", ">", text)
        text = re.sub(r"&#\d+;", "", text)
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()

    return ""


# ---------------------------------------------------------------------------
# Attachment filtering
# ---------------------------------------------------------------------------

# Image extensions to apply junk filtering to
_IMAGE_EXTENSIONS = {".jpg", ".jpeg", ".png", ".gif", ".bmp", ".webp", ".tiff", ".ico"}

# Minimum size in bytes for images to be worth downloading (50 KB)
_MIN_IMAGE_SIZE_BYTES = 50 * 1024

# Filename patterns that indicate junk/inline images
_JUNK_IMAGE_RE = re.compile(
    r"^(image\d*|photo\d*|pic\d*|logo|stamp|seal|banner|footer|header|"
    r"signature|icon|spacer|pixel|tracking|~WRD\d*|Slide\d*)\.",
    re.IGNORECASE,
)


def _is_junk_image(filename: str, size: int, headers: list[dict]) -> bool:
    """Determine if an image attachment is junk (logo, icon, tracking pixel).

    Checks are applied BEFORE downloading to save bandwidth. Non-image
    files (PDFs, Excel, etc.) always pass through.

    Parameters
    ----------
    filename : str
        Attachment filename.
    size : int
        File size in bytes (from Gmail API, available pre-download).
    headers : list[dict]
        Part headers from Gmail API.

    Returns
    -------
    bool
        True if the image should be skipped.
    """
    ext = Path(filename).suffix.lower()

    # Only filter image files — always keep PDFs, Excel, etc.
    if ext not in _IMAGE_EXTENSIONS:
        return False

    # Check 1: Content-ID header → inline/embedded image (logo in HTML body)
    for h in headers:
        name = h.get("name", "").lower()
        if name == "content-id":
            logger.debug("  Skipping inline image: %s (has Content-ID)", filename)
            return True

    # Check 2: Too small → almost certainly a logo/icon/pixel
    if size > 0 and size < _MIN_IMAGE_SIZE_BYTES:
        logger.debug("  Skipping small image: %s (%d bytes)", filename, size)
        return True

    # Check 3: Junk filename pattern
    if _JUNK_IMAGE_RE.match(filename):
        logger.debug("  Skipping junk-named image: %s", filename)
        return True

    return False


# ---------------------------------------------------------------------------
# Attachment downloading
# ---------------------------------------------------------------------------

def _download_attachments(
    service,
    gmail_message_id: str,
    payload: dict,
    folder_path: Path,
) -> list[str]:
    """Download all attachments from a Gmail message.

    Filters out junk images (logos, icons, tracking pixels) before
    downloading to save bandwidth and reduce archive clutter.

    Parameters
    ----------
    service : googleapiclient.discovery.Resource
        Authenticated Gmail API service.
    gmail_message_id : str
        Gmail message ID.
    payload : dict
        Gmail message payload.
    folder_path : Path
        Path to save attachments to.

    Returns
    -------
    list[str]
        Filenames of downloaded attachments.
    """
    saved_files = []
    skipped_count = 0

    def _walk_parts(parts):
        nonlocal skipped_count
        for part in parts:
            filename = part.get("filename", "")
            body = part.get("body", {})
            attachment_id = body.get("attachmentId")

            if filename and attachment_id:
                # Pre-download filter: skip junk images
                size = body.get("size", 0)
                part_headers = part.get("headers", [])

                if _is_junk_image(filename, size, part_headers):
                    skipped_count += 1
                    continue

                try:
                    att = (
                        service.users()
                        .messages()
                        .attachments()
                        .get(userId="me", messageId=gmail_message_id, id=attachment_id)
                        .execute()
                    )

                    data = att.get("data", "")
                    if data:
                        file_data = base64.urlsafe_b64decode(data)

                        safe_name = _sanitise_filename(filename)

                        file_path = folder_path / safe_name
                        if file_path.exists():
                            stem = file_path.stem
                            suffix = file_path.suffix
                            counter = 2
                            while file_path.exists():
                                file_path = folder_path / f"{stem} ({counter}){suffix}"
                                counter += 1

                        file_path.write_bytes(file_data)
                        saved_files.append(file_path.name)
                        logger.info(
                            "    Downloaded: %s (%d bytes)",
                            file_path.name,
                            len(file_data),
                        )

                except Exception as e:
                    logger.warning("    Failed to download %s: %s", filename, e)

            # Recurse into nested parts
            sub_parts = part.get("parts", [])
            if sub_parts:
                _walk_parts(sub_parts)

    parts = payload.get("parts", [])
    if parts:
        _walk_parts(parts)

    if skipped_count:
        logger.info("  Skipped %d junk image(s)", skipped_count)

    return saved_files
