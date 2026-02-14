"""
Email Scanner
=============
Scans Gmail for investment introduction emails using the Gmail API.

Supports filtering by:
- Gmail label (e.g. "Investment Introduction")
- Date range
- Sender domain whitelist
- Subject/body keywords
"""

import base64
import logging
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from typing import Optional

from googleapiclient.discovery import build as _build_service

from email_pipeline.models import EmailSummary, ThreadSummary

logger = logging.getLogger(__name__)


def _parse_headers(headers: list[dict]) -> dict[str, str]:
    """Extract useful headers from a Gmail message."""
    result = {}
    for h in headers:
        name = h.get("name", "").lower()
        if name in ("from", "to", "subject", "date"):
            result[name] = h.get("value", "")
    return result


def _extract_sender_domain(from_header: str) -> str:
    """Extract domain from a From header like 'Name <user@domain.com>'."""
    match = re.search(r"@([\w.-]+)", from_header)
    return match.group(1).lower() if match else ""


def _extract_email_address(from_header: str) -> str:
    """Extract email address from a From header."""
    match = re.search(r"<([^>]+)>", from_header)
    if match:
        return match.group(1).lower()
    # Bare email address
    match = re.search(r"[\w.+-]+@[\w.-]+", from_header)
    return match.group(0).lower() if match else from_header.lower()


def _get_body_text(payload: dict) -> str:
    """Extract plain text body from a Gmail message payload.

    Handles both simple messages and multipart MIME structures.
    """
    mime_type = payload.get("mimeType", "")

    # Simple text message
    if mime_type == "text/plain":
        data = payload.get("body", {}).get("data", "")
        if data:
            return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        return ""

    # Multipart — recurse into parts
    parts = payload.get("parts", [])
    for part in parts:
        part_mime = part.get("mimeType", "")
        if part_mime == "text/plain":
            data = part.get("body", {}).get("data", "")
            if data:
                return base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
        elif part_mime.startswith("multipart/"):
            text = _get_body_text(part)
            if text:
                return text

    # Fallback: try HTML if no plain text
    for part in parts:
        if part.get("mimeType") == "text/html":
            data = part.get("body", {}).get("data", "")
            if data:
                html = base64.urlsafe_b64decode(data).decode("utf-8", errors="replace")
                # Strip HTML tags for keyword matching
                return re.sub(r"<[^>]+>", " ", html)

    return ""


def _get_attachment_names(payload: dict) -> list[str]:
    """Extract attachment filenames from a Gmail message payload."""
    names = []

    def _walk_parts(parts):
        for part in parts:
            filename = part.get("filename", "")
            if filename:
                names.append(filename)
            sub_parts = part.get("parts", [])
            if sub_parts:
                _walk_parts(sub_parts)

    parts = payload.get("parts", [])
    if parts:
        _walk_parts(parts)

    return names


def _matches_keywords(text: str, keywords: list[str]) -> list[str]:
    """Check which keywords appear in the text. Returns matched keywords."""
    if not keywords:
        return []
    text_lower = text.lower()
    return [kw for kw in keywords if kw in text_lower]


def _matches_sender(sender_domain: str, whitelist: list[str]) -> bool:
    """Check if the sender domain matches any entry in the whitelist."""
    if not whitelist:
        return False
    for entry in whitelist:
        # entry might be '@cbre.com' or 'cbre.com'
        domain = entry.lstrip("@").lower()
        if sender_domain == domain or sender_domain.endswith("." + domain):
            return True
    return False


def build_gmail_query(
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    label: Optional[str] = None,
) -> str:
    """Build a Gmail search query string.

    Parameters
    ----------
    after_date : str, optional
        Only include emails after this date (YYYY-MM-DD).
    before_date : str, optional
        Only include emails before this date (YYYY-MM-DD).
    label : str, optional
        Only include emails with this Gmail label.

    Returns
    -------
    str
        Gmail search query string.
    """
    parts = []

    if after_date:
        parts.append(f"after:{after_date}")
    if before_date:
        parts.append(f"before:{before_date}")
    if label:
        parts.append(f"label:{label.replace(' ', '-')}")

    return " ".join(parts)


def scan_emails(
    service,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    label: Optional[str] = None,
    sender_whitelist: Optional[list[str]] = None,
    keywords: Optional[list[str]] = None,
    max_results: int = 500,
    require_all_filters: bool = False,
) -> list[EmailSummary]:
    """Scan Gmail for investment introduction emails.

    Parameters
    ----------
    service : googleapiclient.discovery.Resource
        Authenticated Gmail API service.
    after_date : str, optional
        Only include emails after this date (YYYY-MM-DD).
    before_date : str, optional
        Only include emails before this date (YYYY-MM-DD).
    label : str, optional
        Gmail label to filter by.
    sender_whitelist : list[str], optional
        List of sender domain suffixes to match.
    keywords : list[str], optional
        Keywords to search for in subject/body.
    max_results : int
        Maximum number of emails to return.
    require_all_filters : bool
        If True, require ALL of label+sender+keyword to match.
        If False (default), match on ANY filter (union).

    Returns
    -------
    list[EmailSummary]
        List of matching email summaries, newest first.
    """
    sender_whitelist = sender_whitelist or []
    keywords = keywords or []

    # Build the Gmail API query (server-side filtering)
    query = build_gmail_query(after_date, before_date, label)

    print(f"  Scanning Gmail with query: {query or '(all messages)'}")

    # Fetch message IDs
    message_ids = []
    page_token = None

    while True:
        result = service.users().messages().list(
            userId="me",
            q=query,
            maxResults=min(max_results - len(message_ids), 100),
            pageToken=page_token,
        ).execute()

        messages = result.get("messages", [])
        message_ids.extend(m["id"] for m in messages)

        page_token = result.get("nextPageToken")
        if not page_token or len(message_ids) >= max_results:
            break

    print(f"  Found {len(message_ids)} messages matching Gmail query")

    if not message_ids:
        return []

    # Fetch messages concurrently (10 threads) and apply local filters
    summaries = []

    # Extract credentials from the service so each thread can build its own
    # (google-api-python-client service objects are NOT thread-safe)
    _creds = service._http.credentials

    import threading
    _thread_local = threading.local()

    def _get_thread_service():
        """Return a per-thread Gmail service instance."""
        if not hasattr(_thread_local, "service"):
            _thread_local.service = _build_service(
                "gmail", "v1", credentials=_creds
            )
        return _thread_local.service

    def _fetch_one(msg_id: str) -> dict | None:
        """Fetch a single message using a thread-local service."""
        try:
            svc = _get_thread_service()
            return svc.users().messages().get(
                userId="me", id=msg_id, format="full"
            ).execute()
        except Exception as e:
            logger.warning("Failed to fetch message %s: %s", msg_id, e)
            return None

    print(f"  Fetching {len(message_ids)} messages (10 concurrent threads)...")
    fetched: list[dict] = []

    with ThreadPoolExecutor(max_workers=10) as executor:
        futures = {
            executor.submit(_fetch_one, mid): mid
            for mid in message_ids
        }
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            if done_count % 50 == 0:
                print(f"  Fetched {done_count}/{len(message_ids)}...")
            msg = future.result()
            if msg is not None:
                fetched.append(msg)

    print(f"  Fetched {len(fetched)} messages, applying filters...")

    for msg in fetched:
        msg_id = msg.get("id", "")
        thread_id = msg.get("threadId", "")
        payload = msg.get("payload", {})
        headers = _parse_headers(payload.get("headers", []))

        sender = headers.get("from", "")
        subject = headers.get("subject", "")
        date_str = headers.get("date", "")
        sender_domain = _extract_sender_domain(sender)
        snippet = msg.get("snippet", "")
        label_ids = msg.get("labelIds", [])

        # Get body text for keyword matching
        body_text = _get_body_text(payload)
        searchable_text = f"{subject} {body_text}"

        # Get attachment info
        attachment_names = _get_attachment_names(payload)
        has_attachments = len(attachment_names) > 0

        # Apply filters
        matched_label = label is not None  # If label was in query, it matched server-side
        matched_sender = _matches_sender(sender_domain, sender_whitelist)
        matched_kws = _matches_keywords(searchable_text, keywords)

        # Decide whether this email qualifies
        # Label match always qualifies (user manually tagged it) — OR —
        # sender/keyword filters must pass per the selected mode.
        if matched_label:
            passes = True
        elif require_all_filters:
            # All specified filters must match
            passes = True
            if sender_whitelist and not matched_sender:
                passes = False
            if keywords and not matched_kws:
                passes = False
        else:
            # Any filter match qualifies (or no local filters = pass all)
            if not sender_whitelist and not keywords:
                passes = True
            else:
                passes = matched_sender or bool(matched_kws)

        if not passes:
            continue

        # Parse date
        try:
            # Gmail dates can be complex, extract the core date
            internal_date = msg.get("internalDate", "0")
            dt = datetime.fromtimestamp(int(internal_date) / 1000)
            iso_date = dt.isoformat()
        except (ValueError, TypeError):
            iso_date = date_str

        # Build body preview for batch classifier (~500 chars, more context than snippet)
        body_preview = body_text[:500].strip() if body_text else snippet

        summary = EmailSummary(
            gmail_message_id=msg_id,
            subject=subject,
            sender=sender,
            sender_domain=sender_domain,
            date=iso_date,
            snippet=snippet,
            body_preview=body_preview,
            has_attachments=has_attachments,
            attachment_names=attachment_names,
            labels=label_ids,
            matched_keywords=matched_kws,
            matched_sender=matched_sender,
            matched_label=matched_label,
            thread_id=thread_id,
        )
        summaries.append(summary)

    print(f"  {len(summaries)} emails passed local filters")
    return summaries


def group_by_thread(summaries: list[EmailSummary]) -> list[ThreadSummary]:
    """Group email summaries by Gmail thread ID.

    Emails with no thread_id become single-email threads.
    Returns list sorted by latest date descending (newest threads first).
    """
    from collections import defaultdict

    threads: dict[str, list[EmailSummary]] = defaultdict(list)

    for s in summaries:
        key = s.thread_id if s.thread_id else s.gmail_message_id
        threads[key].append(s)

    result: list[ThreadSummary] = []

    for tid, emails in threads.items():
        # Sort emails oldest first within the thread
        emails.sort(key=lambda e: e.date)

        # Aggregate data
        all_domains = list(dict.fromkeys(e.sender_domain for e in emails))
        all_attachments = list(dict.fromkeys(
            name for e in emails for name in e.attachment_names
        ))
        any_sender = any(e.matched_sender for e in emails)
        any_label = any(e.matched_label for e in emails)
        all_keywords = list(dict.fromkeys(
            kw for e in emails for kw in e.matched_keywords
        ))

        ts = ThreadSummary(
            thread_id=tid,
            email_count=len(emails),
            latest_date=emails[-1].date,
            latest_subject=emails[-1].subject,
            earliest_date=emails[0].date,
            all_sender_domains=all_domains,
            all_attachment_names=all_attachments,
            matched_sender=any_sender,
            matched_label=any_label,
            matched_keywords=all_keywords,
            emails=emails,
        )
        result.append(ts)

    # Sort threads by latest date descending (newest first)
    result.sort(key=lambda t: t.latest_date, reverse=True)
    return result


def print_scan_results(summaries: list[EmailSummary]) -> None:
    """Print a formatted table of scan results."""
    if not summaries:
        print("\n  No matching emails found.")
        return

    print(f"\n  {'#':>3}  {'Date':<12} {'Sender':<35} {'Subject':<50} {'Attachments':<20} {'Match Reason'}")
    print(f"  {'─' * 3}  {'─' * 12} {'─' * 35} {'─' * 50} {'─' * 20} {'─' * 30}")

    for i, s in enumerate(summaries, 1):
        # Format date
        try:
            dt = datetime.fromisoformat(s.date)
            date_str = dt.strftime("%d/%m/%Y")
        except ValueError:
            date_str = s.date[:12]

        # Truncate fields
        sender_short = s.sender[:33] + ".." if len(s.sender) > 35 else s.sender
        subject_short = s.subject[:48] + ".." if len(s.subject) > 50 else s.subject
        attach_str = ", ".join(s.attachment_names)[:18] if s.attachment_names else "-"

        # Match reason
        reasons = []
        if s.matched_label:
            reasons.append("label")
        if s.matched_sender:
            reasons.append("sender")
        if s.matched_keywords:
            reasons.append(f"kw:{','.join(s.matched_keywords[:3])}")
        reason_str = " + ".join(reasons) if reasons else "query"

        print(f"  {i:>3}  {date_str:<12} {sender_short:<35} {subject_short:<50} {attach_str:<20} {reason_str}")

    print(f"\n  Total: {len(summaries)} emails")

    # Summary stats
    with_attachments = sum(1 for s in summaries if s.has_attachments)
    unique_senders = len(set(s.sender_domain for s in summaries))
    print(f"  With attachments: {with_attachments}")
    print(f"  Unique sender domains: {unique_senders}")

    # Top sender domains
    domain_counts = {}
    for s in summaries:
        domain_counts[s.sender_domain] = domain_counts.get(s.sender_domain, 0) + 1
    top_domains = sorted(domain_counts.items(), key=lambda x: x[1], reverse=True)[:10]
    if top_domains:
        print(f"\n  Top sender domains:")
        for domain, count in top_domains:
            print(f"    {domain}: {count}")
