"""
Deal Extractor
==============
Uses the Anthropic Claude API to:
1. Classify emails as investment introductions (or not)
2. Extract structured deal data from introduction emails

Single API call per email handles both classification and extraction.
"""

import json
import logging
import re
from typing import Optional

import anthropic

from email_pipeline.models import (
    ClassificationResult,
    DealExtraction,
)

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

CLASSIFY_AND_EXTRACT_PROMPT = """You are an analyst at a UK commercial property investment firm (Brydell Partners / Montholme Asset Management). Your task is to analyse the email below and:

1. **Classify** whether this email is an **investment introduction** — i.e. an agent marketing a commercial property for sale.

2. If it IS an introduction, **extract** the deal details.

## What counts as an investment introduction:
- An agent (e.g. CBRE, JLL, Savills, Knight Frank, Cushman & Wakefield, etc.) emailing to introduce a property or portfolio for sale
- Could be a formal brochure email, teaser, or brief "new instruction" notification
- May be forwarded by a colleague at Brydell Partners — if so, extract details from the ORIGINAL agent's email within

## What does NOT count:
- Operational emails (rent collection, service charge, legal, insurance, planning)
- Meeting invitations, calendar invites
- Market reports, newsletters, research bulletins
- Tenant correspondence
- Internal administrative emails
- Emails about properties the firm already owns or manages
- Valuation reports for existing assets
- General "keeping in touch" emails from agents with no specific property

## Extraction instructions (only if it IS an introduction):
Extract the following fields for EACH property introduced. Use null for anything not mentioned or unclear.
All monetary values should be in GBP (£). Convert if needed.

**IMPORTANT — Multiple deals**: If the email body contains multiple property introductions (e.g. a reply thread discussing several deals, or an agent introducing more than one property), extract ALL of them as separate deals in the array. Do not limit yourself to the property in the subject line.

- **date**: The date the introduction was sent (DD/MM/YYYY format)
- **agent**: The agent/firm name sending the introduction (e.g. "CBRE", "Knight Frank"). If forwarded by a colleague, use the ORIGINAL agent.
- **asset_name**: The property or estate name (e.g. "Erdington Industrial Estate", "Matrix Park")
- **country**: Country (default "England" unless stated otherwise)
- **town**: Town or city (e.g. "Birmingham", "Manchester")
- **address**: Full address if available
- **postcode**: UK postcode if available (e.g. "DY4 0PY"), empty string if not found
- **classification**: One of: "Multi-Let Industrial", "Single-Let Industrial", "Multi-Let Office", "Single-Let Office", "Retail", "Mixed Use", "Logistics", "Land", "Portfolio", "Other"
- **area_acres**: Site area in acres (null if not stated)
- **area_sqft**: Total floor area in square feet. Convert from sq m if needed (1 sq m = 10.764 sq ft)
- **rent_pa**: Total passing rent per annum in £
- **rent_psf**: Rent per square foot (derive from rent_pa / area_sqft if not stated)
- **asking_price**: Asking price / quoting price in £
- **net_yield**: Net Initial Yield as a percentage (e.g. 6.5, not 0.065)
- **reversionary_yield**: Reversionary yield as a percentage
- **confidence**: Your confidence in the extraction (0.0 to 1.0)

## Response format:
Return ONLY valid JSON with this structure:
{{
    "is_introduction": true/false,
    "reason": "Brief explanation of classification decision",
    "deals": [
        {{
            "date": "DD/MM/YYYY",
            "agent": "...",
            "asset_name": "...",
            "country": "...",
            "town": "...",
            "address": "...",
            "postcode": "...",
            "classification": "...",
            "area_acres": null,
            "area_sqft": null,
            "rent_pa": null,
            "rent_psf": null,
            "asking_price": null,
            "net_yield": null,
            "reversionary_yield": null,
            "confidence": 0.8
        }}
    ]
}}

If not an introduction, return:
{{
    "is_introduction": false,
    "reason": "Brief explanation",
    "deals": []
}}

## Email to analyse:

**From:** {sender}
**Date:** {date}
**Subject:** {subject}

{body}
"""

BATCH_CLASSIFY_PROMPT = """You are an analyst at a UK commercial property investment firm (Brydell Partners / Montholme Asset Management). Classify each email below as either an **investment introduction** (an agent marketing a property for sale) or **not an introduction** (operational, newsletter, admin, etc.).

**IMPORTANT — Forwarded emails**: Emails are often forwarded to us by capital partners or colleagues (e.g. from @brydellpartners.com). If the email is a forward (subject starts with "FW:", "Fwd:", or contains forwarded content) and the ORIGINAL email within is an agent marketing a property, classify it as an introduction. The forwarding itself does not count against it.

Similarly, replies (Re:) that contain an original agent introduction in the thread should also be classified as introductions.

**When uncertain**: If an email could plausibly be an introduction, classify it as one. A false positive costs little (the next stage will verify with the full email body), but a false negative means a deal is permanently missed.

For each email, respond with:
- **id**: The email number (1, 2, 3, etc.)
- **is_introduction**: true/false
- **confidence**: 0.0 to 1.0
- **reason**: Very brief (5-10 words)
- **asset_name**: If introduction, the property name (or "" if unclear)
- **town**: If introduction, the town/city (or "" if unclear)

Return ONLY valid JSON array:
[
    {{"id": 1, "is_introduction": true, "confidence": 0.9, "reason": "Agent marketing industrial estate", "asset_name": "Matrix Park", "town": "Birmingham"}},
    {{"id": 2, "is_introduction": false, "confidence": 0.95, "reason": "Service charge invoice", "asset_name": "", "town": ""}}
]

## Emails:

{emails}
"""


# ---------------------------------------------------------------------------
# Main functions
# ---------------------------------------------------------------------------

def classify_and_extract(
    api_key: str,
    sender: str,
    date: str,
    subject: str,
    body: str,
    gmail_message_id: str = "",
    model: str = "claude-sonnet-4-20250514",
) -> tuple[ClassificationResult, list[DealExtraction]]:
    """Classify an email and extract deal data if it's an introduction.

    Single API call handles both classification and extraction.
    Supports multiple deals per email (e.g. reply threads mentioning several properties).

    Parameters
    ----------
    api_key : str
        Anthropic API key.
    sender, date, subject, body : str
        Email content.
    gmail_message_id : str
        Gmail message ID for tracking.
    model : str
        Claude model to use.

    Returns
    -------
    tuple[ClassificationResult, list[DealExtraction]]
        Classification result, and list of deal extractions (empty if not introduction).
    """
    client = anthropic.Anthropic(api_key=api_key)

    # Truncate body to avoid excessive token usage
    max_body_chars = 8000
    if len(body) > max_body_chars:
        body = body[:max_body_chars] + "\n\n[... truncated ...]"

    prompt = CLASSIFY_AND_EXTRACT_PROMPT.format(
        sender=sender,
        date=date,
        subject=subject,
        body=body,
    )

    try:
        message = client.messages.create(
            model=model,
            max_tokens=3000,
            messages=[{"role": "user", "content": prompt}],
        )

        # Parse response
        response_text = message.content[0].text.strip()

        # Extract JSON from response (handle markdown code blocks)
        if response_text.startswith("```"):
            # Strip code block markers
            lines = response_text.split("\n")
            json_lines = []
            in_block = False
            for line in lines:
                if line.strip().startswith("```"):
                    in_block = not in_block
                    continue
                if in_block or not line.strip().startswith("```"):
                    json_lines.append(line)
            response_text = "\n".join(json_lines)

        data = json.loads(response_text)

        is_intro = data.get("is_introduction", False)
        reason = data.get("reason", "")

        # Parse deals — handle both new "deals" array and legacy "deal" object
        raw_deals = data.get("deals", [])
        if not raw_deals and data.get("deal"):
            # Backward compat: single "deal" object → wrap in list
            raw_deals = [data["deal"]]

        # Build classification result (use first deal for suggested fields)
        first_deal = raw_deals[0] if raw_deals else {}
        classification = ClassificationResult(
            gmail_message_id=gmail_message_id,
            is_introduction=is_intro,
            confidence=first_deal.get("confidence", 0.8) if is_intro else 0.9,
            reason=reason,
            suggested_asset_name=first_deal.get("asset_name", "") if is_intro else "",
            suggested_town=first_deal.get("town", "") if is_intro else "",
        )

        # Parse each deal
        deals = []
        if is_intro:
            for d in raw_deals:
                deal = DealExtraction(
                    date=d.get("date", date),
                    agent=d.get("agent", ""),
                    asset_name=d.get("asset_name", ""),
                    country=d.get("country", "England"),
                    town=d.get("town", ""),
                    address=d.get("address", ""),
                    postcode=d.get("postcode", ""),
                    classification=d.get("classification", ""),
                    area_acres=_to_float(d.get("area_acres")),
                    area_sqft=_to_float(d.get("area_sqft")),
                    rent_pa=_to_float(d.get("rent_pa")),
                    rent_psf=_to_float(d.get("rent_psf")),
                    asking_price=_to_float(d.get("asking_price")),
                    net_yield=_to_float(d.get("net_yield")),
                    reversionary_yield=_to_float(d.get("reversionary_yield")),
                    confidence=d.get("confidence", 0.8),
                    raw_source="email",
                )

                # Fallback: if asset_name is missing, extract from the email subject
                if not deal.asset_name:
                    fallback_name = _extract_asset_name_from_subject(subject)
                    if fallback_name:
                        deal.asset_name = fallback_name
                        logger.info(
                            "  Subject-line fallback for asset_name: %s",
                            fallback_name,
                        )

                deals.append(deal)

        return classification, deals

    except json.JSONDecodeError as e:
        logger.error("Failed to parse Claude response as JSON: %s", e)
        return (
            ClassificationResult(
                gmail_message_id=gmail_message_id,
                is_introduction=False,
                confidence=0.0,
                reason=f"JSON parse error: {e}",
            ),
            [],
        )
    except anthropic.APIError as e:
        logger.error("Anthropic API error: %s", e)
        raise


def batch_classify(
    api_key: str,
    emails: list[dict],
    model: str = "claude-sonnet-4-20250514",
) -> list[ClassificationResult]:
    """Classify a batch of emails as introductions or not.

    Uses a single API call to classify up to 10 emails at once.
    Useful for the preview/confirmation step before full processing.

    Parameters
    ----------
    api_key : str
        Anthropic API key.
    emails : list[dict]
        List of email dicts with keys: gmail_message_id, sender, date, subject, snippet.
    model : str
        Claude model to use.

    Returns
    -------
    list[ClassificationResult]
        Classification for each email.
    """
    if not emails:
        return []

    client = anthropic.Anthropic(api_key=api_key)

    # Format emails for the prompt (use body_preview for more context than snippet)
    email_texts = []
    for i, email in enumerate(emails, 1):
        preview = email.get("body_preview", "") or email.get("snippet", "")
        email_texts.append(
            f"--- Email {i} ---\n"
            f"From: {email.get('sender', '')}\n"
            f"Date: {email.get('date', '')}\n"
            f"Subject: {email.get('subject', '')}\n"
            f"Body preview: {preview}\n"
        )

    prompt = BATCH_CLASSIFY_PROMPT.format(
        emails="\n".join(email_texts),
    )

    try:
        message = client.messages.create(
            model=model,
            max_tokens=2000,
            messages=[{"role": "user", "content": prompt}],
        )

        response_text = message.content[0].text.strip()

        # Strip code block if present
        if response_text.startswith("```"):
            lines = response_text.split("\n")
            json_lines = [
                l for l in lines if not l.strip().startswith("```")
            ]
            response_text = "\n".join(json_lines)

        results_data = json.loads(response_text)

        results = []
        for item in results_data:
            idx = item.get("id", 0) - 1  # Convert to 0-based
            if 0 <= idx < len(emails):
                email = emails[idx]
                results.append(
                    ClassificationResult(
                        gmail_message_id=email.get("gmail_message_id", ""),
                        is_introduction=item.get("is_introduction", False),
                        confidence=item.get("confidence", 0.5),
                        reason=item.get("reason", ""),
                        suggested_asset_name=item.get("asset_name", ""),
                        suggested_town=item.get("town", ""),
                    )
                )

        return results

    except (json.JSONDecodeError, anthropic.APIError) as e:
        logger.error("Batch classification error: %s", e)
        # Return unknown classification for all emails
        return [
            ClassificationResult(
                gmail_message_id=email.get("gmail_message_id", ""),
                is_introduction=False,
                confidence=0.0,
                reason=f"Batch classification error: {e}",
            )
            for email in emails
        ]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_SUBJECT_PREFIX_RE = re.compile(
    r"^(?:(?:RE|FW|Fwd)\s*:\s*)+", re.IGNORECASE
)

_SUBJECT_SUFFIX_RE = re.compile(
    r"\s*-\s*(?:Subject to|Revised|Logistics Warehous|Investment Opport).*$",
    re.IGNORECASE,
)


def _extract_asset_name_from_subject(subject: str) -> str:
    """Extract a usable asset name from an email subject line.

    Used as a fallback when the AI extraction returns None/empty for asset_name.

    Strategy:
    - Strip FW:/RE:/Fwd: prefixes
    - Strip trailing boilerplate (e.g. "- Subject to Contract")
    - If the result contains a comma followed by a town-like word, take the part
      before the town as the asset name (since town is already extracted separately)
    - Otherwise use the cleaned subject as-is

    Examples:
        "FW: Kings Road, Tyseley"               → "Kings Road, Tyseley"
        "Anglian Lane, Bury St Edmunds"          → "Anglian Lane, Bury St Edmunds"
        "FW: 124 Victoria Road, Farnborough ..." → "124 Victoria Road, Farnborough"
        "RE: FW: Some Property - Subject to..."  → "Some Property"
    """
    if not subject:
        return ""

    # Strip reply/forward prefixes (handles nesting: "RE: FW: ...")
    cleaned = _SUBJECT_PREFIX_RE.sub("", subject).strip()

    # Strip trailing boilerplate
    cleaned = _SUBJECT_SUFFIX_RE.sub("", cleaned).strip()

    # Remove anything after a dash that looks like metadata
    # e.g. "Boxes and Packaging, Swindon, SN5 7YZ - GU14 7PW"
    # But be careful: some subjects like "Warrington Central Trading Estate & Causeway Park" have no dash
    # Only strip if the dash is followed by a postcode-like pattern or "GU14" etc.
    cleaned = re.sub(r"\s*-\s+[A-Z]{1,2}\d.*$", "", cleaned).strip()

    return cleaned if cleaned else ""


def _to_float(value) -> Optional[float]:
    """Convert a value to float, handling strings with commas, currency symbols, etc."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        # Remove currency symbols, commas, whitespace
        cleaned = value.replace("£", "").replace(",", "").replace(" ", "").strip()
        if not cleaned or cleaned.lower() in ("null", "none", "n/a", "tbc", "poa"):
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None
    return None
