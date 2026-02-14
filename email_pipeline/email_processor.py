"""
Email Processor (Orchestrator)
==============================
Ties all pipeline components together.

Processing flow:
1. Scan Gmail for matching emails
2. Filter out already-processed emails (idempotency)
3. Classify emails as introductions using Claude API (batch)
4. For each introduction:
   a. Full classify + extract deal details
   b. Archive email + attachments to Investment Introductions folder
   c. Parse brochure attachments for comparables
   d. Merge brochure data with email data (brochure wins for numbers)
   e. Append deal to Pipeline Excel
   f. Append comparables to Investment Comps / Occupational Comps Excel
5. Mark all emails as processed in the database
6. Return a processing report
"""

import json
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Optional

from email_pipeline.database import Database
from email_pipeline.deal_extractor import batch_classify, classify_and_extract
from email_pipeline.email_archiver import archive_email, get_attachment_paths
from email_pipeline.email_scanner import scan_emails, _get_body_text, _parse_headers, _extract_sender_domain
from email_pipeline.brochure_parser import parse_brochure
from email_pipeline.excel_writer import (
    PipelineWriter,
    InvestmentCompsWriter,
    OccupationalCompsWriter,
)
from email_pipeline.models import (
    ClassificationResult,
    DealExtraction,
    ProcessingReport,
    ProcessingResult,
    ProcessingStatus,
)

logger = logging.getLogger(__name__)


def process_emails(
    service,
    api_key: str,
    db: Database,
    archive_root: Path,
    pipeline_excel_path: Optional[Path] = None,
    investment_comps_path: Optional[Path] = None,
    occupational_comps_path: Optional[Path] = None,
    after_date: Optional[str] = None,
    before_date: Optional[str] = None,
    label: Optional[str] = None,
    sender_whitelist: Optional[list[str]] = None,
    keywords: Optional[list[str]] = None,
    max_results: int = 500,
    dry_run: bool = False,
    auto_confirm: bool = False,
    require_all_filters: bool = False,
) -> ProcessingReport:
    """Process emails through the full pipeline.

    Parameters
    ----------
    service : googleapiclient.discovery.Resource
        Authenticated Gmail API service.
    api_key : str
        Anthropic API key.
    db : Database
        SQLite database for tracking.
    archive_root : Path
        Root path for the Investment Introductions archive.
    pipeline_excel_path : Path, optional
        Path to Pipeline 2026.xlsx.
    investment_comps_path : Path, optional
        Path to INVESTMENT COMPARABLES MASTER.xlsx.
    occupational_comps_path : Path, optional
        Path to OCCUPATIONAL COMPARABLES.xlsx.
    after_date, before_date : str, optional
        Date range for scanning.
    label : str, optional
        Gmail label filter.
    sender_whitelist : list[str], optional
        Sender domain whitelist.
    keywords : list[str], optional
        Keyword filter.
    max_results : int
        Maximum emails to scan.
    dry_run : bool
        If True, classify but don't process.
    auto_confirm : bool
        If True, skip confirmation prompt.

    Returns
    -------
    ProcessingReport
        Summary of what was processed.
    """
    report = ProcessingReport()

    # -----------------------------------------------------------------------
    # Step 1: Scan Gmail
    # -----------------------------------------------------------------------
    print("\n[Step 1/6] Scanning Gmail...")
    summaries = scan_emails(
        service=service,
        after_date=after_date,
        before_date=before_date,
        label=label,
        sender_whitelist=sender_whitelist,
        keywords=keywords,
        max_results=max_results,
        require_all_filters=require_all_filters,
    )
    report.total_scanned = len(summaries)
    print(f"  Found {len(summaries)} matching emails")

    if not summaries:
        print("  No emails to process.")
        return report

    # -----------------------------------------------------------------------
    # Step 2: Filter out already-processed emails
    # -----------------------------------------------------------------------
    print("\n[Step 2/6] Checking for already-processed emails...")
    all_ids = [s.gmail_message_id for s in summaries]
    unprocessed_ids = set(db.get_unprocessed_ids(all_ids))
    new_summaries = [s for s in summaries if s.gmail_message_id in unprocessed_ids]
    report.already_processed = len(summaries) - len(new_summaries)
    print(f"  {report.already_processed} already processed, {len(new_summaries)} new")

    if not new_summaries:
        print("  All emails already processed.")
        return report

    # -----------------------------------------------------------------------
    # Step 3: Batch classify with Claude API
    # -----------------------------------------------------------------------
    print(f"\n[Step 3/6] Classifying {len(new_summaries)} emails with AI...")

    # Prepare batches of 10
    classifications: dict[str, ClassificationResult] = {}
    batch_size = 10
    batches = []

    for i in range(0, len(new_summaries), batch_size):
        batch = new_summaries[i : i + batch_size]
        batch_emails = [
            {
                "gmail_message_id": s.gmail_message_id,
                "sender": s.sender,
                "date": s.date,
                "subject": s.subject,
                "snippet": s.snippet,
                "body_preview": s.body_preview,
            }
            for s in batch
        ]
        batches.append((i // batch_size + 1, batch_emails))

    num_batches = len(batches)
    print(f"  Sending {num_batches} batches to Claude API (concurrent)...")

    def _classify_batch(batch_info):
        batch_num, batch_emails = batch_info
        return batch_classify(api_key, batch_emails)

    with ThreadPoolExecutor(max_workers=min(num_batches, 4)) as executor:
        futures = {
            executor.submit(_classify_batch, b): b[0]
            for b in batches
        }
        done_count = 0
        for future in as_completed(futures):
            done_count += 1
            batch_num = futures[future]
            print(f"  Batch {batch_num}/{num_batches} classified ({done_count}/{num_batches} done)")
            results = future.result()
            for result in results:
                classifications[result.gmail_message_id] = result

    # Count classifications
    intro_ids = [
        msg_id
        for msg_id, cls in classifications.items()
        if cls.is_introduction
    ]
    non_intro_ids = [
        msg_id
        for msg_id, cls in classifications.items()
        if not cls.is_introduction
    ]
    report.classified_as_introduction = len(intro_ids)
    report.classified_as_not_introduction = len(non_intro_ids)

    print(f"  Introductions: {len(intro_ids)}")
    print(f"  Not introductions: {len(non_intro_ids)}")

    # Show classification results
    if intro_ids:
        print("\n  Identified introductions:")
        for msg_id in intro_ids:
            cls = classifications[msg_id]
            summary = next(s for s in new_summaries if s.gmail_message_id == msg_id)
            print(f"    ✓ {summary.subject[:60]}")
            if cls.suggested_asset_name:
                print(f"      → {cls.suggested_town}, {cls.suggested_asset_name}")

    if non_intro_ids:
        print(f"\n  Skipped (not introductions): {len(non_intro_ids)}")
        for msg_id in non_intro_ids[:5]:
            cls = classifications[msg_id]
            summary = next(s for s in new_summaries if s.gmail_message_id == msg_id)
            print(f"    ✗ {summary.subject[:60]} ({cls.reason})")
        if len(non_intro_ids) > 5:
            print(f"    ... and {len(non_intro_ids) - 5} more")

    # Mark non-introductions as processed
    for msg_id in non_intro_ids:
        cls = classifications[msg_id]
        summary = next(s for s in new_summaries if s.gmail_message_id == msg_id)
        db.mark_processed(
            gmail_message_id=msg_id,
            subject=summary.subject,
            sender=summary.sender,
            sender_domain=summary.sender_domain,
            email_date=summary.date,
            status="skipped",
            is_introduction=False,
            classification_reason=cls.reason,
        )

    if dry_run:
        print("\n  DRY RUN — stopping before processing.")
        return report

    if not intro_ids:
        print("\n  No introductions to process.")
        return report

    # -----------------------------------------------------------------------
    # Step 4: Confirmation (unless auto_confirm)
    # -----------------------------------------------------------------------
    if not auto_confirm:
        print(f"\n  Ready to process {len(intro_ids)} introductions.")
        print("  This will:")
        if archive_root:
            print(f"    • Archive emails to {archive_root}")
        if pipeline_excel_path:
            print(f"    • Update Pipeline Excel at {pipeline_excel_path.name}")
        if investment_comps_path:
            print(f"    • Extract investment comps to {investment_comps_path.name}")
        if occupational_comps_path:
            print(f"    • Extract occupational comps to {occupational_comps_path.name}")

        response = input("\n  Proceed? [y/N]: ").strip().lower()
        if response not in ("y", "yes"):
            print("  Cancelled.")
            return report

    # -----------------------------------------------------------------------
    # Step 5: Process each introduction
    # -----------------------------------------------------------------------
    print(f"\n[Step 4/6] Processing {len(intro_ids)} introductions...")

    # Set up writers (with path validation)
    pipeline_writer = None
    if pipeline_excel_path:
        if pipeline_excel_path.exists():
            pipeline_writer = PipelineWriter(pipeline_excel_path)
        else:
            print(f"  ⚠ Pipeline Excel not found: {pipeline_excel_path}")
            print(f"    Check PIPELINE_EXCEL_PATH in .env")

    inv_comps_writer = None
    if investment_comps_path:
        if investment_comps_path.exists():
            inv_comps_writer = InvestmentCompsWriter(investment_comps_path)
        else:
            print(f"  ⚠ Investment Comps Excel not found: {investment_comps_path}")

    occ_comps_writer = OccupationalCompsWriter(occupational_comps_path) if occupational_comps_path else None

    for idx, msg_id in enumerate(intro_ids, 1):
        summary = next(s for s in new_summaries if s.gmail_message_id == msg_id)
        print(f"\n  [{idx}/{len(intro_ids)}] Processing: {summary.subject[:60]}")

        try:
            result = _process_single_email(
                service=service,
                api_key=api_key,
                gmail_message_id=msg_id,
                summary=summary,
                archive_root=archive_root,
                pipeline_writer=pipeline_writer,
                inv_comps_writer=inv_comps_writer,
                occ_comps_writer=occ_comps_writer,
            )

            # Update report
            if result.status == ProcessingStatus.PROCESSED:
                report.successfully_processed += 1
                report.pipeline_rows_added += result.pipeline_rows_added
                if result.archive_folders:
                    report.emails_archived += 1
                report.brochures_parsed += result.brochures_parsed
                report.investment_comps_added += len(result.investment_comps)
                report.occupational_comps_added += len(result.occupational_comps)
            elif result.status == ProcessingStatus.ERROR:
                report.errors += 1
                report.error_details.append(
                    f"{summary.subject[:40]}: {result.error_message}"
                )

            # Mark as processed in database
            primary_deal = result.deals[0] if result.deals else None
            db.mark_processed(
                gmail_message_id=msg_id,
                subject=summary.subject,
                sender=summary.sender,
                sender_domain=summary.sender_domain,
                email_date=summary.date,
                status=result.status.value,
                is_introduction=result.is_introduction,
                classification_reason=classifications[msg_id].reason,
                deal_asset_name=", ".join(d.asset_name for d in result.deals) if result.deals else "",
                deal_town=", ".join(d.town for d in result.deals) if result.deals else "",
                archive_folder=result.archive_folders[0] if result.archive_folders else "",
                pipeline_row_added=result.pipeline_rows_added > 0,
                brochures_parsed=result.brochures_parsed,
                error_message=result.error_message,
                raw_extraction_json=json.dumps(
                    [_deal_to_dict(d) for d in result.deals] if result.deals else [],
                    default=str,
                ),
            )

        except Exception as e:
            logger.error("  Error processing %s: %s", msg_id, e)
            report.errors += 1
            report.error_details.append(f"{summary.subject[:40]}: {e}")

            db.mark_processed(
                gmail_message_id=msg_id,
                subject=summary.subject,
                sender=summary.sender,
                sender_domain=summary.sender_domain,
                email_date=summary.date,
                status="error",
                is_introduction=True,
                classification_reason=classifications[msg_id].reason,
                error_message=str(e),
            )

    # -----------------------------------------------------------------------
    # Step 6: Final report
    # -----------------------------------------------------------------------
    print(f"\n[Step 6/6] Processing complete!")

    return report


def _process_single_email(
    service,
    api_key: str,
    gmail_message_id: str,
    summary,
    archive_root: Path,
    pipeline_writer: Optional[PipelineWriter],
    inv_comps_writer: Optional[InvestmentCompsWriter],
    occ_comps_writer: Optional[OccupationalCompsWriter],
) -> ProcessingResult:
    """Process a single introduction email through the full pipeline.

    Parameters
    ----------
    service : Gmail API service.
    api_key : Anthropic API key.
    gmail_message_id : Gmail message ID.
    summary : EmailSummary from the scanner.
    archive_root : Archive folder root.
    pipeline_writer : Pipeline Excel writer (or None).
    inv_comps_writer : Investment comps writer (or None).
    occ_comps_writer : Occupational comps writer (or None).

    Returns
    -------
    ProcessingResult
    """
    result = ProcessingResult(
        gmail_message_id=gmail_message_id,
        status=ProcessingStatus.PENDING,
        is_introduction=True,
    )

    try:
        # 5a. Full classify + extract deal details
        print("    → Extracting deal details with AI...")
        msg = service.users().messages().get(
            userId="me", id=gmail_message_id, format="full"
        ).execute()

        payload = msg.get("payload", {})
        headers = _parse_headers(payload.get("headers", []))
        body_text = _get_body_text(payload)

        classification, deals = classify_and_extract(
            api_key=api_key,
            sender=headers.get("from", summary.sender),
            date=summary.date,
            subject=headers.get("subject", summary.subject),
            body=body_text,
            gmail_message_id=gmail_message_id,
        )

        if not deals:
            result.status = ProcessingStatus.ERROR
            result.error_message = "AI extraction returned no deal data"
            print("    ✗ No deal data extracted")
            return result

        result.deals = deals
        print(f"    ✓ {len(deals)} deal(s) extracted")
        for i, deal in enumerate(deals):
            name_display = deal.asset_name or "(no name)"
            print(f"      [{i+1}] {deal.town}, {name_display} ({deal.classification})")

        # 5b. Archive email + attachments — one folder per deal
        if archive_root:
            print("    → Archiving email and attachments...")
            for deal in deals:
                archive_folder = archive_email(
                    service=service,
                    gmail_message_id=gmail_message_id,
                    deal=deal,
                    archive_root=archive_root,
                    email_subject=summary.subject,
                    email_sender=summary.sender,
                    email_date=summary.date,
                )
                if archive_folder:
                    result.archive_folders.append(str(archive_folder))
                    print(f"    ✓ Archived to: {archive_folder.parent.name}/{archive_folder.name}")
                else:
                    print(f"    ✗ Archiving failed for {deal.asset_name or '(unknown)'}")

            # 5c. Parse brochure attachments from FIRST deal's archive folder only
            # (brochures typically describe the primary/first deal in the email)
            if result.archive_folders:
                primary_folder = Path(result.archive_folders[0])
                brochure_paths = get_attachment_paths(primary_folder)
                if brochure_paths:
                    print(f"    → Parsing {len(brochure_paths)} brochure(s)...")
                    for bp in brochure_paths:
                        try:
                            primary_deal = deals[0]
                            br = parse_brochure(
                                file_path=bp,
                                api_key=api_key,
                                source_deal=f"{primary_deal.town}, {primary_deal.asset_name}",
                            )
                            result.brochures_parsed += 1

                            # 5d. Merge brochure data into the FIRST deal only
                            if br.deal_extraction:
                                deals[0] = _merge_deals(deals[0], br.deal_extraction)
                                result.deals = deals
                                print(f"    ✓ Merged brochure data into primary deal")

                            result.investment_comps.extend(br.investment_comps)
                            result.occupational_comps.extend(br.occupational_comps)

                            if br.investment_comps:
                                print(f"    ✓ Found {len(br.investment_comps)} investment comps")
                            if br.occupational_comps:
                                print(f"    ✓ Found {len(br.occupational_comps)} occupational comps")

                        except Exception as e:
                            logger.warning("    Failed to parse brochure %s: %s", bp.name, e)
                else:
                    print("    — No brochure attachments found")

        # 5e. Append EACH deal to Pipeline Excel
        if pipeline_writer:
            print("    → Updating Pipeline Excel...")
            has_brochure = bool(result.archive_folders and result.brochures_parsed > 0)
            brochure_scraped = result.brochures_parsed > 0
            for i, deal in enumerate(deals):
                row_added = pipeline_writer.append_deal(
                    deal=deal,
                    has_brochure=has_brochure if i == 0 else False,  # brochure only for primary deal
                    brochure_scraped=brochure_scraped if i == 0 else False,
                    comment="Auto-imported",
                )
                if row_added:
                    result.pipeline_rows_added += 1
                    print(f"    ✓ Pipeline row added: {deal.asset_name or '(no name)'}")
                else:
                    print(f"    — Pipeline row skipped (duplicate): {deal.asset_name or '(no name)'}")

        # 5f. Append comparables to Excel files
        if inv_comps_writer and result.investment_comps:
            print(f"    → Writing {len(result.investment_comps)} investment comps...")
            count = inv_comps_writer.append_comps(result.investment_comps)
            print(f"    ✓ {count} investment comps written")

        if occ_comps_writer and result.occupational_comps:
            print(f"    → Writing {len(result.occupational_comps)} occupational comps...")
            count = occ_comps_writer.append_comps(result.occupational_comps)
            print(f"    ✓ {count} occupational comps written")

        result.status = ProcessingStatus.PROCESSED
        return result

    except Exception as e:
        result.status = ProcessingStatus.ERROR
        result.error_message = str(e)
        logger.error("    Error: %s", e)
        return result


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _merge_deals(email_deal: DealExtraction, brochure_deal: DealExtraction) -> DealExtraction:
    """Merge email and brochure deal data.

    Strategy: brochure wins for financial/property fields,
    email wins for date, agent, and metadata.
    """
    return DealExtraction(
        # Email wins for these
        date=email_deal.date,
        agent=email_deal.agent,
        raw_source="merged",
        # Brochure wins for property identification (if available)
        asset_name=brochure_deal.asset_name or email_deal.asset_name,
        country=brochure_deal.country or email_deal.country,
        town=brochure_deal.town or email_deal.town,
        address=brochure_deal.address or email_deal.address,
        postcode=brochure_deal.postcode or email_deal.postcode,
        classification=brochure_deal.classification or email_deal.classification,
        # Brochure wins for financials (if available)
        area_acres=brochure_deal.area_acres or email_deal.area_acres,
        area_sqft=brochure_deal.area_sqft or email_deal.area_sqft,
        rent_pa=brochure_deal.rent_pa or email_deal.rent_pa,
        rent_psf=brochure_deal.rent_psf or email_deal.rent_psf,
        asking_price=brochure_deal.asking_price or email_deal.asking_price,
        net_yield=brochure_deal.net_yield or email_deal.net_yield,
        reversionary_yield=brochure_deal.reversionary_yield or email_deal.reversionary_yield,
        capval_psf=brochure_deal.capval_psf or email_deal.capval_psf,
        # Use higher confidence
        confidence=max(email_deal.confidence, brochure_deal.confidence),
    )


def _deal_to_dict(deal: DealExtraction) -> dict:
    """Convert a DealExtraction to a dictionary."""
    return {
        "date": deal.date,
        "agent": deal.agent,
        "asset_name": deal.asset_name,
        "country": deal.country,
        "town": deal.town,
        "address": deal.address,
        "postcode": deal.postcode,
        "classification": deal.classification,
        "area_acres": deal.area_acres,
        "area_sqft": deal.area_sqft,
        "rent_pa": deal.rent_pa,
        "rent_psf": deal.rent_psf,
        "asking_price": deal.asking_price,
        "net_yield": deal.net_yield,
        "reversionary_yield": deal.reversionary_yield,
        "capval_psf": deal.capval_psf,
        "confidence": deal.confidence,
        "raw_source": deal.raw_source,
    }
