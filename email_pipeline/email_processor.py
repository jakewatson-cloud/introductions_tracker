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
from email_pipeline.deal_extractor import batch_classify, classify_and_extract, _extract_asset_name_from_subject
from email_pipeline.email_archiver import archive_email, build_archive_folder_name, get_attachment_paths
from email_pipeline.email_scanner import scan_emails, group_by_thread, _get_body_text, _parse_headers, _extract_sender_domain
from email_pipeline.brochure_parser import parse_brochure
from email_pipeline.excel_writer import (
    PipelineWriter,
    InvestmentCompsWriter,
    OccupationalCompsWriter,
    is_deal_match,
)
from email_pipeline.models import (
    ClassificationResult,
    DealExtraction,
    EmailSummary,
    ProcessingReport,
    ProcessingResult,
    ProcessingStatus,
    ThreadSummary,
)

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Cross-thread deduplication (within a single pipeline run)
# ---------------------------------------------------------------------------

# Subjects that clearly aren't property introductions (for stub guard)
STUB_DISQUALIFIERS = {
    "comps", "comparables", "comp", "market evidence",
    "rent review", "dilapidations", "service charge",
    "schedule of condition",
}


class _RunDeduplicator:
    """Track deals written in this run to catch cross-thread duplicates.

    Same deal can arrive via multiple Gmail threads (e.g. FW: from colleague,
    separate intro from a different agent, internal forward). Thread
    consolidation only groups by thread_id, so this catches the rest.
    """

    def __init__(self):
        self.seen: list[tuple[str, str, str]] = []  # (name, town, postcode)

    def check(self, deal: DealExtraction) -> tuple[bool, str]:
        """Check if deal matches any already written in this run.

        Returns (is_dup, reason).
        """
        name = deal.asset_name or ""
        town = deal.town or ""
        postcode = deal.postcode or ""

        for seen_name, seen_town, seen_postcode in self.seen:
            matched, reason = is_deal_match(
                name_a=name,
                town_a=town,
                postcode_a=postcode,
                name_b=seen_name,
                town_b=seen_town,
                postcode_b=seen_postcode,
            )
            if matched:
                return True, f'{reason} with "{seen_name}"'

        return False, ""

    def add(self, deal: DealExtraction):
        """Register a deal that was successfully written."""
        self.seen.append((
            deal.asset_name or "",
            deal.town or "",
            deal.postcode or "",
        ))


def _is_stub_disqualified(name: str) -> bool:
    """Check if a stub name suggests non-introduction content.

    Returns True for subjects like "Comps - Banbury", "FW: Comparables".
    """
    name_lower = name.lower()
    return any(term in name_lower for term in STUB_DISQUALIFIERS)


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
    # Step 3: Classify emails
    #   - Labelled emails (user-tagged) skip AI — they're confirmed introductions
    #   - Unlabelled emails go through batch_classify() with Claude API
    # -----------------------------------------------------------------------
    print(f"\n[Step 3/6] Classifying {len(new_summaries)} emails...")

    classifications: dict[str, ClassificationResult] = {}

    # Split: labelled emails are pre-confirmed, unlabelled need AI classification
    labelled = [s for s in new_summaries if s.matched_label]
    unlabelled = [s for s in new_summaries if not s.matched_label]

    # Pre-confirm labelled emails (no API call needed)
    if labelled:
        print(f"  {len(labelled)} email(s) have Gmail label — skipping AI classification")
        for s in labelled:
            classifications[s.gmail_message_id] = ClassificationResult(
                gmail_message_id=s.gmail_message_id,
                is_introduction=True,
                confidence=1.0,
                reason="User-tagged with Investment Introduction label",
            )

    # Batch classify unlabelled emails with Claude API
    if unlabelled:
        print(f"  Classifying {len(unlabelled)} unlabelled email(s) with AI...")

        batch_size = 10
        batches = []

        for i in range(0, len(unlabelled), batch_size):
            batch = unlabelled[i : i + batch_size]
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
        print(f"  Sending {num_batches} batch(es) to Claude API (concurrent)...")

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

    print(f"  Introductions: {len(intro_ids)} ({len(labelled)} labelled + {len(intro_ids) - len(labelled)} AI-classified)")
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
    # Step 5: Group by thread, then process each thread as one introduction
    # -----------------------------------------------------------------------
    intro_summaries = [s for s in new_summaries if s.gmail_message_id in intro_ids]
    threads = group_by_thread(intro_summaries)

    print(f"\n[Step 4/6] Processing {len(intro_ids)} introductions across {len(threads)} thread(s)...")

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

    # Cross-thread deduplicator: catches same deal across different Gmail threads
    run_dedup = _RunDeduplicator()

    for idx, thread in enumerate(threads, 1):
        best = _select_best_email(thread.emails)

        if thread.email_count > 1:
            print(f"\n  [{idx}/{len(threads)}] Thread: {thread.latest_subject[:60]} ({thread.email_count} emails)")
            print(f"    Best email: {best.subject[:60]}")
        else:
            print(f"\n  [{idx}/{len(threads)}] Processing: {best.subject[:60]}")

        try:
            result = _process_thread(
                service=service,
                api_key=api_key,
                thread=thread,
                best_email=best,
                archive_root=archive_root,
                pipeline_writer=pipeline_writer,
                inv_comps_writer=inv_comps_writer,
                occ_comps_writer=occ_comps_writer,
                run_dedup=run_dedup,
            )

            # Update report
            if result.status == ProcessingStatus.PROCESSED:
                report.successfully_processed += 1
                report.threads_processed += 1
                report.pipeline_rows_added += result.pipeline_rows_added
                report.emails_archived += len(result.archive_folders)
                report.brochures_parsed += result.brochures_parsed
                report.investment_comps_added += len(result.investment_comps)
                report.occupational_comps_added += len(result.occupational_comps)
            elif result.status == ProcessingStatus.ERROR:
                report.errors += 1
                report.error_details.append(
                    f"{thread.latest_subject[:40]}: {result.error_message}"
                )

            # Mark ALL emails in thread as processed in database
            for email in thread.emails:
                db.mark_processed(
                    gmail_message_id=email.gmail_message_id,
                    subject=email.subject,
                    sender=email.sender,
                    sender_domain=email.sender_domain,
                    email_date=email.date,
                    status=result.status.value,
                    is_introduction=result.is_introduction,
                    classification_reason=classifications.get(
                        email.gmail_message_id,
                        ClassificationResult(
                            gmail_message_id=email.gmail_message_id,
                            is_introduction=True,
                            confidence=0.0,
                            reason="Thread member",
                        ),
                    ).reason,
                    deal_asset_name=", ".join(d.asset_name or "" for d in result.deals) if result.deals else "",
                    deal_town=", ".join(d.town or "" for d in result.deals) if result.deals else "",
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
            logger.error("  Error processing thread %s: %s", thread.thread_id, e)
            report.errors += 1
            report.error_details.append(f"{thread.latest_subject[:40]}: {e}")

            for email in thread.emails:
                db.mark_processed(
                    gmail_message_id=email.gmail_message_id,
                    subject=email.subject,
                    sender=email.sender,
                    sender_domain=email.sender_domain,
                    email_date=email.date,
                    status="error",
                    is_introduction=True,
                    classification_reason=classifications.get(
                        email.gmail_message_id,
                        ClassificationResult(
                            gmail_message_id=email.gmail_message_id,
                            is_introduction=True,
                            confidence=0.0,
                            reason="Thread member",
                        ),
                    ).reason,
                    error_message=str(e),
                )

    # -----------------------------------------------------------------------
    # Step 6: Final report
    # -----------------------------------------------------------------------
    print(f"\n[Step 6/6] Processing complete!")

    return report


def _select_best_email(emails: list[EmailSummary]) -> EmailSummary:
    """Select the best email in a thread for deal extraction.

    Scoring: +3 per attachment, +1 per 1000 chars of body preview.
    Ties broken by earliest date (original introduction).
    """
    if len(emails) == 1:
        return emails[0]

    def _score(email: EmailSummary) -> float:
        return len(email.attachment_names) * 3 + len(email.body_preview) / 1000

    return max(emails, key=lambda e: (_score(e), -len(e.date)))


def _process_thread(
    service,
    api_key: str,
    thread: ThreadSummary,
    best_email: EmailSummary,
    archive_root: Path,
    pipeline_writer: Optional[PipelineWriter],
    inv_comps_writer: Optional[InvestmentCompsWriter],
    occ_comps_writer: Optional[OccupationalCompsWriter],
    run_dedup: Optional[_RunDeduplicator] = None,
) -> ProcessingResult:
    """Process all emails in a thread as a single introduction.

    1. Extract deals from the best email (most attachments/content)
    2. Archive ALL emails in the thread
    3. Parse brochures from ALL archive folders (deduped by filename)
    4. Write ONE pipeline row per deal (with cross-thread dedup)
    """
    all_ids = [e.gmail_message_id for e in thread.emails]
    result = ProcessingResult(
        gmail_message_id=best_email.gmail_message_id,
        status=ProcessingStatus.PENDING,
        is_introduction=True,
        all_gmail_message_ids=all_ids,
    )

    try:
        # --- Step 1: Extract deals from thread ---
        print("    → Extracting deal details with AI...")

        if thread.email_count == 1:
            # Single email thread: fetch and extract as before
            msg = service.users().messages().get(
                userId="me", id=best_email.gmail_message_id, format="full"
            ).execute()
            payload = msg.get("payload", {})
            headers = _parse_headers(payload.get("headers", []))
            body_text = _get_body_text(payload)
            primary_email = best_email
        else:
            # Multi-email thread: concatenate all bodies oldest-first
            # so Claude sees every deal mentioned across the conversation
            body_parts = []
            primary_email = thread.emails[0]  # earliest = original introduction

            for i, email in enumerate(thread.emails, 1):
                msg = service.users().messages().get(
                    userId="me", id=email.gmail_message_id, format="full"
                ).execute()
                payload = msg.get("payload", {})
                email_body = _get_body_text(payload)

                if i == 1:
                    headers = _parse_headers(payload.get("headers", []))

                label = "(oldest)" if i == 1 else "(newest)" if i == len(thread.emails) else ""
                body_parts.append(
                    f"=== Email {i} of {thread.email_count} {label} ===\n"
                    f"From: {email.sender}\n"
                    f"Date: {email.date}\n"
                    f"Subject: {email.subject}\n\n"
                    f"{email_body}"
                )

            body_text = "\n\n".join(body_parts)

        classification, deals = classify_and_extract(
            api_key=api_key,
            sender=headers.get("from", primary_email.sender),
            date=primary_email.date,
            subject=headers.get("subject", primary_email.subject),
            body=body_text,
            gmail_message_id=primary_email.gmail_message_id,
        )

        if not deals:
            any_has_attachments = any(e.has_attachments for e in thread.emails)
            if not classification.is_introduction and not any_has_attachments:
                result.status = ProcessingStatus.PROCESSED
                result.is_introduction = False
                print(f"    — Not an introduction after full analysis ({classification.reason})")
                return result

            if any_has_attachments:
                stub_name = _extract_asset_name_from_subject(
                    headers.get("subject", primary_email.subject)
                )

                # Guard: disqualify stubs that look like comps/reference data
                if _is_stub_disqualified(stub_name):
                    result.status = ProcessingStatus.PROCESSED
                    result.is_introduction = False
                    print(f"    — Stub disqualified (looks like comps/reference data): {stub_name}")
                    return result

                print(f"    — No deals in email body, but thread has attachments — stub: {stub_name}")
                deals = [DealExtraction(
                    date=primary_email.date,
                    agent=_extract_sender_domain(primary_email.sender) or "",
                    asset_name=stub_name,
                    raw_source="stub_from_subject",
                )]
            else:
                result.status = ProcessingStatus.ERROR
                result.error_message = "AI classified as introduction but extracted no deal data"
                print("    ✗ No deal data extracted")
                return result

        result.deals = deals
        print(f"    ✓ {len(deals)} deal(s) extracted")
        for i, deal in enumerate(deals):
            name_display = deal.asset_name or "(no name)"
            print(f"      [{i+1}] {deal.town}, {name_display} ({deal.classification})")

        # --- Step 2: Archive ALL emails in the thread ---
        # All emails in a thread go into the SAME dated subfolder (created from
        # the primary/first email).  This keeps Re:/Fwd: chains together.
        if archive_root:
            print(f"    → Archiving {len(thread.emails)} email(s)...")

            # Archive primary email first — this creates the subfolder
            primary_email_for_archive = thread.emails[0]  # oldest = original intro
            primary_subfolder = None

            for deal in deals:
                archive_folder = archive_email(
                    service=service,
                    gmail_message_id=primary_email_for_archive.gmail_message_id,
                    deal=deal,
                    archive_root=archive_root,
                    email_subject=primary_email_for_archive.subject,
                    email_sender=primary_email_for_archive.sender,
                    email_date=primary_email_for_archive.date,
                )
                if archive_folder:
                    primary_subfolder = archive_folder
                    result.archive_folders.append(str(archive_folder))
                    print(f"      ✓ {archive_folder.parent.name}/{archive_folder.name}")

            # Archive remaining thread emails INTO the same subfolder
            for email in thread.emails[1:]:
                if primary_subfolder and primary_subfolder.exists():
                    _archive_thread_email_to_subfolder(
                        service, email.gmail_message_id, primary_subfolder,
                        email.subject, email.sender, email.date, deals[0],
                    )
                    print(f"      ✓ {email.subject[:50]} → (same folder)")
                else:
                    # Fallback: archive independently if primary subfolder failed
                    for deal in deals:
                        archive_folder = archive_email(
                            service=service,
                            gmail_message_id=email.gmail_message_id,
                            deal=deal,
                            archive_root=archive_root,
                            email_subject=email.subject,
                            email_sender=email.sender,
                            email_date=email.date,
                        )
                        if archive_folder:
                            result.archive_folders.append(str(archive_folder))
                            print(f"      ✓ {archive_folder.parent.name}/{archive_folder.name}")

            # --- Step 3: Parse brochures from ALL archive folders (deduped by filename) ---
            if result.archive_folders:
                all_brochure_paths = []
                seen_filenames: set[str] = set()
                for folder_str in result.archive_folders:
                    for bp in get_attachment_paths(Path(folder_str)):
                        if bp.name not in seen_filenames:
                            all_brochure_paths.append(bp)
                            seen_filenames.add(bp.name)

                if all_brochure_paths:
                    print(f"    → Parsing {len(all_brochure_paths)} unique brochure(s)...")
                    for bp in all_brochure_paths:
                        try:
                            primary_deal = deals[0]
                            br = parse_brochure(
                                file_path=bp,
                                api_key=api_key,
                                source_deal=f"{primary_deal.town}, {primary_deal.asset_name}",
                            )
                            result.brochures_parsed += 1

                            if br.error_message:
                                print(f"    ⚠ Brochure: {br.error_message}")

                            if br.deal_extraction:
                                source_label = "brochure"
                                if br.deal_extraction.raw_source == "brochure_vision":
                                    source_label = "brochure (vision)"
                                deals[0] = _merge_deals(deals[0], br.deal_extraction)
                                result.deals = deals
                                print(f"    ✓ Merged {source_label} data into primary deal")

                            if br.investment_comps:
                                for comp in br.investment_comps:
                                    comp.source_deal = f"{primary_deal.town}, {primary_deal.asset_name}"
                                    comp.source_file_path = str(bp)
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

                # --- Step 3b: Rename property folder if we now have better data ---
                if result.archive_folders and deals:
                    _try_rename_property_folders(
                        archive_root=archive_root,
                        archive_folders=result.archive_folders,
                        primary_deal=deals[0],
                        result=result,
                    )

        # --- Step 4: Write ONE pipeline row per deal ---
        if pipeline_writer:
            print("    → Updating Pipeline Excel...")
            has_brochure = bool(result.archive_folders and result.brochures_parsed > 0)
            brochure_scraped = result.brochures_parsed > 0
            for i, deal in enumerate(deals):
                # Cross-thread dedup: check against deals already written in THIS run
                if run_dedup:
                    is_dup, dup_reason = run_dedup.check(deal)
                    if is_dup:
                        print(f"    — Pipeline row skipped (cross-thread duplicate — {dup_reason}): {deal.asset_name or '(no name)'}")
                        continue

                row_added = pipeline_writer.append_deal(
                    deal=deal,
                    has_brochure=has_brochure if i == 0 else False,
                    brochure_scraped=brochure_scraped if i == 0 else False,
                    comment="Auto-imported",
                )
                if row_added:
                    result.pipeline_rows_added += 1
                    if run_dedup:
                        run_dedup.add(deal)
                    print(f"    ✓ Pipeline row added: {deal.asset_name or '(no name)'}")
                else:
                    print(f"    — Pipeline row skipped (duplicate in Excel): {deal.asset_name or '(no name)'}")

        # --- Step 5: Write comparables ---
        if inv_comps_writer and result.investment_comps:
            print(f"    → Writing {len(result.investment_comps)} investment comps...")
            count = inv_comps_writer.append_comps(result.investment_comps)
            print(f"    ✓ {count} investment comps written")

        if occ_comps_writer and result.occupational_comps:
            print(f"    → Writing {len(result.occupational_comps)} occupational comps...")
            count = occ_comps_writer.append_comps(result.occupational_comps)
            print(f"    ✓ {count} occupational comps written")

        if thread.email_count > 1:
            print(f"    ✓ Thread complete: {thread.email_count} emails → {result.pipeline_rows_added} pipeline row(s)")

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

def _archive_thread_email_to_subfolder(
    service,
    gmail_message_id: str,
    subfolder: Path,
    subject: str,
    sender: str,
    date: str,
    deal: DealExtraction,
) -> None:
    """Archive a secondary thread email into an existing subfolder.

    Unlike archive_email() which creates a new subfolder, this puts the
    email body + attachments directly into an existing subfolder so all
    emails from one thread stay together.
    """
    import base64 as b64

    from email_pipeline.email_archiver import (
        _extract_full_body,
        _download_attachments,
        _sanitise_filename,
    )

    try:
        msg = service.users().messages().get(
            userId="me", id=gmail_message_id, format="full"
        ).execute()
        payload = msg.get("payload", {})

        # Save email body (append sender+date prefix so they don't overwrite)
        body_text = _extract_full_body(payload)
        safe_sender = _sanitise_filename(sender.split("<")[0].strip() or "unknown")
        body_filename = f"email_body - {safe_sender}.txt"
        body_path = subfolder / body_filename
        # Dedup filename
        counter = 2
        while body_path.exists():
            body_path = subfolder / f"email_body - {safe_sender} ({counter}).txt"
            counter += 1
        body_path.write_text(body_text, encoding="utf-8")

        # Save attachments (dedup handled by _download_attachments)
        _download_attachments(service, gmail_message_id, payload, subfolder)

        # Append to metadata.json if it exists
        metadata_path = subfolder / "metadata.json"
        if metadata_path.exists():
            try:
                metadata = json.loads(metadata_path.read_text(encoding="utf-8"))
                thread_emails = metadata.get("thread_emails", [])
                thread_emails.append({
                    "gmail_message_id": gmail_message_id,
                    "subject": subject,
                    "sender": sender,
                    "date": date,
                })
                metadata["thread_emails"] = thread_emails
                metadata_path.write_text(
                    json.dumps(metadata, indent=2, default=str),
                    encoding="utf-8",
                )
            except (json.JSONDecodeError, OSError):
                pass

    except Exception as e:
        logger.warning("    Failed to archive thread email %s: %s", gmail_message_id, e)


def _merge_deals(email_deal: DealExtraction, brochure_deal: DealExtraction) -> DealExtraction:
    """Merge email and brochure deal data.

    Strategy: brochure wins for financial/property fields,
    email wins for date, agent, asset name, and metadata.

    Exception: when the email deal is a stub (extracted from subject line only),
    the brochure's asset_name and town win because the stub data is minimal.
    """
    is_stub = email_deal.raw_source == "stub_from_subject"

    # For asset name: brochure wins if email is a stub OR has no name
    if is_stub and brochure_deal.asset_name:
        merged_asset_name = brochure_deal.asset_name
    else:
        merged_asset_name = email_deal.asset_name or brochure_deal.asset_name

    # For town: brochure wins if email is a stub OR has no town
    if is_stub and brochure_deal.town:
        merged_town = brochure_deal.town
    else:
        merged_town = brochure_deal.town or email_deal.town

    return DealExtraction(
        # Email wins for these
        date=email_deal.date,
        agent=email_deal.agent,
        raw_source="merged",
        asset_name=merged_asset_name,
        # Brochure wins for location/property identification (if available)
        country=brochure_deal.country or email_deal.country,
        town=merged_town,
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


def _try_rename_property_folders(
    archive_root: Path,
    archive_folders: list[str],
    primary_deal: DealExtraction,
    result: ProcessingResult,
) -> None:
    """Rename property folders if the merged deal data gives a better name.

    After brochure parsing, the primary deal may have richer data (town, asset
    name from brochure) than the initial email extraction that created the folder.
    This renames the property-level folder if the new name is substantially better.

    Only renames when:
    - The ideal name is different from the current name
    - The ideal name is longer (has more info) than the current name
    - The target folder doesn't already exist
    """
    ideal_name = build_archive_folder_name(primary_deal)
    if not ideal_name:
        return

    # Gather unique property-level folders (parents of the dated subfolders)
    seen_parents: set[str] = set()
    for folder_str in archive_folders:
        folder_path = Path(folder_str)
        parent = folder_path.parent  # property-level folder
        if str(parent) in seen_parents:
            continue
        seen_parents.add(str(parent))

        # Only rename if parent is directly under archive_root
        if parent.parent != archive_root:
            continue

        current_name = parent.name
        if current_name == ideal_name:
            continue

        # Only rename if the new name is strictly more informative
        # (longer, or has a comma where the old one doesn't — i.e. gained a town)
        has_more_info = (
            len(ideal_name) > len(current_name)
            or ("," in ideal_name and "," not in current_name)
        )
        if not has_more_info:
            continue

        target = archive_root / ideal_name
        if target.exists():
            continue

        try:
            parent.rename(target)
            # Update archive_folders references in the result
            old_prefix = str(parent)
            for i, af in enumerate(result.archive_folders):
                if af.startswith(old_prefix):
                    result.archive_folders[i] = af.replace(old_prefix, str(target), 1)
            print(f"    ✓ Renamed folder: {current_name} → {ideal_name}")
        except OSError as e:
            logger.warning("    Failed to rename folder %s → %s: %s", current_name, ideal_name, e)
