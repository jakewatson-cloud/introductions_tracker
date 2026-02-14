"""
Investment Email Pipeline — GUI
================================
Tkinter front-end for the introductions_tracker pipeline.

Launch with:
    python gui.py
"""

import importlib
import queue
import subprocess
import sys
import threading
import tkinter as tk
from datetime import datetime, timedelta
from pathlib import Path
from tkinter import filedialog, messagebox, scrolledtext, ttk

# Ensure project root is on the path
_PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(_PROJECT_ROOT))

import config as cfg
from email_pipeline.database import Database
from email_pipeline.gmail_auth import get_gmail_service


# ═══════════════════════════════════════════════════════════════════════════
# Threading helpers
# ═══════════════════════════════════════════════════════════════════════════

class StdoutRedirector:
    """Redirects writes to a queue for the GUI to pick up."""

    def __init__(self, log_queue: queue.Queue):
        self._queue = log_queue

    def write(self, text: str):
        if text and text.strip():
            self._queue.put(text)

    def flush(self):
        pass


def run_in_thread(target, result_queue: queue.Queue, log_queue: queue.Queue | None = None):
    """Run *target* in a daemon thread, capturing stdout if *log_queue* given.

    Puts ("success", result) or ("error", message) into *result_queue* when done.
    """

    def _worker():
        old_stdout = sys.stdout
        try:
            if log_queue is not None:
                sys.stdout = StdoutRedirector(log_queue)
            result = target()
            result_queue.put(("success", result))
        except Exception as exc:
            result_queue.put(("error", str(exc)))
        finally:
            if log_queue is not None:
                sys.stdout = old_stdout

    t = threading.Thread(target=_worker, daemon=True)
    t.start()
    return t


# ═══════════════════════════════════════════════════════════════════════════
# .env read / write helpers
# ═══════════════════════════════════════════════════════════════════════════

_ENV_PATH = _PROJECT_ROOT / ".env"

_ENV_KEYS = [
    "ANTHROPIC_API_KEY",
    "PIPELINE_EXCEL_PATH",
    "INVESTMENT_COMPS_PATH",
    "INTROS_ARCHIVE_PATH",
    "GMAIL_SCAN_LABEL",
    "GMAIL_PROCESSED_LABEL",
    "SENDER_WHITELIST",
    "EMAIL_KEYWORDS",
]


def load_env() -> dict[str, str]:
    """Read .env into {KEY: value} dict (ignores comments & blanks)."""
    result: dict[str, str] = {}
    if _ENV_PATH.exists():
        for line in _ENV_PATH.read_text(encoding="utf-8").splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#") and "=" in stripped:
                key, value = stripped.split("=", 1)
                result[key] = value
    return result


def save_env(updates: dict[str, str]) -> None:
    """Rewrite .env, replacing values for keys in *updates* while preserving
    comments, blank lines, and key ordering."""
    if not _ENV_PATH.exists():
        # Create from scratch
        lines = [f"{k}={v}\n" for k, v in updates.items()]
        _ENV_PATH.write_text("".join(lines), encoding="utf-8")
        return

    original_lines = _ENV_PATH.read_text(encoding="utf-8").splitlines(keepends=True)
    new_lines: list[str] = []
    seen_keys: set[str] = set()

    for line in original_lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key = stripped.split("=", 1)[0]
            if key in updates:
                new_lines.append(f"{key}={updates[key]}\n")
                seen_keys.add(key)
                continue
        new_lines.append(line if line.endswith("\n") else line + "\n")

    # Append any brand-new keys
    for key, value in updates.items():
        if key not in seen_keys:
            new_lines.append(f"{key}={value}\n")

    _ENV_PATH.write_text("".join(new_lines), encoding="utf-8")


# ═══════════════════════════════════════════════════════════════════════════
# Shared filter-row builder (reused by Scan and Process tabs)
# ═══════════════════════════════════════════════════════════════════════════

class FilterFrame(ttk.LabelFrame):
    """Reusable date-range / label / sender / keyword filter controls."""

    def __init__(self, parent, **kw):
        super().__init__(parent, text="Filters", padding=10, **kw)

        row = 0

        # Date range
        ttk.Label(self, text="After (YYYY-MM-DD):").grid(row=row, column=0, sticky="w")
        self.after_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.after_var, width=14).grid(row=row, column=1, sticky="w", padx=(4, 16))

        ttk.Label(self, text="Before (YYYY-MM-DD):").grid(row=row, column=2, sticky="w")
        self.before_var = tk.StringVar()
        ttk.Entry(self, textvariable=self.before_var, width=14).grid(row=row, column=3, sticky="w")
        row += 1

        # Label
        ttk.Label(self, text="Gmail Label:").grid(row=row, column=0, sticky="w", pady=(6, 0))
        self.label_var = tk.StringVar(value=cfg.get_gmail_scan_label())
        ttk.Entry(self, textvariable=self.label_var, width=30).grid(
            row=row, column=1, columnspan=3, sticky="w", padx=(4, 0), pady=(6, 0)
        )
        row += 1

        # Max results
        ttk.Label(self, text="Max results:").grid(row=row, column=0, sticky="w", pady=(6, 0))
        self.max_var = tk.StringVar(value="500")
        ttk.Entry(self, textvariable=self.max_var, width=8).grid(
            row=row, column=1, sticky="w", padx=(4, 0), pady=(6, 0)
        )
        row += 1

        # Use-config checkboxes
        self.use_senders_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(self, text="Use sender whitelist from .env", variable=self.use_senders_var).grid(
            row=row, column=0, columnspan=4, sticky="w", pady=(6, 0)
        )
        row += 1

        self.use_keywords_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(self, text="Use keyword list from .env", variable=self.use_keywords_var).grid(
            row=row, column=0, columnspan=4, sticky="w"
        )
        row += 1

        self.require_all_var = tk.BooleanVar(value=True)
        ttk.Checkbutton(
            self, text="Require all filters to match (AND mode)",
            variable=self.require_all_var,
        ).grid(row=row, column=0, columnspan=4, sticky="w", pady=(4, 0))

    # Convenience getters -------------------------------------------------

    def get_after(self) -> str | None:
        v = self.after_var.get().strip()
        return v if v else None

    def get_before(self) -> str | None:
        v = self.before_var.get().strip()
        return v if v else None

    def get_label(self) -> str | None:
        v = self.label_var.get().strip()
        return v if v else None

    def get_max(self) -> int:
        try:
            return int(self.max_var.get())
        except ValueError:
            return 500

    def get_senders(self) -> list[str]:
        return cfg.get_sender_whitelist() if self.use_senders_var.get() else []

    def get_keywords(self) -> list[str]:
        return cfg.get_email_keywords() if self.use_keywords_var.get() else []

    def get_require_all(self) -> bool:
        return self.require_all_var.get()

    def validate(self) -> str | None:
        """Return an error message, or None if valid."""
        for name, var in [("After", self.after_var), ("Before", self.before_var)]:
            v = var.get().strip()
            if v:
                try:
                    datetime.strptime(v, "%Y-%m-%d")
                except ValueError:
                    return f"{name} date must be YYYY-MM-DD (got '{v}')"
        return None

    def set_last_n_days(self, n: int):
        """Pre-fill date range for the last *n* days."""
        self.before_var.set("")
        self.after_var.set((datetime.now() - timedelta(days=n)).strftime("%Y-%m-%d"))


# ═══════════════════════════════════════════════════════════════════════════
# Gmail service singleton (lazy init)
# ═══════════════════════════════════════════════════════════════════════════

_gmail_service = None


def get_service():
    """Return cached Gmail API service, or authenticate on first call."""
    global _gmail_service
    if _gmail_service is None:
        _gmail_service = get_gmail_service(
            cfg.get_gmail_credentials_path(),
            cfg.get_gmail_token_path(),
        )
    return _gmail_service


def gmail_is_authorized() -> bool:
    return cfg.get_gmail_token_path().exists()


# ═══════════════════════════════════════════════════════════════════════════
# Tab 1 — Dashboard
# ═══════════════════════════════════════════════════════════════════════════

class DashboardTab(ttk.Frame):

    def __init__(self, parent, app: "PipelineGUI"):
        super().__init__(parent, padding=12)
        self.app = app
        self._build_ui()
        self.refresh()

    def _build_ui(self):
        # ── Stats cards ──────────────────────────────────────────────
        cards = ttk.LabelFrame(self, text="Statistics", padding=10)
        cards.pack(fill="x", pady=(0, 10))

        self._stat_labels: dict[str, tk.StringVar] = {}
        for i, (key, label) in enumerate([
            ("total_processed", "Total Processed"),
            ("introductions", "Introductions"),
            ("skipped", "Skipped"),
            ("errors", "Errors"),
            ("pipeline_rows_added", "Pipeline Rows"),
        ]):
            var = tk.StringVar(value="--")
            self._stat_labels[key] = var
            f = ttk.Frame(cards)
            f.grid(row=0, column=i, padx=16)
            ttk.Label(f, textvariable=var, font=("Segoe UI", 22, "bold")).pack()
            ttk.Label(f, text=label, font=("Segoe UI", 9)).pack()

        # ── Recent activity table ────────────────────────────────────
        table_frame = ttk.LabelFrame(self, text="Recent Activity", padding=6)
        table_frame.pack(fill="both", expand=True, pady=(0, 10))

        cols = ("date", "subject", "sender", "status", "asset")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        self.tree.heading("date", text="Date")
        self.tree.heading("subject", text="Subject")
        self.tree.heading("sender", text="Sender")
        self.tree.heading("status", text="Status")
        self.tree.heading("asset", text="Town / Asset")
        self.tree.column("date", width=90, stretch=False)
        self.tree.column("subject", width=320)
        self.tree.column("sender", width=200)
        self.tree.column("status", width=80, stretch=False)
        self.tree.column("asset", width=220)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        # ── Buttons ──────────────────────────────────────────────────
        btn_row = ttk.Frame(self)
        btn_row.pack(fill="x")
        ttk.Button(btn_row, text="Refresh", command=self.refresh).pack(side="left")
        ttk.Button(btn_row, text="Scan Last 7 Days", command=self._quick_scan).pack(side="left", padx=8)
        ttk.Button(btn_row, text="Process New Emails", command=self._quick_process).pack(side="left")

    # ── Data loading ─────────────────────────────────────────────────

    def refresh(self):
        db = Database(str(cfg.get_db_path()))
        stats = db.get_stats()
        for key, var in self._stat_labels.items():
            var.set(str(stats.get(key, 0)))

        self.tree.delete(*self.tree.get_children())
        for r in db.get_recent(20):
            date_str = (r.get("processed_at") or "")[:10]
            subject = (r.get("subject") or "")[:60]
            sender = r.get("sender_domain") or ""
            status = "Intro" if r.get("is_introduction") else r.get("status", "")
            asset = ""
            if r.get("deal_town") or r.get("deal_asset_name"):
                asset = f"{r.get('deal_town', '')}, {r.get('deal_asset_name', '')}"
            self.tree.insert("", "end", values=(date_str, subject, sender, status, asset))

    def _quick_scan(self):
        self.app.scan_tab.filters.set_last_n_days(7)
        self.app.notebook.select(1)

    def _quick_process(self):
        self.app.process_tab.filters.set_last_n_days(7)
        self.app.notebook.select(2)


# ═══════════════════════════════════════════════════════════════════════════
# Tab 2 — Scan
# ═══════════════════════════════════════════════════════════════════════════

class ScanTab(ttk.Frame):

    def __init__(self, parent, app: "PipelineGUI"):
        super().__init__(parent, padding=12)
        self.app = app
        self._result_queue: queue.Queue = queue.Queue()
        self._threads: list = []   # list[ThreadSummary]
        self._busy = False
        self._build_ui()

    def _build_ui(self):
        # Filters
        self.filters = FilterFrame(self)
        self.filters.pack(fill="x", pady=(0, 8))

        # Button row
        row = ttk.Frame(self)
        row.pack(fill="x", pady=(0, 8))
        self.scan_btn = ttk.Button(row, text="Scan Emails", command=self._on_scan)
        self.scan_btn.pack(side="left")
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(row, textvariable=self.status_var).pack(side="right")

        # ── Results table (thread-level) ──────────────────────────────
        table_frame = ttk.Frame(self)
        table_frame.pack(fill="both", expand=True, pady=(0, 6))

        cols = ("date", "sender", "subject", "attachments", "reason")
        self.tree = ttk.Treeview(table_frame, columns=cols, show="headings", height=12)
        self.tree.heading("date", text="Date")
        self.tree.heading("sender", text="Sender")
        self.tree.heading("subject", text="Subject")
        self.tree.heading("attachments", text="Attachments")
        self.tree.heading("reason", text="Match Reason")
        self.tree.column("date", width=90, stretch=False)
        self.tree.column("sender", width=180)
        self.tree.column("subject", width=340)
        self.tree.column("attachments", width=200, stretch=True)
        self.tree.column("reason", width=130, stretch=True)

        vsb = ttk.Scrollbar(table_frame, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill="both", expand=True)
        vsb.pack(side="right", fill="y")

        self.tree.bind("<<TreeviewSelect>>", self._on_select)

        # ── Detail panel (shows selected thread info) ─────────────────
        detail = ttk.LabelFrame(self, text="Selected Thread", padding=8)
        detail.pack(fill="x", pady=(0, 0))

        self._detail_vars = {}
        for i, (key, label) in enumerate([
            ("subject", "Subject:"),
            ("dates", "Dates:"),
            ("senders", "Senders:"),
            ("attachments", "Attachments:"),
            ("reasons", "Match Reasons:"),
        ]):
            ttk.Label(detail, text=label, font=("Segoe UI", 9, "bold")).grid(
                row=i, column=0, sticky="nw", padx=(0, 8), pady=1
            )
            var = tk.StringVar(value="")
            lbl = ttk.Label(detail, textvariable=var, wraplength=900)
            lbl.grid(row=i, column=1, sticky="w", pady=1)
            self._detail_vars[key] = var

        detail.columnconfigure(1, weight=1)

    # ── Scan logic ───────────────────────────────────────────────────

    def _on_scan(self):
        err = self.filters.validate()
        if err:
            messagebox.showerror("Invalid Filter", err)
            return
        if not gmail_is_authorized():
            messagebox.showwarning("Not Authorized", "Gmail is not authorized.\nRun setup_gmail_auth.py first (see Settings tab).")
            return

        self.scan_btn.config(state="disabled")
        self.status_var.set("Scanning...")
        self.tree.delete(*self.tree.get_children())
        self._clear_detail()

        from email_pipeline.email_scanner import scan_emails

        def worker():
            service = get_service()
            return scan_emails(
                service=service,
                after_date=self.filters.get_after(),
                before_date=self.filters.get_before(),
                label=self.filters.get_label(),
                sender_whitelist=self.filters.get_senders(),
                keywords=self.filters.get_keywords(),
                max_results=self.filters.get_max(),
                require_all_filters=self.filters.get_require_all(),
            )

        run_in_thread(worker, self._result_queue)
        self._poll()

    def _poll(self):
        try:
            status, data = self._result_queue.get_nowait()
            self.scan_btn.config(state="normal")
            if status == "success":
                self._display(data)
            else:
                self.status_var.set("Error")
                messagebox.showerror("Scan Error", data)
        except queue.Empty:
            self.after(100, self._poll)

    def _display(self, summaries):
        from email_pipeline.email_scanner import group_by_thread

        threads = group_by_thread(summaries)
        self._threads = threads

        for t in threads:
            try:
                dt = datetime.fromisoformat(t.latest_date)
                date_str = dt.strftime("%d/%m/%Y")
            except ValueError:
                date_str = t.latest_date[:12]

            # Sender: show primary domain(s)
            sender_str = ", ".join(t.all_sender_domains)

            # Subject: prefix with email count if > 1
            if t.email_count > 1:
                subj = f"[{t.email_count}] {t.latest_subject}"
            else:
                subj = t.latest_subject

            # Attachments: full list (no truncation — column stretches now)
            attach = ", ".join(t.all_attachment_names) if t.all_attachment_names else "-"

            # Match reasons
            reasons = []
            if t.matched_label:
                reasons.append("label")
            if t.matched_sender:
                reasons.append("sender")
            if t.matched_keywords:
                reasons.append("keyword")
            reason = " + ".join(reasons) or "query"

            self.tree.insert("", "end", values=(
                date_str,
                sender_str,
                subj[:80],
                attach[:60],
                reason,
            ))

        email_count = sum(t.email_count for t in threads)
        self.status_var.set(f"{email_count} emails in {len(threads)} threads")

    # ── Detail panel ─────────────────────────────────────────────────

    def _on_select(self, _event):
        sel = self.tree.selection()
        if not sel:
            self._clear_detail()
            return

        idx = self.tree.index(sel[0])
        if idx >= len(self._threads):
            return

        t = self._threads[idx]

        self._detail_vars["subject"].set(t.latest_subject)

        # Date range
        try:
            d1 = datetime.fromisoformat(t.earliest_date).strftime("%d/%m/%Y")
            d2 = datetime.fromisoformat(t.latest_date).strftime("%d/%m/%Y")
        except ValueError:
            d1 = t.earliest_date[:10]
            d2 = t.latest_date[:10]

        if t.email_count == 1:
            self._detail_vars["dates"].set(d1)
        else:
            self._detail_vars["dates"].set(f"{d1} to {d2}  ({t.email_count} emails)")

        # All senders (full addresses from individual emails)
        all_senders = list(dict.fromkeys(e.sender for e in t.emails))
        self._detail_vars["senders"].set("\n".join(all_senders))

        # All attachments (one per line)
        if t.all_attachment_names:
            self._detail_vars["attachments"].set("\n".join(t.all_attachment_names))
        else:
            self._detail_vars["attachments"].set("None")

        # Match reasons
        reasons = []
        if t.matched_label:
            reasons.append("Label matched")
        if t.matched_sender:
            reasons.append("Sender whitelisted")
        if t.matched_keywords:
            reasons.append(f"Keywords: {', '.join(t.matched_keywords)}")
        self._detail_vars["reasons"].set("; ".join(reasons) if reasons else "Gmail query only")

    def _clear_detail(self):
        for var in self._detail_vars.values():
            var.set("")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 3 — Process
# ═══════════════════════════════════════════════════════════════════════════

class ProcessTab(ttk.Frame):

    def __init__(self, parent, app: "PipelineGUI"):
        super().__init__(parent, padding=12)
        self.app = app
        self._result_queue: queue.Queue = queue.Queue()
        self._log_queue: queue.Queue = queue.Queue()
        self._build_ui()
        self._poll_log()

    def _build_ui(self):
        # Filters
        self.filters = FilterFrame(self)
        self.filters.pack(fill="x", pady=(0, 6))

        # Options
        opts = ttk.LabelFrame(self, text="Options", padding=6)
        opts.pack(fill="x", pady=(0, 6))
        self.dry_run_var = tk.BooleanVar(value=False)
        ttk.Checkbutton(opts, text="Dry run (classify only, no writes)", variable=self.dry_run_var).pack(anchor="w")

        # Button row
        row = ttk.Frame(self)
        row.pack(fill="x", pady=(0, 6))
        self.proc_btn = ttk.Button(row, text="Process Emails", command=self._on_process)
        self.proc_btn.pack(side="left")
        ttk.Button(row, text="Clear Log", command=self._clear_log).pack(side="left", padx=8)
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(row, textvariable=self.status_var).pack(side="right")

        # Log panel
        self.log = scrolledtext.ScrolledText(self, height=20, state="disabled", wrap="word",
                                             font=("Consolas", 9))
        self.log.pack(fill="both", expand=True)

    # ── Process logic ────────────────────────────────────────────────

    def _on_process(self):
        err = self.filters.validate()
        if err:
            messagebox.showerror("Invalid Filter", err)
            return
        if not gmail_is_authorized():
            messagebox.showwarning("Not Authorized", "Gmail is not authorized.\nRun setup_gmail_auth.py first (see Settings tab).")
            return
        api_key = cfg.get_anthropic_api_key()
        if not api_key:
            messagebox.showwarning("Missing API Key", "ANTHROPIC_API_KEY is not set.\nAdd it in the Settings tab.")
            return

        self.proc_btn.config(state="disabled")
        self.status_var.set("Processing...")
        self._clear_log()

        from email_pipeline.email_processor import process_emails

        db = Database(str(cfg.get_db_path()))

        def worker():
            return process_emails(
                service=get_service(),
                api_key=api_key,
                db=db,
                archive_root=cfg.get_intros_archive_path(),
                pipeline_excel_path=cfg.get_pipeline_excel_path(),
                investment_comps_path=cfg.get_investment_comps_path(),
                occupational_comps_path=cfg.get_occupational_comps_path(),
                after_date=self.filters.get_after(),
                before_date=self.filters.get_before(),
                label=self.filters.get_label(),
                sender_whitelist=self.filters.get_senders(),
                keywords=self.filters.get_keywords(),
                max_results=self.filters.get_max(),
                dry_run=self.dry_run_var.get(),
                auto_confirm=True,
                require_all_filters=self.filters.get_require_all(),
            )

        run_in_thread(worker, self._result_queue, log_queue=self._log_queue)
        self._poll_result()

    def _poll_result(self):
        try:
            status, data = self._result_queue.get_nowait()
            self.proc_btn.config(state="normal")
            if status == "success":
                self.status_var.set(f"Complete -- {data.successfully_processed} processed")
                self._append_log("\n" + data.summary() + "\n")
                self.app.dashboard_tab.refresh()
            else:
                self.status_var.set("Error")
                messagebox.showerror("Processing Error", data)
        except queue.Empty:
            self.after(150, self._poll_result)

    # ── Log panel helpers ────────────────────────────────────────────

    def _poll_log(self):
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self._append_log(msg if msg.endswith("\n") else msg + "\n")
        except queue.Empty:
            pass
        self.after(100, self._poll_log)

    def _append_log(self, text: str):
        self.log.config(state="normal")
        self.log.insert("end", text)
        self.log.see("end")
        self.log.config(state="disabled")

    def _clear_log(self):
        self.log.config(state="normal")
        self.log.delete("1.0", "end")
        self.log.config(state="disabled")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 4 — Brochure
# ═══════════════════════════════════════════════════════════════════════════

class BrochureTab(ttk.Frame):

    def __init__(self, parent, app: "PipelineGUI"):
        super().__init__(parent, padding=12)
        self.app = app
        self._result_queue: queue.Queue = queue.Queue()
        self._log_queue: queue.Queue = queue.Queue()
        self._build_ui()
        self._poll_log()

    def _build_ui(self):
        # File selection
        file_frame = ttk.LabelFrame(self, text="File Selection", padding=8)
        file_frame.pack(fill="x", pady=(0, 8))

        ttk.Label(file_frame, text="File:").grid(row=0, column=0, sticky="w")
        self.file_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.file_var, width=70).grid(row=0, column=1, padx=4)
        ttk.Button(file_frame, text="Browse...", command=self._browse).grid(row=0, column=2)

        ttk.Label(file_frame, text="Source Deal:").grid(row=1, column=0, sticky="w", pady=(6, 0))
        self.deal_var = tk.StringVar()
        ttk.Entry(file_frame, textvariable=self.deal_var, width=40).grid(row=1, column=1, sticky="w", padx=4, pady=(6, 0))

        # Options
        opts = ttk.LabelFrame(self, text="Extraction Options", padding=6)
        opts.pack(fill="x", pady=(0, 8))

        self.extract_deal_var = tk.BooleanVar(value=True)
        self.extract_inv_var = tk.BooleanVar(value=True)
        self.extract_occ_var = tk.BooleanVar(value=True)
        self.write_var = tk.BooleanVar(value=False)

        ttk.Checkbutton(opts, text="Extract deal details", variable=self.extract_deal_var).pack(anchor="w")
        ttk.Checkbutton(opts, text="Extract investment comparables", variable=self.extract_inv_var).pack(anchor="w")
        ttk.Checkbutton(opts, text="Extract occupational comparables", variable=self.extract_occ_var).pack(anchor="w")
        ttk.Checkbutton(opts, text="Write results to Excel files", variable=self.write_var).pack(anchor="w", pady=(4, 0))

        # Button row
        row = ttk.Frame(self)
        row.pack(fill="x", pady=(0, 8))
        self.parse_btn = ttk.Button(row, text="Parse Brochure", command=self._on_parse)
        self.parse_btn.pack(side="left")
        self.status_var = tk.StringVar(value="Ready")
        ttk.Label(row, textvariable=self.status_var).pack(side="right")

        # Results
        self.results = scrolledtext.ScrolledText(self, height=18, state="disabled", wrap="word",
                                                 font=("Consolas", 9))
        self.results.pack(fill="both", expand=True)

    # ── Actions ──────────────────────────────────────────────────────

    def _browse(self):
        path = filedialog.askopenfilename(
            title="Select brochure file",
            filetypes=[
                ("Brochure files", "*.pdf *.xlsx *.xls"),
                ("PDF files", "*.pdf"),
                ("Excel files", "*.xlsx *.xls"),
                ("All files", "*.*"),
            ],
        )
        if path:
            self.file_var.set(path)
            if not self.deal_var.get():
                self.deal_var.set(Path(path).stem)

    def _on_parse(self):
        fpath = self.file_var.get().strip()
        if not fpath or not Path(fpath).exists():
            messagebox.showerror("No File", "Please select a valid brochure file.")
            return

        api_key = cfg.get_anthropic_api_key()
        if not api_key:
            messagebox.showwarning("Missing API Key", "ANTHROPIC_API_KEY is not set.\nAdd it in the Settings tab.")
            return

        self.parse_btn.config(state="disabled")
        self.status_var.set("Parsing...")
        self._clear_results()

        from email_pipeline.brochure_parser import parse_brochure

        def worker():
            return parse_brochure(
                file_path=Path(fpath),
                api_key=api_key,
                source_deal=self.deal_var.get() or Path(fpath).stem,
                extract_deal=self.extract_deal_var.get(),
                extract_investment_comps=self.extract_inv_var.get(),
                extract_occupational_comps=self.extract_occ_var.get(),
            )

        run_in_thread(worker, self._result_queue, log_queue=self._log_queue)
        self._poll_result()

    def _poll_result(self):
        try:
            status, data = self._result_queue.get_nowait()
            self.parse_btn.config(state="normal")
            if status == "success":
                self._display_result(data)
                self._maybe_write_excel(data)
                self.status_var.set("Complete")
            else:
                self.status_var.set("Error")
                messagebox.showerror("Parse Error", data)
        except queue.Empty:
            self.after(150, self._poll_result)

    def _display_result(self, result):
        lines: list[str] = []

        if result.deal_extraction:
            d = result.deal_extraction
            lines.append("Deal Details")
            lines.append("=" * 50)
            lines.append(f"  Asset:          {d.asset_name}")
            lines.append(f"  Town:           {d.town}")
            lines.append(f"  Address:        {d.address}")
            lines.append(f"  Classification: {d.classification}")
            if d.area_sqft:
                lines.append(f"  Area:           {d.area_sqft:,.0f} sqft")
            if d.rent_pa:
                lines.append(f"  Rent PA:        {d.rent_pa:,.0f}")
            if d.asking_price:
                lines.append(f"  Asking Price:   {d.asking_price:,.0f}")
            if d.net_yield:
                lines.append(f"  NIY:            {d.net_yield:.2f}%")
            lines.append(f"  Confidence:     {d.confidence:.0%}")
            lines.append("")

        if result.investment_comps:
            lines.append(f"Investment Comparables ({len(result.investment_comps)})")
            lines.append("-" * 50)
            for i, c in enumerate(result.investment_comps, 1):
                price = f"{c.price:,.0f}" if c.price else "N/A"
                yld = f"{c.yield_niy:.2f}%" if c.yield_niy else "N/A"
                lines.append(f"  {i}. {c.town}, {c.address} -- {price} @ {yld}")
            lines.append("")

        if result.occupational_comps:
            lines.append(f"Occupational Comparables ({len(result.occupational_comps)})")
            lines.append("-" * 50)
            for i, c in enumerate(result.occupational_comps, 1):
                rent = f"{c.rent_pa:,.0f} pa" if c.rent_pa else "N/A"
                size = f"{c.size_sqft:,.0f} sqft" if c.size_sqft else "N/A"
                lines.append(f"  {i}. {c.tenant_name} -- {size} @ {rent}")
            lines.append("")

        if result.error_message:
            lines.append(f"Error: {result.error_message}")

        if not lines:
            lines.append("No data extracted from this file.")

        self.results.config(state="normal")
        self.results.insert("end", "\n".join(lines))
        self.results.config(state="disabled")

    def _maybe_write_excel(self, result):
        if not self.write_var.get():
            return

        from email_pipeline.excel_writer import InvestmentCompsWriter, OccupationalCompsWriter

        written: list[str] = []

        if result.investment_comps:
            inv_path = cfg.get_investment_comps_path()
            if inv_path and inv_path.exists():
                count = InvestmentCompsWriter(inv_path).append_comps(result.investment_comps)
                written.append(f"{count} investment comps -> {inv_path.name}")

        if result.occupational_comps:
            occ_path = cfg.get_occupational_comps_path()
            if occ_path:
                count = OccupationalCompsWriter(occ_path).append_comps(result.occupational_comps)
                written.append(f"{count} occupational comps -> {occ_path.name}")

        if written:
            self._append_result("\n\nExcel writes:\n  " + "\n  ".join(written))

    # ── helpers ──────────────────────────────────────────────────────

    def _poll_log(self):
        try:
            while True:
                msg = self._log_queue.get_nowait()
                self._append_result(msg if msg.endswith("\n") else msg + "\n")
        except queue.Empty:
            pass
        self.after(100, self._poll_log)

    def _append_result(self, text: str):
        self.results.config(state="normal")
        self.results.insert("end", text)
        self.results.see("end")
        self.results.config(state="disabled")

    def _clear_results(self):
        self.results.config(state="normal")
        self.results.delete("1.0", "end")
        self.results.config(state="disabled")


# ═══════════════════════════════════════════════════════════════════════════
# Tab 5 — Settings
# ═══════════════════════════════════════════════════════════════════════════

class SettingsTab(ttk.Frame):

    def __init__(self, parent, app: "PipelineGUI"):
        super().__init__(parent, padding=12)
        self.app = app
        self._entries: dict[str, tk.Widget] = {}
        self._build_ui()
        self._load()

    def _build_ui(self):
        canvas = tk.Canvas(self, highlightthickness=0)
        scrollbar = ttk.Scrollbar(self, orient="vertical", command=canvas.yview)
        inner = ttk.Frame(canvas, padding=4)

        inner.bind("<Configure>", lambda e: canvas.configure(scrollregion=canvas.bbox("all")))
        canvas.create_window((0, 0), window=inner, anchor="nw")
        canvas.configure(yscrollcommand=scrollbar.set)

        canvas.pack(side="left", fill="both", expand=True)
        scrollbar.pack(side="right", fill="y")

        row = 0

        def _add_entry(label: str, key: str, show: str = "", browse: str = ""):
            nonlocal row
            ttk.Label(inner, text=label).grid(row=row, column=0, sticky="w", pady=(8, 0))
            row += 1
            entry = ttk.Entry(inner, width=80, show=show)
            entry.grid(row=row, column=0, sticky="ew", columnspan=2 if not browse else 1)
            self._entries[key] = entry
            if browse:
                btn_cmd = (
                    (lambda k=key: self._browse_file(k))
                    if browse == "file"
                    else (lambda k=key: self._browse_dir(k))
                )
                ttk.Button(inner, text="Browse...", command=btn_cmd).grid(row=row, column=2 if not browse else 1, padx=4)
            row += 1

        def _add_text(label: str, key: str, height: int = 3):
            nonlocal row
            ttk.Label(inner, text=label).grid(row=row, column=0, sticky="w", pady=(8, 0))
            row += 1
            txt = tk.Text(inner, width=80, height=height, wrap="word", font=("Segoe UI", 9))
            txt.grid(row=row, column=0, columnspan=2, sticky="ew")
            self._entries[key] = txt
            row += 1

        _add_entry("Anthropic API Key:", "ANTHROPIC_API_KEY", show="*")
        _add_entry("Pipeline Excel Path:", "PIPELINE_EXCEL_PATH", browse="file")
        _add_entry("Investment Comps Path:", "INVESTMENT_COMPS_PATH", browse="file")
        _add_entry("Intros Archive Path:", "INTROS_ARCHIVE_PATH", browse="dir")
        _add_entry("Gmail Scan Label:", "GMAIL_SCAN_LABEL")
        _add_entry("Gmail Processed Label:", "GMAIL_PROCESSED_LABEL")
        _add_text("Sender Whitelist (comma-separated):", "SENDER_WHITELIST")
        _add_text("Email Keywords (comma-separated):", "EMAIL_KEYWORDS")

        # Gmail auth status
        ttk.Separator(inner, orient="horizontal").grid(row=row, column=0, columnspan=3, sticky="ew", pady=12)
        row += 1

        auth_frame = ttk.Frame(inner)
        auth_frame.grid(row=row, column=0, columnspan=3, sticky="w")
        row += 1

        self.auth_var = tk.StringVar()
        ttk.Label(auth_frame, text="Gmail Auth:").pack(side="left")
        ttk.Label(auth_frame, textvariable=self.auth_var, font=("Segoe UI", 9, "bold")).pack(side="left", padx=6)
        ttk.Button(auth_frame, text="Re-authorize Gmail", command=self._reauth).pack(side="left", padx=8)
        ttk.Button(auth_frame, text="Test Connection", command=self._test_connection).pack(side="left")

        # Save / Reset
        ttk.Separator(inner, orient="horizontal").grid(row=row, column=0, columnspan=3, sticky="ew", pady=12)
        row += 1

        btn_frame = ttk.Frame(inner)
        btn_frame.grid(row=row, column=0, columnspan=3, sticky="w")
        ttk.Button(btn_frame, text="Save Settings", command=self._save).pack(side="left")
        ttk.Button(btn_frame, text="Reload from .env", command=self._load).pack(side="left", padx=8)

        inner.columnconfigure(0, weight=1)

    # ── Data ─────────────────────────────────────────────────────────

    def _load(self):
        env = load_env()
        for key, widget in self._entries.items():
            if isinstance(widget, tk.Text):
                widget.delete("1.0", "end")
                widget.insert("1.0", env.get(key, ""))
            else:
                widget.delete(0, "end")
                widget.insert(0, env.get(key, ""))

        self.auth_var.set("Authorized" if gmail_is_authorized() else "Not authorized")

    def _save(self):
        updates: dict[str, str] = {}
        for key, widget in self._entries.items():
            if isinstance(widget, tk.Text):
                updates[key] = widget.get("1.0", "end").strip()
            else:
                updates[key] = widget.get().strip()

        try:
            save_env(updates)
            importlib.reload(cfg)
            messagebox.showinfo("Saved", "Settings saved to .env")
        except Exception as exc:
            messagebox.showerror("Save Error", str(exc))

    # ── Browse helpers ───────────────────────────────────────────────

    def _browse_file(self, key: str):
        path = filedialog.askopenfilename(
            title=f"Select file for {key}",
            filetypes=[("Excel files", "*.xlsx *.xls"), ("All files", "*.*")],
        )
        if path:
            w = self._entries[key]
            w.delete(0, "end")
            w.insert(0, path)

    def _browse_dir(self, key: str):
        path = filedialog.askdirectory(title=f"Select folder for {key}")
        if path:
            w = self._entries[key]
            w.delete(0, "end")
            w.insert(0, path)

    # ── Gmail ────────────────────────────────────────────────────────

    def _reauth(self):
        script = _PROJECT_ROOT / "setup_gmail_auth.py"
        if not script.exists():
            messagebox.showerror("Missing File", f"setup_gmail_auth.py not found at:\n{script}")
            return
        subprocess.Popen([sys.executable, str(script)])
        messagebox.showinfo(
            "Gmail Authorization",
            "A browser window should open for authorization.\n"
            "Return here and click 'Test Connection' when done.",
        )

    def _test_connection(self):
        if not gmail_is_authorized():
            self.auth_var.set("Not authorized")
            messagebox.showwarning("Not Authorized", "token.json not found.\nPlease authorize Gmail first.")
            return
        try:
            global _gmail_service
            _gmail_service = None  # force re-auth
            service = get_service()
            result = service.users().labels().list(userId="me").execute()
            label_count = len(result.get("labels", []))
            self.auth_var.set("Authorized")
            messagebox.showinfo("Connection OK", f"Gmail connection successful.\n({label_count} labels found)")
        except Exception as exc:
            self.auth_var.set("Error")
            messagebox.showerror("Connection Failed", str(exc))


# ═══════════════════════════════════════════════════════════════════════════
# Main window
# ═══════════════════════════════════════════════════════════════════════════

class PipelineGUI(tk.Tk):

    def __init__(self):
        super().__init__()
        self.title("Investment Email Pipeline")
        self.geometry("1200x800")
        self.minsize(1000, 600)

        # Windows-native theme
        style = ttk.Style(self)
        available = style.theme_names()
        for theme in ("vista", "winnative", "clam"):
            if theme in available:
                style.theme_use(theme)
                break

        # Notebook
        self.notebook = ttk.Notebook(self)
        self.notebook.pack(fill="both", expand=True, padx=6, pady=6)

        self.dashboard_tab = DashboardTab(self.notebook, self)
        self.scan_tab = ScanTab(self.notebook, self)
        self.process_tab = ProcessTab(self.notebook, self)
        self.brochure_tab = BrochureTab(self.notebook, self)
        self.settings_tab = SettingsTab(self.notebook, self)

        self.notebook.add(self.dashboard_tab, text="  Dashboard  ")
        self.notebook.add(self.scan_tab, text="  Scan  ")
        self.notebook.add(self.process_tab, text="  Process  ")
        self.notebook.add(self.brochure_tab, text="  Brochure  ")
        self.notebook.add(self.settings_tab, text="  Settings  ")


# ═══════════════════════════════════════════════════════════════════════════
# Entry point
# ═══════════════════════════════════════════════════════════════════════════

def main():
    app = PipelineGUI()
    app.mainloop()


if __name__ == "__main__":
    main()
