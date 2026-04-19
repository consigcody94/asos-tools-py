"""Pure-Python PDF export for O.W.L. reports (fpdf2).

Generates a NOAA-styled PDF with:

- Title block (O.W.L. branding)
- Executive summary (counts)
- Embedded chart PNG (from matplotlib report)
- Tabular status breakdown
- Footer with ISO UTC timestamp and data source chain

fpdf2 is pure-Python with zero system dependencies, which lets this
work on Hugging Face Spaces without a GTK/Cairo stack.
"""

from __future__ import annotations

import io
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import pandas as pd
from fpdf import FPDF

__all__ = ["build_watchlist_pdf", "build_report_pdf"]


_NAVY = (0, 51, 102)   # NOAA blue
_SLATE_700 = (51, 65, 85)
_SLATE_400 = (148, 163, 184)
_RED = (181, 9, 9)
_AMBER = (255, 190, 46)
_GREEN = (0, 169, 28)


class _OwlPDF(FPDF):
    def __init__(self, title: str = "O.W.L. Report"):
        super().__init__(orientation="P", unit="mm", format="Letter")
        self._owl_title = title
        self.set_auto_page_break(auto=True, margin=18)
        self.set_margins(left=16, top=14, right=16)

    def header(self):
        # Brand bar.
        self.set_fill_color(*_NAVY)
        self.rect(x=0, y=0, w=self.w, h=8, style="F")
        self.set_y(10)
        self.set_font("Helvetica", "B", 16)
        self.set_text_color(*_NAVY)
        self.cell(0, 7, "O.W.L.", new_x="LMARGIN", new_y="NEXT", align="L")
        self.set_font("Helvetica", "", 9)
        self.set_text_color(*_SLATE_400)
        self.cell(0, 4, "OBSERVATION WATCH LOG  ·  NATIONAL ASOS OPERATIONS",
                  new_x="LMARGIN", new_y="NEXT", align="L")
        self.ln(2)
        self.set_draw_color(*_NAVY)
        self.set_line_width(0.4)
        self.line(self.l_margin, self.get_y(), self.w - self.r_margin, self.get_y())
        self.ln(4)

    def footer(self):
        self.set_y(-14)
        self.set_font("Helvetica", "", 7)
        self.set_text_color(*_SLATE_400)
        ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
        self.cell(
            0, 4,
            f"Data Chain: NOAA / NCEI ASOS METAR > Iowa Environmental Mesonet > O.W.L.  -  Generated {ts}",
            align="C",
        )
        self.set_y(-9)
        self.cell(0, 4, f"Page {self.page_no()} / {{nb}}", align="C")

    def h2(self, text: str):
        self.set_font("Helvetica", "B", 13)
        self.set_text_color(*_NAVY)
        self.cell(0, 7, _sanitize(text), new_x="LMARGIN", new_y="NEXT")
        self.set_draw_color(*_NAVY)
        self.set_line_width(0.3)
        self.line(self.l_margin, self.get_y(), self.l_margin + 40, self.get_y())
        self.ln(3)

    def body(self, text: str):
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*_SLATE_700)
        self.multi_cell(0, 5, _sanitize(text))
        self.ln(2)

    def kv_row(self, k: str, v: str, *, color=None):
        self.set_font("Helvetica", "B", 9)
        self.set_text_color(*_SLATE_400)
        self.cell(55, 5.5, _sanitize(k), border=0)
        self.set_font("Helvetica", "", 10)
        self.set_text_color(*(color or _SLATE_700))
        self.cell(0, 5.5, _sanitize(v), new_x="LMARGIN", new_y="NEXT")


#: Unicode -> ASCII fallback map for characters fpdf2's Helvetica can't render.
_UNICODE_MAP = {
    "\u00b7": "-",  # middle dot
    "\u2022": "*",  # bullet
    "\u2013": "-",  # en dash
    "\u2014": "-",  # em dash
    "\u2018": "'", "\u2019": "'",
    "\u201c": '"', "\u201d": '"',
    "\u2026": "...",
    "\u203a": ">", "\u2039": "<",  # angle quotation marks
    "\u2190": "<-", "\u2192": "->",
    "\u2713": "ok", "\u2717": "x",
    "\u00b0": "deg",  # degree sign (Helvetica handles \u00b0, but keep safe)
}


def _sanitize(s: object) -> str:
    """fpdf2 Latin-1 safe text (no emoji, no stray unicode).

    First replace the common offenders with ASCII equivalents so
    ``Data Chain: X > Y`` stays readable, then fall back to latin-1
    coercion for anything else.
    """
    if s is None:
        return ""
    text = str(s)
    for k, v in _UNICODE_MAP.items():
        text = text.replace(k, v)
    try:
        return text.encode("latin-1", "replace").decode("latin-1")
    except Exception:
        return "?"


def build_watchlist_pdf(
    watchlist_df: pd.DataFrame,
    *,
    title: str = "O.W.L. — AOMC Watchlist",
    window_hours: int = 4,
    group_label: str = "All AOMC",
) -> bytes:
    """Render a watchlist DataFrame as a multi-page PDF.

    Returns the raw PDF bytes suitable for :func:`st.download_button`.
    """
    pdf = _OwlPDF(title=title)
    pdf.alias_nb_pages()
    pdf.add_page()

    pdf.h2(title)

    # --- Executive summary --------------------------------------------------
    cts = watchlist_df["status"].value_counts() if not watchlist_df.empty else pd.Series(dtype=int)
    total = int(len(watchlist_df))
    nf = int(cts.get("FLAGGED", 0))
    ni = int(cts.get("INTERMITTENT", 0))
    nr = int(cts.get("RECOVERED", 0))
    nm = int(cts.get("MISSING", 0)) + int(cts.get("NO DATA", 0))
    nc = int(cts.get("CLEAN", 0))
    health = 100.0 * nc / total if total else 0.0

    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%SZ")
    pdf.kv_row("Scan time (UTC)", _sanitize(ts))
    pdf.kv_row("Window", f"Last {window_hours} hours")
    pdf.kv_row("Scope", _sanitize(group_label))
    pdf.kv_row("Stations scanned", f"{total:,}")
    pdf.kv_row("Network health", f"{health:.1f}% clean",
               color=(_GREEN if health >= 85 else _AMBER if health >= 70 else _RED))
    pdf.ln(3)

    # --- Status tally -------------------------------------------------------
    pdf.h2("Status Tally")
    for label, n, color in [
        ("MISSING / NO DATA", nm, _RED),
        ("FLAGGED ($)", nf, _AMBER),
        ("INTERMITTENT", ni, _AMBER),
        ("RECOVERED", nr, (56, 189, 248)),
        ("CLEAN", nc, _GREEN),
    ]:
        # Color swatch box
        pdf.set_fill_color(*color)
        pdf.rect(pdf.get_x(), pdf.get_y() + 1, 3.5, 3.5, style="F")
        pdf.set_x(pdf.l_margin + 6)
        pdf.set_font("Helvetica", "B", 10)
        pdf.set_text_color(*_SLATE_700)
        pdf.cell(60, 6, _sanitize(label))
        pdf.set_font("Helvetica", "", 10)
        pdf.cell(0, 6, f"{n:,}", new_x="LMARGIN", new_y="NEXT")
    pdf.ln(2)

    # --- Detailed table: MISSING + FLAGGED ---------------------------------
    priority_df = watchlist_df[
        watchlist_df["status"].isin(["MISSING", "NO DATA", "FLAGGED", "INTERMITTENT"])
    ].copy() if not watchlist_df.empty else pd.DataFrame()

    if priority_df.empty:
        pdf.h2("Stations Requiring Attention")
        pdf.set_font("Helvetica", "I", 10)
        pdf.set_text_color(*_SLATE_400)
        pdf.cell(0, 6, "None. All scanned stations are CLEAN or RECOVERED.",
                 new_x="LMARGIN", new_y="NEXT")
    else:
        pdf.h2(f"Stations Requiring Attention ({len(priority_df)})")
        # Header
        pdf.set_fill_color(*_NAVY)
        pdf.set_text_color(255, 255, 255)
        pdf.set_font("Helvetica", "B", 8.5)
        widths = [20, 16, 28, 72, 38]
        headers = ["STATION", "ST", "STATUS", "REASON", "DETAIL"]
        for w, h in zip(widths, headers):
            pdf.cell(w, 6, h, border=0, fill=True, align="L")
        pdf.ln()
        # Rows
        pdf.set_font("Helvetica", "", 8.5)
        pdf.set_text_color(*_SLATE_700)
        alt = False
        for _, r in priority_df.iterrows():
            alt = not alt
            if alt:
                pdf.set_fill_color(241, 245, 249)
            else:
                pdf.set_fill_color(255, 255, 255)
            status = r.get("status", "")
            status_color = {
                "MISSING": _RED, "NO DATA": _RED,
                "FLAGGED": _AMBER, "INTERMITTENT": _AMBER,
            }.get(status, _SLATE_700)
            name = (r.get("name") or "").title()[:34]
            detail = ""
            if status in ("MISSING", "NO DATA"):
                g = r.get("missing") or 0
                detail = f"{int(g)} gaps"
            else:
                fg = r.get("flagged") or 0
                tot = r.get("total") or 0
                detail = f"{int(fg)}/{int(tot)} flagged"
            reason = (r.get("probable_reason") or "")[:30]
            cells = [
                (_sanitize(f"{r.get('station','')} · {name}"), _SLATE_700),
                (_sanitize(r.get("state") or ""), _SLATE_700),
                (_sanitize(status), status_color),
                (_sanitize(reason), _SLATE_700),
                (_sanitize(detail), _SLATE_700),
            ]
            for (txt, col), w in zip(cells, widths):
                pdf.set_text_color(*col)
                pdf.set_font("Helvetica", "B" if col != _SLATE_700 else "", 8.5)
                pdf.cell(w, 5.5, txt, border=0, fill=True, align="L")
            pdf.ln()
            if pdf.get_y() > pdf.h - 30:
                pdf.add_page()
                pdf.set_fill_color(*_NAVY)
                pdf.set_text_color(255, 255, 255)
                pdf.set_font("Helvetica", "B", 8.5)
                for w, h in zip(widths, headers):
                    pdf.cell(w, 6, h, border=0, fill=True, align="L")
                pdf.ln()

    return bytes(pdf.output())


def build_report_pdf(
    png_bytes: bytes,
    *,
    title: str,
    subtitle: str,
    body_text: str = "",
) -> bytes:
    """Wrap a report PNG in a NOAA-branded PDF wrapper."""
    pdf = _OwlPDF(title=title)
    pdf.alias_nb_pages()
    pdf.add_page()
    pdf.h2(_sanitize(title))
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*_SLATE_400)
    pdf.cell(0, 5, _sanitize(subtitle), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)
    if body_text:
        pdf.body(_sanitize(body_text))

    # Embed PNG (write to a temp file since fpdf needs a path).
    import tempfile
    with tempfile.NamedTemporaryFile(suffix=".png", delete=False) as f:
        f.write(png_bytes)
        tmp_path = f.name
    try:
        img_width = pdf.w - pdf.l_margin - pdf.r_margin
        pdf.image(tmp_path, w=img_width)
    finally:
        Path(tmp_path).unlink(missing_ok=True)

    return bytes(pdf.output())
