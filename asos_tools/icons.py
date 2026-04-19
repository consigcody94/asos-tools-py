"""Inline SVG icon helper — transparent, theme-aware, zero emoji.

The O.W.L. UI uses no emojis. Icons are rendered as inline SVG so they:

* scale lossless at any size (crisp on 4K displays),
* inherit the surrounding text color via ``currentColor``,
* respect dark/light mode automatically,
* cost nothing in network (no asset requests),
* have zero external dependencies.

Iconography is Lucide-style (24x24 viewBox, 2px stroke, round joins) —
a minimal NOC/federal aesthetic that reads at small sizes.

Usage
-----
>>> from asos_tools.icons import icon
>>> st.markdown(icon("activity", size=16), unsafe_allow_html=True)

Or inline inside larger HTML:

>>> badge = f'<span class="badge">{icon("check-circle", size=14)} OK</span>'
>>> st.html(badge)
"""

from __future__ import annotations

__all__ = ["icon", "ICONS"]

# Each value is the <path> (or multi-element) SVG body at a 24x24 viewBox,
# stroke-only, stroke-width=2. Fill is always "none" on paths so the host
# can set color via CSS currentColor.

ICONS: dict[str, str] = {
    # --- Status / indicators ---
    "activity": (
        '<polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>'
    ),
    "check-circle": (
        '<path d="M22 11.08V12a10 10 0 1 1-5.93-9.14"/>'
        '<polyline points="22 4 12 14.01 9 11.01"/>'
    ),
    "alert-triangle": (
        '<path d="M10.29 3.86 1.82 18a2 2 0 0 0 1.71 3h16.94a2 2 0 0 0 1.71-3L13.71 3.86a2 2 0 0 0-3.42 0z"/>'
        '<line x1="12" y1="9" x2="12" y2="13"/>'
        '<line x1="12" y1="17" x2="12.01" y2="17"/>'
    ),
    "alert-octagon": (
        '<polygon points="7.86 2 16.14 2 22 7.86 22 16.14 16.14 22 7.86 22 2 16.14 2 7.86 7.86 2"/>'
        '<line x1="12" y1="8" x2="12" y2="12"/>'
        '<line x1="12" y1="16" x2="12.01" y2="16"/>'
    ),
    "help-circle": (
        '<circle cx="12" cy="12" r="10"/>'
        '<path d="M9.09 9a3 3 0 0 1 5.83 1c0 2-3 3-3 3"/>'
        '<line x1="12" y1="17" x2="12.01" y2="17"/>'
    ),
    # --- Data / telemetry ---
    "radio": (
        '<circle cx="12" cy="12" r="2"/>'
        '<path d="M16.24 7.76a6 6 0 0 1 0 8.49m-8.48-.01a6 6 0 0 1 0-8.49m11.31-2.82a10 10 0 0 1 0 14.14m-14.14 0a10 10 0 0 1 0-14.14"/>'
    ),
    "wifi-off": (
        '<line x1="1" y1="1" x2="23" y2="23"/>'
        '<path d="M16.72 11.06A10.94 10.94 0 0 1 19 12.55"/>'
        '<path d="M5 12.55a10.94 10.94 0 0 1 5.17-2.39"/>'
        '<path d="M10.71 5.05A16 16 0 0 1 22.58 9"/>'
        '<path d="M1.42 9a15.91 15.91 0 0 1 4.7-2.88"/>'
        '<path d="M8.53 16.11a6 6 0 0 1 6.95 0"/>'
        '<line x1="12" y1="20" x2="12.01" y2="20"/>'
    ),
    "database": (
        '<ellipse cx="12" cy="5" rx="9" ry="3"/>'
        '<path d="M21 12c0 1.66-4 3-9 3s-9-1.34-9-3"/>'
        '<path d="M3 5v14c0 1.66 4 3 9 3s9-1.34 9-3V5"/>'
    ),
    "server": (
        '<rect x="2" y="2" width="20" height="8" rx="2" ry="2"/>'
        '<rect x="2" y="14" width="20" height="8" rx="2" ry="2"/>'
        '<line x1="6" y1="6" x2="6.01" y2="6"/>'
        '<line x1="6" y1="18" x2="6.01" y2="18"/>'
    ),
    # --- Navigation / layout ---
    "globe": (
        '<circle cx="12" cy="12" r="10"/>'
        '<line x1="2" y1="12" x2="22" y2="12"/>'
        '<path d="M12 2a15.3 15.3 0 0 1 4 10 15.3 15.3 0 0 1-4 10 15.3 15.3 0 0 1-4-10 15.3 15.3 0 0 1 4-10z"/>'
    ),
    "map": (
        '<polygon points="1 6 1 22 8 18 16 22 23 18 23 2 16 6 8 2 1 6"/>'
        '<line x1="8" y1="2" x2="8" y2="18"/>'
        '<line x1="16" y1="6" x2="16" y2="22"/>'
    ),
    "layers": (
        '<polygon points="12 2 2 7 12 12 22 7 12 2"/>'
        '<polyline points="2 17 12 22 22 17"/>'
        '<polyline points="2 12 12 17 22 12"/>'
    ),
    "list": (
        '<line x1="8" y1="6" x2="21" y2="6"/>'
        '<line x1="8" y1="12" x2="21" y2="12"/>'
        '<line x1="8" y1="18" x2="21" y2="18"/>'
        '<line x1="3" y1="6" x2="3.01" y2="6"/>'
        '<line x1="3" y1="12" x2="3.01" y2="12"/>'
        '<line x1="3" y1="18" x2="3.01" y2="18"/>'
    ),
    "grid": (
        '<rect x="3" y="3" width="7" height="7"/>'
        '<rect x="14" y="3" width="7" height="7"/>'
        '<rect x="14" y="14" width="7" height="7"/>'
        '<rect x="3" y="14" width="7" height="7"/>'
    ),
    # --- Actions ---
    "refresh": (
        '<polyline points="23 4 23 10 17 10"/>'
        '<polyline points="1 20 1 14 7 14"/>'
        '<path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/>'
    ),
    "download": (
        '<path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/>'
        '<polyline points="7 10 12 15 17 10"/>'
        '<line x1="12" y1="15" x2="12" y2="3"/>'
    ),
    "search": (
        '<circle cx="11" cy="11" r="8"/>'
        '<line x1="21" y1="21" x2="16.65" y2="16.65"/>'
    ),
    "settings": (
        '<circle cx="12" cy="12" r="3"/>'
        '<path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1 0 2.83 2 2 0 0 1-2.83 0l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83 0 2 2 0 0 1 0-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 0-2.83 2 2 0 0 1 2.83 0l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 0 2 2 0 0 1 0 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/>'
    ),
    "play": '<polygon points="5 3 19 12 5 21 5 3"/>',
    "pause": (
        '<rect x="6" y="4" width="4" height="16"/>'
        '<rect x="14" y="4" width="4" height="16"/>'
    ),
    "external-link": (
        '<path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/>'
        '<polyline points="15 3 21 3 21 9"/>'
        '<line x1="10" y1="14" x2="21" y2="3"/>'
    ),
    # --- Weather-domain ---
    "cloud": (
        '<path d="M18 10h-1.26A8 8 0 1 0 9 20h9a5 5 0 0 0 0-10z"/>'
    ),
    "cloud-rain": (
        '<line x1="16" y1="13" x2="16" y2="21"/>'
        '<line x1="8" y1="13" x2="8" y2="21"/>'
        '<line x1="12" y1="15" x2="12" y2="23"/>'
        '<path d="M20 16.58A5 5 0 0 0 18 7h-1.26A8 8 0 1 0 4 15.25"/>'
    ),
    "cloud-lightning": (
        '<path d="M19 16.9A5 5 0 0 0 18 7h-1.26a8 8 0 1 0-11.62 9"/>'
        '<polyline points="13 11 9 17 15 17 11 23"/>'
    ),
    "wind": (
        '<path d="M9.59 4.59A2 2 0 1 1 11 8H2m10.59 11.41A2 2 0 1 0 14 16H2m15.73-8.27A2.5 2.5 0 1 1 19.5 12H2"/>'
    ),
    "thermometer": (
        '<path d="M14 14.76V3.5a2.5 2.5 0 0 0-5 0v11.26a4.5 4.5 0 1 0 5 0z"/>'
    ),
    "eye": (
        '<path d="M1 12s4-8 11-8 11 8 11 8-4 8-11 8-11-8-11-8z"/>'
        '<circle cx="12" cy="12" r="3"/>'
    ),
    "eye-off": (
        '<path d="M17.94 17.94A10.07 10.07 0 0 1 12 20c-7 0-11-8-11-8a18.45 18.45 0 0 1 5.06-5.94M9.9 4.24A9.12 9.12 0 0 1 12 4c7 0 11 8 11 8a18.5 18.5 0 0 1-2.16 3.19m-6.72-1.07a3 3 0 1 1-4.24-4.24"/>'
        '<line x1="1" y1="1" x2="23" y2="23"/>'
    ),
    "camera": (
        '<path d="M23 19a2 2 0 0 1-2 2H3a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h4l2-3h6l2 3h4a2 2 0 0 1 2 2z"/>'
        '<circle cx="12" cy="13" r="4"/>'
    ),
    "radio-tower": (
        '<line x1="12" y1="13" x2="12" y2="22"/>'
        '<path d="M8 22h8"/>'
        '<path d="M4.93 19.07a10 10 0 0 1 0-14.14"/>'
        '<path d="M19.07 4.93a10 10 0 0 1 0 14.14"/>'
        '<path d="M7.76 16.24a6 6 0 0 1 0-8.48"/>'
        '<path d="M16.24 7.76a6 6 0 0 1 0 8.48"/>'
        '<circle cx="12" cy="12" r="2"/>'
    ),
    # --- Misc ---
    "bell": (
        '<path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/>'
        '<path d="M13.73 21a2 2 0 0 1-3.46 0"/>'
    ),
    "calendar": (
        '<rect x="3" y="4" width="18" height="18" rx="2" ry="2"/>'
        '<line x1="16" y1="2" x2="16" y2="6"/>'
        '<line x1="8" y1="2" x2="8" y2="6"/>'
        '<line x1="3" y1="10" x2="21" y2="10"/>'
    ),
    "clock": (
        '<circle cx="12" cy="12" r="10"/>'
        '<polyline points="12 6 12 12 16 14"/>'
    ),
    "file-text": (
        '<path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/>'
        '<polyline points="14 2 14 8 20 8"/>'
        '<line x1="16" y1="13" x2="8" y2="13"/>'
        '<line x1="16" y1="17" x2="8" y2="17"/>'
        '<polyline points="10 9 9 9 8 9"/>'
    ),
    "info": (
        '<circle cx="12" cy="12" r="10"/>'
        '<line x1="12" y1="16" x2="12" y2="12"/>'
        '<line x1="12" y1="8" x2="12.01" y2="8"/>'
    ),
    "circle": '<circle cx="12" cy="12" r="10"/>',
    "circle-filled": '<circle cx="12" cy="12" r="8" fill="currentColor"/>',
}


def icon(name: str, *, size: int = 16, stroke_width: float = 2.0,
         class_: str = "") -> str:
    """Return an inline SVG string for the given icon name.

    Parameters
    ----------
    name
        Icon key (see :data:`ICONS`). If unknown, returns a ``help-circle``
        so broken references are visually obvious at review time.
    size
        Pixel width/height (square). Defaults to 16 for inline text-line
        use. Pass 20 or 24 for buttons, 32+ for section headers.
    stroke_width
        Line thickness on path strokes (Lucide convention is 2).
    class_
        Optional CSS class appended to the root ``<svg>`` element.

    Returns
    -------
    str
        Self-contained SVG markup safe to inject with
        ``st.markdown(..., unsafe_allow_html=True)`` or ``st.html``.
    """
    body = ICONS.get(name) or ICONS["help-circle"]
    cls = f' class="owl-ic {class_}"' if class_ else ' class="owl-ic"'
    return (
        f'<svg{cls} xmlns="http://www.w3.org/2000/svg" '
        f'width="{size}" height="{size}" viewBox="0 0 24 24" '
        f'fill="none" stroke="currentColor" stroke-width="{stroke_width}" '
        f'stroke-linecap="round" stroke-linejoin="round" '
        f'aria-hidden="true" focusable="false">{body}</svg>'
    )
