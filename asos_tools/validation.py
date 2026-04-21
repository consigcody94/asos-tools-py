"""Centralized input-validation helpers.

One source of truth for every user-supplied value that flows into an
outbound HTTP request.  Every data-source module (metars, ncei, awc,
webcams, alerts_feed, nws) should import from here rather than inline
its own regex or accept arbitrary strings.

Two families of checks:

1. **Station ICAO / FAA LID** — must match ``^[A-Z0-9]{3,6}$``.  Rejects
   hostile values that could do URL-parameter injection, path traversal
   into a proxy, or SSRF via station-list-as-URL-parts.

2. **Upstream BASE URLs** — any endpoint we accept from an env var must
   be ``https://`` and not resolve to a private / link-local address.
   Guards against someone flipping ``AWC_API_BASE`` to
   ``http://169.254.169.254/latest/meta-data/`` (AWS IMDS) or a
   LAN address to exfiltrate via our own renderer.
"""

from __future__ import annotations

import ipaddress
import logging
import re
import socket
from urllib.parse import urlparse

__all__ = [
    "validate_icao_id",
    "normalize_icao_list",
    "is_safe_https_base",
    "guard_upstream_base",
]

logger = logging.getLogger(__name__)

# FAA LIDs are 3 characters (digits or letters), ICAOs are 4.  We also
# accept 5- and 6-character IDs to cover unusual combinations (e.g.
# "PAANC" fallback or military-apron "CO90") — but no longer than 6.
_ICAO_RE = re.compile(r"^[A-Z0-9]{3,6}$")


def validate_icao_id(value: object) -> str | None:
    """Return an uppercase station ID if valid, else ``None``.

    The returned value is safe to interpolate into an outbound URL's
    query or path.
    """
    if value is None:
        return None
    s = str(value).strip().upper()
    if not s:
        return None
    return s if _ICAO_RE.fullmatch(s) else None


def normalize_icao_list(values: object) -> list[str]:
    """Return the subset of ``values`` that pass :func:`validate_icao_id`.

    Accepts any iterable of anything; silently drops invalid entries.
    """
    if values is None:
        return []
    if isinstance(values, (str, bytes)):
        values = [values]
    out: list[str] = []
    try:
        for v in values:
            ok = validate_icao_id(v)
            if ok:
                out.append(ok)
    except TypeError:
        return []
    return out


def is_safe_https_base(url: str) -> bool:
    """True iff ``url`` is an https://<public-host>[:port][/path] form.

    Blocks private, loopback, link-local, and multicast destinations.
    Used to vet every ``*_BASE`` env var on startup before any fetch
    module is allowed to use a non-default endpoint.
    """
    if not url or not isinstance(url, str):
        return False
    parsed = urlparse(url.strip())
    if parsed.scheme != "https":
        return False
    if not parsed.hostname:
        return False

    # Reject raw IPs that are private/loopback/link-local.  For hostnames
    # we best-effort resolve and check each A/AAAA against the same
    # ranges — if DNS fails at startup we fail safe (reject) rather
    # than allow a potentially misconfigured target.
    try:
        ip = ipaddress.ip_address(parsed.hostname)
        ips = [ip]
    except ValueError:
        try:
            infos = socket.getaddrinfo(parsed.hostname, None)
            ips = [ipaddress.ip_address(info[4][0]) for info in infos]
        except Exception:
            return False

    for ip in ips:
        if (ip.is_private or ip.is_loopback or ip.is_link_local
                or ip.is_multicast or ip.is_reserved
                or ip.is_unspecified):
            return False
    return True


def guard_upstream_base(env_name: str, default: str) -> str:
    """Return env-var ``env_name`` if it's a safe https base URL, else
    fall back to ``default`` and log a warning.

    Call this at module import in every data-source file so a
    misconfigured (or malicious) env var can never pivot us to an
    internal IMDS / LAN endpoint.
    """
    import os
    raw = os.environ.get(env_name)
    if raw is None or raw == default:
        return default
    if is_safe_https_base(raw):
        return raw
    logger.warning(
        "Env var %s=%r rejected (not a public https URL); using default %s",
        env_name, raw, default,
    )
    return default
