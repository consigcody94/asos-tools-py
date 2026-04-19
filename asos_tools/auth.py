"""Lightweight role-based auth for O.W.L.

streamlit-authenticator's full YAML config is overkill for a single-room
demo Space, so we wrap it with a simple pattern:

- Public tabs (Summary, Stations, Forecasters) — always visible.
- AOMC Controllers tab — requires ``aomc_passcode`` env var.
- Admin tab — requires ``admin_passcode`` env var.

If the env vars are unset (the default in the public demo), we expose a
short demo passcode and label the gate as "DEMO MODE" so it's clear
this is not a real auth system.

The gate state is kept in ``st.session_state`` so the user only logs
in once per session.
"""

from __future__ import annotations

import hashlib
import hmac
import os

import streamlit as st

__all__ = ["require_access", "is_authenticated", "logout", "access_status"]


#: Demo passcodes — used ONLY if the env vars aren't set. The whole
#: point of this module is that real deployments override via env vars.
_DEMO_AOMC = "owl-aomc-demo-2026"
_DEMO_ADMIN = "owl-admin-demo-2026"


def _expected(role: str) -> tuple[str, bool]:
    """Return (expected_passcode, is_demo)."""
    env_key = {
        "aomc": "OWL_AOMC_PASSCODE",
        "admin": "OWL_ADMIN_PASSCODE",
    }.get(role)
    demo_val = {"aomc": _DEMO_AOMC, "admin": _DEMO_ADMIN}.get(role)
    if env_key and (v := os.environ.get(env_key, "").strip()):
        return v, False
    return demo_val or "", True


def _check(provided: str, expected: str) -> bool:
    """Constant-time string compare to resist timing attacks."""
    a = hashlib.sha256(provided.encode("utf-8", "replace")).digest()
    b = hashlib.sha256(expected.encode("utf-8", "replace")).digest()
    return hmac.compare_digest(a, b)


def is_authenticated(role: str) -> bool:
    """Has the user passed the gate for this role in this session?"""
    return bool(st.session_state.get(f"_auth_{role}", False))


def access_status() -> dict:
    """Summary for the Admin tab — which roles are currently unlocked."""
    return {
        "aomc": is_authenticated("aomc"),
        "admin": is_authenticated("admin"),
        "aomc_demo_mode": _expected("aomc")[1],
        "admin_demo_mode": _expected("admin")[1],
    }


def require_access(
    role: str,
    *,
    title: str = "Restricted area",
    description: str = "",
) -> bool:
    """Render a login gate for ``role`` in the current Streamlit container.

    Returns True if the user is authenticated (now or previously this
    session), False if the gate is currently blocking. Calling code should
    simply check the return value and skip rendering on False.
    """
    if is_authenticated(role):
        return True

    expected, is_demo = _expected(role)

    st.markdown(f"### {title}")
    if description:
        st.caption(description)

    if is_demo:
        st.info(
            f"**Demo mode.** This public Space uses a demo passcode. "
            f"Real deployments would set `OWL_{role.upper()}_PASSCODE` "
            f"in the environment.  \n"
            f"Passcode hint: `{expected}`"
        )
    else:
        st.caption("Enter the passcode issued by your O.W.L. administrator.")

    col_a, col_b = st.columns([3, 1])
    with col_a:
        entry = st.text_input(
            "Passcode",
            type="password",
            key=f"_auth_input_{role}",
            label_visibility="collapsed",
            placeholder=f"{role.upper()} passcode",
        )
    with col_b:
        submit = st.button(
            "Unlock",
            key=f"_auth_btn_{role}",
            type="primary",
            use_container_width=True,
        )

    if submit:
        if not expected:
            st.error("No passcode configured for this role.")
        elif _check(entry or "", expected):
            st.session_state[f"_auth_{role}"] = True
            st.rerun()
        else:
            st.error("Incorrect passcode.")

    return False


def logout(role: str = "all") -> None:
    """Log the user out of one or all roles."""
    if role == "all":
        for k in list(st.session_state.keys()):
            if k.startswith("_auth_"):
                st.session_state[k] = False
    else:
        st.session_state[f"_auth_{role}"] = False
