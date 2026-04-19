"""STUMPY-based anomaly detection on 1-min ASOS time series.

Matrix Profile via :func:`stumpy.stump` identifies the most unusual
subsequence (discord) in a univariate time series. We apply it to
temperature, dewpoint, wind speed, and pressure to surface sensor drift
or data-glitch events that the ``$`` flag alone wouldn't catch.

Usage::

    from asos_tools import fetch_1min
    from asos_tools.anomaly import detect_anomalies
    df = fetch_1min("KJFK", start, end)
    result = detect_anomalies(df, column="temp_2m_f")
    # result.discord_index -> row offset of the weirdest 30-min window
    # result.discord_score -> matrix profile distance (bigger = weirder)

STUMPY is CPU-only by default; it's fast enough for single-station
30-day windows (~43k rows) in under a second.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional

import numpy as np
import pandas as pd

try:
    import stumpy
    _HAVE_STUMPY = True
except ImportError:  # pragma: no cover
    stumpy = None  # type: ignore
    _HAVE_STUMPY = False

__all__ = ["detect_anomalies", "AnomalyResult"]


#: Columns likely to carry sensor-drift signals.
DEFAULT_COLUMNS = ("temp_2m_f", "dew_point_f", "wind_speed_2m_mph", "pressure_hg")


@dataclass
class AnomalyResult:
    """Anomaly detection summary for a single column."""
    column: str
    window_minutes: int
    n_points: int
    discord_index: Optional[int]
    discord_score: Optional[float]
    discord_time: Optional[pd.Timestamp]
    top_k_indices: list[int]
    top_k_scores: list[float]
    top_k_times: list[pd.Timestamp]
    matrix_profile: Optional[np.ndarray] = None

    @property
    def has_anomaly(self) -> bool:
        return self.discord_index is not None and self.discord_score is not None


def detect_anomalies(
    df: pd.DataFrame,
    *,
    column: str = "temp_2m_f",
    window_minutes: int = 30,
    top_k: int = 3,
    time_col: str = "valid_utc",
) -> AnomalyResult:
    """Find the most anomalous ``window_minutes`` window in ``df[column]``.

    Parameters
    ----------
    df
        DataFrame from :func:`fetch_1min`. Must contain ``column`` and
        ``time_col``.
    column
        Numeric column to scan (temperature, wind speed, etc.).
    window_minutes
        Subsequence length for the matrix profile. 30 = 30-minute window.
    top_k
        Return the indices of the ``top_k`` most anomalous windows.

    Returns
    -------
    AnomalyResult
        Always returns a result; ``has_anomaly`` is False if STUMPY is
        unavailable or the series is too short / too constant.
    """
    empty = AnomalyResult(
        column=column,
        window_minutes=window_minutes,
        n_points=len(df) if df is not None else 0,
        discord_index=None,
        discord_score=None,
        discord_time=None,
        top_k_indices=[],
        top_k_scores=[],
        top_k_times=[],
    )
    if not _HAVE_STUMPY or df is None or df.empty or column not in df.columns:
        return empty

    s = pd.to_numeric(df[column], errors="coerce").dropna()
    if len(s) < window_minutes * 3:
        return empty

    # Guard against flat/near-flat series (STUMPY requires variance).
    if float(s.std(ddof=0)) < 1e-6:
        return empty

    try:
        mp = stumpy.stump(s.to_numpy(), m=window_minutes)
    except Exception:
        return empty

    distances = mp[:, 0].astype(float)
    # Replace NaN/inf with -1 so argsort ignores them.
    safe = np.where(np.isfinite(distances), distances, -1)
    discord_idx = int(np.argmax(safe))
    discord_score = float(safe[discord_idx])

    if discord_score <= 0:
        return empty

    # Top-k discord positions (largest distances).
    k = min(top_k, len(safe))
    top_idx = list(np.argsort(safe)[-k:][::-1])
    top_scores = [float(safe[i]) for i in top_idx]

    # Map indices back to timestamps. Index on original df (post-dropna).
    s_idx = s.index
    def _ts(i: int) -> Optional[pd.Timestamp]:
        if i < 0 or i >= len(s_idx):
            return None
        original_row = s_idx[i]
        if time_col in df.columns:
            t = df.loc[original_row, time_col]
            if isinstance(t, pd.Timestamp):
                return t
        return None

    return AnomalyResult(
        column=column,
        window_minutes=window_minutes,
        n_points=len(s),
        discord_index=discord_idx,
        discord_score=discord_score,
        discord_time=_ts(discord_idx),
        top_k_indices=top_idx,
        top_k_scores=top_scores,
        top_k_times=[_ts(i) for i in top_idx if _ts(i) is not None],
        matrix_profile=mp[:, 0],
    )
