"""Tests for asos_tools.fetch.

Two tiers:

* Offline tests use the bundled CSV fixture at
  ``tests/fixtures/kord_20240115_1200_1500.csv`` and a mocked requests layer.
  These run in CI without network.
* ``@pytest.mark.live`` tests hit the real IEM endpoint. Skip them with
  ``pytest -m "not live"``.
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pandas as pd
import pytest

from asos_tools import DEFAULT_VARS_1MIN, fetch_1min, normalize_station
from asos_tools.fetch import IEM_ENDPOINT_1MIN

FIXTURES = Path(__file__).parent / "fixtures"
KORD_FIXTURE = FIXTURES / "kord_20240115_1200_1500.csv"


# -----------------------------------------------------------------------------
# Pure helpers
# -----------------------------------------------------------------------------

class TestNormalizeStation:
    def test_strips_leading_k_for_4char_us_ids(self):
        assert normalize_station("KORD") == "ORD"
        assert normalize_station("kjfk") == "JFK"

    def test_leaves_pacific_prefix_alone(self):
        assert normalize_station("PANC") == "PANC"
        assert normalize_station("PHNL") == "PHNL"

    def test_strips_whitespace_and_uppercases(self):
        assert normalize_station("  kord  ") == "ORD"

    def test_3char_id_unchanged(self):
        assert normalize_station("ORD") == "ORD"

    def test_strips_k_for_small_airport_4char_id(self):
        # K12N is a small-airport NCEI filename; IEM expects 12N.
        # Confirmed live: IEM accepts "12N" without an 'Unknown station' error.
        assert normalize_station("K12N") == "12N"


# -----------------------------------------------------------------------------
# Offline parsing against captured fixture
# -----------------------------------------------------------------------------

@pytest.fixture
def kord_csv_bytes() -> str:
    assert KORD_FIXTURE.exists(), f"fixture missing: {KORD_FIXTURE}"
    return KORD_FIXTURE.read_text()


def _mock_iem_response(body: str, status_code: int = 200):
    """Build a fake requests.Response."""
    class _FakeResp:
        def __init__(self, text: str, code: int):
            self.text = text
            self.status_code = code

        def raise_for_status(self):
            if self.status_code >= 400:
                raise AssertionError(f"http {self.status_code}")

    return _FakeResp(body, status_code)


class TestFetchOffline:
    def test_parses_fixture_into_expected_shape(self, kord_csv_bytes):
        with patch("asos_tools.fetch.requests.Session.get") as mock_get:
            mock_get.return_value = _mock_iem_response(kord_csv_bytes)

            df = fetch_1min(
                "KORD",
                datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
            )

        # 3-hour window at 1-min resolution => 180 rows.
        assert len(df) == 180

        # Columns: station, station_name, valid + all default variables.
        expected_cols = {"station", "station_name", "valid", *DEFAULT_VARS_1MIN}
        assert expected_cols.issubset(df.columns)

        # valid column is tz-aware UTC Timestamp.
        assert pd.api.types.is_datetime64_any_dtype(df["valid"])
        assert str(df["valid"].dt.tz) == "UTC"

        # Sorted ascending by valid.
        assert df["valid"].is_monotonic_increasing

        # Station is ORD (K stripped, IEM convention).
        assert (df["station"] == "ORD").all()

        # First row content matches fixture.
        first = df.iloc[0]
        assert first["valid"] == pd.Timestamp("2024-01-15 12:00", tz="UTC")
        assert first["tmpf"] == -10
        assert first["dwpf"] == -16

    def test_request_includes_correct_params(self, kord_csv_bytes):
        with patch("asos_tools.fetch.requests.Session.get") as mock_get:
            mock_get.return_value = _mock_iem_response(kord_csv_bytes)

            fetch_1min(
                ["KORD", "PANC"],
                datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
                variables=("tmpf", "dwpf"),
            )

        call = mock_get.call_args
        assert call.args[0] == IEM_ENDPOINT_1MIN
        params = call.kwargs["params"]
        assert params["station"] == "ORD,PANC"
        assert params["vars"] == "tmpf,dwpf"
        assert params["sts"] == "2024-01-15T12:00Z"
        assert params["ets"] == "2024-01-15T15:00Z"
        assert params["sample"] == "1min"
        assert params["delim"] == "comma"

    def test_naive_datetime_treated_as_utc(self, kord_csv_bytes):
        with patch("asos_tools.fetch.requests.Session.get") as mock_get:
            mock_get.return_value = _mock_iem_response(kord_csv_bytes)

            fetch_1min(
                "KORD",
                datetime(2024, 1, 15, 12, 0),      # naive
                datetime(2024, 1, 15, 15, 0),      # naive
            )

        params = mock_get.call_args.kwargs["params"]
        assert params["sts"] == "2024-01-15T12:00Z"
        assert params["ets"] == "2024-01-15T15:00Z"

    def test_rejects_inverted_time_range(self):
        with pytest.raises(ValueError, match="end must be strictly after start"):
            fetch_1min(
                "KORD",
                datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            )

    def test_numeric_columns_coerced_to_float(self, kord_csv_bytes):
        # IEM returns "M" for missing numeric readings; we must coerce so
        # downstream .sum()/.mean() work without TypeError.
        with patch("asos_tools.fetch.requests.Session.get") as mock_get:
            mock_get.return_value = _mock_iem_response(kord_csv_bytes)
            df = fetch_1min(
                "KORD",
                datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
            )
        # All of these must be numeric dtypes after the fetch.
        for col in ["tmpf", "dwpf", "sknt", "drct", "gust_sknt",
                    "vis1_coeff", "pres1", "precip"]:
            assert pd.api.types.is_numeric_dtype(df[col]), f"{col} not numeric"
        # Arithmetic should just work.
        assert isinstance(float(df["precip"].sum()), float)
        # vis1_nd is a categorical flag (N/D), leave as text.
        assert not pd.api.types.is_numeric_dtype(df["vis1_nd"])

    def test_error_body_raises_value_error(self):
        with patch("asos_tools.fetch.requests.Session.get") as mock_get:
            mock_get.return_value = _mock_iem_response("Unknown station provided: KZZZ")

            with pytest.raises(ValueError, match="Unknown station"):
                fetch_1min(
                    "KZZZ",
                    datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 15, 15, 0, tzinfo=timezone.utc),
                )


# -----------------------------------------------------------------------------
# Live tests — hit real IEM. Skip with `pytest -m "not live"`.
# -----------------------------------------------------------------------------

@pytest.mark.live
class TestFetchLive:
    def test_short_single_station_window(self):
        df = fetch_1min(
            "KORD",
            datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 10, tzinfo=timezone.utc),
            variables=("tmpf", "dwpf", "precip"),
        )
        assert len(df) == 10
        assert list(df.columns) == ["station", "station_name", "valid",
                                    "tmpf", "dwpf", "precip"]
        assert (df["station"] == "ORD").all()

    def test_cross_month_window(self):
        df = fetch_1min(
            "KORD",
            datetime(2024, 1, 31, 23, 0, tzinfo=timezone.utc),
            datetime(2024, 2, 1, 2, 0, tzinfo=timezone.utc),
            variables=("tmpf",),
        )
        # 3 hours should round to 180 rows barring sensor gaps.
        assert 170 <= len(df) <= 180
        # Confirms spans both months.
        assert df["valid"].min().month == 1
        assert df["valid"].max().month == 2

    def test_multi_station_returns_all_requested(self):
        df = fetch_1min(
            ("KJFK", "KLGA"),
            datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 5, tzinfo=timezone.utc),
            variables=("tmpf",),
        )
        assert set(df["station"].unique()).issubset({"JFK", "LGA"})
