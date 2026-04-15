"""Tests for asos_tools.metars and asos_tools.stations."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import patch

import pandas as pd
import pytest

from asos_tools.metars import (
    IEM_ENDPOINT_METAR,
    fetch_metars,
    has_maintenance_flag,
)
from asos_tools.stations import (
    GROUPS,
    LONG_ISLAND,
    all_stations,
    get_group,
    list_groups,
)


# -----------------------------------------------------------------------------
# has_maintenance_flag
# -----------------------------------------------------------------------------

class TestHasMaintenanceFlag:
    def test_detects_trailing_dollar(self):
        assert has_maintenance_flag(
            "KJFK 150051Z 29015G26KT 10SM CLR M01/M15 A3009 RMK AO2 SLP189 $"
        )

    def test_detects_trailing_dollar_with_equals(self):
        assert has_maintenance_flag(
            "KJFK 150051Z 29015KT 10SM CLR A3009 RMK AO2 SLP189 $="
        )

    def test_absent_when_no_dollar(self):
        assert not has_maintenance_flag(
            "KJFK 150051Z 29015KT 10SM CLR A3009 RMK AO2 SLP189"
        )

    def test_handles_none_and_empty(self):
        assert not has_maintenance_flag(None)
        assert not has_maintenance_flag("")
        assert not has_maintenance_flag(float("nan"))

    def test_ignores_middle_dollars(self):
        # A $ inside the remarks, but not at the end, is NOT the flag.
        assert not has_maintenance_flag("KJFK 150051Z 29015KT RMK something $ more text")


# -----------------------------------------------------------------------------
# stations groups
# -----------------------------------------------------------------------------

class TestStations:
    def test_groups_registered(self):
        assert "long_island" in GROUPS
        assert "front_range" in GROUPS
        assert len(list_groups()) >= 10

    def test_get_group_case_insensitive(self):
        assert get_group("Long Island") == LONG_ISLAND
        assert get_group("LONG_ISLAND") == LONG_ISLAND
        assert get_group("long-island") == LONG_ISLAND

    def test_get_group_unknown_raises(self):
        with pytest.raises(KeyError):
            get_group("atlantis")

    def test_all_stations_dedupes(self):
        stations = all_stations()
        assert len(stations) == len(set(stations))
        # Some stations appear in multiple groups (KJFK is in long_island,
        # coastal_east, major_hubs). They should only appear once here.
        assert "KJFK" in stations

    def test_every_preset_is_icao_shaped(self):
        for group, stations in GROUPS.items():
            for s in stations:
                assert 3 <= len(s) <= 4, f"{group}/{s}: unexpected length"
                assert s.isupper(), f"{group}/{s}: must be uppercase"


# -----------------------------------------------------------------------------
# fetch_metars — offline parsing
# -----------------------------------------------------------------------------

SAMPLE_METAR_CSV = (
    "station,valid,metar\n"
    "JFK,2024-01-15 00:51,KJFK 150051Z 29015G26KT 10SM CLR M01/M15 A3009 RMK AO2 SLP189 $\n"
    "JFK,2024-01-15 01:51,KJFK 150151Z 29016KT 10SM CLR M01/M15 A3011 RMK AO2 SLP195 $\n"
    "JFK,2024-01-15 02:51,KJFK 150251Z 28014KT 10SM CLR M02/M15 A3014 RMK AO2 SLP206\n"
    "ISP,2024-01-15 00:51,KISP 150051Z 29010KT 10SM CLR M02/M16 A3008 RMK AO2 SLP187\n"
)


def _fake_resp(body: str, status: int = 200):
    class _R:
        def __init__(self, t, c):
            self.text = t
            self.status_code = c
        def raise_for_status(self):
            if self.status_code >= 400:
                raise AssertionError(self.status_code)
    return _R(body, status)


class TestFetchMetarsOffline:
    def test_parses_into_expected_shape(self):
        with patch("asos_tools.metars.requests.Session.get") as m:
            m.return_value = _fake_resp(SAMPLE_METAR_CSV)
            df = fetch_metars(
                "KJFK",
                datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 15, 3, 0, tzinfo=timezone.utc),
            )

        assert list(df.columns[:4]) == ["station", "valid", "metar", "has_maintenance"]
        assert len(df) == 4
        # valid is tz-aware UTC
        assert pd.api.types.is_datetime64_any_dtype(df["valid"])
        assert str(df["valid"].dt.tz) == "UTC"
        # 2 of 4 rows ended in $; rows are sorted by (valid, station).
        # 00:51 ISP (clean), 00:51 JFK ($), 01:51 JFK ($), 02:51 JFK (clean)
        assert df["has_maintenance"].tolist() == [False, True, True, False]
        assert df["station"].tolist() == ["ISP", "JFK", "JFK", "JFK"]
        # Sorted ascending by valid
        assert df["valid"].is_monotonic_increasing

    def test_request_includes_expected_params(self):
        with patch("asos_tools.metars.requests.Session.get") as m:
            m.return_value = _fake_resp(SAMPLE_METAR_CSV)
            fetch_metars(
                ["KJFK", "KLGA"],
                datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                datetime(2024, 1, 16, 0, 0, tzinfo=timezone.utc),
            )
        call = m.call_args
        assert call.args[0] == IEM_ENDPOINT_METAR
        params = call.kwargs["params"]
        assert params["station"] == "JFK,LGA"
        assert params["data"] == "metar"
        assert params["year1"] == 2024 and params["month1"] == 1 and params["day1"] == 15
        assert params["year2"] == 2024 and params["month2"] == 1 and params["day2"] == 16
        assert params["format"] == "onlycomma"

    def test_error_body_raises(self):
        with patch("asos_tools.metars.requests.Session.get") as m:
            m.return_value = _fake_resp("Unknown station provided: KZZZ")
            with pytest.raises(ValueError, match="Unknown station"):
                fetch_metars(
                    "KZZZ",
                    datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
                    datetime(2024, 1, 15, 3, 0, tzinfo=timezone.utc),
                )


# -----------------------------------------------------------------------------
# Live integration
# -----------------------------------------------------------------------------

@pytest.mark.live
class TestFetchMetarsLive:
    def test_real_jfk_day(self):
        df = fetch_metars(
            "KJFK",
            datetime(2024, 1, 15, 0, 0, tzinfo=timezone.utc),
            datetime(2024, 1, 15, 12, 0, tzinfo=timezone.utc),
        )
        # METARs are ~hourly, so expect ~12 rows (+ occasional SPECI).
        assert 8 <= len(df) <= 30
        assert df["has_maintenance"].dtype == bool
        # Every METAR string should contain the station identifier
        assert df["metar"].str.contains("KJFK").all()
