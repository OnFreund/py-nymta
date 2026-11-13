"""Shared fixtures for py-nymta tests.

Each fixture returns a small factory, so a test can build exactly the feed,
session or cache it needs in one line.
"""

from __future__ import annotations

import csv
import io
import zipfile
from collections.abc import Callable, Iterable, Mapping
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import AsyncMock, Mock

import pytest
from google.transit import gtfs_realtime_pb2

from pymta.gtfs_static import GTFSCache

# A trip in a real-time feed: (route_id, [(stop_id, seconds_from_now), ...]).
# Negative offsets give arrivals in the past.
TripSpec = tuple[str, list[tuple[str, int]]]


# ---------------------------------------------------------------------------
# GTFS-RT feed builder
# ---------------------------------------------------------------------------


@pytest.fixture
def make_feed() -> Callable[[Iterable[TripSpec]], bytes]:
    """Return a factory that builds a serialized GTFS-RT FeedMessage.

    Example:
        content = make_feed([
            ("B", [("A21S", 300), ("A55S", 2400)]),
            ("D", [("A21S", 120), ("D43S", 3000)]),   # rerouted D on CPW
        ])

    Stops get stop_sequence 1..n in the order given, so the last stop listed
    is the trip's terminal (used for the destination name).
    """

    def _make(trips: Iterable[TripSpec]) -> bytes:
        now = int(datetime.now(timezone.utc).timestamp())
        msg = gtfs_realtime_pb2.FeedMessage()
        msg.header.gtfs_realtime_version = "2.0"
        msg.header.timestamp = now

        for i, (route_id, stops) in enumerate(trips):
            entity = msg.entity.add()
            entity.id = f"trip{i}"
            trip_update = entity.trip_update
            trip_update.trip.route_id = route_id
            for seq, (stop_id, offset) in enumerate(stops, start=1):
                stu = trip_update.stop_time_update.add()
                stu.stop_id = stop_id
                stu.stop_sequence = seq
                stu.arrival.time = now + offset

        return msg.SerializeToString()

    return _make


# ---------------------------------------------------------------------------
# aiohttp session mocks
# ---------------------------------------------------------------------------


def _ok_response(content: bytes) -> AsyncMock:
    """A response usable as `async with session.get(...) as response`."""
    response = AsyncMock()
    response.read = AsyncMock(return_value=content)
    response.raise_for_status = Mock()
    response.__aenter__ = AsyncMock(return_value=response)
    response.__aexit__ = AsyncMock(return_value=None)
    return response


def _error_response(error: BaseException) -> Mock:
    """A response whose `async with` raises, like a network failure."""
    response = Mock()
    response.__aenter__ = AsyncMock(side_effect=error)
    response.__aexit__ = AsyncMock(return_value=None)
    return response


@pytest.fixture
def mock_session() -> Callable[[bytes], Mock]:
    """Return a factory for a session that answers every request with `content`."""

    def _make(content: bytes) -> Mock:
        session = Mock()
        session.get = Mock(return_value=_ok_response(content))
        return session

    return _make


@pytest.fixture
def mock_session_by_url() -> Callable[[Mapping[str, bytes | BaseException]], Mock]:
    """Return a factory for a session that answers differently per URL.

    For multi-feed tests, e.g. ACE + BDFM:
        session = mock_session_by_url({
            FEED_URLS["A"]: make_feed([...]),
            FEED_URLS["B"]: make_feed([...]),
        })

    A value that is an exception makes that URL fail. A URL not in the map
    fails the test, so a feed fetched by mistake is caught.
    `session.get.call_args_list` shows which URLs were requested.
    """

    def _make(responses: Mapping[str, bytes | BaseException]) -> Mock:
        def _get(url: str, *args, **kwargs):
            if url not in responses:
                raise AssertionError(f"Unexpected request to {url}")
            value = responses[url]
            if isinstance(value, BaseException):
                return _error_response(value)
            return _ok_response(value)

        session = Mock()
        session.get = Mock(side_effect=_get)
        return session

    return _make


# ---------------------------------------------------------------------------
# GTFSCache mocks (static stop names)
# ---------------------------------------------------------------------------


@pytest.fixture
def subway_cache() -> Callable[..., Mock]:
    """Return a factory for a mocked GTFSCache as SubwayFeed uses it."""

    def _make(stop_names: dict[str, str] | None = None) -> Mock:
        cache = Mock(spec=GTFSCache)
        cache.download_gtfs = AsyncMock(return_value=Path("/fake/subway.zip"))
        cache.get_stop_names = Mock(return_value=stop_names or {})
        return cache

    return _make


@pytest.fixture
def bus_cache() -> Callable[..., Mock]:
    """Return a factory for a mocked GTFSCache as BusFeed uses it."""

    def _make(stop_names: dict[str, str] | None = None) -> Mock:
        cache = Mock(spec=GTFSCache)
        cache.download_gtfs = AsyncMock(return_value=Path("/fake/bus.zip"))
        cache.get_combined_stop_names = AsyncMock(return_value=stop_names or {})
        return cache

    return _make


# ---------------------------------------------------------------------------
# Tiny static GTFS zip (for feeds_for_stop / routes_for_stop tests)
# ---------------------------------------------------------------------------


def _csv(header: list[str], rows: Iterable[Iterable[object]]) -> str:
    buf = io.StringIO()
    writer = csv.writer(buf, lineterminator="\n")
    writer.writerow(header)
    writer.writerows(rows)
    return buf.getvalue()


@pytest.fixture
def make_static_gtfs(tmp_path: Path) -> Callable[..., Path]:
    """Return a factory that writes a minimal static GTFS zip and returns its path.

    Example (a CPW local stop normally served by B and C):
        zip_path = make_static_gtfs(
            trips={
                "c1": ("C", ["A24S", "A21S", "A15S"]),
                "b1": ("B", ["A24S", "A21S", "A15S"]),
                "d1": ("D", ["A24S", "A15S"]),        # express: skips A21
            },
            stop_names={"A21": "81 St-Museum of Natural History"},
        )

    Directional stop IDs (A21S) get a parent station (A21) automatically.
    Check the column names against what GTFSCache's parsers actually read.
    """

    def _make(
        trips: Mapping[str, tuple[str, list[str]]],
        stop_names: Mapping[str, str] | None = None,
        name: str = "static.zip",
    ) -> Path:
        stop_names = dict(stop_names or {})

        route_ids = sorted({route for route, _ in trips.values()})
        stop_ids = sorted({s for _, stops in trips.values() for s in stops})

        stops_rows: list[list[object]] = []
        parents = sorted({s[:-1] for s in stop_ids if s[-1] in "NS"})
        for parent in parents:
            stops_rows.append([parent, stop_names.get(parent, parent), ""])
        for stop_id in stop_ids:
            parent = stop_id[:-1] if stop_id[-1] in "NS" else ""
            label = stop_names.get(stop_id) or stop_names.get(parent, stop_id)
            stops_rows.append([stop_id, label, parent])

        stop_time_rows: list[list[object]] = []
        for trip_id, (_, stops) in trips.items():
            for seq, stop_id in enumerate(stops, start=1):
                t = f"08:{seq:02d}:00"
                stop_time_rows.append([trip_id, t, t, stop_id, seq])

        files = {
            "agency.txt": _csv(
                ["agency_id", "agency_name", "agency_url", "agency_timezone"],
                [
                    [
                        "MTA NYCT",
                        "MTA New York City Transit",
                        "https://mta.info",
                        "America/New_York",
                    ]
                ],
            ),
            "routes.txt": _csv(
                ["route_id", "agency_id", "route_short_name", "route_type"],
                [[r, "MTA NYCT", r, 1] for r in route_ids],
            ),
            "stops.txt": _csv(["stop_id", "stop_name", "parent_station"], stops_rows),
            "trips.txt": _csv(
                ["route_id", "service_id", "trip_id"],
                [[route, "Weekday", trip_id] for trip_id, (route, _) in trips.items()],
            ),
            "stop_times.txt": _csv(
                [
                    "trip_id",
                    "arrival_time",
                    "departure_time",
                    "stop_id",
                    "stop_sequence",
                ],
                stop_time_rows,
            ),
        }

        zip_path = tmp_path / name
        with zipfile.ZipFile(zip_path, "w") as zf:
            for filename, content in files.items():
                zf.writestr(filename, content)
        return zip_path

    return _make
