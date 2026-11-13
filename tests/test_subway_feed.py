"""Tests for SubwayFeed class."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

from google.transit import gtfs_realtime_pb2
import pytest
from pathlib import Path

from pymta import Arrival, MTAFeedError, SubwayFeed
from pymta.constants import FEED_URLS
from pymta.gtfs_static import GTFSCache

def test_init_valid_feed_id():
    """Test initialization with valid feed ID."""
    feed = SubwayFeed(feed_id="N")
    assert feed.feed_id == "N"
    assert feed.timeout == 30


def test_init_invalid_feed_id():
    """Test initialization with invalid feed ID."""
    with pytest.raises(ValueError, match="Invalid feed_id"):
        SubwayFeed(feed_id="INVALID")


def test_get_feed_id_for_route():
    """Test getting feed ID for a route."""
    assert SubwayFeed.get_feed_id_for_route("Q") == "N"
    assert SubwayFeed.get_feed_id_for_route("1") == "1"
    assert SubwayFeed.get_feed_id_for_route("F") == "B"
    # The Franklin Av Shuttle is published in the BDFM feed, not ACE.
    assert SubwayFeed.get_feed_id_for_route("FS") == "B"


def test_get_feed_id_for_invalid_route():
    """Test getting feed ID for invalid route."""
    with pytest.raises(ValueError, match="Invalid route_id"):
        SubwayFeed.get_feed_id_for_route("INVALID")


@pytest.mark.asyncio
async def test_get_arrivals_success():
    """Test getting arrivals successfully with destination from last stop."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Create a GTFS-RT FeedMessage
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Create a trip update entity
    entity = feed_message.entity.add()
    entity.id = "trip1"

    trip_update = entity.trip_update
    trip_update.trip.route_id = "Q"

    # Add stop time updates - the train passes through B08S and ends at D43S
    stop_time1 = trip_update.stop_time_update.add()
    stop_time1.stop_id = "B08S"
    stop_time1.arrival.time = int(datetime.now(timezone.utc).timestamp() + 300)

    stop_time2 = trip_update.stop_time_update.add()
    stop_time2.stop_id = "D43S"  # Last stop - Coney Island
    stop_time2.arrival.time = int(datetime.now(timezone.utc).timestamp() + 900)

    # Mock response
    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=feed_message.SerializeToString())
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    # Mock session
    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Mock GTFSCache with stop names
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.get_stop_names = Mock(return_value={
        "B08S": "Prospect Park",
        "D43S": "Coney Island - Stillwell Av",
    })

    # Test
    feed = SubwayFeed(feed_id="N", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")

    assert len(arrivals) == 1
    assert arrivals[0].route_id == "Q"
    assert arrivals[0].stop_id == "B08S"
    assert arrivals[0].destination == "Coney Island - Stillwell Av"
    assert isinstance(arrivals[0].arrival_time, datetime)


@pytest.mark.asyncio
async def test_get_arrivals_destination_fallback():
    """Test fallback destination when stop names not available."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Create a GTFS-RT FeedMessage
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    entity = feed_message.entity.add()
    entity.id = "trip1"
    trip_update = entity.trip_update
    trip_update.trip.route_id = "Q"

    stop_time = trip_update.stop_time_update.add()
    stop_time.stop_id = "B08S"
    stop_time.arrival.time = int(datetime.now(timezone.utc).timestamp() + 300)

    # Mock response
    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=feed_message.SerializeToString())
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Mock GTFSCache with empty stop names (fallback case)
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.get_stop_names = Mock(return_value={})

    feed = SubwayFeed(feed_id="N", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")

    assert len(arrivals) == 1
    assert arrivals[0].destination == "Q train"


@pytest.mark.asyncio
async def test_get_arrivals_filters_past_arrivals():
    """Test that past arrivals are filtered out."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Create a GTFS-RT FeedMessage
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Create trip with past arrival
    entity = feed_message.entity.add()
    entity.id = "trip1"
    trip_update = entity.trip_update
    trip_update.trip.route_id = "Q"

    stop_time = trip_update.stop_time_update.add()
    stop_time.stop_id = "B08S"
    past_time = datetime.now(timezone.utc).timestamp() - 300  # 5 minutes ago
    stop_time.arrival.time = int(past_time)

    # Mock response
    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=feed_message.SerializeToString())
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    # Mock session
    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Mock GTFSCache
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.get_stop_names = Mock(return_value={})

    # Test
    feed = SubwayFeed(feed_id="N", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")

    assert len(arrivals) == 0


@pytest.mark.asyncio
async def test_get_arrivals_no_route_returns_all_routes(make_feed, mock_session, subway_cache):
    """Test that all arrivals are returned when no route_id is specified.
       Note that there's a northbound train listed and it will not be included.
       Trips that go to other stations are also excluded.
    """
    content = make_feed(
        [
            ("Q", [("B08S", 300)]),
            ("Q", [("B08N", 350)]),
            ("Q", [("D20S", 200)]),  # This arrival is for a different stop and should not be included
            ("R", [("B08S", 600)]),
        ]
    )
    feed = SubwayFeed(
        feed_id="N", session=mock_session(content), gtfs_cache=subway_cache()
    )

    arrivals = await feed.get_arrivals(route_id=None, stop_id="B08S")

    assert len(arrivals) == 2
    assert arrivals[0].route_id == "Q"
    assert arrivals[1].route_id == "R"


@pytest.mark.asyncio
async def test_get_arrivals_max_arrivals():
    """Test max_arrivals parameter."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Create a GTFS-RT FeedMessage with 5 arrivals
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    for i in range(5):
        entity = feed_message.entity.add()
        entity.id = f"trip{i}"
        trip_update = entity.trip_update
        trip_update.trip.route_id = "Q"

        stop_time = trip_update.stop_time_update.add()
        stop_time.stop_id = "B08S"
        future_time = datetime.now(timezone.utc).timestamp() + (i + 1) * 60
        stop_time.arrival.time = int(future_time)

    # Mock response
    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=feed_message.SerializeToString())
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    # Mock session
    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Mock GTFSCache
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.get_stop_names = Mock(return_value={})

    # Test
    feed = SubwayFeed(feed_id="N", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S", max_arrivals=3)

    assert len(arrivals) == 3


@pytest.mark.asyncio
async def test_get_arrivals_network_error():
    """Test handling of network errors."""
    import aiohttp
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Mock session that raises an error when entering context
    mock_response = Mock()
    mock_response.__aenter__ = AsyncMock(side_effect=aiohttp.ClientError("Network error"))
    mock_response.__aexit__ = AsyncMock(return_value=None)

    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Mock GTFSCache
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.get_stop_names = Mock(return_value={})

    feed = SubwayFeed(feed_id="N", session=mock_session, gtfs_cache=mock_cache)
    with pytest.raises(MTAFeedError, match="Error fetching GTFS-RT feed"):
        await feed.get_arrivals(route_id="Q", stop_id="B08S")


def test_arrival_sorting():
    """Test that Arrival objects can be sorted by time."""
    now = datetime.now(timezone.utc)
    arrival1 = Arrival(
        arrival_time=now,
        route_id="Q",
        stop_id="B08S",
        destination="Coney Island",
    )
    arrival2 = Arrival(
        arrival_time=now + timedelta(minutes=5),
        route_id="Q",
        stop_id="B08S",
        destination="Coney Island",
    )

    arrivals = [arrival2, arrival1]
    arrivals.sort()

    assert arrivals[0] == arrival1
    assert arrivals[1] == arrival2


@pytest.mark.asyncio
async def test_get_active_stops():
    """Test getting active stops for a route."""
    # Create a GTFS-RT FeedMessage with multiple stops
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Add trip with multiple stops
    entity = feed_message.entity.add()
    entity.id = "trip1"
    trip_update = entity.trip_update
    trip_update.trip.route_id = "Q"

    # Stop 1 - with future arrival
    stop_time1 = trip_update.stop_time_update.add()
    stop_time1.stop_id = "B08N"
    stop_time1.arrival.time = int(datetime.now(timezone.utc).timestamp() + 300)

    # Stop 2 - with future arrival
    stop_time2 = trip_update.stop_time_update.add()
    stop_time2.stop_id = "B08S"
    stop_time2.arrival.time = int(datetime.now(timezone.utc).timestamp() + 600)

    # Add another trip with a different stop
    entity2 = feed_message.entity.add()
    entity2.id = "trip2"
    trip_update2 = entity2.trip_update
    trip_update2.trip.route_id = "Q"

    stop_time3 = trip_update2.stop_time_update.add()
    stop_time3.stop_id = "D20N"
    stop_time3.arrival.time = int(datetime.now(timezone.utc).timestamp() + 900)

    # Mock response
    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=feed_message.SerializeToString())
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    # Mock session
    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Test
    feed = SubwayFeed(feed_id="N", session=mock_session)
    stops = await feed.get_active_stops(route_id="Q")

    assert len(stops) == 3
    assert stops[0]["stop_id"] == "B08N"
    assert stops[1]["stop_id"] == "B08S"
    assert stops[2]["stop_id"] == "D20N"
    assert all(stop["has_arrivals"] for stop in stops)
    assert all(stop["stop_name"] is None for stop in stops)


@pytest.mark.asyncio
async def test_get_stops():
    """Test getting static stops for a route."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Mock GTFSCache
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.parse_stops_for_route = Mock(return_value=[
        {"stop_id": "D20N", "stop_name": "Atlantic Av-Barclays Ctr", "stop_sequence": 1},
        {"stop_id": "D21N", "stop_name": "DeKalb Av", "stop_sequence": 2},
        {"stop_id": "D22N", "stop_name": "Canal St", "stop_sequence": 3},
    ])

    # Mock session
    mock_session = Mock()

    # Test
    feed = SubwayFeed(feed_id="N", session=mock_session, gtfs_cache=mock_cache)
    stops = await feed.get_stops(route_id="Q")

    assert len(stops) == 3
    assert stops[0]["stop_id"] == "D20N"
    assert stops[0]["stop_name"] == "Atlantic Av-Barclays Ctr"
    assert stops[0]["stop_sequence"] == 1
    assert stops[1]["stop_id"] == "D21N"
    assert stops[1]["stop_name"] == "DeKalb Av"
    assert stops[2]["stop_id"] == "D22N"
    assert stops[2]["stop_name"] == "Canal St"

    # Verify cache was called correctly
    mock_cache.download_gtfs.assert_called_once()
    mock_cache.parse_stops_for_route.assert_called_once_with(Path("/fake/path.zip"), "Q")


@pytest.mark.asyncio
async def test_rerouted_d_included_at_cpw_local_stop(
    tmp_path, make_static_gtfs, make_feed, mock_session_by_url, subway_cache
) -> None:
    """A D rerouted onto the CPW local is included even though the timetable
    doesn't include this station.

    The feeds to poll come from the routes that normally serve the stop
    (B and C), so BDFM is polled because of the B, and the rerouted D
    appears in it.
    """

    # Timetable: B and C stop at A21; the D runs express and skips it.
    zip_path = make_static_gtfs(
        trips={
            "c1": ("C", ["A24S", "A22S", "A21S", "A20S"]),
            "b1": ("B", ["A24S", "A22S", "A21S", "A20S"]),
            "d1": ("D", ["A24S", "A15S"]),
        }
    )

    cache = GTFSCache(cache_dir=tmp_path)
    routes = cache.get_routes_for_stop(zip_path, "A21S")
    assert routes == {"B", "C"}  # the D skips 81 St in the timetable

    feed_ids = SubwayFeed.get_feed_ids_for_routes(routes)
    assert feed_ids == ["A", "B"]

    # Live data on a construction weekend: a D calls at A21S.
    session = mock_session_by_url(
        {
            FEED_URLS["A"]: make_feed([("C", [("A21S", 420), ("A55S", 2400)])]),
            FEED_URLS["B"]: make_feed([("D", [("A21S", 120), ("D43S", 3000)])]),
        }
    )

    arrivals = await SubwayFeed.get_stop_arrivals(
        "A21S", feed_ids, session=session, gtfs_cache=subway_cache()
    )

    assert [a.route_id for a in arrivals] == ["D", "C"]

    requested = {call.args[0] for call in session.get.call_args_list}
    assert requested == {FEED_URLS["A"], FEED_URLS["B"]}


@pytest.mark.asyncio
async def test_multiple_routes_respect_arrivals_limit(
    tmp_path, make_static_gtfs, make_feed, mock_session_by_url, subway_cache
) -> None:
    """It's possible when not filtering by route_id that the total number of
    arrivals exceeds the limit when multiple routes serve the same stop.
    In this case, the limit should be respected.
    """

    # Timetable: B and C stop at A21.
    zip_path = make_static_gtfs(
        trips = {
            "c1": ("C", ["A24S", "A22S", "A21S", "A20S"]),
            "b1": ("B", ["A24S", "A22S", "A21S", "A20S"]),
            "d1": ("D", ["A24S", "A20S"]),
        })

    cache = GTFSCache(cache_dir=tmp_path)
    routes = cache.get_routes_for_stop(zip_path, "A21S")
    assert routes == {"B", "C"}  # the D skips 81 St in the timetable

    feed_ids = SubwayFeed.get_feed_ids_for_routes(routes)
    assert feed_ids == ["A", "B"]

    session = mock_session_by_url(
        {
            FEED_URLS["A"]: make_feed(
                [
                    ("C", [("A21S", 420), ("A55S", 2400)]),
                    ("C", [("A21S", 520), ("A55S", 2400)]),
                    ("C", [("A21S", 720), ("A55S", 2400)]),
                ]
            ),
            FEED_URLS["B"]: make_feed(
                [
                    ("B", [("A21S", 120), ("D43S", 3000)]),
                    ("B", [("A21S", 620), ("D43S", 3000)]),
                    ("B", [("A21S", 820), ("D43S", 3000)]),
                    ("B", [("A21S", 920), ("D43S", 3000)]),
                ]
            ),
        }
    )

    arrivals = await SubwayFeed.get_stop_arrivals(
        "A21S", feed_ids, session=session, gtfs_cache=subway_cache()
    )

    assert [a.route_id for a in arrivals] == ["B", "C", "C"]


@pytest.mark.asyncio
async def test_get_stop_arrivals_no_feeds_returns_empty(
    mock_session_by_url, subway_cache
) -> None:
    """With no feeds to read, no arrivals are returned and nothing is fetched."""
    session = mock_session_by_url({})

    arrivals = await SubwayFeed.get_stop_arrivals(
        "A21S", [], session=session, gtfs_cache=subway_cache()
    )

    assert arrivals == []
    session.get.assert_not_called()


@pytest.mark.asyncio
async def test_get_stop_arrivals_one_feed_fails_raises(
    make_feed, mock_session_by_url, subway_cache
) -> None:
    """If any feed fails, the whole call fails, rather than returning
    a partial list that silently leaves out that feed's routes.
    """
    import aiohttp

    session = mock_session_by_url(
        {
            FEED_URLS["A"]: make_feed([("C", [("A21S", 300)])]),
            FEED_URLS["B"]: aiohttp.ClientError("Network error"),
        }
    )

    with pytest.raises(MTAFeedError, match="Error fetching GTFS-RT feed"):
        await SubwayFeed.get_stop_arrivals(
            "A21S", ["A", "B"], session=session, gtfs_cache=subway_cache()
        )


def test_get_routes_for_stop_without_any_arrivals(
    tmp_path, make_static_gtfs, make_feed, mock_session_by_url, subway_cache
) -> None:
    """If the stop requested has no arrivals, make sure an empty set is returned.
    """

    # Timetable: B and C stop at A21; the D runs express and skips it.
    zip_path = make_static_gtfs(
        trips={
            "c1": ("C", ["A24S", "A22S", "A21S", "A20S"]),
            "b1": ("B", ["A24S", "A22S", "A21S", "A20S"]),
            "d1": ("D", ["A24S", "A15S"]),
        }
    )

    cache = GTFSCache(cache_dir=tmp_path)
    routes = cache.get_routes_for_stop(zip_path, "X99S")  # a stop that doesn't exist in the timetable
    assert routes == set()  # should be empty because no routes serve this stop

    feed_ids = SubwayFeed.get_feed_ids_for_routes(routes)
    assert feed_ids == []  # should be empty because no routes serve this stop


def test_get_routes_for_stop_with_and_without_direction(tmp_path, make_static_gtfs) -> None:
    """With a direction suffix only that direction matches; without one, both do."""
    # Note that this is here for completeness of test coverage, although the current
    # UI does not allow for the user to select this option.
    zip_path = make_static_gtfs(
        trips={
            "b1": ("B", ["A24S", "A21S"]),
            "c1": ("C", ["A20N", "A21N"]),
        }
    )
    cache = GTFSCache(cache_dir=tmp_path)

    assert cache.get_routes_for_stop(zip_path, "A21") == {"B", "C"}
    assert cache.get_routes_for_stop(zip_path, "A21S") == {"B"}
    assert cache.get_routes_for_stop(zip_path, "A21N") == {"C"}


def test_unknown_route_is_quietly_ignored(
    tmp_path, make_static_gtfs, make_feed, mock_session_by_url, subway_cache
) -> None:
    """If an unknown route is requested, just ignore it.
    """

    feed_ids = SubwayFeed.get_feed_ids_for_routes({"B", "X"})
    assert feed_ids == ["B"]


@pytest.mark.asyncio
async def test_get_arrivals_filters_by_route(make_feed, mock_session, subway_cache):
    """Test that arrivals are filtered by route_id when one is given."""
    content = make_feed(
        [
            ("Q", [("B08S", 300)]),
            ("R", [("B08S", 600)]),
        ]
    )
    feed = SubwayFeed(
        feed_id="N", session=mock_session(content), gtfs_cache=subway_cache()
    )

    arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")

    assert len(arrivals) == 1
    assert arrivals[0].route_id == "Q"


@pytest.mark.asyncio
async def test_get_arrivals_no_route_destination_per_trip(
    make_feed, mock_session, subway_cache
) -> None:
    """With no route filter, each arrival's destination comes from its own
    trip, and the fallback names that trip's route, never "None train".
    """
    content = make_feed(
        [
            ("B", [("A21S", 120), ("A55S", 2400)]),
            ("D", [("A21S", 300), ("D43S", 3000)]),
        ]
    )

    # Stop names known: destinations come from each trip's terminal.
    feed = SubwayFeed(
        feed_id="B",
        session=mock_session(content),
        gtfs_cache=subway_cache(
            {"A55S": "Brighton Beach", "D43S": "Coney Island-Stillwell Av"}
        ),
    )
    arrivals = await feed.get_arrivals(None, "A21S")
    assert [(a.route_id, a.destination) for a in arrivals] == [
        ("B", "Brighton Beach"),
        ("D", "Coney Island-Stillwell Av"),
    ]

    # Stop names unknown: the fallback uses each trip's own route.
    feed = SubwayFeed(
        feed_id="B", session=mock_session(content), gtfs_cache=subway_cache()
    )
    arrivals = await feed.get_arrivals(None, "A21S")
    assert [a.destination for a in arrivals] == ["B train", "D train"]


@pytest.mark.asyncio
async def test_get_feed_ids_for_stop() -> None:
    """Feed IDs come from the routes the timetable lists at the stop."""
    cache = Mock(spec=GTFSCache)
    cache.download_gtfs = AsyncMock(return_value=Path("/fake/subway.zip"))
    cache.get_routes_for_stop = Mock(return_value={"B", "C"})

    feed_ids = await SubwayFeed.get_feed_ids_for_stop(
        "A21S", session=Mock(), gtfs_cache=cache
    )

    assert feed_ids == ["A", "B"]
    cache.get_routes_for_stop.assert_called_once_with(Path("/fake/subway.zip"), "A21S")


@pytest.mark.asyncio
async def test_get_feed_ids_for_stop_download_error() -> None:
    """A failed timetable download raises MTAFeedError."""
    import aiohttp

    cache = Mock(spec=GTFSCache)
    cache.download_gtfs = AsyncMock(side_effect=aiohttp.ClientError("Network error"))

    with pytest.raises(MTAFeedError, match="Error fetching static GTFS data"):
        await SubwayFeed.get_feed_ids_for_stop("A21S", session=Mock(), gtfs_cache=cache)

def test_get_routes_for_stop_uses_cache(tmp_path, make_static_gtfs) -> None:
    """A second lookup is served from memory, without reading the ZIP again."""
    zip_path = make_static_gtfs(trips={"b1": ("B", ["A21S"])})
    cache = GTFSCache(cache_dir=tmp_path)

    assert cache.get_routes_for_stop(zip_path, "A21S") == {"B"}
    zip_path.unlink()  # reading the file again would now fail
    assert cache.get_routes_for_stop(zip_path, "A21S") == {"B"}

