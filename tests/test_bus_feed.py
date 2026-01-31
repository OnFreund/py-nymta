"""Tests for BusFeed class."""

from datetime import datetime, timedelta, timezone
from unittest.mock import AsyncMock, Mock, patch

from google.transit import gtfs_realtime_pb2
import pytest

from pymta import Arrival, BusFeed, MTAFeedError


def test_init_with_api_key():
    """Test initialization with valid API key."""
    feed = BusFeed(api_key="test_api_key")
    assert feed.api_key == "test_api_key"
    assert feed.timeout == 30


def test_init_without_api_key():
    """Test initialization without API key raises ValueError."""
    with pytest.raises(ValueError, match="API key is required"):
        BusFeed(api_key="")


@pytest.mark.asyncio
async def test_get_arrivals_success():
    """Test getting bus arrivals successfully with destination from last stop."""
    from pymta.gtfs_static import GTFSCache

    # Create a GTFS-RT FeedMessage
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Create a trip update entity
    entity = feed_message.entity.add()
    entity.id = "trip1"

    trip_update = entity.trip_update
    trip_update.trip.route_id = "M15"

    # Add stop time updates - bus passes through 400561 and ends at 400999
    stop_time1 = trip_update.stop_time_update.add()
    stop_time1.stop_id = "400561"
    stop_time1.arrival.time = int(datetime.now(timezone.utc).timestamp() + 300)

    stop_time2 = trip_update.stop_time_update.add()
    stop_time2.stop_id = "400999"  # Last stop - South Ferry
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
    mock_cache.get_combined_stop_names = AsyncMock(return_value={
        "400561": "1 Av/E 79 St",
        "400999": "South Ferry",
    })

    # Test
    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561")

    # Verify the request was made with the API key
    call_args = mock_session.get.call_args
    assert call_args[1]["params"]["key"] == "test_key"

    assert len(arrivals) == 1
    assert arrivals[0].route_id == "M15"
    assert arrivals[0].stop_id == "400561"
    assert arrivals[0].destination == "South Ferry"
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

    # Create a trip update entity
    entity = feed_message.entity.add()
    entity.id = "trip1"

    trip_update = entity.trip_update
    trip_update.trip.route_id = "M15"

    # Add stop time update
    stop_time = trip_update.stop_time_update.add()
    stop_time.stop_id = "400561"
    future_time = datetime.now(timezone.utc).timestamp() + 300
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

    # Mock GTFSCache with empty stop names (fallback case)
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))
    mock_cache.get_combined_stop_names = AsyncMock(return_value={})

    # Test
    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561")

    assert len(arrivals) == 1
    assert arrivals[0].destination == "M15 bus"  # Default when no stop names


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
    trip_update.trip.route_id = "M15"

    stop_time = trip_update.stop_time_update.add()
    stop_time.stop_id = "400561"
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
    mock_cache.get_combined_stop_names = AsyncMock(return_value={})

    # Test
    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561")

    assert len(arrivals) == 0


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
        trip_update.trip.route_id = "M15"

        stop_time = trip_update.stop_time_update.add()
        stop_time.stop_id = "400561"
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
    mock_cache.get_combined_stop_names = AsyncMock(return_value={})

    # Test
    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561", max_arrivals=3)

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
    mock_cache.get_combined_stop_names = AsyncMock(return_value={})

    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    with pytest.raises(MTAFeedError, match="Error fetching GTFS-RT feed"):
        await feed.get_arrivals(route_id="M15", stop_id="400561")


@pytest.mark.asyncio
async def test_get_arrivals_filters_by_route():
    """Test that arrivals are filtered by route_id."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Create a GTFS-RT FeedMessage with multiple routes
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Add M15 route
    entity1 = feed_message.entity.add()
    entity1.id = "trip1"
    trip_update1 = entity1.trip_update
    trip_update1.trip.route_id = "M15"
    stop_time1 = trip_update1.stop_time_update.add()
    stop_time1.stop_id = "400561"
    stop_time1.arrival.time = int(datetime.now(timezone.utc).timestamp() + 300)

    # Add M34 route at same stop
    entity2 = feed_message.entity.add()
    entity2.id = "trip2"
    trip_update2 = entity2.trip_update
    trip_update2.trip.route_id = "M34"
    stop_time2 = trip_update2.stop_time_update.add()
    stop_time2.stop_id = "400561"
    stop_time2.arrival.time = int(datetime.now(timezone.utc).timestamp() + 600)

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
    mock_cache.get_combined_stop_names = AsyncMock(return_value={})

    # Test - should only get M15
    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561")

    assert len(arrivals) == 1
    assert arrivals[0].route_id == "M15"


@pytest.mark.asyncio
async def test_get_vehicle_positions_success():
    """Test getting vehicle positions successfully."""
    # Create a GTFS-RT FeedMessage
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Create a vehicle position entity
    entity = feed_message.entity.add()
    entity.id = "vehicle1"

    vehicle = entity.vehicle
    vehicle.trip.route_id = "M15"
    vehicle.vehicle.id = "MTA_1234"
    vehicle.position.latitude = 40.7128
    vehicle.position.longitude = -74.0060
    vehicle.position.bearing = 180.0
    vehicle.timestamp = int(datetime.now(timezone.utc).timestamp())

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
    feed = BusFeed(api_key="test_key", session=mock_session)
    positions = await feed.get_vehicle_positions()

    assert len(positions) == 1
    assert positions[0]["vehicle_id"] == "MTA_1234"
    assert positions[0]["route_id"] == "M15"
    assert abs(positions[0]["latitude"] - 40.7128) < 0.0001
    assert abs(positions[0]["longitude"] - -74.0060) < 0.0001
    assert abs(positions[0]["bearing"] - 180.0) < 0.1
    assert isinstance(positions[0]["timestamp"], datetime)


@pytest.mark.asyncio
async def test_get_vehicle_positions_filtered_by_route():
    """Test filtering vehicle positions by route_id."""
    # Create a GTFS-RT FeedMessage with multiple vehicles
    feed_message = gtfs_realtime_pb2.FeedMessage()
    feed_message.header.gtfs_realtime_version = "2.0"
    feed_message.header.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Add M15 vehicle
    entity1 = feed_message.entity.add()
    entity1.id = "vehicle1"
    vehicle1 = entity1.vehicle
    vehicle1.trip.route_id = "M15"
    vehicle1.vehicle.id = "MTA_1234"
    vehicle1.position.latitude = 40.7128
    vehicle1.position.longitude = -74.0060
    vehicle1.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Add M34 vehicle
    entity2 = feed_message.entity.add()
    entity2.id = "vehicle2"
    vehicle2 = entity2.vehicle
    vehicle2.trip.route_id = "M34"
    vehicle2.vehicle.id = "MTA_5678"
    vehicle2.position.latitude = 40.7200
    vehicle2.position.longitude = -73.9900
    vehicle2.timestamp = int(datetime.now(timezone.utc).timestamp())

    # Mock response
    mock_response = AsyncMock()
    mock_response.read = AsyncMock(return_value=feed_message.SerializeToString())
    mock_response.raise_for_status = Mock()
    mock_response.__aenter__ = AsyncMock(return_value=mock_response)
    mock_response.__aexit__ = AsyncMock(return_value=None)

    # Mock session
    mock_session = Mock()
    mock_session.get = Mock(return_value=mock_response)

    # Test - filter by M15
    feed = BusFeed(api_key="test_key", session=mock_session)
    positions = await feed.get_vehicle_positions(route_id="M15")

    assert len(positions) == 1
    assert positions[0]["vehicle_id"] == "MTA_1234"
    assert positions[0]["route_id"] == "M15"


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
    trip_update.trip.route_id = "M15"

    # Stop 1 - with future arrival
    stop_time1 = trip_update.stop_time_update.add()
    stop_time1.stop_id = "400561"
    stop_time1.arrival.time = int(datetime.now(timezone.utc).timestamp() + 300)

    # Stop 2 - with future arrival
    stop_time2 = trip_update.stop_time_update.add()
    stop_time2.stop_id = "400562"
    stop_time2.arrival.time = int(datetime.now(timezone.utc).timestamp() + 600)

    # Add another trip with a different stop
    entity2 = feed_message.entity.add()
    entity2.id = "trip2"
    trip_update2 = entity2.trip_update
    trip_update2.trip.route_id = "M15"

    stop_time3 = trip_update2.stop_time_update.add()
    stop_time3.stop_id = "400563"
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
    feed = BusFeed(api_key="test_key", session=mock_session)
    stops = await feed.get_active_stops(route_id="M15")

    assert len(stops) == 3
    assert stops[0]["stop_id"] == "400561"
    assert stops[1]["stop_id"] == "400562"
    assert stops[2]["stop_id"] == "400563"
    assert all(stop["has_arrivals"] for stop in stops)
    assert all(stop["stop_name"] is None for stop in stops)


@pytest.mark.asyncio
async def test_context_manager():
    """Test BusFeed as async context manager."""
    async with BusFeed(api_key="test_key") as feed:
        assert feed.api_key == "test_key"
    # Ensure close() was called
    assert feed._owned_session is None


@pytest.mark.asyncio
async def test_get_stops():
    """Test getting static stops for a route."""
    from pathlib import Path
    from pymta.gtfs_static import GTFSCache

    # Mock GTFSCache
    mock_cache = Mock(spec=GTFSCache)
    mock_cache.download_gtfs = AsyncMock(return_value=Path("/fake/path.zip"))

    # Simulate searching through borough feeds - return empty for first two, then results for manhattan
    mock_cache.parse_stops_for_route = Mock(side_effect=[
        [],  # bronx - no results
        [],  # brooklyn - no results
        [    # manhattan - found!
            {"stop_id": "400561", "stop_name": "1 Av/E 79 St", "stop_sequence": 1},
            {"stop_id": "400562", "stop_name": "1 Av/E 72 St", "stop_sequence": 2},
            {"stop_id": "400563", "stop_name": "1 Av/E 67 St", "stop_sequence": 3},
        ],
    ])

    # Mock session
    mock_session = Mock()

    # Test
    feed = BusFeed(api_key="test_key", session=mock_session, gtfs_cache=mock_cache)
    stops = await feed.get_stops(route_id="M15")

    assert len(stops) == 3
    assert stops[0]["stop_id"] == "400561"
    assert stops[0]["stop_name"] == "1 Av/E 79 St"
    assert stops[0]["stop_sequence"] == 1
    assert stops[1]["stop_id"] == "400562"
    assert stops[1]["stop_name"] == "1 Av/E 72 St"
    assert stops[2]["stop_id"] == "400563"
    assert stops[2]["stop_name"] == "1 Av/E 67 St"

    # Verify cache was called - should have called download 3 times (bronx, brooklyn, manhattan)
    # and stopped when it found results in manhattan
    assert mock_cache.download_gtfs.call_count == 3
    assert mock_cache.parse_stops_for_route.call_count == 3
