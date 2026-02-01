"""py-nymta library for accessing NYC transit real-time data."""

from datetime import datetime, timezone
from typing import Optional

import aiohttp
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2

from .constants import FEED_URLS, LINE_TO_FEED, BUS_FEED_URLS, STATIC_GTFS_URLS
from .models import Arrival
from .gtfs_static import GTFSCache

__version__ = "0.3.1"
__all__ = ["SubwayFeed", "BusFeed", "Arrival", "MTAError", "MTAFeedError"]


class MTAError(Exception):
    """Base exception for py-nymta library."""


class MTAFeedError(MTAError):
    """Exception raised when feed cannot be fetched or parsed."""


def _get_terminal_stop_id(stop_time_updates) -> Optional[str]:
    """Get the terminal (last) stop ID from a list of stop time updates.

    GTFS-RT does not guarantee stop_time_update ordering, so we find the terminal
    stop by looking for the highest stop_sequence, or falling back to the latest
    arrival/departure time.

    Args:
        stop_time_updates: List of StopTimeUpdate protobuf messages.

    Returns:
        The stop_id of the terminal stop, or None if no stops found.
    """
    if not stop_time_updates:
        return None

    # Try to find terminal by stop_sequence (preferred method)
    max_sequence = -1
    terminal_by_sequence = None
    for stu in stop_time_updates:
        if stu.stop_sequence > max_sequence:
            max_sequence = stu.stop_sequence
            terminal_by_sequence = stu.stop_id

    # If we found a valid sequence (> 0), use it
    if max_sequence > 0:
        return terminal_by_sequence

    # Fall back to latest arrival/departure time
    max_time = -1
    terminal_by_time = None
    for stu in stop_time_updates:
        # Check arrival time
        if stu.HasField("arrival") and stu.arrival.time > max_time:
            max_time = stu.arrival.time
            terminal_by_time = stu.stop_id
        # Check departure time
        if stu.HasField("departure") and stu.departure.time > max_time:
            max_time = stu.departure.time
            terminal_by_time = stu.stop_id

    if terminal_by_time:
        return terminal_by_time

    # Last resort: return the last item (original behavior)
    return stop_time_updates[-1].stop_id if stop_time_updates else None


class SubwayFeed:
    """Interface for MTA subway real-time feeds."""

    def __init__(
        self,
        feed_id: str,
        timeout: int = 30,
        session: Optional[aiohttp.ClientSession] = None,
        gtfs_cache: Optional[GTFSCache] = None,
    ) -> None:
        """Initialize the subway feed.

        Args:
            feed_id: The feed ID (e.g., '1', 'A', 'N', 'B', 'L', 'SI', 'G', 'J', '7').
            timeout: Request timeout in seconds (default: 30).
            session: Optional aiohttp ClientSession. If not provided, a new session
                will be created for each request.
            gtfs_cache: Optional GTFSCache instance. If not provided, a new cache will be created.

        Raises:
            ValueError: If feed_id is not valid.
        """
        if feed_id not in FEED_URLS:
            raise ValueError(
                f"Invalid feed_id '{feed_id}'. "
                f"Must be one of: {', '.join(FEED_URLS.keys())}"
            )

        self.feed_id = feed_id
        self.feed_url = FEED_URLS[feed_id]
        self.timeout = timeout
        self._session = session
        self._owned_session: Optional[aiohttp.ClientSession] = None
        self._gtfs_cache = gtfs_cache or GTFSCache()

    async def __aenter__(self) -> "SubwayFeed":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the owned session if it exists."""
        if self._owned_session is not None:
            await self._owned_session.close()
            self._owned_session = None

    async def get_arrivals(
        self,
        route_id: str,
        stop_id: str,
        max_arrivals: int = 3,
    ) -> list[Arrival]:
        """Get upcoming train arrivals for a specific route and stop.

        Args:
            route_id: The route/line ID (e.g., '1', 'A', 'Q').
            stop_id: The stop ID including direction (e.g., '127N', 'B08S').
            max_arrivals: Maximum number of arrivals to return (default: 3).

        Returns:
            List of Arrival objects sorted by arrival time.

        Raises:
            MTAFeedError: If feed cannot be fetched or parsed.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        # Get stop names from static GTFS for destination lookup
        try:
            gtfs_url = STATIC_GTFS_URLS["subway"]
            zip_path = await self._gtfs_cache.download_gtfs(
                url=gtfs_url,
                feed_name="subway",
                session=session,
                timeout=self.timeout,
            )
            stop_names = self._gtfs_cache.get_stop_names(zip_path)
        except Exception:
            # Fall back to empty dict if static GTFS fails
            stop_names = {}

        # Fetch the GTFS-RT feed
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.get(self.feed_url, timeout=timeout) as response:
                response.raise_for_status()
                content = await response.read()
        except aiohttp.ClientError as err:
            raise MTAFeedError(f"Error fetching GTFS-RT feed: {err}") from err
        except TimeoutError as err:
            raise MTAFeedError(f"Timeout fetching GTFS-RT feed: {err}") from err

        # Parse the protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except DecodeError as err:
            raise MTAFeedError(f"Error parsing GTFS-RT feed: {err}") from err

        arrivals: list[Arrival] = []
        now = datetime.now(timezone.utc)

        # Get base station ID (without direction suffix) for flexible matching
        base_station_id = stop_id.rstrip("NS")
        direction_suffix = stop_id[-1] if stop_id and stop_id[-1] in ("N", "S") else ""

        # Process each entity in the feed
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            trip = trip_update.trip

            # Filter by route/line
            if trip.route_id != route_id:
                continue

            # Get destination from the terminal stop in this trip
            last_stop_id = _get_terminal_stop_id(trip_update.stop_time_update)

            # Look up destination name, fall back to route_id if not found
            if last_stop_id and last_stop_id in stop_names:
                destination = stop_names[last_stop_id]
            elif last_stop_id:
                # Try without a single direction suffix (N/S)
                if last_stop_id.endswith(("N", "S")):
                    base_last_stop = last_stop_id[:-1]
                else:
                    base_last_stop = last_stop_id
                destination = stop_names.get(base_last_stop, f"{trip.route_id} train")
            else:
                destination = f"{trip.route_id} train"

            # Process stop time updates
            for stop_time_update in trip_update.stop_time_update:
                current_stop_id = stop_time_update.stop_id

                # Match on base station ID and direction suffix
                if (
                    current_stop_id
                    and current_stop_id.startswith(base_station_id)
                    and current_stop_id.endswith(direction_suffix)
                    and stop_time_update.HasField("arrival")
                ):
                    # Get the arrival time
                    arrival_timestamp = stop_time_update.arrival.time
                    arrival_time = datetime.fromtimestamp(
                        arrival_timestamp, tz=timezone.utc
                    )

                    # Only include future arrivals
                    if arrival_time > now:
                        arrivals.append(
                            Arrival(
                                arrival_time=arrival_time,
                                route_id=trip.route_id,
                                stop_id=current_stop_id,
                                destination=destination,
                            )
                        )

        # Sort by arrival time and limit to max_arrivals
        arrivals.sort()
        return arrivals[:max_arrivals]

    async def get_stops(
        self,
        route_id: str,
    ) -> list[dict]:
        """Get all stops for a subway route from static GTFS data.

        Args:
            route_id: The route/line ID (e.g., '1', 'A', 'Q').

        Returns:
            List of dictionaries containing stop information:
                - stop_id: The stop ID
                - stop_name: Stop name
                - stop_sequence: Order of stop on the route

        Raises:
            MTAFeedError: If GTFS data cannot be fetched or parsed.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        try:
            # Download/cache the static GTFS feed
            gtfs_url = STATIC_GTFS_URLS["subway"]
            zip_path = await self._gtfs_cache.download_gtfs(
                url=gtfs_url,
                feed_name="subway",
                session=session,
                timeout=self.timeout,
            )

            # Parse stops for this route
            stops = self._gtfs_cache.parse_stops_for_route(zip_path, route_id)
            return stops

        except Exception as err:
            raise MTAFeedError(f"Error fetching static GTFS data: {err}") from err

    async def get_active_stops(
        self,
        route_id: str,
    ) -> list[dict]:
        """Get all active stops for a subway route from the real-time feed.

        Args:
            route_id: The route/line ID (e.g., '1', 'A', 'Q').

        Returns:
            List of dictionaries containing stop information:
                - stop_id: The stop ID (includes direction suffix like N/S)
                - stop_name: Stop name if available (may be None)
                - has_arrivals: Whether there are currently arrivals at this stop

        Raises:
            MTAFeedError: If feed cannot be fetched or parsed.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        # Fetch the GTFS-RT feed
        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.get(self.feed_url, timeout=timeout) as response:
                response.raise_for_status()
                content = await response.read()
        except aiohttp.ClientError as err:
            raise MTAFeedError(f"Error fetching GTFS-RT feed: {err}") from err
        except TimeoutError as err:
            raise MTAFeedError(f"Timeout fetching GTFS-RT feed: {err}") from err

        # Parse the protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except DecodeError as err:
            raise MTAFeedError(f"Error parsing GTFS-RT feed: {err}") from err

        # Collect unique stops
        stops_dict = {}
        now = datetime.now(timezone.utc)

        # Process each entity in the feed
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            trip = trip_update.trip

            # Filter by route/line
            if trip.route_id != route_id:
                continue

            # Process stop time updates
            for stop_time_update in trip_update.stop_time_update:
                stop_id = stop_time_update.stop_id

                if stop_id and stop_id not in stops_dict:
                    # Check if there are future arrivals at this stop
                    has_arrivals = False
                    if stop_time_update.HasField("arrival"):
                        arrival_time = datetime.fromtimestamp(
                            stop_time_update.arrival.time, tz=timezone.utc
                        )
                        has_arrivals = arrival_time > now

                    stops_dict[stop_id] = {
                        "stop_id": stop_id,
                        "stop_name": None,  # Real-time feed doesn't include stop names
                        "has_arrivals": has_arrivals,
                    }

        # Convert to sorted list
        stops = sorted(stops_dict.values(), key=lambda x: x["stop_id"])
        return stops

    @staticmethod
    def get_feed_id_for_route(route_id: str) -> str:
        """Get the feed ID for a given route.

        Args:
            route_id: The route/line ID (e.g., '1', 'A', 'Q').

        Returns:
            The feed ID for the route.

        Raises:
            ValueError: If route_id is not valid.
        """
        if route_id not in LINE_TO_FEED:
            raise ValueError(
                f"Invalid route_id '{route_id}'. "
                f"Must be one of: {', '.join(LINE_TO_FEED.keys())}"
            )
        return LINE_TO_FEED[route_id]


class BusFeed:
    """Interface for MTA bus real-time feeds."""

    def __init__(
        self,
        api_key: str,
        timeout: int = 30,
        session: Optional[aiohttp.ClientSession] = None,
        gtfs_cache: Optional[GTFSCache] = None,
    ) -> None:
        """Initialize the bus feed.

        Args:
            api_key: MTA Bus Time API key (get one at https://bt.mta.info/wiki/Developers/Index).
            timeout: Request timeout in seconds (default: 30).
            session: Optional aiohttp ClientSession. If not provided, a new session
                will be created for each request.
            gtfs_cache: Optional GTFSCache instance. If not provided, a new cache will be created.
        """
        if not api_key:
            raise ValueError("API key is required for bus feeds")

        self.api_key = api_key
        self.timeout = timeout
        self._session = session
        self._owned_session: Optional[aiohttp.ClientSession] = None
        self._gtfs_cache = gtfs_cache or GTFSCache()

    async def __aenter__(self) -> "BusFeed":
        """Async context manager entry."""
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Async context manager exit."""
        await self.close()

    async def close(self) -> None:
        """Close the owned session if it exists."""
        if self._owned_session is not None:
            await self._owned_session.close()
            self._owned_session = None

    async def get_arrivals(
        self,
        route_id: str,
        stop_id: str,
        max_arrivals: int = 3,
    ) -> list[Arrival]:
        """Get upcoming bus arrivals for a specific route and stop.

        Args:
            route_id: The bus route ID (e.g., 'M15', 'B46', 'Q10').
            stop_id: The stop ID (e.g., '400561', '308209').
            max_arrivals: Maximum number of arrivals to return (default: 3).

        Returns:
            List of Arrival objects sorted by arrival time.

        Raises:
            MTAFeedError: If feed cannot be fetched or parsed.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        # Get stop names from static GTFS for destination lookup
        # Uses cached combined dict to avoid rebuilding on every call
        borough_feeds = [
            ("bus_bronx", STATIC_GTFS_URLS["bus_bronx"]),
            ("bus_brooklyn", STATIC_GTFS_URLS["bus_brooklyn"]),
            ("bus_manhattan", STATIC_GTFS_URLS["bus_manhattan"]),
            ("bus_queens", STATIC_GTFS_URLS["bus_queens"]),
            ("bus_staten_island", STATIC_GTFS_URLS["bus_staten_island"]),
        ]
        try:
            stop_names = await self._gtfs_cache.get_combined_stop_names(
                feeds=borough_feeds,
                session=session,
                timeout=self.timeout,
            )
        except Exception:
            stop_names = {}

        # Fetch the GTFS-RT trip updates feed
        feed_url = BUS_FEED_URLS["trip_updates"]
        params = {"key": self.api_key}

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.get(
                feed_url, params=params, timeout=timeout
            ) as response:
                response.raise_for_status()
                content = await response.read()
        except aiohttp.ClientError as err:
            raise MTAFeedError(f"Error fetching GTFS-RT feed: {err}") from err
        except TimeoutError as err:
            raise MTAFeedError(f"Timeout fetching GTFS-RT feed: {err}") from err

        # Parse the protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except DecodeError as err:
            raise MTAFeedError(f"Error parsing GTFS-RT feed: {err}") from err

        arrivals: list[Arrival] = []
        now = datetime.now(timezone.utc)

        # Process each entity in the feed
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            trip = trip_update.trip

            # Filter by route
            if trip.route_id != route_id:
                continue

            # Get destination from the terminal stop in this trip
            last_stop_id = _get_terminal_stop_id(trip_update.stop_time_update)

            # Look up destination name, fall back to route_id if not found
            if last_stop_id and last_stop_id in stop_names:
                destination = stop_names[last_stop_id]
            else:
                destination = f"{trip.route_id} bus"

            # Process stop time updates
            for stop_time_update in trip_update.stop_time_update:
                current_stop_id = stop_time_update.stop_id

                # Match on stop ID
                if (
                    current_stop_id == stop_id
                    and stop_time_update.HasField("arrival")
                ):
                    # Get the arrival time
                    arrival_timestamp = stop_time_update.arrival.time
                    arrival_time = datetime.fromtimestamp(
                        arrival_timestamp, tz=timezone.utc
                    )

                    # Only include future arrivals
                    if arrival_time > now:
                        arrivals.append(
                            Arrival(
                                arrival_time=arrival_time,
                                route_id=trip.route_id,
                                stop_id=current_stop_id,
                                destination=destination,
                            )
                        )

        # Sort by arrival time and limit to max_arrivals
        arrivals.sort()
        return arrivals[:max_arrivals]

    async def get_stops(
        self,
        route_id: str,
    ) -> list[dict]:
        """Get all stops for a bus route from static GTFS data.

        Args:
            route_id: The bus route ID (e.g., 'M15', 'B46', 'Q10').

        Returns:
            List of dictionaries containing stop information:
                - stop_id: The stop ID
                - stop_name: Stop name
                - stop_sequence: Order of stop on the route

        Raises:
            MTAFeedError: If GTFS data cannot be fetched or parsed, or route not found.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        try:
            # Search all borough feeds for the route
            borough_feeds = [
                ("bus_bronx", STATIC_GTFS_URLS["bus_bronx"]),
                ("bus_brooklyn", STATIC_GTFS_URLS["bus_brooklyn"]),
                ("bus_manhattan", STATIC_GTFS_URLS["bus_manhattan"]),
                ("bus_queens", STATIC_GTFS_URLS["bus_queens"]),
                ("bus_staten_island", STATIC_GTFS_URLS["bus_staten_island"]),
            ]

            for feed_name, gtfs_url in borough_feeds:
                # Download/cache the static GTFS feed
                zip_path = await self._gtfs_cache.download_gtfs(
                    url=gtfs_url,
                    feed_name=feed_name,
                    session=session,
                    timeout=self.timeout,
                )

                # Try to parse stops for this route
                stops = self._gtfs_cache.parse_stops_for_route(zip_path, route_id)
                if stops:
                    return stops

            # Route not found in any borough feed
            raise MTAFeedError(f"Route {route_id} not found in any borough GTFS feed")

        except MTAFeedError:
            raise
        except Exception as err:
            raise MTAFeedError(f"Error fetching static GTFS data: {err}") from err

    async def get_active_stops(
        self,
        route_id: str,
    ) -> list[dict]:
        """Get all active stops for a bus route from the real-time feed.

        Args:
            route_id: The bus route ID (e.g., 'M15', 'B46', 'Q10').

        Returns:
            List of dictionaries containing stop information:
                - stop_id: The stop ID
                - stop_name: Stop name if available (may be None)
                - has_arrivals: Whether there are currently arrivals at this stop

        Raises:
            MTAFeedError: If feed cannot be fetched or parsed.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        # Fetch the GTFS-RT trip updates feed
        feed_url = BUS_FEED_URLS["trip_updates"]
        params = {"key": self.api_key}

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.get(
                feed_url, params=params, timeout=timeout
            ) as response:
                response.raise_for_status()
                content = await response.read()
        except aiohttp.ClientError as err:
            raise MTAFeedError(f"Error fetching GTFS-RT feed: {err}") from err
        except TimeoutError as err:
            raise MTAFeedError(f"Timeout fetching GTFS-RT feed: {err}") from err

        # Parse the protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except DecodeError as err:
            raise MTAFeedError(f"Error parsing GTFS-RT feed: {err}") from err

        # Collect unique stops
        stops_dict = {}
        now = datetime.now(timezone.utc)

        # Process each entity in the feed
        for entity in feed.entity:
            if not entity.HasField("trip_update"):
                continue

            trip_update = entity.trip_update
            trip = trip_update.trip

            # Filter by route
            if trip.route_id != route_id:
                continue

            # Process stop time updates
            for stop_time_update in trip_update.stop_time_update:
                stop_id = stop_time_update.stop_id

                if stop_id and stop_id not in stops_dict:
                    # Check if there are future arrivals at this stop
                    has_arrivals = False
                    if stop_time_update.HasField("arrival"):
                        arrival_time = datetime.fromtimestamp(
                            stop_time_update.arrival.time, tz=timezone.utc
                        )
                        has_arrivals = arrival_time > now

                    stops_dict[stop_id] = {
                        "stop_id": stop_id,
                        "stop_name": None,  # Real-time feed doesn't include stop names
                        "has_arrivals": has_arrivals,
                    }

        # Convert to sorted list
        stops = sorted(stops_dict.values(), key=lambda x: x["stop_id"])
        return stops

    async def get_vehicle_positions(
        self,
        route_id: Optional[str] = None,
    ) -> list[dict]:
        """Get current vehicle positions for buses.

        Args:
            route_id: Optional bus route ID to filter by (e.g., 'M15', 'B46').
                If None, returns all vehicles.

        Returns:
            List of vehicle position dictionaries containing:
                - vehicle_id: Vehicle identifier
                - route_id: Route identifier
                - latitude: Current latitude
                - longitude: Current longitude
                - bearing: Current bearing (0-359 degrees)
                - timestamp: Last update timestamp

        Raises:
            MTAFeedError: If feed cannot be fetched or parsed.
        """
        # Get or create session
        session = self._session or self._owned_session
        if session is None:
            session = aiohttp.ClientSession()
            self._owned_session = session

        # Fetch the GTFS-RT vehicle positions feed
        feed_url = BUS_FEED_URLS["vehicle_positions"]
        params = {"key": self.api_key}

        try:
            timeout = aiohttp.ClientTimeout(total=self.timeout)
            async with session.get(
                feed_url, params=params, timeout=timeout
            ) as response:
                response.raise_for_status()
                content = await response.read()
        except aiohttp.ClientError as err:
            raise MTAFeedError(f"Error fetching GTFS-RT feed: {err}") from err
        except TimeoutError as err:
            raise MTAFeedError(f"Timeout fetching GTFS-RT feed: {err}") from err

        # Parse the protobuf
        feed = gtfs_realtime_pb2.FeedMessage()
        try:
            feed.ParseFromString(content)
        except DecodeError as err:
            raise MTAFeedError(f"Error parsing GTFS-RT feed: {err}") from err

        positions = []

        # Process each entity in the feed
        for entity in feed.entity:
            if not entity.HasField("vehicle"):
                continue

            vehicle = entity.vehicle

            # Filter by route if specified
            if route_id and vehicle.trip.route_id != route_id:
                continue

            # Extract position information
            if vehicle.HasField("position"):
                position_data = {
                    "vehicle_id": vehicle.vehicle.id if vehicle.HasField("vehicle") else entity.id,
                    "route_id": vehicle.trip.route_id,
                    "latitude": vehicle.position.latitude,
                    "longitude": vehicle.position.longitude,
                    "bearing": vehicle.position.bearing if vehicle.position.HasField("bearing") else None,
                    "timestamp": datetime.fromtimestamp(
                        vehicle.timestamp, tz=timezone.utc
                    ) if vehicle.HasField("timestamp") else None,
                }
                positions.append(position_data)

        return positions
