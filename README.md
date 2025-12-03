# py-nymta

Python library for accessing MTA (Metropolitan Transportation Authority) real-time transit data for NYC.

## Features

- Simple, clean async API for accessing MTA real-time transit data
- Support for all MTA subway lines and bus routes
- Real-time bus arrivals and vehicle positions
- Compatible with Home Assistant (aiohttp-based)
- Optional session management - use your own aiohttp session or let the library manage it
- Uses gtfs-realtime-bindings 2.0.0+ (compatible with latest protobuf)
- Type hints for better IDE support

## Installation

```bash
pip install py-nymta
```

## Usage

### Subway - Basic Example

```python
import asyncio
from pymta import SubwayFeed

async def main():
    # Create a feed for the N/Q/R/W lines (library manages the session)
    async with SubwayFeed(feed_id="N") as feed:
        # Get the next 3 arrivals for the Q line at station B08S (southbound)
        arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")

        for arrival in arrivals:
            print(f"Route {arrival.route_id} to {arrival.destination}")
            print(f"  Arrives at: {arrival.arrival_time}")
            print(f"  Stop ID: {arrival.stop_id}")

asyncio.run(main())
```

### Bus - Basic Example

```python
import asyncio
from pymta import BusFeed

async def main():
    # Create a bus feed (requires MTA Bus Time API key)
    api_key = "YOUR_MTA_BUS_TIME_API_KEY"

    async with BusFeed(api_key=api_key) as feed:
        # Get the next 3 arrivals for the M15 bus at stop 400561
        arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561")

        for arrival in arrivals:
            print(f"Route {arrival.route_id} to {arrival.destination}")
            print(f"  Arrives at: {arrival.arrival_time}")
            print(f"  Stop ID: {arrival.stop_id}")

asyncio.run(main())
```

### Bus - Vehicle Positions

```python
import asyncio
from pymta import BusFeed

async def main():
    api_key = "YOUR_MTA_BUS_TIME_API_KEY"

    async with BusFeed(api_key=api_key) as feed:
        # Get all vehicle positions for the M15 route
        positions = await feed.get_vehicle_positions(route_id="M15")

        for pos in positions:
            print(f"Vehicle {pos['vehicle_id']} on route {pos['route_id']}")
            print(f"  Location: {pos['latitude']}, {pos['longitude']}")
            print(f"  Bearing: {pos['bearing']}")
            print(f"  Last updated: {pos['timestamp']}")

asyncio.run(main())
```

### Discovering Active Stops for a Route

Both `SubwayFeed` and `BusFeed` support discovering which stops are currently active (have scheduled arrivals) for a route:

```python
import asyncio
from pymta import SubwayFeed, BusFeed

async def main():
    # Get active subway stops for the Q train
    async with SubwayFeed(feed_id="N") as feed:
        stops = await feed.get_active_stops(route_id="Q")

        print("Active Q train stops:")
        for stop in stops:
            status = "has arrivals" if stop["has_arrivals"] else "no arrivals"
            print(f"  {stop['stop_id']}: {status}")

    # Get active bus stops for the M15
    api_key = "YOUR_MTA_BUS_TIME_API_KEY"
    async with BusFeed(api_key=api_key) as feed:
        stops = await feed.get_active_stops(route_id="M15")

        print("\nActive M15 bus stops:")
        for stop in stops:
            status = "has arrivals" if stop["has_arrivals"] else "no arrivals"
            print(f"  {stop['stop_id']}: {status}")

asyncio.run(main())
```

**Note**: This returns stops from the real-time feed (currently active). For a complete list of all stops on a route, use the `get_stops()` method below.

### Getting All Stops for a Route (Static GTFS)

Both `SubwayFeed` and `BusFeed` support getting the complete list of stops for a route from static GTFS data. This is useful for showing dropdown menus or complete route information:

```python
import asyncio
from pymta import SubwayFeed, BusFeed

async def main():
    # Get all subway stops for the Q train
    async with SubwayFeed(feed_id="N") as feed:
        stops = await feed.get_stops(route_id="Q")

        print("All Q train stops:")
        for stop in stops:
            print(f"  {stop['stop_id']}: {stop['stop_name']} (seq: {stop['stop_sequence']})")

    # Get all bus stops for the M15
    api_key = "YOUR_MTA_BUS_TIME_API_KEY"
    async with BusFeed(api_key=api_key) as feed:
        stops = await feed.get_stops(route_id="M15")

        print("\nAll M15 bus stops:")
        for stop in stops:
            print(f"  {stop['stop_id']}: {stop['stop_name']} (seq: {stop['stop_sequence']})")

asyncio.run(main())
```

**Note**: Static GTFS data is cached locally for 24 hours to improve performance and reduce API calls. The first call will download the GTFS ZIP file, subsequent calls will use the cached version.

### Finding the Feed ID for a Route

```python
import asyncio
from pymta import SubwayFeed

async def main():
    # Get the feed ID for a specific route
    feed_id = SubwayFeed.get_feed_id_for_route("Q")
    print(f"The Q line is in feed: {feed_id}")  # Output: N

    # Create a feed using the discovered feed_id
    async with SubwayFeed(feed_id=feed_id) as feed:
        arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")

asyncio.run(main())
```

### Custom Timeout and Max Arrivals

```python
import asyncio
from pymta import SubwayFeed

async def main():
    # Create a feed with custom timeout
    async with SubwayFeed(feed_id="1", timeout=60) as feed:
        # Get up to 5 arrivals instead of the default 3
        arrivals = await feed.get_arrivals(
            route_id="1",
            stop_id="127N",  # Times Square - 42 St (northbound)
            max_arrivals=5
        )

asyncio.run(main())
```

### Using Your Own aiohttp Session (Recommended for Home Assistant)

```python
import asyncio
import aiohttp
from pymta import SubwayFeed

async def main():
    # Provide your own aiohttp session for better connection pooling
    async with aiohttp.ClientSession() as session:
        feed = SubwayFeed(feed_id="N", session=session)

        # Make multiple requests using the same session
        q_arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")
        n_arrivals = await feed.get_arrivals(route_id="N", stop_id="B08S")

        print(f"Q train arrivals: {len(q_arrivals)}")
        print(f"N train arrivals: {len(n_arrivals)}")

asyncio.run(main())
```

### Error Handling

```python
import asyncio
from pymta import SubwayFeed, MTAFeedError

async def main():
    async with SubwayFeed(feed_id="A") as feed:
        try:
            arrivals = await feed.get_arrivals(route_id="A", stop_id="A42N")
        except MTAFeedError as e:
            print(f"Error fetching arrivals: {e}")

asyncio.run(main())
```

## Getting an MTA Bus Time API Key

To use the bus features, you need an MTA Bus Time API key:

1. Visit the [MTA Bus Time Developer Portal](https://bt.mta.info/wiki/Developers/Index)
2. Request an API key (you will receive one within 30 minutes)
3. Use the API key when creating a `BusFeed` instance

**Note**: The subway feeds do not require an API key.

## Station IDs and Directions

MTA station IDs include a direction suffix:
- `N` suffix: Northbound/Uptown direction
- `S` suffix: Southbound/Downtown direction

For example:
- `127N`: Times Square - 42 St (northbound)
- `127S`: Times Square - 42 St (southbound)
- `B08N`: DeKalb Av (northbound)
- `B08S`: DeKalb Av (southbound)

**Note**: These are MTA designations and don't always correspond to geographic north/south.

## Feed IDs

The MTA groups subway lines into feeds:

| Feed ID | Lines |
|---------|-------|
| `1` | 1, 2, 3, 4, 5, 6, GS |
| `A` | A, C, E, H, FS |
| `N` | N, Q, R, W |
| `B` | B, D, F, M |
| `L` | L |
| `SI` | SIR (Staten Island Railway) |
| `G` | G |
| `J` | J, Z |
| `7` | 7, 7X |

## Bus Stop IDs

Bus stop IDs are numeric values (e.g., `400561`, `308209`). You can find stop IDs:
- Using the [MTA Bus Time website](https://bustime.mta.info/)
- Through the OneBusAway API for stop discovery
- From MTA's static GTFS feeds

## API Reference

### `SubwayFeed`

Main class for accessing subway GTFS-RT feeds. Supports async context manager protocol.

#### `__init__(feed_id: str, timeout: int = 30, session: Optional[aiohttp.ClientSession] = None, gtfs_cache: Optional[GTFSCache] = None)`

Initialize the subway feed.

**Parameters:**
- `feed_id`: The feed ID (e.g., '1', 'A', 'N', 'B', 'L', 'SI', 'G', 'J', '7')
- `timeout`: Request timeout in seconds (default: 30)
- `session`: Optional aiohttp ClientSession. If not provided, a new session will be created for each request.
- `gtfs_cache`: Optional GTFSCache instance for static GTFS data caching. If not provided, a new cache will be created.

**Raises:**
- `ValueError`: If feed_id is not valid

#### `async get_arrivals(route_id: str, stop_id: str, max_arrivals: int = 3) -> list[Arrival]`

Get upcoming train arrivals for a specific route and stop.

**Parameters:**
- `route_id`: The route/line ID (e.g., '1', 'A', 'Q')
- `stop_id`: The stop ID including direction (e.g., '127N', 'B08S')
- `max_arrivals`: Maximum number of arrivals to return (default: 3)

**Returns:**
- List of `Arrival` objects sorted by arrival time

**Raises:**
- `MTAFeedError`: If feed cannot be fetched or parsed

#### `async get_stops(route_id: str) -> list[dict]`

Get all stops for a subway route from static GTFS data.

**Parameters:**
- `route_id`: The route/line ID (e.g., '1', 'A', 'Q')

**Returns:**
- List of dictionaries containing:
  - `stop_id`: The stop ID
  - `stop_name`: Stop name
  - `stop_sequence`: Order of stop on the route

**Raises:**
- `MTAFeedError`: If GTFS data cannot be fetched or parsed

**Note:** Static GTFS data is cached locally for 24 hours to improve performance.

#### `async get_active_stops(route_id: str) -> list[dict]`

Get all active stops for a subway route from the real-time feed.

**Parameters:**
- `route_id`: The route/line ID (e.g., '1', 'A', 'Q')

**Returns:**
- List of dictionaries containing:
  - `stop_id`: The stop ID (includes direction suffix like N/S)
  - `stop_name`: Stop name if available (typically None in real-time feeds)
  - `has_arrivals`: Whether there are currently arrivals at this stop

**Raises:**
- `MTAFeedError`: If feed cannot be fetched or parsed

#### `async close()`

Close the owned session if it exists. Only needed if not using the async context manager.

#### Async Context Manager

The `SubwayFeed` class supports the async context manager protocol:

```python
async with SubwayFeed(feed_id="N") as feed:
    arrivals = await feed.get_arrivals(route_id="Q", stop_id="B08S")
# Session is automatically closed when exiting the context
```

#### `get_feed_id_for_route(route_id: str) -> str` (static method)

Get the feed ID for a given route.

**Parameters:**
- `route_id`: The route/line ID (e.g., '1', 'A', 'Q')

**Returns:**
- The feed ID for the route

**Raises:**
- `ValueError`: If route_id is not valid

### `BusFeed`

Main class for accessing bus GTFS-RT feeds. Supports async context manager protocol.

#### `__init__(api_key: str, timeout: int = 30, session: Optional[aiohttp.ClientSession] = None, gtfs_cache: Optional[GTFSCache] = None)`

Initialize the bus feed.

**Parameters:**
- `api_key`: MTA Bus Time API key (get one at https://bt.mta.info/wiki/Developers/Index)
- `timeout`: Request timeout in seconds (default: 30)
- `session`: Optional aiohttp ClientSession. If not provided, a new session will be created for each request.
- `gtfs_cache`: Optional GTFSCache instance for static GTFS data caching. If not provided, a new cache will be created.

**Raises:**
- `ValueError`: If api_key is not provided

#### `async get_arrivals(route_id: str, stop_id: str, max_arrivals: int = 3) -> list[Arrival]`

Get upcoming bus arrivals for a specific route and stop.

**Parameters:**
- `route_id`: The bus route ID (e.g., 'M15', 'B46', 'Q10')
- `stop_id`: The stop ID (e.g., '400561', '308209')
- `max_arrivals`: Maximum number of arrivals to return (default: 3)

**Returns:**
- List of `Arrival` objects sorted by arrival time

**Raises:**
- `MTAFeedError`: If feed cannot be fetched or parsed

#### `async get_vehicle_positions(route_id: Optional[str] = None) -> list[dict]`

Get current vehicle positions for buses.

**Parameters:**
- `route_id`: Optional bus route ID to filter by (e.g., 'M15', 'B46'). If None, returns all vehicles.

**Returns:**
- List of vehicle position dictionaries containing:
  - `vehicle_id`: Vehicle identifier
  - `route_id`: Route identifier
  - `latitude`: Current latitude
  - `longitude`: Current longitude
  - `bearing`: Current bearing (0-359 degrees, or None)
  - `timestamp`: Last update timestamp (datetime object)

**Raises:**
- `MTAFeedError`: If feed cannot be fetched or parsed

#### `async get_stops(route_id: str) -> list[dict]`

Get all stops for a bus route from static GTFS data.

**Parameters:**
- `route_id`: The bus route ID (e.g., 'M15', 'B46', 'Q10')

**Returns:**
- List of dictionaries containing:
  - `stop_id`: The stop ID
  - `stop_name`: Stop name
  - `stop_sequence`: Order of stop on the route

**Raises:**
- `MTAFeedError`: If GTFS data cannot be fetched or parsed, or route not found

**Note:** This method searches all borough GTFS feeds to find the route. Static GTFS data is cached locally for 24 hours to improve performance.

#### `async get_active_stops(route_id: str) -> list[dict]`

Get all active stops for a bus route from the real-time feed.

**Parameters:**
- `route_id`: The bus route ID (e.g., 'M15', 'B46', 'Q10')

**Returns:**
- List of dictionaries containing:
  - `stop_id`: The stop ID
  - `stop_name`: Stop name if available (typically None in real-time feeds)
  - `has_arrivals`: Whether there are currently arrivals at this stop

**Raises:**
- `MTAFeedError`: If feed cannot be fetched or parsed

#### `async close()`

Close the owned session if it exists. Only needed if not using the async context manager.

#### Async Context Manager

The `BusFeed` class supports the async context manager protocol:

```python
async with BusFeed(api_key="YOUR_KEY") as feed:
    arrivals = await feed.get_arrivals(route_id="M15", stop_id="400561")
# Session is automatically closed when exiting the context
```

### `Arrival`

Dataclass representing a single train arrival.

**Attributes:**
- `arrival_time` (datetime): The datetime when the train will arrive (UTC)
- `route_id` (str): The route/line ID (e.g., '1', 'A', 'Q')
- `stop_id` (str): The stop ID including direction (e.g., '127N', 'B08S')
- `destination` (str): The trip headsign/destination

### Exceptions

- `MTAError`: Base exception for the library
- `MTAFeedError`: Raised when feed cannot be fetched or parsed

## Development

### Setup

```bash
git clone https://github.com/OnFreund/py-nymta.git
cd py-nymta
pip install -e .
```

### Running Tests

```bash
pytest
```

## License

MIT License - see LICENSE file for details.

## Credits

This library uses the official GTFS-RT protocol buffers from Google's [gtfs-realtime-bindings](https://github.com/MobilityData/gtfs-realtime-bindings) package.

MTA data is provided by the [Metropolitan Transportation Authority](https://www.mta.info/).
