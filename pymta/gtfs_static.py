"""Static GTFS feed parsing and caching."""

import asyncio
import csv
import io
import zipfile
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional

import aiofiles
import aiohttp


class GTFSCache:
    """Cache for static GTFS data with expiration."""

    def __init__(self, cache_dir: Optional[Path] = None, ttl_hours: int = 168):
        """Initialize GTFS cache.

        Args:
            cache_dir: Directory to store cached GTFS files. Defaults to ~/.cache/pymta.
            ttl_hours: Time-to-live for cached data in hours (default: 168, i.e., 1 week).
        """
        if cache_dir is None:
            cache_dir = Path.home() / ".cache" / "pymta"
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(parents=True, exist_ok=True)
        self.ttl_hours = ttl_hours
        self._route_stops_cache = {}
        self._stop_names_cache = {}
        self._cache_timestamps = {}
        self._download_locks: dict[str, asyncio.Lock] = {}

    def _get_cache_path(self, feed_name: str) -> Path:
        """Get cache file path for a feed."""
        return self.cache_dir / f"{feed_name}.zip"

    def _is_cache_valid(self, feed_name: str) -> bool:
        """Check if cached file exists and is within TTL."""
        cache_path = self._get_cache_path(feed_name)
        if not cache_path.exists():
            return False

        # Check file modification time
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        age = datetime.now() - mtime
        return age < timedelta(hours=self.ttl_hours)

    def _invalidate_feed_caches(self, feed_name: str) -> None:
        """Invalidate all memory caches related to a feed.

        Called when a new ZIP file is downloaded to ensure stale data isn't served.
        """
        # Keys to remove from caches
        keys_to_remove = []

        # Find all cache keys that start with this feed name
        for cache_key in list(self._stop_names_cache.keys()):
            if cache_key.startswith(feed_name) or f"_{feed_name}_" in cache_key or cache_key.endswith(f"_{feed_name}_combined"):
                keys_to_remove.append(cache_key)

        for cache_key in list(self._route_stops_cache.keys()):
            if cache_key.startswith(feed_name):
                keys_to_remove.append(cache_key)

        # Remove from all caches
        for key in keys_to_remove:
            self._stop_names_cache.pop(key, None)
            self._route_stops_cache.pop(key, None)
            self._cache_timestamps.pop(key, None)

        # Also invalidate any combined cache that includes this feed
        combined_keys_to_remove = [
            k for k in self._stop_names_cache.keys()
            if k.endswith("_combined") and feed_name in k
        ]
        for key in combined_keys_to_remove:
            self._stop_names_cache.pop(key, None)
            self._cache_timestamps.pop(key, None)

    async def download_gtfs(
        self,
        url: str,
        feed_name: str,
        session: Optional[aiohttp.ClientSession] = None,
        timeout: int = 60,
    ) -> Path:
        """Download GTFS ZIP file.

        Args:
            url: URL to download from.
            feed_name: Name for caching.
            session: Optional aiohttp session.
            timeout: Download timeout in seconds.

        Returns:
            Path to downloaded/cached ZIP file.
        """
        # Get or create a lock for this feed to prevent concurrent downloads
        if feed_name not in self._download_locks:
            self._download_locks[feed_name] = asyncio.Lock()

        async with self._download_locks[feed_name]:
            cache_path = self._get_cache_path(feed_name)

            # Return cached file if valid (check inside lock in case another
            # coroutine just finished downloading)
            if self._is_cache_valid(feed_name):
                return cache_path

            # Download new file
            owned_session = False
            if session is None:
                session = aiohttp.ClientSession()
                owned_session = True

            try:
                async with session.get(url, timeout=aiohttp.ClientTimeout(total=timeout)) as response:
                    response.raise_for_status()
                    content = await response.read()

                # Write to temp file first, then atomically replace to avoid
                # partial/corrupted files if interrupted mid-write
                temp_path = cache_path.with_suffix('.zip.tmp')
                try:
                    async with aiofiles.open(temp_path, 'wb') as f:
                        await f.write(content)
                    # replace() is atomic and works when target exists (unlike rename on Windows)
                    temp_path.replace(cache_path)
                except BaseException:
                    # Clean up temp file on any failure
                    if temp_path.exists():
                        temp_path.unlink()
                    raise

                # Invalidate memory caches for this feed since we have new data
                self._invalidate_feed_caches(feed_name)

                return cache_path

            finally:
                if owned_session:
                    await session.close()

    def parse_stops_for_route(self, zip_path: Path, route_id: str) -> list[dict]:
        """Parse stops for a specific route from GTFS ZIP.

        Args:
            zip_path: Path to GTFS ZIP file.
            route_id: Route ID to get stops for.

        Returns:
            List of stop dictionaries with stop_id, stop_name, stop_sequence.
        """
        cache_key = f"{zip_path.stem}_{route_id}"

        # Check memory cache
        if cache_key in self._route_stops_cache:
            cache_time = self._cache_timestamps.get(cache_key)
            if cache_time and (datetime.now() - cache_time) < timedelta(hours=self.ttl_hours):
                return self._route_stops_cache[cache_key]

        with zipfile.ZipFile(zip_path, 'r') as zf:
            # Parse stops.txt to get stop names
            stops_dict = {}
            with zf.open('stops.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                for row in reader:
                    stops_dict[row['stop_id']] = row.get('stop_name', '')

            # Parse routes.txt to verify route exists
            route_found = False
            with zf.open('routes.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                for row in reader:
                    if row['route_id'] == route_id:
                        route_found = True
                        break

            if not route_found:
                return []

            # Parse trips.txt to get trip_ids for this route
            trip_ids = []
            with zf.open('trips.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                for row in reader:
                    if row['route_id'] == route_id:
                        trip_ids.append(row['trip_id'])

            if not trip_ids:
                return []

            # Parse stop_times.txt to get stops for these trips
            route_stops = {}
            with zf.open('stop_times.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                for row in reader:
                    if row['trip_id'] in trip_ids:
                        stop_id = row['stop_id']
                        stop_sequence = int(row.get('stop_sequence', 0))

                        # Keep track of unique stops and their typical sequence
                        if stop_id not in route_stops:
                            route_stops[stop_id] = {
                                'stop_id': stop_id,
                                'stop_name': stops_dict.get(stop_id, ''),
                                'stop_sequence': stop_sequence,
                            }

            # Convert to sorted list by sequence
            result = sorted(route_stops.values(), key=lambda x: x['stop_sequence'])

            # Cache result
            self._route_stops_cache[cache_key] = result
            self._cache_timestamps[cache_key] = datetime.now()

            return result

    def get_stop_names(self, zip_path: Path) -> dict[str, str]:
        """Get a mapping of stop_id to stop_name from GTFS ZIP.

        Args:
            zip_path: Path to GTFS ZIP file.

        Returns:
            Dictionary mapping stop_id to stop_name.
        """
        cache_key = f"{zip_path.stem}_stop_names"

        # Check memory cache
        if cache_key in self._stop_names_cache:
            cache_time = self._cache_timestamps.get(cache_key)
            if cache_time and (datetime.now() - cache_time) < timedelta(hours=self.ttl_hours):
                return self._stop_names_cache[cache_key]

        stops_dict = {}
        with zipfile.ZipFile(zip_path, 'r') as zf:
            with zf.open('stops.txt') as f:
                reader = csv.DictReader(io.TextIOWrapper(f, encoding='utf-8-sig'))
                for row in reader:
                    stop_name = (row.get('stop_name') or '').strip()
                    if stop_name:
                        # Only include stops with a usable name
                        stops_dict[row['stop_id']] = stop_name

        # Cache result
        self._stop_names_cache[cache_key] = stops_dict
        self._cache_timestamps[cache_key] = datetime.now()

        return stops_dict

    async def get_combined_stop_names(
        self,
        feeds: list[tuple[str, str]],
        session: aiohttp.ClientSession,
        timeout: int = 60,
    ) -> dict[str, str]:
        """Get a combined mapping of stop_id to stop_name from multiple GTFS feeds.

        This method caches the combined result to avoid rebuilding on every call.

        Args:
            feeds: List of (feed_name, url) tuples.
            session: aiohttp session for downloads.
            timeout: Download timeout in seconds.

        Returns:
            Dictionary mapping stop_id to stop_name from all feeds.
        """
        # Create a cache key from all feed names
        cache_key = "_".join(sorted(name for name, _ in feeds)) + "_combined"

        # Check memory cache
        if cache_key in self._stop_names_cache:
            cache_time = self._cache_timestamps.get(cache_key)
            if cache_time and (datetime.now() - cache_time) < timedelta(hours=self.ttl_hours):
                return self._stop_names_cache[cache_key]

        # Build combined dict from all feeds
        combined: dict[str, str] = {}
        for feed_name, url in feeds:
            try:
                zip_path = await self.download_gtfs(
                    url=url,
                    feed_name=feed_name,
                    session=session,
                    timeout=timeout,
                )
                combined.update(self.get_stop_names(zip_path))
            except (
                aiohttp.ClientError,
                zipfile.BadZipFile,
                KeyError,
                OSError,
                asyncio.TimeoutError,
                TimeoutError,
            ):
                # Continue if one feed fails (download error, timeout, corrupt zip, missing file, etc.)
                pass

        # Cache the combined result
        self._stop_names_cache[cache_key] = combined
        self._cache_timestamps[cache_key] = datetime.now()

        return combined
