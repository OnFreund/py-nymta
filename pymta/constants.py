"""Constants for MTA GTFS-RT library."""

# MTA GTFS-RT feed URLs for subway
FEED_URLS = {
    "1": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs",
    "A": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-ace",
    "N": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-nqrw",
    "B": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-bdfm",
    "L": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-l",
    "SI": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-si",
    "G": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-g",
    "J": "https://api-endpoint.mta.info/Dataservice/mtagtfsfeeds/nyct%2Fgtfs-jz",
}

# MTA Static GTFS feed URLs
STATIC_GTFS_URLS = {
    "subway": "http://web.mta.info/developers/data/nyct/subway/google_transit.zip",
    "bus_bronx": "http://web.mta.info/developers/data/nyct/bus/google_transit_bronx.zip",
    "bus_brooklyn": "http://web.mta.info/developers/data/nyct/bus/google_transit_brooklyn.zip",
    "bus_manhattan": "http://web.mta.info/developers/data/nyct/bus/google_transit_manhattan.zip",
    "bus_queens": "http://web.mta.info/developers/data/nyct/bus/google_transit_queens.zip",
    "bus_staten_island": "http://web.mta.info/developers/data/nyct/bus/google_transit_staten_island.zip",
}

# MTA GTFS-RT feed URLs for buses
# Note: These require an API key as a query parameter
BUS_FEED_URLS = {
    "trip_updates": "https://gtfsrt.prod.obanyc.com/tripUpdates",
    "vehicle_positions": "https://gtfsrt.prod.obanyc.com/vehiclePositions",
    "alerts": "https://gtfsrt.prod.obanyc.com/alerts",
}

# Mapping of subway lines to feed IDs
LINE_TO_FEED = {
    # Feed 1: 1, 2, 3, 4, 5, 6, 7, GS (Grand Central Shuttle)
    "1": "1",
    "2": "1",
    "3": "1",
    "4": "1",
    "5": "1",
    "6": "1",
    "7": "1",
    "GS": "1",
    # Feed A: A, C, E, H (Rockaway Shuttle), FS (Franklin Av Shuttle)
    "A": "A",
    "C": "A",
    "E": "A",
    "H": "A",
    "FS": "A",
    # Feed N: N, Q, R, W
    "N": "N",
    "Q": "N",
    "R": "N",
    "W": "N",
    # Feed B: B, D, F, M
    "B": "B",
    "D": "B",
    "F": "B",
    "M": "B",
    # Feed L: L
    "L": "L",
    # Feed SI: SIR (Staten Island Railway)
    "SI": "SI",
    # Feed G: G
    "G": "G",
    # Feed J: J, Z
    "J": "J",
    "Z": "J",
}
