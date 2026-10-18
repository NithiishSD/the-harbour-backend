"""HBase table layout and row-key builders.

Row keys start with the MMSI (or port id) so writes spread across regions instead of piling onto the
newest one (hotspotting). Timestamps are stored reversed so the newest row sorts first in a scan.
"""

from storage.hbase.client import HBaseClient

LONG_MAX = 2**63 - 1

VESSEL_TRACK = "vessel_track"
VESSEL_STATE = "vessel_state"
PORT_CALLS = "port_calls"
PORT_QUEUE = "port_queue"
AREA_ACTIVITY = "area_activity"

# table -> {column family: versions kept}
TABLES: dict[str, dict[str, int]] = {
    VESSEL_TRACK: {"pos": 1},
    # cur: state changes only (10 kept = state history); last: latest position; info: ship details
    VESSEL_STATE: {"cur": 10, "last": 1, "info": 1, "src": 1},
    PORT_CALLS: {"call": 1},
    PORT_QUEUE: {"q": 1},
    AREA_ACTIVITY: {"act": 1},
}


def rev(ts_ms: int) -> str:
    """Reversed, fixed-width timestamp: newer times give smaller strings."""
    if ts_ms < 0:
        raise ValueError("timestamp must be non-negative")
    return f"{LONG_MAX - ts_ms:019d}"


def unrev(text: str) -> int:
    return LONG_MAX - int(text)


def _check_mmsi(mmsi: str) -> str:
    if len(mmsi) != 9 or not mmsi.isdigit():
        raise ValueError(f"MMSI must be 9 digits: {mmsi!r}")
    return mmsi


def _check_port(port_id: str) -> str:
    if len(port_id) != 5 or not port_id.isalnum():
        raise ValueError(f"port id must be a 5-character UN/LOCODE: {port_id!r}")
    return port_id.upper()


def track_key(mmsi: str, ts_ms: int) -> str:
    return _check_mmsi(mmsi) + rev(ts_ms)


def port_call_key(port_id: str, arrival_ms: int, mmsi: str) -> str:
    return _check_port(port_id) + rev(arrival_ms) + _check_mmsi(mmsi)


def port_queue_key(port_id: str, window_end_ms: int) -> str:
    return _check_port(port_id) + rev(window_end_ms)


def area_activity_key(port_id: str, ts_ms: int, mmsi: str) -> str:
    """Secondary index key: port area + reversed timestamp + MMSI."""
    return _check_port(port_id) + rev(ts_ms) + _check_mmsi(mmsi)


def create_tables(client: HBaseClient | None = None) -> list[str]:
    client = client or HBaseClient()
    return [name for name, families in TABLES.items() if client.create_table(name, families)]


if __name__ == "__main__":
    print(create_tables())
