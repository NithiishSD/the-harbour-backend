"""Message builders in the shapes aisstream and NOAA deliver (field names from their documentation)."""

from datetime import UTC, datetime
from typing import Any


def ais_time(ts_ms: int) -> str:
    dt = datetime.fromtimestamp(ts_ms / 1000, UTC)
    return dt.strftime("%Y-%m-%d %H:%M:%S.%f") + "000 +0000 UTC"


def aisstream_position(
    mmsi: int = 419000123,
    ts_ms: int = 1_672_563_600_000,
    lat: float = 1.25,
    lon: float = 103.9,
    sog: float = 0.1,
    nav: int = 1,
    kind: str = "PositionReport",
) -> dict[str, Any]:
    body: dict[str, Any] = {
        "Cog": 308,
        "Latitude": lat,
        "Longitude": lon,
        "MessageID": 1,
        "Sog": sog,
        "Timestamp": 31,
        "TrueHeading": 235,
        "UserID": mmsi,
        "Valid": True,
    }
    if kind == "PositionReport":
        body["NavigationalStatus"] = nav
    return {
        "Message": {kind: body},
        "MessageType": kind,
        "MetaData": {
            "MMSI": mmsi,
            "MMSI_String": mmsi,
            "ShipName": "TEST TANKER        ",
            "latitude": lat,
            "longitude": lon,
            "time_utc": ais_time(ts_ms),
        },
    }


def aisstream_static(mmsi: int = 419000123, ts_ms: int = 1_672_563_600_000) -> dict[str, Any]:
    return {
        "Message": {
            "ShipStaticData": {
                "CallSign": "VWXY",
                "Destination": "SINGAPORE",
                "Dimension": {"A": 150, "B": 33, "C": 16, "D": 16},
                "ImoNumber": 9123456,
                "MaximumStaticDraught": 11.2,
                "MessageID": 5,
                "Name": "TEST TANKER@@@@",
                "Type": 80,
                "UserID": mmsi,
                "Valid": True,
            }
        },
        "MessageType": "ShipStaticData",
        "MetaData": {"MMSI": mmsi, "ShipName": "TEST TANKER", "time_utc": ais_time(ts_ms)},
    }


def envelope(payload: dict[str, Any], source: str = "aisstream") -> dict[str, Any]:
    return {"source": source, "received_at": "2023-01-01T09:00:01Z", "payload": payload}


def noaa_row(
    mmsi: str = "367000001",
    when: str = "2023-01-01T09:00:00",
    lat: float = 33.70,
    lon: float = -118.20,
    sog: float = 0.0,
    status: str = "1",
) -> dict[str, str]:
    return {
        "MMSI": mmsi,
        "BaseDateTime": when,
        "LAT": str(lat),
        "LON": str(lon),
        "SOG": str(sog),
        "COG": "211.0",
        "Heading": "511",
        "VesselName": "PACIFIC TEST",
        "IMO": "IMO9000001",
        "CallSign": "WDA1234",
        "VesselType": "70",
        "Status": status,
        "Length": "229",
        "Width": "32",
        "Draft": "12.5",
        "Cargo": "70",
        "TransceiverClass": "A",
    }
