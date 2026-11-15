from dataclasses import dataclass
from datetime import datetime

VIBRATION_WARNING = 55.0

REQUIRED_FIELDS = (
    "message_id",
    "device_id",
    "machine_id",
    "event_time",
    "volt",
    "rotate",
    "pressure",
    "vibration",
)


class InvalidReading(ValueError):
    """The message is valid JSON, but not a usable reading."""


@dataclass
class Reading:
    message_id: str
    device_id: str
    machine_id: int
    event_time: datetime
    sent_at: datetime | None
    firmware: str | None
    volt: float
    rotate: float
    pressure: float
    vibration: float
    temperature: float | None
    kafka_partition: int
    kafka_offset: int


def parse_reading(payload: dict, partition: int = 0, offset: int = 0) -> Reading:
    missing = [f for f in REQUIRED_FIELDS if payload.get(f) is None]
    if missing:
        raise InvalidReading(f"missing required fields: {missing}")
    try:
        sent_raw = payload.get("sent_at")
        temp_raw = payload.get("temperature")
        return Reading(
            message_id=str(payload["message_id"]),
            device_id=str(payload["device_id"]),
            machine_id=int(payload["machine_id"]),
            event_time=datetime.fromisoformat(payload["event_time"]),
            sent_at=datetime.fromisoformat(sent_raw) if sent_raw else None,
            firmware=payload.get("firmware"),
            volt=float(payload["volt"]),
            rotate=float(payload["rotate"]),
            pressure=float(payload["pressure"]),
            vibration=float(payload["vibration"]),
            temperature=float(temp_raw) if temp_raw is not None else None,
            kafka_partition=partition,
            kafka_offset=offset,
        )
    except (KeyError, TypeError, ValueError) as e:
        raise InvalidReading(f"invalid reading: {e}") from e


# Keep alias for backwards compatibility
prase_reading = parse_reading


def health(r: Reading) -> str:
    """FAULT = impossible value (broken sensor), WARNING = unusual but possible, OK = normal."""
    if r.pressure < 0:
        return "FAULT"
    if r.vibration > VIBRATION_WARNING:
        return "WARNING"
    return "OK"


def alerts_for(r: Reading) -> list[tuple[str, float]]:
    found = []
    if r.pressure < 0:
        found.append(("NEGATIVE_PRESSURE", r.pressure))
    if r.vibration > VIBRATION_WARNING:
        found.append(("HIGH_VIBRATION", r.vibration))
    return found


def latest_per_device(readings: list[Reading]) -> list[Reading]:
    """Keep only the newest reading (by event time) for each device in a batch."""
    newest: dict[str, Reading] = {}
    for r in readings:
        current = newest.get(r.device_id)
        if current is None or r.event_time >= current.event_time:
            newest[r.device_id] = r
    return list(newest.values())
