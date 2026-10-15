import uuid
from datetime import datetime, timezone

SENSOR_FIELDS = ("volt", "rotate", "pressure", "vibration")


def device_id(machine_id: int) -> str:
    """7 -> 'machine-007'"""
    return f"machine-{int(machine_id):03d}"


def to_iso_utc(value: datetime) -> str:
    """ISO-8601 string in UTC. The dataset has no timezone, so we treat it as UTC."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.isoformat()


def build_message(row: dict, firmware: str = "v1") -> dict:
    """Build the JSON message a machine would send for one hourly reading."""
    msg = {
        "message_id": str(uuid.uuid4()),  # unique id, so duplicates can be found later
        "device_id": device_id(row["machineID"]),
        "machine_id": int(row["machineID"]),
        "event_time": to_iso_utc(row["datetime"]),
        "firmware": firmware,
    }
    for field in SENSOR_FIELDS:
        msg[field] = round(float(row[field]), 4)
    if firmware == "v2":
        # Firmware v2 adds a new sensor. Simulated: derived from vibration.
        msg["temperature"] = round(60 + float(row["vibration"]) * 0.5, 2)
    return msg
