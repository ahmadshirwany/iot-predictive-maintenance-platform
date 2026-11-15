from dataclasses import asdict
from pathlib import Path

from sinks.postgres.records import Reading, alerts_for, health, latest_per_device

SCHEMA_FILE = Path(__file__).with_name("schema.sql")

INSERT_READING = """
INSERT INTO telemetry_reading (message_id, device_id, machine_id, event_time, sent_at,
    firmware, volt, rotate, pressure, vibration, temperature, kafka_partition, kafka_offset)
VALUES (%(message_id)s, %(device_id)s, %(machine_id)s, %(event_time)s, %(sent_at)s,
    %(firmware)s, %(volt)s, %(rotate)s, %(pressure)s, %(vibration)s, %(temperature)s,
    %(kafka_partition)s, %(kafka_offset)s)
ON CONFLICT (message_id) DO NOTHING
"""


# Upsert: insert a new device, or update it, but ONLY if this reading is newer.
# The WHERE clause stops a late (older) message from overwriting newer status.
UPSERT_STATUS = """
INSERT INTO device_status (device_id, machine_id, last_event_time, last_seen_at, firmware,
    volt, rotate, pressure, vibration, temperature, health)
VALUES (%(device_id)s, %(machine_id)s, %(event_time)s, now(), %(firmware)s,
    %(volt)s, %(rotate)s, %(pressure)s, %(vibration)s, %(temperature)s, %(health)s)
ON CONFLICT (device_id) DO UPDATE SET
    machine_id = EXCLUDED.machine_id,
    last_event_time = EXCLUDED.last_event_time,
    last_seen_at = EXCLUDED.last_seen_at,
    firmware = EXCLUDED.firmware,
    volt = EXCLUDED.volt,
    rotate = EXCLUDED.rotate,
    pressure = EXCLUDED.pressure,
    vibration = EXCLUDED.vibration,
    temperature = EXCLUDED.temperature,
    health = EXCLUDED.health,
    updated_at = now()
WHERE device_status.last_event_time <= EXCLUDED.last_event_time
"""

INSERT_ALERT = """
INSERT INTO sensor_alert (message_id, device_id, event_time, alert_type, value)
VALUES (%(message_id)s, %(device_id)s, %(event_time)s, %(alert_type)s, %(value)s)
ON CONFLICT (message_id, alert_type) DO NOTHING
"""


def apply_schema(conn) -> None:
    conn.execute(SCHEMA_FILE.read_text())

def write_batch(conn, readings: list[Reading]) -> dict:
    """Readings + status + alerts all succeed together, or nothing is written."""
    if not readings:
        return {"received": 0, "new_readings": 0, "duplicates": 0, "alerts": 0}
    rows = [asdict(r) for r in readings]
    statuses = [{**asdict(r), "health": health(r)} for r in latest_per_device(readings)]
    alerts = [
        {"message_id": r.message_id, "device_id": r.device_id, "event_time": r.event_time,
         "alert_type": kind, "value": value}
        for r in readings for kind, value in alerts_for(r)
    ]
    with conn.transaction():
        with conn.cursor() as cur:
            cur.executemany(INSERT_READING, rows)
            new_readings = cur.rowcount
            cur.executemany(UPSERT_STATUS, statuses)
            new_alerts = 0
            if alerts:
                cur.executemany(INSERT_ALERT, alerts)
                new_alerts = cur.rowcount
    return {"received": len(readings), "new_readings": new_readings,
            "duplicates": len(readings) - new_readings, "alerts": new_alerts}

