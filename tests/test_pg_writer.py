"""Integration tests against a real PostgreSQL (the docker-compose one, or CI's service).

Each test runs in its own temporary schema, so it never touches your real data.
Skipped automatically if PostgreSQL is not reachable.
"""
import uuid

import pytest

from sinks.postgres.kafka_to_postgres import conninfo
from sinks.postgres.records import parse_reading
from sinks.postgres.writer import apply_schema, write_batch

psycopg = pytest.importorskip("psycopg")


@pytest.fixture
def conn():
    try:
        c = psycopg.connect(conninfo(), autocommit=True, connect_timeout=3)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL not reachable")
    schema = f"test_{uuid.uuid4().hex[:8]}"
    c.execute(f"CREATE SCHEMA {schema}")
    c.execute(f"SET search_path TO {schema}")
    apply_schema(c)
    yield c
    c.execute(f"DROP SCHEMA {schema} CASCADE")
    c.close()


def reading(event_hour=6, pressure=100.0, vibration=40.0, device="machine-001", msg_id=None):
    return parse_reading(
        {
            "message_id": msg_id or str(uuid.uuid4()),
            "device_id": device,
            "machine_id": int(device[-3:]),
            "event_time": f"2015-01-01T{event_hour:02d}:00:00+00:00",
            "firmware": "v1",
            "volt": 170.0,
            "rotate": 450.0,
            "pressure": pressure,
            "vibration": vibration,
        },
        0,
        0,
    )


def scalar(conn, sql):
    return conn.execute(sql).fetchone()[0]


def test_duplicates_are_stored_once(conn):
    r = reading()
    stats = write_batch(conn, [r, r])
    assert stats["new_readings"] == 1 and stats["duplicates"] == 1
    assert scalar(conn, "SELECT count(*) FROM telemetry_reading") == 1


def test_replaying_a_whole_batch_changes_nothing(conn):
    batch = [reading(h) for h in range(6, 10)] + [reading(11, pressure=-1.0)]
    write_batch(conn, batch)
    again = write_batch(conn, batch)  # like Kafka re-delivering after a crash
    assert again["new_readings"] == 0 and again["alerts"] == 0
    assert scalar(conn, "SELECT count(*) FROM telemetry_reading") == 5
    assert scalar(conn, "SELECT count(*) FROM sensor_alert") == 1


def test_late_message_does_not_overwrite_newer_status(conn):
    write_batch(conn, [reading(event_hour=10, vibration=41.0)])
    write_batch(conn, [reading(event_hour=8, vibration=99.0)])  # arrives late
    row = conn.execute(
        "SELECT EXTRACT(HOUR FROM last_event_time AT TIME ZONE 'UTC'), vibration"
        " FROM device_status"
    ).fetchone()
    assert (int(row[0]), row[1]) == (10, 41.0)
    assert scalar(conn, "SELECT count(*) FROM telemetry_reading") == 2  # history keeps both


def test_one_status_row_per_device_with_health(conn):
    write_batch(
        conn,
        [
            reading(6, device="machine-001"),
            reading(7, device="machine-001"),
            reading(6, device="machine-002", pressure=-3.0),
        ],
    )
    rows = dict(conn.execute("SELECT device_id, health FROM device_status").fetchall())
    assert rows == {"machine-001": "OK", "machine-002": "FAULT"}


def test_schema_can_be_applied_twice(conn):
    apply_schema(conn)  # IF NOT EXISTS everywhere: no error


def test_empty_batch_writes_nothing(conn):
    stats = write_batch(conn, [])
    assert stats == {"received": 0, "new_readings": 0, "duplicates": 0, "alerts": 0}
    assert scalar(conn, "SELECT count(*) FROM telemetry_reading") == 0


def test_newer_message_updates_existing_device_status(conn):
    write_batch(conn, [reading(event_hour=6, pressure=-5.0)])  # initially FAULT
    write_batch(conn, [reading(event_hour=7, pressure=105.0)])  # next hour: recovered to OK
    row = conn.execute("SELECT health, pressure FROM device_status").fetchone()
    assert row == ("OK", 105.0)

