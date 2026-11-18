from datetime import datetime, timezone

import pytest

from sinks.postgres.records import (
    InvalidReading,
    alerts_for,
    health,
    latest_per_device,
    parse_reading,
)


def payload(**overrides):
    base = {
        "message_id": "6f1c0f0e-6a3e-4b8e-9a51-1f2d3c4b5a69",
        "device_id": "machine-007",
        "machine_id": 7,
        "event_time": "2015-01-01T06:00:00+00:00",
        "sent_at": "2026-11-14T10:00:00+00:00",
        "firmware": "v1",
        "volt": 170.0,
        "rotate": 450.0,
        "pressure": 100.0,
        "vibration": 40.0,
    }
    base.update(overrides)
    return base


def test_parse_reading_converts_types():
    r = parse_reading(payload(), partition=1, offset=99)
    assert r.machine_id == 7
    assert r.event_time == datetime(2015, 1, 1, 6, tzinfo=timezone.utc)
    assert r.temperature is None
    assert (r.kafka_partition, r.kafka_offset) == (1, 99)


def test_v2_temperature_is_kept():
    assert parse_reading(payload(firmware="v2", temperature=81.5), 0, 0).temperature == 81.5


def test_missing_field_is_invalid():
    bad = payload()
    del bad["pressure"]
    with pytest.raises(InvalidReading):
        parse_reading(bad, 0, 0)


def test_wrong_type_is_invalid():
    with pytest.raises(InvalidReading):
        parse_reading(payload(volt="not a number"), 0, 0)


def test_health_and_alerts():
    ok = parse_reading(payload(), 0, 0)
    fault = parse_reading(payload(pressure=-5.0), 0, 0)
    warn = parse_reading(payload(vibration=80.0), 0, 0)
    assert (health(ok), health(fault), health(warn)) == ("OK", "FAULT", "WARNING")
    assert alerts_for(ok) == []
    assert alerts_for(fault) == [("NEGATIVE_PRESSURE", -5.0)]
    assert alerts_for(warn) == [("HIGH_VIBRATION", 80.0)]


def test_latest_per_device_keeps_newest_event_time():
    old = parse_reading(payload(event_time="2015-01-01T05:00:00+00:00"), 0, 1)
    new = parse_reading(payload(event_time="2015-01-01T07:00:00+00:00"), 0, 2)
    other = parse_reading(payload(device_id="machine-008", machine_id=8), 0, 3)
    result = {r.device_id: r for r in latest_per_device([new, old, other])}
    assert result["machine-007"].kafka_offset == 2
    assert len(result) == 2


def test_both_fault_and_warning_in_same_reading():
    both = parse_reading(payload(pressure=-5.0, vibration=80.0), 0, 0)
    assert health(both) == "FAULT"
    assert alerts_for(both) == [
        ("NEGATIVE_PRESSURE", -5.0),
        ("HIGH_VIBRATION", 80.0),
    ]

