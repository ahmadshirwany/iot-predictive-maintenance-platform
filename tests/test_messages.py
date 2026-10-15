from datetime import datetime

from simulator.messages import build_message, device_id

ROW = {
    "datetime": datetime(2015, 1, 1, 6),
    "machineID": 7,
    "volt": 176.21789,
    "rotate": 418.5,
    "pressure": 113.07,
    "vibration": 45.08,
}


def test_device_id_is_zero_padded():
    assert device_id(7) == "machine-007"
    assert device_id(123) == "machine-123"


def test_v1_message_has_expected_fields():
    msg = build_message(ROW, "v1")
    assert msg["device_id"] == "machine-007"
    assert msg["event_time"] == "2015-01-01T06:00:00+00:00"
    assert msg["volt"] == 176.2179
    assert "temperature" not in msg
    assert len(msg["message_id"]) == 36


def test_v2_message_has_expected_fields():
    msg = build_message(ROW, "v2")
    assert msg["firmware"] == "v2"
    assert "temperature" in msg


def test_every_message_adds_temprature():
    msg = build_message(ROW, "v2")
    assert msg["firmware"] == "v2"
    assert "temperature" in msg


def test_every_message_gets_unique_id():
    msg1 = build_message(ROW, "v1")
    msg2 = build_message(ROW, "v1")
    assert msg1["message_id"] != msg2["message_id"]
