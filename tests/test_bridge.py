import pytest

from bridge.mqtt_to_kafka import device_id_from_topic, is_valid_json


def test_valid_topic_extracts_id():
    assert device_id_from_topic("factory/machine-001/telemetry") == "machine-001"


def test_invalid_topic_raises_value_error():
    with pytest.raises(ValueError):
        device_id_from_topic("factory/machine-001/data")


def test_unexpected_topic_is_rejected():
    with pytest.raises(ValueError):
        device_id_from_topic("factory/machine-001/data")


def test_json_check():
    assert is_valid_json(b'{"a":1}')
    assert not is_valid_json(b"not json")
