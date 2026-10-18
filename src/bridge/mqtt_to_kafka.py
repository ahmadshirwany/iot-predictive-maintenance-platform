import json
import logging

import paho.mqtt.client as mqtt
from confluent_kafka import Producer

MQTT_TOPIC = "factory/+/telemetry"
KAFKA_TOPIC = "machine-telemetry"
DLQ_TOPIC = "machine-telemetry-dlq"
log = logging.getLogger("bridge")


def device_id_from_topic(topic: str) -> str:
    # topic format: "factory/{device_id}/telemetry"
    parts = topic.split("/")
    if len(parts) != 3 or parts[0] != "factory" or parts[2] != "telemetry":
        raise ValueError(f"Invalid topic: {topic}")
    return parts[1]


def is_valid_json(payload):
    try:
        json.loads(payload)
        return True
    except (ValueError, UnicodeDecodeError):
        return False


def main():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s")
    producer = Producer(
        {
            "bootstrap.servers": "localhost:19092",
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": 100,
        }
    )
    counter = {"forward": 0, "dlq": 0, "errors": 0}

    def on_delivery(err, msg):
        if err is not None:
            log.error(f"Failed to deliver message: {err}")

    def on_connect(client, userdata, flags, reason_code, properties):
        log.info("Connected to MQTT (%s), subscribing to %s", reason_code, MQTT_TOPIC)
        client.subscribe(MQTT_TOPIC, qos=1)

    def on_message(client, userdata, msg):
        device = device_id_from_topic(msg.topic)
        log.info("received on %s: %s", msg.topic, msg.payload.decode())
        topic = KAFKA_TOPIC if is_valid_json(msg.payload) else DLQ_TOPIC
        producer.produce(topic, key=device, value=msg.payload, on_delivery=on_delivery)
        producer.poll(0)
        counter["forward" if topic == KAFKA_TOPIC else "dlq"] += 1
        if counter["forward"] % 1000 == 0:
            log.info("forward: %d, dlq: %d", counter["forward"], counter["dlq"])

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="mqtt-kafka-bridge")
    client.on_connect = on_connect
    client.on_message = on_message
    client.connect("localhost", 1883)
    try:
        client.loop_forever()
    except KeyboardInterrupt:
        log.info("Exiting...")
    finally:
        producer.flush()
        log.info("Produced %d messages", counter["forward"])


if __name__ == "__main__":
    main()
