import json
import logging
import os

import psycopg
from confluent_kafka import Consumer

from sinks.postgres.records import InvalidReading, parse_reading
from sinks.postgres.writer import apply_schema, write_batch

KAFKA_TOPIC = "machine-telemetry"
log = logging.getLogger("postgres-sink")


def conninfo() -> str:
    return (
        f"host={os.environ.get('POSTGRES_HOST', 'localhost')} "
        f"port={os.environ.get('POSTGRES_PORT', '5434')} "
        f"dbname={os.environ.get('POSTGRES_DB', 'iot')} "
        f"user={os.environ.get('POSTGRES_USER', 'iot')} "
        f"password={os.environ.get('POSTGRES_PASSWORD', 'iot')}"
    )


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")
    # autocommit=True: each "with conn.transaction()" block is one real transaction
    conn = psycopg.connect(conninfo(), autocommit=True)
    apply_schema(conn)

    consumer = Consumer({
        "bootstrap.servers": "localhost:19092",
        "group.id": "postgres-sink",       # its own group, independent of other sinks
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,       # commit only after the database commit
    })
    consumer.subscribe([KAFKA_TOPIC])
    totals = {"received": 0, "new_readings": 0, "duplicates": 0, "alerts": 0, "invalid": 0}

    try:
        while True:
            messages = consumer.consume(num_messages=500, timeout=1.0)
            if not messages:
                continue
            readings = []
            has_offset = False
            for msg in messages:
                if msg.error():
                    log.error("kafka error: %s", msg.error())
                    continue
                has_offset = True
                try:
                    readings.append(parse_reading(json.loads(msg.value()),
                                                  msg.partition(), msg.offset()))
                except (InvalidReading, ValueError) as exc:
                    totals["invalid"] += 1
                    log.warning("skipping invalid message at offset %s: %s", msg.offset(), exc)
            if not has_offset:
                continue
            stats = write_batch(conn, readings)    # 1) database commit
            consumer.commit(asynchronous=False)    # 2) then tell Kafka "done"
            for k, v in stats.items():
                totals[k] += v
            log.info("batch %s | totals %s", stats, totals)
    except KeyboardInterrupt:
        log.info("stopping")
    finally:
        consumer.close()
        conn.close()


if __name__ == "__main__":
    main()

