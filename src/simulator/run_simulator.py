import argparse
import json
import logging
import time
from datetime import datetime, timezone

import paho.mqtt.client as mqtt
import pandas as pd

from simulator.faults import FaultConfig, FaultInjector
from simulator.messages import build_message

log = logging.getLogger("simulator")


def topic_for(device: str) -> str:
    return f"factory/{device}/telemetry"


def publish(client: mqtt.Client, msg: dict) -> mqtt.MQTTMessageInfo:
    msg = dict(msg)
    msg["sent_at"] = datetime.now(timezone.utc).isoformat()  # real send time
    return client.publish(topic_for(msg["device_id"]), json.dumps(msg), qos=1)


def main() -> None:
    p = argparse.ArgumentParser()
    p.add_argument("--csv", default="data/raw/PdM_telemetry.csv")
    p.add_argument("--start", default="2015-01-01")
    p.add_argument("--end", default="2015-01-15")
    p.add_argument(
        "--hour-seconds", type=float, default=0.2, help="real seconds per simulated hour"
    )
    p.add_argument("--firmware-v2-from", default="2015-01-08")
    p.add_argument("--broker", default="localhost")
    p.add_argument("--port", type=int, default=1883)
    p.add_argument("--seed", type=int, default=42)
    args = p.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(name)s %(message)s")

    df = pd.read_csv(args.csv, parse_dates=["datetime"])
    df = df[(df["datetime"] >= args.start) & (df["datetime"] < args.end)]
    df = df.sort_values(["datetime", "machineID"])
    v2_from = pd.Timestamp(args.firmware_v2_from)
    log.info("Replaying %d readings from %s to %s", len(df), args.start, args.end)

    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="device-simulator")
    client.connect(args.broker, args.port)
    client.loop_start()
    injector = FaultInjector(FaultConfig(), seed=args.seed)
    info = None
    try:
        for ts, hour_rows in df.groupby("datetime"):
            firmware = "v2" if ts >= v2_from else "v1"
            clean = [build_message(r, firmware) for r in hour_rows.to_dict("records")]
            for m in injector.process(clean):
                info = publish(client, m)
            if injector.step % 24 == 0:
                log.info("simulated %s | stats %s", ts, injector.stats)
            time.sleep(args.hour_seconds)
        for m in injector.flush():
            info = publish(client, m)
        if info is not None:
            info.wait_for_publish(timeout=10)
    finally:
        client.loop_stop()
        client.disconnect()
    log.info("Done. Final stats: %s", injector.stats)


if __name__ == "__main__":
    main()
