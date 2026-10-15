"""Add real-world problems to a clean stream of messages."""

import random
from dataclasses import dataclass


@dataclass
class FaultConfig:
    dublicate_rate: float = 0.01
    delay_rate: float = 0.02
    max_delay_steps: int = 3
    bad_values_rate: float = 0.05
    outage_rate: float = 0.001
    outage_steps: int = 12


class FaultInjector:
    """Call process() once per simulated hour with that hour's messages."""

    def __init__(self, config: FaultConfig, seed: int | None = None):
        self.config = config
        self.ring = random.Random(seed)
        self.step = 0
        self._delayed: list[tuple[int, dict]] = []
        self._outage_unit: dict[str, int] = {}
        self.stats = {"out": 0, "duplicated": 0, "delayed": 0, "bad_values": 0, "dropped_outage": 0}

    def _in_outage(self, device: str) -> bool:
        until = self._outage_unit.get(device)
        if until is not None and self.step < until:
            return True
        self._outage_unit.pop(device, None)
        if self.ring.random() < self.config.outage_rate:
            self._outage_unit[device] = self.step + self.config.outage_steps
            return True
        return False

    def process(self, messages: list[dict]) -> list[dict]:
        out: list[dict] = []
        for msg in messages:
            msg = dict(msg)
            if self._in_outage(msg["device_id"]):
                self.stats["dropped_outage"] += 1
                continue
            if self.ring.random() < self.config.bad_values_rate:
                msg["pressure"] = -abs(msg["pressure"])
                self.stats["bad_values"] += 1
            if self.ring.random() < self.config.delay_rate:
                release = self.step + self.ring.randint(1, self.config.max_delay_steps)
                self._delayed.append((release, msg))
                self.stats["delayed"] += 1
                continue
            out.append(msg)
            if self.ring.random() < self.config.dublicate_rate:
                out.append(dict(msg))
                self.stats["duplicated"] += 1

        # release delayed messages
        still_waiting = []
        for release, msg in self._delayed:
            if release <= self.step:
                out.append(msg)
            else:
                still_waiting.append((release, msg))
        self._delayed = still_waiting
        self.step += 1
        self.stats["out"] += len(out)
        return out

    def flush(self) -> list[dict]:
        """Release all delayed messages, even if they are late."""
        out = [msg for _, msg in self._delayed]
        self.stats["out"] += len(out)
        self._delayed.clear()
        self.stats["out"] += len(out)
        return out
