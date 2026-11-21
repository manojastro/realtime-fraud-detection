# File: producer/transaction_producer.py
"""High-volume synthetic transaction producer for the fraud-detection pipeline.

Publishes JSON transaction events to Kafka keyed by ``user_id`` so every event
for a user lands on the same partition — preserving per-user ordering that the
downstream velocity features depend on.

Approximately 10% of emitted traffic carries injected fraud-pattern signals:

* ``VELOCITY_BURST``   – several transactions for one user within seconds.
* ``GEO_IMPOSSIBLE``   – IP country contradicts the cardholder home country.
* ``ROUND_AMOUNT``     – suspiciously exact amounts (e.g. 5000.00 USD).

Usage::

    python producer/transaction_producer.py \\
        --bootstrap-servers localhost:29092 --eps 50 --users 200

Stop with Ctrl+C; in-flight messages are flushed before exit.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import random
import signal
import sys
import threading
import time
import uuid
from collections import deque
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Any

try:
    from kafka import KafkaProducer
    from kafka.errors import KafkaError
except ImportError as exc:  # pragma: no cover
    sys.stderr.write("missing dependency: pip install kafka-python-ng\n")
    raise SystemExit(1) from exc

LOGGER = logging.getLogger("producer")

DEFAULT_BOOTSTRAP = "localhost:29092"
DEFAULT_TOPIC = "transactions.raw"

COUNTRIES: tuple[str, ...] = ("US", "GB", "DE", "FR", "IN", "SG", "BR")

# Coarse IP prefixes per country — good enough to make geo signals plausible.
_COUNTRY_IP_PREFIXES: dict[str, tuple[str, ...]] = {
    "US": ("23.", "104.", "52."),
    "GB": ("51.", "86.", "25."),
    "DE": ("91.", "84.", "62."),
    "FR": ("92.", "78."),
    "IN": ("49.", "103.", "117."),
    "SG": ("128.", "116.", "111."),
    "BR": ("177.", "191.", "168."),
}

_CURRENCIES_BY_WEIGHT: Sequence[tuple[str, float]] = (
    ("USD", 0.70),
    ("EUR", 0.12),
    ("GBP", 0.08),
    ("INR", 0.06),
    ("SGD", 0.04),
)

_ROUND_AMOUNTS: tuple[float, ...] = (100.0, 250.0, 500.0, 750.0, 1000.0, 1500.0, 2000.0, 5000.0)

MAX_SEND_ATTEMPTS = 5


class AnomalySignal(str, Enum):
    """Injected fraud-pattern labels carried on anomalous events."""

    VELOCITY_BURST = "velocity_burst"
    GEO_IMPOSSIBLE = "geo_impossible"
    ROUND_AMOUNT = "round_amount"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class JsonFormatter(logging.Formatter):
    """Minimal structured (JSON lines) log formatter."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        }
        extra = getattr(record, "extra_fields", None)
        if isinstance(extra, dict):
            payload.update(extra)
        if record.exc_info:
            payload["exc"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"))


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO), handlers=[handler])


@dataclass(frozen=True)
class SyntheticUser:
    """Stable synthetic cardholder used to keep device fingerprints realistic."""

    user_id: str
    home_country: str
    device_fingerprint: str


@dataclass(frozen=True)
class TransactionEvent:
    """One transaction event; field names mirror streaming/schemas.py."""

    event_id: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    timestamp: str
    ts_ms: int
    ip: str
    ip_country: str
    home_country: str
    device_fingerprint: str
    card_present: bool
    anomaly_signal: str

    def to_kafka_tuple(self) -> tuple[bytes, bytes]:
        key = self.user_id.encode("utf-8")
        value = json.dumps(
            {k: v for k, v in self.__dict__.items()},
            separators=(",", ":"),
        ).encode("utf-8")
        return key, value


class TransactionFactory:
    """Generates normal traffic plus ~10% injected anomaly patterns."""

    def __init__(self, n_users: int, n_merchants: int, rng: random.Random) -> None:
        self._rng = rng
        self.users: list[SyntheticUser] = [
            SyntheticUser(
                user_id=f"u_{i:04d}",
                home_country=rng.choice(COUNTRIES),
                device_fingerprint=self._fingerprint(f"u_{i:04d}"),
            )
            for i in range(1, n_users + 1)
        ]
        self.merchants: list[str] = [f"m_{i:04d}" for i in range(1, n_merchants + 1)]

    @staticmethod
    def _fingerprint(user_id: str) -> str:
        digest = hashlib.sha256(user_id.encode()).hexdigest()
        return f"fp_{digest[:16]}"

    def _random_ip(self, country: str) -> str:
        prefix = self._rng.choice(_COUNTRY_IP_PREFIXES.get(country, ("203.",)))
        return f"{prefix}{self._rng.randint(1, 254)}.{self._rng.randint(1, 254)}.{self._rng.randint(1, 254)}"

    def _weighted_currency(self) -> str:
        r = self._rng.random()
        cumulative = 0.0
        for code, weight in _CURRENCIES_BY_WEIGHT:
            cumulative += weight
            if r <= cumulative:
                return code
        return "USD"

    def _base_amount(self) -> float:
        # Lognormal: median ≈ 24, heavy right tail capped for sanity.
        amount = min(self._rng.lognormvariate(mu=3.2, sigma=0.9), 25_000.0)
        return round(amount, 2)

    def _make_event(
        self,
        now: datetime,
        user: SyntheticUser,
        *,
        amount: float | None = None,
        ip_country: str | None = None,
        card_present: bool | None = None,
        anomaly_signal: str = "",
    ) -> TransactionEvent:
        ts_ms = int(now.timestamp() * 1000)
        effective_ip_country = ip_country or user.home_country
        currency = self._weighted_currency() if anomaly_signal != AnomalySignal.GEO_IMPOSSIBLE.value else "USD"
        return TransactionEvent(
            event_id=str(uuid.uuid4()),
            user_id=user.user_id,
            merchant_id=self._rng.choice(self.merchants),
            amount=round(amount if amount is not None else self._base_amount(), 2),
            currency=currency,
            timestamp=now.isoformat(timespec="milliseconds").replace("+00:00", "Z"),
            ts_ms=ts_ms,
            ip=self._random_ip(effective_ip_country),
            ip_country=effective_ip_country,
            home_country=user.home_country,
            device_fingerprint=user.device_fingerprint,
            card_present=self._rng.random() < 0.65 if card_present is None else card_present,
            anomaly_signal=anomaly_signal,
        )

    def next_events(self) -> list[TransactionEvent]:
        """Return the events for this tick: either normal traffic or an anomaly burst."""
        now = datetime.now(timezone.utc)
        roll = self._rng.random()

        if roll < 0.033:
            # Velocity burst: same user, several rapid-fire transactions.
            user = self._rng.choice(self.users)
            burst_size = self._rng.randint(4, 8)
            events: list[TransactionEvent] = []
            for i in range(burst_size):
                offset_ms = self._rng.randint(50, 900) * i
                events.append(
                    self._make_event(
                        datetime.fromtimestamp(now.timestamp() + offset_ms / 1000, tz=timezone.utc),
                        user,
                        amount=round(self._rng.uniform(2.0, 120.0), 2),  # small card-testing amounts
                        card_present=False,
                        anomaly_signal=AnomalySignal.VELOCITY_BURST.value,
                    )
                )
            return events

        if roll < 0.066:
            # Geo impossibility: purchase IP country far from the home country.
            user = self._rng.choice(self.users)
            far_countries = [c for c in COUNTRIES if c != user.home_country]
            return [
                self._make_event(
                    now,
                    user,
                    amount=round(self._rng.uniform(80.0, 2400.0), 2),
                    ip_country=self._rng.choice(far_countries),
                    card_present=False,
                    anomaly_signal=AnomalySignal.GEO_IMPOSSIBLE.value,
                )
            ]

        if roll < 0.10:
            # Round high-value amounts.
            user = self._rng.choice(self.users)
            return [
                self._make_event(
                    now,
                    user,
                    amount=self._rng.choice(_ROUND_AMOUNTS),
                    anomaly_signal=AnomalySignal.ROUND_AMOUNT.value,
                )
            ]

        return [self._make_event(now, self._rng.choice(self.users))]


class TransactionProducer:
    """EPS-throttled Kafka publisher with retries and graceful shutdown."""

    def __init__(
        self,
        bootstrap_servers: str,
        topic: str,
        eps: float,
        factory: TransactionFactory,
        stats_interval_s: int = 30,
    ) -> None:
        self._bootstrap_servers = bootstrap_servers
        self._topic = topic
        self._eps = max(eps, 0.01)
        self._factory = factory
        self._stats_interval_s = stats_interval_s
        self._stop = threading.Event()
        self._producer: KafkaProducer | None = None
        self._sent = 0
        self._failed = 0
        self._dropped = 0
        self._retried = 0
        self._anomalies = 0

    # -- lifecycle -------------------------------------------------------------

    def start(self) -> None:
        self._producer = KafkaProducer(
            bootstrap_servers=self._bootstrap_servers.split(","),
            acks="all",
            retries=5,
            retry_backoff_ms=500,
            request_timeout_ms=30_000,
            max_block_ms=10_000,
            linger_ms=20,
            batch_size=32_768,
            compression_type="gzip",
            client_id="txn-producer",
        )
        LOGGER.info(
            "producer connected",
            extra={"extra_fields": {"bootstrap": self._bootstrap_servers, "topic": self._topic}},
        )

    def close(self) -> None:
        if self._producer is not None:
            try:
                self._producer.flush(timeout=10)
                self._producer.close(timeout=10)
            except KafkaError:  # pragma: no cover
                LOGGER.exception("error while closing producer")
        LOGGER.info(
            "producer closed",
            extra={"extra_fields": {"sent": self._sent, "failed": self._failed, "dropped": self._dropped}},
        )

    def request_stop(self, *_: Any) -> None:
        LOGGER.warning("shutdown signal received — draining ...")
        self._stop.set()

    # -- sending ---------------------------------------------------------------

    def _send_with_retry(self, event: TransactionEvent) -> bool:
        assert self._producer is not None, "start() must be called first"
        key, value = event.to_kafka_tuple()
        delay = 0.05
        for attempt in range(1, MAX_SEND_ATTEMPTS + 1):
            if self._stop.is_set():
                return False
            try:
                future = self._producer.send(self._topic, key=key, value=value)
                future.add_errback(lambda exc: self._on_delivery_error(exc))
                self._sent += 1
                return True
            except KafkaError as exc:
                self._retried += 1
                LOGGER.warning(
                    "send attempt failed (%d/%d): %s",
                    attempt,
                    MAX_SEND_ATTEMPTS,
                    exc,
                    extra={"extra_fields": {"event_id": event.event_id}},
                )
                if self._stop.wait(delay):
                    return False
                delay = min(delay * 2, 2.0)
        self._dropped += 1
        LOGGER.error(
            "event dropped after %d attempts",
            MAX_SEND_ATTEMPTS,
            extra={"extra_fields": {"event_id": event.event_id}},
        )
        return False

    def _on_delivery_error(self, exc: Exception) -> None:
        self._failed += 1
        LOGGER.error("async delivery failure: %s", exc)

    # -- main loop -------------------------------------------------------------

    def run(self, duration_s: float | None = None, dry_run: bool = False) -> None:
        interval = 1.0 / self._eps
        deadline = time.monotonic()
        started = time.monotonic()
        last_stats = time.monotonic()

        while not self._stop.is_set():
            if duration_s is not None and time.monotonic() - started >= duration_s:
                break

            for event in self._factory.next_events():
                if event.anomaly_signal:
                    self._anomalies += 1
                if dry_run:
                    key, value = event.to_kafka_tuple()
                    print(value.decode("utf-8"))
                    self._sent += 1
                else:
                    self._send_with_retry(event)

            # Deadline-based pacing; resync if we fell behind (backpressure).
            deadline += interval
            lag = time.monotonic() - deadline
            if lag > 1.0:
                LOGGER.warning(
                    "cannot sustain target eps — resyncing pace",
                    extra={"extra_fields": {"lag_s": round(lag, 2)}},
                )
                deadline = time.monotonic()
            sleep_for = max(0.0, deadline - time.monotonic())

            if time.monotonic() - last_stats >= self._stats_interval_s:
                elapsed = time.monotonic() - started
                LOGGER.info(
                    "throughput stats",
                    extra={
                        "extra_fields": {
                            "sent": self._sent,
                            "failed": self._failed,
                            "dropped": self._dropped,
                            "anomalies": self._anomalies,
                            "achieved_eps": round(self._sent / elapsed, 2),
                            "target_eps": self._eps,
                        }
                    },
                )
                last_stats = time.monotonic()

            if self._stop.wait(sleep_for):
                break

    @property
    def counters(self) -> dict[str, int]:
        return {
            "sent": self._sent,
            "failed": self._failed,
            "dropped": self._dropped,
            "anomalies": self._anomalies,
        }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Synthetic transaction producer for the fraud-detection pipeline.",
        epilog="Example: python producer/transaction_producer.py --eps 100 --dry-run | head -5",
    )
    parser.add_argument("--bootstrap-servers", default="localhost:29092", help="comma-separated broker list")
    parser.add_argument("--topic", default=DEFAULT_TOPIC, help="destination topic")
    parser.add_argument("--eps", type=float, default=50.0, help="target events per second")
    parser.add_argument("--users", type=int, default=200, help="synthetic user pool size")
    parser.add_argument("--merchants", type=int, default=40, help="merchant pool size")
    parser.add_argument("--duration", type=float, default=None, help="stop after N seconds (default: run forever)")
    parser.add_argument("--seed", type=int, default=None, help="RNG seed for reproducibility")
    parser.add_argument("--dry-run", action="store_true", help="print JSON to stdout instead of Kafka")
    parser.add_argument("--log-level", default="INFO", choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args(argv)
    if args.eps <= 0:
        parser.error("--eps must be positive")
    return args


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    configure_logging(args.log_level)

    rng = random.Random(args.seed)
    factory = TransactionFactory(n_users=args.users, n_merchants=args.merchants, rng=rng)
    producer = TransactionProducer(
        bootstrap_servers=args.bootstrap_servers,
        topic=args.topic,
        eps=args.eps,
        factory=factory,
    )

    signal.signal(signal.SIGINT, producer.request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, producer.request_stop)

    if args.dry_run:
        LOGGER.info("dry-run mode enabled — printing events to stdout")
        producer.run(duration_s=args.duration, dry_run=True)
        return 0

    try:
        producer.start()
        producer.run(duration_s=args.duration)
    finally:
        producer.close()
    LOGGER.info("exit", extra={"extra_fields": producer.counters})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
