# File: streaming/schemas.py
"""Shared Spark StructTypes and tuning constants for the fraud pipeline.

The transaction schema is the wire contract with the producer; keep both in
sync. ``ALERT_SCHEMA`` documents the exact column set/order materialized into
the Delta sink (used by ``foreachBatch`` MERGE).
"""

from __future__ import annotations

from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# --- Kafka topics -----------------------------------------------------------

TOPIC_RAW = "transactions.raw"
TOPIC_ALERTS = "transactions.alerts"
TOPIC_DLQ = "transactions.dlq"

# --- Wire schema: raw transaction events (matches the producer) --------------

TRANSACTION_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),
        StructField("user_id", StringType(), False),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("timestamp", StringType(), True),  # RFC-3339 UTC string
        StructField("ts_ms", LongType(), True),  # epoch millis (authoritative)
        StructField("ip", StringType(), True),
        StructField("ip_country", StringType(), True),
        StructField("home_country", StringType(), True),
        StructField("device_fingerprint", StringType(), True),
        StructField("card_present", BooleanType(), True),
        StructField("anomaly_signal", StringType(), True),  # injected label (demo/eval only)
    ]
)

# --- Sink schema: enriched alerts written to Delta ----------------------------

ALERT_SCHEMA = StructType(
    [
        StructField("event_id", StringType(), False),  # idempotent MERGE key
        StructField("user_id", StringType(), False),
        StructField("merchant_id", StringType(), True),
        StructField("amount", DoubleType(), True),
        StructField("currency", StringType(), True),
        StructField("event_time", TimestampType(), False),
        StructField("card_present", BooleanType(), True),
        # velocity features (windowed aggregations, per user)
        StructField("txn_count_5m", LongType(), True),
        StructField("txn_count_1h", LongType(), True),
        StructField("amount_sum_1h", DoubleType(), True),
        # rule features
        StructField("geo_mismatch", BooleanType(), True),
        StructField("round_amount", BooleanType(), True),
        StructField("amount_vs_avg", DoubleType(), True),
        # scoring
        StructField("ml_score", DoubleType(), True),
        StructField("risk_band", StringType(), True),
        StructField("recommended_action", StringType(), True),
        # static profile enrichment (broadcast join)
        StructField("profile_risk_tier", StringType(), True),
        StructField("profile_country", StringType(), True),
        StructField("processed_at", TimestampType(), True),
    ]
)

# --- Tuning constants ---------------------------------------------------------

WATERMARK_DELAY = "10 minutes"  # late-event tolerance for stream-stream joins
WINDOW_SHORT = "5 minutes"  # velocity window A
WINDOW_LONG = "1 hour"  # velocity window B
TRIGGER_INTERVAL = "10 seconds"

ML_HIGH_BAND = 0.85
ML_MEDIUM_BAND = 0.60

BAND_ACTIONS: dict[str, str] = {
    "HIGH": "BLOCK_AND_REVIEW",
    "MEDIUM": "CHALLENGE_2FA",
    "LOW": "ALLOW",
}
