# File: streaming/fraud_stream.py
"""Real-Time Fraud Detection - PySpark Structured Streaming job (THE CORE).

Stages:
  1. Read raw events from Kafka topic ``transactions.raw``.
  2. Parse + validate payloads against an explicit StructType via ``from_json``.
  3. Apply an event-time watermark on ``event_time`` (late-data tolerance).
  4. Velocity features per user: windowed aggregations (count/amount over
     5-minute and 1-hour tumbling windows) over the stateful stream.
  5. Stream-stream join features back onto transactions, then enrich with a
     static User Risk Profile broadcast join.
  6. Score via vectorized ``mock_ml_score`` pandas UDF; classify risk bands.
  7. Upsert into Delta Lake via ``foreachBatch`` + MERGE keyed on ``event_id``.

Why foreachBatch MERGE instead of plain append?
  - Checkpointed offsets give at-least-once into the sink; batches are REPLAYED
    after failures. Append would duplicate rows; MERGE on the unique event_id
    is idempotent => effectively-once materialization.
  - Delta ACID makes each MERGE atomic (no half-applied batches).
  - Consumers get a stable primary key for lookups/downstream joins.

Local run:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 \
      streaming/fraud_stream.py --bootstrap-servers localhost:29092 \
      --checkpoint ./checkpoints/fraud_stream --output ./delta/fraud_alerts \
      --profiles-path ./data/risk_profiles.json

On Databricks use databricks/notebook_run.py (Delta/Kafka connector built in).
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import threading
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.pandas.functions import pandas_udf
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType

try:  # package-style import (Databricks runner)
    from streaming.schemas import (
        ALERT_SCHEMA, BAND_ACTIONS, ML_HIGH_BAND, ML_MEDIUM_BAND,
        TOPIC_RAW, TRANSACTION_SCHEMA, WATERMARK_DELAY, WINDOW_LONG, WINDOW_SHORT,
    )
    from streaming.user_profiles import broadcast_profiles, load_user_profiles
except ImportError:  # script-style import (spark-submit puts script dir on sys.path)
    from schemas import (  # type: ignore[no-redef]
        ALERT_SCHEMA, BAND_ACTIONS, ML_HIGH_BAND, ML_MEDIUM_BAND,
        TOPIC_RAW, TRANSACTION_SCHEMA, WATERMARK_DELAY, WINDOW_LONG, WINDOW_SHORT,
    )
    from user_profiles import broadcast_profiles, load_user_profiles  # type: ignore[no-redef]

try:
    from delta.tables import DeltaTable
    DELTA_AVAILABLE = True
except ImportError:  # pragma: no cover
    DeltaTable = None  # type: ignore[assignment]
    DELTA_AVAILABLE = False

LOGGER = logging.getLogger("fraud_stream")


class JsonFormatter(logging.Formatter):
    """Structured JSON-lines formatter for driver-side logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": pd.Timestamp.utcnow().isoformat(),
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


def configure_logging(level: str = "INFO") -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    logging.basicConfig(level=getattr(logging, level.upper(), logging.INFO),
                        handlers=[handler], force=True)


@dataclass(frozen=True)
class StreamConfig:
    """All runtime knobs; CLI flags override environment defaults."""

    kafka_bootstrap_servers: str
    topic: str
    checkpoint_dir: str
    delta_output_path: str
    profiles_path: str
    trigger_interval: str = "10 seconds"
    starting_offsets: str = "latest"
    max_offsets_per_trigger: int = 50_000  # backpressure cap per micro-batch
    watermark_delay: str = WATERMARK_DELAY
    master: str = "local[*]"
    app_name: str = "realtime-fraud-detection"
    log_level: str = "INFO"
    is_databricks: bool = field(
        default_factory=lambda: bool(os.environ.get("DATABRICKS_RUNTIME_VERSION"))
    )


def build_arg_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="Real-time fraud detection streaming job")
    p.add_argument("--bootstrap-servers", default=os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092"))
    p.add_argument("--topic", default=os.getenv("KAFKA_TOPIC_TRANSACTIONS", TOPIC_RAW))
    p.add_argument("--checkpoint", default=os.getenv("CHECKPOINT_DIR", "./checkpoints/fraud_stream"))
    p.add_argument("--output", default=os.getenv("DELTA_OUTPUT_PATH", "./delta/fraud_alerts"))
    p.add_argument("--profiles-path", default=os.getenv("PROFILES_PATH", "data/risk_profiles.json"))
    p.add_argument("--trigger", default=os.getenv("SPARK_TRIGGER_INTERVAL", "10 seconds"))
    p.add_argument("--starting-offsets", default=os.getenv("STARTING_OFFSETS", "latest"),
                   choices=["earliest", "latest"])
    p.add_argument("--max-offsets-per-trigger", type=int,
                   default=int(os.getenv("MAX_OFFSETS_PER_TRIGGER", "50000")))
    p.add_argument("--watermark-delay", default=os.getenv("WATERMARK_DELAY", WATERMARK_DELAY))
    p.add_argument("--master", default=os.getenv("SPARK_MASTER", "local[*]"))
    p.add_argument("--log-level", default=os.getenv("LOG_LEVEL", "INFO"))
    return p


def config_from_args(argv: list[str] | None = None) -> StreamConfig:
    a = build_arg_parser().parse_args(argv)
    return StreamConfig(
        kafka_bootstrap_servers=a.bootstrap_servers, topic=a.topic,
        checkpoint_dir=a.checkpoint, delta_output_path=a.output,
        profiles_path=a.profiles_path, trigger_interval=a.trigger,
        starting_offsets=a.starting_offsets,
        max_offsets_per_trigger=a.max_offsets_per_trigger,
        watermark_delay=a.watermark_delay, master=a.master, log_level=a.log_level,
    )


class ProgressListener(StreamingQueryListener):
    """Logs per-microbatch progress; wire into Prometheus in production."""

    def onQueryStarted(self, event: Any) -> None:  # noqa: N802
        LOGGER.info("streaming query started",
                    extra={"extra_fields": {"query_id": str(event.id), "name": event.name}})

    def onQueryProgress(self, event: Any) -> None:  # noqa: N802
        p = event.progress
        LOGGER.info("micro-batch complete", extra={"extra_fields": {
            "batch_id": p.batchId,
            "input_rows": p.numInputRows,
            "rows_per_sec": round(float(getattr(p, "processedRowsPerSecond", 0.0) or 0.0), 1),
            "duration_ms": dict(getattr(p, "durationMs", {}) or {}),
            "sink": getattr(p.sink, "description", "unknown"),
        }})

    def onQueryTerminated(self, event: Any) -> None:  # noqa: N802
        LOGGER.error("streaming query terminated", extra={"extra_fields": {
            "query_id": str(event.id), "exception": getattr(event, "exception", None)}})


def build_spark_session(cfg: StreamConfig) -> SparkSession:
    builder = SparkSession.builder.appName(cfg.app_name)
    if not cfg.is_databricks:
        builder = (
            builder.master(cfg.master)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.SparkCatalog")
            .config("spark.sql.shuffle.partitions", "8")  # sized for streaming state stores
            .config("spark.sql.session.timeZone", "UTC")
        )
    spark = builder.getOrCreate()
    if not cfg.is_databricks:
        spark.streams.addListener(ProgressListener())
    spark.sparkContext.setLogLevel("WARN")
    return spark


def parse_and_validate(raw_df: DataFrame) -> DataFrame:
    """Decode Kafka values and enforce the event contract.

    Malformed rows (bad JSON, null ids, unparseable timestamps) are dropped;
    production hardening would route them to transactions.dlq with reason codes.
    """
    parsed = (
        raw_df.select(F.from_json(F.col("value").cast("string"), TRANSACTION_SCHEMA).alias("evt"))
        .select("evt.*")
        .withColumn(
            "event_time",
            F.coalesce(
                F.timestamp_millis(F.col("ts_ms")),   # authoritative epoch millis
                F.to_timestamp(F.col("timestamp")),   # RFC-3339 fallback
            ),
        )
    )
    return parsed.filter(
        F.col("event_id").isNotNull()
        & F.col("user_id").isNotNull()
        & F.col("event_time").isNotNull()
        & F.col("amount").isNotNull()
        & (F.col("amount") >= 0.0)
    )


def build_window_aggregates(base: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Per-user txn count/amount over 5-minute and 1-hour tumbling windows.

    Both aggregate streams inherit the base watermark so they can be joined
    back onto individual transactions (stream-stream join below).
    """
    agg_short = (
        base.groupBy(F.window(F.col("event_time"), WINDOW_SHORT).alias("w"), F.col("user_id"))
        .agg(F.count(F.lit(1)).alias("txn_count_5m"),
             F.sum(F.col("amount")).alias("amount_sum_5m"))
        .select("user_id", F.col("w.start").alias("w5_start"), F.col("w.end").alias("w5_end"),
                "txn_count_5m", "amount_sum_5m")
    )
    agg_long = (
        base.groupBy(F.window(F.col("event_time"), WINDOW_LONG).alias("w"), F.col("user_id"))
        .agg(F.count(F.lit(1)).alias("txn_count_1h"),
             F.sum(F.col("amount")).alias("amount_sum_1h"))
        .select("user_id", F.col("w.start").alias("w1_start"), F.col("w.end").alias("w1_end"),
                "txn_count_1h", "amount_sum_1h")
    )
    return agg_short, agg_long


@pandas_udf(DoubleType())
def mock_ml_score(
    txn_count_5m: pd.Series,
    txn_count_1h: pd.Series,
    amount_vs_avg: pd.Series,
    geo_mismatch: pd.Series,
    round_amount: pd.Series,
) -> pd.Series:
    """Vectorized logistic toy model standing in for a trained fraud model.

    Swap this UDF for an MLflow pyfunc loader or ONNX inference without
    touching surrounding pipeline code. Weights encode intuitive drivers:
    rapid repeat activity (5m weighted > 1h), spend far above the user's
    average, cross-border IP mismatch, suspiciously round amounts.
    """
    z = (
        -1.75
        + 0.55 * np.log1p(txn_count_5m.fillna(0).to_numpy(dtype=float))
        + 0.30 * np.log1p(txn_count_1h.fillna(0).to_numpy(dtype=float))
        + 0.80 * np.log1p(amount_vs_avg.clip(lower=0.0).fillna(0.0).to_numpy(dtype=float))
        + 1.10 * geo_mismatch.fillna(False).to_numpy(dtype=float)
        + 0.60 * round_amount.fillna(False).to_numpy(dtype=float)
    )
    return pd.Series(1.0 / (1.0 + np.exp(-z)), index=txn_count_5m.index)


def build_alert_stream(
    parsed: DataFrame,
    base: DataFrame,
    profiles: DataFrame,
) -> DataFrame:
    """Join velocity aggregates back onto the stream, then broadcast-enrich.

    Stream-stream joins require watermarks on BOTH sides plus an event-time
    range condition - hence the transaction side aliases the watermarked
    ``base`` rather than the raw parsed stream.
    """
    agg_short, agg_long = build_window_aggregates(base)

    tx = base.alias("tx")
    j_short = tx.join(
        agg_short.alias("f5"),
        F.expr("tx.user_id = f5.user_id AND tx.event_time >= f5.w5_start AND tx.event_time < f5.w5_end"),
        "left",
    )
    j_long = j_short.join(
        agg_long.alias("f1"),
        F.expr("tx.user_id = f1.user_id AND tx.event_time >= f1.w1_start AND tx.event_time < f1.w1_end"),
        "left",
    )
    enriched = j_long.join(
        broadcast_profiles(profiles).alias("pr"),
        F.expr("tx.user_id = pr.user_id"),
        "left",
    )

    featured = enriched.select(
        F.col("tx.event_id").alias("event_id"),
        F.col("tx.user_id").alias("user_id"),
        F.col("tx.merchant_id").alias("merchant_id"),
        F.col("tx.amount").alias("amount"),
        F.col("tx.currency").alias("currency"),
        F.col("tx.event_time").alias("event_time"),
        F.col("tx.card_present").alias("card_present"),
        F.coalesce(F.col("f5.txn_count_5m"), F.lit(0)).cast("long").alias("txn_count_5m"),
        F.coalesce(F.col("f1.txn_count_1h"), F.lit(0)).cast("long").alias("txn_count_1h"),
        F.coalesce(F.col("f1.amount_sum_1h"), F.lit(0.0)).cast("double").alias("amount_sum_1h"),
        F.when(
            F.col("tx.ip_country").isNotNull() & F.col("pr.country").isNotNull(),
            F.col("tx.ip_country") != F.col("pr.country"),
        ).otherwise(F.lit(False)).alias("geo_mismatch"),
        ((F.col("tx.amount") % 100.0 == 0.0) & (F.col("tx.amount") >= 100.0)).alias("round_amount"),
        F.when(F.col("pr.avg_txn_amount") > 0,
               F.col("tx.amount") / F.col("pr.avg_txn_amount")).otherwise(F.lit(0.0))
        .alias("amount_vs_avg"),
        F.lower(F.coalesce(F.col("pr.risk_tier"), F.lit("unknown"))).alias("profile_risk_tier"),
        F.coalesce(F.col("pr.country"), F.col("tx.home_country")).alias("profile_country"),
    )

    scored = featured.withColumn(
        "ml_score",
        mock_ml_score(
            F.col("txn_count_5m"), F.col("txn_count_1h"), F.col("amount_vs_avg"),
            F.col("geo_mismatch"), F.col("round_amount"),
        ),
    )

    banded = (
        scored.withColumn(
            "risk_band",
            F.when(F.col("ml_score") >= ML_HIGH_BAND, F.lit("HIGH"))
            .when(F.col("ml_score") >= ML_MEDIUM_BAND, F.lit("MEDIUM"))
            .otherwise(F.lit("LOW")),
        )
        .withColumn(
            "recommended_action",
            F.coalesce(
                F.expr(
                    "map('HIGH','BLOCK_AND_REVIEW','MEDIUM','CHALLENGE_2FA','LOW','ALLOW')[risk_band]"
                ),
                F.lit(BAND_ACTIONS["LOW"]),
            ),
        )
        .withColumn("processed_at", F.current_timestamp())
    )

    # Enforce the documented sink contract (column set + order).
    return banded.select(*ALERT_SCHEMA.fieldNames())


def _last_operation_metrics(spark: SparkSession, path: str) -> dict[str, int]:
    """Extract inserted/updated counts from the last Delta history entry."""
    try:
        hist = DeltaTable.forPath(spark, path).history(1).select("operationMetrics").collect()
        metrics = hist[0][0] if hist else {}
        return {
            "rows_inserted": int(metrics.get("numTargetRowsInserted", 0)),
            "rows_updated": int(metrics.get("numTargetRowsUpdated", 0)),
        }
    except Exception:  # noqa: BLE001 - metrics must never break the loop
        return {}


def make_merge_writer(spark: SparkSession, cfg: StreamConfig):
    """Build the foreachBatch callable performing idempotent MERGE upserts."""

    def upsert_to_delta(batch_df: DataFrame, batch_id: int) -> None:
        deduped = batch_df.dropDuplicates(subset=["event_id"])
        if deduped.isEmpty():
            LOGGER.info("empty micro-batch; skipping", extra={"extra_fields": {"batch_id": batch_id}})
            return

        try:
            if not (DELTA_AVAILABLE and DeltaTable.isDeltaTable(spark, cfg.delta_output_path)):
                # First run: create the table so MERGE has a target.
                (deduped.write.format("delta")
                 .mode("overwrite")
                 .option("overwriteSchema", "true")
                 .save(cfg.delta_output_path))
                LOGGER.info("initialized delta sink", extra={"extra_fields": {
                    "path": cfg.delta_output_path,
                    "rows": deduped.count(),
                }})
                return

            deduped.createOrReplaceTempView("alerts_micro_batch")
            spark.sql(
                f"""
                MERGE INTO delta.`{cfg.delta_output_path}` AS t
                USING alerts_micro_batch AS s
                ON t.event_id = s.event_id
                WHEN MATCHED THEN UPDATE SET *
                WHEN NOT MATCHED THEN INSERT *
                """
            )
            LOGGER.info("merged micro-batch into delta", extra={"extra_fields": {
                "batch_id": batch_id,
                **_last_operation_metrics(spark, cfg.delta_output_path),
            }})
        except Exception:
            LOGGER.exception("failed to write micro-batch",
                             extra={"extra_fields": {"batch_id": batch_id}})
            raise  # fail the query; checkpoint will replay this batch on restart

    return upsert_to_delta


def run(cfg: StreamConfig) -> None:
    configure_logging(cfg.log_level)
    if not DELTA_AVAILABLE and not cfg.is_databricks:
        LOGGER.warning("delta-spark not importable on the driver; pass --packages io.delta:delta-spark_2.12:3.2.0")

    LOGGER.info("starting fraud detection stream", extra={"extra_fields": {
        "brokers": cfg.kafka_bootstrap_servers,
        "topic": cfg.topic,
        "checkpoint": cfg.checkpoint_dir,
        "output": cfg.delta_output_path,
        "profiles": cfg.profiles_path,
        "trigger": cfg.trigger_interval,
        "watermark": cfg.watermark_delay,
        "max_offsets_per_trigger": cfg.max_offsets_per_trigger,
    }})

    spark = build_spark_session(cfg)
    stopping = threading.Event()

    def _request_stop(signum: int, _frame: Any) -> None:
        LOGGER.warning("shutdown signal received (sig=%s); stopping query ...", signum)
        stopping.set()

    signal.signal(signal.SIGINT, _request_stop)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, _request_stop)

    query = None
    try:
        profiles = load_user_profiles(spark, cfg.profiles_path)

        raw_df = (
            spark.readStream.format("kafka")
            .option("kafka.bootstrap.servers", cfg.kafka_bootstrap_servers)
            .option("subscribe", cfg.topic)
            .option("startingOffsets", cfg.starting_offsets)
            .option("failOnDataLoss", "false")
            .option("maxOffsetsPerTrigger", str(cfg.max_offsets_per_trigger))  # backpressure cap
            .load()
        )

        parsed = parse_and_validate(raw_df)
        base = parsed.withWatermark("event_time", cfg.watermark_delay)
        alerts = build_alert_stream(parsed, base, profiles)

        query = (
            alerts.writeStream
            .outputMode("append")  # required for stream-stream joins
            .foreachBatch(make_merge_writer(spark, cfg))
            .option("checkpointLocation", cfg.checkpoint_dir)
            .trigger(processingTime=cfg.trigger_interval)
            .queryName("fraud_detection_alerts")
            .start()
        )

        while not stopping.is_set() and query.isActive:
            query.awaitTermination(timeout=2)

        if query.isActive:
            query.stop()
        final = query.lastProgress
        LOGGER.info("streaming stopped cleanly", extra={"extra_fields": {
            "last_batch_id": getattr(final, "batchId", None),
        }})
    except Exception:
        LOGGER.exception("fatal error in streaming job")
        raise
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    cfg = config_from_args(argv if argv is not None else sys.argv[1:])
    try:
        run(cfg)
        return 0
    except KeyboardInterrupt:
        return 130


if __name__ == "__main__":
    raise SystemExit(main())
