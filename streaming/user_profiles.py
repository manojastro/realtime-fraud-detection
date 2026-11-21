# File: streaming/user_profiles.py
"""Static User Risk Profile loader for the broadcast enrichment join.

Profiles are expected as a JSON array-of-objects file or a parquet directory.
In production this snapshot would be regenerated nightly by a batch job from
the feature store / Postgres; here it ships as ``data/risk_profiles.json``.

Expected columns (enforced):

* ``user_id``           string, unique
* ``risk_tier``         string in {low, medium, high}
* ``lifetime_amount``   double >= 0
* ``avg_txn_amount``    double > 0
* ``country``           ISO-3166 alpha-2 home country
"""

from __future__ import annotations

import logging
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

LOGGER = logging.getLogger("fraud_stream.profiles")

EXPECTED_COLUMNS: tuple[str, ...] = (
    "user_id",
    "risk_tier",
    "lifetime_amount",
    "avg_txn_amount",
    "country",
)

USER_PROFILE_SCHEMA = StructType(
    [
        StructField("user_id", StringType(), False),
        StructField("risk_tier", StringType(), True),
        StructField("lifetime_amount", DoubleType(), True),
        StructField("avg_txn_amount", DoubleType(), True),
        StructField("country", StringType(), True),
    ]
)


class ProfileLoadError(RuntimeError):
    """Raised when the risk-profile source is missing or violates its contract."""


def load_user_profiles(spark: SparkSession, path: str) -> DataFrame:
    """Load and validate the User Risk Profile table.

    Supports ``.json`` (array-of-objects) and parquet paths/directories.
    Returns a deduplicated DataFrame with exactly ``EXPECTED_COLUMNS``.
    """
    resolved = Path(path)
    if not resolved.exists():
        raise ProfileLoadError(f"user risk profiles not found at: {path}")

    if resolved.suffix.lower() == ".json":
        reader = spark.read.option("multiLine", "true")
        df = reader.json(str(resolved))
    elif resolved.is_dir() or resolved.suffix.lower() == ".parquet":
        df = spark.read.parquet(str(resolved))
    else:
        raise ProfileLoadError(
            f"unsupported profile format '{resolved.suffix}' (use .json or .parquet)"
        )

    missing = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing:
        raise ProfileLoadError(f"profile source is missing required columns: {missing}")

    profiles = (
        df.select(
            F.col("user_id").cast(StringType()).alias("user_id"),
            F.lower(F.trim(F.col("risk_tier"))).cast(StringType()).alias("risk_tier"),
            F.col("lifetime_amount").cast(DoubleType()).alias("lifetime_amount"),
            F.col("avg_txn_amount").cast(DoubleType()).alias("avg_txn_amount"),
            F.upper(F.trim(F.col("country"))).cast(StringType()).alias("country"),
        )
        .filter(F.col("user_id").isNotNull())
        .dropDuplicates(["user_id"])
    )

    row_count = profiles.count()  # static DF: bounded, safe action
    if row_count == 0:
        raise ProfileLoadError(f"profile source at {path} contains no rows")

    LOGGER.info(
        "loaded user risk profiles",
        extra={"extra_fields": {"path": path, "rows": row_count}},
    )
    return profiles


def broadcast_profiles(profiles: DataFrame) -> DataFrame:
    """Return the profiles DF wrapped in a broadcast hint.

    Static + tiny (thousands of rows max) => broadcast join avoids shuffling
    the unbounded stream side on user_id.
    """
    return F.broadcast(profiles)
