# File: databricks/notebook_run.py
"""Databricks Jobs-compatible thin runner for the fraud-detection stream.

Setup (one-time):
  1. Attach this repo as a Databricks Repo, e.g.
     /Workspace/Repos/<user>/04-realtime-fraud-detection
  2. Create a Job task of type "Notebook" pointing at this file with a
     cluster running DBR >= 13.x (Delta + Kafka connector are built in).

Widgets mirror the CLI flags of streaming/fraud_stream.py; every widget falls
back to an environment variable of the same upper-cased name so the job can
also be driven from `databricks sdk` / REST without UI interaction.

NOTE: the query runs continuously. Schedule this task with a continuous job
for always-on monitoring, or stop it manually; checkpointing on DBFS/Volumes
makes restarts resume exactly where the previous run left off.
"""

# COMMAND ----------

import os
import sys

try:  # present in notebooks and jobs on DBR >= 12
    from databricks.sdk.runtime import dbutils  # type: ignore
except ImportError:  # local IDE / unit-testing fallback
    dbutils = None  # type: ignore[assignment]


def _widget(name: str, default: str) -> str:
    if dbutils is not None:
        dbutils.widgets.text(name, default, name)
        return str(dbutils.widgets.get(name))
    return os.environ.get(name.upper(), default)


PROJECT_ROOT = _widget("project_root", "/Workspace/Repos/${workspace_user}/04-realtime-fraud-detection")
if PROJECT_ROOT and PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

# COMMAND ----------

from streaming.fraud_stream import StreamConfig, run  # noqa: E402

cfg = StreamConfig(
    kafka_bootstrap_servers=_widget("kafka_bootstrap_servers", "host.docker.internal:29092"),
    topic=_widget("topic", "transactions.raw"),
    # Checkpoints must live on durable storage so restarts resume cleanly.
    checkpoint_dir=_widget("checkpoint_dir", "dbfs:/mnt/fraud/checkpoints/fraud_stream"),
    delta_output_path=_widget("delta_output_path", "/mnt/fraud/delta/fraud_alerts"),
    profiles_path=_widget(
        "profiles_path",
        f"{PROJECT_ROOT}/data/risk_profiles.json",
    ),
    trigger_interval=_widget("trigger_interval", "10 seconds"),
    starting_offsets=_widget("starting_offsets", "latest"),
    max_offsets_per_trigger=int(_widget("max_offsets_per_trigger", "50000")),
    watermark_delay=_widget("watermark_delay", "10 minutes"),
    log_level=_widget("log_level", "INFO"),
)

print(f"[notebook_run] config: {cfg}")

# COMMAND ----------

run(cfg)  # blocks until the query is stopped via UI/API/signal

if dbutils is not None:
    dbutils.notebook.exit("fraud_detection_stream_stopped_cleanly")
