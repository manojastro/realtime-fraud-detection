#!/usr/bin/env bash
# File: infra/topics_setup.sh
# Explicit Kafka topic provisioning with partition counts and retention policies.
# Idempotent (--if-not-exists). Runs inside the kafka-init compose service or on
# the host against localhost:29092.

set -euo pipefail

BOOTSTRAP="${BOOTSTRAP_SERVERS:-localhost:29092}"

# Locate kafka-topics.sh across distributions (bitnami, confluent, plain tarball)
if command -v kafka-topics.sh >/dev/null 2>&1; then
  KT="kafka-topics.sh"
elif [[ -x /opt/bitnami/kafka/bin/kafka-topics.sh ]]; then
  KT="/opt/bitnami/kafka/bin/kafka-topics.sh"
elif [[ -x /opt/kafka/bin/kafka-topics.sh ]]; then
  KT="/opt/kafka/bin/kafka-topics.sh"
else
  echo "[topics] ERROR: kafka-topics.sh not found on PATH or standard locations" >&2
  exit 1
fi

echo "[topics] waiting for broker at ${BOOTSTRAP} ..."
for i in $(seq 1 60); do
  if "$KT" --bootstrap-server "$BOOTSTRAP" --list >/dev/null 2>&1; then
    echo "[topics] broker is up (attempt ${i})"
    break
  fi
  if [[ "$i" -eq 60 ]]; then
    echo "[topics] ERROR: broker did not become ready in time" >&2
    exit 1
  fi
  sleep 2
done

echo "[topics] creating topics ..."

# Raw transactions: 6 partitions for per-user ordering + parallel velocity aggs,
# 24h retention, 128MB segments.
"$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic transactions.raw \
  --partitions 6 \
  --replication-factor 1 \
  --config retention.ms=86400000 \
  --config segment.bytes=134217728 \
  --config cleanup.policy=delete \
  --config min.insync.replicas=1 \
  --config compression.type=producer

# Fraud alerts sink topic (optional mirror of Delta rows for push consumers).
"$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic transactions.alerts \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=604800000 \
  --config cleanup.policy=delete

# Dead-letter queue for poison payloads that fail schema validation downstream.
"$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic transactions.dlq \
  --partitions 3 \
  --replication-factor 1 \
  --config retention.ms=259200000 \
  --config cleanup.policy=delete

echo "[topics] current topics:"
"$KT" --bootstrap-server "$BOOTSTRAP" --list

for t in transactions.raw transactions.alerts transactions.dlq; do
  echo "[topics] describe ${t}:"
  "$KT" --bootstrap-server "$BOOTSTRAP" --describe --topic "$t"
done

echo "[topics] done."
