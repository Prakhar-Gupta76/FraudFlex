#!/usr/bin/env bash
set -euo pipefail

BOOTSTRAP_SERVERS="${KAFKA_BOOTSTRAP_SERVERS:-kafka:19092}"
KAFKA_TOPICS="/opt/kafka/bin/kafka-topics.sh"

create_topic() {
  local name="$1"
  local partitions="$2"
  local replication_factor="$3"
  shift 3

  local args=(
    --bootstrap-server "${BOOTSTRAP_SERVERS}"
    --create
    --if-not-exists
    --topic "${name}"
    --partitions "${partitions}"
    --replication-factor "${replication_factor}"
  )

  local config
  for config in "$@"; do
    args+=(--config "${config}")
  done

  "${KAFKA_TOPICS}" "${args[@]}"
}

create_topic \
  "transactions.raw" \
  3 \
  1 \
  "cleanup.policy=delete" \
  "retention.ms=86400000" \
  "retention.bytes=67108864"

create_topic \
  "transactions.scored" \
  3 \
  1 \
  "cleanup.policy=delete" \
  "retention.ms=86400000" \
  "retention.bytes=67108864"

create_topic \
  "fraud.alerts" \
  1 \
  1 \
  "cleanup.policy=delete" \
  "retention.ms=604800000" \
  "retention.bytes=134217728"

create_topic \
  "transactions.dead-letter" \
  1 \
  1 \
  "cleanup.policy=delete" \
  "retention.ms=604800000" \
  "retention.bytes=134217728"

"${KAFKA_TOPICS}" \
  --bootstrap-server "${BOOTSTRAP_SERVERS}" \
  --describe

