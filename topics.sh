#!/usr/bin/env bash
set -euo pipefail

BROKER="kafka:9092"

topics=(
  questions.pending
  answers.success
  answers.retry
  answers.validated
  answers.rejected.lowquality
)


for t in "${topics[@]}"; do
  echo "[Init] Creando tópico: $t"
  kafka-topics --create --if-not-exists \
    --topic "$t" \
    --partitions 1 \
    --replication-factor 1 \
    --bootstrap-server "$BROKER"
done

echo "[Init] Todos los tópicos creados"
