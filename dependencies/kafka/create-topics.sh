#!/bin/bash
set -e

BROKER=${KAFKA_BROKER:-kafka:9092}

kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic raw-trades --partitions 3 --replication-factor 1

kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic whale-alerts --partitions 1 --replication-factor 1

kafka-topics --bootstrap-server "$BROKER" --create --if-not-exists \
  --topic volatility-alerts --partitions 1 --replication-factor 1

echo "Topics created:"
kafka-topics --bootstrap-server "$BROKER" --list
