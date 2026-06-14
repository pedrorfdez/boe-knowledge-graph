#!/usr/bin/env bash
set -e

echo "Waiting for services..."

wait_for() {
  local name=$1
  local cmd=$2
  local retries=30
  until eval "$cmd" > /dev/null 2>&1; do
    retries=$((retries - 1))
    if [ $retries -eq 0 ]; then
      echo "TIMEOUT: $name did not become ready"
      exit 1
    fi
    echo "  $name not ready, retrying..."
    sleep 2
  done
  echo "  $name ready"
}

wait_for "Neo4j"         "docker compose exec neo4j wget -q --spider http://localhost:7474"
wait_for "Qdrant"        "curl -sf http://localhost:6333/readyz"
wait_for "Elasticsearch" "curl -sf http://localhost:9200/_cluster/health"
wait_for "Redis"         "docker compose exec redis redis-cli ping"
wait_for "MinIO"         "curl -sf http://localhost:9000/minio/health/live"
wait_for "API"           "docker compose exec api python -c \"import urllib.request; urllib.request.urlopen('http://localhost:8000/health')\""

echo "All services ready."
