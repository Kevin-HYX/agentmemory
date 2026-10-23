#!/usr/bin/env bash
set -euo pipefail
service=/home/kevin/services/agentmemory
backup=/data/backups/agentmemory/20261011-search-performance
test -s "$backup/CONSISTENT_BACKUP_VERIFIED"
if ! docker image inspect local/agentmemory:0.9.29-sharded-20260908 >/dev/null 2>&1; then
  sha256sum -c "$backup/original-image.sha256"
  docker image load -i "$backup/original-image.tar"
fi
cp "$backup/service/compose.yaml" "$service/compose.yaml"
cd "$service"
docker compose up -d --no-build --force-recreate agentmemory
docker compose up -d --no-build frpc
