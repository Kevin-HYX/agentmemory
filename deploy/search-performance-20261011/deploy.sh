#!/usr/bin/env bash
set -euo pipefail
service=/home/kevin/services/agentmemory
backup=/data/backups/agentmemory/20261011-search-performance
test -s "$backup/CONSISTENT_BACKUP_VERIFIED"
test "$(findmnt -n -o SOURCE --target /data)" = /dev/sda1
cd "$service"
original=local/agentmemory:0.9.29-sharded-20260908
candidate=local/agentmemory:0.9.29-search-performance-20261011
docker image inspect "$candidate" >/dev/null
rollback() {
  cp "$backup/service/compose.yaml" "$service/compose.yaml"
  docker compose up -d --no-build --force-recreate agentmemory
}
trap rollback ERR
python3 - <<'PY'
from pathlib import Path
p = Path('compose.yaml')
s = p.read_text()
old = 'context: ./patch-20260908\n      dockerfile: Dockerfile.patch\n    image: local/agentmemory:0.9.29-sharded-20260908'
new = 'context: ./search-performance-20261011\n      dockerfile: Dockerfile\n    image: local/agentmemory:0.9.29-search-performance-20261011'
assert s.count(old) == 1, '部署配置不符合预期，停止切换'
p.write_text(s.replace(old, new))
PY
docker compose config --quiet
docker compose up -d --no-build --force-recreate agentmemory
for attempt in $(seq 1 60); do
  status=$(docker inspect agentmemory-agentmemory-1 --format '{{.State.Health.Status}}')
  if [ "$status" = healthy ]; then
    docker compose up -d --no-build frpc
    docker inspect agentmemory-agentmemory-1 --format '{{.Image}} {{.State.Health.Status}}'
    trap - ERR
    exit 0
  fi
  sleep 3
done
printf '候选服务未通过健康检查，自动回滚\n' >&2
false
