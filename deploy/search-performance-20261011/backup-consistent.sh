#!/usr/bin/env bash
set -euo pipefail
service=/home/kevin/services/agentmemory
backup=/data/backups/agentmemory/20261011-search-performance
test "$(findmnt -n -o SOURCE --target /data)" = /dev/sda1
test "$(realpath "$backup/service")" = "$backup/service"
test -s "$backup/original-image.tar"
cd "$service"
trap 'docker compose up -d --no-build agentmemory frpc >/dev/null' EXIT
docker compose stop -t 120 frpc agentmemory
rsync -a --delete "$service/" "$backup/service/"
(cd "$service"; find . -type f -print0 | sort -z | xargs -0 sha256sum) > "$backup/service.sha256"
(cd "$backup/service"; sha256sum --quiet -c ../service.sha256)
sha256sum "$backup/original-image.tar" > "$backup/original-image.sha256"
sync
date -u +%FT%TZ > "$backup/CONSISTENT_BACKUP_VERIFIED"
printf '一致备份已验证：%s\n' "$backup"
docker compose up -d --no-build agentmemory frpc
trap - EXIT
