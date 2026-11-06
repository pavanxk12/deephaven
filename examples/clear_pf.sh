#!/usr/bin/env bash
# Erase ALL PF/registry/limits state for ALL strategies: every row in Postgres (algo_portfolios,
# algo_pf_map, algo_pf_sub, algo_limits) plus every pf_map_*.json, pf_sub_*.json, limits.json and
# portfolio_registry.json backup file, so nothing gets reimported.
# Run this on the deephaven host (104/130/131), from the compose directory, BEFORE
# restarting deephaven.
#
# Usage:
#   ./clear_pf.sh [state_dir]
#
# [state_dir]   directory holding pf_map_*.json and portfolio_registry.json. If left out, it is
#               found automatically from the deephaven container's own /data/deephaven mount
#               (works whether that is a bind mount under the compose dir, like ./deephaven/state,
#               or a named Docker volume, like /var/lib/docker/volumes/..._data/state on 104).
#
# Needs: a running "postgres" container/service (docker compose exec), and ALGO_PG_PASSWORD
# set in the environment (or a ~/.pgpass entry) if the algofusion user requires a password.

set -euo pipefail

if [[ $# -ge 1 ]]; then
    STATE_DIR="$1"
else
    container="$(docker compose ps -q deephaven)"
    src=""
    if [[ -n "$container" ]]; then
        src="$(docker inspect "$container" --format '{{range .Mounts}}{{if eq .Destination "/data/deephaven"}}{{.Source}}{{end}}{{end}}')"
    fi
    if [[ -n "$src" ]]; then
        STATE_DIR="${src}/state"
        echo "state dir auto-detected from the deephaven container's /data/deephaven mount: ${STATE_DIR}"
    else
        STATE_DIR="./deephaven/state"
        echo "deephaven container/mount not found, falling back to ${STATE_DIR}"
    fi
fi

echo "== Wiping ALL rows from algo_portfolios, algo_pf_map, algo_pf_sub, algo_limits =="
docker compose exec -T postgres psql -U algofusion -d algofusion -v ON_ERROR_STOP=1 <<SQL
delete from algo_portfolios;
delete from algo_pf_map;
delete from algo_pf_sub;
delete from algo_limits;
SQL

echo "== Resetting all pf_map_*.json, pf_sub_*.json and limits.json in ${STATE_DIR} to {} =="
for f in "${STATE_DIR}"/pf_map_*.json "${STATE_DIR}"/pf_sub_*.json "${STATE_DIR}"/limits.json; do
    [[ -e "$f" ]] || continue
    echo '{}' > "$f"
    echo "  reset $f"
done

# portfolio_registry.json is NOT a {} object: it is JSONL, one {"u":...,"p":...} record per line.
# Writing "{}" into it corrupts it (the reader throws on line 1 forever). An empty file is its
# correct "nothing to reimport" state.
reg="${STATE_DIR}/portfolio_registry.json"
if [[ -e "$reg" ]]; then
    : > "$reg"
    echo "  reset $reg (emptied, not {})"
fi

echo "Done. Restart deephaven now:  docker compose up -d --no-deps --force-recreate deephaven"
