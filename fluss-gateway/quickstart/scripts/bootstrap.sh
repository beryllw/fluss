#!/usr/bin/env bash
#
# bootstrap.sh — bring up the Fluss + Flink + Gateway + RustFS quickstart cluster.
#
# Usage:
#   bash quickstart/scripts/bootstrap.sh          # start everything
#   bash quickstart/scripts/bootstrap.sh down     # stop and remove (keeps volumes)
#   bash quickstart/scripts/bootstrap.sh clean    # stop and remove + volumes

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUICKSTART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_compose.sh
source "${SCRIPT_DIR}/_compose.sh"
cd "${QUICKSTART_DIR}"

action="${1:-up}"

case "${action}" in
  up)
    echo ">> Starting quickstart cluster (compose up -d)..."
    compose up -d
    echo
    echo ">> Containers:"
    compose ps
    echo
    cat <<'EOF'
>> Next steps:
   1. Wait until all containers are healthy (re-run `docker compose ps`).
      Flink UI:    http://localhost:8083/
      Gateway:     PostgreSQL :5432, REST :8080, MCP :8000
      RustFS:      S3 :9000, Console :9001
   2. Load the refund investigation data:
        bash quickstart/scripts/run-demo-flow.sh
   3. Optional but recommended: start lakehouse tiering after the seed data is loaded:
        bash quickstart/scripts/run-tiering-job.sh
   4. Follow quickstart/README.md to connect MCP and investigate the refund case.
EOF
    ;;
  down)
    echo ">> Stopping quickstart cluster (keeping volumes)..."
    compose down
    ;;
  clean)
    echo ">> Stopping quickstart cluster and removing volumes..."
    compose down -v
    ;;
  *)
    echo "Usage: bootstrap.sh [up|down|clean]" >&2
    exit 2
    ;;
esac
