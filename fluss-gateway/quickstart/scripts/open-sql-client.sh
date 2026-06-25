#!/usr/bin/env bash
#
# open-sql-client.sh — open an interactive Flink SQL Client against the quickstart cluster.
#
# Uses the standard client (sql-client.sh) inside the jobmanager container, NOT
# the image's preloading `/opt/sql-client/sql-client` wrapper.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUICKSTART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_compose.sh
source "${SCRIPT_DIR}/_compose.sh"
cd "${QUICKSTART_DIR}"

echo ">> Opening Flink SQL Client (Ctrl-D or 'quit;' to exit)..."
echo ">> Paste quickstart/sql/init-refund-context-tables.sql first, then quickstart/sql/seed-or-pipeline.sql."
echo ">> If you want Paimon lake history as well, start the separate tiering job after the seed data is loaded."
exec compose exec jobmanager /opt/flink/bin/sql-client.sh
