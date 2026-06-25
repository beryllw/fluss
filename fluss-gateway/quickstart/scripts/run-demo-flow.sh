#!/usr/bin/env bash
#
# run-demo-flow.sh — initialize quickstart tables and seed the refund scenario.
#
# Feeds init + seed SQL into the standard Flink SQL client (`sql-client.sh -f`)
# running inside the jobmanager container.
#
# IMPORTANT: this script does NOT start the Paimon tiering job. If you want the
# lakehouse path enabled, start it separately with:
#   bash quickstart/scripts/run-tiering-job.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUICKSTART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_compose.sh
source "${SCRIPT_DIR}/_compose.sh"
cd "${QUICKSTART_DIR}"

INIT_SQL="sql/init-refund-context-tables.sql"
PIPE_SQL="sql/seed-or-pipeline.sql"
STAGE_DESC="create refund investigation tables -> seed deterministic refund orders, support cases, events, and lake-enabled audit history"

for f in "${INIT_SQL}" "${PIPE_SQL}"; do
  [[ -f "${f}" ]] || { echo "ERROR: missing ${f}" >&2; exit 3; }
done

REMOTE="/tmp/fluss-refund-quickstart.sql"
TMP="$(mktemp -t fluss-refund-quickstart.XXXXXX.sql)"
trap 'rm -f "${TMP}"' EXIT
cat "${INIT_SQL}" "${PIPE_SQL}" > "${TMP}"

echo ">> Submitting refund quickstart SQL to the Flink SQL Client..."
echo ">> Stages: ${STAGE_DESC}."
echo

compose cp "${TMP}" "jobmanager:${REMOTE}"

if compose exec -T jobmanager /opt/flink/bin/sql-client.sh -f "${REMOTE}"; then
  echo
  echo ">> SQL submission finished."
  echo ">> The refund quickstart data is ready for MCP queries."
  echo ">> Optional lakehouse step: bash quickstart/scripts/run-tiering-job.sh"
  echo ">> Next: follow quickstart/README.md to connect MCP and investigate ORD-20260625-1001."
else
  echo
  echo ">> Submission failed. Open an interactive client and paste the SQL manually:"
  echo "     bash quickstart/scripts/open-sql-client.sh"
  echo "   then paste ${INIT_SQL} and ${PIPE_SQL}"
  exit 1
fi
