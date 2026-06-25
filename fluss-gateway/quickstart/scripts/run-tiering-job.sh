#!/usr/bin/env bash
#
# run-tiering-job.sh — start the Fluss lakehouse tiering service (Paimon).
#
# Run this AFTER the refund quickstart data has been loaded. It submits a Flink
# job that tiers lake-enabled Fluss tables into the Paimon warehouse on RustFS.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
QUICKSTART_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
# shellcheck source=_compose.sh
source "${SCRIPT_DIR}/_compose.sh"
cd "${QUICKSTART_DIR}"

# shellcheck disable=SC1091
source ./.env

echo ">> Submitting Fluss lakehouse tiering job (Paimon)..."
compose exec -T jobmanager /bin/sh -lc '
JAR=$(ls /opt/flink/opt/fluss-flink-tiering-*.jar | head -1)
exec /opt/flink/bin/flink run "$JAR" \
  --fluss.bootstrap.servers coordinator-server:9123 \
  --datalake.format paimon \
  --datalake.paimon.metastore filesystem \
  --datalake.paimon.warehouse s3://fluss/paimon \
  --datalake.paimon.s3.endpoint http://rustfs:9000 \
  --datalake.paimon.s3.access.key rustfsadmin \
  --datalake.paimon.s3.secret.key rustfsadmin \
  --datalake.paimon.s3.path.style.access true
'

echo
cat <<'EOF'
>> Tiering job submitted.
>> Check Flink UI at http://localhost:8083/ and wait ~30s before querying:
>>   - fluss.refund_demo.refund_audit_history
>>   - fluss.refund_demo.refund_audit_history$lake
>>   - fluss.refund_demo.refund_audit_history$lake$snapshots
EOF
