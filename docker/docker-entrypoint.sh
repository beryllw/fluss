#!/bin/bash
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

CONF_FILE="${FLUSS_HOME}/conf/server.yaml"
ZK_CONFIG_FILE="${FLUSS_HOME}/conf/zookeeper.properties"

prepare_configuration() {
    # backward compatability: allow to use old [coordinator|tablet-server].host option in FLUSS_PROPERTIES
    sed -i '/bind.listeners:/d' "${CONF_FILE}"
    if [ -n "${FLUSS_PROPERTIES}" ]; then
        echo "${FLUSS_PROPERTIES}" >> "${CONF_FILE}"
    fi
    envsubst < "${CONF_FILE}" > "${CONF_FILE}.tmp" && mv "${CONF_FILE}.tmp" "${CONF_FILE}"

    echo "4lw.commands.whitelist=*" >> "$ZK_CONFIG_FILE"
}

prepare_configuration

# Health Check
# TODO：When the Fluss CLI can display service status, we will use its commands.
local_cluster_health_check() {
    local ZK_HOST="${1:-127.0.0.1}"
    local ZK_PORT="${2:-2181}"
    local -a NODES=("${@:3}")

    if [ ${#NODES[@]} -eq 0 ]; then
        NODES=("/fluss/tabletservers/ids/0" "/fluss/coordinators/active")
    fi

    echo "Starting health check on ZK $ZK_HOST:$ZK_PORT for nodes: ${NODES[*]}"

    check_nodes_existence() {
        local zk_dump=$(echo "dump" | nc "$ZK_HOST" "$ZK_PORT" 2>/dev/null)
        local all_present=true

        for node_path in "${NODES[@]}"; do
            if ! echo "$zk_dump" | grep -q "$node_path"; then
                echo "Node $node_path does not exist"
                all_present=false
            fi
        done

        return $((all_present))
    }

    while sleep 30; do
        if check_nodes_existence; then
            echo "Fluss service is operational."
        else
            echo "Fluss service disruption detected. Exiting."
            exit 1
        fi
    done
}

args=("$@")

if [ "$1" = "help" ]; then
  printf "Usage: $(basename "$0") (coordinatorServer|tabletServer|localCluster)\n"
  printf "    Or $(basename "$0") help\n\n"
  exit 0
elif [ "$1" = "coordinatorServer" ]; then
  args=("${args[@]:1}")
  echo "Starting Coordinator Server"
  exec "$FLUSS_HOME/bin/coordinator-server.sh" start-foreground "${args[@]}"
elif [ "$1" = "tabletServer" ]; then
  args=("${args[@]:1}")
  echo "Starting Tablet Server"
  exec "$FLUSS_HOME/bin/tablet-server.sh" start-foreground "${args[@]}"
elif [ "$1" = "localCluster" ]; then
  args=("${args[@]:1}")
  $FLUSS_HOME/bin/local-cluster.sh start "${args[@]}"
  local_cluster_health_check
fi

args=("${args[@]}")

## Running command in pass-through mode
exec "${args[@]}"