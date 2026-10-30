#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/../.." && pwd)"
CP_CACHE_FILE="${REPO_ROOT}/test/target/e2e-longrunning-cp.txt"

ACTION="${1:-help}"
if [ $# -gt 0 ]; then
  shift
fi

PROFILE="default"
WORK_DIR="/tmp/rmq-e2e-longrunning"
BROKERS=2
BROKER_ID=0
RESTART_MODE="graceful"
DURATION_SEC=25
DRAIN_TIMEOUT_SEC=30
CHAOS_INTERVAL_SEC=10
CHAOS_MODE="graceful"
SEND_INTERVAL_MS=25
ENABLE_E2E_SUITE="true"
NAMESRV_PORT=19876
BASE_BROKER_PORT=20911

while [ $# -gt 0 ]; do
  case "$1" in
    --profile)
      PROFILE="$2"
      shift 2
      ;;
    --work-dir)
      WORK_DIR="$2"
      shift 2
      ;;
    --brokers)
      BROKERS="$2"
      shift 2
      ;;
    --broker-id)
      BROKER_ID="$2"
      shift 2
      ;;
    --mode)
      RESTART_MODE="$2"
      shift 2
      ;;
    --duration)
      DURATION_SEC="$2"
      shift 2
      ;;
    --drain-timeout)
      DRAIN_TIMEOUT_SEC="$2"
      shift 2
      ;;
    --chaos-interval)
      CHAOS_INTERVAL_SEC="$2"
      shift 2
      ;;
    --chaos-mode)
      CHAOS_MODE="$2"
      shift 2
      ;;
    --send-interval-ms)
      SEND_INTERVAL_MS="$2"
      shift 2
      ;;
    --enable-e2e-suite)
      ENABLE_E2E_SUITE="$2"
      shift 2
      ;;
    --enable-e2e-suite)
      ENABLE_E2E_SUITE="$2"
      shift 2
      ;;
    --namesrv-port)
      NAMESRV_PORT="$2"
      shift 2
      ;;
    --base-broker-port)
      BASE_BROKER_PORT="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      exit 1
      ;;
  esac
done

JAVA_BIN="${JAVA_HOME:+${JAVA_HOME}/bin/}java"
JVM_OPTS=(
  "-Xms512m"
  "-Xmx1024m"
  "--add-opens=java.base/java.nio=ALL-UNNAMED"
  "--add-opens=java.base/sun.nio.ch=ALL-UNNAMED"
  "--add-opens=java.base/java.lang=ALL-UNNAMED"
  "--add-opens=java.base/java.util=ALL-UNNAMED"
)

ensure_classpath() {
  if [ -f "${CP_CACHE_FILE}" ] && [ -s "${CP_CACHE_FILE}" ]; then
    return 0
  fi
  echo "[e2e-longrunning] Building runtime classpath..."
  mkdir -p "$(dirname "${CP_CACHE_FILE}")"
  local dep_cp_file="${REPO_ROOT}/test/target/dep-cp.txt"
  (
    cd "${REPO_ROOT}"
    mvn test-compile dependency:build-classpath \
      -pl common,remoting,client,store,tieredstore,srvutil,filter,rocketmq-proto,auth,broker,namesrv,controller,proxy,tools,container,test \
      -Dmdep.outputFile="${dep_cp_file}" -Dmdep.regenerateFile=true \
      -Djacoco.skip=true -Dspotbugs.skip=true -Dcheckstyle.skip=true -q
  )
  local full_cp="${REPO_ROOT}/test/target/classes"
  for mod in common remoting client store tieredstore srvutil filter rocketmq-proto auth broker namesrv controller proxy tools container; do
    if [ -d "${REPO_ROOT}/${mod}/target/classes" ]; then
      full_cp="${full_cp}:${REPO_ROOT}/${mod}/target/classes"
    fi
  done
  full_cp="${full_cp}:$(cat "${dep_cp_file}")"
  echo "${full_cp}" > "${CP_CACHE_FILE}"
}

generate_configs() {
  local target_profile="$1"
  mkdir -p "${WORK_DIR}/conf" "${WORK_DIR}/logs" "${WORK_DIR}/pids" "${WORK_DIR}/store"

  cat > "${WORK_DIR}/conf/namesrv.conf" <<EOF
rocketmqHome=${WORK_DIR}
kvConfigPath=${WORK_DIR}/namesrv/kvConfig.json
configStorePath=${WORK_DIR}/namesrv/namesrv.properties
listenPort=${NAMESRV_PORT}
EOF

  cat > "${WORK_DIR}/conf/logback-driver.xml" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<configuration>
    <appender name="CONSOLE" class="org.apache.rocketmq.logging.ch.qos.logback.core.ConsoleAppender">
        <encoder class="org.apache.rocketmq.logging.ch.qos.logback.classic.encoder.PatternLayoutEncoder">
            <pattern>%d{HH:mm:ss.SSS} [%thread] %-5level %logger{36} - %msg%n</pattern>
        </encoder>
    </appender>
    <logger name="org.apache.rocketmq" level="WARN" additivity="false">
        <appender-ref ref="CONSOLE"/>
    </logger>
    <logger name="org.apache.rocketmq.test.longrunning" level="INFO" additivity="false">
        <appender-ref ref="CONSOLE"/>
    </logger>
    <root level="WARN">
        <appender-ref ref="CONSOLE"/>
    </root>
</configuration>
EOF

  for ((i=0; i<BROKERS; i++)); do
    local listen_port=$((BASE_BROKER_PORT + i * 20))
    local ha_port=$((listen_port + 1))
    local store_dir="${WORK_DIR}/store/broker-${i}"
    mkdir -p "${store_dir}"

    local store_type="default"
    local config_v="v1"
    local timer_rocks="false"
    local trans_rocks="false"
    local index_rocks="false"
    if [ "${target_profile}" = "rocksdb" ]; then
      store_type="defaultRocksDB"
      config_v="v2"
      timer_rocks="true"
      trans_rocks="true"
      index_rocks="true"
    fi

    cat > "${WORK_DIR}/conf/broker-${i}.conf" <<EOF
rocketmqHome=${WORK_DIR}
brokerClusterName=DefaultCluster
brokerName=BrokerNode${i}
brokerId=0
brokerRole=ASYNC_MASTER
flushDiskType=ASYNC_FLUSH
brokerIP1=127.0.0.1
namesrvAddr=127.0.0.1:${NAMESRV_PORT}
listenPort=${listen_port}
haListenPort=${ha_port}
storePathRootDir=${store_dir}
storePathCommitLog=${store_dir}/commitlog
mappedFileSizeCommitLog=104857600
mappedFileSizeConsumeQueue=6000000
autoCreateTopicEnable=true
autoCreateSubscriptionGroup=true
enablePropertyFilter=true
enableCalcFilterBitMap=true
popConsumerKVServiceInit=true
timerWheelEnable=true
timerMaxDelaySec=3600
timerPrecisionMs=200
transactionCheckInterval=2000
transactionTimeOut=2000
transactionCheckMax=15
storeType=${store_type}
configManagerVersion=${config_v}
timerRocksDBEnable=${timer_rocks}
transRocksDBEnable=${trans_rocks}
indexRocksDBEnable=${index_rocks}
EOF
  done
}

start_namesrv() {
  ensure_classpath
  local cp
  cp="$(cat "${CP_CACHE_FILE}")"
  export ROCKETMQ_HOME="${WORK_DIR}"
  mkdir -p "${WORK_DIR}/namesrv"
  nohup "${JAVA_BIN}" "${JVM_OPTS[@]}" \
    -Drocketmq.home.dir="${WORK_DIR}" \
    -Drocketmq.client.logRoot="${WORK_DIR}/logs" \
    -cp "${cp}" \
    org.apache.rocketmq.namesrv.NamesrvStartup \
    -c "${WORK_DIR}/conf/namesrv.conf" \
    > "${WORK_DIR}/logs/namesrv.stdout.log" 2>&1 &
  local pid=$!
  echo "${pid}" > "${WORK_DIR}/pids/namesrv.pid"
  echo "[e2e-longrunning] Started NameServer (PID=${pid}, port=${NAMESRV_PORT})"
}

start_broker() {
  local idx="$1"
  ensure_classpath
  local cp
  cp="$(cat "${CP_CACHE_FILE}")"
  export ROCKETMQ_HOME="${WORK_DIR}"
  local conf_file="${WORK_DIR}/conf/broker-${idx}.conf"
  nohup "${JAVA_BIN}" "${JVM_OPTS[@]}" \
    -Drocketmq.home.dir="${WORK_DIR}" \
    -Drocketmq.client.logRoot="${WORK_DIR}/logs" \
    -cp "${cp}" \
    org.apache.rocketmq.broker.BrokerStartup \
    -c "${conf_file}" \
    >> "${WORK_DIR}/logs/broker-${idx}.stdout.log" 2>&1 &
  local pid=$!
  echo "${pid}" > "${WORK_DIR}/pids/broker-${idx}.pid"
  echo "[e2e-longrunning] Started broker-${idx} (PID=${pid}, conf=${conf_file})"
}

stop_pid_file() {
  local pid_file="$1"
  local mode="${2:-graceful}"
  if [ ! -f "${pid_file}" ]; then
    return 0
  fi
  local pid
  pid="$(cat "${pid_file}")"
  if [ -n "${pid}" ] && kill -0 "${pid}" 2>/dev/null; then
    if [ "${mode}" = "kill" ]; then
      kill -9 "${pid}" 2>/dev/null || true
    else
      kill -15 "${pid}" 2>/dev/null || true
      for _ in {1..20}; do
        if ! kill -0 "${pid}" 2>/dev/null; then
          break
        fi
        sleep 0.5
      done
      if kill -0 "${pid}" 2>/dev/null; then
        kill -9 "${pid}" 2>/dev/null || true
      fi
    fi
  fi
  rm -f "${pid_file}"
}

stop_cluster() {
  if [ -d "${WORK_DIR}/pids" ]; then
    for f in "${WORK_DIR}/pids"/broker-*.pid; do
      [ -e "$f" ] || continue
      stop_pid_file "$f" "graceful"
    done
    stop_pid_file "${WORK_DIR}/pids/namesrv.pid" "graceful"
  fi
  echo "[e2e-longrunning] Stopped cluster in ${WORK_DIR}"
}

start_cluster() {
  stop_cluster
  rm -rf "${WORK_DIR}/store" "${WORK_DIR}/namesrv" "${WORK_DIR}/logs" "${WORK_DIR}/pids"
  generate_configs "${PROFILE}"
  start_namesrv
  sleep 2
  for ((i=0; i<BROKERS; i++)); do
    start_broker "${i}"
  done
  sleep 3
  status_cluster
}

restart_broker() {
  local pid_file="${WORK_DIR}/pids/broker-${BROKER_ID}.pid"
  echo "[e2e-longrunning] Restarting broker-${BROKER_ID} (mode=${RESTART_MODE})..."
  stop_pid_file "${pid_file}" "${RESTART_MODE}"
  sleep 1
  start_broker "${BROKER_ID}"
}

status_cluster() {
  echo "=== Cluster Status (${WORK_DIR}) ==="
  if [ -f "${WORK_DIR}/pids/namesrv.pid" ]; then
    local nspid
    nspid="$(cat "${WORK_DIR}/pids/namesrv.pid")"
    if kill -0 "${nspid}" 2>/dev/null; then
      echo "  NameServer: RUNNING (PID=${nspid})"
    else
      echo "  NameServer: DEAD (stale PID=${nspid})"
    fi
  else
    echo "  NameServer: STOPPED"
  fi
  for f in "${WORK_DIR}/pids"/broker-*.pid; do
    [ -e "$f" ] || continue
    local bname
    bname="$(basename "$f" .pid)"
    local bpid
    bpid="$(cat "$f")"
    if kill -0 "${bpid}" 2>/dev/null; then
      echo "  ${bname}: RUNNING (PID=${bpid})"
    else
      echo "  ${bname}: DEAD (stale PID=${bpid})"
    fi
  done
}

run_single_profile() {
  local target_profile="$1"
  local profile_work_dir="${WORK_DIR}/${target_profile}"
  WORK_DIR="${profile_work_dir}"
  PROFILE="${target_profile}"

  echo "================================================================================"
  echo "[e2e-longrunning] Starting E2E LongRunning Run [profile=${target_profile}, duration=${DURATION_SEC}s]"
  echo "================================================================================"
  start_cluster

  local cp
  cp="$(cat "${CP_CACHE_FILE}")"
  local driver_exit=0
  "${JAVA_BIN}" "${JVM_OPTS[@]}" \
    -Drocketmq.client.logRoot="${WORK_DIR}/logs" \
    -Drmq.logback.configurationFile="${WORK_DIR}/conf/logback-driver.xml" \
    -cp "${cp}" \
    org.apache.rocketmq.test.longrunning.LongRunningTestDriver \
    --namesrvAddr "127.0.0.1:${NAMESRV_PORT}" \
    --clusterName "DefaultCluster" \
    --profile "${target_profile}" \
    --workDir "${WORK_DIR}" \
    --clusterScript "${SCRIPT_DIR}/e2e-longrunning.sh" \
    --durationSeconds "${DURATION_SEC}" \
    --drainTimeoutSeconds "${DRAIN_TIMEOUT_SEC}" \
    --chaosIntervalSeconds "${CHAOS_INTERVAL_SEC}" \
    --chaosMode "${CHAOS_MODE}" \
    --sendIntervalMs "${SEND_INTERVAL_MS}" \
    --enableE2ESuite "${ENABLE_E2E_SUITE}" || driver_exit=$?

  stop_cluster
  return "${driver_exit}"
}

case "${ACTION}" in
  start-cluster)
    start_cluster
    ;;
  stop-cluster)
    stop_cluster
    ;;
  restart-broker)
    restart_broker
    ;;
  status)
    status_cluster
    ;;
  run-suite)
    DURATION_SEC=0
    CHAOS_INTERVAL_SEC=0
    ENABLE_E2E_SUITE="true"
    base_work_dir="${WORK_DIR}"
    if [ "${PROFILE}" = "matrix" ]; then
      run_single_profile "default"
      WORK_DIR="${base_work_dir}"
      NAMESRV_PORT=$((NAMESRV_PORT + 100))
      BASE_BROKER_PORT=$((BASE_BROKER_PORT + 200))
      run_single_profile "rocksdb"
    else
      run_single_profile "${PROFILE}"
    fi
    ;;
  run-suite)
    DURATION_SEC=0
    CHAOS_INTERVAL_SEC=0
    ENABLE_E2E_SUITE="true"
    base_work_dir="${WORK_DIR}"
    if [ "${PROFILE}" = "matrix" ]; then
      run_single_profile "default"
      WORK_DIR="${base_work_dir}"
      NAMESRV_PORT=$((NAMESRV_PORT + 100))
      BASE_BROKER_PORT=$((BASE_BROKER_PORT + 200))
      run_single_profile "rocksdb"
    else
      run_single_profile "${PROFILE}"
    fi
    ;;
  run)
    base_work_dir="${WORK_DIR}"
    if [ "${PROFILE}" = "matrix" ]; then
      run_single_profile "default"
      WORK_DIR="${base_work_dir}"
      NAMESRV_PORT=$((NAMESRV_PORT + 100))
      BASE_BROKER_PORT=$((BASE_BROKER_PORT + 200))
      run_single_profile "rocksdb"
    else
      run_single_profile "${PROFILE}"
    fi
    ;;
  *)
    cat <<EOF
Usage: $0 <command> [options]

Commands:
  start-cluster    Start independent NameServer + Broker OS processes
  stop-cluster     Stop all cluster OS processes in --work-dir
  restart-broker   Restart a specific broker process (--broker-id N --mode graceful|kill)
  status           Show running status of NameServer and Broker processes
  run              Run E2E Scenario Suite (32 cases) + LongRunning verification (--profile default|rocksdb|matrix)
  run-suite        Run only the 32 E2E Scenario Suite cases (--profile default|rocksdb|matrix)

Options:
  --profile <default|rocksdb|matrix>   Storage engine profile (default: default)
  --work-dir <path>                    Working directory for configs, logs, store, reports
  --brokers <N>                        Number of broker processes to launch (default: 2)
  --duration <sec>                     Workload execution duration in seconds (default: 25)
  --drain-timeout <sec>                Max drain wait timeout in seconds (default: 30)
  --chaos-interval <sec>               Seconds between broker chaos restarts, 0 to disable (default: 10)
  --chaos-mode <graceful|kill|mixed>   Fault injection mode (default: graceful)
  --send-interval-ms <ms>              Producer interval in milliseconds (default: 25)
EOF
    ;;
esac
