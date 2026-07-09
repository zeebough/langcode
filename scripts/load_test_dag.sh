#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPORT_DIR="${LOAD_REPORT_DIR:-reports/load}"

mkdir -p "${ROOT_DIR}/${REPORT_DIR}"

export LOAD_SCENARIO="${LOAD_SCENARIO:-claim}"
export LOAD_CLAIM_MODE="${LOAD_CLAIM_MODE:-atomic}"
export LOAD_RUN_ID="${LOAD_RUN_ID:-load_run_$(date +%s)_$$}"
export LOAD_THREAD_ID="${LOAD_THREAD_ID:-${LOAD_RUN_ID}}"
export LOAD_POSTGRES_POOL_MIN_SIZE="${LOAD_POSTGRES_POOL_MIN_SIZE:-10}"
export LOAD_POSTGRES_POOL_TIMEOUT="${LOAD_POSTGRES_POOL_TIMEOUT:-30}"
export LOAD_SEED_TASKS="${LOAD_SEED_TASKS:-1000000}"
export LOAD_REFILL_TASKS="${LOAD_REFILL_TASKS:-true}"
export LOAD_REFILL_THRESHOLD="${LOAD_REFILL_THRESHOLD:-200000}"
export LOAD_REFILL_BATCH="${LOAD_REFILL_BATCH:-200000}"
export LOAD_REPORT_DIR="${REPORT_DIR}"

USERS="${USERS:-2000}"
SPAWN_RATE="${SPAWN_RATE:-100}"
RUN_TIME="${RUN_TIME:-10m}"
LOCUST_PROCESSES="${LOCUST_PROCESSES:-1}"
CSV_PREFIX="${CSV_PREFIX:-${REPORT_DIR}/dag_${LOAD_SCENARIO}}"
HTML_REPORT="${HTML_REPORT:-${REPORT_DIR}/dag_${LOAD_SCENARIO}.html}"

if [[ -z "${LOAD_POSTGRES_POOL_MAX_SIZE+x}" ]]; then
  if [[ "${LOCUST_PROCESSES}" =~ ^[0-9]+$ && "${LOCUST_PROCESSES}" -gt 1 ]]; then
    TOTAL_POOL_MAX="${LOAD_TOTAL_POSTGRES_POOL_MAX_SIZE:-120}"
    export LOAD_POSTGRES_POOL_MAX_SIZE="$(( (TOTAL_POOL_MAX + LOCUST_PROCESSES - 1) / LOCUST_PROCESSES ))"
  else
    export LOAD_POSTGRES_POOL_MAX_SIZE="80"
  fi
fi

LOCUST_ARGS=(
  -f tests/load/locustfile_dag.py
  --headless
  --users "${USERS}"
  --spawn-rate "${SPAWN_RATE}"
  --run-time "${RUN_TIME}"
  --csv "${CSV_PREFIX}"
  --html "${HTML_REPORT}"
)

if [[ "${LOCUST_PROCESSES}" != "1" ]]; then
  LOCUST_ARGS+=(--processes "${LOCUST_PROCESSES}")
fi

cd "${ROOT_DIR}"

locust "${LOCUST_ARGS[@]}"
