#!/usr/bin/env bash

# 获取脚本自身的绝对路径
SCRIPT_PATH="$(realpath "$0")"
SCRIPT_DIR="$(dirname "$SCRIPT_PATH")"

if [ $# -ne 1 ]; then
    echo "Usage: $0 numTrials"
    exit 1
fi

trap 'kill -INT -$pid; exit 1' INT

# Note: because the socketID is based on the current userID,
# ./test-mr.sh cannot be run in parallel
runs=$1
chmod +x ${SCRIPT_DIR}/test-mr.sh

for i in $(seq 1 $runs); do
    timeout -k 2s 900s ${SCRIPT_DIR}/test-mr.sh &
    pid=$!
    if ! wait $pid; then
        echo '***' FAILED TESTS IN TRIAL $i
        exit 1
    fi
done
echo '***' PASSED ALL $i TESTING TRIALS
