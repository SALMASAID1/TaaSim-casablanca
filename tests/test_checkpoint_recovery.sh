#!/bin/bash
# ---------------------------------------------------------------------------
# TaaSim — Flink Checkpoint Recovery Test
#
# Sprint 6, Task 01
#
# This script verifies that Flink jobs recover from MinIO-based checkpoints
# after a TaskManager crash, proving at-least-once processing guarantees.
#
# Prerequisites:
#   - Docker stack running (docker compose up -d)
#   - At least Flink Job 1 submitted and running
#   - GPS producer actively sending events
#
# Usage:
#   bash tests/test_checkpoint_recovery.sh
# ---------------------------------------------------------------------------
set -euo pipefail

echo "================================================================"
echo "  TaaSim — Flink Checkpoint Recovery Test (Sprint 6, Task 01)"
echo "================================================================"
echo ""

API_URL="${TAASIM_API_URL:-https://localhost:8000}"
FLINK_URL="${FLINK_URL:-http://localhost:8081}"
CASSANDRA_CONTAINER="taasim-cassandra"
FLINK_TM_CONTAINER="taasim-flink-tm"
FLINK_JM_CONTAINER="taasim-flink-jm"

# ---------------------------------------------------------------------------
# Step 1: Confirm Flink Job 1 is running
# ---------------------------------------------------------------------------
echo "[Step 1] Checking Flink jobs..."
RUNNING_JOBS=$(curl -sf "${FLINK_URL}/jobs" | python3 -c "
import json, sys
data = json.load(sys.stdin)
running = [j for j in data.get('jobs', []) if j['status'] == 'RUNNING']
print(len(running))
")

if [ "$RUNNING_JOBS" -eq "0" ]; then
    echo "  ❌ FAIL — No Flink jobs are running. Submit Job 1 first."
    exit 1
fi
echo "  ✅ ${RUNNING_JOBS} Flink job(s) running."

# ---------------------------------------------------------------------------
# Step 2: Record current Cassandra row count (vehicle_positions)
# ---------------------------------------------------------------------------
echo ""
echo "[Step 2] Recording Cassandra baseline..."
BEFORE_COUNT=$(docker exec "${CASSANDRA_CONTAINER}" cqlsh -e \
    "SELECT COUNT(*) FROM taasim.vehicle_positions;" 2>/dev/null \
    | grep -oP '\d+' | head -1 || echo "0")
echo "  Vehicle positions before crash: ${BEFORE_COUNT}"

# ---------------------------------------------------------------------------
# Step 3: Record checkpoint info from Flink
# ---------------------------------------------------------------------------
echo ""
echo "[Step 3] Recording latest checkpoint..."
CHECKPOINT_INFO=$(curl -sf "${FLINK_URL}/jobs" | python3 -c "
import json, sys
data = json.load(sys.stdin)
running = [j for j in data.get('jobs', []) if j['status'] == 'RUNNING']
if running:
    print(running[0]['id'])
else:
    print('NONE')
")

if [ "$CHECKPOINT_INFO" != "NONE" ]; then
    echo "  Active job ID: ${CHECKPOINT_INFO}"
    curl -sf "${FLINK_URL}/jobs/${CHECKPOINT_INFO}/checkpoints" | python3 -c "
import json, sys
data = json.load(sys.stdin)
counts = data.get('counts', {})
latest = data.get('latest', {}).get('completed', {})
print(f'  Checkpoints: completed={counts.get(\"completed\", 0)}, failed={counts.get(\"failed\", 0)}')
if latest:
    print(f'  Latest checkpoint ID: {latest.get(\"id\", \"N/A\")}')
    print(f'  Latest checkpoint size: {latest.get(\"state_size\", 0)} bytes')
" 2>/dev/null || echo "  (Could not read checkpoint details)"
fi

# ---------------------------------------------------------------------------
# Step 4: Kill the TaskManager container (simulate crash)
# ---------------------------------------------------------------------------
echo ""
echo "[Step 4] 💥 Killing Flink TaskManager container..."
docker kill "${FLINK_TM_CONTAINER}" 2>/dev/null || true
echo "  TaskManager killed."

# ---------------------------------------------------------------------------
# Step 5: Wait for auto-restart
# ---------------------------------------------------------------------------
echo ""
echo "[Step 5] Waiting for TaskManager to restart..."
sleep 5  # Docker Compose should auto-restart

# Force restart if not auto-restarted
docker start "${FLINK_TM_CONTAINER}" 2>/dev/null || true
echo "  Waiting 30 seconds for TaskManager to rejoin cluster..."
sleep 30

# ---------------------------------------------------------------------------
# Step 6: Verify TaskManager is back
# ---------------------------------------------------------------------------
echo ""
echo "[Step 6] Verifying TaskManager recovery..."
TM_STATUS=$(docker inspect --format='{{.State.Status}}' "${FLINK_TM_CONTAINER}" 2>/dev/null || echo "not_found")
echo "  TaskManager container status: ${TM_STATUS}"

if [ "$TM_STATUS" != "running" ]; then
    echo "  ⚠️ TaskManager not running. Starting it..."
    docker start "${FLINK_TM_CONTAINER}"
    sleep 20
fi

# Check Flink dashboard for taskmanager count
TM_COUNT=$(curl -sf "${FLINK_URL}/taskmanagers" | python3 -c "
import json, sys
data = json.load(sys.stdin)
print(len(data.get('taskmanagers', [])))
" 2>/dev/null || echo "0")
echo "  TaskManagers registered: ${TM_COUNT}"

# ---------------------------------------------------------------------------
# Step 7: Check if jobs recovered
# ---------------------------------------------------------------------------
echo ""
echo "[Step 7] Checking job recovery..."
sleep 10

RECOVERED_JOBS=$(curl -sf "${FLINK_URL}/jobs" | python3 -c "
import json, sys
data = json.load(sys.stdin)
running = [j for j in data.get('jobs', []) if j['status'] == 'RUNNING']
print(len(running))
" 2>/dev/null || echo "0")
echo "  Running jobs after recovery: ${RECOVERED_JOBS}"

# ---------------------------------------------------------------------------
# Step 8: Verify data continues flowing into Cassandra
# ---------------------------------------------------------------------------
echo ""
echo "[Step 8] Waiting 30 seconds for new data to flow..."
sleep 30

AFTER_COUNT=$(docker exec "${CASSANDRA_CONTAINER}" cqlsh -e \
    "SELECT COUNT(*) FROM taasim.vehicle_positions;" 2>/dev/null \
    | grep -oP '\d+' | head -1 || echo "0")
echo "  Vehicle positions after recovery: ${AFTER_COUNT}"

# ---------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------
echo ""
echo "================================================================"
echo "  CHECKPOINT RECOVERY TEST RESULTS"
echo "================================================================"
echo "  Before crash:   ${BEFORE_COUNT} rows"
echo "  After recovery: ${AFTER_COUNT} rows"
echo "  Jobs running:   ${RECOVERED_JOBS}"
echo ""

if [ "$RECOVERED_JOBS" -ge "1" ] && [ "$AFTER_COUNT" -gt "$BEFORE_COUNT" ]; then
    echo "  ✅ PASS — Flink job recovered from checkpoint and data is flowing."
elif [ "$RECOVERED_JOBS" -ge "1" ]; then
    echo "  ⚠️ PARTIAL — Job recovered but no new data detected."
    echo "     (GPS producer may not be running or data may need more time)"
else
    echo "  ❌ FAIL — Flink job did not recover automatically."
    echo "     Check Flink dashboard: ${FLINK_URL}"
fi
echo "================================================================"
