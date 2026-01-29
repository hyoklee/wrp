#!/bin/bash
#
# Submit All TERRA Benchmark Tests via SLURM
#
# This script submits all 16 test configurations (1,2,4,8 nodes × 1,2,4,8 tasks)
#

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(dirname "$SCRIPT_DIR")"

echo "=========================================="
echo "Submitting TERRA Benchmark Tests"
echo "=========================================="
echo ""

# Array of configurations: nodes tasks
CONFIGS=(
    "1 1"
    "1 2"
    "1 4"
    "1 8"
    "2 1"
    "2 2"
    "2 4"
    "2 8"
    "4 1"
    "4 2"
    "4 4"
    "4 8"
    "8 1"
    "8 2"
    "8 4"
    "8 8"
)

# Create results directory
mkdir -p "${TEST_DIR}/results"

JOB_IDS=""

for CONFIG in "${CONFIGS[@]}"; do
    read -r NODES TASKS <<< "$CONFIG"
    JOB_NAME="terra_${NODES}n_${TASKS}t"
    SCRIPT="${SCRIPT_DIR}/slurm_terra_${NODES}n_${TASKS}t.sh"

    if [[ -f "$SCRIPT" ]]; then
        echo "Submitting: $JOB_NAME"
        JOB_ID=$(sbatch "$SCRIPT" | awk '{print $4}')
        JOB_IDS="$JOB_IDS $JOB_ID"
        echo "  Job ID: $JOB_ID"
    else
        echo "ERROR: Script not found: $SCRIPT"
    fi
done

echo ""
echo "=========================================="
echo "All Jobs Submitted"
echo "=========================================="
echo ""
echo "Job IDs:$JOB_IDS"
echo ""
echo "Monitor with: squeue -u $(whoami)"
echo "View results in: ${TEST_DIR}/results/"
