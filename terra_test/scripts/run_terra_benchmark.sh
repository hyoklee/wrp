#!/bin/bash
#
# TERRA Fusion HDF5 I/O Benchmark Script
#
# This script measures IOWarp's I/O performance by reading TERRA HDF5 datasets.
#
# Usage:
#   ./run_terra_benchmark.sh [options]
#
# Options:
#   --nodes N       Number of nodes (default: 1)
#   --tasks N       Number of tasks/threads (default: 1)
#   --output DIR    Output directory for results
#

set -e

# Default configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEST_DIR="$(dirname "$SCRIPT_DIR")"
CORE_DIR="$(dirname "$TEST_DIR")"
BUILD_DIR="${CORE_DIR}/build"

NODES=1
TASKS=1
OUTPUT_DIR="${TEST_DIR}/results"
DATA_DIR="/mnt/common/hyoklee/data"
OMNI_FILE="${TEST_DIR}/configs/terra_omni_64datasets.yaml"
CONFIG_FILE="${TEST_DIR}/configs/terra_runtime_config.yaml"

TERRA_FILE="/mnt/common/datasets-staging/TERRA_BF_L1B_O10204_20011118010522_F000_V001.h5"

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --nodes)
            NODES="$2"
            shift 2
            ;;
        --tasks)
            TASKS="$2"
            shift 2
            ;;
        --output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Create output directories
mkdir -p "$OUTPUT_DIR"
mkdir -p "$DATA_DIR"

# Calculate total data size for the 64 datasets (approximate)
# - 8 Geolocation datasets: 8 × (11×11×8) = 7,744 bytes
# - 16 SWIR datasets: ~16 × 64MB = ~1,024 MB
# - 16 TIR datasets: ~16 × 7MB = ~112 MB
# - 24 VNIR datasets: ~16 × 256MB = ~4,096 MB
# Total: ~5.2 GB
TOTAL_DATA_SIZE_MB=5300

# Export configuration for the runtime
export CHI_SCHED_THREADS=$TASKS
export CHI_SERVER_CONF="$CONFIG_FILE"
export LD_LIBRARY_PATH="${BUILD_DIR}/bin:${LD_LIBRARY_PATH}"

# Get node IP for CSV filename
NODE_IP=$(hostname -I | awk '{print $1}' | tr '.' '_')
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

echo "=========================================="
echo "TERRA Fusion HDF5 I/O Benchmark"
echo "=========================================="
echo ""
echo "Configuration:"
echo "  Nodes: $NODES"
echo "  Tasks: $TASKS"
echo "  Total Data Size: ~${TOTAL_DATA_SIZE_MB} MB"
echo "  TERRA File: $TERRA_FILE"
echo "  OMNI File: $OMNI_FILE"
echo "  Output: $OUTPUT_DIR"
echo ""

# Check if TERRA file exists
if [[ ! -f "$TERRA_FILE" ]]; then
    echo "ERROR: TERRA file not found: $TERRA_FILE"
    exit 1
fi

# Check if build exists
if [[ ! -f "${BUILD_DIR}/bin/chimaera_start_runtime" ]]; then
    echo "ERROR: Build not found at ${BUILD_DIR}/bin/"
    echo "Please build the project first."
    exit 1
fi

# Create a temporary runtime config with actual values
RUNTIME_CONFIG_TEMP="${OUTPUT_DIR}/runtime_config_${NODES}n_${TASKS}t_${TIMESTAMP}.yaml"
sed -e "s/\${CHI_SCHED_THREADS:-8}/${TASKS}/g" \
    -e "s/\${CHI_PORT:-5555}/5555/g" \
    -e "s/\${CHI_NEIGHBORHOOD:-32}/${NODES}/g" \
    -e "s/\${CHI_NEIGHBORHOOD:-1}/${NODES}/g" \
    "$CONFIG_FILE" > "$RUNTIME_CONFIG_TEMP"

export CHI_SERVER_CONF="$RUNTIME_CONFIG_TEMP"

# Results file
RESULT_FILE="${OUTPUT_DIR}/terra_${NODES}n_${TASKS}t_${TIMESTAMP}.log"

echo "Step 1: Starting Chimaera runtime..."
echo "  Config: $RUNTIME_CONFIG_TEMP"

# Start runtime in background
${BUILD_DIR}/bin/chimaera_start_runtime &
RUNTIME_PID=$!

# Wait for runtime to initialize
sleep 5

# Check if runtime is still running
if ! kill -0 $RUNTIME_PID 2>/dev/null; then
    echo "ERROR: Runtime failed to start"
    exit 1
fi

echo "  Runtime started (PID: $RUNTIME_PID)"

echo ""
echo "Step 2: Running HDF5 assimilation benchmark..."

# Record start time
START_TIME=$(date +%s.%N)

# Run the OMNI processor
${BUILD_DIR}/bin/wrp_cae_omni "$OMNI_FILE" 2>&1 | tee "${OUTPUT_DIR}/omni_output_${NODES}n_${TASKS}t_${TIMESTAMP}.log"
OMNI_EXIT=$?

# Record end time
END_TIME=$(date +%s.%N)

# Calculate elapsed time
ELAPSED_TIME=$(echo "$END_TIME - $START_TIME" | bc)

echo ""
echo "Step 3: Calculating results..."

# Calculate bandwidth
BANDWIDTH_MBps=$(echo "scale=2; $TOTAL_DATA_SIZE_MB / $ELAPSED_TIME" | bc)

# Get peak memory usage if available
if command -v free &> /dev/null; then
    MEMORY_USED=$(free -m | awk '/Mem:/ {print $3}')
else
    MEMORY_USED="N/A"
fi

# Write results
{
    echo "=========================================="
    echo "TERRA Fusion I/O Benchmark Results"
    echo "=========================================="
    echo ""
    echo "Configuration:"
    echo "  Date: $(date)"
    echo "  Node IP: ${NODE_IP//_/.}"
    echo "  Nodes: $NODES"
    echo "  Tasks: $TASKS"
    echo "  Total Tasks: $((NODES * TASKS))"
    echo ""
    echo "Test Results:"
    echo "  OMNI Exit Code: $OMNI_EXIT"
    echo "  Elapsed Time: ${ELAPSED_TIME} seconds"
    echo "  Data Size: ${TOTAL_DATA_SIZE_MB} MB"
    echo "  Bandwidth: ${BANDWIDTH_MBps} MB/s"
    echo "  Memory Used: ${MEMORY_USED} MB"
    echo ""
} | tee "$RESULT_FILE"

# Write CSV summary
CSV_SUMMARY="${OUTPUT_DIR}/terra_benchmark_summary.csv"
if [[ ! -f "$CSV_SUMMARY" ]]; then
    echo "timestamp,nodes,tasks,total_tasks,elapsed_time_s,data_size_mb,bandwidth_mbps,omni_exit_code" > "$CSV_SUMMARY"
fi
echo "${TIMESTAMP},${NODES},${TASKS},$((NODES * TASKS)),${ELAPSED_TIME},${TOTAL_DATA_SIZE_MB},${BANDWIDTH_MBps},${OMNI_EXIT}" >> "$CSV_SUMMARY"

echo "Step 4: Stopping runtime..."

# Stop the runtime
kill $RUNTIME_PID 2>/dev/null || true
wait $RUNTIME_PID 2>/dev/null || true

echo "  Runtime stopped"

# Cleanup temporary config
rm -f "$RUNTIME_CONFIG_TEMP"

echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="
echo ""
echo "Results saved to: $RESULT_FILE"
echo "Summary appended to: $CSV_SUMMARY"
echo ""
echo "Key Metrics:"
echo "  - Time: ${ELAPSED_TIME} seconds"
echo "  - Bandwidth: ${BANDWIDTH_MBps} MB/s"
echo ""

# Exit with OMNI exit code
exit $OMNI_EXIT
