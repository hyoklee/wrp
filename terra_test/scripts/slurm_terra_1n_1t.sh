#!/bin/bash
#SBATCH --job-name=terra_1n_1t
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=1
#SBATCH --time=04:00:00
#SBATCH --output=/mnt/common/hyoklee/core.iowarp/terra_test/logs/terra_1n_1t_%j.out
#SBATCH --error=/mnt/common/hyoklee/core.iowarp/terra_test/logs/terra_1n_1t_%j.err
#SBATCH --exclusive

set -e

# Configuration
SCRIPT_DIR="/mnt/common/hyoklee/core.iowarp/terra_test/scripts"
TEST_DIR="/mnt/common/hyoklee/core.iowarp/terra_test"
CORE_DIR="/mnt/common/hyoklee/core.iowarp"
BUILD_DIR="${CORE_DIR}/build"
NODES=1
TASKS=1

# Activate conda environment
source /home/hyoklee/mc3/etc/profile.d/conda.sh
conda activate iowarp-build

# Set library paths
export LD_LIBRARY_PATH="${BUILD_DIR}/bin:${LD_LIBRARY_PATH}"

# Create logs directory
mkdir -p "${TEST_DIR}/logs"

echo "=========================================="
echo "TERRA Benchmark: ${NODES} nodes, ${TASKS} tasks"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Nodes: $(scontrol show hostnames ${SLURM_JOB_NODELIST})"
echo "=========================================="

# Generate hostfile for distributed run
HOSTFILE="${TEST_DIR}/configs/hostfile_${SLURM_JOB_ID}"
scontrol show hostnames "${SLURM_JOB_NODELIST}" > "${HOSTFILE}"

# Create temporary runtime config with actual values
CONFIG_FILE="${TEST_DIR}/configs/terra_runtime_config.yaml"
RUNTIME_CONFIG_TEMP="${TEST_DIR}/configs/runtime_${SLURM_JOB_ID}.yaml"

# Substitute environment variables in config
sed -e "s/\${CHI_SCHED_THREADS:-8}/${TASKS}/g" \
    -e "s/\${CHI_PORT:-5555}/5555/g" \
    -e "s/\${CHI_NEIGHBORHOOD:-32}/${NODES}/g" \
    -e "s/\${CHI_NEIGHBORHOOD:-1}/${NODES}/g" \
    "${CONFIG_FILE}" > "${RUNTIME_CONFIG_TEMP}"

# Add hostfile to config
echo "" >> "${RUNTIME_CONFIG_TEMP}"
echo "# Distributed configuration" >> "${RUNTIME_CONFIG_TEMP}"
echo "distributed:" >> "${RUNTIME_CONFIG_TEMP}"
echo "  hostfile: ${HOSTFILE}" >> "${RUNTIME_CONFIG_TEMP}"

export CHI_SERVER_CONF="${RUNTIME_CONFIG_TEMP}"

# Results file
RESULT_FILE="${TEST_DIR}/results/terra_${NODES}n_${TASKS}t_${SLURM_JOB_ID}.log"
OMNI_FILE="${TEST_DIR}/configs/terra_omni_64datasets.yaml"

# Data size estimate (MB)
TOTAL_DATA_SIZE_MB=5300

echo ""
echo "Step 1: Starting Chimaera runtime on all nodes..."

# Start runtime on all nodes
if [[ ${NODES} -gt 1 ]]; then
    srun --nodes=${NODES} --ntasks=${NODES} \
        "${BUILD_DIR}/bin/chimaera_start_runtime" &
else
    "${BUILD_DIR}/bin/chimaera_start_runtime" &
fi

RUNTIME_PID=$!
sleep 10  # Wait for distributed runtime initialization

echo "  Runtime started (PID: ${RUNTIME_PID})"

echo ""
echo "Step 2: Running HDF5 assimilation benchmark..."

# Record start time
START_TIME=$(date +%s.%N)

# Run the OMNI processor
"${BUILD_DIR}/bin/wrp_cae_omni" "${OMNI_FILE}" 2>&1 | tee "${TEST_DIR}/logs/omni_${NODES}n_${TASKS}t_${SLURM_JOB_ID}.log"
OMNI_EXIT=${PIPESTATUS[0]}

# Record end time
END_TIME=$(date +%s.%N)

# Calculate elapsed time
ELAPSED_TIME=$(echo "${END_TIME} - ${START_TIME}" | bc)

echo ""
echo "Step 3: Calculating results..."

# Calculate bandwidth
BANDWIDTH_MBps=$(echo "scale=2; ${TOTAL_DATA_SIZE_MB} / ${ELAPSED_TIME}" | bc)

# Write results
{
    echo "=========================================="
    echo "TERRA Fusion I/O Benchmark Results"
    echo "=========================================="
    echo ""
    echo "Configuration:"
    echo "  Date: $(date)"
    echo "  Job ID: ${SLURM_JOB_ID}"
    echo "  Nodes: ${NODES}"
    echo "  Tasks: ${TASKS}"
    echo "  Total Tasks: $((NODES * TASKS))"
    echo ""
    echo "Test Results:"
    echo "  OMNI Exit Code: ${OMNI_EXIT}"
    echo "  Elapsed Time: ${ELAPSED_TIME} seconds"
    echo "  Data Size: ${TOTAL_DATA_SIZE_MB} MB"
    echo "  Bandwidth: ${BANDWIDTH_MBps} MB/s"
    echo ""
} | tee "${RESULT_FILE}"

# Write CSV summary
CSV_SUMMARY="${TEST_DIR}/results/terra_benchmark_summary.csv"
if [[ ! -f "${CSV_SUMMARY}" ]]; then
    echo "job_id,timestamp,nodes,tasks,total_tasks,elapsed_time_s,data_size_mb,bandwidth_mbps,omni_exit_code" > "${CSV_SUMMARY}"
fi
echo "${SLURM_JOB_ID},$(date +%Y%m%d_%H%M%S),${NODES},${TASKS},$((NODES * TASKS)),${ELAPSED_TIME},${TOTAL_DATA_SIZE_MB},${BANDWIDTH_MBps},${OMNI_EXIT}" >> "${CSV_SUMMARY}"

echo "Step 4: Stopping runtime..."

# Stop the runtime
kill ${RUNTIME_PID} 2>/dev/null || true
wait ${RUNTIME_PID} 2>/dev/null || true

# Cleanup
rm -f "${RUNTIME_CONFIG_TEMP}"
rm -f "${HOSTFILE}"

echo "  Runtime stopped"
echo ""
echo "=========================================="
echo "Benchmark Complete"
echo "=========================================="

exit ${OMNI_EXIT}
