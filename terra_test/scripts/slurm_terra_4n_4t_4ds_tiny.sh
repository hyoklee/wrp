#!/bin/bash
#SBATCH --job-name=terra_4n_tiny
#SBATCH --nodes=4
#SBATCH --ntasks-per-node=1
#SBATCH --cpus-per-task=4
#SBATCH --time=00:10:00
#SBATCH --output=/mnt/common/hyoklee/core.iowarp/terra_test/logs/terra_4n_4t_4ds_tiny_%j.out
#SBATCH --error=/mnt/common/hyoklee/core.iowarp/terra_test/logs/terra_4n_4t_4ds_tiny_%j.err
#SBATCH --exclusive

set -e

# Configuration
TEST_DIR="/mnt/common/hyoklee/core.iowarp/terra_test"
CORE_DIR="/mnt/common/hyoklee/core.iowarp"
BUILD_DIR="${CORE_DIR}/build"
NODES=4
TASKS=4

# Activate conda environment
source /home/hyoklee/mc3/etc/profile.d/conda.sh
conda activate iowarp-build

# Set library paths
export LD_LIBRARY_PATH="${BUILD_DIR}/bin:${LD_LIBRARY_PATH}"

# Create logs directory
mkdir -p "${TEST_DIR}/logs"

echo "=========================================="
echo "TERRA Benchmark (4 tiny datasets): ${NODES} nodes, ${TASKS} tasks"
echo "Job ID: ${SLURM_JOB_ID}"
echo "Nodes: $(scontrol show hostnames ${SLURM_JOB_NODELIST})"
echo "=========================================="

# Generate hostfile for distributed run with IP addresses
HOSTFILE="${TEST_DIR}/configs/hostfile_${SLURM_JOB_ID}"
echo "Generating hostfile with IP addresses..."
> "${HOSTFILE}"
for hostname in $(scontrol show hostnames "${SLURM_JOB_NODELIST}"); do
    ip_addr=$(getent hosts "${hostname}" | awk '{print $1}' | head -1)
    if [[ -z "${ip_addr}" ]]; then
        ip_addr="${hostname}"
    fi
    echo "${ip_addr}" >> "${HOSTFILE}"
    echo "  ${hostname} -> ${ip_addr}"
done
echo "Hostfile contents:"
cat "${HOSTFILE}"

export CHI_HOSTFILE="${HOSTFILE}"

# Create temporary runtime config
CONFIG_FILE="${TEST_DIR}/configs/terra_runtime_config.yaml"
RUNTIME_CONFIG_TEMP="${TEST_DIR}/configs/runtime_${SLURM_JOB_ID}.yaml"

sed -e "s/\${CHI_SCHED_THREADS:-8}/${TASKS}/g" \
    -e "s/\${CHI_PORT:-5555}/5555/g" \
    -e "s/\${CHI_NEIGHBORHOOD:-32}/${NODES}/g" \
    -e "s/\${CHI_NEIGHBORHOOD:-1}/${NODES}/g" \
    -e "s|\${CHI_HOSTFILE:-}|${HOSTFILE}|g" \
    "${CONFIG_FILE}" > "${RUNTIME_CONFIG_TEMP}"

export CHI_SERVER_CONF="${RUNTIME_CONFIG_TEMP}"

echo ""
echo "Runtime config: ${RUNTIME_CONFIG_TEMP}"

# Use 4 tiny datasets config
OMNI_FILE="${TEST_DIR}/configs/terra_omni_4datasets_tiny.yaml"

echo ""
echo "Step 1: Starting Chimaera runtime on all nodes..."

srun --nodes=${NODES} --ntasks=${NODES} \
    "${BUILD_DIR}/bin/chimaera_start_runtime" &

RUNTIME_PID=$!
echo "  Waiting for distributed runtime initialization (30 seconds)..."
sleep 30

echo "  Runtime started (PID: ${RUNTIME_PID})"

echo ""
echo "Step 2: Running HDF5 assimilation (4 tiny datasets, 1 per node)..."

START_TIME=$(date +%s.%N)

"${BUILD_DIR}/bin/wrp_cae_omni" "${OMNI_FILE}" 2>&1 | tee "${TEST_DIR}/logs/omni_4n_4t_4ds_tiny_${SLURM_JOB_ID}.log"
OMNI_EXIT=${PIPESTATUS[0]}

END_TIME=$(date +%s.%N)
ELAPSED_TIME=$(echo "${END_TIME} - ${START_TIME}" | bc)

echo ""
echo "Step 3: Results..."
echo "  OMNI Exit Code: ${OMNI_EXIT}"
echo "  Elapsed Time: ${ELAPSED_TIME} seconds"

echo ""
echo "Step 4: Stopping runtime..."

kill ${RUNTIME_PID} 2>/dev/null || true
wait ${RUNTIME_PID} 2>/dev/null || true

rm -f "${RUNTIME_CONFIG_TEMP}"
rm -f "${HOSTFILE}"

echo "  Runtime stopped"
echo ""
echo "=========================================="
echo "Benchmark Complete (4 tiny datasets)"
echo "=========================================="

exit ${OMNI_EXIT}
