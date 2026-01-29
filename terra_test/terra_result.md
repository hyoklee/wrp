# TERRA Fusion I/O Performance Results

## Test Environment

- **Date:** 2026-01-27
- **Platform:** IOWarp Core (core.iowarp)
- **HDF5 File:** TERRA_BF_L1B_O10204_20011118010522_F000_V001.h5
- **File Location:** /mnt/common/datasets-staging/
- **Total Datasets:** 64 (8 granules × 4 sub-sensors × 2 lat/lon)
- **Estimated Data Size:** ~5.2 GB
- **kMaxChunkSize:** 768 MB (updated from 1 MB to match MISR chunk size)

## Changes from Previous Run

- **kMaxChunkSize increased:** 1 MB → 768 MB
  - MISR Radiance chunks: 720 MB
  - ASTER VNIR chunks: 244 MB
  - MODIS 250m chunks: 169 MB
- **SLURM time limit increased:** 1 hour → 4 hours

## Dataset Details

| Sensor Type | Datasets | Approximate Size Per Dataset | Total Per Type |
|-------------|----------|------------------------------|----------------|
| Geolocation (11×11) | 16 | 968 bytes | ~15 KB |
| SWIR (2695×2968) | 16 | ~64 MB | ~1,024 MB |
| TIR (899×990) | 16 | ~7 MB | ~112 MB |
| VNIR (5389×5935) | 16 | ~256 MB | ~4,096 MB |
| **Total** | **64** | | **~5,232 MB** |

## Test Configurations

All tests were run with exclusive node allocation via SLURM.

| Run | Nodes | Tasks/Node | Total Tasks | Configuration |
|-----|-------|------------|-------------|---------------|
| 1   | 1     | 1          | 1           | Baseline single-threaded |
| 2   | 1     | 2          | 2           | 2 threads |
| 3   | 1     | 4          | 4           | 4 threads |
| 4   | 1     | 8          | 8           | 8 threads |
| 5   | 2     | 1          | 2           | 2 nodes, 1 thread each |
| 6   | 2     | 2          | 4           | 2 nodes, 2 threads each |
| 7   | 2     | 4          | 8           | 2 nodes, 4 threads each |
| 8   | 2     | 8          | 16          | 2 nodes, 8 threads each |
| 9   | 4     | 1          | 4           | 4 nodes, 1 thread each |
| 10  | 4     | 2          | 8           | 4 nodes, 2 threads each |
| 11  | 4     | 4          | 16          | 4 nodes, 4 threads each |
| 12  | 4     | 8          | 32          | 4 nodes, 8 threads each |
| 13  | 8     | 1          | 8           | 8 nodes, 1 thread each |
| 14  | 8     | 2          | 16          | 8 nodes, 2 threads each |
| 15  | 8     | 4          | 32          | 8 nodes, 4 threads each |
| 16  | 8     | 8          | 64          | 8 nodes, 8 threads each |

## Results

| Nodes | Tasks | Total Tasks | Time (s) | Bandwidth (MB/s) | Status |
|-------|-------|-------------|----------|------------------|--------|
| 1     | 1     | 1           | -        | -                | Running |
| 1     | 2     | 2           | -        | -                | Running |
| 1     | 4     | 4           | -        | -                | Running |
| 1     | 8     | 8           | -        | -                | Running |
| 2     | 1     | 2           | -        | -                | Running |
| 2     | 2     | 4           | -        | -                | Running |
| 2     | 4     | 8           | -        | -                | Running |
| 2     | 8     | 16          | -        | -                | Pending |
| 4     | 1     | 4           | -        | -                | Pending |
| 4     | 2     | 8           | -        | -                | Pending |
| 4     | 4     | 16          | -        | -                | Pending |
| 4     | 8     | 32          | -        | -                | Pending |
| 8     | 1     | 8           | -        | -                | Pending |
| 8     | 2     | 16          | -        | -                | Pending |
| 8     | 4     | 32          | -        | -                | Pending |
| 8     | 8     | 64          | -        | -                | Pending |

## SLURM Job IDs

- 1n_1t: 6571
- 1n_2t: 6572
- 1n_4t: 6573
- 1n_8t: 6574
- 2n_1t: 6575
- 2n_2t: 6576
- 2n_4t: 6577
- 2n_8t: 6578
- 4n_1t: 6579
- 4n_2t: 6580
- 4n_4t: 6581
- 4n_8t: 6582
- 8n_1t: 6583
- 8n_2t: 6584
- 8n_4t: 6585
- 8n_8t: 6586

## Analysis

*To be completed after all tests finish.*

### Scaling Observations

- Single-node scaling:
- Multi-node scaling:
- Optimal configuration:

### Bottlenecks Identified

-
-
-

### Recommendations

-
-
-

## Files Generated

- OMNI Configuration: `terra_test/configs/terra_omni_64datasets.yaml`
- Runtime Configuration: `terra_test/configs/terra_runtime_config.yaml`
- SLURM Scripts: `terra_test/scripts/slurm_terra_*.sh`
- Results CSV: `terra_test/results/terra_benchmark_summary.csv`
- Individual Logs: `terra_test/logs/terra_*_*.out`

## How to Reproduce

1. Build IOWarp Core:
   ```bash
   source /home/hyoklee/mc3/etc/profile.d/conda.sh
   conda activate iowarp-build
   cd /mnt/common/hyoklee/core.iowarp
   mkdir -p build && cd build
   /home/hyoklee/mc3/bin/cmake .. -DCMAKE_BUILD_TYPE=Release -DWRP_CORE_ENABLE_CONDA=ON -DWRP_CORE_ENABLE_TESTS=ON
   make -j$(nproc)
   ```

2. Submit all tests:
   ```bash
   cd /mnt/common/hyoklee/core.iowarp/terra_test
   ./scripts/submit_all_tests.sh
   ```

3. Monitor progress:
   ```bash
   squeue -u hyoklee
   tail -f logs/terra_1n_1t_*.out
   ```

4. View results:
   ```bash
   cat results/terra_benchmark_summary.csv
   ```
