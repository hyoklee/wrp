#ifndef OMNI_JOB_CONFIG_H
#define OMNI_JOB_CONFIG_H

#include <string>
#include <vector>

#ifdef USE_HDF5
#include <hdf5.h>
#endif

struct OmniJobConfig {
  std::string name;
  int max_scale;
  std::string hostfile; // Add hostfile to config

  struct DataEntry {
    std::vector<std::string> paths;  // Changed from single path to multiple paths
    std::vector<size_t> range;
    size_t offset;
    size_t size;
    std::vector<std::string> description;
    std::string hash;

#ifdef  USE_HDF5    
    // HDF5 support
    std::string src;  // Source path/URL (e.g., "hdf5://path/to/file.h5/dataset_name" or "http://example.com/file")
    std::vector<hsize_t> hdf5_start;   // HDF5 hyperslab start coordinates
    std::vector<hsize_t> hdf5_count;   // HDF5 hyperslab count
    std::vector<hsize_t> hdf5_stride;  // HDF5 hyperslab stride
    std::string run_script;            // HDF5 run script
    std::string destination;           // HDF5 destination
#endif
    
    DataEntry() : offset(0), size(0) {}
  };

  std::vector<DataEntry> data_entries;

  OmniJobConfig() : max_scale(100) {}
};

// Function declarations
void ProcessDataEntry(const OmniJobConfig::DataEntry &entry, int nprocs, const std::string &hostfile);
void ProcessDataEntryAsync(const OmniJobConfig::DataEntry &entry, int nprocs, const std::string &hostfile);

#endif // OMNI_JOB_CONFIG_H
