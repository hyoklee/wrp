#ifndef CAE_FORMAT_DATASET_CONFIG_H_
#define CAE_FORMAT_DATASET_CONFIG_H_

#include <string>
#include <vector>
#include <cstdint>
#include <yaml-cpp/yaml.h>

namespace cae {

/**
 * Configuration for reading datasets from HDF5 files
 */
struct DatasetConfig {
  std::string name;
  std::vector<std::string> tags;
  std::string uri;
  std::vector<uint64_t> start;
  std::vector<uint64_t> count;
  std::vector<uint64_t> stride;
  std::string run_script;
  std::string destination;

  DatasetConfig() = default;
};

/**
 * Parse dataset configuration from YAML file
 */
DatasetConfig ParseDatasetConfig(const std::string& yaml_file);


/**
 * Parse HDF5 URI to extract file path and dataset name
 */
bool ParseHdf5UriOld(const std::string& uri, std::string& file_path, std::string& dataset_name);

  
} // namespace cae

#endif // CAE_FORMAT_DATASET_CONFIG_H_
