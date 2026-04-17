#include "dataset_config.h"
#include <iostream>
#include <regex>
#include <sstream>

namespace cae {

DatasetConfig ParseDatasetConfig(const std::string& yaml_file) {
  DatasetConfig config;
  
  try {
    YAML::Node yaml = YAML::LoadFile(yaml_file);
    
    // Parse name
    if (yaml["name"]) {
      config.name = yaml["name"].as<std::string>();
    }
    
    // Parse tags
    if (yaml["tags"]) {
      const YAML::Node& tags_node = yaml["tags"];
      for (const auto& tag : tags_node) {
        config.tags.push_back(tag.as<std::string>());
      }
    }
    
    // Parse URI
    if (yaml["src"]) {
      config.uri = yaml["src"].as<std::string>();
    }
    
    // Parse start coordinates
    if (yaml["start"]) {
      const YAML::Node& start_node = yaml["start"];
      for (const auto& coord : start_node) {
        config.start.push_back(coord.as<uint64_t>());
      }
    }
    
    // Parse count
    if (yaml["count"]) {
      const YAML::Node& count_node = yaml["count"];
      for (const auto& val : count_node) {
        config.count.push_back(val.as<uint64_t>());
      }
    }
    
    // Parse stride
    if (yaml["stride"]) {
      const YAML::Node& stride_node = yaml["stride"];
      for (const auto& val : stride_node) {
        config.stride.push_back(val.as<uint64_t>());
      }
    }
    
    // Parse run script
    if (yaml["run"]) {
      config.run_script = yaml["run"].as<std::string>();
    }
    
    // Parse destination
    if (yaml["dst"]) {
      config.destination = yaml["dst"].as<std::string>();
    }
    
  } catch (const YAML::Exception& e) {
    std::cerr << "YAML parsing error: " << e.what() << std::endl;
    throw;
  }
  
  return config;
}

bool ParseHdf5UriOld(const std::string& uri, std::string& file_path, std::string& dataset_name) {
  // Parse URI format: hdf5://path/to/file.h5/dataset_name
  std::regex uri_regex(R"(hdf5://(.+\.h5)/?(.*))");
  std::smatch match;
  
  if (std::regex_match(uri, match, uri_regex)) {
    if (match.size() >= 3) {
      file_path = match[1].str();
      dataset_name = match[2].str();
      return true;
    }
  }
  
  return false;
}

} // namespace cae
