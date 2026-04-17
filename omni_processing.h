#ifndef OMNI_PROCESSING_H
#define OMNI_PROCESSING_H

#include <string>
#include <vector>
#include "omni_job_config.h"

// OmniJobConfig is now defined in omni_job_config.h

// Function declarations
OmniJobConfig ParseOmniFile(const std::string &yaml_file);
void ProcessHdf5DataEntry(const OmniJobConfig::DataEntry &entry);

// Helper function to expand file paths
std::string ExpandPath(const std::string &path);
std::vector<std::string> ExpandFilePattern(const std::string &pattern);

#endif // OMNI_PROCESSING_H
