// Copyright 2025 Content Assimilation Engine
// SPDX-License-Identifier: Apache-2.0

#include "format/globus_utils.h"

#include <iostream>

#include <Poco/Exception.h>
#include <Poco/URI.h>

bool is_globus_uri(const std::string& uri) {
  return uri.rfind("globus://", 0) == 0;
}

bool parse_globus_uri(const std::string& uri, std::string& endpoint_id,
                      std::string& path) {
  if (!is_globus_uri(uri)) {
    std::cerr << "Error: Not a valid Globus URI: " << uri << std::endl;
    return false;
  }

  // Remove the "globus://" prefix
  const size_t scheme_len = 9;  // Length of "globus://"
  const std::string uri_part = uri.substr(scheme_len);
  std::cout << "uri_part=" << uri_part << std::endl;
  // Find the first slash after the endpoint ID
  size_t first_slash = uri_part.find('/');

  if (first_slash == 0) {
    // Handle case where there's a leading slash after globus://
    // e.g., globus:///~/path/to/file
    endpoint_id = "";
    path = uri_part;
  } else if (first_slash == std::string::npos) {
    // No path specified, only endpoint ID
    // e.g., globus://endpoint-id
    endpoint_id = uri_part;
    path = "/";
  } else {
    // Standard case: globus://endpoint-id/path/to/file
    endpoint_id = uri_part.substr(0, first_slash);
    path = uri_part.substr(first_slash);
  }

  // Validate endpoint ID (should not be empty)
  if (endpoint_id.empty()) {
    std::cerr << "Error: Empty endpoint ID in Globus URI: " << uri
              << std::endl;
    return false;
  }

  // Log the parsed components for debugging
  std::cerr << "Debug - Parsed Globus URI:" << std::endl;
  std::cerr << "  Endpoint ID: " << endpoint_id << std::endl;
  std::cerr << "  Path: " << path << std::endl;

  // Note: path is guaranteed to start with '/' due to the logic above
  // - If first_slash == 0: path = uri_part (starts with '/')
  // - If first_slash == npos: path = "/"
  // - Otherwise: path = uri_part.substr(first_slash) (includes the '/' at
  //   first_slash)

  return true;
}

bool transfer_globus_file(const std::string& source_uri,
                          const std::string& dest_uri,
                          const std::string& access_token,
                          const std::string& transfer_label) {
  // This function is implemented in glo.cc
  // We declare it as extern here to avoid duplicate definition
  extern bool transfer_globus_file_impl(const std::string& source_uri,
                                        const std::string& dest_uri,
                                        const std::string& access_token,
                                        const std::string& transfer_label);

  return transfer_globus_file_impl(source_uri, dest_uri, access_token,
                                   transfer_label);
}
