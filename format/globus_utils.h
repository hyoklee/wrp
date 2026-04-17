#ifndef GLOBUS_UTILS_H
#define GLOBUS_UTILS_H

#include <string>

#ifdef __cplusplus
extern "C" {
#endif

// Check if the given URI is a Globus URI
bool is_globus_uri(const std::string &uri);

// Parse a Globus URI into endpoint ID and path
// Format: globus://<endpoint_id>/<path>
// Returns true if parsing was successful, false otherwise
bool parse_globus_uri(const std::string &uri, std::string &endpoint_id, std::string &path);

// Transfer a file between two Globus endpoints
// Returns true if the transfer was successful, false otherwise
bool transfer_globus_file(
    const std::string& source_uri,
    const std::string& dest_uri,
    const std::string& access_token,
    const std::string& transfer_label = "OMNICAE Transfer"
);

// Implementation of the transfer function (defined in glo.cc)
bool transfer_globus_file_impl(
    const std::string& source_uri,
    const std::string& dest_uri,
    const std::string& access_token,
    const std::string& transfer_label
);

#ifdef __cplusplus
}
#endif

#endif // GLOBUS_UTILS_H
