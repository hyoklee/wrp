///
/// OMNI.cc
///
#ifdef _WIN32
// Fix Winsock conflicts by including winsock2 first
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#ifndef NOMINMAX
#define NOMINMAX  // Prevent Windows headers from defining min/max macros
#endif
#include <winsock2.h>
#include <ws2tcpip.h>
#endif

#include "OMNI.h"
#include "omni_job_config.h"
#include "format/format_factory.h"
#include "repo/filesystem_repo_omni.h"
#include "repo/repo_factory.h"
#include "format/dataset_config.h"
#ifdef USE_HDF5
#include "format/hdf5_dataset_client.h"
#include <hdf5.h>
#include "omni_processing.h"
#endif
#include <cstdlib>
#include <iostream>
#include <limits.h>  // For PATH_MAX
#ifdef USE_MPI
#include <mpi.h>
#endif
#ifndef _WIN32
#include <pwd.h>
#include <unistd.h>
#include <glob.h>
#else
// Windows-specific includes
#include <windows.h>
#include <shlobj.h>
#include <direct.h>
#endif
#include <sstream>
#include <string>
#include <vector>
#include <yaml-cpp/yaml.h>
#include <filesystem>
#include <future>
#include <thread>
#include <algorithm>
#include <mutex>
#include <fstream>
#include <cctype>  // For isspace
#include <cstdio>  // For std::remove
#include <regex>

// Additional includes for put/get functionality
#include <errno.h>
#include <fcntl.h>
#ifdef _WIN32
#include <io.h>
#else
#include <unistd.h>
#endif
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <cassert>
#include <cstring>

#if defined(_MSC_VER)
#include <BaseTsd.h>
typedef SSIZE_T ssize_t;
#endif

#ifdef USE_HERMES
#include <hermes/data_stager/stager_factory.h>
#include <hermes/hermes.h>
#endif

#ifdef USE_POCO
#include <chrono>
#include <iomanip>
#include <thread>
#include "Poco/DigestEngine.h"
#include "Poco/Exception.h"
#include "Poco/File.h"
#include "Poco/FileStream.h"
#include "Poco/Net/Context.h"
#include "Poco/Net/HTTPClientSession.h"
#include "Poco/Net/HTTPRequest.h"
#include "Poco/Net/HTTPResponse.h"
#include "Poco/Net/HTTPSClientSession.h"
#include "Poco/Net/NetException.h"
#include "Poco/Net/SSLManager.h"
#include "Poco/NullStream.h"
#include "Poco/Path.h"
#include "Poco/Pipe.h"
#include "Poco/PipeStream.h"
#include "Poco/Process.h"
#ifndef __OpenBSD__
#include "Poco/SHA2Engine.h"
#include "Poco/HMACEngine.h"
#endif
#include "Poco/SharedMemory.h"
#include "Poco/StreamCopier.h"
#include "Poco/TemporaryFile.h"
#include "Poco/URI.h"
#include "format/globus_utils.h"

// Compatibility wrapper for older POCO versions that don't have SHA2Engine256
// SHA2Engine256 was added in POCO 1.12.0 (0x010C0000)
#if defined(USE_POCO) && !defined(__OpenBSD__)
#include "Poco/Version.h"
#if POCO_VERSION < 0x010C0000
namespace Poco {
// Fallback for older POCO versions
class SHA2Engine256 : public SHA2Engine {
public:
  enum {
    BLOCK_SIZE = 64,
    DIGEST_SIZE = 32
  };

  SHA2Engine256() : SHA2Engine(SHA_256) {}
};
}  // namespace Poco
#endif
#endif

#ifdef USE_POCO
// Cross-platform SHA256 and HMAC-SHA256 helpers used by WriteS3
#ifdef __OpenBSD__
// OpenBSD: use native sha2.h and LibreSSL (no Poco SHA2Engine available)
#include <sha2.h>
#include <openssl/hmac.h>
#include <openssl/evp.h>

static std::string sha256_hex_impl(const std::string& data) {
  uint8_t hash[SHA256_DIGEST_LENGTH];
  SHA2_CTX ctx;
  SHA256Init(&ctx);
  SHA256Update(&ctx, reinterpret_cast<const uint8_t*>(data.data()), data.size());
  SHA256Final(hash, &ctx);
  std::ostringstream ss;
  for (int i = 0; i < SHA256_DIGEST_LENGTH; i++)
    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(hash[i]);
  return ss.str();
}

static std::vector<unsigned char> hmac_sha256_raw_impl(const std::string& key,
                                                        const std::string& data) {
  unsigned char result[EVP_MAX_MD_SIZE];
  unsigned int result_len = 0;
  HMAC(EVP_sha256(), key.data(), static_cast<int>(key.size()),
       reinterpret_cast<const unsigned char*>(data.data()), data.size(),
       result, &result_len);
  return std::vector<unsigned char>(result, result + result_len);
}

static std::string hmac_sha256_hex_impl(const std::string& key,
                                         const std::string& data) {
  auto raw = hmac_sha256_raw_impl(key, data);
  std::ostringstream ss;
  for (unsigned char b : raw)
    ss << std::hex << std::setfill('0') << std::setw(2) << static_cast<int>(b);
  return ss.str();
}
#else
// Non-OpenBSD: use Poco SHA2Engine256 and HMACEngine
static std::string sha256_hex_impl(const std::string& data) {
  Poco::SHA2Engine256 engine;
  engine.update(data);
  return Poco::DigestEngine::digestToHex(engine.digest());
}

static std::vector<unsigned char> hmac_sha256_raw_impl(const std::string& key,
                                                        const std::string& data) {
  Poco::HMACEngine<Poco::SHA2Engine256> hmac(key);
  hmac.update(data);
  return hmac.digest();
}

static std::string hmac_sha256_hex_impl(const std::string& key,
                                         const std::string& data) {
  Poco::HMACEngine<Poco::SHA2Engine256> hmac(key);
  hmac.update(data);
  return Poco::DigestEngine::digestToHex(hmac.digest());
}
#endif  // __OpenBSD__
#endif  // USE_POCO

#ifdef USE_REDIS
#include "Poco/Redis/Client.h"
#include "Poco/Redis/Command.h"
#include "Poco/Redis/Array.h"
#include "Poco/Redis/Type.h"
#endif
#endif

#ifdef USE_MEMCACHED
#ifdef _WIN32
#include <winsock2.h>
#include <ws2tcpip.h>
#pragma comment(lib, "ws2_32.lib")
#else
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <netdb.h>
#endif
#endif

#ifdef USE_AWS
#include <aws/core/Aws.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/utils/StringUtils.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/CreateBucketRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#endif

namespace fs = std::filesystem;

namespace cae {

// Public API implementations
int OMNI::Put(const std::string& input_file) {
  if (SetBlackhole() != 0) {
    return 1;
  }
  return ReadOmni(input_file);
}

int OMNI::Get(const std::string& buffer) {
  return WriteOmni(buffer);
}

int OMNI::List() {
#ifdef USE_DATAHUB
  // Try to query DataHub if configured, otherwise read from local metadata
  if (CheckDataHubConfig()) {
    auto datasets = QueryDataHubForAllDatasets();
    if (!datasets.empty()) {
      // Display datasets from DataHub
      for (const auto& dataset : datasets) {
        if (!quiet_) {
          std::cout << dataset.first << "|" << dataset.second << std::endl;
        }
      }
      return 0;
    } else {
      if (!quiet_) {
        std::cout << "No datasets found in DataHub, trying local metadata..." << std::endl;
      }
      // Fall through to local metadata
    }
  }
#endif

  // Read from local metadata file
  const std::string filename = ".blackhole/ls";
  std::ifstream input_file(filename);
  if (!input_file.is_open()) {
    std::cerr << "Error: Could not open the file \"" << filename << "\""
              << std::endl;
    return 1;
  }

  std::string line;
  while (std::getline(input_file, line)) {
    if (!quiet_) {
      std::cout << line << std::endl;
    }
  }

  if (input_file.bad()) {
    std::cerr << "Error: An unrecoverable error occurred while reading the file."
              << std::endl;
    return 1;
  }
  input_file.close();
  return 0;
}

// Private method implementations
#ifdef USE_POCO
std::string OMNI::Sha256File(const std::string& file_path) {
#ifdef __OpenBSD__
  char result[SHA256_DIGEST_STRING_LENGTH];
  if (SHA256File(file_path.c_str(), result) == nullptr) {
    throw std::runtime_error("Error: calculating SHA256 of file: " + file_path);
  }
  return std::string(result);
#else
  try {
    Poco::FileInputStream fis(file_path);
    Poco::SHA2Engine sha256(Poco::SHA2Engine::SHA_256);
    const size_t buffer_size = 8192;
    char buffer[buffer_size];

    while (!fis.eof()) {
      fis.read(buffer, buffer_size);
      std::streamsize bytes_read = fis.gcount();
      if (bytes_read > 0) {
        sha256.update(buffer, static_cast<unsigned>(bytes_read));
      }
    }

    const Poco::DigestEngine::Digest& digest = sha256.digest();
    std::stringstream ss;
    for (unsigned char b : digest) {
      ss << std::hex << std::setfill('0') << std::setw(2)
         << static_cast<int>(b);
    }
    return ss.str();
  } catch (const Poco::Exception& ex) {
    throw std::runtime_error("Error: calculating SHA256 - " + ex.displayText());
  }
#endif
}
#endif

int OMNI::WriteMeta(const std::string& name, const std::string& tags) {
  std::string file_path = ".blackhole/ls";
  std::ofstream outfile(file_path, std::ios::out | std::ios::app);
  if (outfile.is_open()) {
    outfile << name << "|";
    outfile << tags << std::endl;
    outfile.close();
  }
  return 0;
}

std::string OMNI::ReadConfigFile(const std::string& config_path) {
  std::ifstream config_file(config_path);
  if (!config_file.is_open()) {
#ifdef USE_DATAHUB
    if (!quiet_) {
      std::cout << "Config file '" << config_path << "' not found, skipping DataHub registration" << std::endl;
    }
#endif
    return "";
  }

  std::stringstream buffer;
  buffer << config_file.rdbuf();
  config_file.close();
  return buffer.str();
}

std::string OMNI::ReadConfigValue(const std::string& key) {
  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    return "";
  }
#else
  char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      return "";
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string config_path = home_dir + "/.wrp/config";
  std::string config_content = ReadConfigFile(config_path);

  if (config_content.empty()) {
    return "";
  }

  // Parse config file looking for the key
  std::istringstream stream(config_content);
  std::string line;
  std::string key_prefix = key + " ";

  while (std::getline(stream, line)) {
    // Trim whitespace
    line.erase(0, line.find_first_not_of(" \t\n\r\f\v"));
    line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);

    // Skip comments and empty lines
    if (line.empty() || line[0] == '#') {
      continue;
    }

    // Check if line starts with the key
    if (line.find(key_prefix) == 0) {
      std::string value = line.substr(key_prefix.length());
      // Trim any remaining whitespace
      value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
      value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);
      return value;
    }
  }

  return "";
}

ProxyConfig OMNI::ReadProxyConfig() {
  ProxyConfig config;

  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    return config;
  }
#else
  const char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      return config;
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string config_path = home_dir + "/.wrp/config";
  std::string config_content = ReadConfigFile(config_path);

  // First try to read from config file
  if (!config_content.empty()) {
    // Parse ProxyConfig lines from config
    // Expected format:
    // ProxyConfig enabled
    // ProxyHost proxy.example.com
    // ProxyPort 8080
    // ProxyUsername username (optional)
    // ProxyPassword password (optional)
    std::istringstream stream(config_content);
    std::string line;

    while (std::getline(stream, line)) {
      // Trim whitespace
      line.erase(0, line.find_first_not_of(" \t\n\r\f\v"));
      line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);

      if (line.find("ProxyConfig enabled") == 0) {
        config.enabled = true;
      } else if (line.find("ProxyHost ") == 0) {
        config.host = line.substr(10);
        // Trim any remaining whitespace
        config.host.erase(0, config.host.find_first_not_of(" \t\n\r\f\v"));
        config.host.erase(config.host.find_last_not_of(" \t\n\r\f\v") + 1);
      } else if (line.find("ProxyPort ") == 0) {
        std::string port_str = line.substr(10);
        port_str.erase(0, port_str.find_first_not_of(" \t\n\r\f\v"));
        try {
          config.port = std::stoi(port_str);
        } catch (...) {
          config.port = 0;
        }
      } else if (line.find("ProxyUsername ") == 0) {
        config.username = line.substr(14);
        config.username.erase(0, config.username.find_first_not_of(" \t\n\r\f\v"));
        config.username.erase(config.username.find_last_not_of(" \t\n\r\f\v") + 1);
      } else if (line.find("ProxyPassword ") == 0) {
        config.password = line.substr(14);
        config.password.erase(0, config.password.find_first_not_of(" \t\n\r\f\v"));
        config.password.erase(config.password.find_last_not_of(" \t\n\r\f\v") + 1);
      }
    }
  }

  // If config not found in file, check environment variables
  // This is the standard way proxy settings are configured on compute nodes
  if (!config.enabled) {
    // Check for http_proxy or HTTP_PROXY environment variable
    const char* http_proxy = std::getenv("http_proxy");
    if (http_proxy == nullptr) {
      http_proxy = std::getenv("HTTP_PROXY");
    }

    // Check for https_proxy or HTTPS_PROXY environment variable
    const char* https_proxy = std::getenv("https_proxy");
    if (https_proxy == nullptr) {
      https_proxy = std::getenv("HTTPS_PROXY");
    }

    // Use https_proxy if available, otherwise fall back to http_proxy
    const char* proxy_url = https_proxy ? https_proxy : http_proxy;

    if (proxy_url != nullptr && strlen(proxy_url) > 0) {
      std::string proxy_str(proxy_url);

      // Parse proxy URL format: [http://][username:password@]host:port[/]
      // Remove protocol prefix if present
      size_t protocol_end = proxy_str.find("://");
      if (protocol_end != std::string::npos) {
        proxy_str = proxy_str.substr(protocol_end + 3);
      }

      // Remove trailing slash if present
      if (!proxy_str.empty() && proxy_str.back() == '/') {
        proxy_str.pop_back();
      }

      // Check for username:password@
      size_t at_pos = proxy_str.find('@');
      if (at_pos != std::string::npos) {
        std::string credentials = proxy_str.substr(0, at_pos);
        proxy_str = proxy_str.substr(at_pos + 1);

        // Parse username:password
        size_t colon_pos = credentials.find(':');
        if (colon_pos != std::string::npos) {
          config.username = credentials.substr(0, colon_pos);
          config.password = credentials.substr(colon_pos + 1);
        } else {
          config.username = credentials;
        }
      }

      // Parse host:port
      size_t colon_pos = proxy_str.find(':');
      if (colon_pos != std::string::npos) {
        config.host = proxy_str.substr(0, colon_pos);
        std::string port_str = proxy_str.substr(colon_pos + 1);
        try {
          config.port = std::stoi(port_str);
          config.enabled = true;
        } catch (...) {
          // Invalid port, disable proxy
          config.enabled = false;
        }
      } else {
        // No port specified, use default ports
        config.host = proxy_str;
        config.port = 8080;  // Default proxy port
        config.enabled = true;
      }

      if (!quiet_ && config.enabled) {
        std::cout << "Proxy configured from environment: " << config.host << ":" << config.port << std::endl;
      }
    }
  } else if (!quiet_) {
    std::cout << "Proxy configured from file: " << config.host << ":" << config.port << std::endl;
  }

  return config;
}

AWSConfig OMNI::ReadAWSConfig() {
  AWSConfig config;

  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    return config;
  }
#else
  const char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      return config;
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string config_path = home_dir + "/.aws/config";
  std::string config_content = ReadConfigFile(config_path);

  if (config_content.empty()) {
    return config;
  }

  // Parse AWS config file
  // Expected format:
  // [default]
  // endpoint_url = http://localhost:4566
  // region = us-east-1
  std::istringstream stream(config_content);
  std::string line;
  bool in_default_section = false;

  while (std::getline(stream, line)) {
    // Trim whitespace
    line.erase(0, line.find_first_not_of(" \t\n\r\f\v"));
    line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);

    // Skip comments and empty lines
    if (line.empty() || line[0] == '#' || line[0] == ';') {
      continue;
    }

    // Check for section headers
    if (line[0] == '[') {
      in_default_section = (line == "[default]");
      continue;
    }

    // Parse key-value pairs in the default section
    if (in_default_section) {
      size_t eq_pos = line.find('=');
      if (eq_pos != std::string::npos) {
        std::string key = line.substr(0, eq_pos);
        std::string value = line.substr(eq_pos + 1);

        // Trim whitespace from key and value
        key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
        key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
        value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
        value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);

        if (key == "endpoint_url") {
          config.endpoint_url = value;
        } else if (key == "region") {
          config.region = value;
        }
      }
    }
  }

  if (!quiet_ && !config.endpoint_url.empty()) {
    std::cout << "AWS endpoint_url from config: " << config.endpoint_url << std::endl;
  }

  return config;
}

// Helper function to read AWS credentials from ~/.aws/credentials
static std::pair<std::string, std::string> ReadAWSCredentials() {
  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    return {"", ""};
  }
#else
  const char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      return {"", ""};
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string creds_path = home_dir + "/.aws/credentials";
  std::ifstream creds_file(creds_path);
  if (!creds_file.is_open()) {
    return {"", ""};
  }

  std::string access_key, secret_key;
  std::string line;
  bool in_default_section = false;

  while (std::getline(creds_file, line)) {
    // Trim whitespace
    line.erase(0, line.find_first_not_of(" \t\n\r\f\v"));
    line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);

    // Skip comments and empty lines
    if (line.empty() || line[0] == '#' || line[0] == ';') {
      continue;
    }

    // Check for section headers
    if (line[0] == '[') {
      in_default_section = (line == "[default]");
      continue;
    }

    // Parse key-value pairs in the default section
    if (in_default_section) {
      size_t eq_pos = line.find('=');
      if (eq_pos != std::string::npos) {
        std::string key = line.substr(0, eq_pos);
        std::string value = line.substr(eq_pos + 1);

        // Trim whitespace from key and value
        key.erase(0, key.find_first_not_of(" \t\n\r\f\v"));
        key.erase(key.find_last_not_of(" \t\n\r\f\v") + 1);
        value.erase(0, value.find_first_not_of(" \t\n\r\f\v"));
        value.erase(value.find_last_not_of(" \t\n\r\f\v") + 1);

        if (key == "aws_access_key_id") {
          access_key = value;
        } else if (key == "aws_secret_access_key") {
          secret_key = value;
        }
      }
    }
  }

  return {access_key, secret_key};
}

WaitConfig OMNI::ReadWaitConfig() {
  WaitConfig config;

  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    return config;
  }
#else
  const char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      return config;
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string config_path = home_dir + "/.wrp/config";
  std::string config_content = ReadConfigFile(config_path);

  if (config_content.empty()) {
    return config;
  }

  // Parse WaitConfig lines from config
  // Expected format:
  // WaitTimeout 300
  std::istringstream stream(config_content);
  std::string line;

  while (std::getline(stream, line)) {
    // Trim whitespace
    line.erase(0, line.find_first_not_of(" \t\n\r\f\v"));
    line.erase(line.find_last_not_of(" \t\n\r\f\v") + 1);

    if (line.find("WaitTimeout ") == 0) {
      std::string timeout_str = line.substr(12);
      timeout_str.erase(0, timeout_str.find_first_not_of(" \t\n\r\f\v"));
      try {
        config.timeout_seconds = std::stoi(timeout_str);
        if (!quiet_) {
          std::cout << "Wait timeout configured: " << config.timeout_seconds << " seconds" << std::endl;
        }
      } catch (...) {
        config.timeout_seconds = -1;
      }
    }
  }

  return config;
}

#ifdef USE_DATAHUB
std::string OMNI::ReadDataHubAPIKey() {
  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    if (!quiet_) {
      std::cout << "Could not determine home directory for DataHub API key" << std::endl;
    }
    return "";
  }
#else
  const char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      if (!quiet_) {
        std::cout << "Could not determine home directory for DataHub API key" << std::endl;
      }
      return "";
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string api_key_path = home_dir + "/.wrp/datahub";
  std::ifstream api_key_file(api_key_path);
  if (!api_key_file.is_open()) {
    if (!quiet_) {
      std::cout << "DataHub API key file '" << api_key_path << "' not found, continuing without authentication" << std::endl;
    }
    return "";
  }

  std::string api_key;
  std::getline(api_key_file, api_key);
  api_key_file.close();

  // Trim whitespace
  api_key.erase(0, api_key.find_first_not_of(" \t\n\r\f\v"));
  api_key.erase(api_key.find_last_not_of(" \t\n\r\f\v") + 1);

  return api_key;
}

bool OMNI::CheckDataHubConfig() {
  // Get home directory
  std::string home_dir;
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    home_dir = home_path;
    free(home_path);
  } else {
    if (!quiet_) {
      std::cout << "Could not determine home directory" << std::endl;
    }
    return false;
  }
#else
  const char* home_path = std::getenv("HOME");
  if (home_path == nullptr) {
    struct passwd* pw = getpwuid(getuid());
    if (pw == nullptr) {
      if (!quiet_) {
        std::cout << "Could not determine home directory" << std::endl;
      }
      return false;
    }
    home_dir = pw->pw_dir;
  } else {
    home_dir = home_path;
  }
#endif

  std::string config_path = home_dir + "/.wrp/config";
  std::string config_content = ReadConfigFile(config_path);

  if (config_content.empty()) {
    return false;
  }

  // Check if config contains "MetaStore DataHub"
  if (config_content.find("MetaStore DataHub") != std::string::npos) {
    if (!quiet_) {
      std::cout << "DataHub metastore detected in config" << std::endl;
    }
    return true;
  }

  return false;
}

int OMNI::RegisterWithDataHub(const std::string& name, const std::string& tags) {
  try {
    if (!quiet_) {
      std::cout << "Registering '" << name << "' with DataHub...";
    }

    // Read API key from ~/.wrp/datahub
    std::string api_key = ReadDataHubAPIKey();

    // Parse tags into array format
    std::vector<std::string> tag_list;
    std::stringstream ss(tags);
    std::string tag;
    while (std::getline(ss, tag, ',')) {
      // Trim whitespace
      tag.erase(0, tag.find_first_not_of(" \t\n\r\f\v"));
      tag.erase(tag.find_last_not_of(" \t\n\r\f\v") + 1);
      if (!tag.empty()) {
        tag_list.push_back(tag);
      }
    }

    // Build tags JSON array
    std::ostringstream tags_json;
    tags_json << "[";
    for (size_t i = 0; i < tag_list.size(); ++i) {
      tags_json << "{\"tag\": \"urn:li:tag:" << tag_list[i] << "\"}";
      if (i < tag_list.size() - 1) {
        tags_json << ",";
      }
    }
    tags_json << "]";

    // Construct DataHub API payload for dataset ingestion
    std::ostringstream json_payload;
    json_payload << "{"
                 << "\"entity\": {"
                 << "\"value\": {"
                 << "\"com.linkedin.metadata.snapshot.DatasetSnapshot\": {"
                 << "\"urn\": \"urn:li:dataset:(urn:li:dataPlatform:omni," << name << ",PROD)\","
                 << "\"aspects\": ["
                 << "{"
                 << "\"com.linkedin.common.GlobalTags\": {"
                 << "\"tags\": " << tags_json.str()
                 << "}"
                 << "}"
                 << "]"
                 << "}"
                 << "}"
                 << "}"
                 << "}";

    std::string payload = json_payload.str();

    // Create HTTP session to DataHub GMS API
    Poco::Net::HTTPClientSession session("localhost", 8080);
    session.setTimeout(Poco::Timespan(10, 0));  // 10 second timeout

    Poco::Net::HTTPRequest request(
        Poco::Net::HTTPRequest::HTTP_POST,
        "/entities?action=ingest",
        Poco::Net::HTTPMessage::HTTP_1_1);

    request.setContentType("application/json");
    request.setContentLength(payload.length());

    // Add Authorization header if API key is available
    if (!api_key.empty()) {
      request.set("Authorization", "Bearer " + api_key);
    }

    // Send request
    std::ostream& request_stream = session.sendRequest(request);
    request_stream << payload;

    // Receive response
    Poco::Net::HTTPResponse response;
    std::istream& response_stream = session.receiveResponse(response);

    int status = response.getStatus();

    // Read response body
    std::string response_body;
    Poco::StreamCopier::copyToString(response_stream, response_body);

    if (status == Poco::Net::HTTPResponse::HTTP_OK ||
        status == Poco::Net::HTTPResponse::HTTP_CREATED) {
      if (!quiet_) {
        std::cout << "done" << std::endl;
      }
      return 0;
    } else {
      std::cerr << "Error: DataHub API returned status " << status << std::endl;
      std::cerr << "Response: " << response_body << std::endl;
      return -1;
    }

  } catch (const Poco::Net::NetException& e) {
    std::cerr << "Network error registering with DataHub: " << e.displayText() << std::endl;
    return -1;
  } catch (const Poco::Exception& e) {
    std::cerr << "Error registering with DataHub: " << e.displayText() << std::endl;
    return -1;
  } catch (const std::exception& e) {
    std::cerr << "Error registering with DataHub: " << e.what() << std::endl;
    return -1;
  }
  return 0;
}

std::string OMNI::QueryDataHubForDataset(const std::string& name) {
  try {
    if (!quiet_) {
      std::cout << "Querying DataHub for dataset '" << name << "'...";
    }

    // Read API key from ~/.wrp/datahub
    std::string api_key = ReadDataHubAPIKey();

    // Construct the dataset URN
    std::string dataset_urn = "urn:li:dataset:(urn:li:dataPlatform:omni," + name + ",PROD)";

    // Construct GraphQL query to retrieve dataset tags
    std::ostringstream graphql_query;
    graphql_query << "{"
                  << "\"query\": \"query { "
                  << "dataset(urn: \\\"" << dataset_urn << "\\\") { "
                  << "globalTags { "
                  << "tags { "
                  << "tag { "
                  << "name "
                  << "} "
                  << "} "
                  << "} "
                  << "} "
                  << "}\""
                  << "}";

    std::string payload = graphql_query.str();

    // Create HTTP session to DataHub GraphQL API (port 9002)
    Poco::Net::HTTPClientSession session("localhost", 9002);
    session.setTimeout(Poco::Timespan(10, 0));  // 10 second timeout

    Poco::Net::HTTPRequest request(
        Poco::Net::HTTPRequest::HTTP_POST,
        "/api/v2/graphql",
        Poco::Net::HTTPMessage::HTTP_1_1);

    request.setContentType("application/json");
    request.setContentLength(payload.length());

    // Add Authorization header if API key is available
    if (!api_key.empty()) {
      request.set("Authorization", "Bearer " + api_key);
    }

    // Send request
    std::ostream& request_stream = session.sendRequest(request);
    request_stream << payload;

    // Receive response
    Poco::Net::HTTPResponse response;
    std::istream& response_stream = session.receiveResponse(response);

    int status = response.getStatus();

    // Read response body
    std::string response_body;
    Poco::StreamCopier::copyToString(response_stream, response_body);

    if (status == Poco::Net::HTTPResponse::HTTP_OK) {
      if (!quiet_) {
        std::cout << "done" << std::endl;
      }

      // Parse JSON response to extract tags
      // Expected format: {"data":{"dataset":{"globalTags":{"tags":[{"tag":{"name":"urn:li:tag:tagname"}}]}}}}
      std::string tags_result;

      // Simple JSON parsing - look for tag names
      size_t pos = 0;
      while ((pos = response_body.find("\"name\":\"urn:li:tag:", pos)) != std::string::npos) {
        pos += 19;  // Skip past "name":"urn:li:tag:
        size_t end_pos = response_body.find("\"", pos);
        if (end_pos != std::string::npos) {
          std::string tag = response_body.substr(pos, end_pos - pos);
          if (!tags_result.empty()) {
            tags_result += ",";
          }
          tags_result += tag;
          pos = end_pos;
        } else {
          break;
        }
      }

      if (!tags_result.empty()) {
        if (!quiet_) {
          std::cout << "Retrieved tags from DataHub: " << tags_result << std::endl;
        }
        return tags_result;
      } else {
        if (!quiet_) {
          std::cout << "No tags found in DataHub for dataset '" << name << "'" << std::endl;
        }
        return "";
      }
    } else if (status == Poco::Net::HTTPResponse::HTTP_NOT_FOUND) {
      if (!quiet_) {
        std::cout << "not found" << std::endl;
        std::cout << "Dataset '" << name << "' not found in DataHub" << std::endl;
      }
      return "";
    } else {
      std::cerr << "Error: DataHub GraphQL API returned status " << status << std::endl;
      std::cerr << "Response: " << response_body << std::endl;
      return "";
    }

  } catch (const Poco::Net::NetException& e) {
    std::cerr << "Network error querying DataHub: " << e.displayText() << std::endl;
    return "";
  } catch (const Poco::Exception& e) {
    std::cerr << "Error querying DataHub: " << e.displayText() << std::endl;
    return "";
  } catch (const std::exception& e) {
    std::cerr << "Error querying DataHub: " << e.what() << std::endl;
    return "";
  }
  return "";
}

std::vector<std::pair<std::string, std::string>> OMNI::QueryDataHubForAllDatasets() {
  std::vector<std::pair<std::string, std::string>> results;

  try {
    if (!quiet_) {
      std::cout << "Querying DataHub for all OMNI datasets...";
    }

    // Read API key from ~/.wrp/datahub
    std::string api_key = ReadDataHubAPIKey();

    // Construct GraphQL query to search for all datasets from the omni platform
    std::ostringstream graphql_query;
    graphql_query << "{"
                  << "\"query\": \"query { "
                  << "search(input: { type: DATASET, query: \\\"*\\\", start: 0, count: 1000, filters: [{field: \\\"platform\\\", values: [\\\"urn:li:dataPlatform:omni\\\"]}] }) { "
                  << "searchResults { "
                  << "entity { "
                  << "... on Dataset { "
                  << "urn "
                  << "name "
                  << "globalTags { "
                  << "tags { "
                  << "tag { "
                  << "name "
                  << "} "
                  << "} "
                  << "} "
                  << "} "
                  << "} "
                  << "} "
                  << "} "
                  << "}\""
                  << "}";

    std::string payload = graphql_query.str();

    // Create HTTP session to DataHub GraphQL API (port 9002)
    Poco::Net::HTTPClientSession session("localhost", 9002);
    session.setTimeout(Poco::Timespan(10, 0));  // 10 second timeout

    Poco::Net::HTTPRequest request(
        Poco::Net::HTTPRequest::HTTP_POST,
        "/api/v2/graphql",
        Poco::Net::HTTPMessage::HTTP_1_1);

    request.setContentType("application/json");
    request.setContentLength(payload.length());

    // Add Authorization header if API key is available
    if (!api_key.empty()) {
      request.set("Authorization", "Bearer " + api_key);
    }

    // Send request
    std::ostream& request_stream = session.sendRequest(request);
    request_stream << payload;

    // Receive response
    Poco::Net::HTTPResponse response;
    std::istream& response_stream = session.receiveResponse(response);

    int status = response.getStatus();

    // Read response body
    std::string response_body;
    Poco::StreamCopier::copyToString(response_stream, response_body);

    if (status == Poco::Net::HTTPResponse::HTTP_OK) {
      if (!quiet_) {
        std::cout << "done" << std::endl;
      }

      // Parse JSON response to extract dataset names and tags
      // Expected format has searchResults array with entity.name and entity.globalTags.tags

      // Extract dataset names - look for "name" field after "entity"
      size_t search_pos = 0;
      while ((search_pos = response_body.find("\"name\":\"", search_pos)) != std::string::npos) {
        // Check if this is a dataset name (not a tag name)
        if (search_pos > 50 && response_body.substr(search_pos - 50, 50).find("\"entity\"") != std::string::npos) {
          search_pos += 8;  // Skip past "name":"
          size_t name_end = response_body.find("\"", search_pos);
          if (name_end != std::string::npos) {
            std::string dataset_name = response_body.substr(search_pos, name_end - search_pos);

            // Now find tags for this dataset by looking ahead
            std::string tags_str;
            size_t tag_search_start = name_end;
            size_t tag_search_end = response_body.find("\"entity\"", tag_search_start);
            if (tag_search_end == std::string::npos) {
              tag_search_end = response_body.length();
            }

            std::string section = response_body.substr(tag_search_start, tag_search_end - tag_search_start);
            size_t tag_pos = 0;
            while ((tag_pos = section.find("\"name\":\"urn:li:tag:", tag_pos)) != std::string::npos) {
              tag_pos += 19;  // Skip past "name":"urn:li:tag:
              size_t tag_end = section.find("\"", tag_pos);
              if (tag_end != std::string::npos) {
                std::string tag = section.substr(tag_pos, tag_end - tag_pos);
                if (!tags_str.empty()) {
                  tags_str += ",";
                }
                tags_str += tag;
                tag_pos = tag_end;
              } else {
                break;
              }
            }

            results.push_back(std::make_pair(dataset_name, tags_str));
            search_pos = tag_search_end;
          } else {
            break;
          }
        } else {
          search_pos += 8;
        }
      }

      if (!quiet_) {
        std::cout << "Found " << results.size() << " datasets in DataHub" << std::endl;
      }

    } else {
      std::cerr << "Error: DataHub GraphQL API returned status " << status << std::endl;
      std::cerr << "Response: " << response_body << std::endl;
    }

  } catch (const Poco::Net::NetException& e) {
    // Silently fail - caller will handle empty results with fallback to local metadata
  } catch (const Poco::Exception& e) {
    // Silently fail - caller will handle empty results with fallback to local metadata
  } catch (const std::exception& e) {
    // Silently fail - caller will handle empty results with fallback to local metadata
  }

  return results;
}
#endif // USE_DATAHUB

#ifdef USE_HERMES
int OMNI::PutHermesTags(hermes::Context* ctx, hermes::Bucket* bkt,
                        const std::string& tags) {
  hermes::Blob blob(tags.size());
  memcpy(blob.data(), tags.c_str(), blob.size());
  hermes::BlobId blob_id = bkt->Put("metadata", blob, *ctx);
  if (blob_id.IsNull()) {
    std::cerr << "Error: putting tags into metadata BLOB failed" << std::endl;
    return -1;
  }
  return 0;
}

int OMNI::PutHermes(const std::string& name, const std::string& tags,
                    const std::string& path, unsigned char* buffer,
                    size_t nbyte) {
  hermes::Context ctx;
  hermes::Bucket bkt(name);
  hermes::Blob blob(nbyte);
  memcpy(blob.data(), buffer, blob.size());
  hermes::BlobId blob_id = bkt.Put(path, blob, ctx);
  if (!quiet_) {
    std::cout << "wrote '" << buffer << "' to '" << name << "' buffer."
              << std::endl;
  }
  return PutHermesTags(&ctx, &bkt, tags);
}

int OMNI::GetHermes(const std::string& name, const std::string& path) {
  auto bkt = HERMES->GetBucket(name);
  hermes::BlobId blob_id = bkt.GetBlobId(path);
  hermes::Blob blob2;
  hermes::Context ctx;
  bkt.Get(blob_id, blob2, ctx);
  if (!quiet_) {
    std::cout << "read '" << blob2.data() << "' from '" << name << "' buffer."
              << std::endl;
  }

  // Create a stageable bucket
  hermes::Context ctx_s = hermes::BinaryFileStager::BuildContext(30);
  hermes::Bucket bkt_s(name, ctx_s, 30);
  hermes::Blob blob3(30);
  memcpy(blob3.data(), blob2.data(), blob2.size());
  bkt_s.Put(path, blob3, ctx_s);
  CHI_ADMIN->Flush(HSHM_MCTX, chi::DomainQuery::GetGlobalBcast());
  return 0;
}
#endif

#ifdef USE_MEMCACHED
// Simple memcached client implementation
class MemcachedClient {
private:
#ifdef _WIN32
    SOCKET sockfd;
#else
    int sockfd;
#endif
    std::string host;
    int port;
    
public:
    MemcachedClient(const std::string& host = "localhost", int port = 11211) 
        : sockfd(-1), host(host), port(port) {
#ifdef _WIN32
        WSADATA wsaData;
        WSAStartup(MAKEWORD(2, 2), &wsaData);
#endif
    }
    
    ~MemcachedClient() {
        if (sockfd != -1) {
#ifdef _WIN32
            closesocket(sockfd);
            WSACleanup();
#else
            close(sockfd);
#endif
        }
    }
    
    bool connect() {
#ifdef _WIN32
        sockfd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
        if (sockfd == INVALID_SOCKET) {
            return false;
        }
#else
        sockfd = socket(AF_INET, SOCK_STREAM, 0);
        if (sockfd < 0) {
            return false;
        }
#endif
        
        struct sockaddr_in server_addr;
        server_addr.sin_family = AF_INET;
        server_addr.sin_port = htons(port);
        
#ifdef _WIN32
        if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
            // Try to resolve hostname
            struct addrinfo hints, *result;
            memset(&hints, 0, sizeof(hints));
            hints.ai_family = AF_INET;
            hints.ai_socktype = SOCK_STREAM;
            
            if (getaddrinfo(host.c_str(), nullptr, &hints, &result) != 0) {
                closesocket(sockfd);
                sockfd = INVALID_SOCKET;
                return false;
            }
            
            struct sockaddr_in* addr_in = (struct sockaddr_in*)result->ai_addr;
            server_addr.sin_addr = addr_in->sin_addr;
            freeaddrinfo(result);
        }
        
        if (::connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) == SOCKET_ERROR) {
            closesocket(sockfd);
            sockfd = INVALID_SOCKET;
            return false;
        }
#else
        if (inet_pton(AF_INET, host.c_str(), &server_addr.sin_addr) <= 0) {
            // Try to resolve hostname
            struct hostent* he = gethostbyname(host.c_str());
            if (he == nullptr) {
                close(sockfd);
                sockfd = -1;
                return false;
            }
            memcpy(&server_addr.sin_addr, he->h_addr_list[0], he->h_length);
        }
        
        if (::connect(sockfd, (struct sockaddr*)&server_addr, sizeof(server_addr)) < 0) {
            close(sockfd);
            sockfd = -1;
            return false;
        }
#endif
        
        return true;
    }
    
    bool set(const std::string& key, const std::string& value, int expiration = 0) {
#ifdef _WIN32
        if (sockfd == INVALID_SOCKET) {
            return false;
        }
#else
        if (sockfd < 0) {
            return false;
        }
#endif
        
        std::string command = "set " + key + " 0 " + std::to_string(expiration) + " " + 
                             std::to_string(value.length()) + "\r\n" + value + "\r\n";
        
#ifdef _WIN32
        int sent = send(sockfd, command.c_str(), static_cast<int>(command.length()), 0);
        if (sent == SOCKET_ERROR) {
            return false;
        }
#else
        ssize_t sent = send(sockfd, command.c_str(), command.length(), 0);
        if (sent < 0) {
            return false;
        }
#endif
        
        char response[256];
#ifdef _WIN32
        int received = recv(sockfd, response, sizeof(response) - 1, 0);
        if (received == SOCKET_ERROR) {
            return false;
        }
#else
        ssize_t received = recv(sockfd, response, sizeof(response) - 1, 0);
        if (received < 0) {
            return false;
        }
#endif
        response[received] = '\0';
        
        return (strncmp(response, "STORED", 6) == 0);
    }
    
    bool get(const std::string& key, std::string& value) {
#ifdef _WIN32
        if (sockfd == INVALID_SOCKET) {
            return false;
        }
#else
        if (sockfd < 0) {
            return false;
        }
#endif
        
        std::string command = "get " + key + "\r\n";
        
#ifdef _WIN32
        int sent = send(sockfd, command.c_str(), static_cast<int>(command.length()), 0);
        if (sent == SOCKET_ERROR) {
            return false;
        }
#else
        ssize_t sent = send(sockfd, command.c_str(), command.length(), 0);
        if (sent < 0) {
            return false;
        }
#endif
        
        char response[4096];
#ifdef _WIN32
        int received = recv(sockfd, response, sizeof(response) - 1, 0);
        if (received == SOCKET_ERROR) {
            return false;
        }
#else
        ssize_t received = recv(sockfd, response, sizeof(response) - 1, 0);
        if (received < 0) {
            return false;
        }
#endif
        response[received] = '\0';
        
        // Parse response: VALUE key flags size\r\nvalue\r\nEND\r\n
        std::string resp_str(response);
        size_t value_start = resp_str.find("\r\n");
        if (value_start == std::string::npos) {
            return false;
        }
        value_start += 2;
        
        size_t value_end = resp_str.find("\r\n", value_start);
        if (value_end == std::string::npos) {
            return false;
        }
        
        value = resp_str.substr(value_start, value_end - value_start);
        return true;
    }
};
#endif

int OMNI::PutData(const std::string& name, const std::string& tags,
                  const std::string& path, unsigned char* buffer,
                  size_t nbyte) {
#ifdef USE_HERMES
  // Try to use Hermes if available, but don't fail if it's not running
  try {
    if (chi::chiClient != nullptr) {
      PutHermes(name, tags, path, buffer, nbyte);
    }
  } catch (...) {
    // Hermes not available, fall through to other storage backends
    if (!quiet_) {
      std::cout << "Hermes not available, using alternative storage" << std::endl;
    }
  }
#endif
#ifdef USE_POCO
  const std::size_t shared_memory_size = nbyte + 1;

  try {
    Poco::File file(name);
    if (!quiet_) {
      std::cout << "checking existing buffer '" << name << "'...";
    }
    if (file.exists()) {
      if (!quiet_) {
        std::cout << "yes" << std::endl;
      }
      // Still write metadata even if buffer exists
      return WriteMeta(name, tags);
    }
    if (!quiet_) {
      std::cout << "no" << std::endl;
    }

    if (!quiet_) {
      std::cout << "creating a new buffer '" << name << "' with '" << tags
                << "' tags...";
    }
    file.createFile();
    std::ofstream ofs(name, std::ios::binary);
    ofs.seekp(nbyte);
    ofs.put('\0');
    ofs.close();
    if (!quiet_) {
      std::cout << "done" << std::endl;
    }

    if (!quiet_) {
      std::cout << "putting " << nbyte << " bytes into '" << name
                << "' buffer...";
    }

#ifdef USE_MEMCACHED
    // Try Memcached first if available
    try {
      MemcachedClient memcached_client("localhost", 11211);
      if (memcached_client.connect()) {
        std::string data_str((const char*)buffer, nbyte);
        if (memcached_client.set(name, data_str)) {
          if (!quiet_) {
            std::cout << "done (Memcached)" << std::endl;
          }
#ifndef NDEBUG
          if (!quiet_) {
            std::cout << "wrote data to Memcached key '" << name << "'" << std::endl;
          }
#endif
          return WriteMeta(name, tags);
        } else {
          throw std::runtime_error("Memcached SET command failed");
        }
      } else {
        throw std::runtime_error("Failed to connect to Memcached server");
      }
    } catch (const std::exception& e) {
      if (!quiet_) {
        std::cout << "Memcached error: " << e.what() << ", falling back to other storage..." << std::endl;
      }
      // Fall through to other storage backends
    }
#endif

#ifdef USE_REDIS
    // Try Redis if available
    try {
      Poco::Redis::Client redis_client("localhost", 6379);
      Poco::Redis::Command set_cmd("SET");
      set_cmd << name << std::string((const char*)buffer, nbyte);
      
      std::string result = redis_client.execute<std::string>(set_cmd);
      if (result == "OK") {
        if (!quiet_) {
          std::cout << "done (Redis)" << std::endl;
        }
#ifndef NDEBUG
        if (!quiet_) {
          std::cout << "wrote data to Redis key '" << name << "'" << std::endl;
        }
#endif
        return WriteMeta(name, tags);
      } else {
        throw Poco::Exception("Redis SET command failed");
      }
    } catch (const Poco::Exception& e) {
      if (!quiet_) {
        std::cout << "Redis error: " << e.displayText() << ", falling back to SharedMemory..." << std::endl;
      }
      // Fall through to SharedMemory
    } catch (const std::exception& e) {
      if (!quiet_) {
        std::cout << "Redis error: " << e.what() << ", falling back to SharedMemory..." << std::endl;
      }
      // Fall through to SharedMemory
    }
#endif

    // Fallback to SharedMemory (original implementation)
#ifdef __OpenBSD__
    // OpenBSD: Poco::SharedMemory is unreliable; write directly to file
    {
      std::ofstream ofs_data(name, std::ios::binary | std::ios::trunc);
      ofs_data.write(reinterpret_cast<const char*>(buffer), nbyte);
      ofs_data.close();
      if (!quiet_) {
        std::cout << "done (direct file)" << std::endl;
      }
    }
#else
    Poco::SharedMemory shm(file, Poco::SharedMemory::AM_WRITE);

    char* data = static_cast<char*>(shm.begin());
    std::memcpy(data, (const char*)buffer, nbyte);
    if (!quiet_) {
      std::cout << "done (SharedMemory)" << std::endl;
    }
#ifndef NDEBUG
    if (!quiet_) {
      std::cout << "wrote '" << shm.begin() << "' to '" << name << "' buffer."
                << std::endl;
    }
#endif
#endif  // __OpenBSD__

  } catch (Poco::Exception& e) {
    std::cerr << "Poco Exception: " << e.displayText() << std::endl;
    return -1;
  } catch (std::exception& e) {
    std::cerr << "Standard Exception: " << e.what() << std::endl;
    return -1;
  }
#endif
  return WriteMeta(name, tags);
}

#ifdef _WIN32
std::string OMNI::GetExt(const std::string& filename) {
  std::filesystem::path p(filename);
  std::string extension = p.extension().string();
  for (char& c : extension) {
    c = std::tolower(c);
  }
  return extension;
}
#endif

int OMNI::RunLambda(const std::string& lambda, const std::string& name,
                    const std::string& dest) {
#ifdef _WIN32
  std::string extension = GetExt(lambda);
  if (extension != ".bat") {
    std::cerr << "Error: lambda script extension must be '.bat' on Windows"
              << std::endl;
    return 1;
  }
#endif

#ifdef USE_POCO
  try {
#ifdef _WIN32
    std::string command = "cmd.exe";
#else
    std::string command = lambda;
#endif
    Poco::Process::Args args;
#ifdef _WIN32
    args.push_back("/C");
    Poco::Path poco_path(lambda);
    std::string wpath = poco_path.toString(Poco::Path::PATH_WINDOWS);
    args.push_back(wpath);
#endif
    args.push_back(name);
    args.push_back(dest);

    Poco::Pipe out_pipe;
    Poco::Pipe err_pipe;
    Poco::ProcessHandle ph =
        Poco::Process::launch(command, args, 0, &out_pipe, &err_pipe);

    Poco::PipeInputStream istr(out_pipe);
    Poco::PipeInputStream estr(err_pipe);

    std::string stdout_output;
    std::string stderr_output;

    Poco::StreamCopier::copyToString(istr, stdout_output);
    Poco::StreamCopier::copyToString(estr, stderr_output);

    int exit_code = ph.wait();

    if (!quiet_) {
      std::cout << "\n--- Lambda script output ---\n";
      std::cout << "STDOUT:\n" << stdout_output;
      std::cout << "STDERR:\n" << stderr_output;
      std::cout << "-----------------------------------\n";
      std::cout << "Lambda script exited with status: " << exit_code << std::endl;
    }

    return exit_code;

  } catch (Poco::SystemException& exc) {
    std::cerr << "Error: poco system exception - " << exc.displayText()
              << std::endl;
    return 1;
  } catch (Poco::Exception& exc) {
    std::cerr << "Error: poco exception - " << exc.displayText() << std::endl;
    return 1;
  } catch (std::exception& exc) {
    std::cerr << "Error: standard exception - " << exc.what() << std::endl;
    return 1;
  }
#endif
  return 0;
}

std::string OMNI::GetFileName(const std::string& uri) {
  size_t last_slash_pos = uri.find_last_of('/');
  if (last_slash_pos == std::string::npos) {
    size_t protocol_end = uri.find("://");
    if (protocol_end != std::string::npos) {
      return "";
    }
    std::string filename = uri;
    size_t query_pos = filename.find('?');
    if (query_pos != std::string::npos) {
      filename = filename.substr(0, query_pos);
    }
    size_t fragment_pos = filename.find('#');
    if (fragment_pos != std::string::npos) {
      filename = filename.substr(0, fragment_pos);
    }
    return filename;
  }

  std::string filename_with_params = uri.substr(last_slash_pos + 1);
  size_t query_pos = filename_with_params.find('?');
  if (query_pos != std::string::npos) {
    filename_with_params = filename_with_params.substr(0, query_pos);
  }
  size_t fragment_pos = filename_with_params.find('#');
  if (fragment_pos != std::string::npos) {
    filename_with_params = filename_with_params.substr(0, fragment_pos);
  }
  return filename_with_params;
}

#ifdef USE_AWS
int OMNI::WriteS3(const std::string& dest, char* ptr) {
  // Note: AWS SDK InitAPI/ShutdownAPI are now managed globally in wrp.cc main()
  // to avoid multiple init/shutdown cycles that can cause segfaults

  const Aws::String prefix = "s3://";
  if (dest.find(prefix) != 0) {
    std::cerr << "Error: not a valid S3 URL (missing 's3://' prefix)"
              << std::endl;
    return -1;
  }
  Aws::String path = dest.substr(prefix.length());
  // Find the first '/' to separate bucket and key
  size_t first_slash = path.find('/');
  if (first_slash == Aws::String::npos) {
    std::cerr << "Error: invalid S3 URI (no path separator found)" << std::endl;
    return -1;
  }

  Aws::String bucket_name = path.substr(0, first_slash);
  Aws::String object_key = path.substr(first_slash + 1);

  if (bucket_name.empty()) {
    std::cerr << "Error: bucket name is empty" << std::endl;
    return -1;
  }
  if (object_key.empty()) {
    std::cerr << "Error: object key is empty" << std::endl;
    return -1;
  }

  // Read AWS config to get endpoint_url if available
  AWSConfig aws_config = ReadAWSConfig();

  // Use S3-specific client configuration which supports S3 Express options
  Aws::S3::S3ClientConfiguration client_config;

  // Parse and use endpoint_url from config if available
  std::string endpoint = "localhost:4566";  // default
  Aws::Http::Scheme scheme = Aws::Http::Scheme::HTTP;  // default
  std::string region = "us-east-1";  // default

  if (!aws_config.endpoint_url.empty()) {
    std::string url = aws_config.endpoint_url;

    // Parse scheme (http:// or https://)
    if (url.find("https://") == 0) {
      scheme = Aws::Http::Scheme::HTTPS;
      url = url.substr(8);  // Remove "https://"
    } else if (url.find("http://") == 0) {
      scheme = Aws::Http::Scheme::HTTP;
      url = url.substr(7);  // Remove "http://"
    }

    // Remove trailing slash if present
    if (!url.empty() && url[url.length() - 1] == '/') {
      url = url.substr(0, url.length() - 1);
    }

    endpoint = url;
  }

  if (!aws_config.region.empty()) {
    region = aws_config.region;
  }

  client_config.endpointOverride = endpoint.c_str();
  client_config.scheme = scheme;
  client_config.region = region.c_str();
  client_config.verifySSL = true;  // Keep SSL verification enabled for security
  client_config.useDualStack = false;

  // Disable S3 Express authentication which uses aws-chunked encoding
  // This is critical for S3-compatible services like Synology C2 that don't support aws-chunked
  client_config.disableS3ExpressAuth = true;
  client_config.useVirtualAddressing = false;  // Use path-style for S3-compatible services
  client_config.payloadSigningPolicy = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;

  // For S3-compatible services like Synology, use path-style addressing
  client_config.enableEndpointDiscovery = false;
  client_config.enableTcpKeepAlive = true;
  client_config.requestTimeoutMs = 30000;
  client_config.connectTimeoutMs = 10000;

  // Disable request compression to prevent chunked transfer encoding
  // This is critical for S3-compatible services that don't support aws-chunked encoding
  client_config.requestCompressionConfig.useRequestCompression = Aws::Client::UseRequestCompression::DISABLE;

  // Disable Expect: 100-continue header to avoid chunked encoding issues
  client_config.disableExpectHeader = true;

  // Set HTTP client factory based on platform
#ifdef _WIN32
  client_config.httpLibOverride = Aws::Http::TransferLibType::WIN_HTTP_CLIENT;
#else
  // Try using CRT HTTP client which handles chunked encoding better
  // Fall back to CURL if CRT is not available
  client_config.httpLibOverride = Aws::Http::TransferLibType::DEFAULT_CLIENT;
#endif

  if (!quiet_) {
    std::cout << "S3 Configuration:" << std::endl;
    std::cout << "  Endpoint: " << endpoint << std::endl;
    std::cout << "  Region: " << region << std::endl;
    std::cout << "  Scheme: " << (scheme == Aws::Http::Scheme::HTTPS ? "HTTPS" : "HTTP") << std::endl;
  }

  // Read credentials explicitly from ~/.aws/credentials
  auto [access_key, secret_key] = ReadAWSCredentials();
  if (access_key.empty() || secret_key.empty()) {
    std::cerr << "Error: AWS credentials not found in ~/.aws/credentials" << std::endl;
    return -1;
  }

  if (!quiet_) {
    std::cout << "Using AWS credentials from ~/.aws/credentials" << std::endl;
    std::cout << "  Access Key ID: " << access_key.substr(0, 8) << "..." << std::endl;
  }

  Aws::Auth::AWSCredentials credentials(access_key.c_str(), secret_key.c_str());

  // Create S3 client with the configuration that disables S3 Express (aws-chunked encoding)
  // Use constructor: S3Client(credentials, endpointProvider, clientConfiguration)
  Aws::S3::S3Client s3_client(credentials, nullptr, client_config);
  Aws::S3::Model::CreateBucketRequest create_bucket_request;
  create_bucket_request.SetBucket(bucket_name);

  if (!quiet_) {
    std::cout << "Creating bucket: " << bucket_name << std::endl;
  }
  auto create_bucket_outcome = s3_client.CreateBucket(create_bucket_request);
  if (create_bucket_outcome.IsSuccess()) {
    if (!quiet_) {
      std::cout << "Bucket created successfully!" << std::endl;
    }
  } else {
    auto error = create_bucket_outcome.GetError();
    // Bucket creation might fail if it already exists or user lacks CreateBucket permission
    // This is common with S3-compatible services, so just warn and continue
    if (!quiet_) {
      std::cerr << "Warning: Could not create bucket (may already exist): "
                << error.GetMessage() << std::endl;
      std::cerr << "  Exception: " << error.GetExceptionName() << std::endl;
      std::cerr << "Attempting to upload object anyway..." << std::endl;
    }
  }

  // Create a PutObject request
  Aws::S3::Model::PutObjectRequest put_request;
  put_request.SetBucket(bucket_name);
  put_request.SetKey(object_key);

  std::shared_ptr<Aws::IOStream> input_data;
  size_t content_length = 0;

  // We need to keep the buffer alive for the duration of the upload
  std::shared_ptr<std::vector<unsigned char>> buffer_ptr;

  if (ptr == NULL) {
    std::string file_path = GetFileName(dest);
    // Read entire file into memory to avoid streaming issues with chunked encoding
    std::ifstream file(file_path, std::ios::binary | std::ios::ate);
    if (!file.is_open()) {
      std::cerr << "Error: Unable to open file " << file_path << std::endl;
      return -1;
    }

    content_length = file.tellg();
    file.seekg(0, std::ios::beg);

    // Allocate buffer and read file
    buffer_ptr = Aws::MakeShared<std::vector<unsigned char>>("PutObjectBuffer", content_length);
    if (!file.read(reinterpret_cast<char*>(buffer_ptr->data()), content_length)) {
      std::cerr << "Error: Unable to read file " << file_path << std::endl;
      return -1;
    }
    file.close();

    // Create stream from buffer using string constructor to avoid chunked encoding
    // This ensures the stream has a known, fixed size
    std::string data_string(reinterpret_cast<const char*>(buffer_ptr->data()), content_length);
    input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", data_string);
  } else {
    content_length = std::strlen(ptr);
    // Create stream from string to ensure fixed size and avoid chunked encoding
    std::string data_string(ptr, content_length);
    input_data = Aws::MakeShared<Aws::StringStream>("PutObjectInputStream", data_string);
  }

  // Set content length BEFORE setting body to ensure SDK doesn't use chunked encoding
  put_request.SetContentLength(content_length);
  put_request.SetBody(input_data);

  // TODO: Set content type based on mime input in OMNI.
  // put_request.SetContentType("text/plain");
  put_request.SetContentType("application/octet-stream");

  // Disable checksum calculation to prevent aws-chunked encoding
  // S3-compatible services like Synology C2 don't support aws-chunked
  put_request.SetChecksumAlgorithm(Aws::S3::Model::ChecksumAlgorithm::NOT_SET);

  // Note: We don't set Content-MD5 header to avoid compatibility issues
  // with some S3-compatible services. The AWS SDK will handle integrity checking.

  // Execute the PutObject request
  auto put_object_outcome = s3_client.PutObject(put_request);

  if (put_object_outcome.IsSuccess()) {
    if (!quiet_) {
      std::cout << "Successfully uploaded object to " << bucket_name << "/"
                << object_key << std::endl;
    }
  } else {
    auto error = put_object_outcome.GetError();
    if (!quiet_) {
      std::cout << "Error: uploading object - " << error.GetMessage() << std::endl;
      std::cout << "Error code: " << static_cast<int>(error.GetErrorType()) << std::endl;
      std::cout << "Exception name: " << error.GetExceptionName() << std::endl;
    }
    return -1;
  }
  // Note: AWS SDK ShutdownAPI is called automatically at program exit via atexit()
  return 0;
}
#elif defined(USE_POCO)
// POCO-based S3 upload with manual AWS SigV4 signing
// This is used when AWS SDK is not available (e.g., on Linux to avoid chunked encoding issues)
int OMNI::WriteS3(const std::string& dest, char* ptr) {
  const std::string prefix = "s3://";
  if (dest.find(prefix) != 0) {
    std::cerr << "Error: not a valid S3 URL (missing 's3://' prefix)" << std::endl;
    return -1;
  }

  std::string path = dest.substr(prefix.length());
  size_t first_slash = path.find('/');
  if (first_slash == std::string::npos) {
    std::cerr << "Error: invalid S3 URI (no path separator found)" << std::endl;
    return -1;
  }

  std::string bucket_name = path.substr(0, first_slash);
  std::string object_key = path.substr(first_slash + 1);

  if (bucket_name.empty()) {
    std::cerr << "Error: bucket name is empty" << std::endl;
    return -1;
  }
  if (object_key.empty()) {
    std::cerr << "Error: object key is empty" << std::endl;
    return -1;
  }

  // Read AWS config
  AWSConfig aws_config = ReadAWSConfig();
  auto [access_key, secret_key] = ReadAWSCredentials();

  if (access_key.empty() || secret_key.empty()) {
    std::cerr << "Error: AWS credentials not found in ~/.aws/credentials" << std::endl;
    return -1;
  }

  // Parse endpoint URL
  std::string endpoint = "localhost:4566";
  std::string scheme = "http";
  std::string region = aws_config.region.empty() ? "us-east-1" : aws_config.region;
  int port = 0;

  if (!aws_config.endpoint_url.empty()) {
    std::string url = aws_config.endpoint_url;
    if (url.find("https://") == 0) {
      scheme = "https";
      url = url.substr(8);
      port = 443;
    } else if (url.find("http://") == 0) {
      scheme = "http";
      url = url.substr(7);
      port = 80;
    }

    if (!url.empty() && url[url.length() - 1] == '/') {
      url = url.substr(0, url.length() - 1);
    }

    size_t port_pos = url.find(':');
    if (port_pos != std::string::npos) {
      endpoint = url.substr(0, port_pos);
      port = std::stoi(url.substr(port_pos + 1));
    } else {
      endpoint = url;
    }
  }

  // Read file content
  std::string content;
  size_t content_length = 0;

  if (ptr == NULL) {
    std::string file_path = GetFileName(dest);
    std::ifstream file(file_path, std::ios::binary);
    if (!file.is_open()) {
      std::cerr << "Error: Unable to open file " << file_path << std::endl;
      return -1;
    }
    content = std::string((std::istreambuf_iterator<char>(file)),
                          std::istreambuf_iterator<char>());
    file.close();
    content_length = content.length();
  } else {
    content = std::string(ptr);
    content_length = content.length();
  }

  // Compute SHA256 hash of payload
  std::string payload_hash = sha256_hex_impl(content);

  // Get current timestamp
  auto now = std::chrono::system_clock::now();
  auto time_t_now = std::chrono::system_clock::to_time_t(now);
  std::tm tm_now;
#ifdef _WIN32
  gmtime_s(&tm_now, &time_t_now);
#else
  gmtime_r(&time_t_now, &tm_now);
#endif

  char date_stamp[9];
  char amz_date[17];
  std::strftime(date_stamp, sizeof(date_stamp), "%Y%m%d", &tm_now);
  std::strftime(amz_date, sizeof(amz_date), "%Y%m%dT%H%M%SZ", &tm_now);

  // Create canonical request
  std::string http_method = "PUT";
  std::string canonical_uri = "/" + bucket_name + "/" + object_key;
  std::string canonical_querystring = "";
  std::string host = endpoint;

  std::ostringstream canonical_headers;
  canonical_headers << "content-length:" << content_length << "\n"
                    << "content-type:application/octet-stream\n"
                    << "host:" << host << "\n"
                    << "x-amz-content-sha256:" << payload_hash << "\n"
                    << "x-amz-date:" << amz_date << "\n";

  std::string signed_headers = "content-length;content-type;host;x-amz-content-sha256;x-amz-date";

  std::ostringstream canonical_request;
  canonical_request << http_method << "\n"
                    << canonical_uri << "\n"
                    << canonical_querystring << "\n"
                    << canonical_headers.str() << "\n"
                    << signed_headers << "\n"
                    << payload_hash;

  // Hash canonical request
  std::string canonical_request_hash = sha256_hex_impl(canonical_request.str());

  // Create string to sign
  std::string algorithm = "AWS4-HMAC-SHA256";
  std::string credential_scope = std::string(date_stamp) + "/" + region + "/s3/aws4_request";
  std::ostringstream string_to_sign;
  string_to_sign << algorithm << "\n"
                 << amz_date << "\n"
                 << credential_scope << "\n"
                 << canonical_request_hash;

  // Calculate signature using HMAC-SHA256
  std::string date_key_str = "AWS4" + secret_key;
  auto date_key = hmac_sha256_raw_impl(date_key_str, date_stamp);
  auto region_key = hmac_sha256_raw_impl(std::string(date_key.begin(), date_key.end()), region);
  auto service_key = hmac_sha256_raw_impl(std::string(region_key.begin(), region_key.end()), "s3");
  auto signing_key = hmac_sha256_raw_impl(std::string(service_key.begin(), service_key.end()), "aws4_request");
  std::string signature = hmac_sha256_hex_impl(std::string(signing_key.begin(), signing_key.end()), string_to_sign.str());

  // Build authorization header
  std::ostringstream authorization;
  authorization << algorithm << " Credential=" << access_key << "/" << credential_scope
                << ", SignedHeaders=" << signed_headers
                << ", Signature=" << signature;

  if (!quiet_) {
    std::cout << "S3 Configuration:" << std::endl;
    std::cout << "  Endpoint: " << endpoint << std::endl;
    std::cout << "  Region: " << region << std::endl;
    std::cout << "  Scheme: " << scheme << std::endl;
    std::cout << "Using AWS credentials from ~/.aws/credentials" << std::endl;
    std::cout << "  Access Key ID: " << access_key.substr(0, 8) << "..." << std::endl;
  }

  try {
    // Create HTTP session
    std::unique_ptr<Poco::Net::HTTPClientSession> session;
    if (scheme == "https") {
      Poco::Net::Context::Ptr context = new Poco::Net::Context(
          Poco::Net::Context::CLIENT_USE, "", "", "",
          Poco::Net::Context::VERIFY_NONE, 9, true);
      session = std::make_unique<Poco::Net::HTTPSClientSession>(endpoint, port == 0 ? 443 : port, context);
    } else {
      session = std::make_unique<Poco::Net::HTTPClientSession>(endpoint, port == 0 ? 80 : port);
    }

    // Try to create bucket first
    if (!quiet_) {
      std::cout << "Creating bucket: " << bucket_name << std::endl;
    }

    // Create bucket with AWS SigV4 signing
    std::string bucket_uri = "/" + bucket_name;

    // For regions other than us-east-1, we need CreateBucketConfiguration XML
    std::string bucket_payload;
    std::string bucket_payload_hash;

    if (region != "us-east-1") {
      std::ostringstream xml;
      xml << "<CreateBucketConfiguration xmlns=\"http://s3.amazonaws.com/doc/2006-03-01/\">"
          << "<LocationConstraint>" << region << "</LocationConstraint>"
          << "</CreateBucketConfiguration>";
      bucket_payload = xml.str();

      // Calculate SHA256 of the XML payload
      bucket_payload_hash = sha256_hex_impl(bucket_payload);
    } else {
      // For us-east-1, use empty payload
      bucket_payload_hash = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855";
    }

    // Create canonical request for CreateBucket
    std::ostringstream bucket_canonical_headers;
    bucket_canonical_headers << "host:" << host << "\n"
                              << "x-amz-content-sha256:" << bucket_payload_hash << "\n"
                              << "x-amz-date:" << amz_date << "\n";

    std::string bucket_signed_headers = "host;x-amz-content-sha256;x-amz-date";

    std::ostringstream bucket_canonical_request;
    bucket_canonical_request << "PUT\n"
                              << bucket_uri << "\n"
                              << "\n"  // empty query string
                              << bucket_canonical_headers.str() << "\n"
                              << bucket_signed_headers << "\n"
                              << bucket_payload_hash;

    // Hash canonical request
    std::string bucket_canonical_request_hash = sha256_hex_impl(bucket_canonical_request.str());

    // Create string to sign
    std::ostringstream bucket_string_to_sign;
    bucket_string_to_sign << algorithm << "\n"
                          << amz_date << "\n"
                          << credential_scope << "\n"
                          << bucket_canonical_request_hash;

    // Calculate signature (reuse the signing key from earlier)
    std::string bucket_signature = hmac_sha256_hex_impl(std::string(signing_key.begin(), signing_key.end()), bucket_string_to_sign.str());

    // Build authorization header
    std::ostringstream bucket_authorization;
    bucket_authorization << algorithm << " Credential=" << access_key << "/" << credential_scope
                         << ", SignedHeaders=" << bucket_signed_headers
                         << ", Signature=" << bucket_signature;

    // Send CreateBucket request
    Poco::Net::HTTPRequest bucket_request(Poco::Net::HTTPRequest::HTTP_PUT, bucket_uri);
    bucket_request.setContentLength(bucket_payload.length());
    bucket_request.set("Host", host);
    bucket_request.set("x-amz-date", amz_date);
    bucket_request.set("x-amz-content-sha256", bucket_payload_hash);
    bucket_request.set("Authorization", bucket_authorization.str());

    std::ostream& bucket_os = session->sendRequest(bucket_request);
    if (!bucket_payload.empty()) {
      bucket_os << bucket_payload;
    }
    bucket_os.flush();

    // Get bucket creation response
    Poco::Net::HTTPResponse bucket_response;
    std::istream& bucket_rs = session->receiveResponse(bucket_response);

    if (bucket_response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK ||
        bucket_response.getStatus() == Poco::Net::HTTPResponse::HTTP_NO_CONTENT) {
      if (!quiet_) {
        std::cout << "Bucket created successfully!" << std::endl;
      }
    } else if (bucket_response.getStatus() == Poco::Net::HTTPResponse::HTTP_CONFLICT ||
               bucket_response.getStatus() == Poco::Net::HTTPResponse::HTTP_FORBIDDEN) {
      // Bucket already exists or no permission - continue anyway
      if (!quiet_) {
        std::string response_body;
        Poco::StreamCopier::copyToString(bucket_rs, response_body);
        std::cerr << "Warning: Could not create bucket (may already exist): "
                  << bucket_response.getReason() << std::endl;
        std::cerr << "Attempting to upload object anyway..." << std::endl;
      }
    } else {
      // Other error - log but continue
      if (!quiet_) {
        std::string response_body;
        Poco::StreamCopier::copyToString(bucket_rs, response_body);
        std::cerr << "Warning: Bucket creation returned status " << bucket_response.getStatus()
                  << " " << bucket_response.getReason() << std::endl;
      }
    }

    // Recreate session for PutObject (HTTP connection may be closed after bucket creation)
    if (scheme == "https") {
      Poco::Net::Context::Ptr context = new Poco::Net::Context(
          Poco::Net::Context::CLIENT_USE, "", "", "",
          Poco::Net::Context::VERIFY_NONE, 9, true);
      session = std::make_unique<Poco::Net::HTTPSClientSession>(endpoint, port == 0 ? 443 : port, context);
    } else {
      session = std::make_unique<Poco::Net::HTTPClientSession>(endpoint, port == 0 ? 80 : port);
    }

    // Create PutObject request
    Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_PUT, canonical_uri);
    request.setContentLength(content_length);
    request.setContentType("application/octet-stream");
    request.set("Host", host);
    request.set("x-amz-date", amz_date);
    request.set("x-amz-content-sha256", payload_hash);
    request.set("Authorization", authorization.str());

    // Send request
    std::ostream& os = session->sendRequest(request);
    os << content;

    // Get response
    Poco::Net::HTTPResponse response;
    std::istream& rs = session->receiveResponse(response);

    if (response.getStatus() == Poco::Net::HTTPResponse::HTTP_OK ||
        response.getStatus() == Poco::Net::HTTPResponse::HTTP_NO_CONTENT) {
      if (!quiet_) {
        std::cout << "Successfully uploaded object to " << bucket_name << "/" << object_key << std::endl;
      }
      return 0;
    } else {
      std::string response_body;
      Poco::StreamCopier::copyToString(rs, response_body);
      std::cerr << "Error: S3 upload failed with status " << response.getStatus()
                << " " << response.getReason() << std::endl;
      if (!response_body.empty()) {
        std::cerr << "Response: " << response_body << std::endl;
      }
      return -1;
    }
  } catch (Poco::Exception& ex) {
    std::cerr << "Error: " << ex.displayText() << std::endl;
    return -1;
  }
}
#endif

#ifdef USE_POCO
int OMNI::Download(const std::string& url, const std::string& output_file_name,
                   long long start_byte, long long end_byte) {
  // Call the overloaded version with proxy config
  ProxyConfig proxy = ReadProxyConfig();
  return Download(url, output_file_name, start_byte, end_byte, proxy);
}

int OMNI::Download(const std::string& url, const std::string& output_file_name,
                   long long start_byte, long long end_byte, const ProxyConfig& proxy) {
  try {
    std::string current_url = url;
    const std::string ca_cert_file = "../cacert.pem";

    Poco::Net::Context::Ptr context = new Poco::Net::Context(
        Poco::Net::Context::CLIENT_USE, "", "", "",
        Poco::Net::Context::VERIFY_NONE,  // STRICT
        9, true, ca_cert_file);

    int redirect_count = 0;
    int max_redirects = 20;  // Match Chrome & Firefox
    bool redirected = true;

    while (redirected && redirect_count <= max_redirects) {
      redirected = false;
      redirect_count++;
      Poco::URI uri(current_url);

      std::unique_ptr<Poco::Net::HTTPClientSession> session;

      if (uri.getScheme() == "https") {
        session = std::make_unique<Poco::Net::HTTPSClientSession>(
            uri.getHost(), uri.getPort() == 0 ? 443 : uri.getPort(), context);
      } else {
        session = std::make_unique<Poco::Net::HTTPClientSession>(uri.getHost(),
                                                                  uri.getPort());
      }

      // Configure proxy if enabled
      if (proxy.enabled && !proxy.host.empty() && proxy.port > 0) {
        session->setProxyHost(proxy.host);
        session->setProxyPort(proxy.port);

        // Set proxy credentials if provided
        if (!proxy.username.empty()) {
          session->setProxyUsername(proxy.username);
          if (!proxy.password.empty()) {
            session->setProxyPassword(proxy.password);
          }
        }

        if (!quiet_) {
          std::cout << "Using proxy: " << proxy.host << ":" << proxy.port;
          if (!proxy.username.empty()) {
            std::cout << " (authenticated)";
          }
          std::cout << std::endl;
        }
      } else {
        // Disable proxy to avoid DNS resolution issues with system proxy
        session->setProxyHost("");
        session->setProxyPort(0);
      }

      Poco::Net::HTTPRequest request(Poco::Net::HTTPRequest::HTTP_GET,
                                     uri.getPathAndQuery(),
                                     Poco::Net::HTTPMessage::HTTP_1_1);
      request.set("User-Agent", "POCO HTTP Redirect Client/1.0");

      // Set the Range header
      if (start_byte >= 0) {
        std::string range_header_value;
        if (end_byte == -1) {  // Request bytes from start_byte to end of file
          range_header_value = "bytes=" + std::to_string(start_byte) + "-";
        } else {  // Request a specific range
          range_header_value = "bytes=" + std::to_string(start_byte) + "-" +
                               std::to_string(end_byte);
        }
        request.set("Range", range_header_value);
        if (!quiet_) {
          std::cout << "Requesting Range: " << range_header_value << std::endl;
        }
      }

      Poco::Net::HTTPResponse response;
      int status = 0;
      if (!quiet_) {
        std::cout << "Downloading from: " << url << std::endl;
      }

      session->sendRequest(request);
      std::istream& rs = session->receiveResponse(response);
      status = response.getStatus();
      if (!quiet_) {
        std::cout << "Status: " << status << " - " << response.getReason()
                  << std::endl;
      }

      if (status == Poco::Net::HTTPResponse::HTTP_MOVED_PERMANENTLY ||
          status == Poco::Net::HTTPResponse::HTTP_FOUND ||
          status == Poco::Net::HTTPResponse::HTTP_SEE_OTHER ||
          status == Poco::Net::HTTPResponse::HTTP_TEMPORARY_REDIRECT ||
#ifdef __OpenBSD__
          status == 308 /* HTTP_PERMANENT_REDIRECT - not in POCO 1.4.x */) {
#else
          status == Poco::Net::HTTPResponse::HTTP_PERMANENT_REDIRECT) {
#endif
        if (response.has("Location")) {
          current_url = response.get("Location");
          if (!quiet_) {
            std::cout << "Redirected to: " << current_url << std::endl;
          }
          redirected = true;
          // Consume any remaining data in the current response stream
          Poco::NullOutputStream null_stream;
          Poco::StreamCopier::copyStream(rs, null_stream);
        } else {
          std::cerr << "Redirect status (" << status
                    << ") received but no Location header found." << std::endl;
          return -1;
        }
      } else if (status == Poco::Net::HTTPResponse::HTTP_PARTIAL_CONTENT) {
        if (!quiet_) {
          std::cout << "Received Partial Content (206)." << std::endl;
        }
        if (response.has("Content-Range")) {
          std::string content_range = response.get("Content-Range");
          if (!quiet_) {
            std::cout << "Content-Range: " << content_range << std::endl;
          }
        } else {
          if (!quiet_) {
            std::cout << "Warning: 206 status but no Content-Range header."
                      << std::endl;
          }
        }

        std::ofstream os(output_file_name, std::ios::binary);
        if (os.is_open()) {
          Poco::StreamCopier::copyStream(rs, os);
          os.close();
          if (!quiet_) {
            std::cout << "Partial content downloaded successfully to: "
                      << output_file_name << std::endl;
          }
        } else {
          std::cerr << "Error: Could not open file for writing: "
                    << output_file_name << std::endl;
        }
      } else if (status == Poco::Net::HTTPResponse::HTTP_OK) {
        // Success! Download the content
        std::ofstream os(output_file_name, std::ios::binary);
        if (os.is_open()) {
          Poco::StreamCopier::copyStream(rs, os);
          os.close();
          if (!quiet_) {
            std::cout << "File downloaded successfully to: " << output_file_name
                      << std::endl;
          }
          return 0;
        } else {
          std::cerr << "Error: Could not open file for writing: "
                    << output_file_name << std::endl;
          return -1;
        }
      } else {
        // Non-success, non-redirect status
        std::cerr << "Error: HTTP request failed with status code " << status
                  << std::endl;
        std::string error_body;
        Poco::StreamCopier::copyToString(rs, error_body);
        std::cerr << "Response Body: " << error_body << std::endl;
        return -1;
      }
    }  // while

    if (redirect_count > max_redirects) {
      std::cerr << "Error: Maximum redirect limit (" << max_redirects
                << ") exceeded." << std::endl;
    }
  } catch (const Poco::Net::NetException& e) {
    std::cerr << "Network Error: " << e.displayText() << std::endl;
  } catch (const Poco::IOException& e) {
    std::cerr << "IO Error: " << e.displayText() << std::endl;
  } catch (const Poco::Exception& e) {
    std::cerr << "POCO Error: " << e.displayText() << std::endl;
  } catch (const std::exception& e) {
    std::cerr << "Standard Error: " << e.what() << std::endl;
  } catch (...) {
    std::cerr << "Unknown Error occurred." << std::endl;
  }
  return 0;
}
#endif

int OMNI::ReadOmni(const std::string& input_file) {
  std::string name;
  std::string tags;
  std::string path;
#ifdef USE_POCO
  std::string hash;
#endif
  size_t offset = 0;
  size_t nbyte = 0;
  bool run = false;
  bool f = true;
  bool wait_for_file = false;
  int res = -1;
  std::string lambda;
  std::string dest;

  // Read wait timeout configuration
  WaitConfig wait_config = ReadWaitConfig();

  std::ifstream ifs(input_file);
  if (!ifs.is_open()) {
    std::cerr << "Error: could not open file " << input_file << std::endl;
    return 1;
  }

  try {
    YAML::Node root = YAML::Load(ifs);

    if (root.IsMap()) {
      for (YAML::const_iterator it = root.begin(); it != root.end(); ++it) {
        std::string key = it->first.as<std::string>();

        if (key == "name") {
          name = it->second.as<std::string>();
        }
        if (key == "src") {
          std::string original_path = it->second.as<std::string>();

          // Check if src starts with '>' indicating we should wait for the file
          if (!original_path.empty() && original_path[0] == '>') {
            wait_for_file = true;
            path = original_path.substr(1);  // Remove the '>' prefix
            if (!quiet_) {
              std::cout << "Will wait for file: " << path.c_str() << std::endl;
            }
          } else {
            path = original_path;
          }

#ifndef NDEBUG
          if (!quiet_) {
            std::cout << "Path=" << path.c_str() << std::endl;
          }
#endif
          if (path.find("https://") == path.npos &&
              path.find("hdf5://") == path.npos
#ifdef USE_GLOBUS
              && path.find("globus://") == path.npos
#endif
          ) {
            // Don't wait for file here - wait when we know the expected byte count
            // Just check if file exists when not waiting
            if (!wait_for_file) {
#ifdef USE_POCO
              Poco::File file(path);
              if (!file.exists()) {
                std::cerr << "Error: '" << path << "' does not exist" << std::endl;
                return -1;
              }
#else
              if (!std::filesystem::exists(path)) {
                std::cerr << "Error: '" << path << "' does not exist" << std::endl;
                return -1;
              }
#endif
            }
          } else {
            f = false;
          }
        }
#ifdef USE_POCO
        if (key == "hash") {
          hash = it->second.as<std::string>();
        }
#endif

#ifdef USE_GLOBUS
        // Handle Globus transfer if source is a globus:// URL
        if (path.find("globus://") != path.npos && key == "dst") {
          std::string dest_uri = it->second.as<std::string>();
          if (dest_uri.find("globus://") == 0) {
            // Try environment variable first, then config file
            std::string transfer_token =
                std::getenv("GLOBUS_TRANSFER_TOKEN")
                    ? std::getenv("GLOBUS_TRANSFER_TOKEN")
                    : "";
            if (transfer_token.empty()) {
              transfer_token = ReadConfigValue("GLOBUS_TRANSFER_TOKEN");
            }
            if (transfer_token.empty()) {
              std::cerr
                  << "Error: GLOBUS_TRANSFER_TOKEN not found in environment or ~/.wrp/config"
                  << std::endl;
              return -1;
            }

            if (transfer_globus_file(path, dest_uri, transfer_token,
                                     "OMNI Transfer")) {
              if (!quiet_) {
                std::cout << "Globus transfer initiated successfully from " << path
                          << " to " << dest_uri << std::endl;
              }
              return 0;
            } else {
              std::cerr << "Error: Failed to initiate Globus transfer"
                        << std::endl;
              return -1;
            }
          }
        }
#endif  // USE_GLOBUS
        if (key == "offset") {
          offset = it->second.as<size_t>();
        }
        if (key == "nbyte") {
          nbyte = it->second.as<size_t>();

          // If wait_for_file is true, wait for the file to have all expected bytes
          if (wait_for_file && !path.empty() && f == true) {
            if (!quiet_) {
              std::cout << "Waiting for file '" << path
                        << "' to have " << nbyte << " bytes";
              if (wait_config.timeout_seconds > 0) {
                std::cout << " (timeout: " << wait_config.timeout_seconds << " seconds)";
              }
              std::cout << "..." << std::endl;
            }

            auto start_time = std::chrono::steady_clock::now();
            bool file_ready = false;

            while (!file_ready) {
              // Check if file exists and has enough bytes
#ifdef USE_POCO
              Poco::File file(path);
              if (file.exists() && file.getSize() >= static_cast<Poco::File::FileSize>(offset + nbyte)) {
                file_ready = true;
                break;
              }
#else
              if (std::filesystem::exists(path)) {
                auto file_size = std::filesystem::file_size(path);
                if (file_size >= offset + nbyte) {
                  file_ready = true;
                  break;
                }
              }
#endif

              // Check timeout if configured
              if (wait_config.timeout_seconds > 0) {
                auto elapsed = std::chrono::steady_clock::now() - start_time;
                auto elapsed_seconds = std::chrono::duration_cast<std::chrono::seconds>(elapsed).count();
                if (elapsed_seconds >= wait_config.timeout_seconds) {
                  std::cerr << "Error: Timeout waiting for file '" << path
                            << "' to have " << nbyte << " bytes after "
                            << elapsed_seconds << " seconds" << std::endl;
                  return -1;
                }
              }

              std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }

            if (!quiet_) {
              std::cout << "File '" << path
                        << "' is now ready with all " << nbyte << " bytes, continuing..." << std::endl;
            }
          }

          std::vector<char> buffer(nbyte + 1);  // Allocate extra byte for null terminator
          unsigned char* ptr = reinterpret_cast<unsigned char*>(buffer.data());
          if (!path.empty() && f == true) {
#ifndef NDEBUG
            if (!quiet_) {
              std::cout << "path=" << path << std::endl;
            }
#endif
            if (ReadExactBytesFromOffset(path.c_str(), offset, nbyte, ptr) ==
                0) {
              ptr[nbyte] = '\0';  // Null-terminate the buffer for safe printing
#ifndef NDEBUG
              if (!quiet_) {
                std::cout << "buffer=" << ptr << std::endl;
              }
#endif
              PutData(name, tags, path, ptr, nbyte);
            } else {
              return -1;
            }
          }
        }
        if (key == "run") {
          run = true;
          lambda = it->second.as<std::string>();
        }
        if (key == "dst") {
          dest = it->second.as<std::string>();
#ifndef _WIN32
#ifdef USE_HERMES
          // Only call GetHermes if Hermes client is initialized
          if (chi::chiClient != nullptr) {
            GetHermes(name, path);
          }
#endif
#endif
        }

        if (it->second.IsScalar()) {
          std::string value = it->second.as<std::string>();
#ifndef NDEBUG
          if (!quiet_) {
            std::cout << key << ": " << value << std::endl;
          }
#endif
        } else if (it->second.IsSequence()) {
#ifndef NDEBUG
          if (!quiet_) {
            std::cout << key << ": " << std::endl;
          }
#endif
          for (size_t i = 0; i < it->second.size(); ++i) {
            if (it->second[i].IsScalar()) {
#ifndef NDEBUG
              if (!quiet_) {
                std::cout << " - " << it->second[i].as<std::string>() << std::endl;
              }
#endif
              if (key == "tags") {
                tags += it->second[i].as<std::string>();
                if (i < it->second.size() - 1) {
                  tags += ",";
                }
              }
            }
          }
        } else if (it->second.IsMap()) {
#ifndef NDEBUG
          if (!quiet_) {
            std::cout << key << ": " << std::endl;
          }
#endif
          for (YAML::const_iterator inner_it = it->second.begin();
               inner_it != it->second.end(); ++inner_it) {
            std::string inner_key = inner_it->first.as<std::string>();
            if (inner_it->second.IsScalar()) {
              std::string inner_value = inner_it->second.as<std::string>();
#ifndef NDEBUG
              if (!quiet_) {
                std::cout << "  " << inner_key << ": " << inner_value << std::endl;
              }
#endif
            }
          }  // for
        }
      }  // for
    } else if (root.IsSequence()) {
      for (size_t i = 0; i < root.size(); ++i) {
        if (root[i].IsScalar()) {
          if (!quiet_) {
            std::cout << " - " << root[i].as<std::string>() << std::endl;
          }
        }
      }
    } else if (root.IsScalar()) {
      if (!quiet_) {
        std::cout << root.as<std::string>() << std::endl;
      }
    }
  } catch (YAML::ParserException& e) {
    std::cerr << "Error: parsing YAML - " << e.what() << std::endl;
    return 1;
  }

  // Validate required fields
  if (name.empty()) {
    std::cerr << "Error: 'name' field is required in OMNI YAML file" << std::endl;
    return 1;
  }
  if (tags.empty()) {
    std::cerr << "Error: 'tags' field is required in OMNI YAML file" << std::endl;
    return 1;
  }

#ifdef USE_DATAHUB
  // Check DataHub configuration and register if enabled
  if (CheckDataHubConfig()) {
    int datahub_result = RegisterWithDataHub(name, tags);
    if (datahub_result != 0) {
      std::cerr << "Warning: DataHub registration failed, continuing..." << std::endl;
      // Don't return error - just warn and continue
    }
  }
#endif

#if USE_HDF5
  if (path.find("hdf5://") != path.npos) {
    cae::DatasetConfig dc = cae::ParseDatasetConfig(input_file);
    cae::Hdf5DatasetClient client;

    // Read dataset and get the buffer
    size_t buffer_size = 0;
    unsigned char* buffer = client.ReadDataset(dc, buffer_size);

    if (buffer && buffer_size > 0) {
      // Use PutData to write the buffer
      int result =
          PutData(dc.name,      // name
                  tags,         // tags
                  "",           // path (empty for now)
                  buffer,       // buffer
                  buffer_size   // buffer size in bytes
          );

      if (result != 0) {
        std::cerr << "Error: Failed to write buffer using PutData()"
                  << std::endl;
      } else {
        if (!quiet_) {
          std::cout << "Successfully wrote buffer using PutData()" << std::endl;
        }
      }

      // Free the allocated buffer
      delete[] buffer;
    } else {
      std::cerr << "Error: Failed to read dataset or empty dataset"
                << std::endl;
    }
  }
#endif

#if USE_POCO
  if (!path.empty() && (path.find("https://") != path.npos)) {
    long long start = -1;
    long long end = -1;
    if (offset > 0) {
      start = (long long)offset;
    }
    if (nbyte > 0) {
      end = (long long)(offset + nbyte);
    }
    if (Download(path, name, start, end) != 0)
      std::cerr << "Error: downloading '" << path << "' failed " << std::endl;
  }

  if (!hash.empty()) {
    std::string h;
    if (!path.empty() && path.find("https://") != 0 &&
        path.find("hdf5://") != 0) {
      h = Sha256File(path);
    }
    if (path.find("https://") == 0) {
      h = Sha256File(name);
    }

    if (hash != h) {
      std::cerr << "Error: hash '" << hash << "' is not same as actual '" << h
                << "'" << std::endl;
      return -1;
    }
  }

  if (!dest.empty()) {
#ifndef NDEBUG
    if (!quiet_) {
      std::cerr << "dst=" << dest << std::endl;
    }
#endif

#ifdef USE_POCO
    try {
      if (run) {
        // Execute the lambda script first
        res = RunLambda(lambda, name, dest);

        // After script execution, process dst
        if (res == 0) {
          // Check if destination is an S3 URL
          bool is_s3_dest = (dest.find("s3://") == 0);

          if (is_s3_dest) {
            // S3 destination - upload the file
#if defined(USE_AWS) || defined(USE_POCO)
            WriteS3(dest, NULL);
#endif
          } else {
            // Non-S3 destination - check if file exists
            Poco::File dest_file(dest);
            if (dest_file.exists()) {
              if (!quiet_) {
                std::cout << "Destination file already exists: " << dest << std::endl;
              }
            } else {
              std::cerr << "Error: destination file does not exist and s3:// prefix not specified: " << dest << std::endl;
              return 1;
            }
          }
        } else {
          std::cerr << "Error: lambda failed to generate '" << dest << "'"
                    << std::endl;
        }
      } else {
        // Not using run - check dst before processing
        bool is_s3_dest = (dest.find("s3://") == 0);

        // If not S3, check if the destination file exists
        if (!is_s3_dest) {
          Poco::File dest_file(dest);
          if (dest_file.exists()) {
            if (!quiet_) {
              std::cout << "Destination file already exists: " << dest << std::endl;
            }
            return 0;  // Exit successfully
          } else {
            std::cerr << "Error: destination file does not exist and s3:// prefix not specified: " << dest << std::endl;
            return 1;  // Exit with error
          }
        }

        // S3 destination - proceed with data retrieval and upload
#if defined(USE_AWS) || defined(USE_POCO)
        Poco::File file(name);
        if (!file.exists()) {
          throw Poco::FileNotFoundException("Error: buffer '" + name +
                                            "' not found");
        }

#ifdef USE_MEMCACHED
        // Try Memcached first if available
        try {
          MemcachedClient memcached_client("localhost", 11211);
          if (memcached_client.connect()) {
            std::string memcached_data;
            if (memcached_client.get(name, memcached_data)) {
              if (!quiet_) {
                std::cout << "read data from Memcached key '" << name << "'" << std::endl;
              }
              WriteS3(dest, memcached_data.c_str());
            } else {
              throw std::runtime_error("Memcached GET command failed - key not found");
            }
          } else {
            throw std::runtime_error("Failed to connect to Memcached server");
          }
        } catch (const std::exception& e) {
          if (!quiet_) {
            std::cout << "Memcached error: " << e.what() << ", falling back to other storage..." << std::endl;
          }
          // Fall through to other storage backends
        }
#endif

#ifdef USE_REDIS
        // Try Redis if available
        try {
          Poco::Redis::Client redis_client("localhost", 6379);
          Poco::Redis::Command get_cmd("GET");
          get_cmd << name;

          Poco::Redis::BulkString result = redis_client.execute<Poco::Redis::BulkString>(get_cmd);
          if (!result.isNull()) {
            std::string redis_data = result.value();
            if (!quiet_) {
              std::cout << "read data from Redis key '" << name << "'" << std::endl;
            }
            WriteS3(dest, redis_data.c_str());
          } else {
            throw Poco::Exception("Redis GET command failed - key not found");
          }
        } catch (const Poco::Exception& e) {
          if (!quiet_) {
            std::cout << "Redis error: " << e.displayText() << ", falling back to SharedMemory..." << std::endl;
          }
          // Fall through to SharedMemory
        } catch (const std::exception& e) {
          if (!quiet_) {
            std::cout << "Redis error: " << e.what() << ", falling back to SharedMemory..." << std::endl;
          }
          // Fall through to SharedMemory
        }
#endif

        // Fallback to SharedMemory (original implementation)
#ifdef __OpenBSD__
        // OpenBSD: Poco::SharedMemory is unreliable; read directly from file
        {
          std::ifstream ifs_data(name, std::ios::binary);
          std::string file_content((std::istreambuf_iterator<char>(ifs_data)),
                                   std::istreambuf_iterator<char>());
          if (!quiet_) {
            std::cout << "read from '" << name << "' buffer (direct file)." << std::endl;
          }
          WriteS3(dest, file_content.data());
        }
#else
        Poco::SharedMemory shm_r(file, Poco::SharedMemory::AM_READ);
        if (!quiet_) {
          std::cout << "read '" << shm_r.begin() << "' from '" << name
                    << "' buffer (SharedMemory)." << std::endl;
        }
        WriteS3(dest, shm_r.begin());
#endif  // __OpenBSD__

        // Final WriteS3 call with NULL to finalize
        WriteS3(dest, NULL);
#endif  // USE_AWS || USE_POCO
      }
    } catch (Poco::Exception& e) {
      std::cerr << "Error: poco exception - " << e.displayText() << std::endl;
      return 1;
    } catch (std::exception& e) {
      std::cerr << "Error: standard exception - " << e.what() << std::endl;
      return 1;
    }
#endif  // POCO: dset not empty
  }
#endif  // POCO: URL
  return 0;
}

std::string OMNI::ReadTags(const std::string& buf) {
  const std::string filename = ".blackhole/ls";
  std::ifstream input_file(filename);
  if (!input_file.is_open()) {
    std::cerr << "Error: Could not open the file \"" << filename << "\""
              << std::endl;
    return "";
  }

  std::string line;
  while (std::getline(input_file, line)) {
    std::stringstream ss(line);
    std::string first_string;
    std::string second_string;
    // Get the first part of the string, using '|' as the delimiter
    if (std::getline(ss, first_string, '|')) {
      if (first_string == buf) {
        if (std::getline(ss, second_string, '|')) {
          return second_string;
        }
      }
    }
  }
  return "";
}

int OMNI::WriteOmni(const std::string& buf) {
  std::string ofile = buf + ".omni.yaml";
  if (!quiet_) {
    std::cout << "writing output " << ofile << "...";
  }

#ifdef USE_POCO
  std::string h = Sha256File(buf);
#endif

  std::ofstream of(ofile);
  of << "# OMNI" << std::endl;
  of << "name: " << buf << std::endl;

  // Try to get tags from DataHub if configured, otherwise read from local metadata
  std::string tags;
#ifdef USE_DATAHUB
  if (CheckDataHubConfig()) {
    tags = QueryDataHubForDataset(buf);
    if (tags.empty()) {
      if (!quiet_) {
        std::cout << "DataHub query returned no tags, trying local metadata..." << std::endl;
      }
      tags = ReadTags(buf);
    }
  } else {
    tags = ReadTags(buf);
  }
#else
  tags = ReadTags(buf);
#endif

  if (!tags.empty()) {
    of << "tags: " << tags << std::endl;
  } else {
    of.close();
    std::cerr << "Error: No tags found for dataset '" << buf << "' in DataHub or local metadata" << std::endl;
    return -1;
  }

#ifdef USE_POCO
  Poco::Path path(buf);
  of << "src: " << path.makeAbsolute().toString() << std::endl;
  if (!h.empty()) {
    of << "hash: " << h << std::endl;
  }
#endif
  of.close();
  if (!quiet_) {
    std::cout << "done" << std::endl;
  }
  return 0;
}

int OMNI::SetBlackhole() {
  if (!quiet_) {
    std::cout << "checking IOWarp runtime...";
  }

#ifdef USE_POCO
  Poco::File dir(".blackhole");
  if (dir.exists() == true) {
    if (!quiet_) {
      std::cout << "yes" << std::endl;
    }
  } else {
    if (!quiet_) {
      std::cout << "no" << std::endl;
      std::cout << "launching a new IOWarp runtime...";
    }
    if (dir.createDirectory()) {
      if (!quiet_) {
        std::cout << "done" << std::endl;
      }
      return 0;
    } else {
      std::cerr << "Error: failed to create .blackhole directory" << std::endl;
      return -1;
    }
  }
#else
  if (std::filesystem::exists(".blackhole") == true) {
    if (!quiet_) {
      std::cout << "yes" << std::endl;
    }
  } else {
    if (!quiet_) {
      std::cout << "no" << std::endl;
      std::cout << "launching a new IOWarp runtime...";
    }
    if (std::filesystem::create_directory(".blackhole")) {
      if (!quiet_) {
        std::cout << "done" << std::endl;
      }
      return 0;
    } else {
      std::cerr << "Error: failed to create .blackhole directory" << std::endl;
      return -1;
    }
  }
#endif
  return 0;
}

int OMNI::ReadExactBytesFromOffset(const char* filename, off_t offset,
                                   size_t num_bytes, unsigned char* buffer) {
  int fd = -1;
  ssize_t total_bytes_read = 0;
  ssize_t bytes_read;

  // Retry logic for file opening (handles file locks from concurrent writes)
  const int max_retries = 50;  // Try for up to 5 seconds
  const int retry_delay_ms = 100;  // 100ms between retries
  int retry_count = 0;

  while (retry_count < max_retries) {
#ifdef _WIN32
    fd = open(filename, O_RDONLY | O_BINARY);
#else
    fd = open(filename, O_RDONLY);
#endif
    if (fd != -1) {
      break;  // Successfully opened the file
    }

    // Check if error is due to file locking/permission issues
    if (errno == EACCES || errno == EPERM || errno == EBUSY
#ifdef _WIN32
        || errno == EAGAIN
#endif
    ) {
      retry_count++;
      if (retry_count == 1 && !quiet_) {
        std::cout << "File is locked, waiting for it to become available..." << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));
    } else {
      // Other error, don't retry
      break;
    }
  }

  if (fd == -1) {
    std::cerr << "Error: opening file " << filename;
    if (retry_count > 0) {
      std::cerr << " (tried " << retry_count << " times)";
    }
    std::cerr << std::endl;
    return -1;
  }

  if (retry_count > 0 && !quiet_) {
    std::cout << "File successfully opened after " << retry_count << " retries" << std::endl;
  }

  if (lseek(fd, offset, SEEK_SET) == -1) {
    std::cerr << "Error: seeking file" << filename << std::endl;
    close(fd);
    return -1;
  }

  while (total_bytes_read < num_bytes) {
    bytes_read =
        read(fd, buffer + total_bytes_read, num_bytes - total_bytes_read);
    if (bytes_read == -1) {
      // Check if error is due to file locking/permission issues
      if ((errno == EACCES || errno == EPERM || errno == EBUSY
#ifdef _WIN32
           || errno == EAGAIN
#endif
          ) && retry_count < max_retries) {
        // Close the file, wait, and retry from the beginning
        close(fd);
        retry_count++;
        if (retry_count == 1 && !quiet_) {
          std::cout << "File is locked during read, waiting for it to become available..." << std::endl;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_delay_ms));

        // Retry opening the file
        total_bytes_read = 0;  // Reset read count
#ifdef _WIN32
        fd = open(filename, O_RDONLY | O_BINARY);
#else
        fd = open(filename, O_RDONLY);
#endif
        if (fd == -1) {
          std::cerr << "Error: reopening file " << filename << std::endl;
          return -1;
        }

        // Retry seeking
        if (lseek(fd, offset, SEEK_SET) == -1) {
          std::cerr << "Error: seeking file " << filename << std::endl;
          close(fd);
          return -1;
        }

        continue;  // Retry reading
      } else {
        perror("Error reading file");
        close(fd);
        return -1;
      }
    }
    if (bytes_read == 0) {
      fprintf(stderr,
              "End of file reached after reading %zu bytes, expected %zu.\n",
              total_bytes_read, num_bytes);
      close(fd);
      return -2;
    }
    total_bytes_read += bytes_read;
  }

  if (close(fd) == -1) {
    perror("Error closing file");
    return -1;
  }

  if ((size_t)total_bytes_read == num_bytes)
    return 0;
  else
    return 1;
}

}  // namespace cae
