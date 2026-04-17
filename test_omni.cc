///
/// test_omni.cpp - Unit tests for OMNI class
///
#include "OMNI.h"
#include <fstream>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <sys/stat.h>

namespace fs = std::filesystem;

// Test result tracking
int tests_passed = 0;
int tests_failed = 0;

#define TEST(name) \
  std::cout << "Running test: " << #name << "..." << std::endl; \
  if (test_##name()) { \
    tests_passed++; \
    std::cout << "  PASSED" << std::endl; \
  } else { \
    tests_failed++; \
    std::cout << "  FAILED" << std::endl; \
  }

// Helper to create test files
void create_test_file(const std::string& path, const std::string& content) {
  std::ofstream ofs(path);
  ofs << content;
  ofs.close();
}

void remove_test_file(const std::string& path) {
  if (fs::exists(path)) {
    fs::remove(path);
  }
}

void create_test_dir(const std::string& path) {
  if (!fs::exists(path)) {
    fs::create_directories(path);
  }
}

void remove_test_dir(const std::string& path) {
  if (fs::exists(path)) {
    fs::remove_all(path);
  }
}

// Helper to get home directory (matches OMNI::ReadProxyConfig logic)
std::string get_home_dir() {
#ifdef _WIN32
  char* home_path = nullptr;
  size_t len = 0;
  errno_t err = _dupenv_s(&home_path, &len, "USERPROFILE");
  if (err == 0 && home_path != nullptr) {
    std::string result = home_path;
    free(home_path);
    return result;
  }
  return ".";
#else
  const char* home_path = std::getenv("HOME");
  return home_path ? home_path : ".";
#endif
}

//
// Test 1: Test List() error when .blackhole/ls doesn't exist
//
bool test_List_file_not_found() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  // Ensure directory exists but ls file doesn't
  create_test_dir(".blackhole");
  remove_test_file(".blackhole/ls");

  // List should return error
  int result = omni.List();

  return result == 1;
}

//
// Test 2: Test List() success
//
bool test_List_success() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  // Create test data
  create_test_dir(".blackhole");
  create_test_file(".blackhole/ls", "test1|tag1,tag2\ntest2|tag3\n");

  // List should succeed
  int result = omni.List();

  return result == 0;
}

//
// Test 3: Test Get() - with valid buffer
//
bool test_Get_with_valid_buffer() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");
  create_test_file(".blackhole/ls", "testbuffer|tag1,tag2,tag3\n");

#ifdef USE_POCO
  // Create a test buffer file
  create_test_file("testbuffer", "test content");
#endif

  int result = omni.Get("testbuffer");

  // Clean up
  remove_test_file("testbuffer.omni.yaml");
#ifdef USE_POCO
  remove_test_file("testbuffer");
#endif

  return result == 0;
}

//
// Test 4: Test Get() - buffer not found in metadata
//
bool test_Get_buffer_not_found() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");
  create_test_file(".blackhole/ls", "otherbuffer|tag1\n");  // Different buffer

  // Create a dummy file to avoid Sha256File exception
#ifdef USE_POCO
  create_test_file("nonexistent", "dummy");
#endif

  int result = omni.Get("nonexistent");

  // Clean up
  remove_test_file("nonexistent.omni.yaml");
#ifdef USE_POCO
  remove_test_file("nonexistent");
#endif

  return result == -1;
}

//
// Test 5: Test ReadConfigFile() - file doesn't exist
//
bool test_ReadConfigFile_not_found() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string content = omni.ReadConfigFile("/nonexistent/config/file");

  return content.empty();
}

//
// Test 9: Test ReadConfigFile() - file exists
//
bool test_ReadConfigFile_exists() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  // Create a temp config file
  std::string config_path = "test_config.txt";
  create_test_file(config_path, "ProxyConfig enabled\nProxyHost proxy.example.com\n");

  std::string content = omni.ReadConfigFile(config_path);

  remove_test_file(config_path);

  return !content.empty() && content.find("ProxyConfig enabled") != std::string::npos;
}

//
// Test 10: Test ReadProxyConfig() - no config file
//
bool test_ReadProxyConfig_no_file() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  // Remove config if it exists
  std::string home = get_home_dir();
  std::string config_path = home + "/.wrp/config";
  bool config_existed = fs::exists(config_path);
  std::string backup_content;

  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
    remove_test_file(config_path);
  }

  // Save and clear proxy environment variables to test the "no config" scenario
  const char* saved_http_proxy = std::getenv("http_proxy");
  const char* saved_https_proxy = std::getenv("https_proxy");
  const char* saved_HTTP_PROXY = std::getenv("HTTP_PROXY");
  const char* saved_HTTPS_PROXY = std::getenv("HTTPS_PROXY");

  std::string http_proxy_backup = saved_http_proxy ? saved_http_proxy : "";
  std::string https_proxy_backup = saved_https_proxy ? saved_https_proxy : "";
  std::string HTTP_PROXY_backup = saved_HTTP_PROXY ? saved_HTTP_PROXY : "";
  std::string HTTPS_PROXY_backup = saved_HTTPS_PROXY ? saved_HTTPS_PROXY : "";

#ifndef _WIN32
  unsetenv("http_proxy");
  unsetenv("https_proxy");
  unsetenv("HTTP_PROXY");
  unsetenv("HTTPS_PROXY");
#else
  _putenv("http_proxy=");
  _putenv("https_proxy=");
  _putenv("HTTP_PROXY=");
  _putenv("HTTPS_PROXY=");
#endif

  cae::ProxyConfig config = omni.ReadProxyConfig();

  // Restore proxy environment variables
#ifndef _WIN32
  if (!http_proxy_backup.empty()) setenv("http_proxy", http_proxy_backup.c_str(), 1);
  if (!https_proxy_backup.empty()) setenv("https_proxy", https_proxy_backup.c_str(), 1);
  if (!HTTP_PROXY_backup.empty()) setenv("HTTP_PROXY", HTTP_PROXY_backup.c_str(), 1);
  if (!HTTPS_PROXY_backup.empty()) setenv("HTTPS_PROXY", HTTPS_PROXY_backup.c_str(), 1);
#else
  if (!http_proxy_backup.empty()) _putenv(("http_proxy=" + http_proxy_backup).c_str());
  if (!https_proxy_backup.empty()) _putenv(("https_proxy=" + https_proxy_backup).c_str());
  if (!HTTP_PROXY_backup.empty()) _putenv(("HTTP_PROXY=" + HTTP_PROXY_backup).c_str());
  if (!HTTPS_PROXY_backup.empty()) _putenv(("HTTPS_PROXY=" + HTTPS_PROXY_backup).c_str());
#endif

  // Restore config if it existed
  if (config_existed) {
    create_test_dir(home + "/.wrp");
    create_test_file(config_path, backup_content);
  }

  return !config.enabled;
}

//
// Test 11: Test ReadProxyConfig() - with valid config
//
bool test_ReadProxyConfig_with_file() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string wrp_dir = home + "/.wrp";
  std::string config_path = wrp_dir + "/config";

  // Backup existing config
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create test config
  create_test_dir(wrp_dir);
  create_test_file(config_path,
    "ProxyConfig enabled\n"
    "ProxyHost proxy.test.com\n"
    "ProxyPort 8080\n"
    "ProxyUsername testuser\n"
    "ProxyPassword testpass\n");

  cae::ProxyConfig config = omni.ReadProxyConfig();

  // Restore or remove config
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return config.enabled &&
         config.host == "proxy.test.com" &&
         config.port == 8080 &&
         config.username == "testuser" &&
         config.password == "testpass";
}

//
// Test 12: Test ReadProxyConfig() - with invalid port
//
bool test_ReadProxyConfig_invalid_port() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string wrp_dir = home + "/.wrp";
  std::string config_path = wrp_dir + "/config";

  // Backup existing config
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create test config with invalid port
  create_test_dir(wrp_dir);
  create_test_file(config_path,
    "ProxyConfig enabled\n"
    "ProxyHost proxy.test.com\n"
    "ProxyPort invalid_port\n");

  cae::ProxyConfig config = omni.ReadProxyConfig();

  // Restore or remove config
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return config.enabled && config.port == 0;  // Should handle invalid port gracefully
}

//
// Test 13: Test ReadAWSConfig() - no config file
//
bool test_ReadAWSConfig_no_file() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string config_path = home + "/.aws/config";

  // Temporarily rename config if it exists
  bool config_existed = fs::exists(config_path);
  if (config_existed) {
    fs::rename(config_path, config_path + ".backup");
  }

  cae::AWSConfig config = omni.ReadAWSConfig();

  // Restore config
  if (config_existed) {
    fs::rename(config_path + ".backup", config_path);
  }

  return config.endpoint_url.empty() && config.region.empty();
}

//
// Test 14: Test ReadAWSConfig() - with valid config
//
bool test_ReadAWSConfig_with_file() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string aws_dir = home + "/.aws";
  std::string config_path = aws_dir + "/config";

  // Backup existing config
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create test config
  create_test_dir(aws_dir);
  create_test_file(config_path,
    "[default]\n"
    "endpoint_url = http://localhost:4566\n"
    "region = us-west-2\n");

  cae::AWSConfig config = omni.ReadAWSConfig();

  // Restore or remove config
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return config.endpoint_url == "http://localhost:4566" &&
         config.region == "us-west-2";
}

//
// Test 15: Test ReadAWSConfig() - with comments and other sections
//
bool test_ReadAWSConfig_with_comments() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string aws_dir = home + "/.aws";
  std::string config_path = aws_dir + "/config";

  // Backup existing config
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create test config with comments
  create_test_dir(aws_dir);
  create_test_file(config_path,
    "# This is a comment\n"
    "[default]\n"
    "endpoint_url = http://test.local:9000\n"
    "region = eu-west-1\n"
    "[profile other]\n"
    "region = us-east-1\n");

  cae::AWSConfig config = omni.ReadAWSConfig();

  // Restore or remove config
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  // Should only read from [default] section
  return config.endpoint_url == "http://test.local:9000" &&
         config.region == "eu-west-1";
}


//
// Test 25: Test Put() - file not found
//
bool test_Put_file_not_found() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  int result = omni.Put("/nonexistent/omni/file.yml");

  return result == 1;
}

//
// Test 26: Test Put() - missing name field
//
bool test_Put_missing_name() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  // Create YAML without name field
  std::string yaml_file = "test_missing_name.yml";
  create_test_file(yaml_file,
    "tags:\n"
    "  - tag1\n"
    "  - tag2\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);

  return result == 1;
}

//
// Test 27: Test Put() - missing tags field
//
bool test_Put_missing_tags() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  // Create YAML without tags field
  std::string yaml_file = "test_missing_tags.yml";
  create_test_file(yaml_file,
    "name: testbuffer\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);

  return result == 1;
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "  OMNI Unit Tests" << std::endl;
  std::cout << "========================================" << std::endl << std::endl;

  // Run all tests
  TEST(List_file_not_found);
  TEST(List_success);
  TEST(Get_with_valid_buffer);
  TEST(Get_buffer_not_found);
  TEST(ReadConfigFile_not_found);
  TEST(ReadConfigFile_exists);
  TEST(ReadProxyConfig_no_file);
  TEST(ReadProxyConfig_with_file);
  TEST(ReadProxyConfig_invalid_port);
  TEST(ReadAWSConfig_no_file);
  TEST(ReadAWSConfig_with_file);
  TEST(ReadAWSConfig_with_comments);
  TEST(Put_file_not_found);
  TEST(Put_missing_name);
  TEST(Put_missing_tags);

  // Summary
  std::cout << std::endl << "========================================" << std::endl;
  std::cout << "Test Summary:" << std::endl;
  std::cout << "  Passed: " << tests_passed << std::endl;
  std::cout << "  Failed: " << tests_failed << std::endl;
  std::cout << "========================================" << std::endl;

  if (tests_failed == 0) {
    std::cout << "All tests passed!" << std::endl;
    std::cout.flush();
    return 0;
  } else {
    std::cout << "Some tests failed." << std::endl;
    std::cout.flush();
    return 1;
  }
}
