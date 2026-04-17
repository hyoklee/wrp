///
/// test_omni_extended.cpp - Extended unit tests for OMNI class
/// Tests for YAML parsing, file operations, and integration workflows
///
#include "OMNI.h"
#include <fstream>
#include <filesystem>
#include <iostream>
#include <sstream>
#include <sys/stat.h>
#ifndef _WIN32
#include <unistd.h>
#endif

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

// Helper functions
void create_test_file(const std::string& path, const std::string& content) {
  std::ofstream ofs(path, std::ios::binary);
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
// Test 1: Put with valid YAML containing src with local file
//
bool test_Put_with_local_file() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  // Create a source file
  create_test_file("test_source.txt", "Hello, World! This is test data.");

  // Create YAML with src pointing to local file
  std::string yaml_file = "test_local_src.yml";
  create_test_file(yaml_file,
    "name: testlocal\n"
    "tags:\n"
    "  - integration\n"
    "  - test\n"
    "src: test_source.txt\n"
    "offset: 0\n"
    "nbyte: 5\n");

  int result = omni.Put(yaml_file);

  // Clean up
  remove_test_file(yaml_file);
  remove_test_file("test_source.txt");
  remove_test_file("testlocal");

  return result == 0;
}

//
// Test 2: Put with src file not found
//
bool test_Put_src_not_found() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  std::string yaml_file = "test_src_notfound.yml";
  create_test_file(yaml_file,
    "name: testnotfound\n"
    "tags:\n"
    "  - test\n"
    "src: /nonexistent/file.txt\n"
    "offset: 0\n"
    "nbyte: 10\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);

  return result == -1;
}

//
// Test 3: Put with hash verification (valid)
//
bool test_Put_with_valid_hash() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  // Create a source file with known content
  create_test_file("test_hash.txt", "test");

#ifdef USE_POCO
  // Calculate actual hash first
  std::string actual_hash = omni.Sha256File("test_hash.txt");

  std::string yaml_file = "test_hash_valid.yml";
  create_test_file(yaml_file,
    "name: testhash\n"
    "tags:\n"
    "  - test\n"
    "src: test_hash.txt\n"
    "hash: " + actual_hash + "\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);
  remove_test_file("test_hash.txt");
  remove_test_file("testhash");

  return result == 0;
#else
  remove_test_file("test_hash.txt");
  return true;  // Skip test if POCO not available
#endif
}

//
// Test 4: Put with hash verification (invalid)
//
bool test_Put_with_invalid_hash() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  create_test_file("test_hash_bad.txt", "test");

  std::string yaml_file = "test_hash_invalid.yml";
  create_test_file(yaml_file,
    "name: testhashbad\n"
    "tags:\n"
    "  - test\n"
    "src: test_hash_bad.txt\n"
    "hash: 0000000000000000000000000000000000000000000000000000000000000000\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);
  remove_test_file("test_hash_bad.txt");

  return result == -1;
}

//
// Test 5: Put with offset and nbyte
//
bool test_Put_with_offset_nbyte() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  // Create a file with known content
  create_test_file("test_offset.txt", "0123456789ABCDEFGHIJ");

  std::string yaml_file = "test_offset.yml";
  create_test_file(yaml_file,
    "name: testoffset\n"
    "tags:\n"
    "  - test\n"
    "src: test_offset.txt\n"
    "offset: 5\n"
    "nbyte: 5\n");  // Should read "56789"

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);
  remove_test_file("test_offset.txt");
  remove_test_file("testoffset");

  return result == 0;
}

//
// Test 6: Put with tags as sequence
//
bool test_Put_tags_as_sequence() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");
  remove_test_file(".blackhole/ls");  // Start fresh

  // Create source file
  create_test_file("test_tags_src.txt", "test data");

  std::string yaml_file = "test_tags_seq.yml";
  create_test_file(yaml_file,
    "name: testtagsseq\n"
    "tags:\n"
    "  - tag1\n"
    "  - tag2\n"
    "  - tag3\n"
    "src: test_tags_src.txt\n"
    "offset: 0\n"
    "nbyte: 4\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);
  remove_test_file("test_tags_src.txt");
  remove_test_file("testtagsseq");

  if (result != 0) {
    std::cerr << "Put failed with result: " << result << std::endl;
    return false;
  }

  // Check metadata was written
  if (!fs::exists(".blackhole/ls")) {
    std::cerr << ".blackhole/ls does not exist" << std::endl;
    return false;
  }

  std::ifstream ifs(".blackhole/ls");
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      std::istreambuf_iterator<char>());
  ifs.close();

  bool found = content.find("testtagsseq") != std::string::npos &&
               content.find("tag1,tag2,tag3") != std::string::npos;

  if (!found) {
    std::cerr << "Metadata content: " << content << std::endl;
  }

  return found;
}

//
// Test 7: Put with invalid YAML syntax
//
bool test_Put_invalid_yaml() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  std::string yaml_file = "test_invalid.yml";
  create_test_file(yaml_file,
    "name: test\n"
    "tags:\n"
    "  - tag1\n"
    "  this is invalid yaml syntax\n"
    "  - tag2\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);

  return result == 1;  // Should fail with parse error
}

//
// Test 8: Get with valid metadata and file
//
bool test_Get_complete_workflow() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

#ifdef USE_POCO
  // Create test buffer and metadata
  create_test_file("testcomplete", "test data content");
  create_test_file(".blackhole/ls", "testcomplete|workflow,test\n");

  int result = omni.Get("testcomplete");

  // Check output file was created
  bool yaml_exists = fs::exists("testcomplete.omni.yaml");

  // Verify YAML content
  bool has_correct_content = false;
  if (yaml_exists) {
    std::ifstream ifs("testcomplete.omni.yaml");
    std::string content((std::istreambuf_iterator<char>(ifs)),
                        std::istreambuf_iterator<char>());
    ifs.close();

    has_correct_content = content.find("name: testcomplete") != std::string::npos &&
                         content.find("tags: workflow,test") != std::string::npos;
  }

  remove_test_file("testcomplete.omni.yaml");
  remove_test_file("testcomplete");

  return result == 0 && yaml_exists && has_correct_content;
#else
  return true;  // Skip if POCO not available
#endif
}

//
// Test 9: List with multiple entries
//
bool test_List_multiple_entries() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");
  create_test_file(".blackhole/ls",
    "buffer1|tag1,tag2\n"
    "buffer2|tag3,tag4,tag5\n"
    "buffer3|test\n");

  int result = omni.List();

  return result == 0;
}

//
// Test 10: ReadExactBytesFromOffset - basic read
//
bool test_ReadExactBytesFromOffset_success() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  // Create test file
  create_test_file("test_read.bin", "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ");

  unsigned char buffer[10];
  int result = omni.ReadExactBytesFromOffset("test_read.bin", 5, 10, buffer);

  remove_test_file("test_read.bin");

  // Check if correct bytes were read
  bool correct = (result == 0);
  if (correct) {
    std::string read_data((char*)buffer, 10);
    correct = (read_data == "56789ABCDE");
  }

  return correct;
}

//
// Test 11: ReadExactBytesFromOffset - file not found
//
bool test_ReadExactBytesFromOffset_not_found() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  unsigned char buffer[10];
  int result = omni.ReadExactBytesFromOffset("/nonexistent/file.bin", 0, 10, buffer);

  return result == -1;
}

//
// Test 12: ReadExactBytesFromOffset - read beyond EOF
//
bool test_ReadExactBytesFromOffset_eof() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_file("test_eof.bin", "short");  // Only 5 bytes

  unsigned char buffer[20];
  int result = omni.ReadExactBytesFromOffset("test_eof.bin", 0, 20, buffer);

  remove_test_file("test_eof.bin");

  return result == -2;  // Should return -2 for EOF
}

//
// Test 13: ReadProxyConfig with whitespace in values
//
bool test_ReadProxyConfig_with_whitespace() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string wrp_dir = home + "/.wrp";
  std::string config_path = wrp_dir + "/config";

  // Backup
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create config with extra whitespace
  create_test_dir(wrp_dir);
  create_test_file(config_path,
    "  ProxyConfig enabled  \n"
    "ProxyHost    proxy.test.com    \n"
    "ProxyPort    9090    \n"
    "ProxyUsername    user123    \n"
    "ProxyPassword    pass456    \n");

  cae::ProxyConfig config = omni.ReadProxyConfig();

  // Restore
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return config.enabled &&
         config.host == "proxy.test.com" &&
         config.port == 9090 &&
         config.username == "user123" &&
         config.password == "pass456";
}

//
// Test 14: ReadAWSConfig with empty values
//
bool test_ReadAWSConfig_empty_values() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string aws_dir = home + "/.aws";
  std::string config_path = aws_dir + "/config";

  // Backup
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create config with empty values
  create_test_dir(aws_dir);
  create_test_file(config_path,
    "[default]\n"
    "endpoint_url = \n"
    "region = \n");

  cae::AWSConfig config = omni.ReadAWSConfig();

  // Restore
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return config.endpoint_url.empty() && config.region.empty();
}

//
// Test 15: ReadAWSConfig with semicolon comments
//
bool test_ReadAWSConfig_semicolon_comments() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string home = get_home_dir();
  std::string aws_dir = home + "/.aws";
  std::string config_path = aws_dir + "/config";

  // Backup
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create config with semicolon comments
  create_test_dir(aws_dir);
  create_test_file(config_path,
    "; This is a comment\n"
    "[default]\n"
    "endpoint_url = http://localhost:4566\n"
    "; Another comment\n"
    "region = us-east-1\n");

  cae::AWSConfig config = omni.ReadAWSConfig();

  // Restore
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return config.endpoint_url == "http://localhost:4566" &&
         config.region == "us-east-1";
}

//
// Test 16: Put with scalar root (should fail)
//
bool test_Put_scalar_root() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  std::string yaml_file = "test_scalar.yml";
  create_test_file(yaml_file, "just a scalar value\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);

  return result == 1;  // Should fail validation
}

//
// Test 17: Put with sequence root (should fail)
//
bool test_Put_sequence_root() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");

  std::string yaml_file = "test_sequence.yml";
  create_test_file(yaml_file,
    "- item1\n"
    "- item2\n"
    "- item3\n");

  int result = omni.Put(yaml_file);

  remove_test_file(yaml_file);

  return result == 1;  // Should fail validation
}

//
// Test 18: Multiple Put operations creating different buffers
//
bool test_Multiple_Put_operations() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  create_test_dir(".blackhole");
  remove_test_file(".blackhole/ls");

  // Create source files
  create_test_file("test_src1.txt", "data1");
  create_test_file("test_src2.txt", "data2");
  create_test_file("test_src3.txt", "data3");

  // Create three different buffers
  std::string yaml1 = "test_multi1.yml";
  create_test_file(yaml1,
    "name: buffer1\n"
    "tags:\n"
    "  - test1\n"
    "src: test_src1.txt\n"
    "offset: 0\n"
    "nbyte: 5\n");

  std::string yaml2 = "test_multi2.yml";
  create_test_file(yaml2,
    "name: buffer2\n"
    "tags:\n"
    "  - test2\n"
    "src: test_src2.txt\n"
    "offset: 0\n"
    "nbyte: 5\n");

  std::string yaml3 = "test_multi3.yml";
  create_test_file(yaml3,
    "name: buffer3\n"
    "tags:\n"
    "  - test3\n"
    "src: test_src3.txt\n"
    "offset: 0\n"
    "nbyte: 5\n");

  int r1 = omni.Put(yaml1);
  int r2 = omni.Put(yaml2);
  int r3 = omni.Put(yaml3);

  if (r1 != 0 || r2 != 0 || r3 != 0) {
    std::cerr << "Put failed: r1=" << r1 << " r2=" << r2 << " r3=" << r3 << std::endl;
    remove_test_file(yaml1);
    remove_test_file(yaml2);
    remove_test_file(yaml3);
    remove_test_file("test_src1.txt");
    remove_test_file("test_src2.txt");
    remove_test_file("test_src3.txt");
    return false;
  }

  // Check all three are in metadata
  if (!fs::exists(".blackhole/ls")) {
    std::cerr << ".blackhole/ls does not exist" << std::endl;
    remove_test_file(yaml1);
    remove_test_file(yaml2);
    remove_test_file(yaml3);
    remove_test_file("test_src1.txt");
    remove_test_file("test_src2.txt");
    remove_test_file("test_src3.txt");
    return false;
  }

  std::ifstream ifs(".blackhole/ls");
  std::string content((std::istreambuf_iterator<char>(ifs)),
                      std::istreambuf_iterator<char>());
  ifs.close();

  bool all_present = content.find("buffer1") != std::string::npos &&
                     content.find("buffer2") != std::string::npos &&
                     content.find("buffer3") != std::string::npos;

  if (!all_present) {
    std::cerr << "Metadata content: " << content << std::endl;
  }

  remove_test_file(yaml1);
  remove_test_file(yaml2);
  remove_test_file(yaml3);
  remove_test_file("test_src1.txt");
  remove_test_file("test_src2.txt");
  remove_test_file("test_src3.txt");
  remove_test_file("buffer1");
  remove_test_file("buffer2");
  remove_test_file("buffer3");

  return all_present;
}

//
// Test 19: ReadConfigFile with actual content
//
bool test_ReadConfigFile_with_content() {
  cae::OMNI omni;
  omni.SetQuiet(true);

  std::string config_file = "test_config_content.txt";
  std::string test_content = "Line1\nLine2\nLine3\n";
  create_test_file(config_file, test_content);

  std::string content = omni.ReadConfigFile(config_file);

  remove_test_file(config_file);

  return content == test_content;
}

//
// Test 20: CheckDataHubConfig when not configured
//
bool test_CheckDataHubConfig_not_configured() {
  cae::OMNI omni;
  omni.SetQuiet(true);

#ifdef USE_DATAHUB
  std::string home = get_home_dir();
  std::string config_path = home + "/.wrp/config";

  // Backup
  bool config_existed = fs::exists(config_path);
  std::string backup_content;
  if (config_existed) {
    std::ifstream ifs(config_path);
    std::stringstream ss;
    ss << ifs.rdbuf();
    backup_content = ss.str();
    ifs.close();
  }

  // Create config without DataHub
  create_test_dir(home + "/.wrp");
  create_test_file(config_path, "SomeOtherConfig value\n");

  bool result = omni.CheckDataHubConfig();

  // Restore
  if (config_existed) {
    create_test_file(config_path, backup_content);
  } else {
    remove_test_file(config_path);
  }

  return !result;  // Should return false
#else
  return true;  // Skip if DataHub not compiled
#endif
}

int main() {
  std::cout << "========================================" << std::endl;
  std::cout << "  OMNI Extended Unit Tests" << std::endl;
  std::cout << "========================================" << std::endl << std::endl;

  // Run all tests
  TEST(Put_with_local_file);
  TEST(Put_src_not_found);
  TEST(Put_with_valid_hash);
  TEST(Put_with_invalid_hash);
  TEST(Put_with_offset_nbyte);
  TEST(Put_tags_as_sequence);
  TEST(Put_invalid_yaml);
  TEST(Get_complete_workflow);
  TEST(List_multiple_entries);
  TEST(ReadExactBytesFromOffset_success);
  TEST(ReadExactBytesFromOffset_not_found);
  TEST(ReadExactBytesFromOffset_eof);
  TEST(ReadProxyConfig_with_whitespace);
  TEST(ReadAWSConfig_empty_values);
  TEST(ReadAWSConfig_semicolon_comments);
  TEST(Put_scalar_root);
  TEST(Put_sequence_root);
  TEST(Multiple_Put_operations);
  TEST(ReadConfigFile_with_content);
  TEST(CheckDataHubConfig_not_configured);

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
