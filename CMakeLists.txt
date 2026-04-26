cmake_minimum_required(VERSION 3.22)

if(DEFINED CMAKE_TOOLCHAIN_FILE AND NOT EXISTS "${CMAKE_TOOLCHAIN_FILE}")
  message(FATAL_ERROR
    "Toolchain file not found: ${CMAKE_TOOLCHAIN_FILE}\n"
    "Please install vcpkg:\n"
    "  git clone https://github.com/microsoft/vcpkg ~/vcpkg\n"
    "  ~/vcpkg/bootstrap-vcpkg.sh\n"
    "Then re-run cmake with:\n"
    "  -DCMAKE_TOOLCHAIN_FILE=~/vcpkg/scripts/buildsystems/vcpkg.cmake"
  )
endif()

project(OMNI)

include(CTest)
enable_testing()

set(CMAKE_CXX_STANDARD 17)

find_package(yaml-cpp REQUIRED)
  
include_directories(
        ${CMAKE_BINARY_DIR}
        ${CMAKE_SOURCE_DIR}
	${YAML_CPP_INCLUDE_DIR}
        .
)

option(AWS "Enable AWS SDK support" OFF)
if(AWS)
  find_package(AWSSDK REQUIRED COMPONENTS core s3)
  add_definitions(-DUSE_AWS)    
  include_directories(${AWSSDK_INCLUDE_DIR})
endif()

option(POCO "Enable POCO support" OFF)
if(POCO)
  add_definitions(-DUSE_POCO)
  if(${CMAKE_SYSTEM_NAME} MATCHES "OpenBSD")
    message(STATUS "OpenBSD detected: Using manual Poco path resolution")
    set(POCO_INCLUDE_DIR "/usr/local/include")
    set(POCO_LIB_DIR "/usr/local/lib")
    
    file(GLOB POCO_CRYPTO "/usr/local/lib/libPocoCrypto.so*")
    if(POCO_CRYPTO)
        add_library(Poco::Crypto UNKNOWN IMPORTED)
        set_target_properties(Poco::Crypto PROPERTIES
            IMPORTED_LOCATION "${POCO_CRYPTO}"
            INTERFACE_INCLUDE_DIRECTORIES "/usr/local/include"
        )
    else()
        message(FATAL_ERROR "Poco Crypto library not found in /usr/local/lib")
    endif()
    
    file(GLOB POCO_FOUNDATION "/usr/local/lib/libPocoFoundation.so*")
    if(POCO_FOUNDATION)
        add_library(Poco::Foundation UNKNOWN IMPORTED)
        set_target_properties(Poco::Foundation PROPERTIES
            IMPORTED_LOCATION "${POCO_FOUNDATION}"
            INTERFACE_INCLUDE_DIRECTORIES "/usr/local/include"
        )
    else()
        message(FATAL_ERROR "Poco Foundation library not found in /usr/local/lib")
    endif()    
    
    file(GLOB POCO_NET "/usr/local/lib/libPocoNet.so*")
    if(POCO_NET)
        add_library(Poco::Net UNKNOWN IMPORTED)
        set_target_properties(Poco::Net PROPERTIES
            IMPORTED_LOCATION "${POCO_NET}"
            INTERFACE_INCLUDE_DIRECTORIES "/usr/local/include"
        )
    else()
        message(FATAL_ERROR "Poco Net library not found in /usr/local/lib")
    endif()    


    file(GLOB POCO_NETSSL "/usr/local/lib/libPocoNetSSL.so*")
    if(POCO_NETSSL)
        add_library(Poco::NetSSL UNKNOWN IMPORTED)
        set_target_properties(Poco::NetSSL PROPERTIES
            IMPORTED_LOCATION "${POCO_NETSSL}"
            INTERFACE_INCLUDE_DIRECTORIES "/usr/local/include"
        )
    else()
        message(FATAL_ERROR "Poco NetSSL library not found in /usr/local/lib")
    endif()    
    
  else()
    find_package(Poco REQUIRED COMPONENTS Crypto Foundation Net NetSSL)
  endif()
endif()


option(HERMES "Enable HERMES support" OFF)
if(HERMES)
add_definitions(-DUSE_HERMES)
find_package(Hermes)

find_package(cereal)
message(STATUS "found cereal at ${cereal_DIR}")
set(CDIR  ${cereal_DIR}/../../../include)
message(${CDIR})

find_package(Thallium REQUIRED)
set(TDIR  ${Thallium_DIR}/../../../include)
message(${TDIR})

find_package(PkgConfig REQUIRED)
pkg_check_modules(Margo REQUIRED margo)
message(STATUS "Margo_INCLUDE_DIRS: ${Margo_INCLUDE_DIRS}")

include_directories(
        ${CMAKE_BINARY_DIR}
        ${CMAKE_SOURCE_DIR}
        ${HERMES_SRC_DIR}
        ${HERMES_ADAPTER_DIR}
        ${HERMES_IO_CLIENT_DIR}
        ${TDIR}
        ${Margo_INCLUDE_DIRS}
        ${CDIR}
	${YAML_CPP_INCLUDE_DIR}
        .
)
endif()



add_executable(wrp wrp.cc OMNI.cc)

if(TARGET yaml-cpp::yaml-cpp)
  target_link_libraries(wrp yaml-cpp::yaml-cpp)  
else()
  target_link_libraries(wrp yaml-cpp)
endif()

find_library(FILESYSTEM_LIB stdc++fs
  PATHS ${CMAKE_CXX_IMPLICIT_LINK_DIRECTORIES}
)

if(FILESYSTEM_LIB)
    target_link_libraries(wrp ${FILESYSTEM_LIB})
endif()

 
if(HERMES)
  target_link_libraries(wrp hermes::hermes_core_client)
endif()

if(AWS)
#   target_link_libraries(wrp aws-cpp-sdk-s3)
  target_link_libraries(wrp ${AWSSDK_LINK_LIBRARIES})
endif()


if(POCO)
  target_link_libraries(wrp Poco::Crypto Poco::Foundation Poco::Net Poco::NetSSL)
  if(${CMAKE_SYSTEM_NAME} MATCHES "OpenBSD")
    target_link_libraries(wrp ssl crypto)
  endif()
endif()

option(COV "Enable COVERAGE support" OFF)
if(COV)
if (NOT CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
  target_compile_options(wrp PRIVATE -coverage)
  target_link_options(wrp PRIVATE -coverage)
endif()  
endif()

add_test(NAME put COMMAND wrp put ../test/posix.yml)

add_test(NAME get COMMAND wrp get cae)

add_test(NAME ls COMMAND wrp ls)

add_test(NAME invalid COMMAND wrp invalid)
set_tests_properties(invalid
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME no_cmd COMMAND wrp)
set_tests_properties(no_cmd
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME put_only COMMAND wrp put)
set_tests_properties(put_only
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME get_only COMMAND wrp get)
set_tests_properties(get_only
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME not_found COMMAND wrp put not_found)
set_tests_properties(not_found
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME nested COMMAND wrp put ../test/nested.yml)

add_test(NAME empty COMMAND wrp put ../test/empty.yml)

add_test(NAME root COMMAND wrp put ../test/root.yml)
set_tests_properties(root
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME scalar COMMAND wrp put ../test/scalar.yml)
set_tests_properties(scalar
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME invalid_yml COMMAND wrp put ../test/invalid.yml)
set_tests_properties(invalid_yml
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME path COMMAND wrp put ../test/path.yml)
set_tests_properties(path
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME root_seq COMMAND wrp put ../test/root_seq.yml)
set_tests_properties(root_seq
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME lseek COMMAND wrp put ../test/lseek.yml)
set_tests_properties(lseek
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

if(POCO)
add_test(NAME hash COMMAND wrp put ../test/hash.yml)
set_tests_properties(hash
    PROPERTIES
    WILL_FAIL "TRUE"
    RETURN_VALUE 1
)

add_test(NAME http COMMAND wrp put ../test/http.yml)
add_test(NAME redi COMMAND wrp put ../test/redi.yml)
add_test(NAME rget COMMAND wrp put ../test/rget.yml)
add_test(NAME globus COMMAND wrp put ../test/globus.yml)
endif()

if(POCO)
  add_test(NAME wait_file COMMAND wrp put ../test/wait_file.yml)

  add_executable(test_omni test_omni.cc OMNI.cc)
  if(TARGET yaml-cpp::yaml-cpp)
    target_link_libraries(test_omni yaml-cpp::yaml-cpp)
  else()
    target_link_libraries(test_omni yaml-cpp)
  endif()
  target_link_libraries(test_omni Poco::Crypto Poco::Foundation Poco::Net Poco::NetSSL)
  if(FILESYSTEM_LIB)
    target_link_libraries(test_omni ${FILESYSTEM_LIB})
  endif()
  if(AWS)
    target_link_libraries(test_omni ${AWSSDK_LINK_LIBRARIES})
  endif()
  if(${CMAKE_SYSTEM_NAME} MATCHES "OpenBSD")
    target_link_libraries(test_omni ssl crypto)
  endif()
  add_test(NAME omni_unit COMMAND $<TARGET_FILE:test_omni>)
  set_tests_properties(omni_unit PROPERTIES PASS_REGULAR_EXPRESSION "All tests passed!")

  add_executable(test_omni_extended test_omni_extended.cc OMNI.cc)
  if(TARGET yaml-cpp::yaml-cpp)
    target_link_libraries(test_omni_extended yaml-cpp::yaml-cpp)
  else()
    target_link_libraries(test_omni_extended yaml-cpp)
  endif()
  target_link_libraries(test_omni_extended Poco::Crypto Poco::Foundation Poco::Net Poco::NetSSL)
  if(FILESYSTEM_LIB)
    target_link_libraries(test_omni_extended ${FILESYSTEM_LIB})
  endif()
  if(AWS)
    target_link_libraries(test_omni_extended ${AWSSDK_LINK_LIBRARIES})
  endif()
  if(${CMAKE_SYSTEM_NAME} MATCHES "OpenBSD")
    target_link_libraries(test_omni_extended ssl crypto)
  endif()
  add_test(NAME omni_extended_unit COMMAND $<TARGET_FILE:test_omni_extended>)
  set_tests_properties(omni_extended_unit PROPERTIES PASS_REGULAR_EXPRESSION "All tests passed!")
endif()

install(TARGETS wrp DESTINATION bin)
