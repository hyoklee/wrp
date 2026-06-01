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

project(wrp)

include(CTest)
enable_testing()

set(CMAKE_CXX_STANDARD 17)

# Find installed libomni.
# Build and install ~/omni first:
#   cd ~/omni && mkdir build && cd build
#   cmake .. -DCMAKE_INSTALL_PREFIX=~/omni/install
#   make && make install
# Then configure wrp with:
#   cmake .. -Domni_DIR=~/omni/install/lib/cmake/omni
find_package(omni CONFIG REQUIRED)

option(COV "Enable COVERAGE support" OFF)
set(SANITIZER "" CACHE STRING "Sanitizer type: address, memory, or undefined")
if(SANITIZER)
  add_compile_options(-fsanitize=${SANITIZER} -fno-omit-frame-pointer)
  add_link_options(-fsanitize=${SANITIZER})
endif()
if(COV)
  if(NOT CMAKE_CXX_COMPILER_ID STREQUAL "MSVC")
    add_compile_options(--coverage)
    add_link_options(--coverage)
  endif()
endif()

add_executable(wrp wrp.cc)
target_link_libraries(wrp omni::omni)

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
add_test(NAME wait_file COMMAND wrp put ../test/wait_file.yml)
add_test(NAME hf_r COMMAND wrp put ../test/hf_w.yml)
set_tests_properties(hf_r
    PROPERTIES
    WILL_FAIL "TRUE"
)
add_test(NAME hf_w COMMAND wrp put ../test/hf_r.yml)

add_executable(t_o t_o.cc)
target_link_libraries(t_o omni::omni)
add_test(NAME omni_unit COMMAND $<TARGET_FILE:t_o>)
set_tests_properties(omni_unit PROPERTIES PASS_REGULAR_EXPRESSION "All tests passed!")

add_executable(t_p t_p.cc)
target_link_libraries(t_p omni::omni)
add_test(NAME omni_extended_unit COMMAND $<TARGET_FILE:t_p>)
set_tests_properties(omni_extended_unit PROPERTIES PASS_REGULAR_EXPRESSION "All tests passed!")

# MemorySanitizer cannot run cleanly here: wrp/omni are instrumented, but the
# system libstdc++, yaml-cpp and Poco they link are NOT, so every value that
# flows out of those libraries (std::string buffers, the ctype widen cache,
# std::filesystem path internals, YAML nodes) is reported as a
# use-of-uninitialized-value false positive that aborts the program before it
# reaches its real exit path. Instrumenting the whole dependency stack is not
# feasible (the prebuilt Poco/yaml-cpp are ABI-bound to libstdc++), and MSan
# ignores interceptor suppressions for these reports. Disable the affected
# tests under the memory sanitizer only; they keep running under
# address/leak/undefined and in the non-sanitizer builds.
if(SANITIZER STREQUAL "memory")
  set_tests_properties(
    invalid put_only get_only not_found
    root scalar invalid_yml path root_seq lseek hash hf_r
    omni_unit omni_extended_unit
    PROPERTIES DISABLED TRUE)
endif()

install(TARGETS wrp DESTINATION bin)
