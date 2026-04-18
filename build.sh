#!/usr/bin/env bash
set -e

# Fall back to system compiler if conda cross-compiler is not available
if [ -n "${CXX}" ] && ! command -v "${CXX}" > /dev/null 2>&1; then
    export CC=$(xcrun -f clang 2>/dev/null || which gcc 2>/dev/null || echo gcc)
    export CXX=$(xcrun -f clang++ 2>/dev/null || which g++ 2>/dev/null || echo g++)
fi

export CMAKE_PREFIX_PATH="${PREFIX}:${CMAKE_PREFIX_PATH}"
export CMAKE_INCLUDE_PATH="${PREFIX}/include"

mkdir -p build
cd build
cmake \
    -DPOCO=ON \
    -DCMAKE_INSTALL_PREFIX:PATH="${PREFIX}" \
    -DCMAKE_CXX_COMPILER="${CXX}" \
    -DCMAKE_C_COMPILER="${CC}" \
    ..
cmake --build . --config Release
cmake --install . --config Release
