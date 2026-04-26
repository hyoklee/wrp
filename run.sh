rm -rf run
mkdir run
cd run
if hostname | grep -q polaris; then
    cmake -DCMAKE_TOOLCHAIN_FILE=/home/hyoklee/vcpkg/scripts/buildsystems/vcpkg.cmake \
          -DCMAKE_C_COMPILER=nvc -DCMAKE_CXX_COMPILER=nvc++ \
          -DCMAKE_CXX_FLAGS="--gcc-toolchain=$(dirname $(dirname $(realpath $(which gcc-13))))" \
          -DSITE="polaris" -DBUILDNAME="omni/nvhpc" ..
else
    cmake -DCMAKE_TOOLCHAIN_FILE=/home/hyoklee/vcpkg/scripts/buildsystems/vcpkg.cmake \
          -DSITE="ubu-24.04/WSL" -DBUILDNAME="omni/r" ..
fi
ctest -T Build
ctest -C Release -T Test
cd ..
