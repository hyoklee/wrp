rm -rf poco
mkdir poco
cd poco
if hostname | grep -q polaris; then
    cmake -DPOCO=ON \
          -DCMAKE_C_COMPILER=nvc -DCMAKE_CXX_COMPILER=nvc++ \
          -DCMAKE_CXX_FLAGS="--gcc-toolchain=$(dirname $(dirname $(realpath $(which gcc-13))))" \
          -DSITE="polaris" -DBUILDNAME="omni/nvhpc" ..
else
    cmake -DPOCO=ON -DSITE="ubu-24.04/WSL" -DBUILDNAME="omni/r" ..
fi
ctest -T Build
ctest -C Release -T  Test
cd ..
