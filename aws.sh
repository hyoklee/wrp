rm -rf aws
mkdir aws
cd aws
cmake \
    -DCMAKE_TOOLCHAIN_FILE=/home/hyoklee/vcpkg/scripts/buildsystems/vcpkg.cmake \
    -DAWS=ON -DPOCO=ON -DSITE="ubu-24.04/WSL" -DBUILDNAME="omni/r/poco+aws" \
    ..
ctest -T Build
ctest -C Release -T  Test
cd ..







