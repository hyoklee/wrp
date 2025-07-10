rm -rf run
mkdir run
cd run
cmake -DCMAKE_TOOLCHAIN_FILE=/home/hyoklee/vcpkg/scripts/buildsystems/vcpkg.cmake -DSITE="ubu-24.04/WSL" -DBUILDNAME="omni/r" ..
ctest -T Build
ctest -C Release -T  Test
cd ..







