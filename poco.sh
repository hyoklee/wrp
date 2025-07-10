rm -rf poco
mkdir poco
cd poco
cmake -DPOCO=ON -DSITE="ubu-24.04/WSL" -DBUILDNAME="omni/r" ..
ctest -T Build
ctest -C Release -T  Test
cd ..







