rmdir /q /s aws
mkdir aws
cd aws
cmake -DAWS=ON -DPOCO=ON -DCMAKE_TOOLCHAIN_FILE=C:/src/vcpkg.microsoft/scripts/buildsystems/vcpkg.cmake -DSITE="win-11" -DBUILDNAME="omni/r/aws+poco" ..
REM ctest -D Experimental -C Release
ctest -T Build
ctest -C Release -T  Test
cd ..
