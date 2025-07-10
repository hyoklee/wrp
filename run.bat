rmdir /q /s run
mkdir run
cd run
cmake -DCMAKE_TOOLCHAIN_FILE=C:/src/vcpkg.microsoft/scripts/buildsystems/vcpkg.cmake -DSITE="win-11" -DBUILDNAME="omni/r" ..
REM ctest -D Experimental -C Release
ctest -T Build
ctest -C Release -T  Test
cd ..







