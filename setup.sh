#!/bin/bash

git submodule init
git submodule update

MAIN=${PWD}

echo "************************Building thrift************************"
cd src/thrift
rm -rf build
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel
make -j 8
make install
cd ../../..

echo "************************Building CRoaring**********************"
cd thirdparty/CRoaring
rm -rf build
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel
make -j 8 
make install
cd ../../..

echo "***********************Building ha-lib***************************"
cd thirdparty/ha-lib
cd third-party
make
cd ..
rm -rf build
mkdir build
cd build
cmake .. -DCMAKE_BUILD_TYPE=MinSizeRel
make -j 8

echo "***********************Building prefetch***************************"
cd src/prefetch
make -C third_party
make

echo "***********************Building stord*****************************"
cd ${MAIN}
rm -rf build
mkdir build
cd build
cmake -DUSE_NEP=OFF .. -DCMAKE_BUILD_TYPE=Debug
make -j 8

echo "Done!"
