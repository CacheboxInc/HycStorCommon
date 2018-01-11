#!/bin/bash

git submodule init
git submodule update

MAIN=${PWD}

cd thirdparty/CRoaring
mkdir build
cd build
cmake ..
make
make install

cd ${MAIN}

mkdir build
cd build
cmake -DCMAKE_BUILD_TYPE=ASan ..
make
