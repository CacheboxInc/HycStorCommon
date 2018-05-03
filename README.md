```
# hyc-storage-layer
HyC Storage Layer Code

Complete setup steps :

0. Compiler setup.
	- We are using gcc/g++ 7.2 for compilation.
		#sudo add-apt-repository ppa:jonathonf/gcc-7.2
		#apt-get install gcc-7 g++-7
		#sudo update-alternatives --install /usr/bin/gcc gcc \
			/usr/bin/gcc-7 60 --slave /usr/bin/g++ g++ /usr/bin/g++-7
		#sudo update-alternatives --install /usr/bin/gcc gcc \
			/usr/bin/gcc-7 60 --slave /usr/bin/g++ g++ /usr/bin/g++-7
	- cmake is needs to be installed from source
		# tar zxf cmake-3.11.0.tar.gz && cd cmake-3.11.0 && \
			./bootstrap && make && make install

1. Clone the repository.
	#git clone --recursive git@github.com:CacheboxInc/hyc-storage-layer.git

	There are 2 third party dependency modules linked as submodules in
	hyc-storage-layer/thirdparty,
		- CRoaring
		- ha-lib

2. Install boost from source.
	- boost_1_66_0.tar.bz2
	- ./bootstrap && ./b2 && ./b2 install

3. hyc-storage-layer depends on facebooks folly library.

	Follow https://github.com/CacheboxInc/folly/README.md file for
	installation instructions.

4. stord depends on facebooks fbthrift library.
	fbthrift needs to be cloned, built & installed from
	git@github.com:CacheboxInc/fbthrift.git

	fbthrift has third-party dependencies, see below for installing them.

5. aerospike
	- libuv1-dev pkg install through apt-get
	- aerospike-client-c-libuv-4.3.10.ubuntu16.04.x86_64.tgz, contains 2
		dpkg packages, dpkg -i install.

6. Build hyc-storage-layer
	Building hyc-storage-layer first requires to build all the thirdparty
	dependencies,
		i. CRoaring build
			# cd hyc-storage-layer/thirdparty/CRoaring
			# mkdir build
			# cd build
			# cmake ..
			# make -j $(nproc)
			# make install
		ii. ha-lib build
			# cd hyc-storage-layer/thirdparty/ha-lib
			# cd third-party
			# make
			# cd ..
			# mkdir build
			# cd build
			#cmake ..
			#make -j $(nproc)

	hyc-storage-layer build
		#cd hyc-storage-layer
		#mkdir build
		#cd build
		#cmake ..
		#make -j (nproc)
		#make install

4. Run all tests using,

	#cd hyc-storage-layer/build
	#make test

4. Get list of tests

	#cd hyc-storage-layer/build
	#./src/tests/StorageLayerTests --gtest_list_tests``

5. Run Roaring Bitmap Time Benchmark

	#cd hyc-storage-layer/thirdparty/CRoaring/build
	#./src/tests/StorageLayerTests --gtest_filter=RoaringBitmapSpaceBenchmark.*`

6. Run Roaring Bitmap Space Benchmark

	#cd hyc-storage-layer/thirdparty/CRoaring/build
	#./src/tests/StorageLayerTests --gtest_filter=RoaringBitmapSpaceBenchmark.*``

7. Starting stord,
./stord -etcd_ip="http://127.0.0.1:2379" -stord_version="v1.0" -svc_label="stord_svc" -ha_svc_port=port no



Steps to install third-party needed for fbthrift,

- Download all the dependencies software,
        bison-3.0.4.tar.gz, flex-2.6.4.tar.gz, krb5-1.16.tar.gz, zlib-1.2.11.tar.gz

- Installing flex-2.6.4,
        - need to install autopoint
                #sudo apt-get install autopoint
                #apt install m4
                #./autogen.sh
                #./configure && make && make install
- Installing bison-3.0.4
        #./configure && make && make install

- Installing krb5-1.16
        #./configure && make && make install

- Installing zlib-1.2.11
        #./configure
        #make test (if this goes well)
        # make install

- Installing double-conversion
        #git clone git@github.com:google/double-conversion.git
        #apt install scons
        #scons install

- Installing mstch
        #git clone git@github.com:no1msd/mstch.git
        #cd mstch
        #mkdir build
        #cd build
        #cmake ..
        #make && make install

- Installing wangle
        Present in our CacheBoxInc org on github
        #cd wangle
        #cmake -DBUILD_SHARED_LIBS=ON .
        #make -j 4
        #ctest
        #make install
- Installing zstd
        #make
        #make install

- Installing fbthrift
        # mkdir _build
	# cd _build
	# cmake ..
	# make -j 4

#apt install libgss-dev

#apt-get install libbz2-dev


```
