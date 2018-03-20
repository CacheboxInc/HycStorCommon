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
	- cmake is also needed
		#sudo apt-get install cmake

1. Clone the repository.
	#git clone --recursive git@github.com:CacheboxInc/hyc-storage-layer.git

	There are 2 third party dependency modules linked as submodules in
	hyc-storage-layer/thirdparty,
		- CRoaring
		- restbed

2. hyc-storage-layer depends on facebooks folly library.

	Follow https://github.com/CacheboxInc/folly/README.md file for
	installation instructions.

3. Build hyc-storage-layer
	Building hyc-storage-layer first requires to build all the thirdparty
	dependencies,
		i. CRoaring build
			#cd hyc-storage-layer/thirdparty/CRoaring
			#mkdir build
			#cd build
			#cmake ..
			#make -j $(nproc)
			#make install
		ii. restbed build
			#cd hyc-storage-layer/thirdparty/restbed
			#mkdir build
			#cd build
			#cmake ..
			#make -j $(nproc)
			#make install

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

