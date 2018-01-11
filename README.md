# hyc-storage-layer
HyC Storage Layer Code

1. Install latest version of facebook's folly library

2. Run ``setup.sh``

3. Run all tests using

``cd build; make test``

4. Get list of tests

``cd build; ./src/tests/StorageLayerTests --gtest_list_tests``

5. Run Roaring Bitmap Time Benchmark

`cd build; ./src/tests/StorageLayerTests --gtest_filter=RoaringBitmapSpaceBenchmark.*`

6. Run Roaring Bitmap Space Benchmar

``cd build; ./src/tests/StorageLayerTests --gtest_filter=RoaringBitmapSpaceBenchmark.*``
