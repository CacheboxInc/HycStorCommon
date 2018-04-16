add_executable(StorageLayerTests
	TestMain.cpp
	RangeLockTests.cpp
	RequestTests.cpp
	VmdkTests.cpp
	RamCacheTests.cpp
	UnalignedHandlerTests.cpp
	RoaringBitmapTest.cpp
	FileCacheTests.cpp
	ErrorHandlerTests.cpp
	SuccessHandlerTests.cpp
	TgtInterfaceTests.cpp
)

target_link_libraries(StorageLayerTests
	-Wl,--start-group
	CommonLib
	StorRpcLib
	StorageLayer
	gtest
	lzma
	ssl
	${COMPRESS_LIBS}
	${LIBRARY_LIST}
	-Wl,--end-group
)

ADD_TEST(StorageLayerTests StorageLayerTests ${PROJECT_SOURCE_DIR})
