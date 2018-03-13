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
	CommonLib
	StorageLayer
	gtest
	${LIBRARY_LIST}
)

ADD_TEST(StorageLayerTests StorageLayerTests ${PROJECT_SOURCE_DIR})
