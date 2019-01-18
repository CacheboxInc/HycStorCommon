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
	VirtualMachineTests.cpp
	CompressHandlerTest.cpp
	EncryptHandlerTest.cpp
	ReadAheadTests.cpp
)

if(USE_NEP)
set (LIBRARY_LIST
	${LIBRARY_LIST}
	${NEP_LIBRARY_LIST}
)
endif(USE_NEP)

target_link_libraries(StorageLayerTests
	-Wl,--start-group
	CommonLib
	StorRpcLib
	StorageLayer
	StorRpcMetaDataLib
	gtest
	lzma
	ssl
	${COMPRESS_LIBS}
	${LIBRARY_LIST}
	${AEROSPIKE_LIBS}
	${IOA_LIB_LIST}
	${HA_LIBS}
	-Wl,--end-group
)

ADD_TEST(StorageLayerTests StorageLayerTests ${PROJECT_SOURCE_DIR})
