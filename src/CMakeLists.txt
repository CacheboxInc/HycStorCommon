include_directories(include)

set(LIBRARY_LIST
	gflags
	glog
	folly
	pthread
	roaring
	boost_system
)

set(StorageLayerFiles
	TgtInterfaceImpl.cpp

	VirtualMachine.cpp
	Vmdk.cpp

	RangeLock.cpp
	Request.cpp
	RamCache.cpp

	LockHandler.cpp
	RequestHandler.cpp
	UnalignedHandler.cpp
	RamCacheHandler.cpp
	BlockTraceHandler.cpp
)

add_library(StorageLayer SHARED
	${StorageLayerFiles}
)

target_link_libraries(StorageLayer
	CommonLib
	${LIBRARY_LIST}
)

add_subdirectory(Common)
add_subdirectory(tests)
add_subdirectory(C-Client)