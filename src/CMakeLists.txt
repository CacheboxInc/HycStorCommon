include_directories(include)

set(LIBRARY_LIST
	gflags
	glog
	folly
	pthread
	roaring
)

set(StorageLayerFiles
	"${CMAKE_SOURCE_DIR}/src/Common/JsonConfig.cpp"
	"${CMAKE_SOURCE_DIR}/src/Common/TgtInterfaceImpl.cpp"
	"${CMAKE_SOURCE_DIR}/src/Common/ThreadPool.cpp"
	"${CMAKE_SOURCE_DIR}/src/Common/Utils.cpp"

	VirtualMachine.cpp
	Vmdk.cpp

	RangeLock.cpp
	Request.cpp
	RamCache.cpp

	LockHandler.cpp
	RequestHandler.cpp
	UnalignedHandler.cpp
	RamCacheHandler.cpp
)

add_library(StorageLayer SHARED
	${StorageLayerFiles}
)

add_subdirectory(tests)
add_subdirectory(C-Client)