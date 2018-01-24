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

	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/LockHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/RequestHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/UnalignedHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/RamCacheHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/BlockTraceHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/CacheHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/DirtyHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/CleanHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/EncryptHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/CompressHandler.cpp"
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