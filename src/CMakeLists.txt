include_directories(include)

set(LIBRARY_LIST
	gflags
	glog
	folly
	pthread
	roaring
	boost_system
	boost_context
	boost_thread
	dl
	event
	double-conversion
	iberty
)

set(StorageLayerFiles
	TgtInterfaceImpl.cpp

	VirtualMachine.cpp
	VmConfig.cpp
	Vmdk.cpp
	VmdkConfig.cpp

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
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/FileCacheHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/ErrorHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/SuccessHandler.cpp"
)

add_library(StorageLayer STATIC
	${StorageLayerFiles}
)

target_link_libraries(StorageLayer
	CommonLib
	${LIBRARY_LIST}
)

add_subdirectory(RestServer)
add_subdirectory(Common)
add_subdirectory(Commands)
add_subdirectory(tests)
add_subdirectory(C-Client)

# Install the storage library
install(TARGETS StorageLayer DESTINATION "/usr/lib/hyc")
