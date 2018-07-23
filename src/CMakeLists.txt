include_directories(include)
include_directories(${CMAKE_BINARY_DIR}/src/thrift/)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/ha-lib/third-party/include)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/ha-lib/src)

#Set this to OFF if don't want to include NEP changes
OPTION(USE_NEP "Set ON/OFF to use/avoid NEP with storD" ON)
if(USE_NEP)
    include_directories(/usr/include/hyc-net)
    link_directories(/usr/lib/x86_64-linux-gnu/hyc-net/)
    link_directories(/usr/lib/x86_64-linux-gnu/hyc-vddk/)
    add_definitions(-DUSE_NEP)
endif(USE_NEP)

set(ThriftLibs
	thrift-core
	protocol
	concurrency
	transport
	async
	security
	server
	thriftfrozen2
	thriftprotocol
	thriftcpp2
	wangle
)

set(COMPRESS_LIBS
	snappy
	lz4
	z
	bz2
	zstd
)

set(AEROSPIKE_LIBS
	aerospike
	uv
)

set(NEP_LIBRARY_LIST
	HycNetCommon
	HycNetLayers
	HycNep
	HycTarget
	HycNetSim
	HycGwy
	HycVddkSync
	nanomsg
	anl
	jsoncpp
	vixDiskLib
)

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
	atomic
	${ThriftLibs}
)

if(USE_NEP)
	set (LIBRARY_LIST
		${LIBRARY_LIST}
		${NEP_LIBRARY_LIST}
	)
endif(USE_NEP)

set(StorageLayerFiles
	TgtInterfaceImpl.cpp

	VirtualMachine.cpp
	VmConfig.cpp
	AeroConfig.cpp
	AeroConn.cpp
	Vmdk.cpp
	VmdkConfig.cpp
	VmdkFactory.cpp
	VmManager.cpp
	FlushManager.cpp
	FlushInstance.cpp
	AeroFiberThreads.cpp

	RangeLock.cpp
	Request.cpp
	RamCache.cpp

	MetaDataKV.cpp

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
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/AeroOps.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/FileTargetHandler.cpp"
)

if(USE_NEP)
	set (StorageLayerFiles
		${StorageLayerFiles}
		"${CMAKE_SOURCE_DIR}/src/RequestHandlers/TargetHandler.cpp"
	)
endif(USE_NEP)


add_library(StorageLayer STATIC
	${StorageLayerFiles}
)

target_link_libraries(StorageLayer
	CommonLib
	StorRpcLib
	StorRpcMetaDataLib
	${LIBRARY_LIST}
)

add_subdirectory(thrift)
add_subdirectory(Common)
add_subdirectory(Commands)
add_subdirectory(tests)
add_subdirectory(stord)

# Install the storage library
#install(TARGETS StorageLayer DESTINATION "/usr/lib/hyc")
