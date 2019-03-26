#
# Locate MAKE, which is needed to create some of the runtime headers.
#
message(STATUS "Check for M4 macro processor")
find_program(MAKE
	NAMES "make${CMAKE_EXECUTABLE_SUFFIX}" "make${CMAKE_EXECUTABLE_SUFFIX}"
	DOC "Location of the make"
)
if(NOT MAKE)
	message(FATAL_ERROR "make not found")
endif()
message(STATUS "Check for MAKE: found ${MAKE}")

include_directories(include)
include_directories(${CMAKE_SOURCE_DIR}/src/DataMover)
include_directories(${CMAKE_BINARY_DIR}/src/thrift/)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/ha-lib/third-party/include)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/ha-lib/src)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/vk-proto/ioa/client/)
include_directories(${CMAKE_SOURCE_DIR}/thirdparty/hyc-datapack/src/include)

link_directories(${CMAKE_BINARY_DIR}/thirdparty/hyc-datapack/src/compression/)
link_directories(${CMAKE_BINARY_DIR}/thirdparty/hyc-datapack/src/encryption/)
link_directories(${CMAKE_BINARY_DIR}/lib/)

#Set this to OFF if don't want to include NEP changes
OPTION(USE_NEP "Set ON/OFF to use/avoid NEP with storD" ON)
if(USE_NEP)
    include_directories(/usr/include/hyc-net)
    link_directories(/usr/lib/x86_64-linux-gnu/hyc-net/)
    link_directories(/usr/lib/x86_64-linux-gnu/hyc-vddk/)
    add_definitions(-DUSE_NEP)
endif(USE_NEP)

OPTION(FILETARGET_ASYNC "To Use Async FileTarget Inteface. Set to OFF to use sync interface" ON)
if(FILETARGET_ASYNC)
    add_definitions(-DFILETARGET_ASYNC)
endif(FILETARGET_ASYNC)

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

set(HYC_COMPRESS_LIBS
	HycCompress
	snappy
	lz4
	z
	bz2
	zstd
)

set(HYC_ENCRYPT_LIBS
	HycEncryption
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
	ArmVddkAsync
	task
	vaiorpc
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
	unwind
	atomic
	aio
	ReadAhead
	unwind
	${ThriftLibs}
	${HYC_COMPRESS_LIBS}
	${HYC_ENCRYPT_LIBS}
	jansson
)

set(HA_LIBS
	ha
	base64
	curl
	jansson
	microhttpd
	orcania
	ulfius
	yder
)

set(IOA_LIB_LIST
	${CMAKE_SOURCE_DIR}/thirdparty/vk-proto/ioa/client/build/64/libioaclient.a
	${CMAKE_SOURCE_DIR}/thirdparty/vk-proto/ioa/client/build/64/libjansson.a
	${CMAKE_SOURCE_DIR}/thirdparty/vk-proto/ioa/client/build/64/libmurmur3.a
	${CMAKE_SOURCE_DIR}/thirdparty/vk-proto/ioa/client/build/64/softfloat.a
)

add_custom_command(
	OUTPUT ${IOA_LIB_LIST}
	COMMAND ${MAKE} -C ${CMAKE_SOURCE_DIR}/thirdparty/vk-proto/ioa/client/
	COMMENT "BUILDING IOA_LIB_LIST"
)

if(USE_NEP)
	set (LIBRARY_LIST
		${LIBRARY_LIST}
		${NEP_LIBRARY_LIST}
	)
endif(USE_NEP)

set(StorageLayerFiles
	TgtInterfaceImpl.cpp
	VmdkCacheStats.cpp
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
	ScanManager.cpp
	ScanInstance.cpp
	FlushConfig.cpp
	AeroFiberThreads.cpp
	SetCkptBmapConfig.cpp
	RangeLock.cpp
	Request.cpp
	RamCache.cpp
	MetaDataKV.cpp
	ReadAhead.cpp
	Analyzer.cpp
	Rest.cpp

	"${CMAKE_SOURCE_DIR}/src/DataMover/SyncCookie.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/CheckPointTraverser.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/DataReader.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/DataWriter.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/DataCopier.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/DataSync.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/VmSync.cpp"
	"${CMAKE_SOURCE_DIR}/src/DataMover/ArmSync.cpp"

	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/RequestHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/UnalignedHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/RamCacheHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/BlockTraceHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/CacheTargetHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/DirtyHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/CleanHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/EncryptHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/CompressHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/FileCacheHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/ErrorHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/SuccessHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/AeroOps.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/FileTargetHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/MultiTargetHandler.cpp"
	"${CMAKE_SOURCE_DIR}/src/RequestHandlers/VddkTargetHandler.cpp"
)

if(USE_NEP)
	set (StorageLayerFiles
		${StorageLayerFiles}
		"${CMAKE_SOURCE_DIR}/src/RequestHandlers/NetworkTargetHandler.cpp"
	)
endif(USE_NEP)


add_library(StorageLayer STATIC
	${StorageLayerFiles}
	${IOA_LIB_LIST}
)

target_link_libraries(StorageLayer
	CommonLib
	StorRpcLib
	StorRpcMetaDataLib
	${LIBRARY_LIST}
	${IOA_LIB_LIST}
)

add_subdirectory(thrift)
add_subdirectory(Commands)
add_subdirectory(tests)
add_subdirectory(stord)
add_subdirectory(Common)
add_subdirectory(prefetch)
add_dependencies(CommonLib StorRpcMetaDataLib)


# Install the storage library
#install(TARGETS StorageLayer DESTINATION "/usr/lib/hyc")
