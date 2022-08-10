set(ThriftFile
	StorRpc.thrift
)

set(ThriftRpcFiles
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpcAsyncClient.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_constants.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_data.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_metadata.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_processmap_binary.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_processmap_compact.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_types.cpp"
)

add_custom_command(
	OUTPUT ${ThriftRpcFiles}
	COMMAND cp "${CMAKE_CURRENT_SOURCE_DIR}/${ThriftFile}" .
	COMMAND ${THRIFT} --gen mstch_cpp2 -o . ${ThriftFile}
	DEPENDS ${ThriftFile}
)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(StorRpcLib
	${ThriftRpcFiles}
)

install(TARGETS StorRpcLib DESTINATION "/usr/lib/hyc")