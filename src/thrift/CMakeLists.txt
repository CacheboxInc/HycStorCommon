set(ThriftFile
	StorRpc.thrift
)

set(ThriftRpcFiles
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_client.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_constants.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_data.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_processmap_binary.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_processmap_compact.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/StorRpc_types.cpp"
)

add_custom_command(
	OUTPUT ${ThriftRpcFiles}
	COMMAND cp "${CMAKE_CURRENT_SOURCE_DIR}/${ThriftFile}" .
	COMMAND ${THRIFT} --gen mstch_cpp2 -o .
		--templates "${THRIFT_TEMPLATE}"
		${ThriftFile}
	DEPENDS ${ThriftFile}
)

include_directories(${CMAKE_CURRENT_BINARY_DIR})

add_library(StorRpcLib
	${ThriftRpcFiles}
)

set(ThriftMetaDataFile
	MetaData.thrift
)

set(ThriftMetaDataFiles
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/MetaData_constants.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/MetaData_data.cpp"
	"${CMAKE_CURRENT_BINARY_DIR}/gen-cpp2/MetaData_types.cpp"
)

add_custom_command(
	OUTPUT ${ThriftMetaDataFiles}
	COMMAND cp "${CMAKE_CURRENT_SOURCE_DIR}/${ThriftMetaDataFile}" .
	COMMAND ${THRIFT} --gen mstch_cpp2:json -o .
		--templates "${THRIFT_TEMPLATE}"
		${ThriftMetaDataFile}
	DEPENDS ${ThriftMetaDataFile}
)

add_library(StorRpcMetaDataLib
	${ThriftMetaDataFiles}
)