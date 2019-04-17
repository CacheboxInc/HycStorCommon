set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(include)
include_directories(${PROJECT_SOURCE_DIR}/src/RestServer/include)
include_directories(${CMAKE_BINARY_DIR}/src/thrift/)
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/ha-lib/third-party/include)
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/ha-lib/src)

add_executable(stord
	StorageDaemon.cpp
)

target_link_libraries(stord
	-Wl,--start-group
	lzma
	ssl
	crypto
	double-conversion
	gssapi_krb5
	CommonLib
	StorRpcLib
	StorRpcMetaDataLib
	StorageLayer
	unwind
	${HA_LIBS}
	${COMPRESS_LIBS}
	${LIBRARY_LIST}
	${AEROSPIKE_LIBS}
	${IOA_LIB_LIST}
	-Wl,--end-group
)

install(TARGETS stord DESTINATION "${CMAKE_INSTALL_PREFIX}/hyc/bin")
