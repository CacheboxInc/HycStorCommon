set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(include)
include_directories(${PROJECT_SOURCE_DIR}/src/RestServer/include)
include_directories(${CMAKE_BINARY_DIR}/src/thrift/)
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/ha-lib/third-party/include)
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/ha-lib/src)

add_executable(stord
	StorageDaemon.cpp
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

target_link_libraries(stord
	-Wl,--start-group
	lzma
	ssl
	crypto
	double-conversion
	gss
	gssapi_krb5
	CommonLib
	StorRpcLib
	StorRpcMetaDataLib
	StorageLayer
	${HA_LIBS}
	${COMPRESS_LIBS}
	${LIBRARY_LIST}
	${AEROSPIKE_LIBS}
	-Wl,--end-group
)

install(TARGETS stord DESTINATION "${CMAKE_INSTALL_PREFIX}/hyc/bin")
