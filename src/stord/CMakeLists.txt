set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(include)
include_directories(${PROJECT_SOURCE_DIR}/src/RestServer/include)
include_directories(${CMAKE_BINARY_DIR}/src/thrift/)

add_executable(stord
	StorageDaemon.cpp
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
	StorageLayer
	RestServer
	${COMPRESS_LIBS}
	${LIBRARY_LIST}
	-Wl,--end-group
)