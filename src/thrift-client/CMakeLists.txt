include_directories(${CMAKE_BINARY_DIR}/src/thrift/)

set(HycClientLibFiles
	TgtInterfaceImpl.cpp
)

add_library(HycClientLib
	${HycClientLibFiles}
)

target_link_libraries(HycClientLib
	StorRpcLib
	rt
)

add_subdirectory(tests)

install(TARGETS HycClientLib DESTINATION "/usr/lib/hyc")
