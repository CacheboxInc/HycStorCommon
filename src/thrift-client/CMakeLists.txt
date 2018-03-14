include_directories(${CMAKE_BINARY_DIR}/src/thrift/)

set(HycClientLibFiles
	TgtInterfaceImpl.cpp
)

add_library(HycClientLib
	${HycClientLibFiles}
)

add_subdirectory(tests)

install(TARGETS HycClientLib DESTINATION "/usr/lib/hyc")
