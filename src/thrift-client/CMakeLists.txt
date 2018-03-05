include_directories(${CMAKE_BINARY_DIR}/src/thrift/)

set(HycClientLibFiles
	TgtInterfaceImpl.cpp
)

add_library(HycClientLib
	${HycClientLibFiles}
)