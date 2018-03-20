include_directories(include)
include_directories(${CMAKE_BINARY_DIR}/src/thrift/)

set(RestServerFiles
	HycRestServer.cpp
)

add_library(RestServer STATIC
	${RestServerFiles}
)

target_link_libraries(RestServer
	restbed
)

add_subdirectory(tests)

# Install the REST server header files
install(DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/include"
	DESTINATION "${CMAKE_INSTALL_PREFIX}/include/hyc")

# Install the REST server
install(TARGETS RestServer DESTINATION "/usr/lib/hyc")
