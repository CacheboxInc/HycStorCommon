include_directories(include)

set(RestServerFiles
	HycRestServer.cpp
)

add_library(RestServer SHARED
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
