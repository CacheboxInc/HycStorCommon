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