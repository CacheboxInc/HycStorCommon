set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(include)
include_directories(${PROJECT_SOURCE_DIR}/src/RestServer/include)

add_executable(TestClient
	TestClient.c
)

target_link_libraries(TestClient
	StorageLayer
	RestServer
	${LIBRARY_LIST}
)