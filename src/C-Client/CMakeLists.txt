set(CMAKE_VERBOSE_MAKEFILE on)

include_directories(include)

add_executable(TestClient
	TestClient.c
)

target_link_libraries(TestClient
	StorageLayer
	${LIBRARY_LIST}
)