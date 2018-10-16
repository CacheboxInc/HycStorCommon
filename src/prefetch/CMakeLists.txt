include_directories(include)
include_directories(${CMAKE_CURRENT_SOURCE_DIR}/third_party/murmur3-master/)
link_directories(${CMAKE_CURRENT_SOURCE_DIR}/thirdparty/murmur3-master/)

execute_process(
	COMMAND make
	WORKING_DIRECTORY ${CMAKE_CURRENT_SOURCE_DIR}/third_party/
)

set(ReadAheadFiles
	ghb.c
	dc.c
)

add_library(ReadAhead STATIC
	${ReadAheadFiles}
)

target_link_libraries(ReadAhead
	murmur3
)
