include_directories(include)

add_executable(VmConfig
	VmConfig.cpp
)

target_link_libraries(VmConfig
	CommonLib
	StorageLayer
	${LIBRARY_LIST}
)

add_executable(VmdkConfig
	VmdkConfig.cpp
)

target_link_libraries(VmdkConfig
	CommonLib
	StorageLayer
	${LIBRARY_LIST}
)