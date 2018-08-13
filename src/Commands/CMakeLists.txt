include_directories(include)

add_executable(VmConfig
	VmConfig.cpp
)

target_link_libraries(VmConfig
	-Wl,--start-group
	CommonLib
	StorageLayer
	${LIBRARY_LIST}
	-Wl,--end-group
)

add_executable(VmdkConfig
	VmdkConfig.cpp
)

target_link_libraries(VmdkConfig
	-Wl,--start-group
	CommonLib
	StorageLayer
	${LIBRARY_LIST}
	-Wl,--end-group
)

add_executable(AeroConfig
	AeroConfig.cpp
)

target_link_libraries(AeroConfig
	-Wl,--start-group
	CommonLib
	StorageLayer
	${LIBRARY_LIST}
	-Wl,--end-group
)
