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

add_executable(SimpleDataVerification
	SimpleDataVerification.cpp
)

target_link_libraries(SimpleDataVerification
	-Wl,--start-group
	${LIBRARY_LIST}
	-Wl,--end-group
)

add_executable(AeroBench
	AeroBench.cpp
)

target_link_libraries(AeroBench
	-Wl,--start-group
	CommonLib
	${AEROSPIKE_LIBS}
	gflags
	glog
	folly
	pthread
	dl
	event
	double-conversion
	atomic
	ssl
	z
	iberty
	unwind
	boost_system
	boost_context
	${LIBRARY_LIST}
	-Wl,--end-group
)

add_executable(PrefetchBench
	PrefetchBench.cpp
)

target_link_libraries(PrefetchBench
	-Wl,--start-group
	${LIBRARY_LIST}
	-Wl,--end-group
)
