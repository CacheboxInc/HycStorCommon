set(LIBRARY_LIST
	gflags
	glog
	folly
	pthread
	boost_system
	boost_context
	boost_thread
)

add_subdirectory(tests)
add_subdirectory(thrift)
add_subdirectory(CommonLib)
