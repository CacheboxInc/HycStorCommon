set(ThriftLibs
	thrift-core
	protocol
	concurrency
	transport
	async
	security
	server
	thriftfrozen2
	thriftprotocol
	thriftcpp2
	wangle
)

set(COMPRESSION_LIBS
	lzma
	z
	lz4
	snappy
	bz2
)

set(LIBRARY_LIST
	gflags
	glog
	folly
	pthread
	boost_system
	boost_context
	boost_thread
	dl
	event
	double-conversion
	iberty
	atomic
	unwind
	rt
	${ThriftLibs}
)

add_subdirectory(thrift)
add_subdirectory(CommonLib)
add_subdirectory(thrift-client)
