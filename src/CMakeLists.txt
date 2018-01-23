include_directories(include)

set(LIBRARY_LIST
	gflags
	glog
	folly
	pthread
	roaring
)

add_library(StorageLayer SHARED
	Utils.cpp

	TgtInterfaceImpl.cpp

	VirtualMachine.cpp
	Vmdk.cpp

	RangeLock.cpp
	Request.cpp
	RamCache.cpp

	LockHandler.cpp
	RequestHandler.cpp
	UnalignedHandler.cpp
	RamCacheHandler.cpp

	ThreadPool.cpp
)


add_subdirectory(tests)
add_subdirectory(C-Client)