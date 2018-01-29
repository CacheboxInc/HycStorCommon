include_directories(include)

set(CommonLibFiles
	ConfigConsts.cpp
	JsonConfig.cpp
	ThreadPool.cpp
	Utils.cpp
	SpinLock.cpp
	QLock.cpp
)

add_library(CommonLib SHARED
	${CommonLibFiles}
)

add_subdirectory(tests)