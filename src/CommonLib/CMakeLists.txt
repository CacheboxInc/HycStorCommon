include_directories(include)

set(HycStorCommonFiles
	JsonConfig.cpp
	ThreadPool.cpp
	SpinLock.cpp
	QLock.cpp
)

add_library(HycStorCommon
	${HycStorCommonFiles}
)
