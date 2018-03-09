include_directories(include)

set(CommonLibFiles
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

# Install the storage library
install(TARGETS CommonLib DESTINATION "/usr/lib/hyc")
