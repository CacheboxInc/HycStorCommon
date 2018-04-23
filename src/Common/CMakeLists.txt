include_directories(include)

set(CommonLibFiles
	JsonConfig.cpp
	ThreadPool.cpp
	Utils.cpp
	SpinLock.cpp
	QLock.cpp
	Rendez.cpp
	BackGroundWorker.cpp
)

add_library(CommonLib STATIC
	${CommonLibFiles}
)

add_subdirectory(tests)

# Install the storage library
#install(TARGETS CommonLib DESTINATION "/usr/lib/hyc")
