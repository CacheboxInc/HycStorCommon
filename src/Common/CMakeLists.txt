include_directories(include)

set(CommonLibFiles
	JsonConfig.cpp
	ThreadPool.cpp
	Utils.cpp
)

add_library(CommonLib SHARED
	${CommonLibFiles}
)

add_subdirectory(tests)