include_directories(include)

set(HycStorCommonFiles
	SpinLock.cpp
)

add_library(HycStorCommon
	${HycStorCommonFiles}
)

add_subdirectory(tests)