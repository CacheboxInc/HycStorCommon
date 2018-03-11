include_directories(include)

set(HycStorCommonFiles
	SpinLock.cpp
	TimePoint.cpp
)

add_library(HycStorCommon
	${HycStorCommonFiles}
)

add_subdirectory(tests)