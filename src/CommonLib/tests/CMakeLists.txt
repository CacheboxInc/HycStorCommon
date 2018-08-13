add_executable(HycStorCommonTests
	TestMain.cpp
	SpinLockTests.cpp
)

target_link_libraries(HycStorCommonTests
	-Wl,--start-group
	gtest
	HycStorCommon
	${LIBRARY_LIST}
	-Wl,--end-group
)

ADD_TEST(HycStorCommonTests HycStorCommonTests ${PROJECT_SOURCE_DIR})
