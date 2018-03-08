add_executable(HycStorCommonTests
	TestMain.cpp
	SpinLockTests.cpp
)

target_link_libraries(HycStorCommonTests
	gtest
	HycStorCommon
	${LIBRARY_LIST}
)

ADD_TEST(HycStorCommonTests HycStorCommonTests ${PROJECT_SOURCE_DIR})
