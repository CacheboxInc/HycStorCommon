add_executable(HycStorCommonTests
	TestMain.cpp
	SystemTests.cpp
	JsonConfigTests.cpp
	SpinLockTests.cpp
	QLockTests.cpp
	SingletonTests.cpp
)

target_link_libraries(HycStorCommonTests
	gtest
	HycStorCommon
	${LIBRARY_LIST}
)

ADD_TEST(HycStorCommonTests HycStorCommonTests ${PROJECT_SOURCE_DIR})
