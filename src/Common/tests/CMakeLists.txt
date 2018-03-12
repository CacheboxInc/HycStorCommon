add_executable(CommonTests
	TestMain.cpp
	SystemTests.cpp
	UtilsTests.cpp
	JsonConfigTests.cpp
	SpinLockTests.cpp
	QLockTests.cpp
	SingletonTests.cpp
	RendezTests.cpp
)

target_link_libraries(CommonTests
	gtest
	CommonLib
	${LIBRARY_LIST}
)

ADD_TEST(CommonTests CommonTests ${PROJECT_SOURCE_DIR})