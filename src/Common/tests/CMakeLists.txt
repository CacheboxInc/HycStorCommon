add_executable(CommonTests
	TestMain.cpp
	SystemTests.cpp
	UtilsTests.cpp
	JsonConfigTests.cpp
	SpinLockTests.cpp
	QLockTests.cpp
	SingletonTests.cpp
	RendezTests.cpp
	BackGroundWorkTests.cpp
	WorkSchedulerTests.cpp
)

target_link_libraries(CommonTests
	-Wl,--start-group
	gtest
	CommonLib
	${LIBRARY_LIST}
	${HA_LIBS}
	-Wl,--end-group
)

ADD_TEST(CommonTests CommonTests ${PROJECT_SOURCE_DIR})
