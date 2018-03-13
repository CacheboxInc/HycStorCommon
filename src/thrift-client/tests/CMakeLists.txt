add_executable(TgtInterfaceImplTest
	TestMain.cpp
	TgtInterfaceImplTests.cpp
)

target_link_libraries(TgtInterfaceImplTest
	-Wl,--start-group
	gtest
	gflags

	ssl
	crypto
	double-conversion
	gss
	gssapi_krb5
	zstd

	HycStorCommon
	StorRpcLib
	HycClientLib
	${COMPRESSION_LIBS}
	${LIBRARY_LIST}
	-Wl,--end-group
)

ADD_TEST(TgtInterfaceImplTest TgtInterfaceImplTest ${PROJECT_SOURCE_DIR})