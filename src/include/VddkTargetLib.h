#pragma once
#include <dlfcn.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include <vector>
#include <stdexcept>
#include <algorithm>
#include <list>
#include <memory>

#include "vixDiskLib.h"

using std::string;

namespace pio {

#define VIXDISKLIB_VERSION_MAJOR 6
#define VIXDISKLIB_VERSION_MINOR 7

#define THROW_ERROR(vixError) \
   throw VixDiskLibErrWrapper((vixError), __FILE__, __LINE__)

#define CHECK_AND_THROW_2(vixError, buf)                             \
   do {                                                              \
      if (VIX_FAILED((vixError))) {                                  \
         delete[] buf;                                               \
         throw VixDiskLibErrWrapper((vixError), __FILE__, __LINE__); \
      }                                                              \
   } while (0)

#define CHECK_AND_THROW(vixError) CHECK_AND_THROW_2(vixError, ((int*)0))

struct CallbackData {
	uint8 Buffer[VIXDISKLIB_SECTOR_SIZE];
	unsigned long long Count;
};

class VixDiskLibErrWrapper
{
public:
    explicit VixDiskLibErrWrapper(VixError errCode, const char* file, int line)
          :
          errCode_(errCode),
          file_(file),
          line_(line)
    {
        char* msg = VixDiskLib_GetErrorText(errCode, NULL);
        desc_ = msg;
        VixDiskLib_FreeErrorText(msg);
    }

    VixDiskLibErrWrapper(const char* description, const char* file, int line)
          :
         errCode_(VIX_E_FAIL),
         desc_(description),
         file_(file),
         line_(line)
    {
    }

    std::string Description() const { return desc_; }
    VixError ErrorCode() const { return errCode_; }
    std::string File() const { return file_; }
    int Line() const { return line_; }

private:
    VixError errCode_;
    std::string desc_;
    std::string file_;
    int line_;
};

class ArmVmVcConnection {
public:
	ArmVmVcConnection(char *password, char *username, char* thumbprint, char* vmId, char* vcName):
				password_(password), username_(username), thumbprint_(thumbprint),
				vmId_(vmId), vcName_(vcName) {
		cnxParams_ = VixDiskLib_AllocateConnectParams();
		cnxParams_->credType = VIXDISKLIB_CRED_UID;
		cnxParams_->creds.uid.password = password_;
		cnxParams_->creds.uid.userName = username_;
		cnxParams_->thumbPrint = thumbprint_;
		cnxParams_->specType = VIXDISKLIB_SPEC_VMX;
		cnxParams_->vmxSpec = vmId_;
		cnxParams_->port = 443;
		cnxParams_->nfcHostPort = 902;
		cnxParams_->serverName = vcName_;

		VixError vixError = VixDiskLib_ConnectEx(cnxParams_, false, NULL, "nbdssl", &conn);
		CHECK_AND_THROW(vixError);
	}
	~ArmVmVcConnection() {
		VixDiskLib_Disconnect(conn);
		VixDiskLib_FreeConnectParams(cnxParams_);
		cnxParams_ = NULL;
	}
	VixDiskLibConnection conn;
private:
	VixDiskLibConnectParams *cnxParams_;
	char *password_;
	char *username_;
	char *thumbprint_;
	char *vmId_;
	char *vcName_;
};

class ArmVddkFile {
public:
	ArmVddkFile(VixDiskLibConnection Conn,
			const char* VmdkPath): conn_(Conn){
		handle_ = NULL;
		VixError vixError = VixDiskLib_Open(Conn, VmdkPath, 0, &handle_);
		CHECK_AND_THROW(vixError);
	}
	~ArmVddkFile(){
		VixDiskLib_Close(handle_);
		handle_ = NULL;
	}

	void AsyncRead(unsigned long int, size_t, uint8 *);
	void AsyncWrite(unsigned long int, size_t, uint8 *);

private:
	VixDiskLibConnection conn_;
	VixDiskLibHandle handle_;
};

}
