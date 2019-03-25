#include <iostream>
#include "VddkLib.h"

using std::cout;
using pio::ArmVddkFile;
using std::string;

namespace pio {

void ReadCallBack(CallbackData &cbData, VixError result) {
	cout << "Read Callback called\n";
}

void WriteCallBack(CallbackData &cbData, VixError result) {
	cout << "Write Callback called\n";
}

void ArmVddkFile::AsyncRead(
		unsigned long int offset, size_t size, uint8 *buf, VixDiskLibCompletionCB ReadCallBack, void *cbData) {
	VixDiskLib_ReadAsync(handle_, offset, size, buf, (VixDiskLibCompletionCB)ReadCallBack, cbData);
	VixDiskLib_Wait(handle_);
}

void ArmVddkFile::AsyncWrite(
		unsigned long int offset, size_t size, uint8 *buf, VixDiskLibCompletionCB WriteCallBack, void *cbData) {
	VixDiskLib_WriteAsync(handle_, offset, size, buf, (VixDiskLibCompletionCB)WriteCallBack, cbData);
	VixDiskLib_Wait(handle_);
}

}
