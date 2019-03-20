#include <iostream>
#include "VddkTargetLib.h"

using std::cout;
using pio::ArmVddkLib;
using std::string;

namespace pio {

void ReadCallBack(CallbackData &cbData, VixError result) {
	cout << "Read Callback called\n";
}

void WriteCallBack(CallbackData &cbData, VixError result) {
	cout << "Write Callback called\n";
}

void ArmVddkLib::AsyncRead(
		unsigned long int offset, size_t size, uint8 *buf) {
	VixDiskLib_ReadAsync(handle_, offset, size, buf, (VixDiskLibCompletionCB)ReadCallBack, NULL/*(void *)cbData*/);
	VixDiskLib_Wait(handle_);
}

void ArmVddkLib::AsyncWrite(
		unsigned long int offset, size_t size, uint8 *buf) {
	VixDiskLib_WriteAsync(handle_, offset, size, buf, (VixDiskLibCompletionCB)WriteCallBack, NULL /*(void *)cbData*/);
	VixDiskLib_Wait(handle_);
}

}
