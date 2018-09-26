#pragma once

namespace pio {

class StorD {
public:
	StorD();
	~StorD();
	int InitStordLib(void);
	int DeinitStordLib(void);

private:
	struct {
		std::once_flag initialized_;
		std::once_flag deinitialized_;
	} g_init_;
};

}
