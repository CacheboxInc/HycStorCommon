#pragma once

#include "RequestHandler.h"
namespace pio {

class FileTargetHandler : public RequestHandler {
public:
	FileTargetHandler(const config::VmdkConfig* configp);
	virtual ~FileTargetHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	const std::string& GetFileCachePath() const;
private:
	bool enabled_{false};
	std::string file_path_;
	int fd_{-1};
	bool create_file_{false};
};

}
