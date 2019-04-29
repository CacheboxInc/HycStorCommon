#pragma once

#include <folly/io/async/EventBase.h>

#include "RequestHandler.h"

namespace pio {
class LibAio;

class FileCacheHandler : public RequestHandler {
public:
	static constexpr char kName[] = "FileCache";
	FileCacheHandler(const config::VmdkConfig* configp);
	virtual ~FileCacheHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkWrite(ActiveVmdk* vmdkp,
		::ondisk::CheckPointID ckpt,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkRead(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> BulkReadPopulate(ActiveVmdk* vmdkp,
		const std::vector<std::unique_ptr<Request>>& requests,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock*>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	const std::string& GetFileCachePath() const;

private:
	folly::Future<int> Read(const size_t block_size,
		const std::vector<RequestBlock*> process,
		std::vector<RequestBlock*>& failed);
	folly::Future<int> Write(const size_t block_size,
		const std::vector<RequestBlock*> process,
		std::vector<RequestBlock*>& failed);
private:
	folly::EventBase base_{};
	bool enabled_{false};
	std::string file_path_;
	int fd_;

	std::unique_ptr<std::thread> thread_;
	std::unique_ptr<LibAio> libaio_;
};

}
