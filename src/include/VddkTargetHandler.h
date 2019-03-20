#pragma once

namespace pio {

/* forward declaration for pimpl */
class ArmVddkLib;

class VddkTargetLibHandler : public RequestHandler {
public:
	static constexpr char kName[] = "VddkTargetLib";
	VddkTargetLibHandler(const config::VmdkConfig* configp);
	virtual ~VddkTargetLibHandler();
	virtual folly::Future<int> Read(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> Write(ActiveVmdk *vmdkp, Request *reqp,
		::ondisk::CheckPointID ckpt, const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
	virtual folly::Future<int> ReadPopulate(ActiveVmdk *vmdkp, Request *reqp,
		const std::vector<RequestBlock*>& process,
		std::vector<RequestBlock *>& failed) override;
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
	virtual folly::Future<int> Delete(ActiveVmdk* vmdkp,
		const ::ondisk::CheckPointID ckpt_id,
		const std::pair<::ondisk::BlockID, ::ondisk::BlockID> range) override;

	const VddkTargetLib* Cache() const noexcept;
private:
	int ReadModifyWrite(ActiveVmdk* vmdkp, RequestBlock* blockp,
		RequestBuffer* bufferp);
private:
	std::unique_ptr<ArmVddkLib> target_;
	bool enabled_{false};
};

}
