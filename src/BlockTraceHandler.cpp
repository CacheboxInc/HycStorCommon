
namespace pio {
BlockTraceHandler::BlockTraceHandler() {

}

BlockTraceHandler::~BlockTraceHandler() {

}

int BlockTraceHandler::Read(Vmdk *vmdkp, void *bufp, size_t count, off_t offset) override {
	return nextp_->Read(vmdkp, bufp, count, offset);
}

int BlockTraceHandler::Write(Vmdk *vmdkp, void *bufp, size_t count, off_t offset) override {
	return nextp_->Write(vmdkp, bufp, count, offset);
}
}