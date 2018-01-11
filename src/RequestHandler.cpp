#include <memory>

#include <cassert>

#include "IDs.h"
#include "Request.h"
#include "RequestHandler.h"

namespace pio {

RequestHandler::RequestHandler(void *udatap) : udatap_(udatap) {

}

RequestHandler::~RequestHandler() {

}

void RequestHandler::RegisterNextRequestHandler(std::unique_ptr<RequestHandler> handlerp) {
	if (nextp_ == nullptr) {
		nextp_ = std::move(handlerp);
		return;
	}

	nextp_->RegisterNextRequestHandler(std::move(handlerp));
}

}