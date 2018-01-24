#pragma once

#include <vector>
#include <memory>

struct WriteRecord {

};

struct ReadRecord {

};

struct DeleteRecord {

};

class AeroSpike {
public:
	int Connect();
	folly::Future<int> Write(const std::vector<std::unique_ptr<WriteRecord>>& records);
	folly::Future<int> Read(const std::vector<std::unique_ptr<ReadRecord>>& records);
	folly::Future<int> Delete(const std::vector<std::unique_ptr<<DeleteRecord>>& records);
private:
};