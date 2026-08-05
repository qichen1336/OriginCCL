#pragma once

#include <string>
#include <vector>
#include "types.h"

namespace Utils {
std::string GetLocalIPAddress();

int CreateListenSocket(uint16_t port, uint16_t* actual_port = nullptr);

int AcceptConnection(int listen_fd);

int CreateConnectSocket(const std::string& addr, uint16_t port, int retry_count = 30);

uint16_t GetSocketPort(int sockfd);

void SetTcpNoDelay(int sockfd);

void SetReuseAddr(int sockfd);

bool SendAll(int sockfd, const void* data, size_t size);
bool SendAll(int sockfd, const std::vector<char>& buffer);
bool SendAll(int sockfd, const std::string& str);
bool RecvAll(int sockfd, std::vector<char>& buffer);
bool RecvAll(int sockfd, std::string& str);

void EncodeInt(std::vector<char>& buffer, int value);
int DecodeInt(const char* buffer, size_t& offset);

void EncodeString(std::vector<char>& buffer, const std::string& str);
std::string DecodeString(const char* buffer, size_t& offset);

void EncodeNodeInfo(std::vector<char>& buffer, const NodeInfo& node);
NodeInfo DecodeNodeInfo(const char* buffer, size_t& offset);

template<typename T>
void ReduceData(const T* send_buf, T* recv_buf, size_t count, ReduceOp op);
void PerformReduce(const void* send_buf, void* recv_buf, size_t count, DataType dtype, ReduceOp op, int world_size);

inline size_t AlignUp(size_t size, size_t alignment) {
    return (size + alignment - 1) & ~(alignment - 1);
}

size_t GetDataTypeSize(DataType dtype);
const char* GetDataTypeName(DataType dtype);
const char* GetReduceOpName(ReduceOp op);
};
