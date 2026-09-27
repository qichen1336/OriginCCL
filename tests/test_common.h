#pragma once

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>
#include "types.h"

class Communicator;

namespace TestCommon {

enum class CaseKind {
    Normal,
    ZeroCount,
    ExpectFailure
};

struct CaseSpec {
    CollFunc func = CollFunc::AllReduce;
    size_t count = 0;
    DataType dtype = DataType::FLOAT32;
    ReduceOp op = ReduceOp::SUM;
    int root = 0;
    int channels = 4;
    bool inplace = false;
    CaseKind kind = CaseKind::Normal;
    // 0 = not invalid, 1 = missing buffer, 2 = out-of-range root.
    int invalid = 0;
};

struct TestOptions {
    int level = 0;
    bool multi_machine = false;
    bool list_cases = false;
    std::string report_path;
};

struct EdgeReport {
    int shm = 0;
    int tcp = 0;
    int rdma = 0;
    std::string error;

    bool Ok() const {
        return error.empty();
    }
    int Total() const {
        return shm + tcp + rdma;
    }
    bool NetworkUniform() const {
        return tcp == 0 || rdma == 0;
    }
    std::string Summary() const;
};

const char* FuncName(CollFunc func);
const char* DtypeName(DataType dtype);
const char* OpName(ReduceOp op);
std::string CaseId(const CaseSpec& spec);

TestOptions ParseOptions(int argc, char** argv);
void PrintUsage(const char* program);

int ExpectedWorldSize(int level);
int ExpectedRanksPerMachine(int level, bool multi_machine);
std::vector<int> ChannelProfiles(int level);
std::vector<CaseSpec> BuildCases(int level, int channels);
std::vector<CaseSpec> BuildContractCases(int level);
std::string CaseListDigest(const std::vector<CaseSpec>& cases);

bool InitCommunicator(Communicator& comm, CommConfig& config, int rank, int world_size);
EdgeReport InspectTransports(const Communicator& comm, const std::string& expected);

bool RunOneCase(Communicator& comm, const CaseSpec& spec, std::string& reason);

void ReportOutcome(const std::string& report_path, const std::string& suite, int rank, int world_size,
                   const std::string& status, const std::string& transport, const std::string& reason,
                   const std::vector<std::string>& failures);

class Runner {
public:
    Runner(Communicator& comm, std::string suite, int level, std::string report_path, std::string transport);

    void Run(const std::vector<CaseSpec>& cases);
    void Skip(const std::string& reason);

    bool Passed() const {
        return cases_failed_ == 0 && skip_reason_.empty();
    }

private:
    bool RunCase(const CaseSpec& spec, std::string& reason);
    void Finish();

    Communicator& comm_;
    std::string suite_;
    int level_ = 0;
    std::string report_path_;
    std::string transport_;
    std::vector<std::string> failures_;
    size_t cases_total_ = 0;
    size_t cases_failed_ = 0;
    std::string skip_reason_;
};
} // namespace TestCommon
