#include "test_common.h"

#include <algorithm>
#include <cstdio>
#include <cstdlib>
#include <string>
#include <type_traits>
#include <vector>
#include <mpi.h>
#include <fmt/format.h>
#include "channel.h"
#include "communicator.h"
#include "logger.h"
#include "topology.h"
#include "transport/transport.h"
#include "transport/transport_rdma.h"
#include "transport/transport_shm.h"
#include "transport/transport_tcp.h"

namespace TestCommon {
namespace {

constexpr double kValueScale = 8.0;
constexpr double kSendSentinel = -987654321.0;
constexpr double kRecvSentinel = 987654321.0;

const DataType kDtypes[4] = {DataType::FLOAT32, DataType::FLOAT64, DataType::INT32, DataType::INT64};
const ReduceOp kOps[4] = {ReduceOp::SUM, ReduceOp::MAX, ReduceOp::MIN, ReduceOp::AVG};

std::vector<size_t> CaseCounts(int level) {
    if (level == 0) {
        return {1, 1024, 10240};
    }
    return {1, 1024, 10240, 65536};
}

std::vector<int> CaseRoots(int world_size) {
    std::vector<int> roots = {0, world_size / 2, world_size - 1};
    std::sort(roots.begin(), roots.end());
    roots.erase(std::unique(roots.begin(), roots.end()), roots.end());
    return roots;
}

double Value(int source, size_t index) {
    const int raw = source * 101 + static_cast<int>(index % 97) - 50;
    return static_cast<double>(raw) / kValueScale;
}

int64_t TruncateToInteger(long double value) {
    return static_cast<int64_t>(value);
}

bool IsReduceFunc(CollFunc func) {
    return func == CollFunc::AllReduce || func == CollFunc::Reduce || func == CollFunc::ReduceScatter;
}

// Every value is an exact multiple of 1/8, so float32, float64 and the scaled integer forms
// all represent it without rounding. The integer oracle therefore compares exactly and the
// float oracle compares exactly, which keeps the test independent of the reduction order.
template<typename Item>
Item ItemOf(double value) {
    if (std::is_floating_point<Item>::value) {
        return static_cast<Item>(value);
    }
    return static_cast<Item>(TruncateToInteger(static_cast<long double>(value)));
}

template<typename Item>
bool ExactEqual(Item a, Item b) {
    return a == b;
}

size_t InputCount(const CaseSpec& spec, int world_size) {
    if (spec.func == CollFunc::ReduceScatter) {
        return spec.count * static_cast<size_t>(world_size);
    }
    return spec.count;
}

size_t OutputCount(const CaseSpec& spec, int world_size) {
    if (spec.func == CollFunc::AllGather) {
        return spec.count * static_cast<size_t>(world_size);
    }
    return spec.count;
}

template<typename Item>
bool CheckSentinels(const std::vector<Item>& buffer, size_t used, Item sentinel, const char* what,
                    std::string& reason) {
    for (size_t i = used; i < buffer.size(); ++i) {
        if (!ExactEqual(buffer[i], sentinel)) {
            reason = fmt::format("{} wrote {} at index {} beyond the {} used elements", what,
                                 static_cast<double>(buffer[i]), i, used);
            return false;
        }
    }
    return true;
}

template<typename Item>
bool CheckAllEqual(const Item* actual, const Item* expected, size_t count, const char* what, std::string& reason) {
    for (size_t i = 0; i < count; ++i) {
        if (!ExactEqual(actual[i], expected[i])) {
            reason = fmt::format("{} mismatch at element {}: got {}, expected {}", what, i,
                                 static_cast<double>(actual[i]), static_cast<double>(expected[i]));
            return false;
        }
    }
    return true;
}

// The reduction runs in the element type itself, exactly like the ring does: an integer buffer
// already holds truncated integers, so truncation happens per source value before the sum, and
// an integer AVG divides the accumulated integer by the world size with integer division.
// Values are multiples of 1/8 with a bounded magnitude, so every partial sum stays exactly
// representable in float32 and the comparison can be exact for all four types.
template<typename Item>
void ComputeReduce(int world_size, size_t count, ReduceOp op, int dest_offset, Item* out) {
    for (size_t i = 0; i < count; ++i) {
        Item acc = ItemOf<Item>(Value(0, i) + dest_offset * 1000);
        for (int source = 1; source < world_size; ++source) {
            const Item value = ItemOf<Item>(Value(source, i) + dest_offset * 1000);
            if (op == ReduceOp::MAX) {
                acc = std::max(acc, value);
            } else if (op == ReduceOp::MIN) {
                acc = std::min(acc, value);
            } else {
                acc = acc + value;
            }
        }
        if (op == ReduceOp::AVG) {
            acc = acc / static_cast<Item>(world_size);
        }
        out[i] = acc;
    }
}

std::string JsonEscape(const std::string& text) {
    std::string escaped;
    escaped.reserve(text.size() + 8);
    for (char c : text) {
        switch (c) {
        case '"':
            escaped += "\\\"";
            break;
        case '\\':
            escaped += "\\\\";
            break;
        case '\n':
            escaped += "\\n";
            break;
        case '\r':
            escaped += "\\r";
            break;
        case '\t':
            escaped += "\\t";
            break;
        default:
            escaped += c;
        }
    }
    return escaped;
}

std::string JoinFailures(const std::vector<std::string>& failures) {
    std::string joined;
    for (size_t i = 0; i < failures.size(); ++i) {
        if (i != 0) {
            joined += " | ";
        }
        joined += failures[i];
    }
    return joined;
}

template<typename Item>
bool ExecuteCase(Communicator& comm, const CaseSpec& spec, std::string& reason) {
    const int rank = comm.GetRank();
    const int world_size = comm.GetWorldSize();
    const size_t input_count = InputCount(spec, world_size);
    const size_t output_count = OutputCount(spec, world_size);
    const Item send_sentinel = static_cast<Item>(kSendSentinel);
    const Item recv_sentinel = static_cast<Item>(kRecvSentinel);

    std::vector<Item> baseline(input_count);
    for (size_t i = 0; i < input_count; ++i) {
        if (spec.func == CollFunc::ReduceScatter) {
            const int dest = static_cast<int>(i / spec.count);
            baseline[i] = ItemOf<Item>(Value(rank, i % spec.count) + dest * 1000);
        } else {
            baseline[i] = ItemOf<Item>(Value(rank, i));
        }
    }

    std::vector<Item> send(input_count + 1, send_sentinel);
    std::copy(baseline.begin(), baseline.end(), send.begin());
    std::vector<Item> recv(output_count + 1, recv_sentinel);
    if (spec.inplace) {
        std::copy(baseline.begin(), baseline.end(), recv.begin());
    }

    bool ok = false;
    switch (spec.func) {
    case CollFunc::AllReduce: {
        if (spec.inplace) {
            ok = comm.AllReduce(recv.data(), recv.data(), spec.count, spec.dtype, spec.op);
        } else {
            ok = comm.AllReduce(send.data(), recv.data(), spec.count, spec.dtype, spec.op);
        }
        break;
    }
    case CollFunc::Broadcast:
        ok = comm.Broadcast(send.data(), spec.count, spec.dtype, spec.root);
        break;
    case CollFunc::AllGather:
        ok = comm.AllGather(send.data(), recv.data(), spec.count, spec.dtype);
        break;
    case CollFunc::Reduce:
        ok = comm.Reduce(send.data(), rank == spec.root ? recv.data() : nullptr, spec.count, spec.dtype, spec.op,
                         spec.root);
        break;
    case CollFunc::ReduceScatter:
        ok = comm.ReduceScatter(send.data(), recv.data(), spec.count, spec.dtype, spec.op);
        break;
    }
    if (!ok) {
        reason = "the collective returned false";
        return false;
    }

    if (spec.func == CollFunc::Broadcast) {
        for (size_t i = 0; i < spec.count; ++i) {
            const Item expected = ItemOf<Item>(Value(spec.root, i));
            if (send[i] != expected) {
                reason = fmt::format("Broadcast mismatch at element {}: got {}, expected {}", i,
                                     static_cast<double>(send[i]), static_cast<double>(expected));
                return false;
            }
        }
        return CheckSentinels(send, spec.count, send_sentinel, "Broadcast", reason);
    }

    std::vector<Item> expected(output_count);
    switch (spec.func) {
    case CollFunc::AllReduce:
        ComputeReduce(world_size, spec.count, spec.op, 0, expected.data());
        break;
    case CollFunc::AllGather:
        for (int source = 0; source < world_size; ++source) {
            for (size_t i = 0; i < spec.count; ++i) {
                expected[static_cast<size_t>(source) * spec.count + i] = ItemOf<Item>(Value(source, i));
            }
        }
        break;
    case CollFunc::Reduce:
        if (rank != spec.root) {
            return true;
        }
        ComputeReduce(world_size, spec.count, spec.op, 0, expected.data());
        break;
    case CollFunc::ReduceScatter:
        ComputeReduce(world_size, spec.count, spec.op, rank, expected.data());
        break;
    case CollFunc::Broadcast:
        break;
    }

    if (spec.func == CollFunc::Reduce && rank != spec.root) {
        return true;
    }
    if (!CheckAllEqual(recv.data(), expected.data(), output_count, FuncName(spec.func), reason)) {
        return false;
    }
    if (!CheckSentinels(recv, output_count, recv_sentinel, FuncName(spec.func), reason)) {
        return false;
    }
    if (!spec.inplace && spec.func != CollFunc::Broadcast) {
        if (!CheckAllEqual(send.data(), baseline.data(), input_count, "input", reason)) {
            reason = "the collective modified its input buffer: " + reason;
            return false;
        }
        if (!CheckSentinels(send, input_count, send_sentinel, "input", reason)) {
            return false;
        }
    }
    return true;
}

} // namespace

const char* FuncName(CollFunc func) {
    switch (func) {
    case CollFunc::AllReduce:
        return "AllReduce";
    case CollFunc::Broadcast:
        return "Broadcast";
    case CollFunc::AllGather:
        return "AllGather";
    case CollFunc::Reduce:
        return "Reduce";
    case CollFunc::ReduceScatter:
        return "ReduceScatter";
    }
    return "Unknown";
}

const char* DtypeName(DataType dtype) {
    switch (dtype) {
    case DataType::FLOAT32:
        return "FLOAT32";
    case DataType::FLOAT64:
        return "FLOAT64";
    case DataType::INT32:
        return "INT32";
    case DataType::INT64:
        return "INT64";
    }
    return "Unknown";
}

const char* OpName(ReduceOp op) {
    switch (op) {
    case ReduceOp::SUM:
        return "SUM";
    case ReduceOp::MAX:
        return "MAX";
    case ReduceOp::MIN:
        return "MIN";
    case ReduceOp::AVG:
        return "AVG";
    }
    return "Unknown";
}

std::string CaseId(const CaseSpec& spec) {
    if (spec.kind == CaseKind::ZeroCount) {
        return fmt::format("{}|zero|ch{}", FuncName(spec.func), spec.channels);
    }
    if (spec.kind == CaseKind::ExpectFailure) {
        return fmt::format("{}|invalid{}|root{}|ch{}", FuncName(spec.func), spec.invalid, spec.root, spec.channels);
    }
    std::string id = fmt::format("{}-{}-{}", FuncName(spec.func), DtypeName(spec.dtype), spec.count);
    if (IsReduceFunc(spec.func)) {
        id += fmt::format("-{}", OpName(spec.op));
    }
    if (spec.func == CollFunc::Broadcast || spec.func == CollFunc::Reduce) {
        id += fmt::format("-root{}", spec.root);
    }
    if (spec.inplace) {
        id += "-inplace";
    }
    id += fmt::format("-ch{}", spec.channels);
    return id;
}

int ExpectedWorldSize(int level) {
    return level == 0 ? 4 : (level == 1 ? 8 : 32);
}

int ExpectedRanksPerMachine(int level, bool multi_machine) {
    if (!multi_machine) {
        return ExpectedWorldSize(level);
    }
    return level == 0 ? 1 : (level == 1 ? 2 : 4);
}

std::vector<int> ChannelProfiles(int level) {
    if (level == 0) {
        return {4};
    }
    if (level == 1) {
        return {4, 3};
    }
    return {4, 3, 1};
}

std::string EdgeReport::Summary() const {
    if (!error.empty()) {
        return error;
    }
    if (Total() == 0) {
        return "no-data-plane";
    }
    if (shm == Total()) {
        return "shm";
    }
    if (rdma == Total()) {
        return "rdma";
    }
    if (tcp == Total()) {
        return "tcp";
    }
    return fmt::format("mixed(shm={},tcp={},rdma={})", shm, tcp, rdma);
}

// One communicator is initialised per channel profile, so the cases are built per profile: the
// first profile (n_channels = 4) carries the full matrix and the remaining profiles carry one
// representative case per interface and dtype to cover the planner remainder path.
std::vector<CaseSpec> BuildCases(int level, int channels) {
    const std::vector<size_t> counts = CaseCounts(level);
    const int world_size = ExpectedWorldSize(level);
    const std::vector<int> roots = CaseRoots(world_size);
    const std::vector<int> profiles = ChannelProfiles(level);
    const bool representative = channels != profiles.front();

    std::vector<CaseSpec> cases;
    size_t root_cursor = 0;

    const CollFunc funcs[5] = {CollFunc::AllReduce, CollFunc::Broadcast, CollFunc::AllGather, CollFunc::Reduce,
                               CollFunc::ReduceScatter};

    for (size_t di = 0; di < 4; ++di) {
        for (size_t oi = 0; oi < 4; ++oi) {
            for (CollFunc func : funcs) {
                if (!IsReduceFunc(func) && oi != 0) {
                    continue;
                }
                std::vector<size_t> chosen;
                if (representative) {
                    chosen = {65536};
                } else if (level == 0 && IsReduceFunc(func)) {
                    // Level 0 samples one count per (dtype, op) pair; the rotation still walks
                    // every dtype with every op.
                    chosen = {counts[(di + oi) % counts.size()]};
                } else {
                    chosen = counts;
                }
                for (size_t count : chosen) {
                    CaseSpec spec;
                    spec.func = func;
                    spec.count = count;
                    spec.dtype = kDtypes[di];
                    spec.op = kOps[oi];
                    spec.channels = channels;
                    if (func == CollFunc::Broadcast || func == CollFunc::Reduce) {
                        spec.root = roots[root_cursor % roots.size()];
                        ++root_cursor;
                    }
                    spec.inplace = func == CollFunc::AllReduce && ((di + oi) % 2 == 1);
                    cases.push_back(spec);
                }
            }
        }
    }
    return cases;
}

// Contract cases run once per binary, on the first channel profile: an empty operation and the
// tasks the ring must reject before it touches the network.
std::vector<CaseSpec> BuildContractCases(int level) {
    const int world_size = ExpectedWorldSize(level);
    const int channels = ChannelProfiles(level).front();
    const CollFunc funcs[5] = {CollFunc::AllReduce, CollFunc::Broadcast, CollFunc::AllGather, CollFunc::Reduce,
                               CollFunc::ReduceScatter};

    std::vector<CaseSpec> cases;
    CaseSpec zero;
    zero.kind = CaseKind::ZeroCount;
    zero.count = 0;
    zero.channels = channels;
    for (CollFunc func : funcs) {
        zero.func = func;
        cases.push_back(zero);
    }

    CaseSpec invalid = zero;
    invalid.kind = CaseKind::ExpectFailure;
    invalid.count = 1;
    invalid.root = 0;

    // Missing required buffers: the ring must reject the task before touching the network.
    invalid.invalid = 1;
    invalid.func = CollFunc::AllReduce;
    cases.push_back(invalid);
    invalid.func = CollFunc::AllGather;
    cases.push_back(invalid);
    invalid.func = CollFunc::ReduceScatter;
    cases.push_back(invalid);

    // Out-of-range roots: only Broadcast and Reduce consume root, so only they can reject it.
    invalid.invalid = 2;
    invalid.func = CollFunc::Broadcast;
    invalid.root = -1;
    cases.push_back(invalid);
    invalid.root = world_size;
    cases.push_back(invalid);
    invalid.func = CollFunc::Reduce;
    cases.push_back(invalid);

    return cases;
}

std::string CaseListDigest(const std::vector<CaseSpec>& cases) {
    uint64_t hash = 1469598103934665603ull;
    const std::string text = fmt::format("{}", cases.size());
    for (const char c : text) {
        hash = (hash ^ static_cast<uint64_t>(static_cast<unsigned char>(c))) * 1099511628211ull;
    }
    for (const CaseSpec& spec : cases) {
        for (const char c : CaseId(spec)) {
            hash = (hash ^ static_cast<uint64_t>(static_cast<unsigned char>(c))) * 1099511628211ull;
        }
    }
    return fmt::format("{:016x}", hash);
}

TestOptions ParseOptions(int argc, char** argv) {
    TestOptions options;
    for (int i = 1; i < argc; ++i) {
        const std::string arg = argv[i];
        if (arg == "-h" || arg == "--help") {
            PrintUsage(argv[0]);
            std::exit(0);
        }
        if (arg == "--list-cases") {
            options.list_cases = true;
            continue;
        }
        if (arg == "--level" && i + 1 < argc) {
            options.level = std::atoi(argv[++i]);
            continue;
        }
        if (arg == "--report" && i + 1 < argc) {
            options.report_path = argv[++i];
            continue;
        }
        // The MPI launch passes -np and friends through, which must be ignored here.
    }
    if (options.level < 0 || options.level > 2) {
        LOG_ERROR("The level must be 0, 1 or 2, got {}", options.level);
        std::exit(4);
    }
    return options;
}

void PrintUsage(const char* program) {
    fmt::print(stderr,
               "Usage: {} [--level 0|1|2] [--list-cases] [--report PATH]\n"
               "\n"
               "Collective-interface tests. Rank and world size come from MPI:\n"
               "  mpirun -np 4 {} --level 0\n"
               "\n"
               "Levels (counts are elements per rank):\n"
               "  0  single machine: 4 ranks      multi machine: 4x1 ranks  counts 1 1024 10240\n"
               "  1  single machine: 8 ranks      multi machine: 4x2 ranks  counts 1 1024 10240 65536\n"
               "  2  single machine: 32 ranks     multi machine: 8x4 ranks  counts 1024 10240 65536\n"
               "\n"
               "Level 0 samples every dtype/op/count; level 1 adds the pairwise combinations\n"
               "and the n_channels=3 remainder profile; level 2 runs the full legal core set\n"
               "plus n_channels=3 and n_channels=1. Multi machine groups ranks by a logical\n"
               "hostname, so it never leaves one physical host.\n",
               program, program);
}

bool InitCommunicator(Communicator& comm, CommConfig& config, int rank, int world_size) {
    (void)world_size;
    int id_ok = 1;
    if (rank == 0 && !comm.GetUniqueId(config.unique_id)) {
        id_ok = 0;
    }
    MPI_Bcast(&id_ok, 1, MPI_INT, 0, MPI_COMM_WORLD);
    if (id_ok == 0) {
        return false;
    }
    MPI_Bcast(&config.unique_id, sizeof(config.unique_id), MPI_BYTE, 0, MPI_COMM_WORLD);
    return comm.Init(config);
}

EdgeReport InspectTransports(const Communicator& comm, const std::string& expected) {
    EdgeReport report;
    int local_shm = 0;
    int local_other = 0;
    int remote_tcp = 0;
    int remote_rdma = 0;
    int remote_shm = 0;

    const std::vector<int>& local_ranks = comm.GetLocalRanks();
    const auto is_local = [&local_ranks](int peer) {
        return std::find(local_ranks.begin(), local_ranks.end(), peer) != local_ranks.end();
    };

    for (int channel_id = 0; channel_id < comm.GetNChannels(); ++channel_id) {
        const Channel& channel = comm.GetChannel(channel_id);
        // Only the two ring edges of a channel are connected: send to ring.next and recv from
        // ring.prev. The other slots stay empty and must not be counted or required.
        for (int peer : {channel.ring.next}) {
            const Connector* connector = channel.send.empty()
                                             ? nullptr
                                             : (peer >= 0 && static_cast<size_t>(peer) < channel.send.size()
                                                    ? &channel.send[static_cast<size_t>(peer)]
                                                    : nullptr);
            if (connector == nullptr || !connector->transport) {
                report.error = fmt::format("channel {} has no send edge to its ring successor {}", channel_id, peer);
                return report;
            }
            if (connector->transport->GetDirection() != TransportDirection::Send) {
                report.error = fmt::format("channel {} send edge to peer {} is not marked Send", channel_id, peer);
                return report;
            }
            if (dynamic_cast<const TransportShm*>(connector->transport.get()) != nullptr) {
                ++report.shm;
                is_local(peer) ? ++local_shm : ++remote_shm;
            } else if (dynamic_cast<const TransportRDMA*>(connector->transport.get()) != nullptr) {
                ++report.rdma;
                is_local(peer) ? ++local_other : ++remote_rdma;
            } else if (dynamic_cast<const TransportTCP*>(connector->transport.get()) != nullptr) {
                ++report.tcp;
                is_local(peer) ? ++local_other : ++remote_tcp;
            } else {
                report.error =
                    fmt::format("channel {} send edge to peer {} has an unknown transport", channel_id, peer);
                return report;
            }
        }
        for (int peer : {channel.ring.prev}) {
            const Connector* connector = channel.recv.empty()
                                             ? nullptr
                                             : (peer >= 0 && static_cast<size_t>(peer) < channel.recv.size()
                                                    ? &channel.recv[static_cast<size_t>(peer)]
                                                    : nullptr);
            if (connector == nullptr || !connector->transport) {
                report.error =
                    fmt::format("channel {} has no recv edge from its ring predecessor {}", channel_id, peer);
                return report;
            }
            if (connector->transport->GetDirection() != TransportDirection::Receive) {
                report.error = fmt::format("channel {} recv edge from peer {} is not marked Receive", channel_id, peer);
                return report;
            }
            if (connector->transport->GetFd() < 0) {
                report.error =
                    fmt::format("channel {} recv edge from peer {} has no ready descriptor", channel_id, peer);
                return report;
            }
            const Connector* send_edge = channel.send.empty() || static_cast<size_t>(peer) >= channel.send.size()
                                             ? nullptr
                                             : &channel.send[static_cast<size_t>(peer)];
            if (send_edge != nullptr && send_edge->transport &&
                send_edge->transport->GetFd() == connector->transport->GetFd()) {
                report.error =
                    fmt::format("channel {} peer {} shares one descriptor between send and recv", channel_id, peer);
                return report;
            }
        }
    }

    if (report.Total() == 0) {
        report.error = "no data-plane edge was established";
        return report;
    }
    if (expected == "all-shm") {
        if (report.shm != report.Total()) {
            report.error = fmt::format("expected every edge to be shared memory, got {}", report.Summary());
        }
        return report;
    }
    if (expected == "network") {
        // One rank per machine: there is no same-machine edge at all, so every edge must be on
        // the same network transport.
        if (report.shm != 0) {
            report.error = fmt::format("expected no shared-memory edge, got {}", report.Summary());
        } else if ((report.tcp == 0) == (report.rdma == 0)) {
            report.error = fmt::format("expected one network transport for every edge, got {}", report.Summary());
        }
        return report;
    }
    if (expected == "split") {
        // Same-machine edges must stay on shared memory; every cross-machine edge must use the
        // same network transport, because the transport is a cluster-wide decision.
        if (local_shm == 0 || remote_shm != 0 || local_other != 0) {
            report.error = fmt::format("expected shared memory only on same-machine edges, got {}", report.Summary());
            return report;
        }
        if ((remote_tcp == 0) == (remote_rdma == 0)) {
            report.error =
                fmt::format("expected one network transport for every cross-machine edge, got {}", report.Summary());
        }
    }
    return report;
}

void ReportOutcome(const std::string& report_path, const std::string& suite, int rank, int world_size,
                   const std::string& status, const std::string& transport, const std::string& reason,
                   const std::vector<std::string>& failures) {
    std::string json = fmt::format(
        "{{\"suite\":\"{}\",\"rank\":{},\"world_size\":{},\"status\":\"{}\",\"transport\":\"{}\",\"reason\":\"{}\","
        "\"failures\":[",
        JsonEscape(suite), rank, world_size, JsonEscape(status), JsonEscape(transport), JsonEscape(reason));
    for (size_t i = 0; i < failures.size(); ++i) {
        json += fmt::format("{}\"{}\"", i == 0 ? "" : ",", JsonEscape(failures[i]));
    }
    json += "]}";

    fmt::print(stdout, "OCCL_TEST_RESULT {}\n", json);
    std::fflush(stdout);

    if (report_path.empty()) {
        return;
    }
    FILE* file = std::fopen(report_path.c_str(), "a");
    if (file == nullptr) {
        LOG_WARN("Rank {}: cannot append the result to {}", rank, report_path);
        return;
    }
    std::fprintf(file, "%s\n", json.c_str());
    std::fclose(file);
}

Runner::Runner(Communicator& comm, std::string suite, int level, std::string report_path, std::string transport)
    : comm_(comm), suite_(std::move(suite)), level_(level), report_path_(std::move(report_path)),
      transport_(std::move(transport)) {}

void Runner::Skip(const std::string& reason) {
    skip_reason_ = reason;
    Finish();
}

void Runner::Run(const std::vector<CaseSpec>& cases) {
    const int rank = comm_.GetRank();
    const int world_size = comm_.GetWorldSize();
    for (const CaseSpec& spec : cases) {
        ++cases_total_;
        std::string reason;
        int local_ok = RunCase(spec, reason) ? 1 : 0;
        int global_ok = 1;
        MPI_Allreduce(&local_ok, &global_ok, 1, MPI_INT, MPI_MIN, MPI_COMM_WORLD);
        if (global_ok == 0) {
            ++cases_failed_;
            if (local_ok == 0) {
                failures_.push_back(fmt::format("{}: {}", CaseId(spec), reason));
                LOG_ERROR("Rank {}: case {} failed: {}", rank, CaseId(spec), reason);
            } else {
                failures_.push_back(fmt::format("{}: another rank failed", CaseId(spec)));
                LOG_ERROR("Rank {}: case {} failed on another rank", rank, CaseId(spec));
            }
            // Stop on every rank together: continuing would leave the ring out of step.
            break;
        }
        (void)world_size;
    }
    Finish();
}

bool Runner::RunCase(const CaseSpec& spec, std::string& reason) {
    return RunOneCase(comm_, spec, reason);
}

bool RunOneCase(Communicator& comm, const CaseSpec& spec, std::string& reason) {
    const int rank = comm.GetRank();
    const int world_size = comm.GetWorldSize();

    if (spec.kind == CaseKind::ZeroCount) {
        bool ok = false;
        switch (spec.func) {
        case CollFunc::AllReduce:
            ok = comm.AllReduce(nullptr, nullptr, 0, spec.dtype, spec.op);
            break;
        case CollFunc::Broadcast:
            ok = comm.Broadcast(nullptr, 0, spec.dtype, spec.root);
            break;
        case CollFunc::AllGather:
            ok = comm.AllGather(nullptr, nullptr, 0, spec.dtype);
            break;
        case CollFunc::Reduce:
            ok = comm.Reduce(nullptr, nullptr, 0, spec.dtype, spec.op, spec.root);
            break;
        case CollFunc::ReduceScatter:
            ok = comm.ReduceScatter(nullptr, nullptr, 0, spec.dtype, spec.op);
            break;
        }
        if (!ok) {
            reason = "a zero-count collective must succeed as an empty operation";
        }
        return ok;
    }

    if (spec.kind == CaseKind::ExpectFailure) {
        const std::shared_ptr<Topology> topology = comm.GetTopology();
        if (!topology) {
            reason = "the communicator has no topology";
            return false;
        }
        PlanTask task;
        task.func = spec.func;
        task.world_size = world_size;
        task.rank = rank;
        task.elem_count = spec.count;
        task.dtype = spec.dtype;
        task.reduce_op = spec.op;
        task.root = spec.root;
        task.recv_buf = nullptr;
        task.send_buf = nullptr;
        const bool accepted = topology->CollectiveInit(task);
        if (accepted) {
            reason = "an invalid task must be rejected";
        }
        return !accepted;
    }

    switch (spec.dtype) {
    case DataType::FLOAT32:
        return ExecuteCase<float>(comm, spec, reason);
    case DataType::FLOAT64:
        return ExecuteCase<double>(comm, spec, reason);
    case DataType::INT32:
        return ExecuteCase<int32_t>(comm, spec, reason);
    case DataType::INT64:
        return ExecuteCase<int64_t>(comm, spec, reason);
    }
    reason = "unknown data type";
    return false;
}

void Runner::Finish() {
    const std::string status = !skip_reason_.empty() ? "SKIP" : (cases_failed_ == 0 ? "PASS" : "FAIL");
    const std::string reason = !skip_reason_.empty() ? skip_reason_ : JoinFailures(failures_);
    ReportOutcome(report_path_, suite_, comm_.GetRank(), comm_.GetWorldSize(), status, transport_, reason, failures_);
    LOG_INFO("Rank {}: suite {} {} ({} cases, {} failed)", comm_.GetRank(), suite_, status, cases_total_,
             cases_failed_);
}
} // namespace TestCommon
