#pragma once

#include <cstddef>

namespace OcclConfig {

// -DOCCL_SMALL_TESTS=ON scales the planner's chunk granularity and the collective test
// counts down together. Tests run on low-resource machines then build exactly the same
// units_total / channel split as the production sizes, only with 256x fewer bytes.
#ifdef OCCL_SMALL_TESTS
constexpr size_t kTestScale = 256;
#else
constexpr size_t kTestScale = 1;
#endif

constexpr size_t kChunkBytes = 32 * 1024 / kTestScale;

// Per-rank chunk count below which the planner routes a collective through the tree
// topology instead of the ring. Kept in chunks so OCCL_SMALL_TESTS leaves the tree/ring
// decision unchanged (both kChunkBytes and the test counts scale by the same factor).
constexpr size_t kTreeThresholdChunks = 64;

static_assert(32 * 1024 % kTestScale == 0, "kTestScale must divide the 32 KiB chunk");
static_assert(32768 % kTestScale == 0 && 1048576 % kTestScale == 0, "kTestScale must divide every test count");

} // namespace OcclConfig
