#include <gtest/gtest.h>

#include <limits>
#include <stdexcept>
#include <thread>
#include <unordered_map>

#include "metadata/kv_core_heap_accounting.h"
#include "metadata/kv_core_tenant_id.h"

namespace mooncake::test {
namespace {

uint64_t Bytes() { return GetMasterCoreHeapAccount().Load(); }

struct ThrowingObject {
    ThrowingObject() { throw std::runtime_error("construction failed"); }
};

TEST(CoreHeapAccounting, UniqueConstructionFailureRollsBackAllocation) {
    const auto before = Bytes();
    EXPECT_THROW(MakeCoreUnique<ThrowingObject>(), std::runtime_error);
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, UniqueOwnershipMoveDoesNotChangeAllocation) {
    const auto before = Bytes();
    {
        auto object = MakeCoreUnique<uint64_t>(42);
        EXPECT_EQ(Bytes(), before + sizeof(uint64_t));
        auto moved = std::move(object);
        EXPECT_EQ(*moved, 42);
        EXPECT_EQ(Bytes(), before + sizeof(uint64_t));
    }
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, VectorCapacitySurvivesClearAndOwnershipMove) {
    const auto before = Bytes();
    {
        CoreVector<uint64_t> values;
        values.reserve(128);
        const auto capacity_bytes = values.capacity() * sizeof(uint64_t);
        EXPECT_EQ(Bytes(), before + capacity_bytes);
        values.push_back(7);
        values.clear();
        EXPECT_EQ(Bytes(), before + capacity_bytes);
        auto deferred = std::move(values);
        EXPECT_EQ(Bytes(), before + capacity_bytes);
        EXPECT_EQ(deferred.capacity() * sizeof(uint64_t), capacity_bytes);
    }
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, NodeBucketsAndLongKeysUseTheSameAccount) {
    const auto before = Bytes();
    using Map = std::unordered_map<
        CoreString, uint64_t, CoreStringHash, CoreStringEqual,
        CoreHeapAllocator<std::pair<const CoreString, uint64_t>>>;
    {
        Map entries;
        entries.reserve(128);
        const auto buckets_only = Bytes();
        EXPECT_GT(buckets_only, before);
        const std::string key(512, 'k');
        entries.emplace(key, 1);
        const auto with_key = Bytes();
        EXPECT_GE(with_key - buckets_only, key.size() + 1);
        EXPECT_NE(entries.find(key), entries.end());
        EXPECT_EQ(Bytes(), with_key);  // Heterogeneous lookup allocates no key.
        entries.clear();
        EXPECT_EQ(Bytes(), buckets_only);
    }
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, PersistentTenantTextIsTrackedButQueriesAreNot) {
    const auto before = Bytes();
    const TenantId tenant(std::string(512, 't'));
    EXPECT_EQ(Bytes(), before);
    {
        CoreTenantId stored(tenant);
        EXPECT_GE(Bytes() - before, tenant.value().size() + 1);
        EXPECT_TRUE(CoreTenantIdEqual{}(stored, tenant));
        EXPECT_EQ(CoreTenantIdHash{}(stored), CoreTenantIdHash{}(tenant));
        const auto held = Bytes();
        const TenantId query = stored;
        EXPECT_EQ(query, tenant);
        EXPECT_EQ(Bytes(), held);
    }
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, SharedControlBlockLivesUntilLastWeakReference) {
    const auto before = Bytes();
    std::weak_ptr<uint64_t> weak;
    {
        auto shared = MakeCoreShared<uint64_t>(9);
        const auto allocated = Bytes();
        EXPECT_GT(allocated, before);
        auto other = shared;
        weak = shared;
        EXPECT_EQ(Bytes(), allocated);
    }
    EXPECT_TRUE(weak.expired());
    EXPECT_GT(Bytes(), before);
    weak.reset();
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, OversizedAllocationDoesNotChangeCounter) {
    const auto before = Bytes();
    CoreHeapAllocator<uint64_t> allocator;
    EXPECT_THROW((void)allocator.allocate(std::numeric_limits<size_t>::max()),
                 std::bad_alloc);
    EXPECT_EQ(Bytes(), before);
}

TEST(CoreHeapAccounting, ConcurrentLifetimesReturnToBaseline) {
    const auto before = Bytes();
    std::vector<std::thread> workers;
    for (size_t i = 0; i < 8; ++i) {
        workers.emplace_back([] {
            for (size_t j = 0; j < 1000; ++j) {
                CoreString key(std::string(256, 'k'));
                CoreVector<uint64_t> replicas(4, j);
                auto lease = MakeCoreShared<uint64_t>(j);
            }
        });
    }
    for (auto& worker : workers) worker.join();
    EXPECT_EQ(Bytes(), before);
}

}  // namespace
}  // namespace mooncake::test
