// object_part_test.cpp
//
// The content the envelope now holds in a part: the operations on the part
// itself, the envelope's forwarding to its single part, and the OpLog payload
// that carries the fields.

#include "object_metadata.h"
#include "object_part.h"

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <memory>
#include <optional>
#include <ranges>
#include <string>
#include <utility>
#include <vector>

#include <glog/logging.h>
#include <gtest/gtest.h>

#include "master_config.h"
#include "master_service.h"
#include "master_service/master_service_test_peer.h"
#include "metadata_store.h"
#include "replica.h"
#include "test_buffer_allocator.h"

namespace mooncake {
namespace {

constexpr size_t kObjectLength = 4096;
constexpr uint64_t kChecksum = 77;
constexpr const char* kGroupId = "group-1";
constexpr auto kWriteTime =
    std::chrono::system_clock::time_point(std::chrono::seconds(1700000000));

// A disk replica names a file and needs no allocator, so a suite about the
// part's own bookkeeping builds one without a segment.
Replica MakeDiskReplica(ReplicaStatus status) {
    return Replica(std::string("file://part-test"), kObjectLength, status);
}

ReplicaVector MakeDiskReplicas(const std::vector<ReplicaStatus>& statuses) {
    ReplicaVector replicas;
    replicas.reserve(statuses.size());
    for (const auto status : statuses) {
        replicas.push_back(MakeDiskReplica(status));
    }
    return replicas;
}

std::unique_ptr<ObjectMetadata> MakeEnvelope(size_t length,
                                             ReplicaVector&& replicas) {
    return std::make_unique<ObjectMetadata>(
        UUID{1, 2}, kWriteTime, length, std::move(replicas), std::nullopt,
        false, ObjectDataType::UNKNOWN, std::string(kGroupId), TenantId(),
        "part-test-key");
}

}  // namespace

// Building the OpLog payload needs a service, so this case carries the logging
// setup the other service-backed suites use.
class OplogPayloadTest : public ::testing::Test {
   protected:
    void SetUp() override {
        google::InitGoogleLogging("OplogPayloadTest");
        FLAGS_logtostderr = true;
    }

    // glog rejects a second InitGoogleLogging with the same process, so each
    // case releases the one it took.
    void TearDown() override { google::ShutdownGoogleLogging(); }
};

TEST(ObjectPartTest, HoldsItsLengthAndChecksum) {
    ObjectPart part(kObjectLength, ReplicaVector{});

    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_FALSE(part.checksum().has_value());
    EXPECT_TRUE(part.replicas().empty());

    part.SetChecksum(kChecksum);
    ASSERT_TRUE(part.checksum().has_value());
    EXPECT_EQ(*part.checksum(), kChecksum);
}

TEST(ObjectPartTest, NoFMetadataIsHeldUntilDeferredReplicasAreReleased) {
    const auto before = GetMasterCoreHeapAccount().Load();
    auto allocator = std::make_shared<test::TestBufferAllocator>(
        "nof-meta-test", "nof-meta-endpoint", 16 * 1024 * 1024);
    EXPECT_EQ(GetMasterCoreHeapAccount().Load(), before);
    ReplicaVector deferred;
    uint64_t held_bytes = 0;
    {
        auto small = allocator->allocate(4096);
        ASSERT_NE(small, nullptr);
        const auto one_buffer = GetMasterCoreHeapAccount().Load() - before;
        auto large = allocator->allocate(1024 * 1024);
        ASSERT_NE(large, nullptr);
        // A larger remote value does not allocate a larger metadata object.
        EXPECT_EQ(GetMasterCoreHeapAccount().Load() - before, one_buffer * 2);
        small.reset();

        ReplicaVector replicas;
        replicas.emplace_back(std::move(large), ReplicaStatus::COMPLETE,
                              ReplicaType::NOF_SSD);
        ObjectPart part(1024 * 1024, std::move(replicas));
        held_bytes = GetMasterCoreHeapAccount().Load();
        deferred = part.PopReplicas();
        EXPECT_EQ(GetMasterCoreHeapAccount().Load(), held_bytes);
    }
    EXPECT_EQ(GetMasterCoreHeapAccount().Load(), held_bytes);
    deferred.clear();
    EXPECT_GT(GetMasterCoreHeapAccount().Load(), before);  // Reserved array.
    ReplicaVector{}.swap(deferred);
    EXPECT_EQ(GetMasterCoreHeapAccount().Load(), before);
}

TEST(EnvelopePartTest, PendingAndDynamicStateDoNotCountAsCoreHeap) {
    const auto before = GetMasterCoreHeapAccount().Load();
    {
        auto metadata = MakeEnvelope(kObjectLength, {});
        const auto core = GetMasterCoreHeapAccount().Load();
        metadata->BeginSessionTag(std::string(512, 's'), {1, 2, 3});
        metadata->BeginSoftPinAction({}, {1, 2, 3});
        metadata->MarkDynamicReplica(
            1, {kWriteTime, std::string(512, 'a'), std::string(512, 'b'),
                std::string(512, 'd'), false});
        EXPECT_EQ(GetMasterCoreHeapAccount().Load(), core);
        metadata->CommitSessionTag(1);
        EXPECT_GE(GetMasterCoreHeapAccount().Load() - core, 513u);
    }
    EXPECT_EQ(GetMasterCoreHeapAccount().Load(), before);
}

TEST(ObjectPartTest, AppendsReplicasAndReadsThemThroughTheSpan) {
    ObjectPart part(kObjectLength, ReplicaVector{});
    auto replicas =
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING});
    const ReplicaID processing_id = replicas.back().id();
    part.AddReplicas(std::move(replicas));

    EXPECT_EQ(part.replicas().size(), 2u);
    EXPECT_TRUE(
        std::ranges::any_of(part.replicas(), Replica::fn_is_processing));
    EXPECT_FALSE(
        std::ranges::all_of(part.replicas(), Replica::fn_is_completed));
    EXPECT_TRUE(std::ranges::any_of(part.replicas(), [&](const Replica& r) {
        return r.id() == processing_id;
    }));

    // These are disk replicas, so the medium predicates answer false for them.
    EXPECT_FALSE(
        std::ranges::any_of(part.replicas(), Replica::fn_is_memory_replica));
    EXPECT_FALSE(
        std::ranges::any_of(part.replicas(), Replica::fn_is_nof_replica));
}

TEST(ObjectPartTest, TheSpanReachesTheStoredReplicas) {
    ObjectPart part(kObjectLength, ReplicaVector{});
    part.AddReplicas(MakeDiskReplicas({ReplicaStatus::PROCESSING}));

    const auto processing =
        std::ranges::find_if(part.replicas(), Replica::fn_is_processing);
    ASSERT_NE(processing, part.replicas().end());
    processing->mark_complete();

    EXPECT_TRUE(std::ranges::all_of(part.replicas(), Replica::fn_is_completed));
}

TEST(ObjectPartTest, PopsOnlyTheMatchingReplicas) {
    ObjectPart part(kObjectLength, ReplicaVector{});
    part.AddReplicas(
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    auto popped = part.PopReplicas(&Replica::fn_is_processing);
    ASSERT_EQ(popped.size(), 1u);
    EXPECT_EQ(popped.front().status(), ReplicaStatus::PROCESSING);
    EXPECT_EQ(part.replicas().size(), 2u);
    EXPECT_TRUE(std::ranges::all_of(part.replicas(), Replica::fn_is_completed));

    const ReplicaID kept_id = part.replicas().front().id();
    auto erased = part.PopReplicas(
        [&](const Replica& replica) { return replica.id() == kept_id; });
    EXPECT_EQ(erased.size(), 1u);
    EXPECT_EQ(part.replicas().size(), 1u);

    auto remaining = part.PopReplicas();
    EXPECT_EQ(remaining.size(), 1u);
    EXPECT_TRUE(part.replicas().empty());
}

TEST(EnvelopePartTest, TheEnvelopeReadsAndWritesItsSinglePart) {
    auto metadata = MakeEnvelope(kObjectLength,
                                 MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    metadata->MutablePart(0).SetChecksum(kChecksum);

    // The read accessor and the write accessor name the same part, and the
    // envelope's own operations answer with what that part holds.
    const ObjectPart& part = metadata->GetPart(0);
    EXPECT_EQ(&metadata->MutablePart(0), &part);
    EXPECT_EQ(part.length(), kObjectLength);
    EXPECT_EQ(*part.checksum(), kChecksum);
    EXPECT_EQ(metadata->TotalLength(), part.length());
    EXPECT_EQ(metadata->CountReplicas(), part.replicas().size());
    EXPECT_EQ(metadata->GetAllReplicas().data(), part.replicas().data());
    EXPECT_EQ(metadata->GetAllReplicas().size(), part.replicas().size());
    EXPECT_TRUE(metadata->HasReplica(&Replica::fn_is_completed));
}

TEST(EnvelopePartTest, VisitsAndErasesThroughTheEnvelope) {
    auto metadata = MakeEnvelope(
        kObjectLength,
        MakeDiskReplicas({ReplicaStatus::COMPLETE, ReplicaStatus::PROCESSING,
                          ReplicaStatus::COMPLETE}));

    size_t visited = 0;
    const size_t matched = metadata->VisitReplicas(
        &Replica::fn_is_completed, [&](Replica& replica) {
            ++visited;
            EXPECT_EQ(replica.status(), ReplicaStatus::COMPLETE);
        });
    EXPECT_EQ(matched, 2u);
    EXPECT_EQ(visited, 2u);

    // Only the ones the predicate matches leave the part, and what comes out
    // is dropped because the caller here only wants them gone.
    EXPECT_EQ(metadata->EraseReplicas(&Replica::fn_is_processing), 1u);
    EXPECT_EQ(metadata->CountReplicas(), 2u);
    EXPECT_TRUE(metadata->AllReplicas(&Replica::fn_is_completed));

    const ObjectMetadata& read_only = *metadata;
    const size_t counted = read_only.VisitReplicas(&Replica::fn_is_completed,
                                                   [](const Replica&) {});
    EXPECT_EQ(counted, 2u);
}

TEST(EnvelopePartTest, ValidityFollowsThePartsLengthAndReplicas) {
    auto with_replica = MakeEnvelope(
        kObjectLength, MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_TRUE(with_replica->IsValid());

    auto without_replica = MakeEnvelope(kObjectLength, {});
    EXPECT_FALSE(without_replica->IsValid());

    auto without_content =
        MakeEnvelope(0, MakeDiskReplicas({ReplicaStatus::COMPLETE}));
    EXPECT_FALSE(without_content->IsValid());
}

// The OpLog payload carries the envelope's own fields and no checksum, so this
// case pins the length the part holds and the fields beside it.
TEST_F(OplogPayloadTest, TheOplogPayloadCarriesTheEnvelopeFields) {
    MasterService service{MasterServiceConfig{}};
    auto metadata = MakeEnvelope(kObjectLength, {});

    const std::string bytes =
        test::MasterServiceTestPeer::SerializeMetadataForOpLogForTesting(
            service, *metadata);

    MetadataPayload payload;
    ASSERT_EQ(struct_pack::deserialize_to(payload, bytes),
              struct_pack::errc::ok);
    EXPECT_EQ(payload.client_id, (UUID{1, 2}));
    EXPECT_EQ(payload.size, metadata->TotalLength());
    EXPECT_TRUE(payload.replicas.empty());
    EXPECT_EQ(payload.group_id.value_or(""), kGroupId);
    EXPECT_EQ(payload.data_type.value_or(ObjectDataType::UNKNOWN),
              ObjectDataType::UNKNOWN);
    EXPECT_FALSE(payload.hard_pinned.value_or(true));
}

TEST_F(OplogPayloadTest,
       MasterCoreHeapReturnsToBaselineAfterServiceDestruction) {
    const auto before = GetMasterCoreHeapAccount().Load();
    {
        MasterServiceConfig config;
        config.default_kv_lease_ttl = 0;
        MasterService service(config);
        EXPECT_EQ(GetMasterCoreHeapAccount().Load(), before);

        Segment segment;
        segment.id = generate_uuid();
        segment.name = "core-heap-segment";
        segment.te_endpoint = segment.name;
        segment.base = 0x300000000;
        segment.size = 16 * 1024 * 1024;
        const UUID client = generate_uuid();
        ASSERT_TRUE(service.MountSegment(segment, client).has_value());
        // Offset allocator nodes and segment registrations are not core KV.
        EXPECT_EQ(GetMasterCoreHeapAccount().Load(), before);

        const std::string key(512, 'k');
        ReplicateConfig replicas;
        replicas.replica_num = 1;
        ASSERT_TRUE(
            service.PutStart(client, key, TenantId::Default(), 4096, replicas)
                .has_value());
        ASSERT_TRUE(
            service
                .PutEnd(client, key, TenantId::Default(), ReplicaType::MEMORY)
                .has_value());
        const auto published = GetMasterCoreHeapAccount().Load();
        EXPECT_GE(published - before, 2 * (key.size() + 1));
        ASSERT_TRUE(service.Remove(key, TenantId::Default()).has_value());
        EXPECT_LT(GetMasterCoreHeapAccount().Load(), published);
    }
    EXPECT_EQ(GetMasterCoreHeapAccount().Load(), before);
}

}  // namespace mooncake
