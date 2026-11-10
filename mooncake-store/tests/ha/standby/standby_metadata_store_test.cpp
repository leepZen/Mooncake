#include "ha/standby_metadata_store.h"

#include <gtest/gtest.h>

#include <set>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

namespace mooncake::test {
namespace {

StandbyObjectMetadata MakeMetadata(uint64_t size) {
    StandbyObjectMetadata metadata;
    metadata.size = size;
    return metadata;
}

}  // namespace

static_assert(
    std::is_move_constructible_v<StandbyMetadataStore::SnapshotCursor>);
static_assert(
    !std::is_copy_constructible_v<StandbyMetadataStore::SnapshotCursor>);

TEST(StandbyMetadataStoreTest, EmptySnapshotCompletesWithoutObjects) {
    StandbyMetadataStore store;
    auto cursor = store.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;

    ASSERT_TRUE(store.CopyNextSnapshotChunk(2, cursor, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_TRUE(cursor.done());
}

TEST(StandbyMetadataStoreTest, CopiesMultipleTenantsAcrossBoundedChunks) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a1", MakeMetadata(1)));
    ASSERT_TRUE(store.PutMetadata("tenant-a", "a2", MakeMetadata(2)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b1", MakeMetadata(3)));
    ASSERT_TRUE(store.PutMetadata("tenant-b", "b2", MakeMetadata(4)));
    ASSERT_TRUE(store.PutMetadata("tenant-c", "c1", MakeMetadata(5)));

    auto cursor = store.BeginSnapshotTraversal();
    std::set<std::pair<std::string, std::string>> copied;
    while (!cursor.done()) {
        std::vector<StandbyObjectEntry> chunk;
        ASSERT_TRUE(store.CopyNextSnapshotChunk(2, cursor, chunk));
        ASSERT_LE(chunk.size(), 2u);
        for (const auto& entry : chunk) {
            EXPECT_TRUE(copied.emplace(entry.tenant_id, entry.key).second);
        }
    }

    EXPECT_EQ(5u, copied.size());
    EXPECT_EQ(5u, store.GetKeyCount());
}

TEST(StandbyMetadataStoreTest, RejectsZeroSizedChunkWithoutAdvancing) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.PutMetadata("tenant", "key", MakeMetadata(1)));
    auto cursor = store.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;

    EXPECT_FALSE(store.CopyNextSnapshotChunk(0, cursor, chunk));
    EXPECT_TRUE(chunk.empty());
    EXPECT_FALSE(cursor.done());
}

TEST(StandbyMetadataStoreTest, RestoreInsertRejectsDuplicateWithoutOverwrite) {
    StandbyMetadataStore store;
    ASSERT_TRUE(store.RestoreMetadata("tenant", "key", MakeMetadata(1)));

    EXPECT_FALSE(store.RestoreMetadata("tenant", "key", MakeMetadata(2)));
    auto restored = store.GetMetadata("tenant", "key");
    ASSERT_TRUE(restored.has_value());
    EXPECT_EQ(1u, restored->size);
}

TEST(StandbyMetadataStoreTest,
     IncrementalReplicaValidationSurvivesOverwriteAndDrain) {
    StandbyMetadataStore store;
    auto metadata = MakeMetadata(1);
    Replica::Descriptor replica;
    replica.id = 7;
    replica.descriptor_variant = DiskDescriptor{"file", 1};
    metadata.replicas.push_back(replica);
    ASSERT_TRUE(store.RestoreMetadata("tenant", "key", metadata));
    ReplicaID maximum = 0;
    EXPECT_TRUE(store.ValidateReplicaIds(maximum));
    EXPECT_EQ(7u, maximum);
    metadata.replicas.push_back(replica);
    store.PutMetadata("tenant", "key", metadata);
    EXPECT_FALSE(store.ValidateReplicaIds(maximum));
    metadata.replicas.pop_back();
    metadata.replicas.front().id = 3;
    store.PutMetadata("tenant", "key", metadata);
    EXPECT_TRUE(store.ValidateReplicaIds(maximum));
    EXPECT_EQ(3u, maximum);
    store.TrackChanges();
    std::vector<StandbyObjectEntry> chunk;
    ASSERT_TRUE(store.DrainChunk(1, chunk));
    EXPECT_EQ(0u, store.GetKeyCount());
    EXPECT_TRUE(store.ValidateReplicaIds(maximum));
    EXPECT_EQ(0u, maximum);
    EXPECT_EQ((std::set<StandbyMetadataStore::ObjectKey>{{"tenant", "key"}}),
              store.TakeChangedObjects());
    EXPECT_TRUE(store.TakeChangedObjects().empty());
    store.PutMetadata("tenant", "key", metadata);
    store.Clear();
    EXPECT_TRUE(store.ValidateReplicaIds(maximum));
    EXPECT_EQ(0u, maximum);
}

TEST(StandbyMetadataStoreTest, WeightMetadataSnapshotRoundTripsCompleteState) {
    const WeightRevisionIdentity identity{
        .tenant_id = "default",
        .name_space = "production",
        .resource_id = "llama-70b",
        .revision = "step-100",
        .weight_generation = 7,
    };
    const WeightMetadataSnapshot snapshot{
        .metadata = {WeightRevisionMetadata{
            .identity = identity,
            .manifest =
                WeightManifestReference{
                    .manifest_key =
                        "weights/production/llama-70b/step-100/7/manifest",
                    .manifest_sha256 = std::string(64, 'a'),
                    .payload_group_id = MakeWeightPayloadGroupId(identity),
                    .payload_keys_sha256 = std::string(64, 'b'),
                    .payload_count = 1,
                    .logical_bytes = 1024,
                },
            .availability = WeightAvailabilityState::READY,
            .residency = WeightResidencyState::HOT,
            .operation = WeightOperationState::EVICTING,
            .operation_id = 3,
            .metadata_generation = 4,
            .created_at_ms = 100,
            .updated_at_ms = 200,
        }},
        .leases = {WeightRevisionLease{
            .lease_id = 5,
            .identity = identity,
            .holder = "worker-0",
            .expires_at_ms = 300,
            .fenced_metadata_generation = 2,
        }},
        .operations = {WeightResidencyOperation{
            .operation_id = 3,
            .identity = identity,
            .operation = WeightOperationState::EVICTING,
            .target_residency = WeightResidencyState::COLD,
            .fenced_metadata_generation = 4,
            .started_at_ms = 150,
            .updated_at_ms = 200,
            .cursor = {},
            .message = {},
        }},
        .next_lease_id = 6,
        .next_operation_id = 4,
    };

    StandbyMetadataStore store;
    ASSERT_TRUE(store.RestoreWeightMetadata(snapshot));
    EXPECT_EQ(snapshot, store.SnapshotWeightMetadata());
}

TEST(StandbyMetadataStoreTest,
     RejectsInvalidWeightMetadataStoreWithoutMutation) {
    StandbyMetadataStore store;
    const WeightMetadataSnapshot empty;
    ASSERT_TRUE(store.RestoreWeightMetadata(empty));

    WeightMetadataSnapshot invalid;
    invalid.next_lease_id = 0;
    EXPECT_FALSE(store.RestoreWeightMetadata(invalid));
    EXPECT_EQ(empty, store.SnapshotWeightMetadata());
}

}  // namespace mooncake::test
