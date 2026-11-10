#include "ha/prepared_standby_state.h"

#include <gtest/gtest.h>
#include <async_simple/coro/SyncAwait.h>
#include <ylt/coro_rpc/coro_rpc_client.hpp>

#include "common/network.h"
#include "master_service/master_service_test_peer.h"
#include "master_service_test_fixture.h"
#include "rpc_service.h"

namespace mooncake::test {
namespace {
StandbySegmentInfo MemorySegment(const std::string& endpoint = "memory") {
    StandbySegmentInfo info;
    info.segment_name = endpoint;
    info.transport_endpoint = endpoint;
    info.capacity = 16 * 1024 * 1024;
    info.is_memory_segment = true;
    return info;
}

StandbyObjectMetadata MemoryObject(uint64_t id, uintptr_t address,
                                   const std::string& endpoint = "memory") {
    StandbyObjectMetadata metadata;
    metadata.client_id = {1, 2};
    metadata.size = 1024;
    AllocatedBuffer::Descriptor buffer;
    buffer.size_ = metadata.size;
    buffer.buffer_address_ = address;
    buffer.transport_endpoint_ = endpoint;
    Replica::Descriptor replica;
    replica.id = id;
    replica.status = ReplicaStatus::COMPLETE;
    replica.descriptor_variant = MemoryDescriptor{buffer};
    metadata.replicas.push_back(replica);
    return metadata;
}

MasterServiceConfig Config() {
    auto config = MasterServiceConfig::builder().set_enable_ha(false).build();
    config.root_fs_dir.clear();
    return config;
}

bool Published(MasterService& service, const std::string& key,
               const std::string& tenant = "default") {
    MasterServiceTestPeer::MetadataAccessorRO object(&service,
                                                     {TenantId(tenant), key});
    return object.IsPublished();
}
}  // namespace

TEST(PreparedStandbyStateTest, TailReplayAndTermBindingRetainUnchangedObjects) {
    auto config = Config();
    PreparedStandbyState prepared(config);
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    registry.OnSegmentMount(MemorySegment());
    for (uint64_t i = 0; i < 2000; ++i)
        ASSERT_TRUE(
            store.PutMetadata("default", std::to_string(i),
                              MemoryObject(i + 1, 0x100000 + i * 1024)));
    ASSERT_EQ(ErrorCode::OK,
              prepared.Bootstrap(store, registry, {1, 2000}, 17));
    auto service = prepared.Service();
    EXPECT_FALSE(MasterServiceTestPeer::EvictionThread(*service).joinable());
    const ObjectMetadata* unchanged;
    {
        MasterServiceTestPeer::MetadataAccessorRO object(
            service.get(), {TenantId::Default(), "1500"});
        unchanged = &object.Get();
    }
    ASSERT_TRUE(store.Remove("default", "1"));
    ASSERT_TRUE(store.PutMetadata("default", "replacement",
                                  MemoryObject(3000, 0x100400)));
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {2, 2002}));
    EXPECT_EQ(2001u, prepared.PreparedObjects());
    EXPECT_FALSE(Published(*service, "1"));
    EXPECT_TRUE(Published(*service, "replacement"));
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {2, 2002}));
    EXPECT_EQ(2001u, prepared.PreparedObjects());
    config.view_version = 7;
    ASSERT_EQ(ErrorCode::OK, prepared.BindTerm(config));
    EXPECT_EQ(service.get(), prepared.Service().get());
    EXPECT_EQ(2001u, prepared.PreparedObjects());
    EXPECT_FALSE(MasterServiceTestPeer::EvictionThread(*service).joinable());
    {
        MasterServiceTestPeer::MetadataAccessorRO object(
            service.get(), {TenantId::Default(), "1500"});
        EXPECT_EQ(unchanged, &object.Get());
    }
    prepared.AbortTerm();
    ASSERT_TRUE(store.Remove("default", "replacement"));
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {3, 2003}));
    EXPECT_FALSE(Published(*service, "replacement"));
    config.view_version = 8;
    EXPECT_EQ(ErrorCode::OK, prepared.BindTerm(config));
}

TEST(PreparedStandbyStateTest,
     MemoryRequiresRemountAndPreservesOccupiedExtents) {
    PreparedStandbyState prepared(Config());
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    registry.OnSegmentMount(MemorySegment());
    ASSERT_TRUE(store.PutMetadata("default", "key", MemoryObject(1, 0x100000)));
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 1}, 10));
    auto service = prepared.Service();
    EXPECT_FALSE(
        service->GetReplicaList("key", TenantId::Default()).has_value());
    Segment segment;
    segment.id = {3, 4};
    segment.name = "memory";
    segment.te_endpoint = "memory";
    segment.base = 0x100000;
    segment.size = MemorySegment().capacity;
    ASSERT_TRUE(service->ReMountSegment({segment}, UUID(5, 6)));
    auto replicas = service->GetReplicaList("key", TenantId::Default());
    ASSERT_TRUE(replicas);
    ASSERT_EQ(1u, replicas->replicas.size());
    EXPECT_EQ(0x100000u, replicas->replicas[0]
                             .get_memory_descriptor()
                             .buffer_descriptor.buffer_address_);
    ReplicateConfig replicate;
    auto allocation = service->PutStart(UUID(5, 6), "new", TenantId::Default(),
                                        1024, replicate);
    ASSERT_TRUE(allocation);
    EXPECT_NE(0x100000u, allocation->front()
                             .get_memory_descriptor()
                             .buffer_descriptor.buffer_address_);
}

TEST(PreparedStandbyStateTest, SegmentChangeRebuildsOnlyItsMembers) {
    PreparedStandbyState prepared(Config());
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    registry.OnSegmentMount(MemorySegment("first"));
    registry.OnSegmentMount(MemorySegment("second"));
    ASSERT_TRUE(store.PutMetadata("default", "first-key",
                                  MemoryObject(1, 0x100000, "first")));
    ASSERT_TRUE(store.PutMetadata("default", "second-key",
                                  MemoryObject(2, 0x200000, "second")));
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 2}, 1));
    registry.OnSegmentUnmount("first");
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {2, 3}));
    EXPECT_EQ(3u, prepared.PreparedObjects());
    EXPECT_FALSE(Published(*prepared.Service(), "first-key"));
    EXPECT_TRUE(Published(*prepared.Service(), "second-key"));
    registry.OnSegmentMount(MemorySegment("first"));
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {3, 4}));
    EXPECT_EQ(4u, prepared.PreparedObjects());
    EXPECT_TRUE(Published(*prepared.Service(), "first-key"));
}

TEST(PreparedStandbyStateTest, OverlappingTailNeverBecomesReady) {
    PreparedStandbyState prepared(Config());
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    registry.OnSegmentMount(MemorySegment());
    store.PutMetadata("default", "old", MemoryObject(1, 0x100000));
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 1}, 1));
    store.PutMetadata("default", "overlap", MemoryObject(2, 0x100200));
    EXPECT_EQ(ErrorCode::INVALID_PARAMS,
              prepared.Synchronize(store, registry, {2, 2}));
    EXPECT_FALSE(prepared.ReadyAt({1, 1}));
    EXPECT_FALSE(prepared.ReadyAt({2, 2}));
    auto config = Config();
    config.view_version = 8;
    EXPECT_EQ(ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS,
              prepared.BindTerm(config));
}

TEST(PreparedStandbyStateTest, NoFAllocatorIsReadyBeforeTermBinding) {
    PreparedStandbyState prepared(Config());
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    auto info = MemorySegment("nof");
    info.is_memory_segment = false;
    NoFSegmentOp nof;
    nof.segment_id = {3, 4};
    nof.owner_client_id = {5, 6};
    nof.segment_name = "nof";
    nof.transport_endpoint = "nof";
    nof.base = 0x100000;
    nof.capacity = info.capacity;
    info.nof = nof;
    registry.OnSegmentMount(info);
    auto object = MemoryObject(1, nof.base, "nof");
    object.replicas[0].descriptor_variant = NoFDescriptor{
        object.replicas[0].get_memory_descriptor().buffer_descriptor};
    store.PutMetadata("default", "key", object);
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 1}, 1));
    ASSERT_TRUE(prepared.Service()->GetReplicaList("key", TenantId::Default()));
    store.Remove("default", "key");
    store.PutMetadata("default", "replacement", object);
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {2, 3}));
    EXPECT_FALSE(Published(*prepared.Service(), "key"));
    ASSERT_TRUE(
        prepared.Service()->GetReplicaList("replacement", TenantId::Default()));
    registry.OnSegmentUnmount("nof");
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {3, 4}));
    EXPECT_FALSE(Published(*prepared.Service(), "replacement"));
}

TEST(PreparedStandbyStateTest, DormantRpcCannotModifyRetainedBaseline) {
    auto config = Config();
    PreparedStandbyState prepared(config);
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    StandbyObjectMetadata disk;
    disk.size = 1024;
    Replica::Descriptor replica;
    replica.id = 1;
    replica.status = ReplicaStatus::COMPLETE;
    replica.descriptor_variant = DiskDescriptor{"prepared", 1024};
    disk.replicas.push_back(replica);
    store.PutMetadata("default", "key", disk);
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 1}, 1));
    config.view_version = 7;
    ASSERT_EQ(ErrorCode::OK, prepared.BindTerm(config));
    WrappedMasterServiceConfig wrapper_config;
    wrapper_config.default_kv_lease_ttl = 1000;
    WrappedMasterService wrapper(wrapper_config, nullptr, "",
                                 prepared.Service());
    EXPECT_FALSE(wrapper.ServiceReady());
    EXPECT_FALSE(wrapper.Ping(UUID(3, 4)));

    const auto ports = getFreeTcpPorts(1);
    ASSERT_EQ(1u, ports.size());
    coro_rpc::coro_rpc_server server(1, ports.front(), "127.0.0.1");
    RegisterRpcService(server, wrapper);
    auto started = server.async_start();
    ASSERT_FALSE(started.hasResult());
    coro_rpc::coro_rpc_client client;
    using async_simple::coro::syncAwait;
    ASSERT_FALSE(
        syncAwait(client.connect("127.0.0.1", std::to_string(ports.front()))));
    auto rejected = syncAwait(
        client.call<&WrappedMasterService::Remove>("key", false, "default"));
    EXPECT_FALSE(rejected);
    EXPECT_TRUE(Published(*prepared.Service(), "key"));

    prepared.Service()->ActivatePreparedRuntime();
    EXPECT_TRUE(wrapper.ServiceReady());
    EXPECT_TRUE(
        MasterServiceTestPeer::EvictionThread(*prepared.Service()).joinable());
    // coro_rpc closes a connection after an RPC exception; clients reconnect
    // after the preparation gate opens.
    coro_rpc::coro_rpc_client serving_client;
    ASSERT_FALSE(syncAwait(
        serving_client.connect("127.0.0.1", std::to_string(ports.front()))));
    auto removed = syncAwait(serving_client.call<&WrappedMasterService::Remove>(
        "key", false, "default"));
    ASSERT_TRUE(removed);
    EXPECT_TRUE(removed.value());
    EXPECT_FALSE(Published(*prepared.Service(), "key"));
    server.stop();
}

TEST(PreparedStandbyStateTest, OldLocalDiskOwnerGetsActivationGrace) {
    auto config = Config();
    config.client_active_ttl_sec = 60;
    PreparedStandbyState prepared(config);
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    auto metadata = MemoryObject(1, 0);
    const UUID owner(3, 4);
    metadata.replicas[0].descriptor_variant =
        LocalDiskDescriptor{owner, 1024, "owner"};
    store.PutMetadata("default", "key", metadata);
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 1}, 1));
    auto& records =
        MasterServiceTestPeer::ClientLivenessRecords(*prepared.Service());
    auto record = records.at(owner);
    // Simulate a long standby interval without making the test sleep for TTL.
    ASSERT_EQ(ClientLivenessObservation::REFRESHED_ACTIVE,
              record->Observe(ClientLivenessRecord::Clock::now() -
                              std::chrono::hours(1)));
    config.view_version = 7;
    ASSERT_EQ(ErrorCode::OK, prepared.BindTerm(config));
    prepared.Service()->ActivatePreparedRuntime();
    std::this_thread::sleep_for(std::chrono::milliseconds(250));
    EXPECT_EQ(ClientLivenessState::ACTIVE, record->state());
    EXPECT_TRUE(prepared.Service()->GetReplicaList("key", TenantId::Default()));
}

TEST(PreparedStandbyStateTest, MissingRepairWriterCannotActivateTerm) {
    auto config = Config();
    config.enable_ha = true;
    config.enable_oplog = true;
    config.cluster_id = "prepared-repair";
    PreparedStandbyState prepared(config);
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    auto metadata = MemoryObject(1, 0x100000, "missing-nof");
    metadata.replicas[0].descriptor_variant = NoFDescriptor{
        metadata.replicas[0].get_memory_descriptor().buffer_descriptor};
    store.PutMetadata("default", "key", metadata);
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 1}, 1));
    config.view_version = 7;
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, prepared.BindTerm(config));
    EXPECT_FALSE(prepared.Service()->IsRuntimeServing());
    prepared.AbortTerm();
    EXPECT_TRUE(prepared.ReadyAt({1, 1}));
    EXPECT_EQ(ErrorCode::INTERNAL_ERROR, prepared.BindTerm(config));
    EXPECT_TRUE(store.GetMetadata("default", "key"));
}

class PreparedTenantStandbyTest : public MasterServiceTest {};

TEST(PreparedStandbyStateTest, TerminalRepairFailureWithoutTimeoutCanRetry) {
    auto config = Config();
    config.enable_ha = true;
    config.enable_oplog = true;
    config.cluster_id = "prepared-terminal-repair";
    config.restore_repair_durable_timeout_ms = 0;
    PreparedStandbyState prepared(config);
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    auto missing = MemoryObject(1, 0x100000, "missing-nof");
    missing.replicas[0].descriptor_variant = NoFDescriptor{
        missing.replicas[0].get_memory_descriptor().buffer_descriptor};
    store.PutMetadata("default", "removed", missing);
    auto partial = missing;
    Replica::Descriptor surviving;
    surviving.id = 2;
    surviving.status = ReplicaStatus::COMPLETE;
    surviving.descriptor_variant = DiskDescriptor{"survivor", 1024};
    partial.replicas.push_back(surviving);
    store.PutMetadata("default", "partial", partial);
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 2}, 1));
    auto& writer =
        MasterServiceTestPeer::OrderedOplogWriter(*prepared.Service());
    writer = std::make_unique<OrderedOpLogWriter>(
        OrderedOpLogWriterConfig{},
        [](const OpLogBatchRecord&, const DurablePrefix&) {
            return ErrorCode::INVALID_PARAMS;
        });
    writer->Start();
    config.view_version = 7;
    std::atomic<bool> terminal{false};
    EXPECT_EQ(
        ErrorCode::INVALID_PARAMS,
        prepared.BindTerm(config, [&](const auto&) { terminal.store(true); }));
    prepared.AbortTerm();
    EXPECT_TRUE(terminal.load());
    EXPECT_TRUE(prepared.ReadyAt({1, 2}));
    EXPECT_FALSE(prepared.Service()->IsRuntimeServing());
    EXPECT_TRUE(store.GetMetadata("default", "removed"));
    EXPECT_EQ(2u, store.GetMetadata("default", "partial")->replicas.size());

    std::vector<OpLogEntry> repaired;
    writer = std::make_unique<OrderedOpLogWriter>(
        OrderedOpLogWriterConfig{},
        [&](const OpLogBatchRecord& batch, const DurablePrefix&) {
            repaired = batch.entries;
            return ErrorCode::OK;
        });
    writer->Start();
    config.view_version = 8;
    ASSERT_EQ(ErrorCode::OK, prepared.BindTerm(config));
    prepared.AbortTerm();
    ASSERT_EQ(2u, repaired.size());
    EXPECT_EQ("partial", repaired[0].object_key);
    EXPECT_EQ(OpType::PUT_END, repaired[0].op_type);
    MetadataPayload canonical;
    ASSERT_EQ(struct_pack::errc::ok,
              struct_pack::deserialize_to(canonical, repaired[0].payload));
    ASSERT_EQ(1u, canonical.replicas.size());
    EXPECT_TRUE(canonical.replicas[0].is_disk_replica());
    EXPECT_EQ("removed", repaired[1].object_key);
    EXPECT_EQ(OpType::REMOVE, repaired[1].op_type);
}

TEST_F(PreparedTenantStandbyTest, QuotaAndGroupTrackReplacementAndDelete) {
    auto config = MakeStrictTenantConfig({"tenant-a", "tenant-b"});
    PreparedStandbyState prepared(config);
    StandbyMetadataStore store;
    StandbySegmentRegistry registry;
    registry.OnSegmentMount(MemorySegment());
    auto first = MemoryObject(1, 0x100000);
    first.group_id = "group";
    store.PutMetadata("tenant-a", "key", first);
    store.PutMetadata("tenant-b", "key", MemoryObject(2, 0x100400));
    ASSERT_EQ(ErrorCode::OK, prepared.Bootstrap(store, registry, {1, 2}, 1));
    {
        MasterServiceTestPeer::MetadataAccessorRO object(
            prepared.Service().get(), {TenantId("tenant-a"), "key"});
        ASSERT_TRUE(object.IsPublished());
        EXPECT_EQ(1024u, object.Get().quota_ledger.CommittedBytes());
    }
    store.Remove("tenant-a", "key");
    auto replacement = MemoryObject(3, 0x100800);
    replacement.group_id = "new-group";
    store.PutMetadata("tenant-b", "key", replacement);
    ASSERT_EQ(ErrorCode::OK, prepared.Synchronize(store, registry, {2, 4}));
    EXPECT_FALSE(Published(*prepared.Service(), "key", "tenant-a"));
    MasterServiceTestPeer::MetadataAccessorRW object(
        prepared.Service().get(), {TenantId("tenant-b"), "key"});
    EXPECT_EQ(1024u, object.QuotaAccount()->ChargedBytes());
    EXPECT_EQ(1024u, object.Get().quota_ledger.CommittedBytes());
    EXPECT_EQ("new-group", object.Get().group_id);
}
}  // namespace mooncake::test
