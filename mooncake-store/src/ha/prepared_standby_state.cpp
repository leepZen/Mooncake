#include "ha/prepared_standby_state.h"

#include <future>
#include <limits>

#include "master_service.h"
#include "segment_allocator_registration.h"

namespace mooncake {
namespace {
std::string MemoryKey(const UUID& id) { return "memory:" + UuidToString(id); }
bool HasPreparedMemory(const StandbySegmentInfo& info) {
    return info.memory && info.memory->segment.protocol != "cxl";
}
std::string LegacyMemoryKey(const std::string& endpoint) {
    return "legacy-memory:" + endpoint;
}
bool OccupiesSpace(const Replica::Descriptor& replica) {
    return replica.is_nof_replica()
               ? replica.status == ReplicaStatus::COMPLETE
               : replica.status != ReplicaStatus::FAILED &&
                     replica.status != ReplicaStatus::REMOVED;
}
const AllocatedBuffer::Descriptor* Buffer(const Replica::Descriptor& replica) {
    if (replica.is_memory_replica())
        return &replica.get_memory_descriptor().buffer_descriptor;
    if (replica.is_nof_replica())
        return &replica.get_nof_descriptor().buffer_descriptor;
    return nullptr;
}
}  // namespace

PreparedStandbyState::PreparedStandbyState(const MasterServiceConfig& config)
    : service_(std::make_shared<MasterService>(
          config, MasterService::StartMode::kStandby)) {
    service_->standby_replica_index_complete_ = true;
}
PreparedStandbyState::~PreparedStandbyState() = default;

ErrorCode PreparedStandbyState::UpdateSegments(
    const StandbySegmentRegistry& registry, std::set<Key>& changed) {
    if (segment_revision_ == registry.Revision()) return ErrorCode::OK;
    std::map<std::string, StandbySegmentInfo> next;
    std::unordered_map<std::string, std::string> aliases;
    std::map<UUID, std::string> memory_by_id;
    std::unordered_map<std::string, std::vector<std::string>> memory_by_alias;
    std::set<UUID> nof_ids;
    for (const auto& info : registry.GetAllSegments()) {
        if (!info.is_memory_segment && !info.nof) continue;
        if (info.segment_name.empty() || info.transport_endpoint.empty() ||
            info.capacity == 0)
            return ErrorCode::INVALID_PARAMS;
        if (info.memory) {
            const auto& memory = *info.memory;
            const auto& segment = memory.segment;
            if (!info.is_memory_segment || info.nof ||
                segment.id == UUID{0, 0} ||
                memory.owner_client_id == UUID{0, 0} || segment.base == 0 ||
                segment.name != info.segment_name ||
                segment.size != info.capacity ||
                segment.te_endpoint != info.transport_endpoint ||
                segment.base >
                    std::numeric_limits<uintptr_t>::max() - segment.size)
                return ErrorCode::INVALID_PARAMS;
        }
        if (info.nof) {
            const auto& n = *info.nof;
            if (n.segment_id == UUID{0, 0} || n.owner_client_id == UUID{0, 0} ||
                n.segment_name != info.segment_name ||
                n.transport_endpoint != info.transport_endpoint ||
                n.capacity != info.capacity ||
                !nof_ids.insert(n.segment_id).second ||
                n.base > std::numeric_limits<uintptr_t>::max() - n.capacity)
                return ErrorCode::INVALID_PARAMS;
        }
        auto add_alias = [&](const std::string& name) {
            return aliases.emplace(name, info.transport_endpoint).second;
        };
        if (HasPreparedMemory(info)) {
            const auto key = MemoryKey(info.memory->segment.id);
            if (!memory_by_id.emplace(info.memory->segment.id, key).second)
                return ErrorCode::INVALID_PARAMS;
            memory_by_alias[info.transport_endpoint].push_back(key);
            if (info.segment_name != info.transport_endpoint)
                memory_by_alias[info.segment_name].push_back(key);
            next.emplace(key, info);
        } else {
            if (!add_alias(info.transport_endpoint) ||
                (!info.nof && info.segment_name != info.transport_endpoint &&
                 !add_alias(info.segment_name)))
                return ErrorCode::INVALID_PARAMS;
            next.emplace(info.transport_endpoint, info);
        }
    }
    const auto touch = [&](const StandbySegmentInfo& info) {
        const auto add_members = [&](const std::string& identity) {
            const auto it = members_.find(identity);
            if (it != members_.end())
                changed.insert(it->second.begin(), it->second.end());
        };
        if (HasPreparedMemory(info)) {
            add_members(MemoryKey(info.memory->segment.id));
            add_members(LegacyMemoryKey(info.transport_endpoint));
            add_members(LegacyMemoryKey(info.segment_name));
            return;
        }
        for (const auto& alias : {info.transport_endpoint, info.segment_name}) {
            add_members(alias);
        }
        if (info.nof) dirty_nof_.insert(info.transport_endpoint);
    };
    for (const auto& [endpoint, info] : segments_) {
        auto it = next.find(endpoint);
        if (it == next.end() ||
            struct_pack::serialize(info) != struct_pack::serialize(it->second))
            touch(info);
    }
    for (const auto& [endpoint, info] : next) {
        auto it = segments_.find(endpoint);
        if (it == segments_.end() ||
            struct_pack::serialize(info) != struct_pack::serialize(it->second))
            touch(info);
    }
    auto& service = *service_;
    for (auto it = service.prepared_memory_segments_.begin();
         it != service.prepared_memory_segments_.end();) {
        auto replacement = next.find(MemoryKey(it->first));
        if (replacement != next.end() && replacement->second.memory &&
            *replacement->second.memory == it->second->metadata) {
            ++it;
            continue;
        }
        ReleaseOwner(it->second->metadata.owner_client_id);
        retired_memory_segments_.push_back(std::move(it->second));
        it = service.prepared_memory_segments_.erase(it);
    }
    for (const auto& [endpoint, info] : next) {
        if (!HasPreparedMemory(info) ||
            service.prepared_memory_segments_.contains(info.memory->segment.id))
            continue;
        auto prepared =
            std::make_shared<MasterService::PreparedMemorySegment>();
        prepared->metadata = *info.memory;
        prepared->allocator =
            std::make_shared<RestoredMemoryAllocator>(info.memory->segment);
        prepared->allocator->SetLocality(info.memory->segment.locality);
        auto owner = RetainOwner(info.memory->owner_client_id);
        prepared->registration = std::shared_ptr<SegmentAllocatorRegistration>(
            new SegmentAllocatorRegistration(prepared->allocator, owner));
        prepared->registration->segment_id_ = info.memory->segment.id;
        prepared->registration->SetAvailable(false);
        prepared->remount_lease = std::make_shared<Lease>();
        service.prepared_memory_segments_.emplace(info.memory->segment.id,
                                                  std::move(prepared));
    }
    // Old dummy allocators remain alive until the affected objects are erased.
    segments_ = std::move(next);
    aliases_ = std::move(aliases);
    memory_by_id_ = std::move(memory_by_id);
    memory_by_alias_ = std::move(memory_by_alias);
    service.standby_memory_segments_.clear();
    service.invalid_replica_endpoints_.clear();
    for (const auto& [endpoint, info] : segments_) {
        if (!info.nof) {
            service.standby_memory_segments_.push_back(info);
            service.invalid_replica_endpoints_.insert(info.transport_endpoint);
            service.invalid_replica_endpoints_.insert(info.segment_name);
        }
    }
    segment_revision_ = registry.Revision();
    return ErrorCode::OK;
}

std::shared_ptr<ClientLivenessRecord> PreparedStandbyState::RetainOwner(
    const UUID& owner) {
    auto [record, inserted] = service_->client_liveness_records_.try_emplace(
        owner, std::make_shared<ClientLivenessRecord>(
                   ClientLivenessRecord::Clock::now()));
    if (inserted)
        MasterMetricManager::instance().client_liveness_record_created();
    ++owner_refs_[owner];
    return record->second;
}

void PreparedStandbyState::ReleaseOwner(const UUID& owner) {
    auto count = owner_refs_.find(owner);
    if (count == owner_refs_.end() || --count->second != 0) return;
    owner_refs_.erase(count);
    auto record = service_->client_liveness_records_.find(owner);
    if (record != service_->client_liveness_records_.end()) {
        MasterMetricManager::instance().on_client_liveness_record_removed(
            record->second->state());
        service_->client_liveness_records_.erase(record);
    }
}

void PreparedStandbyState::RemoveObject(const Key& key) {
    auto& service = *service_;
    MasterService::MetadataAccessorRW object(
        &service, {TenantId(key.first), key.second},
        MasterService::MetadataAccessorRW::AsIs{});
    if (object.IsPublished()) {
        for (const auto& replica : object.Get().GetAllReplicas()) {
            auto descriptor = replica.get_descriptor();
            if (descriptor.is_local_disk_replica()) {
                ReleaseOwner(descriptor.get_local_disk_descriptor().client_id);
            }
            const auto* buffer = Buffer(descriptor);
            if (!buffer) continue;
            const auto& endpoint = buffer->transport_endpoint_;
            const bool prepared_memory =
                descriptor.is_memory_replica() &&
                std::dynamic_pointer_cast<RestoredMemoryAllocator>(
                    std::get<MemoryReplicaData>(replica.data_)
                        .buffer->getAllocator());
            const auto range_key =
                prepared_memory
                    ? MemoryKey(*descriptor.get_memory_descriptor().segment_id)
                    : endpoint;
            if (OccupiesSpace(descriptor)) {
                ranges_[range_key].erase(buffer->buffer_address_);
                if (ranges_[range_key].empty()) ranges_.erase(range_key);
                used_bytes_[range_key] -= buffer->size_;
                if (used_bytes_[range_key] == 0) used_bytes_.erase(range_key);
                if (descriptor.is_memory_replica() && !prepared_memory) {
                    // The dummy holds the original name, including across a
                    // registry replacement, so subtract from the old account.
                    const auto allocator =
                        service.standby_allocator_keepalive_.at(endpoint);
                    service.standby_accounted_memory_bytes_
                        [allocator->getSegmentName()] -= buffer->size_;
                    MasterMetricManager::instance().dec_allocated_mem_size(
                        allocator->getSegmentName(), buffer->size_);
                    if (service.standby_accounted_memory_bytes_
                            [allocator->getSegmentName()] == 0)
                        service.standby_accounted_memory_bytes_.erase(
                            allocator->getSegmentName());
                }
            }
            if (descriptor.is_nof_replica()) dirty_nof_.insert(endpoint);
        }
        object.Erase();
    }
    const auto old = object_endpoints_.find(key);
    if (old != object_endpoints_.end()) {
        for (const auto& endpoint : old->second) {
            auto it = members_.find(endpoint);
            if (it != members_.end()) {
                it->second.erase(key);
                if (it->second.empty()) {
                    members_.erase(it);
                    service.standby_allocator_keepalive_.erase(endpoint);
                }
            }
            auto index = service.standby_replica_index_.find(endpoint);
            if (index != service.standby_replica_index_.end()) {
                index->second.erase(key);
                if (index->second.empty())
                    service.standby_replica_index_.erase(index);
            }
        }
        object_endpoints_.erase(old);
    }
    repairs_.erase(key);
}

ErrorCode PreparedStandbyState::PutObject(
    const Key& key, const StandbyObjectMetadata& metadata) {
    ++prepared_objects_;
    auto& service = *service_;
    TenantId tenant(key.first);
    if (!tenant.IsValid()) return ErrorCode::INVALID_PARAMS;
    std::vector<Replica> replicas;
    std::vector<std::shared_ptr<Lease>> recovery_leases;
    bool discarded = false;
    for (const auto& descriptor : metadata.replicas) {
        if (const auto* buffer = Buffer(descriptor)) {
            const auto& original_endpoint = buffer->transport_endpoint_;
            object_endpoints_[key].insert(original_endpoint);
            members_[original_endpoint].insert(key);
            if (descriptor.is_memory_replica()) {
                const auto& id = descriptor.get_memory_descriptor().segment_id;
                const auto identity =
                    id ? MemoryKey(*id) : LegacyMemoryKey(original_endpoint);
                object_endpoints_[key].insert(identity);
                members_[identity].insert(key);
            }
            std::string segment_key;
            if (descriptor.is_memory_replica()) {
                const auto& id = descriptor.get_memory_descriptor().segment_id;
                if (id) {
                    auto known = memory_by_id_.find(*id);
                    if (known != memory_by_id_.end()) {
                        const auto& info = segments_.at(known->second);
                        if (original_endpoint == info.transport_endpoint ||
                            original_endpoint == info.segment_name)
                            segment_key = known->second;
                    } else if (memory_by_alias_.contains(original_endpoint)) {
                        discarded = true;
                        repairs_.insert(key);
                        continue;
                    }
                } else if (auto candidates =
                               memory_by_alias_.find(original_endpoint);
                           candidates != memory_by_alias_.end()) {
                    for (const auto& candidate : candidates->second) {
                        const auto& value =
                            segments_.at(candidate).memory->segment;
                        if (buffer->buffer_address_ < value.base ||
                            buffer->buffer_address_ - value.base >=
                                value.size ||
                            buffer->size_ >
                                value.size -
                                    (buffer->buffer_address_ - value.base))
                            continue;
                        if (!segment_key.empty())
                            return ErrorCode::INVALID_PARAMS;
                        segment_key = candidate;
                    }
                }
            }
            if (segment_key.empty()) {
                auto alias = aliases_.find(original_endpoint);
                if (alias != aliases_.end()) segment_key = alias->second;
            }
            if (segment_key.empty() || bool(segments_.at(segment_key).nof) !=
                                           descriptor.is_nof_replica()) {
                discarded = true;
                if (descriptor.is_nof_replica() ||
                    memory_by_alias_.contains(original_endpoint))
                    repairs_.insert(key);
                continue;
            }
            const auto& segment = segments_.at(segment_key);
            const auto& endpoint = segment.transport_endpoint;
            if (descriptor.is_memory_replica() && segment.memory &&
                descriptor.get_memory_descriptor().segment_id &&
                *descriptor.get_memory_descriptor().segment_id !=
                    segment.memory->segment.id) {
                discarded = true;
                repairs_.insert(key);
                continue;
            }
            object_endpoints_[key].insert(endpoint);
            members_[endpoint].insert(key);
            if (HasPreparedMemory(segment)) {
                object_endpoints_[key].insert(segment_key);
                members_[segment_key].insert(key);
            }
            if (buffer->size_ == 0 || buffer->size_ != metadata.size ||
                buffer->buffer_address_ >
                    std::numeric_limits<uintptr_t>::max() - buffer->size_)
                return ErrorCode::INVALID_PARAMS;
            if (segment.nof &&
                (buffer->buffer_address_ < segment.nof->base ||
                 buffer->buffer_address_ - segment.nof->base >=
                     segment.capacity ||
                 buffer->size_ > segment.capacity - (buffer->buffer_address_ -
                                                     segment.nof->base)))
                return ErrorCode::INVALID_PARAMS;
            if (OccupiesSpace(descriptor)) {
                auto& ranges = ranges_[segment_key];
                auto next = ranges.lower_bound(buffer->buffer_address_);
                if ((next != ranges.end() &&
                     buffer->buffer_address_ + buffer->size_ > next->first) ||
                    (next != ranges.begin() &&
                     std::prev(next)->first + std::prev(next)->second >
                         buffer->buffer_address_) ||
                    used_bytes_[segment_key] > segment.capacity ||
                    buffer->size_ > segment.capacity - used_bytes_[segment_key])
                    return ErrorCode::INVALID_PARAMS;
                ranges.emplace(buffer->buffer_address_, buffer->size_);
                used_bytes_[segment_key] += buffer->size_;
                if (descriptor.is_memory_replica() &&
                    !HasPreparedMemory(segment)) {
                    service.standby_accounted_memory_bytes_
                        [segment.segment_name] += buffer->size_;
                    MasterMetricManager::instance().inc_allocated_mem_size(
                        segment.segment_name, buffer->size_);
                }
            }
            auto canonical = *buffer;
            canonical.transport_endpoint_ = endpoint;
            std::unique_ptr<AllocatedBuffer> restored;
            auto prepared_memory = service.prepared_memory_segments_.find(
                segment.memory ? segment.memory->segment.id : UUID{0, 0});
            if (descriptor.is_memory_replica() && OccupiesSpace(descriptor) &&
                prepared_memory != service.prepared_memory_segments_.end()) {
                auto& prepared = *prepared_memory->second;
                restored = prepared.allocator->ReserveAt(canonical);
                if (!restored) return ErrorCode::INVALID_PARAMS;
                prepared.registration->BindBuffer(*restored);
                recovery_leases.push_back(prepared.remount_lease);
            } else {
                auto& allocator =
                    service.standby_allocator_keepalive_[endpoint];
                if (!allocator ||
                    allocator->getSegmentName() != segment.segment_name)
                    allocator = std::make_shared<DummyBufferAllocator>(
                        segment.segment_name, endpoint);
                restored =
                    std::make_unique<AllocatedBuffer>(allocator, canonical);
            }
            if (descriptor.is_nof_replica()) {
                replicas.push_back(Replica(descriptor.id, std::move(restored),
                                           descriptor.status,
                                           ReplicaType::NOF_SSD));
                dirty_nof_.insert(endpoint);
            } else {
                replicas.push_back(Replica(descriptor.id, std::move(restored),
                                           descriptor.status));
            }
        } else if (descriptor.is_disk_replica()) {
            const auto& disk = descriptor.get_disk_descriptor();
            if (disk.object_size != metadata.size)
                return ErrorCode::INVALID_PARAMS;
            replicas.push_back(Replica(descriptor.id, disk.file_path,
                                       disk.object_size, descriptor.status));
        } else if (descriptor.is_local_disk_replica()) {
            const auto& disk = descriptor.get_local_disk_descriptor();
            if (disk.object_size != metadata.size)
                return ErrorCode::INVALID_PARAMS;
            auto owner = RetainOwner(disk.client_id);
            replicas.push_back(Replica(
                descriptor.id, disk.client_id, disk.object_size,
                disk.transport_endpoint, descriptor.status, std::move(owner)));
        } else {
            return ErrorCode::INVALID_PARAMS;
        }
    }
    if (replicas.empty() && discarded) return ErrorCode::OK;
    MasterService::MetadataAccessorRW object(
        &service, {tenant, key.second},
        MasterService::MetadataAccessorRW::CreateKey{});
    const auto generation = object.Publish(
        metadata.client_id, std::chrono::system_clock::now(), metadata.size,
        std::move(replicas), std::nullopt, metadata.hard_pinned.value_or(false),
        metadata.data_type, metadata.group_id, tenant, key.second);
    object.Get().session_id = metadata.session_id.value_or("");
    if (!metadata.group_id.empty())
        object.Get().lease_ =
            service.RegisterGroupMember(tenant, key.second, metadata.group_id);
    for (const auto& lease : recovery_leases)
        object.Get().lease_->AttachRecoveryLease(lease);
    object.OnDiskReplicaAdded(object.Get());
    if (service.enable_multi_tenants_) {
        auto* account = object.QuotaAccount();
        const uint64_t bytes = service.CompletedMemoryQuotaCharge(object.Get());
        if (bytes >
            TenantQuotaAccount::kMaxChargedBytes - account->ChargedBytes())
            return ErrorCode::INVALID_PARAMS;
        if (!object.Get().quota_ledger.Rebuild(account, bytes))
            return ErrorCode::INVALID_PARAMS;
        account->ChargeUnchecked(bytes);
    }
    for (const auto& endpoint : object_endpoints_[key])
        service.standby_replica_index_[endpoint][key] = {key.second,
                                                         generation};
    return ErrorCode::OK;
}

ErrorCode PreparedStandbyState::RebuildNoF(const std::string& endpoint) {
    auto& service = *service_;
    std::vector<Replica*> replicas;
    std::vector<LiveAllocation> allocations;
    auto segment = segments_.find(endpoint);
    if (segment != segments_.end() && segment->second.nof) {
        const auto& info = *segment->second.nof;
        for (const auto& key : members_[endpoint]) {
            MasterService::MetadataAccessorRW object(
                &service, {TenantId(key.first), key.second},
                MasterService::MetadataAccessorRW::AsIs{});
            if (!object.IsPublished()) continue;
            object.Get().VisitReplicas(
                [](const Replica& r) {
                    return r.is_nof_replica() && r.is_completed();
                },
                [&](Replica& r) {
                    const auto buffer = r.get_descriptor()
                                            .get_nof_descriptor()
                                            .buffer_descriptor;
                    if (buffer.transport_endpoint_ == endpoint) {
                        replicas.push_back(&r);
                        allocations.push_back(
                            {buffer.buffer_address_ - info.base, buffer.size_});
                    }
                });
        }
    }
    if (auto mounted = mounted_nof_.find(endpoint);
        mounted != mounted_nof_.end()) {
        auto access = service.nof_segment_manager_.getNoFSegmentAccess();
        size_t capacity = 0;
        auto err =
            access.PrepareUnmountSegment(mounted->second.segment_id, capacity);
        if (err != ErrorCode::OK) return err;
        err = access.CommitUnmountSegment(mounted->second.segment_id,
                                          mounted->second.owner_client_id,
                                          capacity);
        if (err != ErrorCode::OK) return err;
        mounted_nof_.erase(mounted);
    }
    if (segment == segments_.end() || !segment->second.nof)
        return ErrorCode::OK;
    const auto& info = *segment->second.nof;
    auto imported = ImportOffsetBufferAllocator(
        info.segment_name, info.base, info.capacity, endpoint, allocations,
        ReplicaType::NOF_SSD);
    if (!imported) return ErrorCode::INVALID_PARAMS;
    NoFSegment value;
    value.id = info.segment_id;
    value.name = info.segment_name;
    value.te_endpoint = endpoint;
    value.base = info.base;
    value.size = info.capacity;
    value.host_id = info.host_id;
    value.rack_id = info.rack_id;
    {
        auto access = service.nof_segment_manager_.getNoFSegmentAccess();
        auto err = access.MountSegment(value, info.owner_client_id,
                                       imported->allocator);
        if (err != ErrorCode::OK) return err;
    }
    mounted_nof_[endpoint] = info;
    for (size_t i = 0; i < replicas.size(); ++i) {
        if (!replicas[i]->replace_nof_buffer(std::move(imported->buffers[i])))
            return ErrorCode::INTERNAL_ERROR;
    }
    return ErrorCode::OK;
}

ErrorCode PreparedStandbyState::Finish(StandbyMetadataStore& metadata,
                                       DurablePrefix cursor) {
    for (const auto& endpoint : dirty_nof_) {
        auto err = RebuildNoF(endpoint);
        if (err != ErrorCode::OK) return err;
    }
    dirty_nof_.clear();
    retired_memory_segments_.clear();
    if (weight_revision_ != metadata.WeightRevision()) {
        if (!service_->weight_manager_.RestoreSnapshot(
                metadata.SnapshotWeightMetadata()))
            return ErrorCode::INVALID_PARAMS;
        weight_revision_ = metadata.WeightRevision();
    }
    ReplicaID maximum = 0;
    if (!metadata.ValidateReplicaIds(maximum)) return ErrorCode::INVALID_PARAMS;
    auto current = Replica::next_id_.load();
    while (current < maximum + 1 &&
           !Replica::next_id_.compare_exchange_weak(current, maximum + 1)) {
    }
    cursor_ = cursor;
    ready_ = true;
    return ErrorCode::OK;
}

ErrorCode PreparedStandbyState::Bootstrap(
    StandbyMetadataStore& metadata, const StandbySegmentRegistry& registry,
    DurablePrefix cursor, size_t chunk_size) {
    if (chunk_size == 0 || ready_ || bound_ ||
        ((cursor.batch_id == 0) != (cursor.last_seq == 0)))
        return ErrorCode::INVALID_PARAMS;
    ReplicaID maximum;
    if (!metadata.ValidateReplicaIds(maximum)) return ErrorCode::INVALID_PARAMS;
    std::set<Key> ignored;
    auto error = UpdateSegments(registry, ignored);
    if (error != ErrorCode::OK) return error;
    auto scan = metadata.BeginSnapshotTraversal();
    std::vector<StandbyObjectEntry> chunk;
    while (!scan.done()) {
        if (!metadata.CopyNextSnapshotChunk(chunk_size, scan, chunk))
            return ErrorCode::INTERNAL_ERROR;
        for (const auto& entry : chunk) {
            error = PutObject({entry.tenant_id, entry.key}, entry.metadata);
            if (error != ErrorCode::OK) return error;
        }
    }
    error = Finish(metadata, cursor);
    if (error == ErrorCode::OK) metadata.TrackChanges();
    return error;
}

ErrorCode PreparedStandbyState::Synchronize(
    StandbyMetadataStore& metadata, const StandbySegmentRegistry& registry,
    DurablePrefix cursor) {
    if (!ready_ || bound_ || cursor.last_seq < cursor_.last_seq ||
        cursor.batch_id < cursor_.batch_id ||
        ((cursor.batch_id == 0) != (cursor.last_seq == 0)))
        return ErrorCode::INVALID_PARAMS;
    ready_ = false;
    ReplicaID maximum;
    if (!metadata.ValidateReplicaIds(maximum)) return ErrorCode::INVALID_PARAMS;
    auto changed = metadata.TakeChangedObjects();
    auto error = UpdateSegments(registry, changed);
    if (error != ErrorCode::OK) return error;
    // Remove every old extent before installing the final batch image. A
    // delete followed by address reuse must not depend on key sort order.
    for (const auto& key : changed) RemoveObject(key);
    for (const auto& key : changed) {
        auto value = metadata.GetMetadata(key.first, key.second);
        if (value) {
            error = PutObject(key, *value);
            if (error != ErrorCode::OK) return error;
        }
    }
    return Finish(metadata, cursor);
}

ErrorCode PreparedStandbyState::PersistRepairs() {
    if (repairs_.empty() || !service_->enable_oplog_) return ErrorCode::OK;
    if (!service_->ordered_oplog_writer_) return ErrorCode::INTERNAL_ERROR;
    std::vector<OpLogEntry> entries;
    for (const auto& key : repairs_) {
        OpLogEntry entry;
        entry.tenant_id = key.first;
        entry.object_key = key.second;
        MasterService::MetadataAccessorRO object(
            service_.get(), {TenantId(key.first), key.second});
        entry.op_type = object.IsPublished() ? OpType::PUT_END : OpType::REMOVE;
        if (object.IsPublished())
            entry.payload = service_->SerializeMetadataForOpLog(object.Get());
        entries.push_back(std::move(entry));
    }
    auto done = std::make_shared<std::promise<ErrorCode>>();
    auto future = done->get_future();
    auto result = service_->AppendOpLogBatchWithDurableFinalize(
        std::move(entries), nullptr);
    if (!result) return result.error();
    // A successful-admission callback alone never fires on terminal failure.
    // AwaitDurable also wakes on fencing/Stop, including when timeout is zero.
    service_->ordered_oplog_writer_->AwaitDurable(*result).thenValue(
        [done](ErrorCode error) { done->set_value(error); });
    if (service_->restore_repair_durable_timeout_ms_ == 0)
        future.wait();
    else if (future.wait_for(std::chrono::milliseconds(
                 service_->restore_repair_durable_timeout_ms_)) !=
             std::future_status::ready)
        return ErrorCode::INTERNAL_ERROR;
    return future.get();
}

ErrorCode PreparedStandbyState::BindTerm(
    const MasterServiceConfig& config,
    OrderedOpLogWriter::TerminalCallback terminal) {
    if (!ready_ || bound_ || !service_->standby_runtime_ ||
        config.view_version == 0)
        return ErrorCode::UNAVAILABLE_IN_CURRENT_STATUS;
    bound_ = true;
    service_->view_version_ = config.view_version;
    try {
        if (service_->enable_multi_tenants_)
            service_->tenant_quota_manager_.LoadPoliciesOrThrow();
        service_->InitializeTermServices(config);
        service_->SetBatchOpLogTerminalCallback(std::move(terminal));
        return PersistRepairs();
    } catch (const std::exception& error) {
        LOG(ERROR) << "Prepared term binding failed: " << error.what();
        return ErrorCode::INTERNAL_ERROR;
    }
}

void PreparedStandbyState::AbortTerm() {
    if (!service_ || !bound_ || !service_->standby_runtime_) return;
    service_->StopBatchOpLogWriter();
    service_->ordered_oplog_writer_.reset();
    service_->batch_oplog_storage_.reset();
    service_->batch_oplog_kv_backend_.reset();
    service_->kv_event_publisher_.reset();
    service_->kv_track_tenant_epochs_ = false;
    service_->view_version_ = 0;
    bound_ = false;
}

void PreparedStandbyState::ReleaseActivatedService() {
    if (service_ && !service_->standby_runtime_) {
        ready_ = false;
        service_.reset();
    }
}
}  // namespace mooncake
