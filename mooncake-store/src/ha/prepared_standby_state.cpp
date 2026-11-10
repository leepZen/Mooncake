#include "ha/prepared_standby_state.h"

#include <future>
#include <limits>

#include "master_service.h"

namespace mooncake {
namespace {
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
    std::set<UUID> nof_ids;
    for (const auto& info : registry.GetAllSegments()) {
        if (!info.is_memory_segment && !info.nof) continue;
        if (info.segment_name.empty() || info.transport_endpoint.empty() ||
            info.capacity == 0)
            return ErrorCode::INVALID_PARAMS;
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
        if (!add_alias(info.transport_endpoint) ||
            (!info.nof && info.segment_name != info.transport_endpoint &&
             !add_alias(info.segment_name)))
            return ErrorCode::INVALID_PARAMS;
        next.emplace(info.transport_endpoint, info);
    }
    const auto touch = [&](const StandbySegmentInfo& info) {
        for (const auto& alias : {info.transport_endpoint, info.segment_name}) {
            const auto it = members_.find(alias);
            if (it != members_.end())
                changed.insert(it->second.begin(), it->second.end());
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
    // Old dummy allocators remain alive until the affected objects are erased.
    segments_ = std::move(next);
    aliases_ = std::move(aliases);
    auto& service = *service_;
    service.standby_memory_segments_.clear();
    service.invalid_replica_endpoints_.clear();
    for (const auto& [endpoint, info] : segments_) {
        if (!info.nof) {
            service.standby_memory_segments_.push_back(info);
            service.invalid_replica_endpoints_.insert(endpoint);
            service.invalid_replica_endpoints_.insert(info.segment_name);
        }
    }
    segment_revision_ = registry.Revision();
    return ErrorCode::OK;
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
                const auto owner =
                    descriptor.get_local_disk_descriptor().client_id;
                auto count = owner_refs_.find(owner);
                if (count != owner_refs_.end() && --count->second == 0) {
                    owner_refs_.erase(count);
                    const auto record =
                        service.client_liveness_records_.find(owner);
                    if (record != service.client_liveness_records_.end()) {
                        MasterMetricManager::instance()
                            .on_client_liveness_record_removed(
                                record->second->state());
                        service.client_liveness_records_.erase(record);
                    }
                }
            }
            const auto* buffer = Buffer(descriptor);
            if (!buffer) continue;
            const auto& endpoint = buffer->transport_endpoint_;
            if (OccupiesSpace(descriptor)) {
                ranges_[endpoint].erase(buffer->buffer_address_);
                if (ranges_[endpoint].empty()) ranges_.erase(endpoint);
                used_bytes_[endpoint] -= buffer->size_;
                if (used_bytes_[endpoint] == 0) used_bytes_.erase(endpoint);
                if (descriptor.is_memory_replica()) {
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
    bool discarded = false;
    for (const auto& descriptor : metadata.replicas) {
        if (const auto* buffer = Buffer(descriptor)) {
            const auto& original_endpoint = buffer->transport_endpoint_;
            object_endpoints_[key].insert(original_endpoint);
            members_[original_endpoint].insert(key);
            auto alias = aliases_.find(original_endpoint);
            if (alias == aliases_.end() ||
                bool(segments_.at(alias->second).nof) !=
                    descriptor.is_nof_replica()) {
                discarded = true;
                if (descriptor.is_nof_replica()) repairs_.insert(key);
                continue;
            }
            const auto& endpoint = alias->second;
            const auto& segment = segments_.at(endpoint);
            object_endpoints_[key].insert(endpoint);
            members_[endpoint].insert(key);
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
                auto& ranges = ranges_[endpoint];
                auto next = ranges.lower_bound(buffer->buffer_address_);
                if ((next != ranges.end() &&
                     buffer->buffer_address_ + buffer->size_ > next->first) ||
                    (next != ranges.begin() &&
                     std::prev(next)->first + std::prev(next)->second >
                         buffer->buffer_address_) ||
                    used_bytes_[endpoint] > segment.capacity ||
                    buffer->size_ > segment.capacity - used_bytes_[endpoint])
                    return ErrorCode::INVALID_PARAMS;
                ranges.emplace(buffer->buffer_address_, buffer->size_);
                used_bytes_[endpoint] += buffer->size_;
                if (descriptor.is_memory_replica()) {
                    service.standby_accounted_memory_bytes_
                        [segment.segment_name] += buffer->size_;
                    MasterMetricManager::instance().inc_allocated_mem_size(
                        segment.segment_name, buffer->size_);
                }
            }
            auto& allocator = service.standby_allocator_keepalive_[endpoint];
            if (!allocator ||
                allocator->getSegmentName() != segment.segment_name)
                allocator = std::make_shared<DummyBufferAllocator>(
                    segment.segment_name, endpoint);
            auto canonical = *buffer;
            canonical.transport_endpoint_ = endpoint;
            auto restored =
                std::make_unique<AllocatedBuffer>(allocator, canonical);
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
            auto [owner, inserted] =
                service.client_liveness_records_.try_emplace(
                    disk.client_id, std::make_shared<ClientLivenessRecord>(
                                        ClientLivenessRecord::Clock::now()));
            if (inserted) {
                MasterMetricManager::instance()
                    .client_liveness_record_created();
            }
            ++owner_refs_[disk.client_id];
            replicas.push_back(Replica(
                descriptor.id, disk.client_id, disk.object_size,
                disk.transport_endpoint, descriptor.status, owner->second));
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
