#pragma once

// ObjectMetadata: the per-object metadata envelope.
//
// Moved out of MasterService so the service and the per-object metadata
// module share one definition. The envelope owns the object identity, the
// authoritative read lease, the soft-pin state and the tenant quota ledger.
// The object's content and the replicas holding it live in the envelope's
// single ObjectPart.
//
// It has no ownership lock: whoever owns the envelope decides how the
// envelope as a whole is guarded. The `lock` member below is internal and
// narrower — it protects only `lease_`, `last_access_time` and `soft_pin_ttl`,
// so a holder of the envelope can read either without them tearing.

#include <algorithm>
#include <chrono>
#include <cstdint>
#include <functional>
#include <memory>
#include <optional>
#include <ranges>
#include <span>
#include <string>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "lease.h"
#include "master_metric_manager.h"
#include "mutex.h"
#include "object_part.h"
#include "replica.h"
#include "tenant_id.h"
#include "tenant_quota_ledger.h"
#include "types.h"

namespace mooncake {

struct ResolvedSoftPinRequest {
    SoftPinAction action{SoftPinAction::PRESERVE};
    std::chrono::milliseconds ttl{0};
};

// Carried soft-pin state when an existing object's metadata is rebuilt (e.g.
// a size-changing upsert). The sliding window is reconstructed as
// `last_access_time + soft_pin_ttl`; `soft_pin_ttl == 0` means not pinned.
struct CommittedSoftPinState {
    std::chrono::system_clock::time_point last_access_time;
    std::chrono::milliseconds soft_pin_ttl{0};
};

struct ObjectMetadata {
    struct SoftPinEvaluation {
        bool active{false};
        int metric_delta{0};
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
    };

    // Three-state outcome of the background renewal check. Encapsulated here
    // so the object spin lock serializes the effective-deadline read and any
    // state transition (see EvaluateSoftPinAtExpiry).
    enum class SoftPinExpiryAction { kRenew, kExpire, kAlreadyCleared };

    struct SoftPinExpiryEvaluation {
        SoftPinExpiryAction action{SoftPinExpiryAction::kAlreadyCleared};
        std::chrono::system_clock::time_point new_deadline;
    };

    struct PendingSoftPinAction {
        SoftPinAction action{SoftPinAction::PRESERVE};
        std::chrono::milliseconds ttl{0};
        std::unordered_set<ReplicaID> eligible_replica_ids;
    };

    // RAII-style metric management
    ~ObjectMetadata() {
        MasterMetricManager::instance().dec_key_count(1);
        if (soft_pin_ttl != std::chrono::milliseconds::zero()) {
            MasterMetricManager::instance().dec_soft_pin_key_count(1);
        }
    }

    ObjectMetadata() = delete;

    ObjectMetadata(const UUID& client_id_,
                   const std::chrono::system_clock::time_point put_start_time_,
                   size_t value_length, std::vector<Replica>&& reps,
                   std::optional<CommittedSoftPinState> committed_soft_pin =
                       std::nullopt,
                   bool enable_hard_pin = false,
                   ObjectDataType data_type_ = ObjectDataType::UNKNOWN,
                   std::string group_id_ = "", TenantId tenant_id_ = TenantId(),
                   std::string user_key_ = {})
        : client_id(client_id_),
          put_start_time(put_start_time_),
          data_type(data_type_),
          group_id(std::move(group_id_)),
          tenant_id(std::move(tenant_id_)),
          user_key(std::move(user_key_)),
          last_access_time(committed_soft_pin
                               ? committed_soft_pin->last_access_time
                               : put_start_time_),
          soft_pin_ttl(committed_soft_pin
                           ? committed_soft_pin->soft_pin_ttl
                           : std::chrono::milliseconds::zero()),
          hard_pinned(enable_hard_pin) {
        parts_.emplace_back(value_length, std::move(reps));
        MasterMetricManager::instance().inc_key_count(1);
        if (soft_pin_ttl != std::chrono::milliseconds::zero()) {
            MasterMetricManager::instance().inc_soft_pin_key_count(1);
        }
        MasterMetricManager::instance().observe_value_size(value_length);
    }

    ObjectMetadata(const ObjectMetadata&) = delete;
    ObjectMetadata& operator=(const ObjectMetadata&) = delete;
    ObjectMetadata(ObjectMetadata&&) = delete;
    ObjectMetadata& operator=(ObjectMetadata&&) = delete;

    // Updated by UpsertStart (Case B) to reflect the new writer.
    UUID client_id;
    // Updated by UpsertStart (Case B) to reset the discard timeout.
    std::chrono::system_clock::time_point put_start_time;
    const ObjectDataType data_type{ObjectDataType::UNKNOWN};
    const std::string group_id;
    const TenantId tenant_id;
    const std::string user_key;

    // Committed attribution only; protected by the owning metadata shard lock.
    std::string session_id;
    struct PendingSessionTag {
        std::string session_id;
        std::unordered_set<ReplicaID> eligible_replica_ids;
    };
    std::optional<PendingSessionTag> pending_session_tag;

    void BeginSessionTag(std::string id,
                         const std::unordered_set<ReplicaID>& replicas) {
        pending_session_tag.reset();
        if (!id.empty()) {
            pending_session_tag = PendingSessionTag{std::move(id), replicas};
        }
    }

    void CommitSessionTag(ReplicaID replica_id) {
        if (pending_session_tag &&
            pending_session_tag->eligible_replica_ids.contains(replica_id)) {
            session_id = std::move(pending_session_tag->session_id);
            pending_session_tag.reset();
        }
    }

    void ClearSessionTagIfNoViableReplica() {
        if (pending_session_tag && !HasReplica([this](const Replica& replica) {
                return pending_session_tag->eligible_replica_ids.contains(
                           replica.id()) &&
                       replica.is_processing() &&
                       !replica.has_invalid_mem_handle() &&
                       !replica.has_invalid_nof_handle();
            })) {
            pending_session_tag.reset();
        }
    }

    mutable SpinLock lock;
    // Authoritative lease: ungrouped objects own one; grouped objects share
    // the group's. Never null after construction.
    mutable std::shared_ptr<Lease> lease_ GUARDED_BY(lock) =
        std::make_shared<Lease>();
    // Sliding soft-pin window: active iff soft_pin_ttl != 0, with
    // effective_deadline = last_access_time + soft_pin_ttl. Both are
    // runtime-only (never persisted).
    mutable std::chrono::system_clock::time_point last_access_time
        GUARDED_BY(lock);
    mutable std::chrono::milliseconds soft_pin_ttl{0} GUARDED_BY(lock);
    // Replica IDs scope this action to the current write. PutEnd does not
    // carry a generation token, so a stale End from the same client cannot
    // otherwise be distinguished from the current write.
    std::optional<PendingSoftPinAction> pending_soft_pin_action;
    const bool hard_pinned{false};  // immutable, set at creation
    bool memory_cache_total_accounted{false};
    bool disk_cache_total_accounted{false};
    TenantQuotaLedger quota_ledger;

    struct DynamicReplicaRecord {
        std::chrono::system_clock::time_point created_at;
        std::string source_segment;
        std::string target_segment;
        std::string target_domain;
        bool complete{false};
    };

    std::unordered_map<ReplicaID, DynamicReplicaRecord> dynamic_replicas;
    std::chrono::steady_clock::time_point dynamic_replication_recreate_after{};

    void MarkDynamicReplica(ReplicaID replica_id, DynamicReplicaRecord record) {
        dynamic_replicas[replica_id] = std::move(record);
    }

    void MarkDynamicReplicasComplete(
        const std::vector<ReplicaID>& replica_ids) {
        for (const auto& replica_id : replica_ids) {
            auto it = dynamic_replicas.find(replica_id);
            if (it != dynamic_replicas.end()) {
                it->second.complete = true;
            }
        }
    }

    size_t ForgetDynamicReplicas(const std::vector<ReplicaID>& replica_ids) {
        size_t forgotten = 0;
        for (const auto& replica_id : replica_ids) {
            forgotten += dynamic_replicas.erase(replica_id);
        }
        return forgotten;
    }

    size_t DynamicReplicaCount() const { return dynamic_replicas.size(); }

    bool DynamicReplicationRecreateBlocked(
        std::chrono::steady_clock::time_point now) const {
        return now < dynamic_replication_recreate_after;
    }

    void SetDynamicReplicationRecreateAfter(
        std::chrono::steady_clock::time_point deadline) {
        dynamic_replication_recreate_after =
            std::max(dynamic_replication_recreate_after, deadline);
    }

    // Read under the owner's shared lock, change under its exclusive lock. A
    // replica pointer taken from a part is invalidated by any add or pop on it.
    const ObjectPart& GetPart(size_t index) const { return parts_.at(index); }

    ObjectPart& MutablePart(size_t index) { return parts_.at(index); }

    size_t TotalLength() const {
        size_t total = 0;
        for (const auto& part : parts_) {
            total += part.length();
        }
        return total;
    }

    // Temporary: these operate on part 0. The multi-part work replaces them
    // with `parts()` and a flattened `replicas()` view, so each call site says
    // which it means.
    void AddReplicas(std::vector<Replica>&& replicas) {
        MutablePart(0).AddReplicas(std::move(replicas));
    }

    std::vector<Replica> PopReplicas(
        const std::function<bool(const Replica&)>& pred_fn) {
        return MutablePart(0).PopReplicas(pred_fn);
    }

    std::vector<Replica> PopReplicas() { return MutablePart(0).PopReplicas(); }

    size_t EraseReplicas(const std::function<bool(const Replica&)>& pred_fn) {
        return MutablePart(0).PopReplicas(pred_fn).size();
    }

    // visit_fn may modify the replica but must not add or pop replicas.
    size_t VisitReplicas(const std::function<bool(const Replica&)>& pred_fn,
                         const std::function<void(Replica&)>& visit_fn) {
        size_t num_visited = 0;

        for (auto& replica : MutablePart(0).replicas()) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    size_t VisitReplicas(
        const std::function<bool(const Replica&)>& pred_fn,
        const std::function<void(const Replica&)>& visit_fn) const {
        size_t num_visited = 0;

        for (const auto& replica : GetPart(0).replicas()) {
            if (pred_fn(replica)) {
                visit_fn(replica);
                num_visited++;
            }
        }

        return num_visited;
    }

    bool HasReplica(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::ranges::any_of(GetPart(0).replicas(), pred_fn);
    }

    // True iff the object is backed by NoF replicas only, with no memory or
    // disk replica keeping it resident. Gates both access-based soft pin
    // renewal and the TTL reclaim of expired pins.
    bool IsNoFOnly() const {
        return HasReplica([](const Replica& replica) {
                   return replica.is_nof_replica();
               }) &&
               !HasReplica([](const Replica& replica) {
                   return !replica.is_nof_replica();
               });
    }

    bool AllReplicas(const std::function<bool(const Replica&)>& pred_fn) const {
        return std::ranges::all_of(GetPart(0).replicas(), pred_fn);
    }

    size_t CountReplicas(
        const std::function<bool(const Replica&)>& pred_fn) const {
        return static_cast<size_t>(
            std::ranges::count_if(GetPart(0).replicas(), pred_fn));
    }

    size_t CountReplicas() const { return GetPart(0).replicas().size(); }

    std::span<const Replica> GetAllReplicas() const {
        return GetPart(0).replicas();
    }

    Replica* GetFirstReplica(
        const std::function<bool(const Replica&)>& pred_fn) {
        auto replicas = MutablePart(0).replicas();
        const auto it = std::ranges::find_if(replicas, pred_fn);
        return it != replicas.end() ? &(*it) : nullptr;
    }

    Replica* GetReplicaByID(const ReplicaID& id) {
        return GetFirstReplica(
            [&id](const Replica& replica) { return replica.id() == id; });
    }

    Replica* GetReplicaBySegmentName(const std::string& segment_name) {
        return GetFirstReplica([&segment_name](const Replica& replica) {
            const auto names = replica.get_segment_names();
            return std::ranges::any_of(names,
                                       [&segment_name](const auto& name_opt) {
                                           return name_opt == segment_name;
                                       });
        });
    }

    // Grant a read lease at now() + ttl. Grouped objects extend the shared
    // group TTL; a zero ttl is a no-op on a live lease (PutEnd cannot
    // expire a live group).
    void GrantReadLease(std::chrono::milliseconds ttl) const {
        SpinLocker locker(&lock);
        lease_->GrantReadLease(ttl);
    }

    // Whether the authoritative lease is expired.
    bool IsLeaseExpired() const {
        SpinLocker locker(&lock);
        return lease_->IsExpired(std::chrono::system_clock::now());
    }

    bool IsLeaseExpired(std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        return lease_->IsExpired(now);
    }

    // Lease deadline for the eviction census.
    std::chrono::system_clock::time_point EvictionDeadline() const {
        return lease_->ExpiresAt();
    }

    // Active state of the soft pin: soft_pin_ttl != 0. Requires the object spin
    // lock; unlocked callers use the public accessors below.
    bool SoftPinActiveLocked() const REQUIRES(lock) {
        return soft_pin_ttl != std::chrono::milliseconds::zero();
    }

    // Record a read hit for access-based renewal: overwrite `last_access_time`
    // under the object spin lock. Called from the read paths (GetReplicaList
    // et al.) while holding a RO metadata shard lock; never touches the index.
    void TouchLastAccess() const {
        SpinLocker locker(&lock);
        last_access_time = std::chrono::system_clock::now();
    }

    SoftPinEvaluation EvaluateSoftPin(
        const std::chrono::system_clock::time_point& now) const {
        SpinLocker locker(&lock);
        if (SoftPinActiveLocked() &&
            ComputeSoftPinDeadline(last_access_time, soft_pin_ttl) <= now) {
            const auto removed_deadline =
                ComputeSoftPinDeadline(last_access_time, soft_pin_ttl);
            soft_pin_ttl = std::chrono::milliseconds::zero();
            return {.active = false,
                    .metric_delta = -1,
                    .removed_deadline = removed_deadline,
                    .deadline_to_index = std::nullopt};
        }
        return {.active = SoftPinActiveLocked(),
                .metric_delta = 0,
                .removed_deadline = std::nullopt,
                .deadline_to_index = std::nullopt};
    }

    // Background renewal check for CleanupExpiredSoftPins: atomically decide
    // under the object spin lock whether the popped deadline is renewed (the
    // sliding window moved past `now`), expired, or already cleared by a
    // concurrent path. `allow_renew` (switch on && NoF-only) is passed in by
    // the caller; when false, a still-live window is forced to expire to keep
    // the static-window semantics.
    SoftPinExpiryEvaluation EvaluateSoftPinAtExpiry(
        const std::chrono::system_clock::time_point& now,
        bool allow_renew) const {
        SpinLocker locker(&lock);
        if (!SoftPinActiveLocked()) {
            return {.action = SoftPinExpiryAction::kAlreadyCleared,
                    .new_deadline = std::chrono::system_clock::time_point{}};
        }
        const auto deadline =
            ComputeSoftPinDeadline(last_access_time, soft_pin_ttl);
        if (allow_renew && deadline > now) {
            return {.action = SoftPinExpiryAction::kRenew,
                    .new_deadline = deadline};
        }
        soft_pin_ttl = std::chrono::milliseconds::zero();
        return {.action = SoftPinExpiryAction::kExpire,
                .new_deadline = std::chrono::system_clock::time_point{}};
    }

    // Saturates at the clock's maximum rather than overflowing when the TTL
    // reaches past it. The comparison is done in milliseconds because
    // converting a huge TTL up to the clock's finer period would overflow.
    static std::chrono::system_clock::time_point ComputeSoftPinDeadline(
        const std::chrono::system_clock::time_point& now,
        std::chrono::milliseconds ttl) {
        const auto max_time = std::chrono::system_clock::time_point::max();
        const auto headroom = max_time - now;
        const auto remaining =
            std::chrono::duration_cast<std::chrono::milliseconds>(headroom);
        return ttl > remaining ? max_time : now + ttl;
    }

    // The current effective soft-pin deadline (saturated), a thin wrapper over
    // ComputeSoftPinDeadline reading the sliding window. Pure accessor that
    // acquires the object spin lock; locked call sites reuse the static
    // ComputeSoftPinDeadline directly to avoid re-entrant locking.
    std::chrono::system_clock::time_point EffectiveSoftPinDeadline() const {
        SpinLocker locker(&lock);
        return ComputeSoftPinDeadline(last_access_time, soft_pin_ttl);
    }

    // Snapshot of the committed soft-pin sliding window (last_access_time +
    // soft_pin_ttl) for metadata rebuild. Returns nullopt when the object is
    // not soft-pinned.
    std::optional<CommittedSoftPinState> GetCommittedSoftPinState() const {
        SpinLocker locker(&lock);
        if (!SoftPinActiveLocked()) {
            return std::nullopt;
        }
        return CommittedSoftPinState{last_access_time, soft_pin_ttl};
    }

    // Assign the sliding window so its effective deadline equals `deadline`
    // exactly. The window is anchored one millisecond before `deadline` with a
    // one-millisecond TTL, so soft_pin_ttl stays non-zero (active) while
    // EffectiveSoftPinDeadline() reproduces `deadline`. Used by tests and the
    // scenario DSL that specify soft-pin state as an absolute expiry.
    void SetSoftPinDeadline(std::chrono::system_clock::time_point deadline) {
        SpinLocker locker(&lock);
        last_access_time = deadline - std::chrono::milliseconds(1);
        soft_pin_ttl = std::chrono::milliseconds(1);
    }

    void BeginSoftPinAction(
        const ResolvedSoftPinRequest& request,
        std::unordered_set<ReplicaID> eligible_replica_ids) {
        pending_soft_pin_action = PendingSoftPinAction{
            request.action, request.ttl, std::move(eligible_replica_ids)};
    }

    bool PendingSoftPinOwnsReplica(ReplicaID replica_id) const {
        if (!pending_soft_pin_action) {
            return false;
        }
        return pending_soft_pin_action->eligible_replica_ids.contains(
            replica_id);
    }

    void ClearPendingSoftPinAction() { pending_soft_pin_action.reset(); }

    SoftPinEvaluation CommitPendingSoftPin(
        const std::chrono::system_clock::time_point& now) {
        if (!pending_soft_pin_action) {
            return EvaluateSoftPin(now);
        }

        const PendingSoftPinAction pending =
            std::move(*pending_soft_pin_action);
        pending_soft_pin_action.reset();

        return ApplySoftPin({pending.action, pending.ttl}, now);
    }

    // Caller holds the object's metadata write lock. Independent commands do
    // not consume or replace a pending write's soft-pin action.
    SoftPinEvaluation ApplySoftPin(
        const ResolvedSoftPinRequest& request,
        const std::chrono::system_clock::time_point& now) {
        SpinLocker locker(&lock);
        int metric_delta = 0;
        std::optional<std::chrono::system_clock::time_point> removed_deadline;
        std::optional<std::chrono::system_clock::time_point> deadline_to_index;
        if (SoftPinActiveLocked() &&
            ComputeSoftPinDeadline(last_access_time, soft_pin_ttl) <= now) {
            removed_deadline =
                ComputeSoftPinDeadline(last_access_time, soft_pin_ttl);
            soft_pin_ttl = std::chrono::milliseconds::zero();
            --metric_delta;
        }

        switch (request.action) {
            case SoftPinAction::PRESERVE:
                break;
            case SoftPinAction::ENABLE:
                if (request.ttl == std::chrono::milliseconds::zero()) {
                    if (SoftPinActiveLocked()) {
                        removed_deadline =
                            ComputeSoftPinDeadline(last_access_time,
                                                   soft_pin_ttl);
                        soft_pin_ttl = std::chrono::milliseconds::zero();
                        --metric_delta;
                    }
                } else {
                    if (!SoftPinActiveLocked()) {
                        ++metric_delta;
                    }
                    last_access_time = now;
                    soft_pin_ttl = request.ttl;
                    deadline_to_index =
                        ComputeSoftPinDeadline(now, request.ttl);
                    // Upserting the latest registration supersedes any
                    // expired or previously active deadline.
                    removed_deadline.reset();
                }
                break;
            case SoftPinAction::DISABLE:
                if (SoftPinActiveLocked()) {
                    removed_deadline =
                        ComputeSoftPinDeadline(last_access_time, soft_pin_ttl);
                    soft_pin_ttl = std::chrono::milliseconds::zero();
                    --metric_delta;
                }
                break;
        }
        return {.active = SoftPinActiveLocked(),
                .metric_delta = metric_delta,
                .removed_deadline = removed_deadline,
                .deadline_to_index = deadline_to_index};
    }

    void ClearPendingSoftPinIfNoViableReplica() {
        if (!pending_soft_pin_action) {
            return;
        }
        const auto& eligible = pending_soft_pin_action->eligible_replica_ids;
        const bool has_viable_replica =
            HasReplica([&eligible](const Replica& replica) {
                const bool belongs_to_write = eligible.contains(replica.id());
                const bool valid_handle = !replica.has_invalid_mem_handle() &&
                                          !replica.has_invalid_nof_handle();
                return belongs_to_write && replica.is_processing() &&
                       valid_handle;
            });
        if (!has_viable_replica) {
            pending_soft_pin_action.reset();
        }
    }

    bool IsHardPinned() const { return hard_pinned; }

    bool IsGrouped() const { return !group_id.empty(); }

    // Check if the metadata is valid
    // Valid means it has at least one valid replica and size is greater
    // than 0
    bool IsValid() const {
        return GetPart(0).length() > 0 &&
               HasReplica([](const Replica& replica) {
                   return !replica.is_memory_replica() ||
                          !replica.has_invalid_mem_handle();
               });
    }

    std::vector<std::string> GetReplicaSegmentNames() const {
        std::vector<std::string> segment_names;
        for (const auto& replica : GetPart(0).replicas()) {
            const auto& segment_name_options = replica.get_segment_names();
            for (const auto& segment_name_opt : segment_name_options) {
                if (segment_name_opt.has_value()) {
                    segment_names.push_back(segment_name_opt.value());
                }
            }
        }
        return segment_names;
    }

   private:
    std::vector<ObjectPart> parts_;
};

}  // namespace mooncake
