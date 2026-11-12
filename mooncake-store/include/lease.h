// lease.h
//
// Lock-free shared deadline for a group: every member holds a shared_ptr to the
// same Lease (CAS monotonic-max), so a read of any member extends one TTL.

#pragma once

#include <atomic>
#include <chrono>
#include <cstdint>
#include <algorithm>
#include <memory>
#include <vector>

namespace mooncake {

class Lease {
   public:
    // Extend the deadline (monotonic max) to now + ttl. The read path
    // advertises the full ttl to clients, so the server must honor now + ttl
    // rather than keeping an earlier existing deadline (which would let the
    // server evict before the client-held lease expires).
    void GrantReadLease(std::chrono::milliseconds ttl) {
        const auto ttl_ns =
            std::chrono::duration_cast<std::chrono::nanoseconds>(ttl).count();
        const int64_t now = NowNs();
        const int64_t new_deadline = now + ttl_ns;
        int64_t cur = deadline_ns_.load(std::memory_order_relaxed);
        while (cur < new_deadline &&
               !deadline_ns_.compare_exchange_weak(cur, new_deadline,
                                                   std::memory_order_relaxed)) {
        }
    }

    // Merge an externally-derived deadline (used when rebuilding a group's
    // lease from a snapshot so the group TTL is the max of member deadlines).
    void ExtendTo(const std::chrono::system_clock::time_point deadline) {
        const int64_t d = std::chrono::duration_cast<std::chrono::nanoseconds>(
                              deadline.time_since_epoch())
                              .count();
        int64_t cur = deadline_ns_.load(std::memory_order_relaxed);
        while (cur < d && !deadline_ns_.compare_exchange_weak(
                              cur, d, std::memory_order_relaxed)) {
        }
    }

    // Overwrite the deadline with an exact value (may lower it). Unlike
    // GrantReadLease/ExtendTo (monotonic max), this lets test scaffolding
    // (e.g. the scenario DSL) force a lease to a precise expiry.
    void SetDeadline(const std::chrono::system_clock::time_point deadline) {
        deadline_ns_.store(NowNs(deadline), std::memory_order_relaxed);
    }

    // True when the group has not been read within the TTL at `now`.
    bool IsExpired(const std::chrono::system_clock::time_point now) const {
        return now >= ExpiresAt();
    }

    // Recovery attaches segment leases before exposing objects. A group lease
    // shares these dependencies with every member, preserving group protection
    // when one segment remounts without visiting its individual objects.
    void AttachRecoveryLease(const std::shared_ptr<Lease>& lease) {
        if (!lease || lease.get() == this) return;
        auto current = std::atomic_load(&recovery_leases_);
        if (current && std::find(current->begin(), current->end(), lease) !=
                           current->end())
            return;
        auto next = std::make_shared<std::vector<std::shared_ptr<Lease>>>(
            current ? *current : std::vector<std::shared_ptr<Lease>>{});
        next->push_back(lease);
        std::atomic_store(
            &recovery_leases_,
            std::shared_ptr<const std::vector<std::shared_ptr<Lease>>>(
                std::move(next)));
        has_recovery_leases_.store(true, std::memory_order_release);
    }

    std::chrono::system_clock::time_point ExpiresAt() const {
        // system_clock is coarser than nanoseconds on some platforms (e.g.
        // microseconds with libc++), so the conversion must be explicit.
        auto deadline = std::chrono::system_clock::time_point(
            std::chrono::duration_cast<std::chrono::system_clock::duration>(
                std::chrono::nanoseconds(
                    deadline_ns_.load(std::memory_order_relaxed))));
        if (has_recovery_leases_.load(std::memory_order_acquire)) {
            auto leases = std::atomic_load(&recovery_leases_);
            for (const auto& lease : *leases)
                deadline = std::max(deadline, lease->ExpiresAt());
        }
        return deadline;
    }

   private:
    static int64_t NowNs() {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   std::chrono::system_clock::now().time_since_epoch())
            .count();
    }
    static int64_t NowNs(std::chrono::system_clock::time_point t) {
        return std::chrono::duration_cast<std::chrono::nanoseconds>(
                   t.time_since_epoch())
            .count();
    }

    std::atomic<int64_t> deadline_ns_{0};
    std::shared_ptr<const std::vector<std::shared_ptr<Lease>>> recovery_leases_;
    std::atomic<bool> has_recovery_leases_{false};
};

}  // namespace mooncake
