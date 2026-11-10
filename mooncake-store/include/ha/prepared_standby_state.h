#pragma once

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>

#include "ha/oplog/oplog_batch_types.h"
#include "ha/oplog/ordered_oplog_writer.h"
#include "ha/standby_metadata_store.h"
#include "master_config.h"

namespace mooncake {
class MasterService;

// A single replay writer prepares this runtime behind a closed serving gate.
// Internal references stay at stable addresses when the RPC wrapper adopts
// Service(). The descriptor store remains a rollback/snapshot baseline until
// activation; it is never copied or converted during handoff.
class PreparedStandbyState {
   public:
    explicit PreparedStandbyState(const MasterServiceConfig& config);
    ~PreparedStandbyState();
    ErrorCode Bootstrap(StandbyMetadataStore& metadata,
                        const StandbySegmentRegistry& registry,
                        DurablePrefix cursor, size_t chunk_size);
    ErrorCode Synchronize(StandbyMetadataStore& metadata,
                          const StandbySegmentRegistry& registry,
                          DurablePrefix cursor);
    ErrorCode BindTerm(const MasterServiceConfig& config,
                       OrderedOpLogWriter::TerminalCallback terminal = {});
    void AbortTerm();
    // The RPC wrapper already owns the activated service. Do not retain its
    // workers while the supervisor asynchronously reclaims preparation indexes.
    void ReleaseActivatedService();
    bool ReadyAt(DurablePrefix cursor) const {
        return ready_ && cursor_ == cursor;
    }
    std::shared_ptr<MasterService> Service() const { return service_; }
    // Work counter used to assert that tail replay/handoff does not visit
    // unrelated objects. Counts actual object reconstruction, including drops.
    uint64_t PreparedObjects() const { return prepared_objects_; }

   private:
    using Key = StandbyMetadataStore::ObjectKey;
    ErrorCode UpdateSegments(const StandbySegmentRegistry& registry,
                             std::set<Key>& changed);
    void RemoveObject(const Key& key);
    ErrorCode PutObject(const Key& key, const StandbyObjectMetadata& metadata);
    ErrorCode RebuildNoF(const std::string& endpoint);
    ErrorCode Finish(StandbyMetadataStore& metadata, DurablePrefix cursor);
    ErrorCode PersistRepairs();

    std::shared_ptr<MasterService> service_;
    std::map<std::string, StandbySegmentInfo> segments_;
    std::unordered_map<std::string, std::string> aliases_;
    std::unordered_map<std::string, std::set<Key>> members_;
    std::map<Key, std::set<std::string>> object_endpoints_;
    std::map<UUID, size_t> owner_refs_;
    std::unordered_map<std::string, std::map<uintptr_t, uint64_t>> ranges_;
    std::unordered_map<std::string, uint64_t> used_bytes_;
    std::map<std::string, NoFSegmentOp> mounted_nof_;
    std::set<std::string> dirty_nof_;
    std::set<Key> repairs_;
    uint64_t segment_revision_{UINT64_MAX};
    uint64_t weight_revision_{UINT64_MAX};
    uint64_t prepared_objects_{0};
    DurablePrefix cursor_;
    bool ready_{false};
    bool bound_{false};
};
}  // namespace mooncake
