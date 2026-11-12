#include "metadata_store.h"
#include <algorithm>

namespace mooncake {

void StandbySegmentRegistry::OnSegmentMount(const StandbySegmentInfo& info) {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    if (info.memory) {
        memory_segments_[info.memory->segment.id] = info;
        auto legacy = segments_by_endpoint_.find(info.transport_endpoint);
        if (legacy != segments_by_endpoint_.end() &&
            legacy->second.is_memory_segment)
            segments_by_endpoint_.erase(legacy);
    } else {
        segments_by_endpoint_[info.transport_endpoint] = info;
    }
    ++revision_;
}

void StandbySegmentRegistry::OnSegmentUnmount(
    const std::string& transport_endpoint,
    std::optional<UUID> memory_segment_id) {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    if (memory_segment_id) {
        auto found = memory_segments_.find(*memory_segment_id);
        if (found != memory_segments_.end() &&
            found->second.transport_endpoint == transport_endpoint)
            memory_segments_.erase(found);
        auto legacy = segments_by_endpoint_.find(transport_endpoint);
        if (legacy != segments_by_endpoint_.end() &&
            legacy->second.is_memory_segment)
            segments_by_endpoint_.erase(legacy);
    } else {
        segments_by_endpoint_.erase(transport_endpoint);
        std::erase_if(memory_segments_, [&](const auto& entry) {
            return entry.second.transport_endpoint == transport_endpoint;
        });
    }
    ++revision_;
}

void StandbySegmentRegistry::OnSegmentUpdate(const StandbySegmentInfo& info) {
    OnSegmentMount(info);
}

bool StandbySegmentRegistry::HasSegment(
    const std::string& transport_endpoint) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return segments_by_endpoint_.contains(transport_endpoint) ||
           std::any_of(memory_segments_.begin(), memory_segments_.end(),
                       [&](const auto& entry) {
                           return entry.second.transport_endpoint ==
                                  transport_endpoint;
                       });
}

std::optional<StandbySegmentInfo> StandbySegmentRegistry::GetSegment(
    const std::string& transport_endpoint) const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    auto it = segments_by_endpoint_.find(transport_endpoint);
    if (it == segments_by_endpoint_.end()) {
        for (const auto& [id, info] : memory_segments_)
            if (info.transport_endpoint == transport_endpoint) return info;
        return std::nullopt;
    }
    return it->second;
}

std::vector<StandbySegmentInfo> StandbySegmentRegistry::GetAllSegments() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    std::vector<StandbySegmentInfo> result;
    result.reserve(segments_by_endpoint_.size());
    for (const auto& [endpoint, info] : segments_by_endpoint_) {
        result.push_back(info);
    }
    for (const auto& [id, info] : memory_segments_) result.push_back(info);
    return result;
}

void StandbySegmentRegistry::Clear() {
    std::lock_guard<std::shared_mutex> lock(mutex_);
    segments_by_endpoint_.clear();
    memory_segments_.clear();
    ++revision_;
}

uint64_t StandbySegmentRegistry::Revision() const {
    std::shared_lock<std::shared_mutex> lock(mutex_);
    return revision_;
}

}  // namespace mooncake
