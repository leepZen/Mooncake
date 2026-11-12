#include <gflags/gflags.h>
#include <glog/logging.h>

#include <chrono>
#include <fstream>
#include <iostream>
#include <limits>
#include <string>

#include "ha/snapshot/batch_oplog/promotion.h"
#include "ha/prepared_standby_state.h"
#include "master_service.h"

DEFINE_uint64(objects, 100000, "Synthetic standby object count");
DEFINE_uint64(chunk_objects, 1000, "Objects drained per chunk");
DEFINE_bool(prepared, true,
            "Measure prepared runtime instead of legacy restore");
DEFINE_uint64(tail_objects, 1, "Objects replaced after initial preparation");
DEFINE_bool(memory_remount, false,
            "Measure remount of a prepared memory segment");

namespace {

size_t PeakRssBytes() {
    std::ifstream status("/proc/self/status");
    std::string name;
    size_t rss_kib = 0;
    while (status >> name) {
        if (name == "VmHWM:") {
            status >> rss_kib;
            break;
        }
        status.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    return rss_kib * 1024;
}

}  // namespace

int main(int argc, char** argv) {
    google::InitGoogleLogging(argv[0]);
    FLAGS_logtostderr = true;
    gflags::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_objects == 0 || FLAGS_chunk_objects == 0 ||
        FLAGS_objects == std::numeric_limits<uint64_t>::max() ||
        FLAGS_chunk_objects > std::numeric_limits<size_t>::max() ||
        FLAGS_tail_objects > FLAGS_objects ||
        (FLAGS_memory_remount &&
         (!FLAGS_prepared ||
          FLAGS_objects >
              (std::numeric_limits<uintptr_t>::max() - 0x100000000ULL) / 64))) {
        std::cerr << "objects and chunk_objects must be non-zero\n";
        return 1;
    }

    mooncake::Segment memory_segment;
    memory_segment.id = {1, 2};
    memory_segment.name = memory_segment.te_endpoint = "bench-memory";
    memory_segment.base = 0x100000000ULL;
    memory_segment.size = FLAGS_objects * 64;
    memory_segment.protocol = "tcp";
    auto source_store = std::make_unique<mooncake::StandbyMetadataStore>();
    for (uint64_t i = 0; i < FLAGS_objects; ++i) {
        mooncake::StandbyObjectMetadata metadata;
        metadata.size = FLAGS_memory_remount ? 64 : 1;
        mooncake::Replica::Descriptor replica;
        replica.id = i + 1;
        replica.status = mooncake::ReplicaStatus::COMPLETE;
        if (FLAGS_memory_remount) {
            mooncake::MemoryDescriptor descriptor;
            descriptor.buffer_descriptor = {64, memory_segment.base + i * 64,
                                            "tcp", memory_segment.te_endpoint};
            descriptor.segment_id = memory_segment.id;
            replica.descriptor_variant = std::move(descriptor);
        } else {
            replica.descriptor_variant = mooncake::DiskDescriptor{"bench", 1};
        }
        metadata.replicas.push_back(std::move(replica));
        if (!source_store->PutMetadata("default", "bench-" + std::to_string(i),
                                       metadata)) {
            return 1;
        }
    }

    if (FLAGS_prepared) {
        using Clock = std::chrono::steady_clock;
        const auto micros = [](Clock::time_point from, Clock::time_point to) {
            return std::chrono::duration_cast<std::chrono::microseconds>(to -
                                                                         from)
                .count();
        };
        auto config = mooncake::MasterServiceConfig::builder()
                          .set_enable_ha(false)
                          .build();
        mooncake::PreparedStandbyState prepared(config);
        mooncake::StandbySegmentRegistry segments;
        if (FLAGS_memory_remount) {
            mooncake::StandbySegmentInfo info;
            info.segment_name = memory_segment.name;
            info.transport_endpoint = memory_segment.te_endpoint;
            info.capacity = memory_segment.size;
            info.is_memory_segment = true;
            info.memory =
                mooncake::MemorySegmentOp{memory_segment, mooncake::UUID(3, 4)};
            segments.OnSegmentMount(info);
        }
        const auto start = Clock::now();
        auto error = prepared.Bootstrap(*source_store, segments, {1, 1},
                                        FLAGS_chunk_objects);
        const auto bootstrapped = Clock::now();
        if (error != mooncake::ErrorCode::OK) return 1;
        for (uint64_t i = 0; i < FLAGS_tail_objects; ++i) {
            const auto key = "bench-" + std::to_string(i);
            auto metadata = source_store->GetMetadata("default", key);
            metadata->session_id = "updated";
            source_store->PutMetadata("default", key, *metadata);
        }
        const auto tail_start = Clock::now();
        error = prepared.Synchronize(*source_store, segments,
                                     {2, FLAGS_tail_objects + 1});
        const auto synchronized = Clock::now();
        if (error != mooncake::ErrorCode::OK) return 1;
        config.view_version = 1;
        error = prepared.BindTerm(config);
        const auto bound = Clock::now();
        if (error != mooncake::ErrorCode::OK) return 1;
        prepared.Service()->ActivatePreparedRuntime();
        const auto activated = Clock::now();
        int64_t remount_us = 0;
        if (FLAGS_memory_remount) {
            const auto before_remount = Clock::now();
            if (!prepared.Service()->ReMountSegment({memory_segment},
                                                    mooncake::UUID(3, 4)))
                return 1;
            remount_us = micros(before_remount, Clock::now());
            if (!prepared.Service()->GetReplicaList(
                    "bench-0", mooncake::TenantId::Default()))
                return 1;
        }
        std::cout << "objects=" << FLAGS_objects
                  << " tail_objects=" << FLAGS_tail_objects
                  << " prepare_us=" << micros(start, bootstrapped)
                  << " tail_us=" << micros(tail_start, synchronized)
                  << " bind_us=" << micros(synchronized, bound)
                  << " activate_us=" << micros(bound, activated)
                  << " memory_remount_us=" << remount_us
                  << " reconstructed_objects=" << prepared.PreparedObjects()
                  << " peak_rss_bytes=" << PeakRssBytes() << '\n';
        return 0;
    }

    mooncake::BatchOpLogPromotionHandoff handoff;
    handoff.metadata_store = std::move(source_store);
    handoff.applied_cursor = {.batch_id = 1, .last_seq = 1};
    handoff.max_replica_id = FLAGS_objects;
    mooncake::MasterService primary(
        mooncake::MasterServiceConfig::builder().set_enable_ha(false).build());

    const auto started = std::chrono::steady_clock::now();
    auto restored = primary.RestoreFromBatchOpLogPromotion(
        std::move(handoff), static_cast<size_t>(FLAGS_chunk_objects));
    if (!restored) {
        std::cerr << "restore failed: " << static_cast<int>(restored.error())
                  << '\n';
        return 1;
    }
    const auto elapsed = std::chrono::duration_cast<std::chrono::microseconds>(
        std::chrono::steady_clock::now() - started);

    std::cout << "objects=" << FLAGS_objects
              << " source_store=StandbyMetadataStore"
              << " chunk_objects=" << FLAGS_chunk_objects
              << " restore_us=" << elapsed.count()
              << " peak_rss_bytes=" << PeakRssBytes() << '\n';
    return 0;
}
