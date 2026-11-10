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
        FLAGS_tail_objects > FLAGS_objects) {
        std::cerr << "objects and chunk_objects must be non-zero\n";
        return 1;
    }

    auto source_store = std::make_unique<mooncake::StandbyMetadataStore>();
    for (uint64_t i = 0; i < FLAGS_objects; ++i) {
        mooncake::StandbyObjectMetadata metadata;
        metadata.size = 1;
        mooncake::Replica::Descriptor replica;
        replica.id = i + 1;
        replica.status = mooncake::ReplicaStatus::COMPLETE;
        replica.descriptor_variant = mooncake::DiskDescriptor{"bench", 1};
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
        std::cout << "objects=" << FLAGS_objects
                  << " tail_objects=" << FLAGS_tail_objects
                  << " prepare_us=" << micros(start, bootstrapped)
                  << " tail_us=" << micros(tail_start, synchronized)
                  << " bind_us=" << micros(synchronized, bound)
                  << " activate_us=" << micros(bound, activated)
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
