/*
 * Copyright (C) 2014-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.0
 */

#include <algorithm>

#include <fmt/ranges.h>
#include <fmt/std.h>
#include "locator/network_topology_strategy.hh"
#include "locator/tablets.hh"
#include "locator/token_metadata_fwd.hh"
#include "utils/log.hh"
#include "replica/database_fwd.hh"
#include "utils/assert.hh"
#include "utils/lister.hh"
#include "replica/database.hh"
#include <seastar/core/future-util.hh>
#include "db/system_keyspace.hh"
#include "db/system_keyspace_sstables_registry.hh"
#include "db/system_distributed_keyspace.hh"
#include "db/commitlog/commitlog.hh"
#include "db/config.hh"
#include "db/extensions.hh"
#include "cql3/functions/functions.hh"
#include "cql3/functions/user_function.hh"
#include "cql3/functions/user_aggregate.hh"
#include <seastar/core/seastar.hh>
#include <seastar/core/coroutine.hh>
#include <seastar/coroutine/parallel_for_each.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/core/reactor.hh>
#include <seastar/core/metrics.hh>
#include "sstables/sstables.hh"
#include "sstables/sstables_manager.hh"
#include <boost/container/static_vector.hpp>
#include "mutation/frozen_mutation.hh"
#include "mutation/async_utils.hh"
#include <seastar/core/do_with.hh>
#include "service/migration_listener.hh"
#include "cell_locking.hh"
#include "view_info.hh"
#include "db/schema_tables.hh"
#include "compaction/compaction_manager.hh"
#include "gms/feature_service.hh"
#include "timeout_config.hh"
#include "service/storage_proxy.hh"
#include "db/operation_type.hh"
#include "db/view/view_update_generator.hh"
#include "multishard_mutation_query.hh"

#include "utils/human_readable.hh"
#include "utils/error_injection.hh"

#include "db/timeout_clock.hh"
#include "db/large_data_handler.hh"
#include "db/corrupt_data_handler.hh"
#include "db/data_listeners.hh"

#include "data_dictionary/user_types_metadata.hh"
#include <seastar/core/shared_ptr_incomplete.hh>
#include <seastar/coroutine/as_future.hh>
#include <seastar/util/memory_diagnostics.hh>
#include <seastar/util/file.hh>

#include "locator/abstract_replication_strategy.hh"
#include "timeout_config.hh"
#include "tombstone_gc.hh"
#include "service/qos/service_level_controller.hh"

#include "replica/data_dictionary_impl.hh"
#include "replica/global_table_ptr.hh"
#include "replica/exceptions.hh"
#include "readers/multi_range.hh"
#include "readers/multishard.hh"
#include "utils/labels.hh"

#include <algorithm>

using namespace std::chrono_literals;
using namespace db;

logging::logger dblog("database");

namespace replica {

// Used for tests where the CF exists without a database object. We need to pass a valid
// dirty_memory manager in that case.
thread_local dirty_memory_manager default_dirty_memory_manager;

inline
flush_controller
make_flush_controller(const db::config& cfg, backlog_controller::scheduling_group& sg, std::function<double()> fn) {
    return flush_controller(sg, cfg.memtable_flush_static_shares(), 50ms, cfg.unspooled_dirty_soft_limit(), std::move(fn));
}

keyspace::keyspace(lw_shared_ptr<keyspace_metadata> metadata, config cfg, locator::effective_replication_map_factory& erm_factory)
    : _metadata(std::move(metadata))
    , _config(std::move(cfg))
    , _erm_factory(erm_factory)
{}

future<> keyspace::shutdown() noexcept {
    update_effective_replication_map({});
    return make_ready_future<>();
}

lw_shared_ptr<keyspace_metadata> keyspace::metadata() const {
    return _metadata;
}

void keyspace::add_or_update_column_family(const schema_ptr& s) {
    _metadata->add_or_update_column_family(s);
}

void keyspace::add_user_type(const user_type ut) {
    _metadata->add_user_type(ut);
}

void keyspace::remove_user_type(const user_type ut) {
    _metadata->remove_user_type(ut);
}

bool string_pair_eq::operator()(spair lhs, spair rhs) const {
    return lhs == rhs;
}

table_schema_version database::empty_version = table_schema_version(utils::UUID_gen::get_name_UUID(bytes{}));

namespace {

class memory_diagnostics_line_writer {
    std::array<char, 4096> _line_buf;
    memory::memory_diagnostics_writer _wr;

public:
    memory_diagnostics_line_writer(memory::memory_diagnostics_writer wr) : _wr(std::move(wr)) { }
    void operator() (const char* fmt) {
        _wr(fmt);
    }
    void operator() (const char* fmt, const auto& param1, const auto&... params) {
        const auto begin = _line_buf.begin();
        auto it = fmt::format_to(begin, fmt::runtime(fmt), param1, params...);
        _wr(std::string_view(begin, it - begin));
    }
};

const boost::container::static_vector<std::pair<size_t, boost::container::static_vector<table*, 16>>, 10>
phased_barrier_top_10_counts(const database::tables_metadata& tables_metadata, std::function<size_t(table&)> op_count_getter) {
    using table_list = boost::container::static_vector<table*, 16>;
    using count_and_tables = std::pair<size_t, table_list>;
    const auto less = [] (const count_and_tables& a, const count_and_tables& b) {
        return a.first < b.first;
    };

    boost::container::static_vector<count_and_tables, 10> res;
    count_and_tables* min_element = nullptr;

    tables_metadata.for_each_table([&] (table_id tid, lw_shared_ptr<table> table) {
        const auto count = op_count_getter(*table);
        if (!count) {
            return;
        }
        if (res.size() < res.capacity()) {
            auto& elem = res.emplace_back(count, table_list({table.get()}));
            if (!min_element || min_element->first > count) {
                min_element = &elem;
            }
            return;
        }
        if (min_element->first > count) {
            return;
        }

        auto it = std::ranges::find_if(res, [count] (const count_and_tables& x) {
            return x.first == count;
        });
        if (it != res.end()) {
            it->second.push_back(table.get());
            return;
        }

        // If we are here, min_element->first < count
        *min_element = {count, table_list({table.get()})};
        min_element = &*std::ranges::min_element(res, less);
    });

    std::ranges::sort(res, less);

    return res;
}

} // anonymous namespace

void database::setup_scylla_memory_diagnostics_producer() {
    memory::set_additional_diagnostics_producer([this] (memory::memory_diagnostics_writer wr) {
        auto writeln = memory_diagnostics_line_writer(std::move(wr));

        const auto lsa_occupancy_stats = logalloc::shard_tracker().global_occupancy();
        writeln("LSA\n");
        writeln("  allocated: {}\n", utils::to_hr_size(lsa_occupancy_stats.total_space()));
        writeln("  used:      {}\n", utils::to_hr_size(lsa_occupancy_stats.used_space()));
        writeln("  free:      {}\n\n", utils::to_hr_size(lsa_occupancy_stats.free_space()));

        const auto row_cache_occupancy_stats = _row_cache_tracker.region().occupancy();
        writeln("Cache:\n");
        writeln("  total: {}\n", utils::to_hr_size(row_cache_occupancy_stats.total_space()));
        writeln("  used:  {}\n", utils::to_hr_size(row_cache_occupancy_stats.used_space()));
        writeln("  free:  {}\n\n", utils::to_hr_size(row_cache_occupancy_stats.free_space()));

        writeln("Memtables:\n");
        writeln(" total: {}\n", utils::to_hr_size(lsa_occupancy_stats.total_space() - row_cache_occupancy_stats.total_space()));

        writeln(" Regular:\n");
        writeln("  real dirty: {}\n", utils::to_hr_size(_dirty_memory_manager.real_dirty_memory()));
        writeln("  virt dirty: {}\n", utils::to_hr_size(_dirty_memory_manager.unspooled_dirty_memory()));
        writeln(" System:\n");
        writeln("  real dirty: {}\n", utils::to_hr_size(_system_dirty_memory_manager.real_dirty_memory()));
        writeln("  virt dirty: {}\n\n", utils::to_hr_size(_system_dirty_memory_manager.unspooled_dirty_memory()));

        writeln("Replica:\n");

        writeln("  Read Concurrency Semaphores:\n");

        auto semaphore_dump = [&writeln] (const sstring& name, const reader_concurrency_semaphore& sem) {
            const auto initial_res = sem.initial_resources();
            const auto available_res = sem.available_resources();
            if (sem.is_unlimited()) {
                writeln("    {}: {}/unlimited, {}/unlimited\n",
                        name,
                        initial_res.count - available_res.count,
                        utils::to_hr_size(initial_res.memory - available_res.memory),
                        sem.get_stats().waiters);
            } else {
                writeln("    {}: {}/{}, {}/{}, queued: {}\n",
                        name,
                        initial_res.count - available_res.count,
                        initial_res.count,
                        utils::to_hr_size(initial_res.memory - available_res.memory),
                        utils::to_hr_size(initial_res.memory),
                        sem.get_stats().waiters);
            }
        };

        semaphore_dump("streaming", _streaming_concurrency_sem);
        semaphore_dump("system", _system_read_concurrency_sem);
        semaphore_dump("compaction", _compaction_concurrency_sem);
        _reader_concurrency_semaphores_group.foreach_semaphore([&semaphore_dump] (scheduling_group sg, reader_concurrency_semaphore& sem) {
             semaphore_dump(sg.name(), sem);
        });
        _view_update_read_concurrency_semaphores_group.foreach_semaphore([&semaphore_dump] (scheduling_group sg, reader_concurrency_semaphore& sem) {
             semaphore_dump(sg.name(), sem);
        });

        writeln("  Execution Stages:\n");
        const std::pair<const char*, inheriting_execution_stage::stats> execution_stage_summaries[] = {
                {"apply stage", _apply_stage.get_stats()},
        };
        for (const auto& [name, exec_stage_summary] : execution_stage_summaries) {
            writeln("    {}:\n", name);
            size_t total = 0;
            for (const auto& [sg, stats ] : exec_stage_summary) {
                const auto count = stats.function_calls_enqueued - stats.function_calls_executed;
                if (!count) {
                    continue;
                }
                writeln("      {}\t{}\n", sg.name(), count);
                total += count;
            }
            writeln("         Total: {}\n", total);
        }

        writeln("  Tables - Ongoing Operations:\n");
        const std::pair<const char*, std::function<size_t(table&)>> phased_barriers[] = {
                {"Pending writes", std::mem_fn(&table::writes_in_progress)},
                {"Pending reads", std::mem_fn(&table::reads_in_progress)},
                {"Pending streams", std::mem_fn(&table::streams_in_progress)},
        };
        for (const auto& [name, op_count_getter] : phased_barriers) {
            writeln("    {} (top 10):\n", name);
            auto total = 0;
            for (const auto& [count, table_list] : phased_barrier_top_10_counts(_tables_metadata, op_count_getter)) {
                total += count;
                writeln("      {}", count);
                if (table_list.empty()) {
                    writeln("\n");
                    continue;
                }
                auto it = table_list.begin();
                for (; it != table_list.end() - 1; ++it) {
                    writeln(" {}.{},", (*it)->schema()->ks_name(), (*it)->schema()->cf_name());
                }
                writeln(" {}.{}\n", (*it)->schema()->ks_name(), (*it)->schema()->cf_name());
            }
            writeln("      {} Total (all)\n", total);
        }
        writeln("\n");
    });
}

class db_user_types_storage : public data_dictionary::dummy_user_types_storage {
    const replica::database* _db = nullptr;
public:
    db_user_types_storage(const database& db) noexcept : _db(&db) {}

    virtual const user_types_metadata& get(const sstring& ks) const override {
        if (_db == nullptr) {
            return dummy_user_types_storage::get(ks);
        }

        return _db->find_keyspace(ks).metadata()->user_types();
    }

    void deactivate() noexcept {
        _db = nullptr;
    }
};

reader_concurrency_semaphore&
database::read_concurrency_sem() {
    reader_concurrency_semaphore* sem = _reader_concurrency_semaphores_group.get_or_null(current_scheduling_group());
    if (!sem) {
        // this line is commented out, however we shouldn't get here because it means that a user query or even worse,
        // some random query was triggered from an unanticipated scheduling groups and this violates the isolation we are trying to achieve.
        // It is commented out for two reasons:
        // 1. So we will be able to ease into this new system, first testing functionality and effect and only then mix in exceptions and asserts.
        // 2. So the series containing those changes will be backportable without causing too harsh regressions (aborts) on one hand and without forcing
        //    extensive changes on the other hand.
        // Follow Up: uncomment this line and run extensive testing. Handle every case of abort.
        // seastar::on_internal_error(dblog, format("Tried to run a user query in a wrong scheduling group (scheduling group: '{}')", current_scheduling_group().name()));
        sem = _reader_concurrency_semaphores_group.get_or_null(_default_read_concurrency_group);
        if (!sem) {
            // If we got here - the initialization went very wrong and we can't do anything about it.
            // This can only happen if someone touched the initialization code which is assumed to initialize at least
            // this default semaphore.
            seastar::on_internal_error(dblog, "Default read concurrency semaphore wasn't found, something probably went wrong during database::start");
        }
    }
    return *sem;
}

// With same concerns as read_concurrency_sem().
reader_concurrency_semaphore&
database::view_update_read_concurrency_sem() {
    reader_concurrency_semaphore* sem = _view_update_read_concurrency_semaphores_group.get_or_null(current_scheduling_group());
    if (!sem) {
        sem = _view_update_read_concurrency_semaphores_group.get_or_null(_default_read_concurrency_group);
        if (!sem) {
            seastar::on_internal_error(dblog, "Default view update read concurrency semaphore wasn't found, something probably went wrong during database::start");
        }
    }
    return *sem;
}

database::database(const db::config& cfg, database_config dbcfg, service::migration_notifier& mn, gms::feature_service& feat, locator::shared_token_metadata& stm,
        compaction_manager& cm, sstables::storage_manager& sstm, lang::manager& langm, sstables::directory_semaphore& sst_dir_sem, sstable_compressor_factory& scf, const abort_source& abort, utils::cross_shard_barrier barrier)
    : _stats(make_lw_shared<db_stats>())
    , _user_types(std::make_shared<db_user_types_storage>(*this))
    , _cl_stats(std::make_unique<cell_locker_stats>())
    , _cfg(cfg)
    // Allow system tables a pool of 10 MB memory to write, but never block on other regions.
    , _system_dirty_memory_manager(*this, 10 << 20, cfg.unspooled_dirty_soft_limit(), default_scheduling_group())
    , _dirty_memory_manager(*this, dbcfg.available_memory * 0.50, cfg.unspooled_dirty_soft_limit(), dbcfg.statement_scheduling_group)
    , _dbcfg(dbcfg)
    , _flush_sg(dbcfg.memtable_scheduling_group)
    , _memtable_controller(make_flush_controller(_cfg, _flush_sg, [this, limit = float(_dirty_memory_manager.throttle_threshold())] {
        auto backlog = (_dirty_memory_manager.unspooled_dirty_memory()) / limit;
        if (_dirty_memory_manager.has_extraneous_flushes_requested()) {
            backlog = std::max(backlog, _memtable_controller.backlog_of_shares(200));
        }
        return backlog;
    }))
    // No timeouts or queue length limits - a failure here can kill an entire repair.
    // Trust the caller to limit concurrency.
    , _streaming_concurrency_sem(
            _cfg.maintenance_reader_concurrency_semaphore_count_limit,
            max_memory_streaming_concurrent_reads(),
            "streaming",
            std::numeric_limits<size_t>::max(),
            utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value(uint32_t(1)),
            reader_concurrency_semaphore::register_metrics::yes)
    // No limits, just for accounting.
    , _compaction_concurrency_sem(reader_concurrency_semaphore::no_limits{}, "compaction", reader_concurrency_semaphore::register_metrics::no)
    , _system_read_concurrency_sem(
            // Using higher initial concurrency, see revert_initial_system_read_concurrency_boost().
            max_count_concurrent_reads,
            max_memory_system_concurrent_reads(),
            "system",
            std::numeric_limits<size_t>::max(),
            utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            utils::updateable_value(std::numeric_limits<uint32_t>::max()),
            reader_concurrency_semaphore::register_metrics::yes)
    , _view_update_read_concurrency_semaphores_group(
            max_memory_concurrent_view_update_reads(),
            utils::updateable_value<int>(max_count_concurrent_view_update_reads),
            std::numeric_limits<size_t>::max(),
            _cfg.view_update_reader_concurrency_semaphore_serialize_limit_multiplier,
            _cfg.view_update_reader_concurrency_semaphore_kill_limit_multiplier,
            _cfg.view_update_reader_concurrency_semaphore_cpu_concurrency,
            "view_update")
    , _row_cache_tracker(_cfg.index_cache_fraction.operator utils::updateable_value<double>(), cache_tracker::register_metrics::yes)
    , _apply_stage("db_apply", &database::do_apply)
    , _version(empty_version)
    , _compaction_manager(cm)
    , _enable_incremental_backups(cfg.incremental_backups())
    , _querier_cache([this] (const reader_concurrency_semaphore& s) {
        return this->is_user_semaphore(s);
    })
    , _large_data_handler(std::make_unique<db::cql_table_large_data_handler>(feat,
              _cfg.compaction_large_partition_warning_threshold_mb,
              _cfg.compaction_large_row_warning_threshold_mb,
              _cfg.compaction_large_cell_warning_threshold_mb,
              _cfg.compaction_rows_count_warning_threshold,
              _cfg.compaction_collection_elements_count_warning_threshold))
    , _nop_large_data_handler(std::make_unique<db::nop_large_data_handler>())
    , _corrupt_data_handler(std::make_unique<db::system_table_corrupt_data_handler>(db::system_table_corrupt_data_handler::config{.entry_ttl = std::chrono::days(10)}, db::corrupt_data_handler::register_metrics::yes))
    , _nop_corrupt_data_handler(std::make_unique<db::nop_corrupt_data_handler>(db::corrupt_data_handler::register_metrics::no))
    , _user_sstables_manager(std::make_unique<sstables::sstables_manager>("user", *_large_data_handler, *_corrupt_data_handler, _cfg, feat, _row_cache_tracker, dbcfg.available_memory, sst_dir_sem, [&stm]{ return stm.get()->get_my_id(); }, scf, abort, dbcfg.streaming_scheduling_group, &sstm))
    , _system_sstables_manager(std::make_unique<sstables::sstables_manager>("system", *_nop_large_data_handler, *_nop_corrupt_data_handler, _cfg, feat, _row_cache_tracker, dbcfg.available_memory, sst_dir_sem, [&stm]{ return stm.get()->get_my_id(); }, scf, abort, dbcfg.streaming_scheduling_group))
    , _result_memory_limiter(dbcfg.available_memory / 10)
    , _data_listeners(std::make_unique<db::data_listeners>())
    , _mnotifier(mn)
    , _feat(feat)
    , _shared_token_metadata(stm)
    , _lang_manager(langm)
    , _reader_concurrency_semaphores_group(max_memory_concurrent_reads(), max_count_concurrent_reads, max_inactive_queue_length(),
        _cfg.reader_concurrency_semaphore_serialize_limit_multiplier,
        _cfg.reader_concurrency_semaphore_kill_limit_multiplier,
        _cfg.reader_concurrency_semaphore_cpu_concurrency)
    , _stop_barrier(std::move(barrier))
    , _update_memtable_flush_static_shares_action([this, &cfg] { return _memtable_controller.update_static_shares(cfg.memtable_flush_static_shares()); })
    , _memtable_flush_static_shares_observer(cfg.memtable_flush_static_shares.observe(_update_memtable_flush_static_shares_action.make_observer()))
{
    SCYLLA_ASSERT(dbcfg.available_memory != 0); // Detect misconfigured unit tests, see #7544

    local_schema_registry().init(*this); // TODO: we're never unbound.
    setup_metrics();

    _row_cache_tracker.set_compaction_scheduling_group(dbcfg.memory_compaction_scheduling_group);

    setup_scylla_memory_diagnostics_producer();
    if (_dbcfg.sstables_format) {
        set_format(*_dbcfg.sstables_format);
    }
}

const db::extensions& database::extensions() const {
    return get_config().extensions();
}

std::shared_ptr<data_dictionary::user_types_storage> database::as_user_types_storage() const noexcept {
    return _user_types;
}

const data_dictionary::user_types_storage& database::user_types() const noexcept {
    return *_user_types;
}

locator::vnode_effective_replication_map_ptr keyspace::get_vnode_effective_replication_map() const {
    // FIXME: Examine all users.
    if (get_replication_strategy().is_per_table()) {
        on_internal_error(dblog, format("Tried to obtain per-keyspace effective replication map of {} but it's per-table", _metadata->name()));
    }
    return _effective_replication_map;
}

} // namespace replica

void backlog_controller::adjust() {
    if (controller_disabled()) {
        update_controller(_static_shares);
        return;
    }

    auto backlog = _current_backlog();

    if (backlog >= _control_points.back().input) {
        update_controller(_control_points.back().output);
        return;
    }

    // interpolate to find out which region we are. This run infrequently and there are a fixed
    // number of points so a simple loop will do.
    size_t idx = 1;
    while ((idx < _control_points.size() - 1) && (_control_points[idx].input < backlog)) {
        idx++;
    }

    control_point& cp = _control_points[idx];
    control_point& last = _control_points[idx - 1];
    float result = last.output + (backlog - last.input) * (cp.output - last.output)/(cp.input - last.input);
    update_controller(result);
}

float backlog_controller::backlog_of_shares(float shares) const {
    size_t idx = 1;
    if (controller_disabled() || _control_points.size() == 0) {
            return 1.0f;
    }
    while ((idx < _control_points.size() - 1) && (_control_points[idx].output < shares)) {
        idx++;
    }
    const control_point& cp = _control_points[idx];
    const control_point& last = _control_points[idx - 1];
    // Compute the inverse function of the backlog in the interpolation interval that we fall
    // into.
    //
    // The formula for the backlog inside an interpolation point is y = a + bx, so the inverse
    // function is x = (y - a) / b

    return last.input + (shares - last.output) * (cp.input - last.input) / (cp.output - last.output);
}

void backlog_controller::update_controller(float shares) {
    _scheduling_group.set_shares(shares);
}


namespace replica {

static const metrics::label class_label("class");


auto
database::sum_read_concurrency_sem_stat(std::invocable<reader_concurrency_semaphore::stats&> auto stats_member) {
    return _reader_concurrency_semaphores_group.sum_read_concurrency_sem_var([&] (reader_concurrency_semaphore& rcs) { return std::invoke(stats_member, rcs.get_stats()); });
}

void
database::setup_metrics() {
    _dirty_memory_manager.setup_collectd("regular");
    _system_dirty_memory_manager.setup_collectd("system");

    namespace sm = seastar::metrics;

    _metrics.add_group("memory", {
        sm::make_gauge("dirty_bytes", [this] { return _dirty_memory_manager.real_dirty_memory() + _system_dirty_memory_manager.real_dirty_memory(); },
                       sm::description("Holds the current size of all (\"regular\" and \"system\") non-free memory in bytes: used memory + released memory that hasn't been returned to a free memory pool yet. "
                                       "Total memory size minus this value represents the amount of available memory. "
                                       "If this value minus unspooled_dirty_bytes is too high then this means that the dirty memory eviction lags behind.")),

        sm::make_gauge("unspooled_dirty_bytes", [this] { return _dirty_memory_manager.unspooled_dirty_memory() + _system_dirty_memory_manager.unspooled_dirty_memory(); },
                       sm::description("Holds the size of all (\"regular\" and \"system\") used memory in bytes. Compare it to \"dirty_bytes\" to see how many memory is wasted (neither used nor available).")),
    });

    _metrics.add_group("memtables", {
        sm::make_gauge("pending_flushes", _cf_stats.pending_memtables_flushes_count,
                       sm::description("Holds the current number of memtables that are currently being flushed to sstables. "
                                       "High value in this metric may be an indication of storage being a bottleneck.")),

        sm::make_gauge("pending_flushes_bytes", _cf_stats.pending_memtables_flushes_bytes,
                       sm::description("Holds the current number of bytes in memtables that are currently being flushed to sstables. "
                                       "High value in this metric may be an indication of storage being a bottleneck.")),
        sm::make_gauge("failed_flushes", _cf_stats.failed_memtables_flushes_count,
                       sm::description("Holds the number of failed memtable flushes. "
                                       "High value in this metric may indicate a permanent failure to flush a memtable.")),
    });

    _metrics.add_group("database", {
        sm::make_gauge("requests_blocked_memory_current", [this] { return _dirty_memory_manager.region_group().blocked_requests(); },
                       sm::description(
                           seastar::format("Holds the current number of requests blocked due to reaching the memory quota ({}B). "
                                           "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"database\" component.", _dirty_memory_manager.throttle_threshold())))(basic_level),

        sm::make_counter("requests_blocked_memory", [this] { return _dirty_memory_manager.region_group().blocked_requests_counter(); },
                       sm::description(seastar::format("Holds the current number of requests blocked due to reaching the memory quota ({}B). "
                                       "Non-zero value indicates that our bottleneck is memory and more specifically - the memory quota allocated for the \"database\" component.", _dirty_memory_manager.throttle_threshold())))(basic_level),

        sm::make_counter("clustering_filter_count", _cf_stats.clustering_filter_count,
                       sm::description("Counts bloom filter invocations.")),

        sm::make_counter("clustering_filter_sstables_checked", _cf_stats.sstables_checked_by_clustering_filter,
                       sm::description("Counts sstables checked after applying the bloom filter. "
                                       "High value indicates that bloom filter is not very efficient.")),

        sm::make_counter("clustering_filter_fast_path_count", _cf_stats.clustering_filter_fast_path_count,
                       sm::description("Counts number of times bloom filtering short cut to include all sstables when only one full range was specified.")),

        sm::make_counter("clustering_filter_surviving_sstables", _cf_stats.surviving_sstables_after_clustering_filter,
                       sm::description("Counts sstables that survived the clustering key filtering. "
                                       "High value indicates that bloom filter is not very efficient and still have to access a lot of sstables to get data.")),

        sm::make_counter("dropped_view_updates", _cf_stats.dropped_view_updates,
                       sm::description("Counts the number of view updates that have been dropped due to cluster overload. "))(basic_level),

       sm::make_counter("view_building_paused", _cf_stats.view_building_paused,
                      sm::description("Counts the number of times view building process was paused (e.g. due to node unavailability). ")),

        sm::make_counter("total_writes", _stats->total_writes,
                       sm::description("Counts the total number of successful write operations performed by this shard."))(basic_level),

        sm::make_counter("total_writes_failed", _stats->total_writes_failed,
                       sm::description("Counts the total number of failed write operations. "
                                       "A sum of this value plus total_writes represents a total amount of writes attempted on this shard."))(basic_level),

        sm::make_counter("total_writes_timedout", _stats->total_writes_timedout,
                       sm::description("Counts write operations failed due to a timeout. A positive value is a sign of storage being overloaded."))(basic_level),

        sm::make_counter("total_writes_rate_limited", _stats->total_writes_rate_limited,
                       sm::description("Counts write operations which were rejected on the replica side because the per-partition limit was reached."))(basic_level),

        sm::make_counter("total_reads_rate_limited", _stats->total_reads_rate_limited,
                       sm::description("Counts read operations which were rejected on the replica side because the per-partition limit was reached.")),

        sm::make_current_bytes("view_update_backlog", [this] { return get_view_update_backlog().get_current_bytes(); },
                       sm::description("Holds the current size in bytes of the pending view updates for all tables"))(basic_level),

        sm::make_counter("querier_cache_lookups", _querier_cache.get_stats().lookups,
                       sm::description("Counts querier cache lookups (paging queries)")),

        sm::make_counter("querier_cache_misses", _querier_cache.get_stats().misses,
                       sm::description("Counts querier cache lookups that failed to find a cached querier")),

        sm::make_counter("querier_cache_drops", _querier_cache.get_stats().drops,
                       sm::description("Counts querier cache lookups that found a cached querier but had to drop it")),

        sm::make_counter("querier_cache_scheduling_group_mismatches", _querier_cache.get_stats().scheduling_group_mismatches,
                       sm::description("Counts querier cache lookups that found a cached querier but had to drop it due to scheduling group mismatch")),

        sm::make_counter("querier_cache_time_based_evictions", _querier_cache.get_stats().time_based_evictions,
                       sm::description("Counts querier cache entries that timed out and were evicted.")),

        sm::make_counter("querier_cache_resource_based_evictions", _querier_cache.get_stats().resource_based_evictions,
                       sm::description("Counts querier cache entries that were evicted to free up resources "
                                       "(limited by reader concurrency limits) necessary to create new readers.")),

        sm::make_gauge("querier_cache_population", _querier_cache.get_stats().population,
                       sm::description("The number of entries currently in the querier cache.")),

    });

    // Registering all the metrics with a single call causes the stack size to blow up.
    _metrics.add_group("database", {
        sm::make_gauge("total_result_bytes", [this] { return get_result_memory_limiter().total_used_memory(); },
                       sm::description("Holds the current amount of memory used for results.")),

        sm::make_counter("short_data_queries", _stats->short_data_queries,
                       sm::description("The rate of data queries (data or digest reads) that returned less rows than requested due to result size limiting.")),

        sm::make_counter("short_mutation_queries", _stats->short_mutation_queries,
                       sm::description("The rate of mutation queries that returned less rows than requested due to result size limiting.")),

        sm::make_counter("multishard_query_unpopped_fragments", _stats->multishard_query_unpopped_fragments,
                       sm::description("The total number of fragments that were extracted from the shard reader but were unconsumed by the query and moved back into the reader.")),

        sm::make_counter("multishard_query_unpopped_bytes", _stats->multishard_query_unpopped_bytes,
                       sm::description("The total number of bytes that were extracted from the shard reader but were unconsumed by the query and moved back into the reader.")),

        sm::make_counter("multishard_query_failed_reader_stops", _stats->multishard_query_failed_reader_stops,
                       sm::description("The number of times the stopping of a shard reader failed.")),

        sm::make_counter("multishard_query_failed_reader_saves", _stats->multishard_query_failed_reader_saves,
                       sm::description("The number of times the saving of a shard reader failed.")),

        sm::make_total_operations("counter_cell_lock_acquisition", _cl_stats->lock_acquisitions,
                                 sm::description("The number of acquired counter cell locks.")),

        sm::make_queue_length("counter_cell_lock_pending", _cl_stats->operations_waiting_for_lock,
                             sm::description("The number of counter updates waiting for a lock.")),

        sm::make_counter("large_partition_exceeding_threshold", [this] { return _large_data_handler->stats().partitions_bigger_than_threshold; },
            sm::description("Number of large partitions exceeding compaction_large_partition_warning_threshold_mb. "
                "Large partitions have performance impact and should be avoided, check the documentation for details.")),

        sm::make_total_operations("total_view_updates_pushed_local", _cf_stats.total_view_updates_pushed_local,
                sm::description("Total number of view updates generated for tables and applied locally."))(basic_level),

        sm::make_total_operations("total_view_updates_pushed_remote", _cf_stats.total_view_updates_pushed_remote,
                sm::description("Total number of view updates generated for tables and sent to remote replicas."))(basic_level),

        sm::make_total_operations("total_view_updates_failed_local", _cf_stats.total_view_updates_failed_local,
                sm::description("Total number of view updates generated for tables and failed to be applied locally.")),

        sm::make_total_operations("total_view_updates_failed_remote", _cf_stats.total_view_updates_failed_remote,
                sm::description("Total number of view updates generated for tables and failed to be sent to remote replicas.")),

        sm::make_total_operations("total_view_updates_on_wrong_node", _cf_stats.total_view_updates_on_wrong_node,
                sm::description("Total number of view updates which are computed on the wrong node.")).set_skip_when_empty(),

        sm::make_total_operations("total_view_updates_failed_pairing", _cf_stats.total_view_updates_failed_pairing,
                sm::description("Total number of view updates for which we failed base/view pairing.")).set_skip_when_empty(),
    });
    if (this_shard_id() == 0) {
        _metrics.add_group("database", {
                sm::make_counter("schema_changed", _schema_change_count,
                        sm::description("The number of times the schema changed"))(basic_level),
        });
    }
}

void database::set_format(sstables::sstable_version_types format) noexcept {
    get_user_sstables_manager().set_format(format);
    get_system_sstables_manager().set_format(format);
}

database::~database() {
    _user_types->deactivate();
    local_schema_registry().clear();
}

void database::update_version(const table_schema_version& version) {
    if (_version.get() != version) {
        _schema_change_count++;
    }
    _version.set(version);
}

const table_schema_version& database::get_version() const {
    return _version.get();
}

static future<>
do_parse_schema_tables(distributed<service::storage_proxy>& proxy, const sstring cf_name, std::function<future<> (db::schema_tables::schema_result_value_type&)> func) {
    using namespace db::schema_tables;

    auto rs = co_await db::system_keyspace::query(proxy.local().get_db(), db::schema_tables::NAME, cf_name);
    auto names = std::set<sstring>();
    for (auto& r : rs->rows()) {
        auto keyspace_name = r.template get_nonnull<sstring>("keyspace_name");
        names.emplace(keyspace_name);
    }
    co_await coroutine::parallel_for_each(names, [&] (sstring name) mutable -> future<> {
        if (is_system_keyspace(name)) {
            co_return;
        }

        auto v = co_await read_schema_partition_for_keyspace(proxy, cf_name, name);
        try {
            co_await func(v);
        } catch (...) {
            dblog.error("Skipping: {}. Exception occurred when loading system table {}: {}", v.first, cf_name, std::current_exception());
        }
    });
}

future<> database::parse_system_tables(distributed<service::storage_proxy>& proxy, sharded<db::system_keyspace>& sys_ks) {
    using namespace db::schema_tables;
    co_await do_parse_schema_tables(proxy, db::schema_tables::KEYSPACES, coroutine::lambda([&] (schema_result_value_type &v) -> future<> {
        auto scylla_specific_rs = co_await extract_scylla_specific_keyspace_info(proxy, v);
        auto ksm = co_await create_keyspace_from_schema_partition(proxy, v, scylla_specific_rs);
        co_return co_await create_keyspace(ksm, proxy.local().get_erm_factory(), system_keyspace::no);
    }));
    co_await do_parse_schema_tables(proxy, db::schema_tables::TYPES, coroutine::lambda([&] (schema_result_value_type &v) -> future<> {
        auto& ks = this->find_keyspace(v.first);
        auto&& user_types = co_await create_types_from_schema_partition(*ks.metadata(), v.second);
        for (auto&& type : user_types) {
            ks.add_user_type(type);
        }
        co_return;
    }));
    cql3::functions::change_batch batch;
    co_await do_parse_schema_tables(proxy, db::schema_tables::FUNCTIONS, coroutine::lambda([&] (schema_result_value_type& v) -> future<> {
        auto&& user_functions = co_await create_functions_from_schema_partition(*this, v.second);
        for (auto&& func : user_functions) {
            batch.add_function(func);
        }
        co_return;
    }));
    co_await do_parse_schema_tables(proxy, db::schema_tables::AGGREGATES, coroutine::lambda([&] (schema_result_value_type& v) -> future<> {
        auto v2 = co_await read_schema_partition_for_keyspace(proxy, db::schema_tables::SCYLLA_AGGREGATES, v.first);
        auto&& user_aggregates = create_aggregates_from_schema_partition(*this, v.second, v2.second, batch);
        for (auto&& agg : user_aggregates) {
            batch.add_function(agg);
        }
        co_return;
    }));
    batch.commit();
    co_await do_parse_schema_tables(proxy, db::schema_tables::TABLES, coroutine::lambda([&] (schema_result_value_type &v) -> future<> {
        std::map<sstring, schema_ptr> tables = co_await create_tables_from_tables_partition(proxy, v.second);
        co_await coroutine::parallel_for_each(tables, [&] (auto& t) -> future<> {
            co_await this->add_column_family_and_make_directory(t.second, replica::database::is_new_cf::no);
            auto s = t.second;
            // Recreate missing column mapping entries in case
            // we failed to persist them for some reason after a schema change
            bool cm_exists = co_await db::schema_tables::column_mapping_exists(sys_ks.local(), s->id(), s->version());
            if (cm_exists) {
                co_return;
            }
            co_return co_await db::schema_tables::store_column_mapping(proxy, s, false);
        });
    }));
    co_await do_parse_schema_tables(proxy, db::schema_tables::VIEWS, coroutine::lambda([&] (schema_result_value_type &v) -> future<> {
        std::vector<view_ptr> views = co_await create_views_from_schema_partition(proxy, v.second);
        co_await coroutine::parallel_for_each(views, [&] (auto&& v) -> future<> {
            check_no_legacy_secondary_index_mv_schema(*this, v, nullptr);
            co_await this->add_column_family_and_make_directory(v, replica::database::is_new_cf::no);
        });
    }));
}

static auto add_fragmented_listeners(const gms::feature& f, db::commitlog& cl) {
    return f.when_enabled([&cl]() mutable {
        auto cfg = cl.active_config();
        if (!std::exchange(cfg.allow_fragmented_entries, true)) {
            cl.update_configuration(cfg);
        }
    });
}

future<>
database::init_commitlog() {
    if (_commitlog) {
        return make_ready_future<>();
    }

    auto config = db::commitlog::config::from_db_config(_cfg, _dbcfg.commitlog_scheduling_group, _dbcfg.available_memory);
    // todo: it would be much cleaner to allow the test to set the appropriate value:
    // utils::get_local_injector().resolve("decrease_commitlog_base_segment_id")
    if (utils::get_local_injector().enter("decrease_commitlog_base_segment_id")) {
        config.base_segment_id = 0;
    }
    if (features().fragmented_commitlog_entries) {
        config.allow_fragmented_entries = true;
    }
    return db::commitlog::create_commitlog(config).then([this](db::commitlog&& log) {
        _commitlog = std::make_unique<db::commitlog>(std::move(log));

        auto reg = add_fragmented_listeners(features().fragmented_commitlog_entries, *_commitlog);

        _commitlog->add_flush_handler([this, reg = std::move(reg)](db::cf_id_type id, db::replay_position pos) {
            if (!_tables_metadata.contains(id)) {
                // the CF has been removed.
                _commitlog->discard_completed_segments(id);
                return;
            }
            // Initiate a background flush. Waited upon in `stop()`.
            (void)_tables_metadata.get_table(id).flush(pos);
        }).release(); // we have longer life time than CL. Ignore reg anchor

        _cfg.commitlog_max_data_lifetime_in_seconds.observe([this](uint32_t max_time) {
            _commitlog->update_max_data_lifetime(max_time == 0 ? std::nullopt : std::make_optional(uint64_t(max_time)));
        });
    });
}

future<> database::modify_keyspace_on_all_shards(sharded<database>& sharded_db, std::function<future<>(replica::database&)> func, std::function<future<>(replica::database&)> notifier) {
    // Run func first on shard 0
    // to allow "seeding" of the effective_replication_map
    // with a new e_r_m instance.
    co_await sharded_db.invoke_on(0, func);
    co_await sharded_db.invoke_on_all([&] (replica::database& db) {
        if (this_shard_id() == 0) {
            return make_ready_future<>();
        }
        return func(db);
    });
    co_await sharded_db.invoke_on_all(notifier);
}

future<> database::update_keyspace(const keyspace_metadata& tmp_ksm) {
    auto& ks = find_keyspace(tmp_ksm.name());
    auto new_ksm = ::make_lw_shared<keyspace_metadata>(tmp_ksm.name(), tmp_ksm.strategy_name(), tmp_ksm.strategy_options(), tmp_ksm.initial_tablets(), tmp_ksm.durable_writes(),
                    ks.metadata()->cf_meta_data() | std::views::values | std::ranges::to<std::vector>(), std::move(ks.metadata()->user_types()), tmp_ksm.get_storage_options());

    bool old_durable_writes = ks.metadata()->durable_writes();
    bool new_durable_writes = new_ksm->durable_writes();
    if (old_durable_writes != new_durable_writes) {
        for (auto& [cf_name, cf_schema] : new_ksm->cf_meta_data()) {
            auto& cf = find_column_family(cf_schema);
            cf.set_durable_writes(new_durable_writes);
        }
    }

    co_await ks.update_from(get_shared_token_metadata(), std::move(new_ksm));
}

future<> database::update_keyspace_on_all_shards(sharded<database>& sharded_db, const keyspace_metadata& ksm) {
    return modify_keyspace_on_all_shards(sharded_db, [&] (replica::database& db) {
        return db.update_keyspace(ksm);
    }, [&] (replica::database& db) {
        const auto& ks = db.find_keyspace(ksm.name());
        return db.get_notifier().update_keyspace(ks.metadata());
    });
}

void database::drop_keyspace(const sstring& name) {
    _keyspaces.erase(name);
}

future<> database::drop_keyspace_on_all_shards(sharded<database>& sharded_db, const sstring& name) {
    return modify_keyspace_on_all_shards(sharded_db, [&] (replica::database& db) {
        db.drop_keyspace(name);
        return make_ready_future<>();
    }, [&] (replica::database& db) {
        return db.get_notifier().drop_keyspace(name);
    });
}

static bool is_system_table(const schema& s) {
    auto& k = s.ks_name();
    return k == db::system_keyspace::NAME ||
        k == db::system_distributed_keyspace::NAME ||
        k == db::system_distributed_keyspace::NAME_EVERYWHERE;
}

sstables::sstables_manager& database::get_sstables_manager(const schema& s) const {
    return get_sstables_manager(system_keyspace(is_system_table(s)));
}

void database::init_schema_commitlog() {
    SCYLLA_ASSERT(this_shard_id() == 0);

    db::commitlog::config c;
    c.sched_group = _dbcfg.schema_commitlog_scheduling_group;
    c.commit_log_location = _cfg.schema_commitlog_directory();
    c.fname_prefix = db::schema_tables::COMMITLOG_FILENAME_PREFIX;
    c.metrics_category_name = "schema-commitlog";
    c.commitlog_total_space_in_mb = 2 * _cfg.schema_commitlog_segment_size_in_mb();
    c.commitlog_segment_size_in_mb = _cfg.schema_commitlog_segment_size_in_mb();
    c.mode = db::commitlog::sync_mode::BATCH;
    c.extensions = &_cfg.extensions();
    c.use_o_dsync = _cfg.commitlog_use_o_dsync();
    c.allow_going_over_size_limit = true; // for lower latency
    if (features().fragmented_commitlog_entries) {
        c.allow_fragmented_entries = true;
    }

    _schema_commitlog = std::make_unique<db::commitlog>(db::commitlog::create_commitlog(c).get());

    auto reg = add_fragmented_listeners(features().fragmented_commitlog_entries, *_schema_commitlog);

    _schema_commitlog->add_flush_handler([this, reg = std::move(reg)] (db::cf_id_type id, db::replay_position pos) {
        if (!_tables_metadata.contains(id)) {
            // the CF has been removed.
            _schema_commitlog->discard_completed_segments(id);
            return;
        }
        // Initiate a background flush. Waited upon in `stop()`.
        (void)_tables_metadata.get_table(id).flush(pos);
    }).release();
}

std::optional<table_id> database::get_base_table_for_tablet_colocation(const schema& s, const std::unordered_map<table_id, schema_ptr>& new_cfms) {
    auto find_schema_from_db_or_new = [this, &new_cfms] (table_id table_id) -> schema_ptr {
        auto it = new_cfms.find(table_id);
        if (it != new_cfms.end()) {
            return it->second;
        }
        return find_schema(table_id);
    };

    // Co-locate a view table with its base table when it has exactly the same partition key - the same columns
    // in the same order. In this case the tokens of corresponding partitions are equal and we can benefit from
    // locality of view updates.
    bool is_colocated_view = std::invoke([&] {
        if (!s.is_view()) {
            return false;
        }

        auto base_schema_ptr = find_schema_from_db_or_new(s.view_info()->base_id());

        if (s.partition_key_size() != base_schema_ptr->partition_key_size()) {
            return false;
        }

        auto&& view_pk = s.partition_key_columns();
        auto&& base_pk = base_schema_ptr->partition_key_columns();
        for (const auto& [a,b] : std::views::zip(view_pk, base_pk)) {
            if (a.name() != b.name()) {
                return false;
            }
        }
        return true;
    });

    if (is_colocated_view) {
        return s.view_info()->base_id();
    }

    return std::nullopt;
}

future<> database::create_local_system_table(
        schema_ptr table, bool write_in_user_memory, locator::effective_replication_map_factory& erm_factory) {
    auto ks_name = table->ks_name();
    if (!has_keyspace(ks_name)) {
        bool durable = _cfg.data_file_directories().size() > 0;
        auto ksm = make_lw_shared<keyspace_metadata>(ks_name,
                "org.apache.cassandra.locator.LocalStrategy",
                std::map<sstring, sstring>{},
                std::nullopt,
                durable
                );
        co_await create_keyspace(ksm, erm_factory, replica::database::system_keyspace::yes);
    }
    auto& ks = find_keyspace(ks_name);
    auto cfg = ks.make_column_family_config(*table, *this);
    if (write_in_user_memory) {
        cfg.dirty_memory_manager = &_dirty_memory_manager;
    } else {
        cfg.memtable_scheduling_group = default_scheduling_group();
        cfg.memtable_to_cache_scheduling_group = default_scheduling_group();
    }
    co_await add_column_family(ks, table, std::move(cfg), replica::database::is_new_cf::no);
}

db::commitlog* database::commitlog_for(const schema_ptr& schema) {
    return schema->static_props().use_schema_commitlog
        ? _schema_commitlog.get()
        : _commitlog.get();
}

future<> database::add_column_family(keyspace& ks, schema_ptr schema, column_family::config cfg, is_new_cf is_new) {
    schema = local_schema_registry().learn(schema);
    auto&& rs = ks.get_replication_strategy();
    locator::effective_replication_map_ptr erm;
    if (auto pt_rs = rs.maybe_as_per_table()) {
        erm = pt_rs->make_replication_map(schema->id(), _shared_token_metadata.get());
    } else {
        erm = ks.get_vnode_effective_replication_map();
    }
    // avoid self-reporting
    auto& sst_manager = get_sstables_manager(*schema);
    auto cf = make_lw_shared<column_family>(schema, std::move(cfg), ks.metadata()->get_storage_options_ptr(), _compaction_manager, sst_manager, *_cl_stats, _row_cache_tracker, erm);
    cf->set_durable_writes(ks.metadata()->durable_writes());

    if (is_new) {
        cf->mark_ready_for_writes(commitlog_for(schema));
        cf->set_truncation_time(db_clock::time_point::min());
    }

    auto uuid = schema->id();
    if (_tables_metadata.contains(uuid)) {
        throw std::invalid_argument("UUID " + uuid.to_sstring() + " already mapped");
    }
    auto kscf = std::make_pair(schema->ks_name(), schema->cf_name());
    if (_tables_metadata.contains(kscf)) {
        throw std::invalid_argument("Column family " + schema->cf_name() + " exists");
    }
    cf->start();
    auto f = co_await coroutine::as_future(_tables_metadata.add_table(*this, ks, *cf, schema));
    if (f.failed()) {
        co_await cf->stop();
        co_await coroutine::return_exception_ptr(f.get_exception());
    }
    // Table must be added before entry is marked synced.
    schema->registry_entry()->mark_synced();
}

future<> database::add_column_family_and_make_directory(schema_ptr schema, is_new_cf is_new) {
    auto& ks = find_keyspace(schema->ks_name());
    co_await add_column_family(ks, schema, ks.make_column_family_config(*schema, *this), is_new);
    auto& cf = find_column_family(schema);
    cf.get_index_manager().reload();
    co_await cf.init_storage();
}

bool database::update_column_family(schema_ptr new_schema) {
    column_family& cfm = find_column_family(new_schema->id());
    bool columns_changed = !cfm.schema()->equal_columns(*new_schema);
    auto s = local_schema_registry().learn(new_schema);
    s->registry_entry()->mark_synced();
    cfm.set_schema(s);
    find_keyspace(s->ks_name()).metadata()->add_or_update_column_family(s);
    if (s->is_view()) {
        // We already tested that the base table exists
        find_column_family(s->view_info()->base_id()).add_or_update_view(view_ptr(s));
    }
    cfm.get_index_manager().reload();
    return columns_changed;
}

future<> database::remove(table& cf) noexcept {
    cf.deregister_metrics();
    return _tables_metadata.remove_table(*this, cf);
}

future<> database::detach_column_family(table& cf) {
    auto uuid = cf.schema()->id();
    co_await remove(cf);
    cf.clear_views();
    co_await cf.await_pending_ops();
    co_await foreach_reader_concurrency_semaphore([uuid] (reader_concurrency_semaphore& sem) -> future<> {
        co_await sem.evict_inactive_reads_for_table(uuid);
    });
}

global_table_ptr::global_table_ptr() {
    _p.resize(smp::count);
}

global_table_ptr::global_table_ptr(global_table_ptr&& o) noexcept
    : _p(std::move(o._p))
{ }

global_table_ptr::~global_table_ptr() {}

void global_table_ptr::assign(table& t) {
    _p[this_shard_id()] = make_foreign(t.shared_from_this());
}

table* global_table_ptr::operator->() const noexcept { return &*_p[this_shard_id()]; }
table& global_table_ptr::operator*() const noexcept { return *_p[this_shard_id()]; }

future<global_table_ptr> get_table_on_all_shards(sharded<database>& sharded_db, sstring ks_name, sstring cf_name) {
    auto uuid = sharded_db.local().find_uuid(ks_name, cf_name);
    return get_table_on_all_shards(sharded_db, std::move(uuid));
}

future<global_table_ptr> get_table_on_all_shards(sharded<database>& sharded_db, table_id uuid) {
    global_table_ptr table_shards;
    co_await sharded_db.invoke_on_all([&] (auto& db) {
        try {
            table_shards.assign(db.find_column_family(uuid));
        } catch (no_such_column_family&) {
            on_internal_error(dblog, fmt::format("Table UUID={} not found", uuid));
        }
    });
    co_return table_shards;
}

future<> database::drop_table_on_all_shards(sharded<database>& sharded_db, sharded<db::system_keyspace>& sys_ks,
        sstring ks_name, sstring cf_name, bool with_snapshot) {
    auto auto_snapshot = sharded_db.local().get_config().auto_snapshot();
    dblog.info("Dropping {}.{} {}snapshot", ks_name, cf_name, with_snapshot && auto_snapshot ? "with auto-" : "without ");

    auto uuid = sharded_db.local().find_uuid(ks_name, cf_name);
    auto table_shards = co_await get_table_on_all_shards(sharded_db, uuid);
    std::optional<sstring> snapshot_name_opt;
    if (with_snapshot) {
        snapshot_name_opt = format("pre-drop-{}", db_clock::now().time_since_epoch().count());
    }
    co_await sharded_db.invoke_on_all([&] (database& db) {
        return db.detach_column_family(*table_shards);
    });
    // Use a time point in the far future (9999-12-31T00:00:00+0000)
    // to ensure all sstables are truncated,
    // but be careful to stays within the client's datetime limits.
    constexpr db_clock::time_point truncated_at(std::chrono::seconds(253402214400));
    auto f = co_await coroutine::as_future(truncate_table_on_all_shards(sharded_db, sys_ks, table_shards, truncated_at, with_snapshot, std::move(snapshot_name_opt)));
    co_await smp::invoke_on_all([&] {
        return table_shards->stop();
    });
    f.get(); // re-throw exception from truncate() if any
    co_await table_shards->destroy_storage();
}

table_id database::find_uuid(std::string_view ks, std::string_view cf) const {
    try {
        return _tables_metadata.get_table_id(std::make_pair(ks, cf));
    } catch (std::out_of_range&) {
        throw no_such_column_family(ks, cf);
    }
}

table_id database::find_uuid(const schema_ptr& schema) const {
    return find_uuid(schema->ks_name(), schema->cf_name());
}

keyspace& database::find_keyspace(std::string_view name) {
    try {
        return _keyspaces.at(name);
    } catch (std::out_of_range&) {
        throw no_such_keyspace(name);
    }
}

const keyspace& database::find_keyspace(std::string_view name) const {
    try {
        return _keyspaces.at(name);
    } catch (std::out_of_range&) {
        throw no_such_keyspace(name);
    }
}

bool database::has_keyspace(std::string_view name) const {
    return _keyspaces.contains(name);
}

std::vector<sstring>  database::get_non_system_keyspaces() const {
    std::vector<sstring> res;
    for (auto const &i : _keyspaces) {
        if (!is_system_keyspace(i.first)) {
            res.push_back(i.first);
        }
    }
    return res;
}

std::vector<sstring> database::get_user_keyspaces() const {
    std::vector<sstring> res;
    for (auto const& i : _keyspaces) {
        if (!is_internal_keyspace(i.first)) {
            res.push_back(i.first);
        }
    }
    return res;
}

std::vector<sstring> database::get_all_keyspaces() const {
    std::vector<sstring> res;
    res.reserve(_keyspaces.size());
    for (auto const& i : _keyspaces) {
        res.push_back(i.first);
    }
    return res;
}

std::vector<sstring> database::get_non_local_strategy_keyspaces() const {
    std::vector<sstring> res;
    res.reserve(_keyspaces.size());
    for (auto const& i : _keyspaces) {
        if (i.second.get_replication_strategy().get_type() != locator::replication_strategy_type::local) {
            res.push_back(i.first);
        }
    }
    return res;
}

std::vector<sstring> database::get_non_local_vnode_based_strategy_keyspaces() const {
    std::vector<sstring> res;
    res.reserve(_keyspaces.size());
    for (auto const& [name, ks] : _keyspaces) {
        auto&& rs = ks.get_replication_strategy();
        if (rs.get_type() != locator::replication_strategy_type::local && rs.is_vnode_based()) {
            res.push_back(name);
        }
    }
    return res;
}

std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr> database::get_non_local_strategy_keyspaces_erms() const {
    std::unordered_map<sstring, locator::vnode_effective_replication_map_ptr> res;
    res.reserve(_keyspaces.size());
    for (auto const& [name, ks] : _keyspaces) {
        auto&& rs = ks.get_replication_strategy();
        if (rs.get_type() != locator::replication_strategy_type::local && !rs.is_per_table()) {
            res.emplace(name, ks.get_vnode_effective_replication_map());
        }
    }
    return res;
}

std::vector<sstring> database::get_tablets_keyspaces() const {
    std::vector<sstring> res;
    res.reserve(_keyspaces.size());
    for (auto const& [name, ks] : _keyspaces) {
        auto&& rs = ks.get_replication_strategy();
        if (rs.is_per_table()) {
            res.emplace_back(name);
        }
    }
    return res;
}

std::vector<lw_shared_ptr<column_family>> database::get_non_system_column_families() const {
    return get_tables_metadata().filter([] (auto uuid_and_cf) {
            return !is_system_keyspace(uuid_and_cf.second->schema()->ks_name());
        }) | std::views::values | std::ranges::to<std::vector>();
}

column_family& database::find_column_family(std::string_view ks_name, std::string_view cf_name) {
    auto uuid = find_uuid(ks_name, cf_name);
    try {
        return find_column_family(uuid);
    } catch (no_such_column_family&) {
        on_internal_error(dblog, fmt::format("find_column_family {}.{}: UUID={} not found", ks_name, cf_name, uuid));
    }
}

const column_family& database::find_column_family(std::string_view ks_name, std::string_view cf_name) const {
    auto uuid = find_uuid(ks_name, cf_name);
    try {
        return find_column_family(uuid);
    } catch (no_such_column_family&) {
        on_internal_error(dblog, fmt::format("find_column_family {}.{}: UUID={} not found", ks_name, cf_name, uuid));
    }
}

column_family& database::find_column_family(const table_id& uuid) {
    try {
        return _tables_metadata.get_table(uuid);
    } catch (...) {
        throw no_such_column_family(uuid);
    }
}

const column_family& database::find_column_family(const table_id& uuid) const {
    try {
        return _tables_metadata.get_table(uuid);
    } catch (...) {
        throw no_such_column_family(uuid);
    }
}

bool database::column_family_exists(const table_id& uuid) const {
    return _tables_metadata.contains(uuid);
}

future<>
keyspace::create_replication_strategy(const locator::shared_token_metadata& stm) {
    using namespace locator;

    locator::replication_strategy_params params(_metadata->strategy_options(), _metadata->initial_tablets());
    _replication_strategy =
            abstract_replication_strategy::create_replication_strategy(_metadata->strategy_name(), params);
    rslogger.debug("replication strategy for keyspace {} is {}, opts={}",
            _metadata->name(), _metadata->strategy_name(), _metadata->strategy_options());
    if (!_replication_strategy->is_per_table()) {
        auto erm = co_await _erm_factory.create_effective_replication_map(_replication_strategy, stm.get());
        update_effective_replication_map(std::move(erm));
    }
}

void
keyspace::update_effective_replication_map(locator::vnode_effective_replication_map_ptr erm) {
    _effective_replication_map = std::move(erm);
}

const locator::abstract_replication_strategy&
keyspace::get_replication_strategy() const {
    return *_replication_strategy;
}

future<> keyspace::update_from(const locator::shared_token_metadata& stm, ::lw_shared_ptr<keyspace_metadata> ksm) {
    _metadata = std::move(ksm);
   return create_replication_strategy(stm);
}

column_family::config
keyspace::make_column_family_config(const schema& s, const database& db) const {
    column_family::config cfg;
    const db::config& db_config = db.get_config();

    cfg.enable_disk_reads = _config.enable_disk_reads;
    cfg.enable_disk_writes = _config.enable_disk_writes;
    cfg.enable_commitlog = _config.enable_commitlog;
    cfg.enable_cache = _config.enable_cache;
    cfg.enable_dangerous_direct_import_of_cassandra_counters = _config.enable_dangerous_direct_import_of_cassandra_counters;
    cfg.compaction_enforce_min_threshold = _config.compaction_enforce_min_threshold;
    cfg.dirty_memory_manager = _config.dirty_memory_manager;
    cfg.streaming_read_concurrency_semaphore = _config.streaming_read_concurrency_semaphore;
    cfg.compaction_concurrency_semaphore = _config.compaction_concurrency_semaphore;
    cfg.cf_stats = _config.cf_stats;
    cfg.enable_incremental_backups = _config.enable_incremental_backups;
    cfg.compaction_scheduling_group = _config.compaction_scheduling_group;
    cfg.memory_compaction_scheduling_group = _config.memory_compaction_scheduling_group;
    cfg.memtable_scheduling_group = _config.memtable_scheduling_group;
    cfg.memtable_to_cache_scheduling_group = _config.memtable_to_cache_scheduling_group;
    cfg.streaming_scheduling_group = _config.streaming_scheduling_group;
    cfg.statement_scheduling_group = _config.statement_scheduling_group;
    cfg.enable_metrics_reporting = db_config.enable_keyspace_column_family_metrics();
    cfg.enable_node_aggregated_table_metrics = db_config.enable_node_aggregated_table_metrics();
    cfg.tombstone_warn_threshold = db_config.tombstone_warn_threshold();
    cfg.view_update_concurrency_semaphore_limit = _config.view_update_concurrency_semaphore_limit;
    cfg.data_listeners = &db.data_listeners();
    cfg.enable_compacting_data_for_streaming_and_repair = db_config.enable_compacting_data_for_streaming_and_repair;
    cfg.enable_tombstone_gc_for_streaming_and_repair = db_config.enable_tombstone_gc_for_streaming_and_repair;

    return cfg;
}

future<> table::init_storage() {
    _storage_opts = co_await _sstables_manager.init_table_storage(*_schema, *_storage_opts);
}

future<> table::destroy_storage() {
    return _sstables_manager.destroy_table_storage(*_storage_opts);
}

column_family& database::find_column_family(const schema_ptr& schema) {
    return find_column_family(schema->id());
}

const column_family& database::find_column_family(const schema_ptr& schema) const {
    return find_column_family(schema->id());
}

void database::validate_keyspace_update(keyspace_metadata& ksm) {
    ksm.validate(_feat, get_token_metadata().get_topology());
    if (!has_keyspace(ksm.name())) {
        throw exceptions::configuration_exception(format("Cannot update non existing keyspace '{}'.", ksm.name()));
    }
}

void database::validate_new_keyspace(keyspace_metadata& ksm) {
    ksm.validate(_feat, get_token_metadata().get_topology());
    if (has_keyspace(ksm.name())) {
        throw exceptions::already_exists_exception{ksm.name()};
    }
    _user_sstables_manager->validate_new_keyspace_storage_options(ksm.get_storage_options());
}

schema_ptr database::find_schema(const sstring& ks_name, const sstring& cf_name) const {
    auto uuid = find_uuid(ks_name, cf_name);
    try {
        return find_schema(uuid);
    } catch (no_such_column_family&) {
        on_internal_error(dblog, fmt::format("find_schema {}.{}: UUID={} not found", ks_name, cf_name, uuid));
    }
}

schema_ptr database::find_schema(const table_id& uuid) const {
    return find_column_family(uuid).schema();
}

bool database::has_schema(std::string_view ks_name, std::string_view cf_name) const {
    return _tables_metadata.contains(std::make_pair(ks_name, cf_name));
}

std::vector<view_ptr> database::get_views() const {
    return std::ranges::to<std::vector<view_ptr>>(get_non_system_column_families()
            | std::views::filter([] (auto& cf) { return cf->schema()->is_view(); })
            | std::views::transform([] (auto& cf) { return view_ptr(cf->schema()); }));
}

future<> database::create_in_memory_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, locator::effective_replication_map_factory& erm_factory, system_keyspace system) {
    auto kscfg = make_keyspace_config(*ksm, system);
    keyspace ks(ksm, std::move(kscfg), erm_factory);
    co_await ks.create_replication_strategy(get_shared_token_metadata());
    _keyspaces.emplace(ksm->name(), std::move(ks));
}

future<>
database::create_keyspace(const lw_shared_ptr<keyspace_metadata>& ksm, locator::effective_replication_map_factory& erm_factory, system_keyspace system) {
    if (_keyspaces.contains(ksm->name())) {
        co_return;
    }

    co_await create_in_memory_keyspace(ksm, erm_factory, system);
    co_await get_sstables_manager(system).init_keyspace_storage(ksm->get_storage_options(), ksm->name());
}

future<> database::create_keyspace_on_all_shards(sharded<database>& sharded_db, sharded<service::storage_proxy>& proxy, const keyspace_metadata& ks_metadata) {
    co_await modify_keyspace_on_all_shards(sharded_db, [&] (replica::database& db) -> future<> {
        auto ksm = keyspace_metadata::new_keyspace(ks_metadata);
        co_await db.create_keyspace(ksm, proxy.local().get_erm_factory(), system_keyspace::no);
    }, [&] (replica::database& db) -> future<> {
        const auto& ks = db.find_keyspace(ks_metadata.name());
        co_await db.get_notifier().create_keyspace(ks.metadata());
    });
}

future<>
database::drop_caches() const {
    std::unordered_map<table_id, lw_shared_ptr<column_family>> tables = get_tables_metadata().get_column_families_copy();
    for (auto&& e : tables) {
        table& t = *e.second;
        co_await t.get_row_cache().invalidate(row_cache::external_updater([] {}));
        auto sstables = t.get_sstables();
        for (sstables::shared_sstable sst : *sstables) {
            co_await sst->drop_caches();
        }
    }
    co_return;
}

std::set<sstring>
database::existing_index_names(const sstring& ks_name, const sstring& cf_to_exclude) const {
    return secondary_index::existing_index_names(find_keyspace(ks_name).metadata()->tables(), cf_to_exclude);
}

namespace {

enum class request_class {
    user,
    system,
    maintenance,
};

request_class classify_request(const database_config& _dbcfg) {
    const auto current_group = current_scheduling_group();

    // Everything running in the statement group is considered a user request
    if (current_group == _dbcfg.statement_scheduling_group) {
        return request_class::user;
    // System requests run in the default (main) scheduling group
    // All requests executed on behalf of internal work also uses the system semaphore
    } else if (current_group == default_scheduling_group()
            || current_group == _dbcfg.compaction_scheduling_group
            || current_group == _dbcfg.gossip_scheduling_group
            || current_group == _dbcfg.memory_compaction_scheduling_group
            || current_group == _dbcfg.memtable_scheduling_group
            || current_group == _dbcfg.memtable_to_cache_scheduling_group) {
        return request_class::system;
    // Requests done on behalf of view update generation run in the streaming group
    } else if (current_scheduling_group() == _dbcfg.streaming_scheduling_group) {
        return request_class::maintenance;
    // Everything else is considered a user request
    } else {
        return request_class::user;
    }
}

} // anonymous namespace

static bool can_apply_per_partition_rate_limit(const schema& s, const database_config& dbcfg, db::operation_type op_type) {
    return s.per_partition_rate_limit_options().get_max_ops_per_second(op_type).has_value()
            && classify_request(dbcfg) == request_class::user;
}

bool database::can_apply_per_partition_rate_limit(const schema& s, db::operation_type op_type) const {
    return replica::can_apply_per_partition_rate_limit(s, _dbcfg, op_type);
}

bool database::is_internal_query() const {
    return classify_request(_dbcfg) != request_class::user;
}

std::optional<db::rate_limiter::can_proceed> database::account_coordinator_operation_to_rate_limit(table& tbl, const dht::token& token,
        db::per_partition_rate_limit::account_and_enforce account_and_enforce_info,
        db::operation_type op_type) {

    std::optional<uint32_t> table_limit = tbl.schema()->per_partition_rate_limit_options().get_max_ops_per_second(op_type);
    db::rate_limiter::label& lbl = tbl.get_rate_limiter_label_for_op_type(op_type);
    return _rate_limiter.account_operation(lbl, dht::token::to_int64(token), *table_limit, account_and_enforce_info);
}

static db::rate_limiter::can_proceed account_singular_ranges_to_rate_limit(
        db::rate_limiter& limiter, column_family& cf,
        const dht::partition_range_vector& ranges,
        const database_config& dbcfg,
        db::per_partition_rate_limit::info rate_limit_info) {
    using can_proceed = db::rate_limiter::can_proceed;

    if (std::holds_alternative<std::monostate>(rate_limit_info) || !can_apply_per_partition_rate_limit(*cf.schema(), dbcfg, db::operation_type::read)) {
        // Rate limiting is disabled for this query
        return can_proceed::yes;
    }

    auto table_limit = *cf.schema()->per_partition_rate_limit_options().get_max_reads_per_second();
    can_proceed ret = can_proceed::yes;

    auto& read_label = cf.get_rate_limiter_label_for_reads();
    for (const auto& range : ranges) {
        if (!range.is_singular()) {
            continue;
        }
        auto token = dht::token::to_int64(ranges.front().start()->value().token());
        if (limiter.account_operation(read_label, token, table_limit, rate_limit_info) == db::rate_limiter::can_proceed::no) {
            // Don't return immediately - account all ranges first
            ret = can_proceed::no;
        }
    }

    return ret;
}

future<std::tuple<lw_shared_ptr<query::result>, cache_temperature>>
database::query(schema_ptr query_schema, const query::read_command& cmd, query::result_options opts, const dht::partition_range_vector& ranges,
                tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout, db::per_partition_rate_limit::info rate_limit_info) {
    column_family& cf = find_column_family(cmd.cf_id);

    if (account_singular_ranges_to_rate_limit(_rate_limiter, cf, ranges, _dbcfg, rate_limit_info) == db::rate_limiter::can_proceed::no) {
        ++_stats->total_reads_rate_limited;
        co_await coroutine::return_exception(replica::rate_limit_exception());
    }

    auto& semaphore = get_reader_concurrency_semaphore();
    auto max_result_size = cmd.max_result_size ? *cmd.max_result_size : get_query_max_result_size();

    std::optional<query::querier> querier_opt;
    lw_shared_ptr<query::result> result;
    std::exception_ptr ex;

    if (cmd.query_uuid && !cmd.is_first_page) {
        querier_opt = _querier_cache.lookup_data_querier(cmd.query_uuid, *query_schema, ranges.front(), cmd.slice, semaphore, trace_state, timeout);
    }

    auto read_func = [&, this] (reader_permit permit) {
        reader_permit::need_cpu_guard ncpu_guard{permit};
        permit.set_max_result_size(max_result_size);
        return cf.query(std::move(query_schema), std::move(permit), cmd, opts, ranges, trace_state, get_result_memory_limiter(),
                timeout, &querier_opt).then([&result, ncpu_guard = std::move(ncpu_guard)] (lw_shared_ptr<query::result> res) {
            result = std::move(res);
        });
    };

    try {
        auto op = cf.read_in_progress();

        future<> f = make_ready_future<>();
        if (querier_opt) {
            querier_opt->permit().set_trace_state(trace_state);
            f = co_await coroutine::as_future(semaphore.with_ready_permit(querier_opt->permit(), read_func));
        } else {
            reader_permit_opt permit_holder;
            f = co_await coroutine::as_future(semaphore.with_permit(query_schema, "data-query", cf.estimate_read_memory_cost(), timeout,
                        trace_state, permit_holder, read_func));
        }

        if (!f.failed()) {
            if (cmd.query_uuid && querier_opt) {
                _querier_cache.insert_data_querier(cmd.query_uuid, std::move(*querier_opt), std::move(trace_state));
            }
        } else {
            ex = f.get_exception();
        }
    } catch (...) {
        ex = std::current_exception();
    }

    if (querier_opt) {
        co_await querier_opt->close();
    }
    if (ex) {
        ++semaphore.get_stats().total_failed_reads;
        co_return coroutine::exception(std::move(ex));
    }

    auto hit_rate = cf.get_global_cache_hit_rate();
    ++semaphore.get_stats().total_successful_reads;
    _stats->short_data_queries += bool(result->is_short_read());
    co_return std::tuple(std::move(result), hit_rate);
}

future<std::tuple<reconcilable_result, cache_temperature>>
database::query_mutations(schema_ptr query_schema, const query::read_command& cmd, const dht::partition_range& range,
                          tracing::trace_state_ptr trace_state, db::timeout_clock::time_point timeout) {
    const auto short_read_allwoed = query::short_read(cmd.slice.options.contains<query::partition_slice::option::allow_short_read>());
    auto& semaphore = get_reader_concurrency_semaphore();
    auto max_result_size = cmd.max_result_size ? *cmd.max_result_size : get_query_max_result_size();
    auto accounter = co_await get_result_memory_limiter().new_mutation_read(max_result_size, short_read_allwoed);
    column_family& cf = find_column_family(cmd.cf_id);

    std::optional<query::querier> querier_opt;
    reconcilable_result result;
    std::exception_ptr ex;

    if (cmd.query_uuid && !cmd.is_first_page) {
        querier_opt = _querier_cache.lookup_mutation_querier(cmd.query_uuid, *query_schema, range, cmd.slice, semaphore, trace_state, timeout);
    }

    auto read_func = [&] (reader_permit permit) {
        reader_permit::need_cpu_guard ncpu_guard{permit};
        permit.set_max_result_size(max_result_size);
        return cf.mutation_query(std::move(query_schema), std::move(permit), cmd, range,
                std::move(trace_state), std::move(accounter), timeout, &querier_opt).then([&result, ncpu_guard = std::move(ncpu_guard)] (reconcilable_result res) {
            result = std::move(res);
        });
    };

    try {
        auto op = cf.read_in_progress();

        future<> f = make_ready_future<>();
        if (querier_opt) {
            querier_opt->permit().set_trace_state(trace_state);
            f = co_await coroutine::as_future(semaphore.with_ready_permit(querier_opt->permit(), read_func));
        } else {
            reader_permit_opt permit_holder;
            f = co_await coroutine::as_future(semaphore.with_permit(query_schema, "mutation-query", cf.estimate_read_memory_cost(), timeout,
                        trace_state, permit_holder, read_func));
        }

        if (!f.failed()) {
            if (cmd.query_uuid && querier_opt) {
                _querier_cache.insert_mutation_querier(cmd.query_uuid, std::move(*querier_opt), std::move(trace_state));
            }
        } else {
            ex = f.get_exception();
        }

    } catch (...) {
        ex = std::current_exception();
    }

    if (querier_opt) {
        co_await querier_opt->close();
    }
    if (ex) {
        ++semaphore.get_stats().total_failed_reads;
        co_return coroutine::exception(std::move(ex));
    }

    auto hit_rate = cf.get_global_cache_hit_rate();
    ++semaphore.get_stats().total_successful_reads;
    _stats->short_mutation_queries += bool(result.is_short_read());
    co_return std::tuple(std::move(result), hit_rate);
}

query::max_result_size database::get_query_max_result_size() const {
    switch (classify_request(_dbcfg)) {
        case request_class::user:
            return query::max_result_size(_cfg.max_memory_for_unlimited_query_soft_limit(), _cfg.max_memory_for_unlimited_query_hard_limit(),
                    _cfg.query_page_size_in_bytes());
        case request_class::system: [[fallthrough]];
        case request_class::maintenance:
            return query::max_result_size(query::result_memory_limiter::unlimited_result_size, query::result_memory_limiter::unlimited_result_size,
                    query::result_memory_limiter::maximum_result_size);
    }
    std::abort();
}

reader_concurrency_semaphore& database::get_reader_concurrency_semaphore() {
    switch (classify_request(_dbcfg)) {
        case request_class::user: return read_concurrency_sem();
        case request_class::system: return _system_read_concurrency_sem;
        case request_class::maintenance: return _streaming_concurrency_sem;
    }
    std::abort();
}

future<reader_permit> database::obtain_reader_permit(table& tbl, const char* const op_name, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) {
    return get_reader_concurrency_semaphore().obtain_permit(tbl.schema(), op_name, tbl.estimate_read_memory_cost(), timeout, std::move(trace_ptr));
}

future<reader_permit> database::obtain_reader_permit(schema_ptr schema, const char* const op_name, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_ptr) {
    return obtain_reader_permit(find_column_family(std::move(schema)), op_name, timeout, std::move(trace_ptr));
}

bool database::is_user_semaphore(const reader_concurrency_semaphore& semaphore) const {
    return &semaphore != &_streaming_concurrency_sem
        && &semaphore != &_compaction_concurrency_sem
        && &semaphore != &_system_read_concurrency_sem;
}

future<> database::clear_inactive_reads_for_tablet(table_id table, dht::token_range tablet_range) {
    const auto partition_range = dht::to_partition_range(tablet_range);
    co_await foreach_reader_concurrency_semaphore([table, &partition_range] (reader_concurrency_semaphore& sem) -> future<> {
        co_await sem.evict_inactive_reads_for_table(table, &partition_range);
    });
}

future<> database::foreach_reader_concurrency_semaphore(std::function<future<>(reader_concurrency_semaphore&)> func) {
    for (auto* sem : {&_streaming_concurrency_sem, &_compaction_concurrency_sem, &_system_read_concurrency_sem}) {
        co_await func(*sem);
    }
    co_await _reader_concurrency_semaphores_group.foreach_semaphore_async([&] (scheduling_group sg, reader_concurrency_semaphore& sem) -> future<> {
        co_await func(sem);
    });
    co_await _view_update_read_concurrency_semaphores_group.foreach_semaphore_async([&] (scheduling_group sg, reader_concurrency_semaphore& sem) -> future<> {
        co_await func(sem);
    });
}

std::ostream& operator<<(std::ostream& out, const column_family& cf) {
    fmt::print(out, "{{column_family: {}/{}}}", cf._schema->ks_name(), cf._schema->cf_name());
    return out;
}

std::ostream& operator<<(std::ostream& out, const database& db) {
    out << "{\n";
    db._tables_metadata.for_each_table([&] (table_id id, const lw_shared_ptr<table> tp) {
        auto&& cf = *tp;
        out << "(" << id.to_sstring() << ", " << cf.schema()->cf_name() << ", " << cf.schema()->ks_name() << "): " << cf << "\n";
    });
    out << "}";
    return out;
}

future<mutation> database::do_apply_counter_update(column_family& cf, const frozen_mutation& fm, schema_ptr m_schema,
                                                   db::timeout_clock::time_point timeout,tracing::trace_state_ptr trace_state) {
    auto m = fm.unfreeze(m_schema);
    m.upgrade(cf.schema());

    // prepare partition slice
    query::column_id_vector static_columns;
    static_columns.reserve(m.partition().static_row().size());
    m.partition().static_row().for_each_cell([&] (auto id, auto&&) {
        static_columns.emplace_back(id);
    });

    query::clustering_row_ranges cr_ranges;
    cr_ranges.reserve(8);
    query::column_id_vector regular_columns;
    regular_columns.reserve(32);

    for (auto&& cr : m.partition().clustered_rows()) {
        cr_ranges.emplace_back(query::clustering_range::make_singular(cr.key()));
        cr.row().cells().for_each_cell([&] (auto id, auto&&) {
            regular_columns.emplace_back(id);
        });
    }

    std::ranges::sort(regular_columns);
    regular_columns.erase(std::unique(regular_columns.begin(), regular_columns.end()),
                          regular_columns.end());

    auto slice = query::partition_slice(std::move(cr_ranges), std::move(static_columns),
        std::move(regular_columns), { }, { }, query::max_rows);

    auto op = cf.write_in_progress();

    tracing::trace(trace_state, "Acquiring counter locks");
    auto locks = co_await cf.lock_counter_cells(m, timeout);

    // Before counter update is applied it needs to be transformed from
    // deltas to counter shards. To do that, we need to read the current
    // counter state for each modified cell...

    tracing::trace(trace_state, "Reading counter values from the CF");
    auto permit = get_reader_concurrency_semaphore().make_tracking_only_permit(cf.schema(), "counter-read-before-write", timeout, trace_state);
    auto mopt = co_await counter_write_query(cf.schema(), cf.as_mutation_source(), std::move(permit), m.decorated_key(), slice, trace_state);

    // ...now, that we got existing state of all affected counter
    // cells we can look for our shard in each of them, increment
    // its clock and apply the delta.
    transform_counter_updates_to_shards(m, mopt ? &*mopt : nullptr, cf.failed_counter_applies_to_memtable(), get_token_metadata().get_my_id());
    tracing::trace(trace_state, "Applying counter update");
    co_await apply_with_commitlog(cf, m, timeout);

    if (utils::get_local_injector().enter("apply_counter_update_delay_5s")) {
        co_await seastar::sleep(std::chrono::seconds(5));
    }

    co_return m;
}

api::timestamp_type memtable_list::min_live_timestamp(const dht::decorated_key& dk, is_shadowable is, api::timestamp_type max_seen_timestamp) const noexcept {
    const auto get_min_ts = [is] (const memtable& mt) {
        // see get_max_purgeable_timestamp() in compaction.cc for comments on choosing min timestamp
        return is ? mt.get_min_live_row_marker_timestamp() : mt.get_min_live_timestamp();
    };

    auto min_live_ts = api::max_timestamp;

    for (const auto& mt : _memtables) {
        const auto mt_min_live_ts = get_min_ts(*mt);
        if (mt_min_live_ts > max_seen_timestamp) {
            continue;
        }
        // We cannot do lookups on flushing memtables, they might be in the
        // process of merging into cache. Keys already merged will not be seen
        // by the lookup.
        if (!mt->is_merging_to_cache() && !mt->contains_partition(dk)) {
            continue;
        }
        min_live_ts = std::min(min_live_ts, mt_min_live_ts);
    }

    for (const auto& mt : _flushed_memtables_with_active_reads) {
        // We cannot check if the flushed memtable contains the key as it
        // becomes empty after the merge to cache completes, so we only use the
        // min ts metadata.
        min_live_ts = std::min(min_live_ts, get_min_ts(mt));
    }

    return min_live_ts;
}

future<> memtable_list::flush() {
    if (!may_flush()) {
        return make_ready_future<>();
    } else if (!_flush_coalescing) {
        promise<> flushed;
        future<> ret = _flush_coalescing.emplace(flushed.get_future());
        _dirty_memory_manager->start_extraneous_flush();
        _dirty_memory_manager->get_flush_permit().then([this] (auto permit) {
            _flush_coalescing.reset();
            return _dirty_memory_manager->flush_one(*this, std::move(permit)).finally([this] {
                _dirty_memory_manager->finish_extraneous_flush();
            });
        }).forward_to(std::move(flushed));
        return ret;
    } else {
        return *_flush_coalescing;
    }
}

lw_shared_ptr<memtable> memtable_list::new_memtable() {
    return make_lw_shared<memtable>(_current_schema(), *_dirty_memory_manager,
            _table_shared_data,
            _table_stats, this, _compaction_scheduling_group);
}

// Synchronously swaps the active memtable with a new, empty one,
// returning the old memtables list.
// Exception safe.
std::vector<replica::shared_memtable> memtable_list::clear_and_add() {
    std::vector<replica::shared_memtable> new_memtables;
    new_memtables.emplace_back(new_memtable());
    return std::exchange(_memtables, std::move(new_memtables));
}

future<> database::apply_in_memory(const frozen_mutation& m, schema_ptr m_schema, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    auto& cf = find_column_family(m.column_family_id());

    data_listeners().on_write(m_schema, m);

    if (m.representation().size() > 128*1024) {
        return unfreeze_gently(m, std::move(m_schema)).then([&cf, h = std::move(h), timeout] (auto m) mutable {
            return do_with(std::move(m), [&cf, h = std::move(h), timeout] (auto& m) mutable {
                return cf.apply(m, std::move(h), timeout);
            });
        });
    }

    return cf.apply(m, std::move(m_schema), std::move(h), timeout);
}

future<> database::apply_in_memory(const mutation& m, column_family& cf, db::rp_handle&& h, db::timeout_clock::time_point timeout) {
    return cf.apply(m, std::move(h), timeout);
}

future<mutation> database::apply_counter_update(schema_ptr s, const frozen_mutation& m, db::timeout_clock::time_point timeout, tracing::trace_state_ptr trace_state) {
    if (timeout <= db::timeout_clock::now()) {
        update_write_metrics_for_timed_out_write();
        return make_exception_future<mutation>(timed_out_error{});
    }
  return update_write_metrics(seastar::futurize_invoke([&] {
    if (!s->is_synced()) {
        throw std::runtime_error(format("attempted to mutate using not synced schema of {}.{}, version={}",
                                        s->ks_name(), s->cf_name(), s->version()));
    }
    try {
        auto& cf = find_column_family(m.column_family_id());
        return do_apply_counter_update(cf, m, s, timeout, std::move(trace_state));
    } catch (no_such_column_family&) {
        dblog.error("Attempting to mutate non-existent table {}", m.column_family_id());
        throw;
    }
  }));
}

// #9919 etc. The initiative to wrap exceptions here
// causes a bunch of problems with (implicit) call sites
// catching timed_out_error (not checking is_timeout_exception).
// Fixing the call sites is a good idea, but it is also hard
// to verify. This workaround should ensure we take the
// correct code paths in all cases, until we can clean things up
// proper.
class wrapped_timed_out_error : public timed_out_error {
private:
    sstring _msg;
public:
    wrapped_timed_out_error(sstring msg)
        : _msg(std::move(msg))
    {}
    const char* what() const noexcept override {
        return _msg.c_str();
    }
};

// see above (#9919)
// Wrap the exception in a nested_exception to add additional error context.
static std::exception_ptr wrap_commitlog_add_error(const schema_ptr& s, const frozen_mutation& m, std::exception_ptr eptr) {
    // it is tempting to do a full pretty print here, but the mutation is likely
    // humungous if we got an error, so just tell us where and pk...
    auto commitlog_error_message = format("Could not write mutation {}:{} ({}) to commitlog", s->ks_name(), s->cf_name(), m.key());
    if (is_timeout_exception(eptr)) {
        return make_nested_exception_ptr(wrapped_timed_out_error(std::move(commitlog_error_message)), std::move(eptr));
    }
    return make_nested_exception_ptr(utils::internal::default_nested_exception_type(std::move(commitlog_error_message)), std::move(eptr));
}

future<> database::apply_with_commitlog(column_family& cf, const mutation& m, db::timeout_clock::time_point timeout) {
    db::rp_handle h;
    if (cf.commitlog() != nullptr && cf.durable_writes()) {
        auto fm = freeze(m);
        std::exception_ptr ex;
        try {
            commitlog_entry_writer cew(m.schema(), fm, db::commitlog::force_sync::no);
            auto f_h = co_await coroutine::as_future(cf.commitlog()->add_entry(m.schema()->id(), cew, timeout));
            if (!f_h.failed()) {
                h = f_h.get();
            } else {
                ex = f_h.get_exception();
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            ex = wrap_commitlog_add_error(cf.schema(), fm, std::move(ex));
            co_await coroutine::exception(std::move(ex));
        }
    }
    try {
        co_await apply_in_memory(m, cf, std::move(h), timeout);
    } catch (mutation_reordered_with_truncate_exception&) {
        // This mutation raced with a truncate, so we can just drop it.
        dblog.debug("replay_position reordering detected");
    }
}

future<> database::apply(const utils::chunked_vector<frozen_mutation>& muts, db::timeout_clock::time_point timeout) {
    if (timeout <= db::timeout_clock::now()) {
        update_write_metrics_for_timed_out_write();
        return make_exception_future<>(timed_out_error{});
    }
    return update_write_metrics(do_apply_many(muts, timeout));
}

future<> database::do_apply_many(const utils::chunked_vector<frozen_mutation>& muts, db::timeout_clock::time_point timeout) {
    utils::chunked_vector<commitlog_entry_writer> writers;
    db::commitlog* cl = nullptr;

    if (muts.empty()) {
        co_return;
    }

    writers.reserve(muts.size());

    for (size_t i = 0; i < muts.size(); ++i) {
        auto s = local_schema_registry().get(muts[i].schema_version());
        auto&& cf = find_column_family(muts[i].column_family_id());

        if (!cl) {
            cl = cf.commitlog();
        } else if (cl != cf.commitlog()) {
            auto&& first_cf = find_column_family(muts[0].column_family_id());
            on_internal_error(dblog, format("Cannot apply atomically across commitlog domains: {}.{}, {}.{}",
                              cf.schema()->ks_name(), cf.schema()->cf_name(),
                              first_cf.schema()->ks_name(), first_cf.schema()->cf_name()));
        }

        auto m_shards = cf.shard_for_writes(dht::get_token(*s, muts[i].key()));
        if (std::ranges::find(m_shards, this_shard_id()) == std::ranges::end(m_shards)) {
            on_internal_error(dblog, format("Must call apply() on the owning shard ({} not in {})", this_shard_id(), m_shards));
        }

        dblog.trace("apply [{}/{}]: {}", i, muts.size() - 1, muts[i].pretty_printer(s));
        writers.emplace_back(s, muts[i], commitlog_entry_writer::force_sync::yes);
    }

    if (!cl) {
        on_internal_error(dblog, "Cannot apply atomically without commitlog");
    }

    auto handles = co_await cl->add_entries(std::move(writers), timeout);

    // FIXME: Memtable application is not atomic so reads may observe mutations partially applied until restart.
    for (size_t i = 0; i < muts.size(); ++i) {
        auto s = local_schema_registry().get(muts[i].schema_version());
        co_await apply_in_memory(muts[i], s, std::move(handles[i]), timeout);
    }
}

future<> database::do_apply(schema_ptr s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout, db::commitlog::force_sync sync, db::per_partition_rate_limit::info rate_limit_info) {
    ++_stats->total_writes;
    // assume failure until proven otherwise
    auto update_writes_failed = defer([&] { ++_stats->total_writes_failed; });

    utils::get_local_injector().inject("database_apply", [&s] () {
        if (!is_system_keyspace(s->ks_name())) {
            throw std::runtime_error("injected error");
        }
    });

    // I'm doing a nullcheck here since the init code path for db etc
    // is a little in flux and commitlog is created only when db is
    // initied from datadir.
    auto uuid = m.column_family_id();
    auto& cf = find_column_family(uuid);

    if (!std::holds_alternative<std::monostate>(rate_limit_info) && can_apply_per_partition_rate_limit(*s, db::operation_type::write)) {
        auto table_limit = *s->per_partition_rate_limit_options().get_max_writes_per_second();
        auto& write_label = cf.get_rate_limiter_label_for_writes();
        auto token = dht::token::to_int64(dht::get_token(*s, m.key()));
        if (_rate_limiter.account_operation(write_label, token, table_limit, rate_limit_info) == db::rate_limiter::can_proceed::no) {
            ++_stats->total_writes_rate_limited;
            co_await coroutine::return_exception(replica::rate_limit_exception());
        }
    }

    sync = sync || db::commitlog::force_sync(s->wait_for_sync_to_commitlog());

    // Signal to view building code that a write is in progress,
    // so it knows when new writes start being sent to a new view.
    auto op = cf.write_in_progress();

    row_locker::lock_holder lock;
    if (!cf.views().empty()) {
        if (!_view_update_generator) {
            co_await coroutine::return_exception(std::runtime_error("view update generator not plugged to push updates"));
        }

        auto lock_f = co_await coroutine::as_future(cf.push_view_replica_updates(_view_update_generator, s, m, timeout, std::move(tr_state), view_update_read_concurrency_sem()));
        if (lock_f.failed()) {
            auto ex = lock_f.get_exception();
            if (is_timeout_exception(ex)) {
                ++_stats->total_writes_timedout;
            }
            co_await coroutine::return_exception_ptr(std::move(ex));
        }
        lock = lock_f.get();
    }

    // purposefully manually "inlined" apply_with_commitlog call here to reduce # coroutine
    // frames.
    db::rp_handle h;
    auto cl = cf.commitlog();
    if (cl != nullptr && cf.durable_writes()) {
        std::exception_ptr ex;
        try {
            commitlog_entry_writer cew(s, m, sync);
            auto f_h = co_await coroutine::as_future(cf.commitlog()->add_entry(uuid, cew, timeout));
            if (!f_h.failed()) {
                h = f_h.get();
            } else {
                ex = f_h.get_exception();
            }
        } catch (...) {
            ex = std::current_exception();
        }
        if (ex) {
            if (is_timeout_exception(ex)) {
                ++_stats->total_writes_timedout;
            }
            ex = wrap_commitlog_add_error(s, m, std::move(ex));
            co_await coroutine::exception(std::move(ex));
        }
    }
    auto f = co_await coroutine::as_future(this->apply_in_memory(m, s, std::move(h), timeout));
    if (f.failed()) {
      auto ex = f.get_exception();
      if (try_catch<mutation_reordered_with_truncate_exception>(ex)) {
        // This mutation raced with a truncate, so we can just drop it.
        dblog.debug("replay_position reordering detected");
        co_return;
      } else if (is_timeout_exception(ex)) {
        ++_stats->total_writes_timedout;
      }
      co_await coroutine::return_exception_ptr(std::move(ex));
    }
    // Success, prevent incrementing failure counter
    update_writes_failed.cancel();
}

template<typename Future>
Future database::update_write_metrics(Future&& f) {
    return f.then_wrapped([s = _stats] (auto f) {
        if (f.failed()) {
            ++s->total_writes_failed;
            auto ep = f.get_exception();
            if (is_timeout_exception(ep)) {
                ++s->total_writes_timedout;
            } else if (try_catch<replica::rate_limit_exception>(ep)) {
                ++s->total_writes_rate_limited;
            }
            return futurize<Future>::make_exception_future(std::move(ep));
        }
        ++s->total_writes;
        return f;
    });
}

void database::update_write_metrics_for_timed_out_write() {
    ++_stats->total_writes;
    ++_stats->total_writes_failed;
    ++_stats->total_writes_timedout;
}

future<> database::apply(schema_ptr s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::commitlog::force_sync sync, db::timeout_clock::time_point timeout, db::per_partition_rate_limit::info rate_limit_info) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("apply {}", m.pretty_printer(s));
    }
    if (timeout <= db::timeout_clock::now()) {
        update_write_metrics_for_timed_out_write();
        return make_exception_future<>(timed_out_error{});
    }
    if (!s->is_synced()) {
        on_internal_error(dblog, format("attempted to apply mutation using not synced schema of {}.{}, version={}", s->ks_name(), s->cf_name(), s->version()));
    }
    return _apply_stage(this, std::move(s), seastar::cref(m), std::move(tr_state), timeout, sync, rate_limit_info);
}

future<> database::apply_hint(schema_ptr s, const frozen_mutation& m, tracing::trace_state_ptr tr_state, db::timeout_clock::time_point timeout) {
    if (dblog.is_enabled(logging::log_level::trace)) {
        dblog.trace("apply hint {}", m.pretty_printer(s));
    }
    if (!s->is_synced()) {
        on_internal_error(dblog, format("attempted to apply hint using not synced schema of {}.{}, version={}", s->ks_name(), s->cf_name(), s->version()));
    }
    return with_scheduling_group(_dbcfg.streaming_scheduling_group, [this, s = std::move(s), &m, tr_state = std::move(tr_state), timeout] () mutable {
        return _apply_stage(this, std::move(s), seastar::cref(m), std::move(tr_state), timeout, db::commitlog::force_sync::no, std::monostate{});
    });
}

keyspace::config
database::make_keyspace_config(const keyspace_metadata& ksm, system_keyspace is_system) {
    keyspace::config cfg;
    if (is_system == system_keyspace::yes) {
        cfg.enable_disk_reads = cfg.enable_disk_writes = cfg.enable_commitlog = !_cfg.volatile_system_keyspace_for_testing();
        cfg.enable_cache = _cfg.enable_cache();
    } else if (_cfg.data_file_directories().size() > 0) {
        cfg.enable_disk_writes = !_cfg.enable_in_memory_data_store();
        cfg.enable_disk_reads = true; // we always read from disk
        cfg.enable_commitlog = _cfg.enable_commitlog() && !_cfg.enable_in_memory_data_store();
        cfg.enable_cache = _cfg.enable_cache();
    } else {
        cfg.enable_disk_writes = false;
        cfg.enable_disk_reads = false;
        cfg.enable_commitlog = false;
        cfg.enable_cache = false;
    }
    cfg.enable_dangerous_direct_import_of_cassandra_counters = _cfg.enable_dangerous_direct_import_of_cassandra_counters();
    cfg.compaction_enforce_min_threshold = _cfg.compaction_enforce_min_threshold;
    // don't make system or internal keyspace writes wait for user writes (if under pressure)
    if (is_system || extensions().is_extension_internal_keyspace(ksm.name())) {
        cfg.dirty_memory_manager = &_system_dirty_memory_manager;
    } else {
        cfg.dirty_memory_manager = &_dirty_memory_manager;
    }
    cfg.streaming_read_concurrency_semaphore = &_streaming_concurrency_sem;
    cfg.compaction_concurrency_semaphore = &_compaction_concurrency_sem;
    cfg.cf_stats = &_cf_stats;
    cfg.enable_incremental_backups = _enable_incremental_backups;

    cfg.compaction_scheduling_group = _dbcfg.compaction_scheduling_group;
    cfg.memory_compaction_scheduling_group = _dbcfg.memory_compaction_scheduling_group;
    cfg.memtable_scheduling_group = _dbcfg.memtable_scheduling_group;
    cfg.memtable_to_cache_scheduling_group = _dbcfg.memtable_to_cache_scheduling_group;
    cfg.streaming_scheduling_group = _dbcfg.streaming_scheduling_group;
    cfg.statement_scheduling_group = _dbcfg.statement_scheduling_group;
    cfg.enable_metrics_reporting = _cfg.enable_keyspace_column_family_metrics();

    cfg.view_update_concurrency_semaphore_limit = max_memory_pending_view_updates();
    return cfg;
}

} // namespace replica

auto fmt::formatter<db::write_type>::format(db::write_type t,
                                            fmt::format_context& ctx) const
        -> decltype(ctx.out()) {
    std::string_view name;
    switch (t) {
    using enum db::write_type;
    case SIMPLE:
        name = "SIMPLE";
        break;
    case BATCH:
        name = "BATCH";
        break;
    case UNLOGGED_BATCH:
        name = "UNLOGGED_BATCH";
        break;
    case COUNTER:
        name = "COUNTER";
        break;
    case BATCH_LOG:
        name = "BATCH_LOG";
        break;
    case CAS:
        name = "CAS";
        break;
    case VIEW:
        name = "VIEW";
        break;
    }
    return fmt::format_to(ctx.out(), "{}", name);
}

auto fmt::formatter<db::operation_type>::format(db::operation_type op_type, fmt::format_context& ctx) const -> decltype(ctx.out()) {
    switch (op_type) {
    case operation_type::read: return fmt::format_to(ctx.out(), "read");
    case operation_type::write: return fmt::format_to(ctx.out(), "write");
    }
    abort();
}


std::string_view fmt::formatter<db::consistency_level>::to_string(db::consistency_level cl) {
    switch (cl) {
    using enum db::consistency_level;
    case ANY:
        return "ANY";
    case ONE:
        return "ONE";
    case TWO:
        return "TWO";
    case THREE:
        return "THREE";
    case QUORUM:
        return "QUORUM";
    case ALL:
        return "ALL";
    case LOCAL_QUORUM:
        return "LOCAL_QUORUM";
    case EACH_QUORUM:
        return "EACH_QUORUM";
    case SERIAL:
        return "SERIAL";
    case LOCAL_SERIAL:
        return "LOCAL_SERIAL";
    case LOCAL_ONE:
        return "LOCAL_ONE";
    default:
        abort();
    }
}

namespace replica {

sstring database::get_available_index_name(const sstring &ks_name, const sstring &cf_name,
                                           std::optional<sstring> index_name_root) const
{
    return secondary_index::get_available_index_name(ks_name, cf_name, index_name_root, existing_index_names(ks_name),
            [this] (std::string_view ks, std::string_view cf) { return has_schema(ks, cf); });
}

schema_ptr database::find_indexed_table(const sstring& ks_name, const sstring& index_name) const {
    for (auto& schema : find_keyspace(ks_name).metadata()->tables()) {
        if (schema->has_index(index_name)) {
            return schema;
        }
    }
    return nullptr;
}

future<> database::close_tables(table_kind kind_to_close) {
    auto b = defer([this] { _stop_barrier.abort(); });
    co_await _tables_metadata.parallel_for_each_table(coroutine::lambda([this, kind_to_close] (table_id, lw_shared_ptr<table> table) -> future<> {
        auto& s = table->schema();
        table_kind k = is_system_table(*s) || _cfg.extensions().is_extension_internal_keyspace(s->ks_name()) ? table_kind::system : table_kind::user;
        if (k == kind_to_close) {
            co_await table->stop();
        }
    }));
    co_await _stop_barrier.arrive_and_wait();
    b.cancel();
}

void database::revert_initial_system_read_concurrency_boost() {
    _system_read_concurrency_sem.set_resources({database::max_count_system_concurrent_reads, max_memory_system_concurrent_reads()});
    dblog.debug("Reverted system read concurrency from initial {} to normal {}", database::max_count_concurrent_reads, database::max_count_system_concurrent_reads);
}

future<> database::start(sharded<qos::service_level_controller>& sl_controller) {
    sl_controller.local().register_subscriber(this);
    _unsubscribe_qos_configuration_change = [this, &sl_controller] () {
        return sl_controller.local().unregister_subscriber(this);
    };
    qos::service_level default_service_level = sl_controller.local().get_service_level(qos::service_level_controller::default_service_level_name);
    int32_t default_shares = 1000;
    if (int32_t* default_shares_p = std::get_if<int32_t>(&(default_service_level.slo.shares))) {
        default_shares = *default_shares_p;
    } else {
        on_internal_error(dblog, "The default service_level should always contain shares value");
    }

    // The former _dbcfg.statement_scheduling_group and the later can be the same group, so we want
    // the later to be the accurate one.
    _default_read_concurrency_group = default_service_level.sg;
    _reader_concurrency_semaphores_group.add_or_update(default_service_level.sg, default_shares);
    _view_update_read_concurrency_semaphores_group.add_or_update(default_service_level.sg, default_shares);

    // lets insert the statement scheduling group only if we haven't reused it in sl_controller,
    // but it shouldn't happen
    if (!_reader_concurrency_semaphores_group.get_or_null(_dbcfg.statement_scheduling_group)) {
        // This is super ugly, we need to either force the database to use system scheduling group for non-user queries
        // or, if we have user queries running on this scheduling group make it's definition more robust (what runs in it).
        // Another ugly thing here is that we have to have a pre-existing knowledge about the shares amount this group was
        // built with. I think we should have a followup that makes this more robust.
        _reader_concurrency_semaphores_group.add_or_update(_dbcfg.statement_scheduling_group, 1000);
        _view_update_read_concurrency_semaphores_group.add_or_update(_dbcfg.statement_scheduling_group, 1000);
    }

    // This will wait for the semaphores to be given some memory.
    // We need this since the below statements (get_distributed_service_levels in particular) will need
    // to run queries and for this they will need to admit some memory.
    co_await _reader_concurrency_semaphores_group.wait_adjust_complete();
    co_await _view_update_read_concurrency_semaphores_group.wait_adjust_complete();

    auto service_levels = co_await sl_controller.local().get_distributed_service_levels(qos::query_context::group0);
    for (auto&& service_level_record : service_levels) {
        auto service_level = sl_controller.local().get_service_level(service_level_record.first);
        if (service_level.slo.shares_name && *service_level.slo.shares_name != qos::service_level_controller::default_service_level_name) {
            // We know slo.shares is valid because we know that slo.shares_name is valid
            _reader_concurrency_semaphores_group.add_or_update(service_level.sg, std::get<int32_t>(service_level.slo.shares));
            _view_update_read_concurrency_semaphores_group.add_or_update(service_level.sg, std::get<int32_t>(service_level.slo.shares));
        }
    }

    co_await _reader_concurrency_semaphores_group.adjust();
    co_await _view_update_read_concurrency_semaphores_group.adjust();
    _large_data_handler->start();
    // We need the compaction manager ready early so we can reshard.
    _compaction_manager.enable();
    co_await init_commitlog();
}

future<> database::shutdown() {
    _shutdown = true;
    auto b = defer([this] { _stop_barrier.abort(); });
    co_await _stop_barrier.arrive_and_wait();
    b.cancel();

    // stop compaction across all shards before closing tables
    co_await _compaction_manager.drain();
    co_await _stop_barrier.arrive_and_wait();

    // Closing a table can cause us to find a large partition. Since we want to record that, we have to close
    // system.large_partitions after the regular tables.
    co_await close_tables(database::table_kind::user);
    co_await close_tables(database::table_kind::system);
    co_await _large_data_handler->stop();
    // Don't shutdown the keyspaces just yet,
    // since they are needed during shutdown.
    // FIXME: restore when https://github.com/scylladb/scylla/issues/8995
    // is fixed and no queries are issued after the database shuts down.
    // (see also https://github.com/scylladb/scylla/issues/9684)
    // for (auto& [ks_name, ks] : _keyspaces) {
    //     co_await ks.shutdown();
    // }
}

future<> database::stop() {
    if (_unsubscribe_qos_configuration_change) {
        co_await std::exchange(_unsubscribe_qos_configuration_change, {})();
    }
    if (!_shutdown) {
        co_await shutdown();
    }
    // try to ensure that CL has done disk flushing
    if (_commitlog) {
        dblog.info("Shutting down commitlog");
        co_await _commitlog->shutdown();
        dblog.info("Shutting down commitlog complete");
    }
    if (_schema_commitlog) {
        dblog.info("Shutting down schema commitlog");
        co_await _schema_commitlog->shutdown();
        dblog.info("Shutting down schema commitlog complete");
    }
    co_await _view_update_concurrency_sem.wait(max_memory_pending_view_updates());
    if (_commitlog) {
        co_await _commitlog->release();
    }
    if (_schema_commitlog) {
        co_await _schema_commitlog->release();
    }
    dblog.info("Shutting down system dirty memory manager");
    co_await _system_dirty_memory_manager.shutdown();
    dblog.info("Shutting down dirty memory manager");
    co_await _dirty_memory_manager.shutdown();
    dblog.info("Shutting down memtable controller");
    co_await _memtable_controller.shutdown();
    dblog.info("Stopping querier cache");
    co_await _querier_cache.stop();
    dblog.info("Closing user sstables manager");
    co_await _user_sstables_manager->close();
    dblog.info("Closing system sstables manager");
    co_await _system_sstables_manager->close();
    dblog.info("Stopping concurrency semaphores");
    co_await _reader_concurrency_semaphores_group.stop();
    co_await _view_update_read_concurrency_semaphores_group.stop();
    co_await _streaming_concurrency_sem.stop();
    co_await _compaction_concurrency_sem.stop();
    co_await _system_read_concurrency_sem.stop();
    dblog.info("Joining memtable update action");
    co_await _update_memtable_flush_static_shares_action.join();
}

future<> database::flush_all_memtables() {
    return _tables_metadata.parallel_for_each_table([] (table_id, lw_shared_ptr<table> table) {
        return table->flush();
    });
}

future<> database::flush(const sstring& ksname, const sstring& cfname) {
    auto& cf = find_column_family(ksname, cfname);
    return cf.flush();
}

future<> database::flush_table_on_all_shards(sharded<database>& sharded_db, table_id id) {
    return sharded_db.invoke_on_all([id] (replica::database& db) {
        if (db.column_family_exists(id)) {
            return db.find_column_family(id).flush();
        }
        return make_ready_future();
    });
}

future<> database::drop_cache_for_table_on_all_shards(sharded<database>& sharded_db, table_id id) {
    return sharded_db.invoke_on_all([id] (replica::database& db) {
        return db.find_column_family(id).get_row_cache().invalidate(row_cache::external_updater([] {}));
    });
}

future<> database::flush_table_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, std::string_view table_name) {
    return flush_table_on_all_shards(sharded_db, sharded_db.local().find_uuid(ks_name, table_name));
}

static future<> force_new_commitlog_segments(std::unique_ptr<db::commitlog>& cl1, std::unique_ptr<db::commitlog>& cl2) {
    co_await cl1->force_new_active_segment();
    if (cl2) {
        co_await cl2->force_new_active_segment();
    }
}

future<> database::flush_tables_on_all_shards(sharded<database>& sharded_db, std::vector<table_info> tables) {
    /**
     * #14870 
     * To ensure tests which use nodetool flush to force data
     * to sstables and do things post this get what they expect,
     * we do an extra call here and below, asking commitlog
     * to discard the currently active segment, This ensures we get 
     * as sstable-ish a universe as we can, as soon as we can.
    */
    if (utils::get_local_injector().enter("flush_tables_on_all_shards_table_drop")) {
        tables.push_back(table_info{});
    }
    return sharded_db.invoke_on_all([] (replica::database& db) {
        return force_new_commitlog_segments(db._commitlog, db._schema_commitlog);
    }).then([&, tables = std::move(tables)] {
        return parallel_for_each(tables, [&] (const auto& ti) {
            return flush_table_on_all_shards(sharded_db, ti.id);
        });
    });
}

future<> database::flush_keyspace_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name) {
    // see above
    return sharded_db.invoke_on_all([] (replica::database& db) {
        return force_new_commitlog_segments(db._commitlog, db._schema_commitlog);
    }).then([&, ks_name] {
        auto& ks = sharded_db.local().find_keyspace(ks_name);
        return parallel_for_each(ks.metadata()->cf_meta_data(), [&] (auto& pair) {
            return flush_table_on_all_shards(sharded_db, pair.second->id());
        });
    });
}

future<> database::flush_all_tables() {
    // see above
    dblog.info("Forcing new commitlog segment and flushing all tables");
    co_await _commitlog->force_new_active_segment();
    co_await get_tables_metadata().parallel_for_each_table([] (table_id, lw_shared_ptr<table> t) {
        return t->flush();
    });
    _all_tables_flushed_at = db_clock::now();
    co_await _commitlog->wait_for_pending_deletes();
}

future<db_clock::time_point> database::get_all_tables_flushed_at(sharded<database>& sharded_db) {
    return sharded_db.map_reduce0([&] (const database& db) {
        return db._all_tables_flushed_at;
    }, db_clock::now(), [] (db_clock::time_point l, db_clock::time_point r) {
        return std::min(l, r);
    });
}

future<> database::drop_cache_for_keyspace_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name) {
    auto& ks = sharded_db.local().find_keyspace(ks_name);
    return parallel_for_each(ks.metadata()->cf_meta_data(), [&] (auto& pair) {
        return drop_cache_for_table_on_all_shards(sharded_db, pair.second->id());
    });
}

future<> database::snapshot_table_on_all_shards(sharded<database>& sharded_db, table_id uuid, sstring tag, bool skip_flush) {
    if (!skip_flush) {
        co_await flush_table_on_all_shards(sharded_db, uuid);
    }
    auto table_shards = co_await get_table_on_all_shards(sharded_db, uuid);
    co_await table::snapshot_on_all_shards(sharded_db, table_shards, tag);
}

future<> database::snapshot_tables_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, std::vector<sstring> table_names, sstring tag, bool skip_flush) {
    return parallel_for_each(table_names, [&sharded_db, ks_name, tag = std::move(tag), skip_flush] (auto& table_name) {
        auto uuid = sharded_db.local().find_uuid(ks_name, table_name);
        return snapshot_table_on_all_shards(sharded_db, uuid, tag, skip_flush);
    });
}

future<> database::snapshot_keyspace_on_all_shards(sharded<database>& sharded_db, std::string_view ks_name, sstring tag, bool skip_flush) {
    auto& ks = sharded_db.local().find_keyspace(ks_name);
    co_await coroutine::parallel_for_each(ks.metadata()->cf_meta_data(), [&, tag = std::move(tag), skip_flush] (const auto& pair) -> future<> {
        auto uuid = pair.second->id();
        co_await snapshot_table_on_all_shards(sharded_db, uuid, tag, skip_flush);
    });
}

future<> database::truncate_table_on_all_shards(sharded<database>& sharded_db, sharded<db::system_keyspace>& sys_ks,
        sstring ks_name, sstring cf_name, std::optional<db_clock::time_point> truncated_at_opt, bool with_snapshot, std::optional<sstring> snapshot_name_opt) {
    auto uuid = sharded_db.local().find_uuid(ks_name, cf_name);
    auto table_shards = co_await get_table_on_all_shards(sharded_db, uuid);
    co_return co_await truncate_table_on_all_shards(sharded_db, sys_ks, table_shards, truncated_at_opt, with_snapshot, std::move(snapshot_name_opt));
}

struct database::table_truncate_state {
    gate::holder holder;
    // This RP mark accounts for all data (includes memtable) generated until truncated_at.
    db::replay_position low_mark;
    db_clock::time_point truncated_at;
    std::vector<compaction_manager::compaction_reenabler> cres;
    bool did_flush;
};

future<> database::truncate_table_on_all_shards(sharded<database>& sharded_db, sharded<db::system_keyspace>& sys_ks,
        const global_table_ptr& table_shards, std::optional<db_clock::time_point> truncated_at_opt, bool with_snapshot, std::optional<sstring> snapshot_name_opt) {
    auto& cf = *table_shards;
    auto s = cf.schema();

    // Schema tables changed commitlog domain at some point and this node will refuse to boot with
    // truncation record present for schema tables to protect against misinterpreting of replay positions.
    // Also, the replay_position returned by discard_sstables() may refer to old commit log domain.
    if (s->ks_name() == db::schema_tables::NAME) {
        throw std::runtime_error(format("Truncating of {}.{} is not allowed.", s->ks_name(), s->cf_name()));
    }

    if (!sharded_db.local().get_config().auto_snapshot()) {
        with_snapshot = false;
    }
    if (with_snapshot && !table_shards->get_storage_options().is_local_type()) {
        dblog.warn("Not snapshotting dropped/truncated table {}.{} despite auto_snapshot=true - table is not using local disk", s->ks_name(), s->cf_name());
        with_snapshot = false;
    }

    dblog.info("Truncating {}.{} {}snapshot", s->ks_name(), s->cf_name(), with_snapshot ? "with auto-" : "without ");

    std::vector<foreign_ptr<std::unique_ptr<table_truncate_state>>> table_states;
    table_states.resize(smp::count);

    co_await coroutine::parallel_for_each(std::views::iota(0u, smp::count), [&] (unsigned shard) -> future<> {
        table_states[shard] = co_await smp::submit_to(shard, [&] () -> future<foreign_ptr<std::unique_ptr<table_truncate_state>>> {
            auto& cf = *table_shards;
            auto st = std::make_unique<table_truncate_state>();

            st->holder = cf.async_gate().hold();

            st->cres.reserve(1 + cf.views().size());
            auto& db = sharded_db.local();
            auto& cm = db.get_compaction_manager();
            co_await cf.parallel_foreach_table_state([&cm, &st] (compaction::table_state& ts) -> future<> {
                st->cres.emplace_back(co_await cm.stop_and_disable_compaction(ts));
            });
            co_await coroutine::parallel_for_each(cf.views(), [&] (view_ptr v) -> future<> {
                auto& vcf = db.find_column_family(v);
                co_await vcf.parallel_foreach_table_state([&cm, &st] (compaction::table_state& ts) -> future<> {
                    st->cres.emplace_back(co_await cm.stop_and_disable_compaction(ts));
                });
            });

            co_return make_foreign(std::move(st));
        });
    });

    co_await utils::get_local_injector().inject("truncate_compaction_disabled_wait", [] (auto& handler) -> future<> {
        dblog.info("truncate_compaction_disabled_wait: wait");
        co_await handler.wait_for_message(std::chrono::steady_clock::now() + std::chrono::minutes{5});
        dblog.info("truncate_compaction_disabled_wait: done");
    }, false);

    const auto should_flush = with_snapshot && cf.can_flush();
    dblog.trace("{} {}.{} and views on all shards", should_flush ? "Flushing" : "Clearing", s->ks_name(), s->cf_name());
    std::function<future<>(replica::table&)> flush_or_clear = should_flush ?
            [] (replica::table& cf) {
                // TODO:
                // this is not really a guarantee at all that we've actually
                // gotten all things to disk. Again, need queue-ish or something.
                return cf.flush();
            } :
            [] (replica::table& cf) {
                return cf.clear();
            };
    co_await sharded_db.invoke_on_all([&] (replica::database& db) -> future<> {
        unsigned shard = this_shard_id();
        auto& cf = *table_shards;
        auto& st = *table_states[shard];

        // Force mutations coming in to re-acquire higher rp:s
        // This creates a "soft" ordering, in that we will guarantee that
        // any sstable written _after_ we issue the flush below will
        // only have higher rp:s than we will get from the discard_sstables
        // call.
        st.low_mark = cf.set_low_replay_position_mark();

        co_await flush_or_clear(cf);

        co_await coroutine::parallel_for_each(cf.views(), [&] (view_ptr v) -> future<> {
            auto& vcf = db.find_column_family(v);
            co_await flush_or_clear(vcf);
        });
        // Since writes could be appended to active memtable between getting low_mark above
        // and flush, the low_mark has to be adjusted to account for those writes, where
        // memtable was flushed with a higher replay position than the one obtained above.
        st.low_mark = std::max(st.low_mark, cf.highest_flushed_replay_position());
        // truncated_at is a time point that describes both the truncation time, and also
        // serves as a filter, where a sstable is only filtered in if it was created before
        // the truncated_at. The reason for saving it right after flush, is to prevent a
        // sstable created after we're done here in this shard from being included, since
        // different shards might have different pace.
        st.truncated_at = truncated_at_opt.value_or(db_clock::now());
        st.did_flush = should_flush;
    });
    co_await utils::get_local_injector().inject("database_truncate_wait", utils::wait_for_message(1min));

    if (with_snapshot) {
        auto truncated_at = truncated_at_opt.value_or(db_clock::now());
        auto name = snapshot_name_opt.value_or(
            format("{:d}-{}", truncated_at.time_since_epoch().count(), cf.schema()->cf_name()));
        co_await table::snapshot_on_all_shards(sharded_db, table_shards, name);
    }

    co_await sharded_db.invoke_on_all([&] (database& db) {
        auto shard = this_shard_id();
        auto& cf = *table_shards;
        auto& st = *table_states[shard];

        return db.truncate(sys_ks.local(), cf, st);
    });
    dblog.info("Truncated {}.{}", s->ks_name(), s->cf_name());
}

future<> database::truncate(db::system_keyspace& sys_ks, column_family& cf, const table_truncate_state& st) {
    dblog.trace("Truncating {}.{} on shard", cf.schema()->ks_name(), cf.schema()->cf_name());

    const auto uuid = cf.schema()->id();
    const auto truncated_at = st.truncated_at;

    dblog.debug("Discarding sstable data for truncated CF + indexes");
    // TODO: notify truncation

    db::replay_position rp = co_await cf.discard_sstables(truncated_at);
    // TODO: indexes.
    // Note: since discard_sstables was changed to only count tables owned by this shard,
    // we can get zero rp back. Changed SCYLLA_ASSERT, and ensure we save at least low_mark.
    // #6995 - the SCYLLA_ASSERT below was broken in c2c6c71 and remained so for many years. 
    // We nowadays do not flush tables with sstables but autosnapshot=false. This means
    // the low_mark assertion does not hold, because we maybe/probably never got around to 
    // creating the sstables that would create them.
    //
    // What we want to assert is that only data generated until truncation time was included,
    // since we don't want to leave behind data on disk with RP lower than the one we set
    // in the truncation table.
    if (st.did_flush && rp != db::replay_position() && st.low_mark < rp) {
        on_internal_error(dblog, "Data written after truncation time was incorrectly truncated. Truncate is known to not work well with concurrent writes. Retry!");
    }
    if (rp == db::replay_position()) {
        // If this shard had no mutations, st.low_mark will be an empty, default constructed
        // replay_position. This is a problem because an empty replay_position has the shard_id
        // part of segment_id set to 0, even though we may be running on a shard other than
        // shard 0. In that case, we will save the empty low_mark as a replay position into
        // system.truncated with an incorrect shard number, which could overwrite the replay
        // position of the actual shard 0. So, we fix the problem by creating a replay position
        // with the correct shard_id and 0 for base_id and position
        if (st.low_mark == db::replay_position()) {
            rp = db::replay_position(this_shard_id(), 0, 0);
        } else {
            rp = st.low_mark;
        }
    }
    co_await coroutine::parallel_for_each(cf.views(), [this, &sys_ks, truncated_at] (view_ptr v) -> future<> {
        auto& vcf = find_column_family(v);
            db::replay_position rp = co_await vcf.discard_sstables(truncated_at);
            co_await sys_ks.save_truncation_record(vcf, truncated_at, rp);
    });
    // save_truncation_record() may actually fail after we cached the truncation time
    // but this is not be worse that if failing without caching: at least the correct time
    // will be available until next reboot and a client will have to retry truncation anyway.
    cf.set_truncation_time(truncated_at);
    co_await sys_ks.save_truncation_record(cf, truncated_at, rp);

    auto& gc_state = get_compaction_manager().get_tombstone_gc_state();
    gc_state.drop_repair_history_for_table(uuid);
}

const sstring& database::get_snitch_name() const {
    return _cfg.endpoint_snitch();
}

future<dht::token_range_vector> database::get_keyspace_local_ranges(locator::vnode_effective_replication_map_ptr erm) {
    co_return co_await erm->get_ranges(erm->get_topology().my_host_id());
}

/*!
 * \brief a helper function that gets a table name and returns a prefix
 * of the directory name of the table.
 */
static sstring get_snapshot_table_dir_prefix(const sstring& table_name) {
    return table_name + "-";
}

std::pair<sstring, table_id> parse_table_directory_name(const sstring& directory_name) {
    // cf directory is of the form: 'cf_name-uuid'
    // uuid is assumed to be exactly 32 hex characters wide.
    constexpr size_t uuid_size = 32;
    ssize_t pos = directory_name.size() - uuid_size - 1;
    if (pos <= 0 || directory_name[pos] != '-') {
        on_internal_error(dblog, format("table directory entry name '{}' is invalid: no '-' separator found at pos {}", directory_name, pos));
    }
    return std::make_pair(directory_name.substr(0, pos), table_id(utils::UUID(directory_name.substr(pos + 1))));
}

future<std::unordered_map<sstring, database::snapshot_details>> database::get_snapshot_details() {
    std::vector<sstring> data_dirs = _cfg.data_file_directories();
    std::unordered_map<sstring, snapshot_details> details;

    for (auto& datadir : data_dirs) {
        co_await lister::scan_dir(fs::path{datadir}, lister::dir_entry_types::of<directory_entry_type::directory>(), [&details] (fs::path parent_dir, directory_entry de) -> future<> {
            // KS directory
            sstring ks_name = de.name;

            co_return co_await lister::scan_dir(parent_dir / de.name, lister::dir_entry_types::of<directory_entry_type::directory>(), [&details, ks_name = std::move(ks_name)] (fs::path parent_dir, directory_entry de) -> future<> {
                // CF directory
                auto cf_dir = parent_dir / de.name;

                // Skip tables with no snapshots.
                // Also, skips non-keyspace parent_dir (e.g. commitlog or view_hints directories)
                // that may also be present under the data directory alongside keyspaces
                if (!co_await file_exists((cf_dir / sstables::snapshots_dir).native())) {
                    co_return;
                }

                auto cf_name_and_uuid = parse_table_directory_name(de.name);
                co_return co_await lister::scan_dir(cf_dir / sstables::snapshots_dir, lister::dir_entry_types::of<directory_entry_type::directory>(), [&details, &ks_name, &cf_name = cf_name_and_uuid.first, &cf_dir] (fs::path parent_dir, directory_entry de) -> future<> {
                    auto snapshot_name = de.name;
                    auto cf_details = co_await table::get_snapshot_details(parent_dir / snapshot_name, cf_dir);
                    details[snapshot_name].emplace_back(ks_name, cf_name, std::move(cf_details));
                });
            });
        });
    }

    co_return details;
}

// For the filesystem operations, this code will assume that all keyspaces are visible in all shards
// (as we have been doing for a lot of the other operations, like the snapshot itself).
future<> database::clear_snapshot(sstring tag, std::vector<sstring> keyspace_names, const sstring& table_name) {
    std::vector<sstring> data_dirs = _cfg.data_file_directories();
    std::unordered_set<sstring> ks_names_set(keyspace_names.begin(), keyspace_names.end());
    auto table_name_param = table_name;

    // if specific keyspaces names were given - filter only these keyspaces directories
    auto filter = ks_names_set.empty()
            ? lister::filter_type([] (const fs::path&, const directory_entry&) { return true; })
            : lister::filter_type([&] (const fs::path&, const directory_entry& dir_entry) {
                return ks_names_set.contains(dir_entry.name);
            });

    // if specific table name was given - filter only these table directories
    auto table_filter = table_name.empty()
            ? lister::filter_type([] (const fs::path&, const directory_entry& dir_entry) { return true; })
            : lister::filter_type([table_name = get_snapshot_table_dir_prefix(table_name)] (const fs::path&, const directory_entry& dir_entry) {
                return dir_entry.name.find(table_name) == 0;
            });

    co_await coroutine::parallel_for_each(data_dirs, [&, this] (const sstring& parent_dir) {
        return async([&] {
            //
            // The keyspace data directories and their snapshots are arranged as follows:
            //
            //  <data dir>
            //  |- <keyspace name1>
            //  |  |- <column family name1>
            //  |     |- snapshots
            //  |        |- <snapshot name1>
            //  |          |- <snapshot file1>
            //  |          |- <snapshot file2>
            //  |          |- ...
            //  |        |- <snapshot name2>
            //  |        |- ...
            //  |  |- <column family name2>
            //  |  |- ...
            //  |- <keyspace name2>
            //  |- ...
            //
            auto data_dir = fs::path(parent_dir);
            auto data_dir_lister = directory_lister(data_dir, lister::dir_entry_types::of<directory_entry_type::directory>(), filter);
            auto close_data_dir_lister = deferred_close(data_dir_lister);
            dblog.debug("clear_snapshot: listing data dir {} with filter={}", data_dir, ks_names_set.empty() ? "none" : fmt::format("{}", ks_names_set));
            while (auto ks_ent = data_dir_lister.get().get()) {
                auto ks_name = ks_ent->name;
                auto ks_dir = data_dir / ks_name;
                auto ks_dir_lister = directory_lister(ks_dir, lister::dir_entry_types::of<directory_entry_type::directory>(), table_filter);
                auto close_ks_dir_lister = deferred_close(ks_dir_lister);
                dblog.debug("clear_snapshot: listing keyspace dir {} with filter={}", ks_dir, table_name_param.empty() ? "none" : fmt::format("{}", table_name_param));
                while (auto table_ent = ks_dir_lister.get().get()) {
                    auto table_dir = ks_dir / table_ent->name;
                    auto snapshots_dir = table_dir / sstables::snapshots_dir;
                    auto has_snapshots = file_exists(snapshots_dir.native()).get();
                    if (has_snapshots) {
                        if (tag.empty()) {
                            dblog.info("Removing {}", snapshots_dir);
                            recursive_remove_directory(std::move(snapshots_dir)).get();
                            has_snapshots = false;
                        } else {
                            // if specific snapshots tags were given - filter only these snapshot directories
                            auto snapshots_dir_lister = directory_lister(snapshots_dir, lister::dir_entry_types::of<directory_entry_type::directory>());
                            auto close_snapshots_dir_lister = deferred_close(snapshots_dir_lister);
                            dblog.debug("clear_snapshot: listing snapshots dir {} with filter={}", snapshots_dir, tag);
                            has_snapshots = false;  // unless other snapshots are found
                            while (auto snapshot_ent = snapshots_dir_lister.get().get()) {
                                if (snapshot_ent->name == tag) {
                                    auto snapshot_dir = snapshots_dir / snapshot_ent->name;
                                    dblog.info("Removing {}", snapshot_dir);
                                    recursive_remove_directory(std::move(snapshot_dir)).get();
                                } else {
                                    has_snapshots = true;
                                }
                            }
                        }
                    } else {
                        dblog.debug("clear_snapshot: {} not found", snapshots_dir);
                    }
                    // zap the table directory if the table is dropped
                    // and has no remaining snapshots
                    if (!has_snapshots) {
                        auto [cf_name, cf_uuid] = parse_table_directory_name(table_ent->name);
                        auto id_opt = _tables_metadata.get_table_id_if_exists(std::make_pair(ks_name, cf_name));
                        auto dropped = !id_opt || (cf_uuid != id_opt);
                        if (dropped) {
                            dblog.info("Removing dropped table dir {}", table_dir);
                            sstables::remove_table_directory_if_has_no_snapshots(table_dir).get();
                        }
                    }
                }
            }
        });
    });
}

future<> database::flush_non_system_column_families() {
    auto non_system_cfs = get_tables_metadata().filter([this] (auto uuid_and_cf) {
        auto cf = uuid_and_cf.second;
        auto& ks = cf->schema()->ks_name();
        return !is_system_keyspace(ks) && !_cfg.extensions().is_extension_internal_keyspace(ks);
    });
    // count CFs first
    auto total_cfs = std::ranges::distance(non_system_cfs);
    _drain_progress.total_cfs = total_cfs;
    _drain_progress.remaining_cfs = total_cfs;
    // flush
    dblog.info("Flushing non-system tables");
    return parallel_for_each(non_system_cfs, [this] (auto&& uuid_and_cf) {
        auto cf = uuid_and_cf.second;
        return cf->flush().then([this] {
            _drain_progress.remaining_cfs--;
        });
    }).finally([] {
        dblog.info("Flushed non-system tables");
    });
}

future<> database::flush_system_column_families() {
    auto system_cfs = get_tables_metadata().filter([this] (auto uuid_and_cf) {
        auto cf = uuid_and_cf.second;
        auto& ks = cf->schema()->ks_name();
        return is_system_keyspace(ks) || _cfg.extensions().is_extension_internal_keyspace(ks);
    });
    dblog.info("Flushing system tables");
    return parallel_for_each(system_cfs, [] (auto&& uuid_and_cf) {
        auto cf = uuid_and_cf.second;
        return cf->flush();
    }).finally([] {
        dblog.info("Flushed system tables");
    });
}

future<> database::drain() {
    auto b = defer([this] { _stop_barrier.abort(); });
    // Interrupt on going compaction and shutdown to prevent further compaction
    co_await _compaction_manager.drain();

    // flush the system ones after all the rest are done, just in case flushing modifies any system state
    // like CASSANDRA-5151. don't bother with progress tracking since system data is tiny.
    co_await _stop_barrier.arrive_and_wait();
    co_await flush_non_system_column_families();
    co_await _stop_barrier.arrive_and_wait();
    co_await flush_system_column_families();
    co_await _stop_barrier.arrive_and_wait();
    co_await _commitlog->shutdown();
    if (_schema_commitlog) {
        co_await _schema_commitlog->shutdown();
    }
    b.cancel();
}

void database::tables_metadata::add_table_helper(database& db, keyspace& ks, table& cf, schema_ptr s) {
    // A table needs to be added atomically.
    auto id = s->id();
    ks.add_or_update_column_family(s);
    auto remove_cf1 = defer([&] () noexcept { ks.metadata()->remove_column_family(s); });
    // A table will be removed via weak pointer and destructors.
    s->registry_entry()->set_table(cf.weak_from_this());

    _column_families.emplace(id, s->table().shared_from_this());
    auto remove_cf2 = defer([&] () noexcept {
        _column_families.erase(s->id());
    });
    _ks_cf_to_uuid.emplace(std::make_pair(s->ks_name(), s->cf_name()), id);
    auto remove_cf3 = defer([&] () noexcept {
        _ks_cf_to_uuid.erase(std::make_pair(s->ks_name(), s->cf_name()));
    });

    if (s->is_view()) {
        db.find_column_family(s->view_info()->base_id()).add_or_update_view(view_ptr(s));
    }
    auto remove_view = defer([&] () noexcept {
        if (s->is_view()) {
            try {
                db.find_column_family(s->view_info()->base_id()).remove_view(view_ptr(s));
            } catch (no_such_column_family&) {
                // Drop view mutations received after base table drop.
            }
        }
    });

    remove_cf1.cancel();
    remove_cf2.cancel();
    remove_cf3.cancel();
    remove_view.cancel();
}

void database::tables_metadata::remove_table_helper(database& db, keyspace& ks, table& cf, schema_ptr s) {
    // A table needs to be removed atomically.
    _column_families.erase(s->id());
    _ks_cf_to_uuid.erase(std::make_pair(s->ks_name(), s->cf_name()));
    ks.metadata()->remove_column_family(s);
    if (s->is_view()) {
        try {
            db.find_column_family(s->view_info()->base_id()).remove_view(view_ptr(s));
        } catch (no_such_column_family&) {
            // Drop view mutations received after base table drop.
        }
    }
}

size_t database::tables_metadata::size() const noexcept {
    return _column_families.size();
}

future<> database::tables_metadata::add_table(database& db, keyspace& ks, table& cf, schema_ptr s) {
    auto holder = co_await _cf_lock.hold_write_lock();
    add_table_helper(db, ks, cf, s);
}

future<> database::tables_metadata::remove_table(database& db, table& cf) noexcept {
    try {
        auto holder = co_await _cf_lock.hold_write_lock();
        auto s = cf.schema();
        auto& ks = db.find_keyspace(s->ks_name());
        remove_table_helper(db, ks, cf, s);
    } catch (...) {
        on_fatal_internal_error(dblog, format("tables_metadata::remove_cf: {}", std::current_exception()));
    }
}

table& database::tables_metadata::get_table(table_id id) const {
    return *_column_families.at(id);
}

table_id database::tables_metadata::get_table_id(const std::pair<std::string_view, std::string_view>& kscf) const {
    return _ks_cf_to_uuid.at(kscf);
}

lw_shared_ptr<table> database::tables_metadata::get_table_if_exists(table_id id) const {
    if (auto it = _column_families.find(id); it != _column_families.end()) {
        return it->second;
    }
    return nullptr;
}

table_id database::tables_metadata::get_table_id_if_exists(const std::pair<std::string_view, std::string_view>& kscf) const {
    if (auto it = _ks_cf_to_uuid.find(kscf); it != _ks_cf_to_uuid.end()) {
        return it->second;
    }
    return table_id::create_null_id();
}

bool database::tables_metadata::contains(table_id id) const {
    return _column_families.contains(id);
}

bool database::tables_metadata::contains(std::pair<std::string_view, std::string_view> kscf) const {
    return _ks_cf_to_uuid.contains(kscf);
}

void database::tables_metadata::for_each_table(std::function<void(table_id, lw_shared_ptr<table>)> f) const {
    for (auto& [id, table]: _column_families) {
        f(id, table);
    }
}

void database::tables_metadata::for_each_table_id(std::function<void(const ks_cf_t&, table_id)> f) const {
    for (auto& [kscf, id]: _ks_cf_to_uuid) {
        f(kscf, id);
    }
}

future<> database::tables_metadata::for_each_table_gently(std::function<future<>(table_id, lw_shared_ptr<table>)> f) {
    auto holder = co_await _cf_lock.hold_read_lock();
    for (auto& [id, table]: _column_families) {
        co_await f(id, table);
    }
}

future<> database::tables_metadata::parallel_for_each_table(std::function<future<>(table_id, lw_shared_ptr<table>)> f) {
    auto holder = co_await _cf_lock.hold_read_lock();
    co_await coroutine::parallel_for_each(_column_families, [f = std::move(f)] (auto& table) {
        return f(table.first, table.second);
    });
}

const std::unordered_map<table_id, lw_shared_ptr<table>> database::tables_metadata::get_column_families_copy() const {
    return _column_families;
}

data_dictionary::database
database::as_data_dictionary() const {
    static constinit data_dictionary_impl _impl;
    return _impl.wrap(*this);
}

void database::plug_system_keyspace(db::system_keyspace& sys_ks) noexcept {
    _compaction_manager.plug_system_keyspace(sys_ks);
    _large_data_handler->plug_system_keyspace(sys_ks);
    _corrupt_data_handler->plug_system_keyspace(sys_ks);
    _user_sstables_manager->plug_sstables_registry(std::make_unique<db::system_keyspace_sstables_registry>(sys_ks));
}

future<> database::unplug_system_keyspace() noexcept {
    _user_sstables_manager->unplug_sstables_registry();
    co_await _compaction_manager.unplug_system_keyspace();
    co_await _large_data_handler->unplug_system_keyspace();
    co_await _corrupt_data_handler->unplug_system_keyspace();
}

void database::plug_view_update_generator(db::view::view_update_generator& generator) noexcept {
    _view_update_generator = generator.shared_from_this();
}

void database::unplug_view_update_generator() noexcept {
    _view_update_generator = nullptr;
}

} // namespace replica

mutation_reader make_multishard_streaming_reader(distributed<replica::database>& db,
        schema_ptr schema, reader_permit permit,
        std::function<std::optional<dht::partition_range>()> range_generator,
        gc_clock::time_point compaction_time,
        std::optional<size_t> multishard_reader_buffer_size,
        read_ahead read_ahead) {

    auto& table = db.local().find_column_family(schema);
    auto erm = table.get_effective_replication_map();
    auto ms = mutation_source([&db, erm, compaction_time, multishard_reader_buffer_size, read_ahead] (schema_ptr s,
            reader_permit permit,
            const dht::partition_range& pr,
            const query::partition_slice& ps,
            tracing::trace_state_ptr trace_state,
            streamed_mutation::forwarding,
            mutation_reader::forwarding fwd_mr) {
        auto table_id = s->id();
        const auto buffer_hint = multishard_reader_buffer_hint(multishard_reader_buffer_size.has_value());
        auto rd = make_multishard_combining_reader(seastar::make_shared<streaming_reader_lifecycle_policy>(db, table_id, compaction_time),
                std::move(s), erm, std::move(permit), pr, ps, std::move(trace_state), fwd_mr, buffer_hint, read_ahead);
        if (multishard_reader_buffer_size) {
            rd.set_max_buffer_size(*multishard_reader_buffer_size);
        }
        return rd;
    });
    auto&& full_slice = schema->full_slice();
    return make_multi_range_reader(schema, std::move(permit), std::move(ms),
            std::move(range_generator), std::move(full_slice), {}, mutation_reader::forwarding::no);
}

mutation_reader make_multishard_streaming_reader(distributed<replica::database>& db,
        schema_ptr schema,
        reader_permit permit,
        const dht::partition_range& range,
        gc_clock::time_point compaction_time,
        std::optional<size_t> multishard_reader_buffer_size,
        read_ahead read_ahead)
{
    const auto table_id = schema->id();
    const auto& full_slice = schema->full_slice();
    auto erm = db.local().find_column_family(schema).get_effective_replication_map();
    auto rd = make_multishard_combining_reader(
        seastar::make_shared<streaming_reader_lifecycle_policy>(db, table_id, compaction_time),
        std::move(schema),
        std::move(erm),
        std::move(permit),
        range,
        full_slice,
        {},
        mutation_reader::forwarding::no,
        multishard_reader_buffer_hint(multishard_reader_buffer_size.has_value()),
        read_ahead);
    if (multishard_reader_buffer_size) {
        rd.set_max_buffer_size(*multishard_reader_buffer_size);
    }
    return rd;
}

auto fmt::formatter<gc_clock::time_point>::format(gc_clock::time_point tp, fmt::format_context& ctx) const
    -> decltype(ctx.out()) {
    auto sec = std::chrono::duration_cast<std::chrono::seconds>(tp.time_since_epoch()).count();
    return fmt::format_to(ctx.out(), "{:>12}", sec);
}

const timeout_config infinite_timeout_config = {
        // not really infinite, but long enough
        1h, 1h, 1h, 1h, 1h, 1h, 1h,
};

namespace replica {

future<foreign_ptr<lw_shared_ptr<reconcilable_result>>> query_mutations(
        sharded<database>& db,
        schema_ptr s,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        db::timeout_clock::time_point timeout) {
    auto max_res_size = db.local().get_query_max_result_size();
    auto cmd = query::read_command(s->id(), s->version(), ps, max_res_size, query::tombstone_limit::max);
    auto erm = s->table().get_effective_replication_map();
    if (auto shard_opt = dht::is_single_shard(erm->get_sharder(*s), *s, pr)) {
        auto shard = *shard_opt;
        co_return co_await db.invoke_on(shard, [gs = global_schema_ptr(s), &cmd, &pr, timeout] (replica::database& db) mutable {
            return db.query_mutations(gs, cmd, pr, {}, timeout).then([] (std::tuple<reconcilable_result, cache_temperature>&& res) {
                return make_foreign(make_lw_shared<reconcilable_result>(std::move(std::get<0>(res))));
            });
        });
    } else {
        auto prs = dht::partition_range_vector{pr};
        auto&& [res, _] = co_await query_mutations_on_all_shards(db, std::move(s), cmd, prs, {}, timeout);
        co_return std::move(res);
    }
}

future<foreign_ptr<lw_shared_ptr<query::result>>> query_data(
        sharded<database>& db,
        schema_ptr s,
        const dht::partition_range& pr,
        const query::partition_slice& ps,
        db::timeout_clock::time_point timeout) {
    auto max_res_size = db.local().get_query_max_result_size();
    auto cmd = query::read_command(s->id(), s->version(), ps, max_res_size, query::tombstone_limit::max);
    auto prs = dht::partition_range_vector{pr};
    auto opts = query::result_options::only_result();
    auto erm = s->table().get_effective_replication_map();
    if (auto shard_opt = dht::is_single_shard(erm->get_sharder(*s), *s, pr)) {
        auto shard = *shard_opt;
        co_return co_await db.invoke_on(shard, [gs = global_schema_ptr(s), &cmd, opts, &prs, timeout] (replica::database& db) mutable {
            return db.query(gs, cmd, opts, prs, {}, timeout).then([] (std::tuple<lw_shared_ptr<query::result>, cache_temperature>&& res) {
                return make_foreign(std::move(std::get<0>(res)));
            });
        });
    } else {
        auto&& [res, _] = co_await query_data_on_all_shards(db, std::move(s), cmd, prs, opts, {}, timeout);
        co_return std::move(res);
    }
}

} // namespace replica

namespace replica {

/** This callback is going to be called just before the service level is available **/
future<> database::on_before_service_level_add(qos::service_level_options slo, qos::service_level_info sl_info) {
    if (auto shares_p = std::get_if<int32_t>(&slo.shares)) {
        _reader_concurrency_semaphores_group.add_or_update(sl_info.sg, *shares_p);
        _view_update_read_concurrency_semaphores_group.add_or_update(sl_info.sg, *shares_p);
        // the call to add_or_update_read_concurrency_sem will take the semaphore until the adjustment
        // is completed, we need to wait for the operation to complete.
        co_await _reader_concurrency_semaphores_group.wait_adjust_complete();
        co_await _view_update_read_concurrency_semaphores_group.wait_adjust_complete();
    }
}
/** This callback is going to be called just after the service level is removed **/
future<> database::on_after_service_level_remove(qos::service_level_info sl_info) {
    co_await _reader_concurrency_semaphores_group.remove(sl_info.sg);
    co_await _view_update_read_concurrency_semaphores_group.remove(sl_info.sg);
}
/** This callback is going to be called just before the service level is changed **/
future<> database::on_before_service_level_change(qos::service_level_options slo_before, qos::service_level_options slo_after,
        qos::service_level_info sl_info) {
    if (auto shares_p = std::get_if<int32_t>(&slo_after.shares)) {
        _reader_concurrency_semaphores_group.add_or_update(sl_info.sg, *shares_p);
        _view_update_read_concurrency_semaphores_group.add_or_update(sl_info.sg, *shares_p);
        // the call to add_or_update_read_concurrency_sem will take the semaphore until the adjustment
        // is completed, we need to wait for the operation to complete.
        co_await _reader_concurrency_semaphores_group.wait_adjust_complete();
        co_await _view_update_read_concurrency_semaphores_group.wait_adjust_complete();
    }
}

future<>
database::on_effective_service_levels_cache_reloaded() {
    co_return;
}

void database::check_rf_rack_validity(const locator::token_metadata_ptr tmptr) const {
    SCYLLA_ASSERT(get_config().rf_rack_valid_keyspaces());

    for (const auto& [name, info] : get_keyspaces()) {
        locator::assert_rf_rack_valid_keyspace(name, tmptr, info.get_replication_strategy());
    }
}

utils::chunked_vector<uint64_t> compute_random_sorted_ints(uint64_t max_value, uint64_t n_values) {
    static thread_local std::minstd_rand rng{std::random_device{}()};
    std::uniform_int_distribution<uint64_t> dist(0, max_value);
    utils::chunked_vector<uint64_t> chosen;
    chosen.reserve(n_values);
    for (size_t i = 0; i < n_values; ++i) {
        chosen.push_back(dist(rng));
    }
    std::ranges::sort(chosen);
    return chosen;
}

// In this function, we imagine a global list of all Data.db file chunks
// (with size and alignment equal to `chunk_size`), sorted by <shard id; sstable index; offset within sstable>,
// and we "address" each chunk by its offset in this list.
// Then, we randomly select `n_chunks` of those chunks,
// and we let each shard fulfill the choices which belong to its files.
future<utils::chunked_vector<temporary_buffer<char>>> database::sample_data_files(
    table_id id,
    uint64_t chunk_size,
    uint64_t n_chunks
) {
    // If the volume of samples is bigger than the semaphore allows,
    // we still want to let the request in, so we clip the number of units to the semaphore's capacity.
    auto memory_consumption = std::min(chunk_size * n_chunks, _memory_for_data_file_samples);
    auto memory_units = co_await get_units(_sample_data_files_memory_limiter, memory_consumption);

    // Note: this shard owns the result's `temporary_buffer`s. Other shards only write to them.
    //
    // The returned buffers will hold the semaphore units until they are freed.
    auto result = utils::chunked_vector<temporary_buffer<char>>{};

    struct state_by_shard {
        // For sanity, we hold onto a stable snapshot of the sstable set throughout this function.
        utils::chunked_vector<sstables::shared_sstable> snapshot;
        // We need the schema for a semaphore permit, which is needed to perform a SSTable read.
        schema_ptr schema;
    };

    sharded<state_by_shard> state;
    co_await state.start();

    // We *must* call `state.stop()` before returning,
    // and we can't call it in a `defer`.
    // so we surround everything between `start()` and `stop()`
    // with a try..catch.
    std::exception_ptr ep;
    try {
        // After the `exclusive_scan` later, this will say which range of chunks
        // (in the global "list" of chunks) belongs to which shard.
        // (Shard X owns range `global_offset[X] .. global_offset[X + 1]`).
        std::vector<uint64_t> global_offset(smp::count + 1);

        // Watch out: static lambda. Don't add captures to it.
        static auto size_in_chunks = [] (const sstables::shared_sstable& sst, uint64_t chunk_size) {
            return sst->data_size() / chunk_size;
        };

        // Initialize `state` and `global_offset`.
        co_await container().invoke_on_all(coroutine::lambda([&global_offset, id, chunk_size] (
            replica::database& local_db,
            state_by_shard& local_state
        ) -> future<> {
            auto t = local_db.get_tables_metadata().get_table_if_exists(id);
            if (!t) {
                throw std::runtime_error(fmt::format("sample_data_files: table {} does not exist", id));
            }

            local_state.schema = t->schema();
            local_state.snapshot = co_await t->take_sstable_set_snapshot();

            uint64_t my_total_chunks = 0;
            for (const auto& sst : local_state.snapshot) {
                my_total_chunks += size_in_chunks(sst, chunk_size);
            }
            global_offset[this_shard_id()] = my_total_chunks;
        }), std::ref(state));

        // [1, 2, 3, 0] --> [0, 1, 3, 6]
        std::exclusive_scan(global_offset.begin(), global_offset.end(), global_offset.begin(), 0, std::plus());

        // We can't generate random non-negative integers smaller than 0,
        // so let's just deal with the `total_chunks == 0` case with an early return.
        const uint64_t total_chunks = global_offset.back();
        if (total_chunks == 0) {
            co_await state.stop();
            co_return utils::chunked_vector<temporary_buffer<char>>{};
        }

        // Generate `n_chunks` integers in the (inclusive) range [0, total_chunks - 1].
        const auto chosen_chunks = compute_random_sorted_ints(total_chunks - 1, n_chunks);

        // Allocate `n_chunks` output buffers of size `chunk_size`,
        // and tie semaphore units (the ones we obtained at the top of the function) to their memory.
        result.reserve(chosen_chunks.size());
        for (uint64_t i = 0; i < chosen_chunks.size(); ++i) {
            // Attach semaphore units to each sample.
            auto buf = temporary_buffer<char>(chunk_size);
            buf = temporary_buffer<char>(buf.get_write(), buf.size(),
                                         make_object_deleter(buf.release(), memory_units.split(chunk_size)));
            result.push_back(std::move(buf));
        }

        auto sample_one_shard = [
            &chosen_chunks = std::as_const(chosen_chunks),
            &global_offset = std::as_const(global_offset),
            &result,
            chunk_size
        ] (database& local_db, state_by_shard& local_state) -> future<> {
            auto ticket = get_units(local_db._sample_data_files_local_concurrency_limiter, 1);

            // In `chosen_chunks`, the sorted array of chosen chunk offsets (in the "global chunk list"),
            // find the range of offsets which belongs to us.
            const uint64_t my_offset = global_offset[this_shard_id()];
            const uint64_t neighbour_offset = global_offset[this_shard_id() + 1];
            auto choices_it = std::ranges::lower_bound(chosen_chunks, my_offset);
            const auto choices_end = std::ranges::lower_bound(chosen_chunks, neighbour_offset);
            const uint64_t n_chunks_to_read = choices_end - choices_it;

            // Output iterator, pointing into our subrange in `result`.
            auto out_it = result.begin() + (choices_it - std::begin(chosen_chunks));

            // Iterator over our SSTables, and the range of "global chunk offsets"
            // belonging to the SSTable under the iterator.
            auto sst_it = local_state.snapshot.begin();
            const auto sst_end = local_state.snapshot.end();
            uint64_t current_sst_beg = my_offset;
            uint64_t current_sst_end = current_sst_beg
                + (sst_it != sst_end ? size_in_chunks(*sst_it, chunk_size) : 0);

            // Chooses the the next sample to be read.
            // Returns a pointer to the sstable and the offset of the sample *in bytes*.
            auto get_next_chunk = [&] () -> std::pair<sstables::shared_sstable, uint64_t> {
                SCYLLA_ASSERT(sst_it != sst_end);
                while (*choices_it >= current_sst_end) {
                    ++sst_it;
                    SCYLLA_ASSERT(sst_it != sst_end);
                    current_sst_beg = current_sst_end;
                    current_sst_end = current_sst_beg + size_in_chunks(*sst_it, chunk_size);
                }
                SCYLLA_ASSERT(choices_it != choices_end);
                uint64_t chosen_chunk = *choices_it++;
                return {*sst_it, (chosen_chunk - current_sst_beg) * chunk_size};
            };

            // An arbitrary limit.
            int concurrency_limit = 10;
            co_await max_concurrent_for_each(
                std::views::iota(uint64_t(0), n_chunks_to_read),
                concurrency_limit,
                coroutine::lambda([&] (int) -> future<>
            {
                auto permit = co_await local_db._system_read_concurrency_sem.obtain_permit(
                    local_state.schema, "sample_data_files", chunk_size, no_timeout, nullptr);
                auto [sst, offset] = get_next_chunk();
                auto sample = co_await sst->data_read(offset, chunk_size, permit);
                auto& out_buf = *out_it++;
                std::copy(sample.begin(), sample.end(), out_buf.get_write());
            }));
        };

        co_await container().invoke_on_all(sample_one_shard, std::ref(state));
    } catch (...) {
        ep = std::current_exception();
    }
    co_await state.stop();
    if (ep) {
        co_return coroutine::exception(std::move(ep));
    }
    co_return result;
}

} // namespace replica
