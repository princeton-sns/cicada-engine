#pragma once
#ifndef MICA_TRANSACTION_DB_IMPL_H_
#define MICA_TRANSACTION_DB_IMPL_H_

#include "mica/transaction/db.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
DB<StaticConfig>::DB(PagePool<StaticConfig>** page_pools, Logger* logger,
                     Stopwatch* sw, uint16_t num_threads, bool is_replica)
    : page_pools_(page_pools),
      logger_(logger),
      sw_(sw),
      num_threads_(num_threads),
      is_replica_{is_replica} {
  assert(num_threads_ <=
         static_cast<uint16_t>(::mica::util::lcore.lcore_count()));
  assert(num_threads_ <= StaticConfig::kMaxLCoreCount);

  num_numa_ = 0;

  for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
    uint8_t numa_id =
        static_cast<uint8_t>(::mica::util::lcore.numa_id(thread_id));
    if (num_numa_ <= numa_id) num_numa_ = static_cast<uint8_t>(numa_id + 1);

    ctxs_[thread_id] = new Context<StaticConfig>(this, thread_id, numa_id);

    thread_active_[thread_id] = false;
    clock_init_[thread_id] = false;
    thread_states_[thread_id].quiescence = false;
  }
  assert(num_numa_ <= StaticConfig::kMaxNUMACount);

  for (uint8_t numa_id = 0; numa_id < num_numa_; numa_id++)
    shared_row_version_pools_[numa_id] =
        new SharedRowVersionPool<StaticConfig>(page_pools_[numa_id], numa_id);

  for (uint16_t thread_id = 0; thread_id < num_threads_; thread_id++) {
    auto pool = new RowVersionPool<StaticConfig>(ctxs_[thread_id],
                                                 shared_row_version_pools_);
    row_version_pools_[thread_id] = pool;
  }

  printf("thread count = %" PRIu16 "\n", num_threads_);
  printf("NUMA count = %" PRIu8 "\n", num_numa_);
  printf("\n");

  last_backoff_print_ = 0;
  last_backoff_update_ = 0;
  backoff_ = 0.;

  active_thread_count_ = 0;
  leader_thread_id_ = static_cast<uint16_t>(-1);

  if (!is_replica_) {
    min_wts_.init(ctxs_[0]->generate_timestamp());
  } else {
    min_wts_.init(Timestamp::make(0, 0, 0));
  }
  min_rts_.init(min_wts_.get());
  ref_clock_ = 0;
  // gc_epoch_ = 0;
}

template <class StaticConfig>
DB<StaticConfig>::~DB() {
  // TODO: Deallocate all rows that are cached in Context before deleting
  // tables.

  for (auto& t : tables_) delete t;
  tables_index_.clear();

  for (auto thread_id = 0; thread_id < num_threads_; thread_id++)
    delete row_version_pools_[thread_id];

  for (uint8_t numa_id = 0; numa_id < num_numa_; numa_id++)
    delete shared_row_version_pools_[numa_id];

  for (auto i = 0; i < num_threads_; i++) delete ctxs_[i];
}

template <class StaticConfig>
bool DB<StaticConfig>::create_table(std::string name, uint16_t cf_count,
                                    const uint64_t* data_size_hints) {
  if (tables_index_.find(name) != tables_index_.end()) return false;

  std::size_t i = create_table_(name, cf_count, data_size_hints);
  auto tbl = tables_[i];
  tables_index_[name] = i;

  if (!is_replica_) {
    if (!logger_->log(context(0), tbl))  // TODO: Fix hardcoded thread_id
      return false;
  }

  return true;
}

template <class StaticConfig>
std::size_t DB<StaticConfig>::create_table_(std::string name, uint16_t cf_count,
                                            const uint64_t* data_size_hints) {
  std::size_t i = tables_.size();
  auto tbl = new Table<StaticConfig>(this, name, cf_count, data_size_hints, i);
  tables_.push_back(tbl);

  return i;
}

template <class StaticConfig>
bool DB<StaticConfig>::create_hash_index_unique_u64(
    std::string name, Table<StaticConfig>* main_tbl,
    uint64_t expected_row_count) {
  if (hash_idxs_unique_u64_.find(name) != hash_idxs_unique_u64_.end())
    return false;

  const uint64_t kDataSizes[] = {HashIndexUniqueU64::kDataSize};
  std::size_t i = create_table_(name, 1, kDataSizes);
  auto tbl = tables_[i];

  auto idx = new HashIndexUniqueU64(this, main_tbl, tbl, expected_row_count);
  hash_idxs_unique_u64_[name] = idx;

  if (!is_replica_) {
    if (!logger_->log(context(0), idx))  // TODO: Fix hardcoded thread_id
      return false;
  }

  return true;
}

template <class StaticConfig>
bool DB<StaticConfig>::create_hash_index_nonunique_u64(
    std::string name, Table<StaticConfig>* main_tbl,
    uint64_t expected_row_count) {
  if (hash_idxs_nonunique_u64_.find(name) != hash_idxs_nonunique_u64_.end())
    return false;

  const uint64_t kDataSizes[] = {HashIndexNonuniqueU64::kDataSize};
  std::size_t i = create_table_(name, 1, kDataSizes);
  auto tbl = tables_[i];
  auto idx = new HashIndexNonuniqueU64(this, main_tbl, tbl, expected_row_count);
  hash_idxs_nonunique_u64_[name] = idx;
  return true;
}

template <class StaticConfig>
bool DB<StaticConfig>::create_btree_index_unique_u64(
    std::string name, Table<StaticConfig>* main_tbl) {
  if (btree_idxs_unique_u64_.find(name) != btree_idxs_unique_u64_.end())
    return false;

  const uint64_t kDataSizes[] = {BTreeIndexUniqueU64::kDataSize};
  std::size_t i = create_table_(name, 1, kDataSizes);
  auto tbl = tables_[i];
  auto idx = new BTreeIndexUniqueU64(this, main_tbl, tbl);
  btree_idxs_unique_u64_[name] = idx;
  return true;
}

template <class StaticConfig>
bool DB<StaticConfig>::create_btree_index_nonunique_u64(
    std::string name, Table<StaticConfig>* main_tbl) {
  if (btree_idxs_nonunique_u64_.find(name) != btree_idxs_nonunique_u64_.end())
    return false;

  const uint64_t kDataSizes[] = {BTreeIndexNonuniqueU64::kDataSize};
  std::size_t i = create_table_(name, 1, kDataSizes);
  auto tbl = tables_[i];
  auto idx = new BTreeIndexNonuniqueU64(this, main_tbl, tbl);
  btree_idxs_nonunique_u64_[name] = idx;
  return true;
}

template <class StaticConfig>
void DB<StaticConfig>::activate(uint16_t thread_id) {
  // printf("DB::activate(): thread_id=%hu\n", thread_id);
  if (thread_active_[thread_id]) return;

  if (!clock_init_[thread_id]) {
    // Add one to avoid reusing the same clock value.
    ctxs_[thread_id]->set_clock(ref_clock_ + 1);
    clock_init_[thread_id] = true;
  }

  if (!is_replica_) {
    ctxs_[thread_id]->generate_timestamp();
  }

  // Ensure that no bogus clock/rts is accessed by other threads.
  ::mica::util::memory_barrier();

  thread_active_[thread_id] = true;

  ::mica::util::memory_barrier();

  // auto init_gc_epoch = gc_epoch_;

  ::mica::util::memory_barrier();

  // Keep updating timestamp until it is reflected to min_wts and min_rts.
  while (!is_replica_ && (min_wts() > ctxs_[thread_id]->wts() ||
                          min_rts() > ctxs_[thread_id]->rts())) {
    ::mica::util::pause();

    quiescence(thread_id);

    // We also perform clock syncronization to bump up this thread's clock if
    // necessary.
    ctxs_[thread_id]->synchronize_clock();
    ctxs_[thread_id]->generate_timestamp();
  }

  __sync_fetch_and_add(&active_thread_count_, 1);
}

template <class StaticConfig>
void DB<StaticConfig>::deactivate(uint16_t thread_id) {
  // printf("DB::deactivate(): thread_id=%hu\n", thread_id);
  if (!thread_active_[thread_id]) return;

  // TODO: Clear any garbage collection item in the context.

  // Wait until ref_clock becomes no smaller than this thread's clock.
  // This allows this thread to resume with ref_clock later.
  while (!is_replica_ &&
         static_cast<int64_t>(ctxs_[thread_id]->clock() - ref_clock_) > 0) {
    ::mica::util::pause();

    quiescence(thread_id);
  }

  thread_active_[thread_id] = false;

  if (leader_thread_id_ == thread_id)
    leader_thread_id_ = static_cast<uint16_t>(-1);

  __sync_sub_and_fetch(&active_thread_count_, 1);
}

template <class StaticConfig>
void DB<StaticConfig>::reset_clock(uint16_t thread_id) {
  assert(!thread_active_[thread_id]);
  clock_init_[thread_id] = false;
}

template <class StaticConfig>
void DB<StaticConfig>::idle(uint16_t thread_id) {
  quiescence(thread_id);

  ctxs_[thread_id]->synchronize_clock();
  ctxs_[thread_id]->generate_timestamp();
}

template <class StaticConfig>
void DB<StaticConfig>::quiescence(uint16_t thread_id) {
  ::mica::util::memory_barrier();

  thread_states_[thread_id].quiescence = true;

  if (!is_replica_ && leader_thread_id_ == static_cast<uint16_t>(-1)) {
    if (__sync_bool_compare_and_swap(&leader_thread_id_,
                                     static_cast<uint16_t>(-1), thread_id)) {
      last_non_quiescence_thread_id_ = 0;

      auto now = sw_->now();
      last_backoff_update_ = now;
      last_backoff_ = backoff_;
    }
  }

  if (leader_thread_id_ != thread_id) return;

  uint16_t i = last_non_quiescence_thread_id_;
  for (; i < num_threads_; i++)
    if (thread_active_[i] && !thread_states_[i].quiescence) break;
  if (i != num_threads_) {
    last_non_quiescence_thread_id_ = i;
    return;
  }

  last_non_quiescence_thread_id_ = 0;

  bool first = true;
  Timestamp min_wts;
  Timestamp min_rts;

  for (i = 0; i < num_threads_; i++) {
    if (!thread_active_[i]) continue;

    auto wts = ctxs_[i]->wts();
    auto rts = ctxs_[i]->rts();
    if (first) {
      min_wts = wts;
      min_rts = rts;
      first = false;
    } else {
      if (min_wts > wts) min_wts = wts;
      if (min_rts > rts) min_rts = rts;
    }

    thread_states_[i].quiescence = false;
  }

  assert(!first);
  if (!first) {
    // We only increment gc_epoch and update timestamp/clocks when
    // min_rts increases. The equality is required because having a
    // single active thread will make it the same.

    // Ensure wts is no earlier than rts (this can happen if memory ordering is
    // not strict).
    if (min_wts < min_rts) min_wts = min_rts;

    if (min_wts_.get() < min_wts) min_wts_.write(min_wts);

    if (min_rts_.get() <= min_rts) {
      min_rts_.write(min_rts);

      ref_clock_ = ctxs_[thread_id]->clock();
      // gc_epoch_++;
    }
  }
}

template <class StaticConfig>
void DB<StaticConfig>::update_backoff(uint16_t thread_id) {
  if (leader_thread_id_ != thread_id) return;

  uint64_t now = sw_->now();
  uint64_t time_diff = now - last_backoff_update_;

  const uint64_t us = sw_->c_1_usec();

  if (time_diff < StaticConfig::kBackoffUpdateInterval * us) return;

  assert(time_diff != 0);

  uint64_t committed_count = 0;
  for (uint16_t i = 0; i < num_threads_; i++)
    committed_count += ctxs_[i]->stats().committed_count;

  uint64_t committed_diff = committed_count - last_committed_count_;

  double committed_tput =
      static_cast<double>(committed_diff) / static_cast<double>(time_diff);

  double committed_tput_diff = committed_tput - last_committed_tput_;

  double backoff_diff = backoff_ - last_backoff_;

  double new_last_backoff = backoff_;
  double new_backoff = new_last_backoff;

  // If gradient > 0, higher backoff will cause higher tput.
  // If gradient < 0, lower backoff will cause higher tput.
  double gradient;
  if (backoff_diff != 0.)
    gradient = committed_tput_diff / backoff_diff;
  else
    gradient = 0.;

  double incr = StaticConfig::kBackoffHCIncrement * static_cast<double>(us);
  // If we are updating backoff infrequently, we increase a large amount at
  // once.
  incr *= static_cast<double>(time_diff) /
          static_cast<double>(StaticConfig::kBackoffUpdateInterval * us);

  if (gradient < 0)
    new_backoff -= incr;
  else if (gradient > 0)
    new_backoff += incr;
  else {
    if ((now & 2) == 0)
      new_backoff -= incr;
    else
      new_backoff += incr;
  }

  if (new_backoff < StaticConfig::kBackoffMin * static_cast<double>(us))
    new_backoff = StaticConfig::kBackoffMin * static_cast<double>(us);
  if (new_backoff > StaticConfig::kBackoffMax * static_cast<double>(us))
    new_backoff = StaticConfig::kBackoffMax * static_cast<double>(us);

  last_backoff_ = new_last_backoff;
  backoff_ = new_backoff;

  last_backoff_update_ = now;
  last_committed_count_ = committed_count;
  last_committed_tput_ = committed_tput;

  if (StaticConfig::kPrintBackoff &&
      now - last_backoff_print_ >= 100 * 1000 * us) {
    last_backoff_print_ = now;
    printf("backoff=%.3f us\n", backoff_ / static_cast<double>(us));
  }
}

template <class StaticConfig>
void DB<StaticConfig>::reset_backoff() {
  // This requires reset_stats() to be effective.
  backoff_ = 0.;
}
}  // namespace transaction
}  // namespace mica

#endif
