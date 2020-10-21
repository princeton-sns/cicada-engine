#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
SnapshotThread<StaticConfig>::SnapshotThread(
    DB<StaticConfig>* db, pthread_barrier_t* start_barrier,
    tbb::concurrent_queue<std::pair<uint64_t, uint64_t>>* op_count_queue,
    std::vector<moodycamel::ReaderWriterQueue<uint64_t>*> op_done_queues)
    : db_{db},
      counts_index_{},
      counts_{},
      op_count_queue_{op_count_queue},
      op_done_queues_{op_done_queues},
      stop_{false},
      thread_{},
      start_barrier_{start_barrier} {};

template <class StaticConfig>
SnapshotThread<StaticConfig>::~SnapshotThread(){};

template <class StaticConfig>
void SnapshotThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&SnapshotThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void SnapshotThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void SnapshotThread<StaticConfig>::run() {
  printf("Starting snapshot manager\n");

  // printf("db min_rts: %lu\n", db_->min_rts().t2);

  // TODO: fix thread pinning
  printf("pinning to thread %lu\n", 4);
  mica::util::lcore.pin_thread(4);

  std::unordered_map<uint64_t, uint64_t> temp_counts{};
  std::pair<uint64_t, uint64_t> op_count{};
  uint64_t txn_ts;
  uint64_t count;
  uint64_t rts_updates = 0;
  uint64_t batch_count = 0;
  const uint64_t batch_size = 128;

  pthread_barrier_wait(start_barrier_);

  while (true) {
    batch_count = 0;
    while (batch_count < batch_size && op_count_queue_->try_pop(op_count)) {
      // printf("popped op count: %lu %lu\n", op_count.first, op_count.second);

      auto search = counts_index_.find(op_count.first);
      if (search == counts_index_.end()) {  // Not found
        count = op_count.second;

        auto search2 = temp_counts.find(op_count.first);
        if (search2 != temp_counts.end()) {  // Found
          count -= search2->second;
          temp_counts.erase(search2);
        }

        if (count != 0) {
          // printf("inserting op count: %lu %lu\n", op_count.first, count);
          counts_.push_back(op_count);
          counts_index_[op_count.first] = std::prev(counts_.end());
        } else {
          // printf("skipping inserting op count: %lu %lu\n", op_count.first, count);
          // TODO: update min write TS
        }
      }
      batch_count++;
    }

    for (auto op_done_queue : op_done_queues_) {
      batch_count = 0;
      while (batch_count < batch_size && op_done_queue->try_dequeue(txn_ts)) {
        // printf("popped done op txn ts: %lu\n", txn_ts);
        auto search = counts_index_.find(txn_ts);
        if (search == counts_index_.end()) {  // Not found
          auto search2 = temp_counts.find(txn_ts);
          if (search2 == temp_counts.end()) {  // Not found
            temp_counts[txn_ts] = 1;
          } else {
            temp_counts[txn_ts] = search2->second + 1;
          }
        } else {  // Found
          // printf("found txn ts: %lu\n", txn_ts);
          op_count = *(search->second);
          count = op_count.second - 1;
          // printf("decremented txn ts: %lu %lu\n", txn_ts, count);
          search->second->second = count;

          for (auto it = counts_.begin(); it != counts_.end();) {
            if (it->second != 0) {
              break;
            }

            uint64_t ts = it->first;
            // db_->set_min_wts(ts);
            // TODO: account for executing read-only threads when setting min_rts
            // db_->set_min_rts(ts);

            // if (rts_updates <= 5) {
            //   printf("updated min_rts to %lu\n", ts);
            // }
            // rts_updates++;

            counts_index_.erase(it->first);
            it = counts_.erase(it);
          }
        }
      }
      batch_count++;
    }

    if (stop_) {  // && op_count_queue_->unsafe_size() == 0) {
      break;
    }
  }

  printf("Exiting snapshot manager\n");
  // printf("db min_rts: %lu\n", db_->min_rts().t2);
};
};  // namespace transaction
};  // namespace mica

#endif
