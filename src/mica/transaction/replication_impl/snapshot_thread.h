#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
SnapshotThread<StaticConfig>::SnapshotThread(
    pthread_barrier_t* start_barrier,
    tbb::concurrent_queue<std::pair<uint64_t, uint64_t>>* op_count_queue,
    std::vector<tbb::concurrent_queue<uint64_t>*> op_done_queues)
    : counts_{},
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

  // printf("pinning to thread %lu\n", id_);
  // mica::util::lcore.pin_thread(id_);

  std::unordered_map<uint64_t, uint64_t> temp_counts{};
  std::pair<uint64_t, uint64_t> op_count{};
  uint64_t txn_ts;
  uint64_t count;

  pthread_barrier_wait(start_barrier_);

  while (true) {
    while (op_count_queue_->try_pop(op_count)) {
      printf("popped op count: %lu %lu\n", op_count.first, op_count.second);

      auto search = counts_.find(op_count.first);
      if (search == counts_.end()) {  // Not found
        search = temp_counts.find(op_count.first);
        count = op_count.second;
        if (search != temp_counts.end()) { // Found
          count -= search->second;
          temp_counts.erase(search);
        }

        printf("inserting op count: %lu %lu\n", op_count.first, count);
        counts_[op_count.first] = count;
      }
    }

    for (auto op_done_queue : op_done_queues_) {
      while (op_done_queue->try_pop(txn_ts)) {
        // printf("popped done op txn ts: %lu\n", txn_ts);
        auto search = counts_.find(txn_ts);
        if (search == counts_.end()) {  // Not found
          search = temp_counts.find(txn_ts);
          if (search == temp_counts.end()) { // Not found
            temp_counts[txn_ts] = 1;
          } else {
            temp_counts[txn_ts] = search->second + 1;
          }
        } else {  // Found
          // printf("found txn ts: %lu\n", txn_ts);
          count = search->second - 1;
          printf("decremented txn ts: %lu %lu\n", txn_ts, count);
          if (count == 0) {
            counts_.erase(search);
            // TODO: update min write TS
          } else {
            counts_[txn_ts] = count;
          }
        }
      }
    }

    if (stop_ && op_count_queue_->unsafe_size() == 0) {
      break;
    }
  }

  printf("Exiting snapshot manager\n");
};
};  // namespace transaction
};  // namespace mica

#endif
