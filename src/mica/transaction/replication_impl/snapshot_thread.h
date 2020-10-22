#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
SnapshotThread<StaticConfig>::SnapshotThread(
    DB<StaticConfig>* db, pthread_barrier_t* start_barrier,
    moodycamel::ReaderWriterQueue<std::pair<uint64_t, uint64_t>>* op_count_queue,
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

  // TODO: fix thread pinning
  printf("pinning to thread %d\n", 4);
  mica::util::lcore.pin_thread(4);

  std::unordered_map<uint64_t, uint64_t> temp_counts{};
  std::pair<uint64_t, uint64_t> op_count{};
  uint64_t txn_ts;
  uint64_t count;

  pthread_barrier_wait(start_barrier_);

  while (true) {
    if (op_count_queue_->try_dequeue(op_count)) {
      txn_ts = op_count.first;
      count = op_count.second;

      auto search = counts_index_.find(txn_ts);
      if (search == counts_index_.end()) {  // Not found

        auto search2 = temp_counts.find(txn_ts);
        if (search2 != temp_counts.end()) {  // Found
          count -= search2->second;
          temp_counts.erase(search2);
        }

        if (count != 0) {
          counts_.push_back(op_count);
          counts_index_[txn_ts] = std::prev(counts_.end());
        } else if (txn_ts < counts_.front().first) {
          db_->set_min_wts(txn_ts);
          // TODO: account for executing read-only threads when setting min_rts
          db_->set_min_rts(txn_ts);
        }
      }
    }

    for (auto op_done_queue : op_done_queues_) {
      if (op_done_queue->try_dequeue(txn_ts)) {
        auto search = counts_index_.find(txn_ts);
        if (search == counts_index_.end()) {  // Not found
          auto search2 = temp_counts.find(txn_ts);
          if (search2 == temp_counts.end()) {  // Not found
            temp_counts[txn_ts] = 1;
          } else {
            temp_counts[txn_ts] = search2->second + 1;
          }
        } else {  // Found
          op_count = *(search->second);
          count = op_count.second - 1;
          search->second->second = count;

          for (auto it = counts_.begin(); it != counts_.end();) {
            if (it->second != 0) {
              break;
            }

            txn_ts = it->first;
            db_->set_min_wts(txn_ts);
            // TODO: account for executing read-only threads when setting min_rts
            db_->set_min_rts(txn_ts);

            counts_index_.erase(it->first);
            it = counts_.erase(it);
          }
        }
      }
    }

    if (stop_) {  // && op_count_queue_->unsafe_size() == 0) {
      break;
    }
  }

  printf("Exiting snapshot manager\n");
};
};  // namespace transaction
};  // namespace mica

#endif
