#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
SnapshotThread<StaticConfig>::SnapshotThread(
    pthread_barrier_t* start_barrier,
    tbb::concurrent_queue<std::pair<uint64_t, uint64_t>>* op_count_queue)
    : op_count_queue_{op_count_queue},
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

  pthread_barrier_wait(start_barrier_);

  std::pair<uint64_t, uint64_t> op_count{};
  while (true) {

    if (op_count_queue_->try_pop(op_count)) {
      // printf("popped op count: %lu %lu\n", op_count.first, op_count.second);
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
