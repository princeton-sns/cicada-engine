#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SNAPSHOT_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
SnapshotThread<StaticConfig>::SnapshotThread(
    DB<StaticConfig>* db, pthread_barrier_t* start_barrier,
    std::vector<WorkerMinWTS>& min_wtss)
    : db_{db},
      min_wtss_{min_wtss},
      counts_index_{},
      counts_{},
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
  printf("pinning to thread %d\n", 6);
  mica::util::lcore.pin_thread(6);

  nanoseconds time_total{0};
  high_resolution_clock::time_point total_start;
  high_resolution_clock::time_point total_end;

  pthread_barrier_wait(start_barrier_);

  while (true) {
    std::this_thread::sleep_for(std::chrono::microseconds(1));

    uint64_t min_wts = static_cast<uint64_t>(-1);
    for (WorkerMinWTS wts : min_wtss_) {
      uint64_t ts = wts.min_wts;
      if (ts < min_wts) {
        min_wts = ts;
      }
    }

    // printf("min_wts: %lu\n", min_wts);
    db_->set_min_wts(min_wts);
    // TODO: account for executing read-only threads when setting min_rts
    db_->set_min_rts(min_wts);

    if (stop_) {
      break;
    }
  }


  printf("Exiting snapshot manager\n");
  printf("Time total: %ld nanoseconds\n", time_total.count());
};
};  // namespace transaction
};  // namespace mica

#endif
