#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
SchedulerThread<StaticConfig>::SchedulerThread(
    moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>* io_queue,
    moodycamel::ProducerToken* io_queue_ptok,
    moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>* scheduler_queue,
    moodycamel::ProducerToken* scheduler_queue_ptok,
    pthread_barrier_t* start_barrier, uint16_t id, uint16_t lcore)
    : io_queue_{io_queue},
      io_queue_ptok_{io_queue_ptok},
      scheduler_queue_{scheduler_queue},
      scheduler_queue_ptok_{scheduler_queue_ptok},
      start_barrier_{start_barrier},
      id_{id},
      lcore_{lcore},
      stop_{false},
      thread_{} {};

template <class StaticConfig>
SchedulerThread<StaticConfig>::~SchedulerThread(){};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&SchedulerThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::run() {
  printf("Starting replica scheduler: %u - %u\n", id_, lcore_);

  printf("pinning to thread %u\n", lcore_);
  mica::util::lcore.pin_thread(lcore_);

  nanoseconds time_total{0};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  uint64_t nqueues = 0;
  uint64_t npops = 0;

  uint64_t n = 0;
  uint64_t max = 2048;
  LogEntryList<StaticConfig>* queues[max];

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  while (true) {
    n = io_queue_->try_dequeue_bulk_from_producer(*io_queue_ptok_, queues, max);

    if (n != 0) {
      npops += 1;
      nqueues += n;
      scheduler_queue_->enqueue_bulk(*scheduler_queue_ptok_, queues, n);
    } else if (stop_) {
      break;
    }
  }

  run_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(run_end - run_start);

  printf("Exiting replica scheduler: %u\n", id_);
  printf("Avg. queues per pop: %g queues\n", (double)nqueues / (double)npops);
  printf("Time total: %ld nanoseconds\n", time_total.count());
};

};  // namespace transaction
};  // namespace mica

#endif
