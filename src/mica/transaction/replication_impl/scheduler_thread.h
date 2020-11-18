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
    SchedulerPool<StaticConfig>* pool,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue,
    std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
        scheduler_queues,
    std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
        ack_queues,
    pthread_barrier_t* start_barrier, uint16_t id, uint16_t lcore)
    : pool_{pool},
      io_queue_{io_queue},
      scheduler_queues_{scheduler_queues},
      ack_queues_{ack_queues},
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

  uint64_t nworkers = scheduler_queues_.size();

  nanoseconds time_total{0};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  while (true) {
    // Ack executed rows
    ack_executed_rows();

    LogEntryList<StaticConfig>* queue;
    if (io_queue_->try_dequeue(queue)) {
      uint64_t table_index = queue->table_index;
      uint64_t row_id = queue->row_id;
      TableRowID key = {table_index, row_id};

      uint64_t wid = std::hash<TableRowID>{}(key) % nworkers;
      scheduler_queues_[wid]->enqueue(queue);
    } else if (stop_) {
      break;
    }
  }

  run_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(run_end - run_start);

  printf("Exiting replica scheduler: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::ack_executed_rows() {
  for (auto ack_queue : ack_queues_) {
    LogEntryList<StaticConfig>* queue;
    while (ack_queue->try_dequeue(queue)) {
      free_list(queue);
    }
  }
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::free_list(
    LogEntryList<StaticConfig>* list) {
  pool_->free_list(list);
}

};  // namespace transaction
};  // namespace mica

#endif
