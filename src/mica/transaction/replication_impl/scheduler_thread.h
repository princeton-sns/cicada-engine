#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
robin_hood::unordered_map<TableRowID, WorkerAssignment>
    SchedulerThread<StaticConfig>::assignments_{};

template <class StaticConfig>
SchedulerThread<StaticConfig>::SchedulerThread(
    SchedulerPool<StaticConfig>* pool,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* scheduler_queue,
    std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
        ack_queues,
    pthread_barrier_t* start_barrier, uint16_t id, uint16_t lcore)
    : pool_{pool},
      io_queue_{io_queue},
      scheduler_queue_{scheduler_queue},
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

  nanoseconds time_noncritical{0};
  nanoseconds time_waiting{0};
  nanoseconds time_critical{0};
  nanoseconds time_total{0};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  high_resolution_clock::time_point start;
  high_resolution_clock::time_point end;
  nanoseconds diff;

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  while (true) {
    start = high_resolution_clock::now();

    // Ack executed rows
    ack_executed_rows();

    LogEntryList<StaticConfig>* queue;
    if (io_queue_->try_dequeue(queue)) {
      uint64_t table_index = queue->table_index;
      uint64_t row_id = queue->row_id;
      TableRowID key = {table_index, row_id};

      auto search = assignments_.find(key);
      if (search == assignments_.end()) {  // Not found

        // TODO: Use one queue per worker
        scheduler_queue_->enqueue(queue);

        // TODO: Choose next worker ID
        uint64_t wid = 0;

        assignments_[key] = {wid, 1};
      } else {  // Found
        WorkerAssignment assignment = search->second;
        assignment.nqueues += 1;

        // TODO: Get correct worker queue
        scheduler_queue_->enqueue(queue);

        search->second = assignment;
      }

      end = high_resolution_clock::now();
      diff = duration_cast<nanoseconds>(end - start);
      time_critical += diff;

    } else if (stop_) {
      break;
    }
  }

  run_end = high_resolution_clock::now();
  diff = duration_cast<nanoseconds>(run_end - run_start);
  time_total += diff;

  printf("Exiting replica scheduler: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
  printf("Time noncritical: %ld nanoseconds\n", time_noncritical.count());
  printf("Time critical: %ld nanoseconds\n", time_critical.count());
  printf("Time waiting: %ld nanoseconds\n", time_waiting.count());
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::ack_executed_rows() {
  for (auto ack_queue : ack_queues_) {
    LogEntryList<StaticConfig>* queue;
    while (ack_queue->try_dequeue(queue)) {
      uint64_t table_index = queue->table_index;
      uint64_t row_id = queue->row_id;
      TableRowID key = {table_index, row_id};
      // printf("acking row id %lu at %lu\n", row_id,
      //        duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count());
      auto search = assignments_.find(key);
      if (search != assignments_.end()) {  // Found
        WorkerAssignment assignment = search->second;
        assignment.nqueues -= 1;

        if (assignment.nqueues == 0) {
          assignments_.erase(search);
        } else {
          search->second = assignment;
        }
      } else {
        throw std::runtime_error("unexpected row id: " + row_id);
      }

      while (queue != nullptr) {
        auto next = queue->next;
        free_list(queue);
        queue = next;
      }
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
