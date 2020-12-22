#ifndef MICA_TRANSACTION_REPLICATION_IMPL_KUAFU_SCHEDULER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_KUAFU_SCHEDULER_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
KuaFuSchedulerThread<StaticConfig>::KuaFuSchedulerThread(
    moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
        io_queue,
    moodycamel::ProducerToken* io_queue_ptok,
    moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
        scheduler_queue,
    moodycamel::ProducerToken* scheduler_queue_ptok,
    pthread_barrier_t* start_barrier, uint16_t id, uint16_t lcore)
    : conflicts_{},
      io_queue_{io_queue},
      io_queue_ptok_{io_queue_ptok},
      scheduler_queue_{scheduler_queue},
      scheduler_queue_ptok_{scheduler_queue_ptok},
      start_barrier_{start_barrier},
      id_{id},
      lcore_{lcore},
      stop_{false},
      thread_{} {};

template <class StaticConfig>
KuaFuSchedulerThread<StaticConfig>::~KuaFuSchedulerThread(){};

template <class StaticConfig>
void KuaFuSchedulerThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&KuaFuSchedulerThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void KuaFuSchedulerThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void KuaFuSchedulerThread<StaticConfig>::run() {
  printf("Starting KuaFu scheduler: %u - %u\n", id_, lcore_);

  printf("pinning to thread %u\n", lcore_);
  mica::util::lcore.pin_thread(lcore_);

  nanoseconds time_total{0};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  uint64_t nqueues = 0;
  uint64_t npops = 0;

  uint64_t n = 0;
  uint64_t max = 2048;
  PerTransactionQueue<StaticConfig>* in_queues[max];
  PerTransactionQueue<StaticConfig>* out_queues[max];

  uint64_t nout = 0;
  uint64_t nconflicts = 0;

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  while (true) {
    n = io_queue_->try_dequeue_bulk_from_producer(*io_queue_ptok_, in_queues,
                                                  max);

    if (n != 0) {
      npops += 1;
      nqueues += n;

      // Each queue contains the log entries for a transaction
      nout = 0;
      for (uint64_t i = 0; i < n; i++) {
        PerTransactionQueue<StaticConfig>* queue = in_queues[i];
        queue->scheduled = false;
        queue->executed = false;

        nconflicts = 0;
        uint64_t nwrites = queue->nwrites;

        TableRowID* write_set = queue->write_set;
        for (uint64_t j = 0; j < nwrites; j++) {
          TableRowID key = write_set[j];

          // printf("key: %lu, %lu\n", key.table_index, key.row_id);
          auto search = conflicts_.find(key);
          if (search == conflicts_.end()) {  // Not found
            conflicts_.insert({key, queue});
          } else {  // Found
            auto conflict = search->second;
            if (!conflict->executed && conflict->add_dependent(queue)) {
              nconflicts += 1;
            }

            search->second = queue;
          }
        }

        queue->nconflicts = nconflicts;
        queue->scheduled = true;

        if (nconflicts == 0) {
          out_queues[nout++] = queue;
        }
      }

      scheduler_queue_->enqueue_bulk(*scheduler_queue_ptok_, out_queues, nout);
    } else if (stop_) {
      break;
    }
  }

  run_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(run_end - run_start);

  printf("Exiting KuaFu scheduler: %u\n", id_);
  printf("Avg. queues per pop: %g queues\n", (double)nqueues / (double)npops);
  printf("Time total: %ld nanoseconds\n", time_total.count());
};

};  // namespace transaction
};  // namespace mica

#endif
