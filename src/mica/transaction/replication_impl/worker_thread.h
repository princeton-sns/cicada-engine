#ifndef MICA_TRANSACTION_REPLICATION_IMPL_WORKER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_WORKER_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
WorkerThread<StaticConfig>::WorkerThread(
    DB<StaticConfig>* db, SchedulerPool<StaticConfig>* pool,
    moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>* scheduler_queue,
    moodycamel::ProducerToken* scheduler_queue_ptok,
    WorkerMinWTS* min_wts, pthread_barrier_t* start_barrier, uint16_t id,
    uint16_t db_id, uint16_t lcore)
    : db_{db},
      pool_{pool},
      scheduler_queue_{scheduler_queue},
      scheduler_queue_ptok_{scheduler_queue_ptok},
      min_wts_{min_wts},
      start_barrier_{start_barrier},
      freed_lists_head_{nullptr},
      freed_lists_tail_{nullptr},
      num_freed_lists_{0},
      id_{id},
      db_id_{db_id},
      lcore_{lcore},
      stop_{false},
      thread_{} {};

template <class StaticConfig>
WorkerThread<StaticConfig>::~WorkerThread() {
  if (thread_.joinable()) {
    thread_.join();
  }
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&WorkerThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::run() {
  printf("Starting replica worker: %u %u %u\n", id_, db_id_, lcore_);

  printf("pinning to thread %u\n", lcore_);
  mica::util::lcore.pin_thread(lcore_);

  db_->activate(db_id_);

  Context<StaticConfig>* ctx = db_->context(db_id_);
  Transaction<StaticConfig> tx{ctx};
  RowAccessHandle<StaticConfig> rah{&tx};

  nanoseconds time_total{0};

  high_resolution_clock::time_point total_start;
  high_resolution_clock::time_point total_end;

  uint64_t nqueues = 0;
  uint64_t queuelen = 0;
  uint64_t npops = 0;

  uint64_t n = 0;
  uint64_t max = 2048;
  LogEntryList<StaticConfig>* queues[max];

  pthread_barrier_wait(start_barrier_);

  total_start = high_resolution_clock::now();
  while (true) {
    n = scheduler_queue_->try_dequeue_bulk_from_producer(*scheduler_queue_ptok_, queues, max);
    if (n != 0) {
      npops += 1;
      nqueues += n;
      for (uint64_t i = 0; i < n; i++) {
        LogEntryList<StaticConfig>* queue = queues[i];
        uint64_t nentries = queue->nentries;
        queuelen += nentries;

        Table<StaticConfig>* tbl = db_->get_table_by_index(queue->table_index);
        uint16_t cf_id = 0;  // TODO: fix hardcoded column family
        uint64_t row_id = queue->row_id;
        uint64_t tail_ts = queue->tail_ts;
        uint64_t min_min_wts = 0;

        // printf("executing queue: %lu %lu %lu\n", row_id, tail_ts, nentries);

        if (!tx.begin_replica()) {
          throw std::runtime_error("run: Failed to begin transaction.");
        }

        row_id = ctx->allocate_row(tbl, row_id);
        if (row_id == static_cast<uint64_t>(-1)) {
          throw std::runtime_error("run: Unable to allocate row ID.");
        }

        RowHead<StaticConfig>* row_head = tbl->head(cf_id, row_id);
        RowVersion<StaticConfig>* queue_head = queue->head_rv;
        RowVersion<StaticConfig>* queue_tail = queue->tail_rv;
        RowVersion<StaticConfig>* older_rv = nullptr;

        // RowVersion<StaticConfig>* temp = queue_head;
        // while (temp != nullptr) {
        //   printf("rv wts: %lu\n", temp->wts.t2);
        //   temp = temp->older_rv;
        // }

        do {
          older_rv = row_head->older_rv;
          if (older_rv != nullptr && older_rv->wts.t2 > tail_ts) {
            min_min_wts = std::max(min_min_wts, older_rv->wts.t2);
            break;
          }

          queue_tail->older_rv = older_rv;

        } while (!__sync_bool_compare_and_swap(&row_head->older_rv, older_rv,
                                               queue_head));

        ::mica::util::memory_barrier();

        if (queue_tail->older_rv != nullptr) {
          uint8_t deleted = false;  // TODO: Handle deleted rows
          ctx->schedule_gc({tail_ts}, tbl, cf_id, deleted, row_id, row_head,
                           queue_tail);
        }

        if (tail_ts >= min_min_wts) {
          min_wts_->min_wts = tail_ts;
        }

        if (!tx.commit_replica(nentries)) {
          throw std::runtime_error("run: Failed to commit transaction.");
        }

        //free_list(queue);
      }
    } else if (stop_) {
      break;
    }
  }

  total_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(total_end - total_start);

  db_->deactivate(db_id_);

  printf("Exiting replica worker: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
  printf("Avg. queues per pop: %g queues\n", (double)nqueues / (double)npops);
  printf("Avg. queue len: %g entries\n", (double)queuelen / (double)nqueues);
  printf("Total row versions: %lu versions\n", queuelen);
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::free_list(LogEntryList<StaticConfig>* list) {
  if (freed_lists_tail_ == nullptr) freed_lists_tail_ = list;

  list->next = freed_lists_head_;
  freed_lists_head_ = list;
  num_freed_lists_ += 1;

  uint64_t n = SchedulerPool<StaticConfig>::kAllocSize;
  if (num_freed_lists_ == n) {
    pool_->free_lists(freed_lists_head_, freed_lists_tail_, n);
    freed_lists_head_ = nullptr;
    freed_lists_tail_ = nullptr;
    num_freed_lists_ = 0;
  }
};

};  // namespace transaction
};  // namespace mica

#endif
