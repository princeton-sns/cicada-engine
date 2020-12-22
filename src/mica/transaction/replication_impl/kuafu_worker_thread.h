#ifndef MICA_TRANSACTION_REPLICATION_IMPL_KUAFU_WORKER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_KUAFU_WORKER_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
KuaFuWorkerThread<StaticConfig>::KuaFuWorkerThread(
    DB<StaticConfig>* db,
    SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool,
    moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
        scheduler_queue,
    WorkerMinWTS* min_wts, pthread_barrier_t* start_barrier, uint16_t id,
    uint16_t db_id, uint16_t lcore)
    : db_{db},
      pool_{pool},
      scheduler_queue_{scheduler_queue},
      scheduler_queue_ptok_{*scheduler_queue_},
      min_wts_{min_wts},
      start_barrier_{start_barrier},
      freed_queues_head_{nullptr},
      freed_queues_tail_{nullptr},
      num_freed_queues_{0},
      id_{id},
      db_id_{db_id},
      lcore_{lcore},
      stop_{false},
      thread_{} {};

template <class StaticConfig>
KuaFuWorkerThread<StaticConfig>::~KuaFuWorkerThread() {
  if (thread_.joinable()) {
    thread_.join();
  }
};

template <class StaticConfig>
void KuaFuWorkerThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&KuaFuWorkerThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void KuaFuWorkerThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void KuaFuWorkerThread<StaticConfig>::execute_queue(
    Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
    PerTransactionQueue<StaticConfig>* queue) {
  if (!tx->begin_replica()) {
    throw std::runtime_error("run: Failed to begin transaction.");
  }

  uint64_t nwrites = queue->nwrites;
  RowHead<StaticConfig>* row_head = nullptr;
  RowVersion<StaticConfig>* rv = queue->head_rv;
  RowVersion<StaticConfig>* older_rv = rv->older_rv;
  TableRowID* write_set = queue->write_set;
  for (uint64_t j = 0; j < nwrites; j++) {
    TableRowID key = write_set[j];
    Table<StaticConfig>* tbl = db_->get_table_by_index(key.table_index);
    uint16_t cf_id = 0;  // TODO: fix hardcoded column family
    uint64_t row_id = key.row_id;

    row_id = ctx->allocate_row(tbl, row_id);
    if (row_id == static_cast<uint64_t>(-1)) {
      throw std::runtime_error("run: Unable to allocate row ID.");
    }

    row_head = tbl->head(cf_id, row_id);

    older_rv = rv->older_rv;
    rv->older_rv = row_head->older_rv;
    row_head->older_rv = rv;

    // TODO: garbage collection

    rv = older_rv;
  }

  if (!tx->commit_replica(1)) {
    throw std::runtime_error("run: Failed to commit transaction.");
  }
};

template <class StaticConfig>
void KuaFuWorkerThread<StaticConfig>::run() {
  printf("Starting replica worker: %u %u %u\n", id_, db_id_, lcore_);

  printf("pinning to thread %u\n", lcore_);
  mica::util::lcore.pin_thread(lcore_);

  db_->activate(db_id_);

  Context<StaticConfig>* ctx = db_->context(db_id_);
  Transaction<StaticConfig> tx{ctx};

  nanoseconds time_total{0};

  high_resolution_clock::time_point total_start;
  high_resolution_clock::time_point total_end;

  uint64_t nqueues = 0;
  uint64_t queuelen = 0;
  uint64_t npops = 0;

  uint64_t n = 0;
  uint64_t max = 2048;
  PerTransactionQueue<StaticConfig>* in_queues[max];
  PerTransactionQueue<StaticConfig>* out_queues[max];

  uint64_t nout = 0;

  std::set<PerTransactionQueue<StaticConfig>*> done{};

  pthread_barrier_wait(start_barrier_);

  total_start = high_resolution_clock::now();
  while (true) {
    n = scheduler_queue_->try_dequeue_bulk(in_queues, max);
    if (n != 0) {
      npops += 1;
      nqueues += n;
      for (uint64_t i = 0; i < n; i++) {
        PerTransactionQueue<StaticConfig>* queue = in_queues[i];
        queuelen += queue->nwrites;

        execute_queue(ctx, &tx, queue);

        queue->ack_dependents(done);
      }

      if (done.size() != 0) {
        nout = 0;
        for (PerTransactionQueue<StaticConfig>* queue : done) {
          out_queues[nout++] = queue;
        }
        scheduler_queue_->enqueue_bulk(scheduler_queue_ptok_, out_queues, nout);
        done.clear();
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
void KuaFuWorkerThread<StaticConfig>::free_queue(
    PerTransactionQueue<StaticConfig>* queue) {
  if (freed_queues_tail_ == nullptr) freed_queues_tail_ = queue;

  queue->next = freed_queues_head_;
  freed_queues_head_ = queue;
  num_freed_queues_ += 1;

  uint64_t n = pool_.kAllocSize;
  if (num_freed_queues_ == n) {
    pool_->free_n(freed_queues_head_, freed_queues_tail_, n);
    freed_queues_head_ = nullptr;
    freed_queues_tail_ = nullptr;
    num_freed_queues_ = 0;
  }
};

};  // namespace transaction
};  // namespace mica

#endif
