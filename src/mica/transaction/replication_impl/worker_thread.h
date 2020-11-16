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
    DB<StaticConfig>* db,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* scheduler_queue,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* ack_queue,
    WorkerMinWTS* min_wts, pthread_barrier_t* start_barrier, uint16_t id,
    uint16_t nschedulers)
    : db_{db},
      scheduler_queue_{scheduler_queue},
      ack_queue_{ack_queue},
      min_wts_{min_wts},
      start_barrier_{start_barrier},
      id_{id},
      nschedulers_{nschedulers},
      stop_{false},
      thread_{},
      time_working_{0},
      working_start_{},
      working_end_{} {};

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
  printf("Starting replica worker: %u\n", id_);

  printf("pinning to thread %d\n", id_ + 4);
  mica::util::lcore.pin_thread(id_ + 4);

  db_->activate(id_);

  Context<StaticConfig>* ctx = db_->context(id_);
  Transaction<StaticConfig> tx{ctx};
  RowAccessHandle<StaticConfig> rah{&tx};

  nanoseconds time_total{0};
  time_working_ = nanoseconds{0};

  high_resolution_clock::time_point total_start;
  high_resolution_clock::time_point total_end;

  uint64_t nqueues = 0;
  uint64_t queuelen = 0;
  uint64_t npops = 0;

  pthread_barrier_wait(start_barrier_);

  total_start = high_resolution_clock::now();
  while (true) {
    LogEntryList<StaticConfig>* queue = nullptr;
    if (scheduler_queue_->try_dequeue(queue)) {
      uint64_t nentries = queue->nentries;
      // working_start_ = high_resolution_clock::now();
      npops += 1;
      nqueues += 1;
      queuelen += nentries;

      Table<StaticConfig>* tbl = db_->get_table_by_index(queue->table_index);
      uint16_t cf_id = 0;  // TODO: fix hardcoded column family
      uint64_t row_id = queue->row_id;
      uint64_t head_ts = queue->head_ts;

      // printf("executing queue: %lu %lu %lu\n", row_id, head_ts, nentries);

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

      // RowVersion<StaticConfig>* temp = queue_head;
      // while (temp != nullptr) {
      //   printf("rv wts: %lu\n", temp->wts.t2);
      //   temp = temp->older_rv;
      // }

      queue_tail->older_rv = row_head->older_rv;

      row_head->older_rv = queue_head;

      ::mica::util::memory_barrier();

      if (queue_tail->older_rv != nullptr) {
        uint8_t deleted = false;  // TODO: Handle deleted rows
        ctx->schedule_gc({head_ts}, tbl, cf_id, deleted, row_id, row_head,
                         queue_tail);
      }

      min_wts_->min_wts = head_ts;
      ack_queue_->enqueue(queue);

      if (!tx.commit_replica(nentries)) {
        throw std::runtime_error("run: Failed to commit transaction.");
      }

      // working_end_ = high_resolution_clock::now();
      // time_working_ +=
      //   duration_cast<nanoseconds>(working_end_ - working_start_);
    } else if (stop_) {
      break;
    }
  }

  total_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(total_end - total_start);

  db_->deactivate(id_);

  printf("Exiting replica worker: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
  printf("Time working: %ld nanoseconds\n", time_working_.count());
  printf("Avg. queues per pop: %g queues\n", (double)nqueues / (double)npops);
  printf("Avg. queue len: %g entries\n", (double)queuelen / (double)nqueues);
  printf("Total row versions: %lu versions\n", queuelen);
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::insert_row(
    Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  // typename StaticConfig::Timestamp txn_ts;
  // txn_ts.t2 = le->txn_ts;

  // if (!tx->has_began()) {
  //   if (!tx->begin(false, &txn_ts)) {
  //     throw std::runtime_error("insert_row: Failed to begin transaction.");
  //   }
  // } else if (tx->ts() != txn_ts) {
  //   Result result;
  //   tx->commit_replica(&result);
  //   if (result != Result::kCommitted) {
  //     throw std::runtime_error("insert_row: Failed to commit transaction.");
  //   }

  //   if (!tx->begin(false, &txn_ts)) {
  //     throw std::runtime_error("insert_row: Failed to begin transaction.");
  //   }
  // }

  // rah->reset();

  // if (StaticConfig::kReplUseUpsert) {
  //   if (!rah->upsert_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
  //     throw std::runtime_error("insert_row: Failed to upsert row " +
  //                              le->row_id);
  //   }
  // } else {
  //   if (!rah->new_row_replica(tbl, le->cf_id, le->row_id, false,
  //                             le->data_size)) {
  //     throw std::runtime_error("insert_row: Failed to create new row " +
  //                              le->row_id);
  //   }
  // }

  // char* data = rah->data();
  // std::memcpy(data, le->data, le->data_size);
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::write_row(Table<StaticConfig>* tbl,
                                           Transaction<StaticConfig>* tx,
                                           RowAccessHandle<StaticConfig>* rah,
                                           WriteRowLogEntry<StaticConfig>* le) {
  // typename StaticConfig::Timestamp txn_ts;
  // txn_ts.t2 = le->txn_ts;

  // if (!tx->has_began()) {
  //   if (!tx->begin(false, &txn_ts)) {
  //     throw std::runtime_error("write_row: Failed to begin transaction.");
  //   }
  // } else if (tx->ts() != txn_ts) {
  //   Result result;
  //   tx->commit_replica(&result);
  //   if (result != Result::kCommitted) {
  //     throw std::runtime_error("write_row: Failed to commit transaction.");
  //   }

  //   if (!tx->begin(false, &txn_ts)) {
  //     throw std::runtime_error("write_row: Failed to begin transaction.");
  //   }
  // }

  // rah->reset();

  // if (!rah->peek_row_replica(tbl, le->cf_id, le->row_id, false, false, true)) {
  //   throw std::runtime_error("write_row: Failed to peek row.");
  // }

  // if (!rah->write_row_replica(le->data_size)) {
  //   throw std::runtime_error("write_row: Failed to write row.");
  // }

  // char* data = rah->data();
  // std::memcpy(data, le->data, le->data_size);
}

};  // namespace transaction
};  // namespace mica

#endif
