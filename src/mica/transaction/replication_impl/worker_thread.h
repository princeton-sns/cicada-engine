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
  uint64_t txn_ts = static_cast<uint64_t>(-1);
  Table<StaticConfig>* tbl = nullptr;

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
    LogEntryList<StaticConfig>* first = nullptr;
    LogEntryList<StaticConfig>* queue = nullptr;
    if (scheduler_queue_->try_dequeue(first)) {
      npops += 1;
      // working_start_ = high_resolution_clock::now();
      queue = first;
      tbl = db_->get_table(std::string{queue->tbl_name});
      txn_ts = static_cast<uint64_t>(-1);
      while (queue != nullptr) {
        nqueues += 1;
        queuelen += queue->nentries;
        // printf("executing queue with %lu entries\n", queue->nentries);
        // row_id = static_cast<uint64_t>(-1);
        uint64_t nentries = queue->nentries;
        char* ptr = queue->buf;
        for (uint64_t i = 0; i < nentries; i++) {
          LogEntry<StaticConfig>* le =
              reinterpret_cast<LogEntry<StaticConfig>*>(ptr);

          // le->print();

          InsertRowLogEntry<StaticConfig>* irle = nullptr;
          WriteRowLogEntry<StaticConfig>* wrle = nullptr;
          switch (le->type) {
            case LogEntryType::INSERT_ROW:
              irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
              if (txn_ts == static_cast<uint64_t>(-1)) {
                txn_ts = irle->txn_ts;
              }
              // irle->print();
              insert_row(tbl, &tx, &rah, irle);
              break;

            case LogEntryType::WRITE_ROW:
              wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
              if (txn_ts == static_cast<uint64_t>(-1)) {
                txn_ts = wrle->txn_ts;
              }
              // wrle->print();
              write_row(tbl, &tx, &rah, wrle);

              break;

            default:
              throw std::runtime_error(
                  "WorkerThread::run: Unexpected log entry type.");
          }

          ptr += le->size;
        }

        queue = queue->next;
      }

      if (txn_ts != static_cast<uint64_t>(-1)) {
        min_wts_->min_wts = txn_ts;
      }

      ack_queue_->enqueue(first);

      // working_end_ = high_resolution_clock::now();
      // time_working_ +=
      //   duration_cast<nanoseconds>(working_end_ - working_start_);
    } else if (stop_) {
      break;
    }
  }

  if (tx.has_began()) {
    Result result;
    tx.commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
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
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::insert_row(
    Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  TableType tbl_type = static_cast<TableType>(le->tbl_type);

  switch (tbl_type) {
    case TableType::DATA:
      insert_data_row(tbl, tx, rah, le);
      break;
    case TableType::HASH_IDX:
      insert_hash_idx_row(tbl, tx, rah, le);
      break;
    default:
      throw std::runtime_error("Insert: Unsupported table type.");
  }
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::insert_data_row(
    Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->has_began()) {
    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error("insert_data_row: Failed to begin transaction.");
    }
  } else if (tx->ts() != txn_ts) {
    Result result;
    tx->commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error(
          "insert_data_row: Failed to commit transaction.");
    }

    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error("insert_data_row: Failed to begin transaction.");
    }
  }

  rah->reset();

  if (StaticConfig::kReplUseUpsert) {
    if (!rah->upsert_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
      throw std::runtime_error("insert_data_row: Failed to upsert row " +
                               le->row_id);
    }
  } else {
    if (!rah->new_row_replica(tbl, le->cf_id, le->row_id, false,
                              le->data_size)) {
      throw std::runtime_error("insert_data_row: Failed to create new row " +
                               le->row_id);
    }
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::insert_hash_idx_row(
    Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  auto unique_hash_idx =
      db_->get_hash_index_unique_u64(std::string{le->tbl_name});
  auto nonunique_hash_idx =
      db_->get_hash_index_nonunique_u64(std::string{le->tbl_name});
  if (unique_hash_idx != nullptr) {
    tbl = unique_hash_idx->index_table();
  } else if (nonunique_hash_idx != nullptr) {
    tbl = nonunique_hash_idx->index_table();
  }

  if (tbl == nullptr) {
    throw std::runtime_error(
        "insert_hash_idx_row: Failed to find index table.");
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->has_began()) {
    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error(
          "insert_hash_idx_row: Failed to begin transaction.");
    }
  } else if (tx->ts() != txn_ts) {
    Result result;
    tx->commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error(
          "insert_hash_idx_row: Failed to commit transaction.");
    }

    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error(
          "insert_hash_idx_row: Failed to begin transaction.");
    }
  }

  rah->reset();

  if (StaticConfig::kReplUseUpsert) {
    if (!rah->upsert_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
      throw std::runtime_error("insert_hash_idx_row: Failed to upsert row " +
                               le->row_id);
    }
  } else {
    if (!rah->new_row_replica(tbl, le->cf_id, le->row_id, false,
                              le->data_size)) {
      throw std::runtime_error(
          "insert_hash_idx_row: Failed to create new row " + le->row_id);
    }
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::write_row(Table<StaticConfig>* tbl,
                                           Transaction<StaticConfig>* tx,
                                           RowAccessHandle<StaticConfig>* rah,
                                           WriteRowLogEntry<StaticConfig>* le) {
  TableType tbl_type = static_cast<TableType>(le->tbl_type);

  switch (tbl_type) {
    case TableType::DATA:
      write_data_row(tbl, tx, rah, le);
      break;
    case TableType::HASH_IDX:
      write_hash_idx_row(tbl, tx, rah, le);
      break;
    default:
      throw std::runtime_error("Insert: Unsupported table type.");
  }
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::write_data_row(
    Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, WriteRowLogEntry<StaticConfig>* le) {
  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->has_began()) {
    if (!tx->begin(false, &txn_ts)) {
      // if (!tx->begin(false)) {
      throw std::runtime_error("write_data_row: Failed to begin transaction.");
    }
  } else if (tx->ts() != txn_ts) {
    // } else {
    Result result;
    tx->commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("write_data_row: Failed to commit transaction.");
    }

    if (!tx->begin(false, &txn_ts)) {
      // if (!tx->begin(false)) {
      throw std::runtime_error("write_data_row: Failed to begin transaction.");
    }
  }

  rah->reset();

  if (!rah->peek_row_replica(tbl, le->cf_id, le->row_id, false, false, true)) {
    throw std::runtime_error("write_data_row: Failed to peek row.");
  }

  if (!rah->write_row_replica(le->data_size)) {
    throw std::runtime_error("write_data_row: Failed to write row.");
  }

  // char* data = rah->data();
  // std::memcpy(data, le->data, le->data_size);
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::write_hash_idx_row(
    Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, WriteRowLogEntry<StaticConfig>* le) {
  auto unique_hash_idx =
      db_->get_hash_index_unique_u64(std::string{le->tbl_name});
  auto nonunique_hash_idx =
      db_->get_hash_index_nonunique_u64(std::string{le->tbl_name});
  tbl = nullptr;
  if (unique_hash_idx != nullptr) {
    tbl = unique_hash_idx->index_table();
  } else if (nonunique_hash_idx != nullptr) {
    tbl = nonunique_hash_idx->index_table();
  }

  if (tbl == nullptr) {
    throw std::runtime_error("write_hash_idx_row: Failed to find index table.");
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->has_began()) {
    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error(
          "write_hash_idx_row: Failed to begin transaction.");
    }
  } else if (tx->ts() != txn_ts) {
    Result result;
    tx->commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error(
          "write_hash_idx_row: Failed to commit transaction.");
    }

    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error(
          "write_hash_idx_row: Failed to begin transaction.");
    }
  }

  rah->reset();

  if (!rah->peek_row_replica(tbl, le->cf_id, le->row_id, false, false, true)) {
    throw std::runtime_error("write_hash_idx_row: Failed to write row: peek");
  }

  if (!rah->write_row(le->data_size)) {
    throw std::runtime_error("write_hash_idx_row: Failed to write row: write");
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);
}

};  // namespace transaction
};  // namespace mica

#endif
