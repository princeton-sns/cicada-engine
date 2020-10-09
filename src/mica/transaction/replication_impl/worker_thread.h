#ifndef MICA_TRANSACTION_REPLICATION_IMPL_WORKER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_WORKER_THREAD_H_

#include "mica/transaction/replication.h"

#include <thread>

namespace mica {
namespace transaction {

template <class StaticConfig>
WorkerThread<StaticConfig>::WorkerThread(
    DB<StaticConfig>* db, tbb::concurrent_queue<LogEntryList*>* scheduler_queue,
    tbb::concurrent_queue<LogEntryList*> done_queue,
    pthread_barrier_t* start_barrier, uint16_t id, uint16_t nschedulers)
    : db_{db},
      scheduler_queue_{scheduler_queue},
      done_queue_{done_queue},
      start_barrier_{start_barrier},
      id_{id},
      nschedulers_{nschedulers},
      stop_{false},
      thread_{} {};

template <class StaticConfig>
WorkerThread<StaticConfig>::~WorkerThread(){};

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

  mica::util::lcore.pin_thread(id_ + nschedulers_);

  db_->activate(id_);

  Context<StaticConfig>* ctx = db_->context(id_);
  Transaction<StaticConfig> tx{ctx};
  RowAccessHandle<StaticConfig> rah{&tx};

  pthread_barrier_wait(start_barrier_);

  while (true) {
    LogEntryList* queue = nullptr;
    if (scheduler_queue_->try_pop(queue)) {
      LogEntryNode* node = queue->list;
      while (node != nullptr) {
        LogEntry<StaticConfig>* le =
            reinterpret_cast<LogEntry<StaticConfig>*>(node->ptr);

        // le->print();

        InsertRowLogEntry<StaticConfig>* irle = nullptr;
        WriteRowLogEntry<StaticConfig>* wrle = nullptr;
        switch (le->type) {
          case LogEntryType::INSERT_ROW:
            irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
            // irle->print();
            insert_row(ctx, &tx, &rah, irle);
            break;

          case LogEntryType::WRITE_ROW:
            wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
            // wrle->print();
            write_row(ctx, &tx, &rah, wrle);
            break;

          default:
            throw std::runtime_error(
                "WorkerThread::run: Unexpected log entry type.");
        }

        node = node->next;
      }
    } else if (scheduler_queue_->unsafe_size() == 0 && stop_) {
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

  db_->deactivate(id_);

  printf("Exiting replica worker: %u\n", id_);
};

template <class StaticConfig>
void WorkerThread<StaticConfig>::insert_row(
    Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  TableType tbl_type = static_cast<TableType>(le->tbl_type);

  switch (tbl_type) {
    case TableType::DATA:
      insert_data_row(ctx, tx, rah, le);
      break;
    case TableType::HASH_IDX:
      insert_hash_idx_row(ctx, tx, rah, le);
      break;
    default:
      throw std::runtime_error("Insert: Unsupported table type.");
  }
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::insert_data_row(
    Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  auto db = ctx->db();
  Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
  if (tbl == nullptr) {
    throw std::runtime_error("insert_data_row: Failed to find table " +
                             std::string{le->tbl_name});
  }

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
    Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, InsertRowLogEntry<StaticConfig>* le) {
  auto db = ctx->db();

  auto unique_hash_idx =
      db->get_hash_index_unique_u64(std::string{le->tbl_name});
  auto nonunique_hash_idx =
      db->get_hash_index_nonunique_u64(std::string{le->tbl_name});
  Table<StaticConfig>* tbl = nullptr;
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
void WorkerThread<StaticConfig>::write_row(Context<StaticConfig>* ctx,
                                           Transaction<StaticConfig>* tx,
                                           RowAccessHandle<StaticConfig>* rah,
                                           WriteRowLogEntry<StaticConfig>* le) {
  TableType tbl_type = static_cast<TableType>(le->tbl_type);

  switch (tbl_type) {
    case TableType::DATA:
      write_data_row(ctx, tx, rah, le);
      break;
    case TableType::HASH_IDX:
      write_hash_idx_row(ctx, tx, rah, le);
      break;
    default:
      throw std::runtime_error("Insert: Unsupported table type.");
  }
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::write_data_row(
    Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, WriteRowLogEntry<StaticConfig>* le) {
  auto db = ctx->db();

  Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
  if (tbl == nullptr) {
    throw std::runtime_error("write_data_row: Failed to find table " +
                             std::string{le->tbl_name});
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->has_began()) {
    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error("write_data_row: Failed to begin transaction.");
    }
  } else if (tx->ts() != txn_ts) {
    Result result;
    tx->commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("write_data_row: Failed to commit transaction.");
    }

    if (!tx->begin(false, &txn_ts)) {
      throw std::runtime_error("write_data_row: Failed to begin transaction.");
    }
  }

  rah->reset();

  if (!rah->peek_row_replica(tbl, le->cf_id, le->row_id, false, false, true) ||
      !rah->write_row(le->data_size)) {
    throw std::runtime_error("write_data_row: Failed to write row.");
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);
}

template <class StaticConfig>
void WorkerThread<StaticConfig>::write_hash_idx_row(
    Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
    RowAccessHandle<StaticConfig>* rah, WriteRowLogEntry<StaticConfig>* le) {
  auto db = ctx->db();
  auto unique_hash_idx =
      db->get_hash_index_unique_u64(std::string{le->tbl_name});
  auto nonunique_hash_idx =
      db->get_hash_index_nonunique_u64(std::string{le->tbl_name});
  Table<StaticConfig>* tbl = nullptr;
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
