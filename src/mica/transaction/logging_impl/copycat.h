#pragma once
#ifndef MICA_TRANSACTION_COPYCAT_H_
#define MICA_TRANSACTION_COPYCAT_H_

#include <thread>

#include "mica/transaction/logging.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {

using mica::util::PosixIO;

template <class StaticConfig>
CopyCat<StaticConfig>::CopyCat(DB<StaticConfig>* db, uint16_t nloggers,
                               uint16_t nworkers, std::string logdir)
    : db_{db},
      len_{StaticConfig::kPageSize},
      nloggers_{nloggers},
      nworkers_{nworkers},
      logdir_{logdir},
      workers_{},
      workers_stop_{false}
{
  int ret = pthread_barrier_init(&worker_barrier_, nullptr, nworkers + 1);
  if (ret != 0) {
    throw std::runtime_error("Failed to init worker barrier: " + ret);
  }
}

template <class StaticConfig>
CopyCat<StaticConfig>::~CopyCat() {
  int ret = pthread_barrier_destroy(&worker_barrier_);
  if (ret != 0) {
    throw std::runtime_error("Failed to destroy worker barrier: " + ret);
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_workers() {
  for (uint16_t wid = 0; wid < nworkers_; wid++) {
    workers_.emplace_back(
        std::thread{&CopyCat<StaticConfig>::worker_thread, this, db_, wid});
  }

  pthread_barrier_wait(&worker_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_workers() {
  workers_stop_ = true;

  for (auto& w : workers_) {
    w.join();
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::create_table(
    DB<StaticConfig>* db, CreateTableLogEntry<StaticConfig>* ctle) {
  if (!db->create_table(std::string{ctle->name}, ctle->cf_count,
                        ctle->data_size_hints)) {
    throw std::runtime_error("Failed to create table: " +
                             std::string{ctle->name});
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::create_hash_index(
    DB<StaticConfig>* db, CreateHashIndexLogEntry<StaticConfig>* chile) {
  if (chile->unique_key) {
    if (!db->create_hash_index_unique_u64(
            std::string{chile->name},
            db->get_table(std::string{chile->main_tbl_name}),
            chile->expected_num_rows)) {
      throw std::runtime_error("Failed to create unique index: " +
                               std::string{chile->name});
    }

  } else if (!db->create_hash_index_nonunique_u64(
                 std::string{chile->name},
                 db->get_table(std::string{chile->main_tbl_name}),
                 chile->expected_num_rows)) {
    throw std::runtime_error("Failed to create unique index: " +
                             std::string{chile->name});
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::insert_row(Context<StaticConfig>* ctx,
                                       Transaction<StaticConfig>* tx,
                                       RowAccessHandle<StaticConfig>* rah,
                                       InsertRowLogEntry<StaticConfig>* le) {
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
void CopyCat<StaticConfig>::insert_data_row(Context<StaticConfig>* ctx,
                                            Transaction<StaticConfig>* tx,
                                            RowAccessHandle<StaticConfig>* rah,
                                            InsertRowLogEntry<StaticConfig>* le) {
  auto db = ctx->db();
  Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
  if (tbl == nullptr) {
    throw std::runtime_error("Failed to find table " +
                             std::string{le->tbl_name});
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->begin(false, &txn_ts)) {
    throw std::runtime_error("Failed to begin transaction.");
  }

  if (!rah->new_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
    throw std::runtime_error("Failed to create new row " + le->row_id);
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);

  Result result;
  tx->commit(&result);
  rah->reset();
  if (result != Result::kCommitted) {
    throw std::runtime_error("Failed to commit transaction.");
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::insert_hash_idx_row(Context<StaticConfig>* ctx,
                                                Transaction<StaticConfig>* tx,
                                                RowAccessHandle<StaticConfig>* rah,
                                                InsertRowLogEntry<StaticConfig>* le) {
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
    throw std::runtime_error("Failed to find index table.");
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->begin(false, &txn_ts)) {
    throw std::runtime_error("Failed to begin transaction.");
  }

  if (!rah->new_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
    throw std::runtime_error("Failed to create new row " + le->row_id);
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);

  Result result;
  tx->commit(&result);
  rah->reset();
  if (result != Result::kCommitted) {
    throw std::runtime_error("Failed to commit transaction.");
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::write_row(Context<StaticConfig>* ctx,
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
void CopyCat<StaticConfig>::write_data_row(Context<StaticConfig>* ctx,
                                           Transaction<StaticConfig>* tx,
                                           RowAccessHandle<StaticConfig>* rah,
                                           WriteRowLogEntry<StaticConfig>* le) {
  auto db = ctx->db();

  Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
  if (tbl == nullptr) {
    throw std::runtime_error("Failed to find table " +
                             std::string{le->tbl_name});
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->begin(false, &txn_ts)) {
    throw std::runtime_error("Failed to begin transaction.");
  }

  if (!rah->template peek_row<true>(tbl, le->cf_id, le->row_id, false, false, true) ||
      !rah->write_row(le->data_size)) {
    throw std::runtime_error("Failed to write row.");
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);

  Result result;
  tx->commit(&result);
  rah->reset();
  if (result != Result::kCommitted) {
    throw std::runtime_error("Failed to commit transaction.");
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::write_hash_idx_row(Context<StaticConfig>* ctx,
                                               Transaction<StaticConfig>* tx,
                                               RowAccessHandle<StaticConfig>* rah,
                                               WriteRowLogEntry<StaticConfig>* le) {
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
    throw std::runtime_error("Failed to find index table.");
  }

  typename StaticConfig::Timestamp txn_ts;
  txn_ts.t2 = le->txn_ts;

  if (!tx->begin(false, &txn_ts)) {
    throw std::runtime_error("Failed to begin transaction.");
  }

  if (!rah->template peek_row<true>(tbl, le->cf_id, le->row_id, false, false, true) ||
      !rah->write_row(le->data_size)) {
    throw std::runtime_error("Failed to write row.");
  }

  char* data = rah->data();
  std::memcpy(data, le->data, le->data_size);

  Result result;
  tx->commit(&result);
  rah->reset();
  if (result != Result::kCommitted) {
    throw std::runtime_error("Failed to commit transaction.");
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::worker_thread(DB<StaticConfig>* db, uint16_t id) {
  printf("Starting replica worker: %u\n", id);

  mica::util::lcore.pin_thread(id);
  db->activate(id);

  Context<StaticConfig>* ctx = db->context(id);
  Transaction<StaticConfig> tx {ctx};
  RowAccessHandle<StaticConfig> rah {&tx};

  pthread_barrier_wait(&worker_barrier_);

  while (true) {
    for (uint64_t file_index = 0;; file_index++) {
      std::string fname = std::string{MICA_RELAY_DIR} + "/out." +
                          std::to_string(id) + "." +
                          std::to_string(file_index) + ".log";

      if (!PosixIO::Exists(fname.c_str())) break;

      int fd = PosixIO::Open(fname.c_str(), O_RDONLY);
      void* start = PosixIO::Mmap(nullptr, len_, PROT_READ, MAP_SHARED, fd, 0);

      LogFile<StaticConfig>* lf = static_cast<LogFile<StaticConfig>*>(start);
      // lf->print();

      char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

      InsertRowLogEntry<StaticConfig>* irle = nullptr;
      WriteRowLogEntry<StaticConfig>* wrle = nullptr;

      for (uint64_t i = 0; i < lf->nentries; i++) {
        LogEntry<StaticConfig>* le =
            reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
        // le->print();

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

          case LogEntryType::CREATE_TABLE:
          case LogEntryType::CREATE_HASH_IDX:
            break;
          default:
            throw std::runtime_error("worker_thread: Unexpected log entry type.");
        }

        ptr += le->size;
      }

      PosixIO::Munmap(start, len_);
      PosixIO::Close(fd);
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (workers_stop_) break;
  }

  db->deactivate(id);
  printf("Exiting replica worker: %u\n", id);
}

template <class StaticConfig>
  void CopyCat<StaticConfig>::preprocess_logs() {
  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    for (uint64_t file_index = 0;; file_index++) {
      std::string fname = logdir_ + "/out." +
                          std::to_string(thread_id) + "." +
                          std::to_string(file_index) + ".log";

      if (!PosixIO::Exists(fname.c_str())) break;

      int fd = PosixIO::Open(fname.c_str(), O_RDONLY);
      void* start = PosixIO::Mmap(nullptr, len_, PROT_READ, MAP_SHARED, fd, 0);

      LogFile<StaticConfig>* lf = static_cast<LogFile<StaticConfig>*>(start);
      lf->print();

      char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* chile = nullptr;

      for (uint64_t i = 0; i < lf->nentries; i++) {
        LogEntry<StaticConfig>* le =
            reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
        // le->print();

        switch (le->type) {
          case LogEntryType::CREATE_TABLE:
            ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);
            create_table(db_, ctle);
            // ctle->print();
            break;

          case LogEntryType::CREATE_HASH_IDX:
            chile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);
            create_hash_index(db_, chile);
            // chile->print();
            break;

          case LogEntryType::INSERT_ROW:
          case LogEntryType::WRITE_ROW:
            break;
          default:
            throw std::runtime_error("preprocess_logs: Unexpected log entry type.");
        }

        ptr += le->size;
      }

      PosixIO::Munmap(start, len_);
      PosixIO::Close(fd);
    }
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::read_logs() {
  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    for (uint64_t file_index = 0;; file_index++) {
      std::string fname = std::string{MICA_RELAY_DIR} + "/out." +
                          std::to_string(thread_id) + "." +
                          std::to_string(file_index) + ".log";

      if (!PosixIO::Exists(fname.c_str())) break;

      int fd = PosixIO::Open(fname.c_str(), O_RDONLY);
      void* start = PosixIO::Mmap(nullptr, len_, PROT_READ, MAP_SHARED, fd, 0);

      LogFile<StaticConfig>* lf = static_cast<LogFile<StaticConfig>*>(start);
      lf->print();

      char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* chile = nullptr;
      InsertRowLogEntry<StaticConfig>* irle = nullptr;
      WriteRowLogEntry<StaticConfig>* wrle = nullptr;

      for (uint64_t i = 0; i < lf->nentries; i++) {
        LogEntry<StaticConfig>* le =
            reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
        // le->print();

        switch (le->type) {
          case LogEntryType::CREATE_TABLE:
            ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);
            // ctle->print();
            break;

          case LogEntryType::CREATE_HASH_IDX:
            chile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);
            // chile->print();
            break;

          case LogEntryType::INSERT_ROW:
            irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
            // irle->print();
            break;

          case LogEntryType::WRITE_ROW:
            wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
            // wrle->print();
            break;
        }

        ptr += le->size;
      }

      PosixIO::Munmap(start, len_);
      PosixIO::Close(fd);
    }
  }
}

}  // namespace transaction
};  // namespace mica

#endif
