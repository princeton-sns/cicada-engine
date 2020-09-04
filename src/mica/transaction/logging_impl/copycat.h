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
      workers_stop_{false} {
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
  workers_stop_ = false;
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

  workers_.clear();
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
void CopyCat<StaticConfig>::insert_data_row(
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
void CopyCat<StaticConfig>::insert_hash_idx_row(
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
void CopyCat<StaticConfig>::write_hash_idx_row(
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

template <class StaticConfig>
void CopyCat<StaticConfig>::worker_thread(DB<StaticConfig>* db, uint16_t id) {
  printf("Starting replica worker: %u\n", id);

  mica::util::lcore.pin_thread(id);
  db->activate(id);

  Context<StaticConfig>* ctx = db->context(id);
  Transaction<StaticConfig> tx{ctx};
  RowAccessHandle<StaticConfig> rah{&tx};

  pthread_barrier_wait(&worker_barrier_);

  while (true) {
    for (uint64_t file_index = 0;; file_index++) {
      std::string fname = logdir_ + "/out." + std::to_string(id) + "." +
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
            throw std::runtime_error(
                "worker_thread: Unexpected log entry type.");
        }

        ptr += le->size;
      }

      PosixIO::Munmap(start, len_);
      PosixIO::Close(fd);
    }

    // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    if (workers_stop_) break;
  }

  if (tx.has_began()) {
    Result result;
    tx.commit_replica(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  db->deactivate(id);
  printf("Exiting replica worker: %u\n", id);
}

template <class StaticConfig>
typename CopyCat<StaticConfig>::MmappedLogFile
CopyCat<StaticConfig>::try_open_log_file(std::string logdir, uint16_t thread_id,
                                         uint64_t file_index) {
  std::string fname = logdir + "/out." + std::to_string(thread_id) + "." +
                      std::to_string(file_index) + ".log";

  if (PosixIO::Exists(fname.c_str())) {
    int fd = PosixIO::Open(fname.c_str(), O_RDONLY);
    void* start = PosixIO::Mmap(nullptr, len_, PROT_READ, MAP_SHARED, fd, 0);

    LogFile<StaticConfig>* lf = static_cast<LogFile<StaticConfig>*>(start);

    char* cur_ptr = reinterpret_cast<char*>(&lf->entries[0]);
    uint64_t nentries = lf->nentries;
    uint64_t cur_n = 0;

    return MmappedLogFile{thread_id, file_index, start, cur_ptr, nentries, cur_n, len_, fd};
  } else {
    return MmappedLogFile{0, 0, nullptr, nullptr, 0, 0, 0, -1};
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::preprocess_logs() {
  const std::string outfname = logdir_ + "/out.log";

  // Find file size
  std::size_t outf_size = 0;
  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    for (uint64_t file_index = 0;; file_index++) {
      MmappedLogFile mlf = try_open_log_file(logdir_, thread_id, file_index);
      if (mlf.fd == -1) break;

      LogFile<StaticConfig>* lf =
          static_cast<LogFile<StaticConfig>*>(mlf.start);
      lf->print();

      outf_size += lf->size;

      PosixIO::Munmap(mlf.start, mlf.len);
      PosixIO::Close(mlf.fd);
    }
  }

  // Allocate out file
  int out_fd = PosixIO::Open(outfname.c_str(), O_RDWR | O_CREAT,
                             S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  outf_size = len_ * ((outf_size + (len_ - 1)) / len_); // Round up to next multiple of len_
  PosixIO::Ftruncate(out_fd, static_cast<off_t>(outf_size));
  void* out_addr = PosixIO::Mmap(nullptr, outf_size, PROT_READ | PROT_WRITE,
                                 MAP_SHARED, out_fd, 0);
  char* out_cur = static_cast<char*>(out_addr);
  LogFile<StaticConfig>* lf = reinterpret_cast<LogFile<StaticConfig>*>(out_cur);
  lf->nentries = 0;
  lf->size = sizeof(LogFile<StaticConfig>);
  out_cur = reinterpret_cast<char*>(&lf->entries[0]);

  std::vector<MmappedLogFile> lfs{};
  std::vector<std::size_t> lfs_to_remove{};

  uint64_t file_index = 0;
  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    MmappedLogFile mlf = try_open_log_file(logdir_, thread_id, file_index);
    if (mlf.fd != -1 && mlf.nentries != 0) {
      lfs.push_back(mlf);
    }
  }

  // Sort log files by transaction timestamp
  while (lfs.size() != 0) {
    uint64_t min_txn_ts = static_cast<uint64_t>(-1);
    char* min_addr = nullptr;
    std::size_t min_size = 0;

    // Find log file with min transaction timestamp
    for (std::size_t i = 0; i < lfs.size(); i++) {
      MmappedLogFile lf = lfs[i];

      LogEntry<StaticConfig>* le =
          reinterpret_cast<LogEntry<StaticConfig>*>(lf.cur_ptr);

      le->print();

      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* chile = nullptr;
      InsertRowLogEntry<StaticConfig>* irle = nullptr;
      WriteRowLogEntry<StaticConfig>* wrle = nullptr;

      switch (le->type) {
        case LogEntryType::CREATE_TABLE:
          ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);
          // ctle->print();
          create_table(db_, ctle);
          break;

        case LogEntryType::CREATE_HASH_IDX:
          chile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);
          // chile->print();
          create_hash_index(db_, chile);
          break;

        case LogEntryType::INSERT_ROW:
          irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
          // irle->print();
          if (irle->txn_ts < min_txn_ts) {
            min_txn_ts = irle->txn_ts;
            min_addr = reinterpret_cast<char*>(le);
            min_size = le->size;
          }
          break;

        case LogEntryType::WRITE_ROW:
          wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
          // wrle->print();
          if (wrle->txn_ts < min_txn_ts) {
            min_txn_ts = wrle->txn_ts;
            min_addr = reinterpret_cast<char*>(le);
            min_size = le->size;
          }
          break;

        default:
          throw std::runtime_error(
              "preprocess_logs: Unexpected log entry type.");
      }

      lf.cur_ptr += le->size;
      lf.cur_n += 1;
      if (lf.cur_n == lf.nentries) {
        lfs_to_remove.push_back(i);
      }

      lfs[i] = lf;
    }

    if (min_txn_ts != static_cast<uint64_t>(-1)) {
      printf("min_txn_ts: %lu\n", min_txn_ts);
      std::memcpy(out_cur, min_addr, min_size);
      out_cur += min_size;
    }

    // Remove lfs
    std::sort(lfs_to_remove.begin(), lfs_to_remove.end(), std::greater<int>());
    for (std::size_t i : lfs_to_remove) {
      MmappedLogFile lf = lfs[i];
      PosixIO::Munmap(lf.start, lf.len);
      PosixIO::Close(lf.fd);

      lf = try_open_log_file(logdir_, lf.thread_id, lf.file_index + 1);
      if (lf.fd == -1) {  // No more log files for this thread ID
        lfs.erase(lfs.begin() + static_cast<long int>(i));
      } else {
        lfs[i] = lf;
      }
    }
    lfs_to_remove.clear();
  }

  PosixIO::Munmap(out_addr, outf_size);
  PosixIO::Close(out_fd);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::read_logs() {
  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    for (uint64_t file_index = 0;; file_index++) {
      std::string fname = logdir_ + "/out." + std::to_string(thread_id) + "." +
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
