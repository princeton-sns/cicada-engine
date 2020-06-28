#pragma once
#ifndef MICA_TRANSACTION_LOGGING_H_
#define MICA_TRANSACTION_LOGGING_H_

#include <stdio.h>

#include <atomic>
#include <chrono>
#include <iostream>
#include <fstream>
#include <sstream>
#include <string>
#include <thread>

#include "mica/common.h"
#include "mica/transaction/context.h"
#include "mica/transaction/db.h"
#include "mica/transaction/page_pool.h"
#include "mica/transaction/row.h"
#include "mica/transaction/row_version_pool.h"
#include "mica/transaction/transaction.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class LoggerInterface {
 public:
  bool log(const Context<StaticConfig>* ctx, const Table<StaticConfig>* tbl);

  template <bool UniqueKey>
  bool log(const Context<StaticConfig>* ctx,
           const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx);

  bool log(const Context<StaticConfig>* ctx,
           const Transaction<StaticConfig>* tx);
};

template <class StaticConfig>
class NullLogger : public LoggerInterface<StaticConfig> {
 public:
  bool log(const Context<StaticConfig>* ctx, const Table<StaticConfig>* tbl) {
    (void)ctx;
    (void)tbl;
    return true;
  }

  template <bool UniqueKey>
  bool log(const Context<StaticConfig>* ctx,
           const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx) {
    (void)ctx;
    (void)idx;
    return true;
  }

  bool log(const Context<StaticConfig>* ctx,
           const Transaction<StaticConfig>* tx) {
    (void)ctx;
    (void)tx;
    return true;
  }
};

enum class LogEntryType : uint8_t {
  CREATE_TABLE = 0,
  CREATE_HASH_IDX,
  INSERT_ROW,
  WRITE_ROW,
};

template <class StaticConfig>
class LogEntry {
 public:
  std::size_t size;
  LogEntryType type;

  void print() {
    std::cout << std::endl;
    std::cout << "Log entry:" << std::endl;
    std::cout << "Size: " << size << std::endl;
    std::cout << "Type: " << std::to_string(static_cast<uint8_t>(type))
              << std::endl;
  }
};

template <class StaticConfig>
class CreateTableLogEntry : public LogEntry<StaticConfig> {
 public:
  uint64_t data_size_hints[StaticConfig::kMaxColumnFamilyCount];
  char name[StaticConfig::kMaxTableNameSize];
  uint16_t cf_count;

  void print() {
    LogEntry<StaticConfig>::print();

    std::cout << "Name: " << std::string{name} << std::endl;
    std::cout << "CF count: " << cf_count << std::endl;

    for (uint16_t cf_id = 0; cf_id < cf_count; cf_id++) {
      std::cout << "data_size_hints[" << cf_id
                << "]: " << data_size_hints[cf_id] << std::endl;
    }
  }
};

template <class StaticConfig>
class CreateHashIndexLogEntry : public LogEntry<StaticConfig> {
 public:
  uint64_t expected_num_rows;
  char name[StaticConfig::kMaxHashIndexNameSize];
  char main_tbl_name[StaticConfig::kMaxTableNameSize];
  bool unique_key;

  void print() {
    LogEntry<StaticConfig>::print();

    std::cout << "Name: " << std::string{name} << std::endl;
    std::cout << "Main Table Name: " << std::string{main_tbl_name} << std::endl;
    std::cout << "Expected Row Count: " << expected_num_rows << std::endl;
    std::cout << "UniqueKey: " << unique_key << std::endl;
  }
};

template <class StaticConfig>
class InsertRowLogEntry : public LogEntry<StaticConfig> {
 public:
  uint64_t txn_ts;

  uint64_t row_id;

  uint64_t wts;
  uint64_t rts;

  uint32_t data_size;

  uint16_t cf_id;

  uint8_t tbl_type;

  char tbl_name[StaticConfig::kMaxTableNameSize];
  char data[0] __attribute__((aligned(8)));

  void print() {
    LogEntry<StaticConfig>::print();

    std::cout << "Table Name: " << std::string{tbl_name} << std::endl;
    std::cout << "Table Type: " << std::to_string(tbl_type) << std::endl;
    std::cout << "Column Family ID: " << cf_id << std::endl;
    std::cout << "Row ID: " << row_id << std::endl;
    std::cout << "Transaction TS: " << txn_ts << std::endl;
    std::cout << "Write TS: " << wts << std::endl;
    std::cout << "Read TS: " << rts << std::endl;
    std::cout << "Data Size: " << data_size << std::endl;
  }
};

template <class StaticConfig>
class WriteRowLogEntry : public LogEntry<StaticConfig> {
 public:
  uint64_t txn_ts;

  uint64_t row_id;

  uint64_t wts;
  uint64_t rts;

  uint32_t data_size;

  uint16_t cf_id;

  uint8_t tbl_type;

  char tbl_name[StaticConfig::kMaxTableNameSize];
  char data[0] __attribute__((aligned(8)));

  void print() {
    LogEntry<StaticConfig>::print();

    std::cout << "Table Name: " << std::string{tbl_name} << std::endl;
    std::cout << "Table Type: " << std::to_string(tbl_type) << std::endl;
    std::cout << "Column Family ID: " << cf_id << std::endl;
    std::cout << "Row ID: " << row_id << std::endl;
    std::cout << "Transaction TS: " << txn_ts << std::endl;
    std::cout << "Write TS: " << wts << std::endl;
    std::cout << "Read TS: " << rts << std::endl;
    std::cout << "Data Size: " << data_size << std::endl;
  }
};

template <class StaticConfig>
class FileLogger : public LoggerInterface<StaticConfig> {
 private:
  std::vector<int> files_;

  std::thread log_consumer_;
  std::atomic<bool> log_consumer_stop_;

 public:
  FileLogger(uint16_t nthreads) : files_{}, log_consumer_stop_{false} {
    for (auto i = 0; i < nthreads; i++) {
      std::stringstream fname;
      fname << StaticConfig::kDBLogDir << "/out." << i << ".log";

      files_.push_back(::mica::util::PosixIO::Open(
          fname.str().c_str(), O_WRONLY | O_APPEND | O_CREAT,
          S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH));
    }
  }

  ~FileLogger() {
    for (int fd : files_) {
      ::mica::util::PosixIO::Close(fd);
    }

    files_.clear();
  }

  void start_log_consumer(Context<StaticConfig>* ctx) {
    log_consumer_ = std::thread{&FileLogger::log_consumer_thread, this, ctx};
  }

  void stop_log_consumer() {
    log_consumer_stop_ = true;
    log_consumer_.join();
  }

  bool flush_log(Context<StaticConfig>* ctx) {
    int fd = files_[static_cast<std::size_t>(ctx->thread_id())];
    ::mica::util::PosixIO::FSync(fd);
    return true;
  }

  bool log(Context<StaticConfig>* ctx, const Table<StaticConfig>* tbl) {
    uint16_t thread_id = ctx->thread_id();
    int fd = files_[static_cast<int>(thread_id)];

    CreateTableLogEntry<StaticConfig> le;

    le.size = sizeof le;
    le.type = LogEntryType::CREATE_TABLE;
    le.cf_count = tbl->cf_count();

    std::memcpy(&le.name[0], tbl->name().c_str(), 1 + tbl->name().size());
    for (uint16_t cf_id = 0; cf_id < le.cf_count; cf_id++) {
      le.data_size_hints[cf_id] = tbl->data_size_hint(cf_id);
    }

    ::mica::util::PosixIO::Write(fd, &le, le.size);

    return true;
  }

  template <bool UniqueKey>
  bool log(Context<StaticConfig>* ctx,
           const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx) {
    uint16_t thread_id = ctx->thread_id();
    int fd = files_[static_cast<int>(thread_id)];

    CreateHashIndexLogEntry<StaticConfig> le;

    le.size = sizeof le;
    le.type = LogEntryType::CREATE_HASH_IDX;
    le.expected_num_rows = idx->expected_num_rows();
    le.unique_key = UniqueKey;

    std::memcpy(&le.name[0], idx->index_table()->name().c_str(),
                1 + idx->index_table()->name().size());

    std::memcpy(&le.main_tbl_name[0], idx->main_table()->name().c_str(),
                1 + idx->main_table()->name().size());

    ::mica::util::PosixIO::Write(fd, &le, le.size);

    return true;
  }

  bool log(Context<StaticConfig>* ctx, const Transaction<StaticConfig>* tx) {
    uint16_t thread_id = ctx->thread_id();
    int fd = files_[static_cast<int>(thread_id)];
    PagePool<StaticConfig>* page_pool = ctx->db()->page_pool(ctx->numa_id());

    char* page = page_pool->allocate();

    auto accesses = tx->accesses();
    auto iset_idx = tx->iset_idx();

    for (auto j = 0; j < tx->iset_size(); j++) {
      int i = iset_idx[j];
      RowAccessItem<StaticConfig> item = accesses[i];
      RowVersion<StaticConfig>* write_rv = item.write_rv;
      uint32_t data_size = write_rv->data_size;
      char* data = write_rv->data;
      Table<StaticConfig>* tbl = item.tbl;
      InsertRowLogEntry<StaticConfig>* le =
          reinterpret_cast<InsertRowLogEntry<StaticConfig>*>(page);

      std::size_t size = sizeof *le + data_size;

      if (size > PagePool<StaticConfig>::kPageSize) {
        throw std::runtime_error("WARNING: Log entry is larger than a page!");
      }

      le->size = size;
      le->type = LogEntryType::INSERT_ROW;

      le->txn_ts = tx->ts().t2;
      le->cf_id = item.cf_id;
      le->row_id = item.row_id;

      le->wts = write_rv->wts.t2;
      le->rts = write_rv->rts.get().t2;

      le->data_size = data_size;
      le->tbl_type = static_cast<uint8_t>(tbl->type());

      std::memcpy(&le->tbl_name[0], tbl->name().c_str(),
                  1 + tbl->name().size());
      std::memcpy(le->data, data, data_size);

      // le->print();

      ::mica::util::PosixIO::Write(fd, le, size);
    }

    auto wset_idx = tx->wset_idx();

    for (auto j = 0; j < tx->wset_size(); j++) {
      int i = wset_idx[j];
      RowAccessItem<StaticConfig> item = accesses[i];
      RowVersion<StaticConfig>* write_rv = item.write_rv;
      uint32_t data_size = write_rv->data_size;
      char* data = write_rv->data;
      Table<StaticConfig>* tbl = item.tbl;
      WriteRowLogEntry<StaticConfig>* le =
          reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(page);

      std::size_t size = sizeof *le + data_size;

      if (size > PagePool<StaticConfig>::kPageSize) {
        throw std::runtime_error("WARNING: Log entry is larger than a page!");
      }

      le->size = size;
      le->type = LogEntryType::WRITE_ROW;

      le->txn_ts = tx->ts().t2;
      le->cf_id = item.cf_id;
      le->row_id = item.row_id;

      le->wts = write_rv->wts.t2;
      le->rts = write_rv->rts.get().t2;

      le->data_size = data_size;
      le->tbl_type = static_cast<uint8_t>(tbl->type());

      std::memcpy(&le->tbl_name[0], tbl->name().c_str(),
                  1 + tbl->name().size());
      std::memcpy(le->data, data, data_size);

      // le->print();

      ::mica::util::PosixIO::Write(fd, le, size);
    }

    page_pool->free(page);

    return true;
  }

 private:
  bool read_log_entry(int fd, char* buf, std::size_t max_size) {
    LogEntry<StaticConfig>* rle = nullptr;

    LogEntry<StaticConfig> le;

    if (::mica::util::PosixIO::Read(fd, &le, sizeof le) == sizeof le) {
      if (le.size > max_size) {
        throw std::runtime_error(
            "WARNING: Log entry is larger than buffer size!");
      }

      switch (le.type) {
        case LogEntryType::CREATE_TABLE:
          rle = reinterpret_cast<CreateTableLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          // static_cast<CreateTableLogEntry<StaticConfig>*>(rle)->print();
          break;

        case LogEntryType::CREATE_HASH_IDX:
          rle = reinterpret_cast<CreateHashIndexLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          // static_cast<CreateHashIndexLogEntry<StaticConfig>*>(rle)->print();
          break;

        case LogEntryType::INSERT_ROW:
          rle = reinterpret_cast<InsertRowLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          // static_cast<InsertRowLogEntry<StaticConfig>*>(rle)->print();
          break;

        case LogEntryType::WRITE_ROW:
          rle = reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          // static_cast<WriteRowLogEntry<StaticConfig>*>(rle)->print();
          break;
      }

      return true;
    }

    return false;
  }

  void insert_data_row(Context<StaticConfig>* ctx,
                       InsertRowLogEntry<StaticConfig>* le) {
    auto db = ctx->db();
    Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
    if (tbl == nullptr) {
      throw std::runtime_error("Failed to find table " +
                               std::string{le->tbl_name});
    }

    typename StaticConfig::Timestamp txn_ts;
    txn_ts.t2 = le->txn_ts;

    Transaction<StaticConfig> tx(db->context(ctx->thread_id()));
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.new_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
      throw std::runtime_error("Failed to create new row " + le->row_id);
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void insert_hash_idx_row(Context<StaticConfig>* ctx,
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

    Transaction<StaticConfig> tx(db->context(ctx->thread_id()));
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.new_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
      throw std::runtime_error("Failed to create new row " + le->row_id);
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void insert_row(Context<StaticConfig>* ctx,
                  InsertRowLogEntry<StaticConfig>* le) {
    TableType tbl_type = static_cast<TableType>(le->tbl_type);

    switch (tbl_type) {
      case TableType::DATA:
        insert_data_row(ctx, le);
        break;
      case TableType::HASH_IDX:
        insert_hash_idx_row(ctx, le);
        break;
      default:
        throw std::runtime_error("Insert: Unsupported table type.");
    }
  }

  void write_data_row(Context<StaticConfig>* ctx,
                      WriteRowLogEntry<StaticConfig>* le) {
    auto db = ctx->db();

    Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
    if (tbl == nullptr) {
      throw std::runtime_error("Failed to find table " +
                               std::string{le->tbl_name});
    }

    typename StaticConfig::Timestamp txn_ts;
    txn_ts.t2 = le->txn_ts;

    Transaction<StaticConfig> tx(db->context(ctx->thread_id()));
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.peek_row(tbl, le->cf_id, le->row_id, false, false, true) ||
        !rah.write_row(le->data_size)) {
      throw std::runtime_error("Failed to write row.");
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void write_hash_idx_row(Context<StaticConfig>* ctx,
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

    Transaction<StaticConfig> tx(db->context(ctx->thread_id()));
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.peek_row(tbl, le->cf_id, le->row_id, false, false, true) ||
        !rah.write_row(le->data_size)) {
      throw std::runtime_error("Failed to write row.");
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void write_row(Context<StaticConfig>* ctx,
                 WriteRowLogEntry<StaticConfig>* le) {
    TableType tbl_type = static_cast<TableType>(le->tbl_type);

    switch (tbl_type) {
      case TableType::DATA:
        write_data_row(ctx, le);
        break;
      case TableType::HASH_IDX:
        write_hash_idx_row(ctx, le);
        break;
      default:
        throw std::runtime_error("Insert: Unsupported table type.");
    }
  }

  void log_consumer_thread(Context<StaticConfig>* ctx) {
    printf("Starting log consumer\n");
    auto db = ctx->db();

    PagePool<StaticConfig>* page_pool = db->page_pool(ctx->numa_id());
    char* page = page_pool->allocate();

    while (true) {
      uint16_t tid = 0;
      std::stringstream fname;
      fname << StaticConfig::kRelayLogDir << "/out." << tid << ".log";
      int fd = ::mica::util::PosixIO::Open(fname.str().c_str(), O_RDONLY);

      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* hile = nullptr;
      InsertRowLogEntry<StaticConfig>* irle = nullptr;
      WriteRowLogEntry<StaticConfig>* wrle = nullptr;

      while (read_log_entry(fd, page, PagePool<StaticConfig>::kPageSize)) {
        LogEntry<StaticConfig>* le =
            reinterpret_cast<LogEntry<StaticConfig>*>(page);

        switch (le->type) {
          case LogEntryType::CREATE_TABLE:
            ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);

            if (!db->create_table(std::string{ctle->name}, ctle->cf_count,
                                  ctle->data_size_hints)) {
              throw std::runtime_error("Failed to create table: " +
                                       std::string{ctle->name});
            }

            break;
          case LogEntryType::CREATE_HASH_IDX:
            hile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);

            if (hile->unique_key) {
              if (!db->create_hash_index_unique_u64(
                      std::string{hile->name},
                      db->get_table(std::string{hile->main_tbl_name}),
                      hile->expected_num_rows)) {
                throw std::runtime_error("Failed to create unique index: " +
                                         std::string{hile->name});
              }

            } else if (!db->create_hash_index_nonunique_u64(
                           std::string{hile->name},
                           db->get_table(std::string{hile->main_tbl_name}),
                           hile->expected_num_rows)) {
              throw std::runtime_error("Failed to create unique index: " +
                                       std::string{hile->name});
            }

            break;

          case LogEntryType::INSERT_ROW:
            irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
            insert_row(ctx, irle);
            break;

          case LogEntryType::WRITE_ROW:
            wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
            write_row(ctx, wrle);
            break;
        };
      }

      std::this_thread::sleep_for(std::chrono::milliseconds(10));
      if (log_consumer_stop_) break;
    }

    page_pool->free(page);

    printf("Exiting log consumer\n");
  }
};
}  // namespace transaction
}  // namespace mica

#endif
