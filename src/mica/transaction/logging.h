#pragma once
#ifndef MICA_TRANSACTION_LOGGING_H_
#define MICA_TRANSACTION_LOGGING_H_

#include <pthread.h>
#include <stdio.h>

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <deque>
#include <fstream>
#include <iostream>
#include <memory>
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <utility>

#include "mica/common.h"
#include "mica/transaction/context.h"
#include "mica/transaction/hash_index.h"
#include "mica/transaction/transaction.h"

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

  void flush();

  void change_logdir(std::string logdir);
  void copy_logs(std::string srcdir, std::string dstdir);
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

  void flush() {}

  void change_logdir(std::string logdir) { (void)logdir; }
  void copy_logs(std::string srcdir, std::string dstdir) {
    (void)srcdir;
    (void)dstdir;
  }
};

template <class StaticConfig>
class MmapLogger : public LoggerInterface<StaticConfig> {
 public:
  MmapLogger(uint16_t nthreads, std::string logdir);
  ~MmapLogger();

  bool log(const Context<StaticConfig>* ctx, const Table<StaticConfig>* tbl);

  template <bool UniqueKey>
  bool log(const Context<StaticConfig>* ctx,
           const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx);

  bool log(const Context<StaticConfig>* ctx,
           const Transaction<StaticConfig>* tx);

  void flush();

  void change_logdir(std::string logdir);
  void copy_logs(std::string srcdir, std::string dstdir);

 private:
  class Mmapping {
   public:
    void* addr;
    std::size_t len;
    int fd;

    Mmapping(void* addr, std::size_t len, int fd)
        : addr{addr}, len{len}, fd{fd} {}
    ~Mmapping() {}
  };

  class LogBuffer {
   public:
    char* start;
    char* end;
    char* cur;
    uint64_t cur_file_index;

    void print() {
      std::stringstream stream;

      stream << "Log Buffer:" << std::endl;
      stream << "Start: " << static_cast<void*>(start) << std::endl;
      stream << "End: " << static_cast<void*>(end) << std::endl;
      stream << "Cur: " << static_cast<void*>(cur) << std::endl;
      stream << "Cur File Index: " << cur_file_index << std::endl;

      std::cout << stream.str();
    }
  };

  uint16_t nthreads_;
  std::string logdir_;

  std::size_t len_;

  std::vector<std::vector<Mmapping>> mappings_;
  std::vector<LogBuffer> bufs_;

  LogBuffer mmap_log_buf(uint16_t thread_id, uint64_t file_index);

  char* alloc_log_buf(uint16_t thread_id, std::size_t nbytes);
  void release_log_buf(uint16_t thread_id, std::size_t nbytes);
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
    std::stringstream stream;

    stream << std::endl;
    stream << "Log entry:" << std::endl;
    stream << "Size: " << size << std::endl;
    stream << "Type: " << std::to_string(static_cast<uint8_t>(type))
           << std::endl;

    std::cout << stream.str();
  }
};

template <class StaticConfig>
class LogFile {
 public:
  LogFile<StaticConfig>* next = nullptr;
  uint64_t nentries;
  std::size_t size;
  LogEntry<StaticConfig> entries[0];

  void print() {
    std::stringstream stream;

    stream << "Log file:" << std::endl;
    stream << "N entries: " << nentries << std::endl;
    stream << "Size: " << size << std::endl;
    stream << "First entry ptr: " << &entries[0] << std::endl;

    std::cout << stream.str();
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

    std::stringstream stream;

    stream << "Name: " << std::string{name} << std::endl;
    stream << "CF count: " << cf_count << std::endl;

    for (uint16_t cf_id = 0; cf_id < cf_count; cf_id++) {
      stream << "data_size_hints[" << cf_id << "]: " << data_size_hints[cf_id]
             << std::endl;
    }

    std::cout << stream.str();
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

    std::stringstream stream;

    stream << "Name: " << std::string{name} << std::endl;
    stream << "Main Table Name: " << std::string{main_tbl_name} << std::endl;
    stream << "Expected Row Count: " << expected_num_rows << std::endl;
    stream << "UniqueKey: " << unique_key << std::endl;

    std::cout << stream.str();
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

    std::stringstream stream;

    stream << "Table Name: " << std::string{tbl_name} << std::endl;
    stream << "Table Type: " << std::to_string(tbl_type) << std::endl;
    stream << "Column Family ID: " << cf_id << std::endl;
    stream << "Row ID: " << row_id << std::endl;
    stream << "Transaction TS: " << txn_ts << std::endl;
    stream << "Write TS: " << wts << std::endl;
    stream << "Read TS: " << rts << std::endl;
    stream << "Data Size: " << data_size << std::endl;

    std::cout << stream.str();
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

    std::stringstream stream;

    stream << "Table Name: " << std::string{tbl_name} << std::endl;
    stream << "Table Type: " << std::to_string(tbl_type) << std::endl;
    stream << "Column Family ID: " << cf_id << std::endl;
    stream << "Row ID: " << row_id << std::endl;
    stream << "Transaction TS: " << txn_ts << std::endl;
    stream << "Write TS: " << wts << std::endl;
    stream << "Read TS: " << rts << std::endl;
    stream << "Data Size: " << data_size << std::endl;

    std::cout << stream.str();
  }
};

}  // namespace transaction
}  // namespace mica

#include "logging_impl/mmap_logger.h"

#endif
