#pragma once
#ifndef MICA_TRANSACTION_LOG_FORMAT_H_
#define MICA_TRANSACTION_LOG_FORMAT_H_

#include <stdint.h>

#include <sstream>
#include <string>

namespace mica {
namespace transaction {

enum class LogEntryType : uint8_t {
  CREATE_TABLE = 0,
  CREATE_HASH_IDX,
  BEGIN_TXN,
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
class BeginTxnLogEntry : public LogEntry<StaticConfig> {
 public:
  uint64_t ts;
  uint64_t nwrites;

  void print() {
    LogEntry<StaticConfig>::print();

    std::stringstream stream;

    stream << "Txn timestamp: " << std::to_string(ts) << std::endl;
    stream << "N writes: " << std::to_string(nwrites) << std::endl;

    std::cout << stream.str();
  }
};

template <class StaticConfig>
class WriteRowLogEntry : public LogEntry<StaticConfig> {
 public:
  uint64_t table_index;
  uint64_t row_id;
  uint16_t cf_id;

  RowVersion<StaticConfig> rv;

  void print() {
    LogEntry<StaticConfig>::print();

    std::stringstream stream;

    stream << "Table Index: " << std::to_string(table_index) << std::endl;
    stream << "Row ID: " << std::to_string(row_id) << std::endl;
    stream << "Column Family ID: " << std::to_string(cf_id) << std::endl;

    std::cout << stream.str();
  }
};

};  // namespace transaction
};  // namespace mica

#endif
