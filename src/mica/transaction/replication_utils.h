#pragma once
#ifndef MICA_TRANSACTION_REPLICATION_UTILS_H_
#define MICA_TRANSACTION_REPLICATION_UTILS_H_

#include <stdint.h>

#include <cstddef>
#include <queue>
#include <string>

#include "mica/transaction/db.h"
#include "mica/transaction/logging.h"
#include "mica/transaction/mmap_lf.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
class ReplicationUtils {
 public:
  ReplicationUtils() = delete;

  static uint16_t nsegments(std::size_t len) {
    return static_cast<uint16_t>(len / StaticConfig::kPageSize);
  }

  static uint64_t get_ts(LogEntry<StaticConfig>* le) {
    switch (le->type) {
      case LogEntryType::CREATE_TABLE:
      case LogEntryType::CREATE_HASH_IDX:
        return 0;

      case LogEntryType::BEGIN_TXN:
        return static_cast<BeginTxnLogEntry<StaticConfig>*>(le)->ts;

      case LogEntryType::WRITE_ROW:
        return static_cast<WriteRowLogEntry<StaticConfig>*>(le)->rv.wts.t2;

      default:
        throw std::runtime_error("get_ts: Unexpected log entry type.");
    }
  }

  static std::size_t get_total_log_size(std::string logdir, uint64_t nloggers) {
    // Find output log file size
    std::size_t out_size = 0;
    for (uint16_t thread_id = 0; thread_id < nloggers; thread_id++) {
      for (uint64_t file_index = 0;; file_index++) {
        std::string fname = logdir + "/out." + std::to_string(thread_id) + "." +
                            std::to_string(file_index) + ".log";

        auto mlf = MmappedLogFile<StaticConfig>::open_existing(fname, PROT_READ,
                                                               MAP_SHARED);
        if (mlf == nullptr || mlf->get_nentries() == 0) {
          break;
        }

        out_size += mlf->get_size();
      }
    }

    return out_size;
  }

  static void create_table(DB<StaticConfig>* db,
                           CreateTableLogEntry<StaticConfig>* ctle) {
    if (!db->create_table(std::string{ctle->name}, ctle->cf_count,
                          ctle->data_size_hints)) {
      throw std::runtime_error("Failed to create table: " +
                               std::string{ctle->name});
    }
  }

  static void create_hash_index(DB<StaticConfig>* db,
                                CreateHashIndexLogEntry<StaticConfig>* chile) {
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

  static void preprocess_logs(DB<StaticConfig>* db, uint16_t nloggers,
                              std::string srcdir, std::string dstdir) {
    const std::string outfname = dstdir + "/out.log";

    std::size_t out_size =
        ReplicationUtils<StaticConfig>::get_total_log_size(srcdir, nloggers);

    // Add space overhead of segments
    out_size += ReplicationUtils<StaticConfig>::nsegments(out_size) *
                sizeof(LogFile<StaticConfig>);

    // Round up to next multiple of StaticConfig::kPageSize
    out_size =
        StaticConfig::kPageSize *
        ((out_size + (StaticConfig::kPageSize - 1)) / StaticConfig::kPageSize);
    if (out_size == 0) {
      return;
    }

    // Add an extra StaticConfig::kPageSize to account for internal fragmentation at segment boundaries
    out_size += StaticConfig::kPageSize;

    // Allocate out file
    std::shared_ptr<MmappedLogFile<StaticConfig>> out_mlf =
        MmappedLogFile<StaticConfig>::open_new(
            outfname, out_size, PROT_READ | PROT_WRITE, MAP_SHARED,
            ReplicationUtils<StaticConfig>::nsegments(out_size));

    class MLF {
     public:
      std::shared_ptr<MmappedLogFile<StaticConfig>> mlf;
      uint64_t ts;
      uint16_t thread_id;

      MLF(std::shared_ptr<MmappedLogFile<StaticConfig>> mlf, uint64_t ts,
          uint16_t thread_id) {
        this->mlf = mlf;
        this->ts = ts;
        this->thread_id = thread_id;
      }
    };

    auto cmp = [](MLF left, MLF right) { return left.ts > right.ts; };

    std::priority_queue<MLF, std::vector<MLF>, decltype(cmp)> pq{cmp};
    std::vector<uint64_t> cur_file_indices{};

    for (uint16_t thread_id = 0; thread_id < nloggers; thread_id++) {
      uint64_t file_index = 0;
      std::string fname = srcdir + "/out." + std::to_string(thread_id) + "." +
                          std::to_string(file_index) + ".log";

      auto mlf = MmappedLogFile<StaticConfig>::open_existing(fname, PROT_READ,
                                                             MAP_SHARED);
      if (mlf != nullptr && mlf->get_nentries() > 0) {
        pq.emplace(mlf,
                   ReplicationUtils<StaticConfig>::get_ts(mlf->get_cur_le()),
                   thread_id);
      }

      cur_file_indices.push_back(file_index);
    }

    while (!pq.empty()) {
      auto wrapper = pq.top();
      pq.pop();
      auto mlf = wrapper.mlf;
      auto thread_id = wrapper.thread_id;

      LogEntry<StaticConfig>* le = mlf->get_cur_le();

      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* chile = nullptr;

      switch (le->type) {
        case LogEntryType::CREATE_TABLE:
          ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);
          // ctle->print();
          create_table(db, ctle);
          break;

        case LogEntryType::CREATE_HASH_IDX:
          chile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);
          // chile->print();
          create_hash_index(db, chile);
          break;

        case LogEntryType::BEGIN_TXN:
        case LogEntryType::WRITE_ROW:
          // static_cast<WriteRowLogEntry<StaticConfig>*>(le)->print();
          out_mlf->write_next_le(le, le->size);
          break;

        default:
          throw std::runtime_error(
              "preprocess_logs: Unexpected log entry type.");
      }

      mlf->read_next_le();
      if (mlf->has_next_le()) {
        pq.emplace(mlf,
                   ReplicationUtils<StaticConfig>::get_ts(mlf->get_cur_le()),
                   thread_id);
      } else {
        uint64_t file_index = cur_file_indices[thread_id] + 1;
        std::string fname = srcdir + "/out." + std::to_string(thread_id) + "." +
                            std::to_string(file_index) + ".log";

        mlf = MmappedLogFile<StaticConfig>::open_existing(fname, PROT_READ,
                                                          MAP_SHARED);
        if (mlf != nullptr && mlf->get_nentries() > 0) {
          pq.emplace(mlf,
                     ReplicationUtils<StaticConfig>::get_ts(mlf->get_cur_le()),
                     thread_id);
          cur_file_indices[thread_id] = file_index;
        }
      }
    }
  }
};
};  // namespace transaction
};  // namespace mica

#endif
