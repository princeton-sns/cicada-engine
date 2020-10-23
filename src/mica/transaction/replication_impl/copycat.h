#pragma once
#ifndef MICA_TRANSACTION_COPYCAT_H_
#define MICA_TRANSACTION_COPYCAT_H_

#include <chrono>
#include <thread>

#include "mica/transaction/logging.h"
#include "mica/transaction/replication.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {

using mica::util::PosixIO;

template <class StaticConfig>
CopyCat<StaticConfig>::CopyCat(DB<StaticConfig>* db,
                               SchedulerPool<StaticConfig>* pool,
                               uint16_t nloggers, uint16_t nschedulers,
                               uint16_t nworkers, std::string logdir)
    : scheduler_queue_{},
      op_count_queue_{4096},
      db_{db},
      pool_{pool},
      len_{StaticConfig::kPageSize},
      nloggers_{nloggers},
      nschedulers_{nschedulers},
      nworkers_{nworkers},
      logdir_{logdir},
      log_{nullptr},
      scheduler_locks_{},
      schedulers_{},
      workers_{},
      ack_queues_{},
      op_done_queues_{},
      snapshot_manager_{nullptr} {
  int ret =
      pthread_barrier_init(&scheduler_barrier_, nullptr, nschedulers_ + 1);
  if (ret != 0) {
    throw std::runtime_error("Failed to init scheduler barrier: " + ret);
  }

  ret = pthread_barrier_init(&worker_barrier_, nullptr, nworkers_ + 1);
  if (ret != 0) {
    throw std::runtime_error("Failed to init worker barrier: " + ret);
  }

  ret = pthread_barrier_init(&snapshot_barrier_, nullptr, 2);
  if (ret != 0) {
    throw std::runtime_error("Failed to init snapshot barrier: " + ret);
  }

  for (uint16_t wid = 0; wid < nworkers_; wid++) {
    ack_queues_.push_back(new moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>{4096});
    op_done_queues_.push_back(new moodycamel::ReaderWriterQueue<uint64_t>{4096});
  }
}

template <class StaticConfig>
CopyCat<StaticConfig>::~CopyCat() {
  int ret = pthread_barrier_destroy(&scheduler_barrier_);
  if (ret != 0) {
    std::cerr << "Failed to destroy scheduler barrier: " + ret;
  }

  ret = pthread_barrier_destroy(&worker_barrier_);
  if (ret != 0) {
    std::cerr << "Failed to destroy worker barrier: " + ret;
  }

  ret = pthread_barrier_destroy(&snapshot_barrier_);
  if (ret != 0) {
    std::cerr << "Failed to destroy snapshot barrier: " + ret;
  }

  for (auto ack_queue : ack_queues_) {
    LogEntryList<StaticConfig>* list;
    while (ack_queue->try_dequeue(list)) {
      while (list != nullptr) {
        auto next = list->next;
        pool_->free_list(list);
        list = next;
      }
    }
    delete ack_queue;
  }
  ack_queues_.clear();

  for (auto op_done_queue : op_done_queues_) {
    delete op_done_queue;
  }
  op_done_queues_.clear();
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_workers() {
  for (uint16_t wid = 0; wid < nworkers_; wid++) {
    auto w = new WorkerThread<StaticConfig>{db_,
                                            &scheduler_queue_,
                                            ack_queues_[wid],
                                            op_done_queues_[wid],
                                            &worker_barrier_,
                                            wid,
                                            nschedulers_};

    w->start();

    workers_.push_back(w);
  }

  pthread_barrier_wait(&worker_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_workers() {
  for (auto w : workers_) {
    w->stop();
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::reset() {
  for (auto s : schedulers_) {
    delete s;
  }

  scheduler_locks_.clear();
  schedulers_.clear();

  for (auto w : workers_) {
    delete w;
  }

  LogEntryList<StaticConfig>* list;
  for (auto queue : ack_queues_) {
    while (queue->try_dequeue(list)) {
      while (list != nullptr) {
        auto next = list->next;
        pool_->free_list(list);
        list = next;
      }
    }
  }

  log_.reset();
  workers_.clear();

  delete snapshot_manager_;

  std::pair<uint64_t,uint64_t> op_count{};
  while (op_count_queue_.try_dequeue(op_count)) {} // Empty queue
  // op_count_queue_.clear();

  uint64_t txn_ts;
  for (auto queue : op_done_queues_) {
    while (queue->try_dequeue(txn_ts)) {} // Empty queue
  }

  snapshot_manager_ = nullptr;
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_schedulers() {
  const std::string fname = logdir_ + "/out.log";
  if (!PosixIO::Exists(fname.c_str())) {
    return;
  }

  std::size_t len = PosixIO::Size(fname.c_str());

  log_ = MmappedLogFile<StaticConfig>::open_existing(
      fname, PROT_READ, MAP_SHARED, nsegments(len));

  for (uint16_t sid = 0; sid < nschedulers_; sid++) {
    bool locked = true;
    if (sid == 0) {
      locked = false;
    }

    scheduler_locks_.push_back({nullptr, locked, false});
  }

  for (uint16_t sid = 0; sid < nschedulers_; sid++) {
    std::size_t next_sid = static_cast<std::size_t>((sid + 1) % nschedulers_);
    scheduler_locks_[sid].next = &scheduler_locks_[next_sid];
  }

  for (uint16_t sid = 0; sid < nschedulers_; sid++) {
    auto s = new SchedulerThread<StaticConfig>{log_,
                                               pool_,
                                               &scheduler_queue_,
                                               &op_count_queue_,
                                               ack_queues_,
                                               &scheduler_barrier_,
                                               sid,
                                               nschedulers_,
                                               &scheduler_locks_[sid]};

    s->start();

    schedulers_.push_back(s);
  }

  pthread_barrier_wait(&scheduler_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_schedulers() {
  for (auto s : schedulers_) {
    s->stop();
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_snapshot_manager() {
  snapshot_manager_ = new SnapshotThread<StaticConfig>{
      db_, &snapshot_barrier_, &op_count_queue_, op_done_queues_};

  snapshot_manager_->start();

  pthread_barrier_wait(&snapshot_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_snapshot_manager() {
  snapshot_manager_->stop();
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
void CopyCat<StaticConfig>::preprocess_logs() {
  const std::string outfname = logdir_ + "/out.log";

  std::vector<std::shared_ptr<MmappedLogFile<StaticConfig>>> mlfs{};

  // Find output log file size
  std::size_t out_size = 0;
  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    for (uint64_t file_index = 0;; file_index++) {
      std::string fname = logdir_ + "/out." + std::to_string(thread_id) + "." +
                          std::to_string(file_index) + ".log";

      auto mlf = MmappedLogFile<StaticConfig>::open_existing(fname, PROT_READ,
                                                             MAP_SHARED);
      if (mlf == nullptr || mlf->get_nentries() == 0) {
        break;
      }

      // mlf->get_lf()->print();

      out_size += mlf->get_size();
      mlfs.push_back(mlf);
    }
  }

  // Round up to next multiple of len_
  out_size = len_ * ((out_size + (len_ - 1)) / len_);
  if (out_size == 0) {
    return;
  }

  // Allocate out file
  std::shared_ptr<MmappedLogFile<StaticConfig>> out_mlf =
      MmappedLogFile<StaticConfig>::open_new(outfname, out_size,
                                             PROT_READ | PROT_WRITE, MAP_SHARED,
                                             nsegments(out_size));

  // Sort log files by transaction timestamp
  while (mlfs.size() != 0) {
    uint64_t min_txn_ts = static_cast<uint64_t>(-1);
    std::size_t next_i = 0;

    // Find log file with min transaction timestamp
    for (std::size_t i = 0; i < mlfs.size(); i++) {
      std::shared_ptr<MmappedLogFile<StaticConfig>> mlf = mlfs[i];

      LogEntry<StaticConfig>* le = mlf->get_cur_le();
      // le->print();

      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* chile = nullptr;
      InsertRowLogEntry<StaticConfig>* irle = nullptr;
      WriteRowLogEntry<StaticConfig>* wrle = nullptr;

      switch (le->type) {
        case LogEntryType::CREATE_TABLE:
          ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);
          // ctle->print();
          create_table(db_, ctle);
          mlf->read_next_le();
          break;

        case LogEntryType::CREATE_HASH_IDX:
          chile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);
          // chile->print();
          create_hash_index(db_, chile);
          mlf->read_next_le();
          break;

        case LogEntryType::INSERT_ROW:
          irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
          // irle->print();
          if (irle->txn_ts < min_txn_ts) {
            min_txn_ts = irle->txn_ts;
            next_i = i;
          }
          break;

        case LogEntryType::WRITE_ROW:
          wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
          // wrle->print();
          if (wrle->txn_ts < min_txn_ts) {
            min_txn_ts = wrle->txn_ts;
            next_i = i;
          }
          break;

        default:
          throw std::runtime_error(
              "preprocess_logs: Unexpected log entry type.");
      }
    }

    if (min_txn_ts != static_cast<uint64_t>(-1)) {
      auto mlf = mlfs[next_i];
      auto le = mlf->get_cur_le();

      // le->print();

      out_mlf->write_next_le(le, le->size);

      mlf->read_next_le();
    }

    // Remove empty mlfs
    mlfs.erase(
        std::remove_if(mlfs.begin(), mlfs.end(),
                       [](std::shared_ptr<MmappedLogFile<StaticConfig>> mlf) {
                         return !mlf->has_next_le();
                       }),
        mlfs.end());
  }
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
