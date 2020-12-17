#pragma once
#ifndef MICA_TRANSACTION_COPYCAT_H_
#define MICA_TRANSACTION_COPYCAT_H_

#include <chrono>
#include <queue>
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
                               uint16_t nloggers, uint16_t nios,
                               uint16_t nschedulers, uint16_t nworkers,
                               std::string logdir)
    : io_queue_{4096, 1, 0},
      scheduler_queue_{4096, 1, 0},
      io_queue_ptok_{io_queue_},
      scheduler_queue_ptok_{scheduler_queue_},
      db_{db},
      pool_{pool},
      len_{StaticConfig::kPageSize},
      nloggers_{nloggers},
      nios_{nios},
      nschedulers_{nschedulers},
      nworkers_{nworkers},
      db_id_{0},
      lcore_{0},
      logdir_{logdir},
      log_{nullptr},
      ios_{},
      io_locks_{},
      schedulers_{},
      min_wtss_{},
      workers_{},
      snapshot_manager_{nullptr} {
  int ret = pthread_barrier_init(&io_barrier_, nullptr, nios_ + 1);
  if (ret != 0) {
    throw std::runtime_error("Failed to init IO barrier: " + ret);
  }

  ret = pthread_barrier_init(&scheduler_barrier_, nullptr, nschedulers_ + 1);
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
}

template <class StaticConfig>
CopyCat<StaticConfig>::~CopyCat() {
  int ret = pthread_barrier_destroy(&io_barrier_);
  if (ret != 0) {
    std::cerr << "Failed to destroy IO barrier: " + ret;
  }

  ret = pthread_barrier_destroy(&scheduler_barrier_);
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
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_ios() {
  const std::string fname = logdir_ + "/out.log";
  if (!PosixIO::Exists(fname.c_str())) {
    return;
  }

  std::size_t len = PosixIO::Size(fname.c_str());

  log_ = MmappedLogFile<StaticConfig>::open_existing(
      fname, PROT_READ, MAP_SHARED, nsegments(len));

  for (uint16_t iid = 0; iid < nios_; iid++) {
    bool locked = true;
    if (iid == 0) {
      locked = false;
    }

    io_locks_.push_back({nullptr, locked, false});
  }

  for (uint16_t iid = 0; iid < nios_; iid++) {
    std::size_t next_iid = static_cast<std::size_t>((iid + 1) % nios_);
    io_locks_[iid].next = &io_locks_[next_iid];
  }

  // Put IO threads on NUMA 1
  uint16_t lcore1 = (uint16_t)::mica::util::lcore.first_lcore_id_with_numa_id(1);
  db_id_ = std::max(db_id_, lcore1);
  lcore_ = std::max(lcore_, lcore1);
  for (uint16_t iid = 0; iid < nios_; iid++) {
    auto i = new IOThread<StaticConfig>{db_,
                                        log_,
                                        pool_,
                                        &io_barrier_,
                                        &io_queue_,
                                        &io_queue_ptok_,
                                        &io_locks_[iid],
                                        iid,
                                        nios_,
                                        db_id_++,
                                        lcore_++};

    i->start();

    ios_.push_back(i);
  }

  pthread_barrier_wait(&io_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_ios() {
  for (auto i : ios_) {
    i->stop();
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_schedulers() {
  for (uint16_t sid = 0; sid < nschedulers_; sid++) {
    //lcore_++;
    //auto s = new SchedulerThread<StaticConfig>{&io_queue_,
    //                                           &io_queue_ptok_,
    //                                           &scheduler_queue_,
    //                                           &scheduler_queue_ptok_,
    //                                           &scheduler_barrier_,
    //                                           sid,
    //                                           lcore_++};

    //s->start();

    //schedulers_.push_back(s);
  }

  //pthread_barrier_wait(&scheduler_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_schedulers() {
  //for (auto s : schedulers_) {
  //  s->stop();
  //}
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_snapshot_manager() {
  for (uint16_t wid = 0; wid < nworkers_; wid++) {
    min_wtss_.push_back({0});
  }

  snapshot_manager_ = new SnapshotThread<StaticConfig>{db_, &snapshot_barrier_,
                                                       min_wtss_, 0, lcore_++};

  snapshot_manager_->start();

  pthread_barrier_wait(&snapshot_barrier_);
}

template <class StaticConfig>
void CopyCat<StaticConfig>::stop_snapshot_manager() {
  snapshot_manager_->stop();

  min_wtss_.clear();
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_workers() {
  for (uint16_t wid = 0; wid < nworkers_; wid++) {
    auto w = new WorkerThread<StaticConfig>{db_,
                                            pool_,
                                            &io_queue_,
                                            &io_queue_ptok_,
                                            //&scheduler_queue_,
                                            //&scheduler_queue_ptok_,
                                            &min_wtss_[wid],
                                            &worker_barrier_,
                                            wid,
                                            db_id_++,
                                            lcore_++};

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
  db_id_ = 0;
  lcore_ = 0;

  for (auto i : ios_) {
    delete i;
  }

  io_locks_.clear();
  log_.reset();
  ios_.clear();

  LogEntryList<StaticConfig>* list;
  while (io_queue_.try_dequeue_from_producer(io_queue_ptok_, list)) {
  }  // Empty queue

  for (auto s : schedulers_) {
    delete s;
  }

  schedulers_.clear();

  for (auto w : workers_) {
    delete w;
  }

  workers_.clear();

  while (scheduler_queue_.try_dequeue(list)) {
  }  // Empty queue

  delete snapshot_manager_;

  snapshot_manager_ = nullptr;
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
std::size_t get_total_log_size(std::string logdir, uint64_t nloggers) {
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

template <class StaticConfig>
uint64_t get_ts(LogEntry<StaticConfig>* le) {
  switch (le->type) {
    case LogEntryType::CREATE_TABLE:
    case LogEntryType::CREATE_HASH_IDX:
      return 0;

    case LogEntryType::INSERT_ROW:
      return static_cast<InsertRowLogEntry<StaticConfig>*>(le)->txn_ts;

    case LogEntryType::WRITE_ROW:
      return static_cast<WriteRowLogEntry<StaticConfig>*>(le)->txn_ts;

    default:
      throw std::runtime_error("get_ts: Unexpected log entry type.");
  }
}

template <class StaticConfig>
void CopyCat<StaticConfig>::preprocess_logs() {
  const std::string outfname = logdir_ + "/out.log";

  std::size_t out_size = get_total_log_size<StaticConfig>(logdir_, nloggers_);

  // Add space overhead of segments
  out_size += nsegments(out_size) * sizeof(LogFile<StaticConfig>);

  // Round up to next multiple of len_
  out_size = len_ * ((out_size + (len_ - 1)) / len_);
  if (out_size == 0) {
    return;
  }

  // Add an extra len_ to account for internal fragmentation at segment boundaries
  out_size += len_;

  // Allocate out file
  std::shared_ptr<MmappedLogFile<StaticConfig>> out_mlf =
      MmappedLogFile<StaticConfig>::open_new(outfname, out_size,
                                             PROT_READ | PROT_WRITE, MAP_SHARED,
                                             nsegments(out_size));

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

  for (uint16_t thread_id = 0; thread_id < nloggers_; thread_id++) {
    uint64_t file_index = 0;
    std::string fname = logdir_ + "/out." + std::to_string(thread_id) + "." +
                        std::to_string(file_index) + ".log";

    auto mlf = MmappedLogFile<StaticConfig>::open_existing(fname, PROT_READ,
                                                           MAP_SHARED);
    if (mlf != nullptr && mlf->get_nentries() > 0) {
      pq.emplace(mlf, get_ts(mlf->get_cur_le()), thread_id);
    }

    cur_file_indices.push_back(file_index);
  }

  while (!pq.empty()) {
    auto wrapper = pq.top();
    pq.pop();
    auto ts = wrapper.ts;
    auto mlf = wrapper.mlf;
    auto thread_id = wrapper.thread_id;

    LogEntry<StaticConfig>* le = mlf->get_cur_le();

    CreateTableLogEntry<StaticConfig>* ctle = nullptr;
    CreateHashIndexLogEntry<StaticConfig>* chile = nullptr;

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
        // static_cast<InsertRowLogEntry<StaticConfig>*>(le)->print();
        out_mlf->write_next_le(le, le->size);
        break;

      case LogEntryType::WRITE_ROW:
        // static_cast<WriteRowLogEntry<StaticConfig>*>(le)->print();
        out_mlf->write_next_le(le, le->size);
        break;

      default:
        throw std::runtime_error("preprocess_logs: Unexpected log entry type.");
    }

    mlf->read_next_le();
    if (mlf->has_next_le()) {
      pq.emplace(mlf, get_ts(mlf->get_cur_le()), thread_id);
    } else {
      uint64_t file_index = cur_file_indices[thread_id] + 1;
      std::string fname = logdir_ + "/out." + std::to_string(thread_id) + "." +
                          std::to_string(file_index) + ".log";

      mlf = MmappedLogFile<StaticConfig>::open_existing(fname, PROT_READ,
                                                        MAP_SHARED);
      if (mlf != nullptr && mlf->get_nentries() > 0) {
        pq.emplace(mlf, get_ts(mlf->get_cur_le()), thread_id);
        cur_file_indices[thread_id] = file_index;
      }
    }
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
            ctle->print();
            break;

          case LogEntryType::CREATE_HASH_IDX:
            chile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);
            chile->print();
            break;

          case LogEntryType::INSERT_ROW:
            irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
            irle->print();
            break;

          case LogEntryType::WRITE_ROW:
            wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
            wrle->print();
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
