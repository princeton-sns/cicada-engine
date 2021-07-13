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
CopyCat<StaticConfig>::CopyCat(DB<StaticConfig>* db, Alloc* alloc,
                               uint64_t max_sched_pool_size,
                               uint64_t sched_pool_lcore, uint16_t nloggers,
                               uint16_t nios, uint16_t nschedulers,
                               uint16_t nworkers, std::string logdir)
    : io_queue_{4096, 1, 0},
      scheduler_queue_{4096, 1, 0},
      io_queue_ptok_{io_queue_},
      scheduler_queue_ptok_{scheduler_queue_},
      db_{db},
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
  // pool_ = new SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>(
  //     alloc, max_sched_pool_size, sched_pool_lcore);

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
  // delete pool_;

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
      fname, PROT_READ, MAP_SHARED,
      ReplicationUtils<StaticConfig>::nsegments(len));

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
  uint16_t lcore1 =
      (uint16_t)::mica::util::lcore.first_lcore_id_with_numa_id(1);
  db_id_ = std::max(db_id_, lcore1);
  lcore_ = std::max(lcore_, lcore1);
  for (uint16_t iid = 0; iid < nios_; iid++) {
    auto i = new IOThread<StaticConfig>{db_, log_,
                                        // pool_,
                                        &io_barrier_, &io_queue_,
                                        &io_queue_ptok_, &io_locks_[iid], iid,
                                        nios_, db_id_++, lcore_++};

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
}

template <class StaticConfig>
void CopyCat<StaticConfig>::start_workers() {
  for (uint16_t wid = 0; wid < nworkers_; wid++) {
    auto w = new WorkerThread<StaticConfig>{db_,
                                            // pool_,
                                            &io_queue_, &io_queue_ptok_,
                                            //&scheduler_queue_,
                                            //&scheduler_queue_ptok_,
                                            &min_wtss_[wid], &worker_barrier_,
                                            wid, db_id_++, lcore_++};

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

  min_wtss_.clear();
}

template <class StaticConfig>
void CopyCat<StaticConfig>::preprocess_logs(std::string srcdir,
                                            std::string dstdir) {
  ReplicationUtils<StaticConfig>::preprocess_logs(db_, nloggers_, srcdir,
                                                  dstdir);
}

}  // namespace transaction
};  // namespace mica

#endif
