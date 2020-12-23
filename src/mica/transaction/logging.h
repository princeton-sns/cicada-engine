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
#include "mica/transaction/log_format.h"
#include "mica/transaction/mmap_lf.h"
#include "mica/transaction/transaction.h"
#include "mica/util/readerwriterqueue.h"

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
};

template <class StaticConfig>
class PerThreadLog {
 public:
  PerThreadLog() : request_queue_{}, mlfs_{}, cur_mlf_{nullptr}, file_index_{0}, lock_{0} {}
  ~PerThreadLog() {}

  bool request_mlf() { return request_queue_.enqueue(1); }

  std::size_t num_mlfs() {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();
    std::size_t n = mlfs_.size();
    __sync_lock_release(&lock_);
    return n;
  }

  void add_mlf(std::shared_ptr<MmappedLogFile<StaticConfig>> mlf) {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();
    if (cur_mlf_ == nullptr) {
      cur_mlf_ = mlf;
    }

    mlfs_.push_back(mlf);
    __sync_lock_release(&lock_);
  }

  void advance_file_index() {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();
    if (file_index_ + 1 >= mlfs_.size()) {
      throw std::runtime_error("file_index_ >= mlfs_.size()");
    }
    file_index_ += 1;
    cur_mlf_ = mlfs_[file_index_];
    __sync_lock_release(&lock_);
  }

  bool has_space(std::size_t nbytes) {
    return cur_mlf_->has_space_next_le(nbytes);
  }

  char* get_cur_write_ptr() {
    return cur_mlf_->get_cur_write_ptr();
  }

  void advance_cur_write_ptr(std::size_t nbytes) {
    cur_mlf_->advance_cur_write_ptr(nbytes);
  }

  void flush() {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();
    for (auto mlf : mlfs_) {
      mlf->flush();
    }
    __sync_lock_release(&lock_);
  }

  LogFile<StaticConfig>* get_lf() {
    return cur_mlf_->get_lf(0);
  }

  moodycamel::BlockingReaderWriterQueue<uint16_t>* get_request_queue() {
    return &request_queue_;
  }

 private:
  moodycamel::BlockingReaderWriterQueue<uint16_t> request_queue_;

  std::vector<std::shared_ptr<MmappedLogFile<StaticConfig>>> mlfs_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> cur_mlf_;
  std::size_t file_index_;

  volatile uint32_t lock_;
} __attribute__((aligned(64)));

template <class StaticConfig>
class MmapperThread {
 public:
  MmapperThread(PerThreadLog<StaticConfig>* thread_log, std::string logdir, uint16_t id);
  ~MmapperThread();

  void start();
  void stop();

 private:
  std::thread thread_;
  std::string logdir_;
  PerThreadLog<StaticConfig>* thread_log_;
  uint16_t id_;
  bool stop_;

  void run();
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

 private:
  std::vector<PerThreadLog<StaticConfig>*> thread_logs_;
  std::vector<MmapperThread<StaticConfig>*> mmappers_;

  std::string logdir_;
  uint16_t nthreads_;

  char* alloc_log_buf(uint16_t thread_id, std::size_t nbytes);
  void release_log_buf(uint16_t thread_id, std::size_t nbytes);
};

}  // namespace transaction
}  // namespace mica

#include "logging_impl/mmap_logger.h"
#include "logging_impl/mmapper_thread.h"

#endif
