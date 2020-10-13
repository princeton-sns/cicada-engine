#pragma once
#ifndef MICA_TRANSACTION_REPLICATION_H_
#define MICA_TRANSACTION_REPLICATION_H_

#include <pthread.h>
#include <stdio.h>

#include <chrono>
#include <memory>
#include <sstream>
#include <string>

#include "mica/transaction/db.h"
#include "mica/transaction/logging.h"
#include "mica/util/posix_io.h"
#include "tbb/concurrent_queue.h"

namespace mica {
namespace transaction {

using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

using mica::util::PosixIO;

template <class StaticConfig>
class MmappedLogFile {
 public:
  static std::shared_ptr<MmappedLogFile<StaticConfig>> open_new(
      std::string fname, std::size_t len, int prot, int flags,
      uint16_t nsegments = 1) {
    int fd = PosixIO::Open(fname.c_str(), O_RDWR | O_CREAT,
                           S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    PosixIO::Ftruncate(fd, static_cast<off_t>(len));
    char* start =
        static_cast<char*>(PosixIO::Mmap(nullptr, len, prot, flags, fd, 0));

    std::vector<LogFile<StaticConfig>*> lfs{};
    std::size_t segment_len = len / nsegments;
    for (int s = 0; s < nsegments; s++) {
      if (s == nsegments - 1) {
        segment_len = len - (nsegments - 1) * segment_len;
      }

      LogFile<StaticConfig>* lf =
          reinterpret_cast<LogFile<StaticConfig>*>(start);

      lf->nentries = 0;
      lf->size = sizeof(LogFile<StaticConfig>);

      lfs.push_back(lf);
      start += segment_len;
    }

    auto mlf = new MmappedLogFile<StaticConfig>{fname, len, fd, lfs};

    return std::shared_ptr<MmappedLogFile<StaticConfig>>{mlf};
  }

  static std::shared_ptr<MmappedLogFile<StaticConfig>> open_existing(
      std::string fname, int prot, int flags, uint16_t nsegments = 1) {
    if (PosixIO::Exists(fname.c_str())) {
      int fd = PosixIO::Open(fname.c_str(), O_RDONLY);
      std::size_t len = PosixIO::Size(fname.c_str());
      char* start =
          static_cast<char*>(PosixIO::Mmap(nullptr, len, prot, flags, fd, 0));

      std::vector<LogFile<StaticConfig>*> lfs{};
      std::size_t segment_len = len / nsegments;
      for (int s = 0; s < nsegments; s++) {
        if (s == nsegments - 1) {
          segment_len = len - (nsegments - 1) * segment_len;
        }

        LogFile<StaticConfig>* lf =
            reinterpret_cast<LogFile<StaticConfig>*>(start);

        lfs.push_back(lf);
        start += segment_len;
      }

      auto mlf = new MmappedLogFile<StaticConfig>{fname, len, fd, lfs};

      return std::shared_ptr<MmappedLogFile<StaticConfig>>{mlf};
    } else {
      return std::shared_ptr<MmappedLogFile<StaticConfig>>{nullptr};
    }
  }

  ~MmappedLogFile() {
    PosixIO::Munmap(get_lf(0), len_);
    PosixIO::Close(fd_);
  }

  std::size_t get_nsegments() { return lfs_.size(); }

  LogFile<StaticConfig>* get_lf(std::size_t segment = 0) {
    return reinterpret_cast<LogFile<StaticConfig>*>(lfs_[segment]);
  }

  LogEntry<StaticConfig>* get_cur_le() {
    return reinterpret_cast<LogEntry<StaticConfig>*>(cur_read_ptr_);
  }

  bool has_next_le(std::size_t segment) {
    return cur_read_ptr_ <
           (reinterpret_cast<char*>(get_lf(segment)) + get_size(segment));
  }

  bool has_next_le() {
    return has_next_le(cur_segment_) || (cur_segment_ < get_nsegments() - 1 &&
                                         get_size(cur_segment_ + 1) != 0);
  }

  void read_next_le() {
    auto le = reinterpret_cast<LogEntry<StaticConfig>*>(cur_read_ptr_);
    if (has_next_le(cur_segment_)) {
      cur_read_ptr_ += le->size;
    } else if (has_next_le()) {
      cur_segment_ += 1;
      cur_read_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
    } else {
      throw std::runtime_error("read_next_le: nothing more to read!");
    }
  }

  bool has_space_next_le(std::size_t n) {
    char* end = reinterpret_cast<char*>(get_lf(0)) + len_;
    if (cur_segment_ < get_nsegments() - 1) {
      end = reinterpret_cast<char*>(get_lf(cur_segment_ + 1));
    }

    return cur_write_ptr_ + n < end;
  }

  void write_next_le(void* src, std::size_t n) {
    if (!has_space_next_le(n)) {
      cur_segment_ += 1;
      cur_write_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
    }

    std::memcpy(cur_write_ptr_, src, n);
    cur_write_ptr_ += n;
    auto lf = get_lf(cur_segment_);
    lf->nentries += 1;
    lf->size += n;
  }

  std::size_t get_size(std::size_t segment = 0) {
    return get_lf(segment)->size;
  }

  uint64_t get_nentries(std::size_t segment = 0) {
    return get_lf(segment)->nentries;
  }

 private:
  std::string fname_;
  std::size_t len_;
  int fd_;
  char* cur_read_ptr_;
  char* cur_write_ptr_;
  std::vector<LogFile<StaticConfig>*> lfs_;
  std::size_t cur_segment_;

  MmappedLogFile(std::string fname, std::size_t len, int fd,
                 std::vector<LogFile<StaticConfig>*> lfs)
      : fname_{fname}, len_{len}, fd_{fd}, lfs_{lfs}, cur_segment_{0} {
    cur_read_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
    cur_write_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
  }
};

class LogEntryNode {
 public:
  LogEntryNode* next;
  void* ptr;

  void print() {
    std::stringstream stream;

    stream << "LogEntryNode: " << this << std::endl;
    stream << "next: " << next << std::endl;
    stream << "ptr: " << ptr << std::endl;

    std::cout << stream.str();
  }
} __attribute__((aligned(64)));

template <class StaticConfig>
class LogEntryList {
 public:
  static const std::size_t kBufSize = 509; // Total bytes: (509 * 8) + 8 + 8 + 8 = 4096

  LogEntryList<StaticConfig>* next;
  LogEntryList<StaticConfig>* tail;
  uint64_t nentries;
  LogEntry<StaticConfig>* buf[kBufSize]; // TODO: make this a constant parameter

  void append(LogEntryList<StaticConfig>* list) {
    tail->next = list;
    tail = list->tail;

    // printf("appended queue at %p with %lu entries to queue at %p\n", list, list->nentries, this);
    // printf("updated tail to %p\n", tail);
  }

  bool push(LogEntry<StaticConfig>* le) {
    if (nentries >= kBufSize) {
      return false;
    }

    buf[nentries++] = le;

    // printf("Pushed le at %p to queue at %p: %lu\n", le, this, nentries);

    return true;
  }

  // void print() {
  //   std::stringstream stream;

  //   stream << "LogEntryList: " << this << std::endl;
  //   stream << "next:" << next << std::endl;
  //   stream << "status:" << status << std::endl;
  //   stream << "tail:" << tail << std::endl;
  //   stream << "list:" << std::endl;

  //   std::cout << stream.str();

  //   LogEntryNode* next = list;
  //   while (next != nullptr) {
  //     next->print();
  //     next = next->next;
  //   }
  // }

} __attribute__((aligned(64)));

template <class StaticConfig>
class SchedulerPool {
 public:
  typedef typename StaticConfig::Alloc Alloc;

  static constexpr uint64_t list_size = sizeof(LogEntryList<StaticConfig>);
  static constexpr uint64_t node_size = sizeof(LogEntryNode);

  SchedulerPool(Alloc* alloc, uint64_t size, size_t lcore);
  ~SchedulerPool();

  LogEntryList<StaticConfig>* allocate_list(uint64_t n = 1);
  void free_list(LogEntryList<StaticConfig>* p);

  LogEntryNode* allocate_node(uint64_t n = 1);
  void free_node(LogEntryNode* p);

  void print_status() const {
    printf("SchedulerPool on numa node %" PRIu8 "\n", numa_id_);
    printf(" lists:");
    printf("  in use: %7.3lf GB\n",
           static_cast<double>((total_lists_ - free_lists_) * list_size) /
               1000000000.);
    printf("  free:   %7.3lf GB\n",
           static_cast<double>(free_lists_ * list_size) / 1000000000.);
    printf("  total:  %7.3lf GB\n",
           static_cast<double>(total_lists_ * list_size) / 1000000000.);
    printf(" nodes:");
    printf("  in use: %7.3lf GB\n",
           static_cast<double>((total_nodes_ - free_nodes_) * node_size) /
               1000000000.);
    printf("  free:   %7.3lf GB\n",
           static_cast<double>(free_nodes_ * node_size) / 1000000000.);
    printf("  total:  %7.3lf GB\n",
           static_cast<double>(total_nodes_ * node_size) / 1000000000.);
  }

 private:
  Alloc* alloc_;
  uint64_t size_;
  uint8_t numa_id_;

  uint64_t total_nodes_;
  uint64_t free_nodes_;
  char* node_pages_;
  LogEntryNode* next_node_;

  uint64_t total_lists_;
  uint64_t free_lists_;
  char* list_pages_;
  LogEntryList<StaticConfig>* next_list_;

  volatile uint32_t lock_;
} __attribute__((aligned(64)));

struct SchedulerLock {
  volatile SchedulerLock* next;
  volatile bool locked;
  volatile bool done;
} __attribute__((__aligned__(64)));

template <class StaticConfig>
class SchedulerThread {
 public:
  SchedulerThread(std::shared_ptr<MmappedLogFile<StaticConfig>> log,
                  SchedulerPool<StaticConfig>* pool,
                  tbb::concurrent_queue<LogEntryList<StaticConfig>*>* scheduler_queue,
                  std::vector<tbb::concurrent_queue<uint64_t>*> done_queues,
                  pthread_barrier_t* start_barrier, uint16_t id,
                  uint16_t nschedulers, SchedulerLock* my_lock);

  ~SchedulerThread();

  void start();
  void stop();

 private:
  static std::unordered_map<uint64_t, LogEntryList<StaticConfig>*> waiting_queues_;

  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;
  SchedulerPool<StaticConfig>* pool_;
  LogEntryNode* allocated_nodes_;
  LogEntryList<StaticConfig>* allocated_lists_;
  tbb::concurrent_queue<LogEntryList<StaticConfig>*>* scheduler_queue_;
  std::vector<tbb::concurrent_queue<uint64_t>*> done_queues_;
  pthread_barrier_t* start_barrier_;
  uint16_t id_;
  uint16_t nschedulers_;
  SchedulerLock* my_lock_;
  volatile bool stop_;
  std::thread thread_;

  void run();

  void acquire_scheduler_lock();
  void release_scheduler_lock(bool done = false);

  void ack_executed_rows();

  uint64_t build_local_lists(
      std::size_t segment, std::unordered_map<uint64_t, LogEntryList<StaticConfig>*>& lists);

  void free_nodes_and_list(LogEntryList<StaticConfig>* list);

  LogEntryList<StaticConfig>* allocate_list();
  void free_list(LogEntryList<StaticConfig>* list);

  LogEntryNode* allocate_node();
  void free_node(LogEntryNode* node);
};

template <class StaticConfig>
class WorkerThread {
 public:
  WorkerThread(DB<StaticConfig>* db,
               tbb::concurrent_queue<LogEntryList<StaticConfig>*>* scheduler_queue,
               tbb::concurrent_queue<uint64_t>* done_queue,
               pthread_barrier_t* start_barrier, uint16_t id,
               uint16_t nschedulers);

  ~WorkerThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  tbb::concurrent_queue<LogEntryList<StaticConfig>*>* scheduler_queue_;
  tbb::concurrent_queue<uint64_t>* done_queue_;
  pthread_barrier_t* start_barrier_;
  uint16_t id_;
  uint16_t nschedulers_;
  volatile bool stop_;
  std::thread thread_;

  nanoseconds time_working_;
  high_resolution_clock::time_point working_start_;
  high_resolution_clock::time_point working_end_;

  void run();

  void insert_row(Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
                  RowAccessHandle<StaticConfig>* rah,
                  InsertRowLogEntry<StaticConfig>* le);
  void insert_data_row(Context<StaticConfig>* ctx,
                       Transaction<StaticConfig>* tx,
                       RowAccessHandle<StaticConfig>* rah,
                       InsertRowLogEntry<StaticConfig>* le);
  void insert_hash_idx_row(Context<StaticConfig>* ctx,
                           Transaction<StaticConfig>* tx,
                           RowAccessHandle<StaticConfig>* rah,
                           InsertRowLogEntry<StaticConfig>* le);

  void write_row(Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
                 RowAccessHandle<StaticConfig>* rah,
                 WriteRowLogEntry<StaticConfig>* le);
  void write_data_row(Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
                      RowAccessHandle<StaticConfig>* rah,
                      WriteRowLogEntry<StaticConfig>* le);
  void write_hash_idx_row(Table<StaticConfig>* tbl,
                          Transaction<StaticConfig>* tx,
                          RowAccessHandle<StaticConfig>* rah,
                          WriteRowLogEntry<StaticConfig>* le);
};

template <class StaticConfig>
class CCCInterface {
 public:
  void read_logs();

  void set_logdir(std::string logdir);
  void preprocess_logs();

  void start_schedulers();
  void stop_schedulers();

  void start_workers();
  void stop_workers();
};

template <class StaticConfig>
class CopyCat : public CCCInterface<StaticConfig> {
 public:
  CopyCat(DB<StaticConfig>* db, SchedulerPool<StaticConfig>* pool,
          uint16_t nloggers, uint16_t nschedulers, uint16_t nworkers,
          std::string logdir);

  ~CopyCat();

  void read_logs();

  void set_logdir(std::string logdir) { logdir_ = logdir; }

  void preprocess_logs();

  void start_schedulers();
  void stop_schedulers();

  void start_workers();
  void stop_workers();

 private:
  tbb::concurrent_queue<LogEntryList<StaticConfig>*> scheduler_queue_;
  DB<StaticConfig>* db_;
  SchedulerPool<StaticConfig>* pool_;

  std::size_t len_;

  uint16_t nloggers_;
  uint16_t nschedulers_;
  uint16_t nworkers_;

  std::string logdir_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;

  std::vector<SchedulerLock> scheduler_locks_;
  pthread_barrier_t scheduler_barrier_;
  std::vector<SchedulerThread<StaticConfig>*> schedulers_;

  pthread_barrier_t worker_barrier_;
  std::vector<WorkerThread<StaticConfig>*> workers_;
  std::vector<tbb::concurrent_queue<uint64_t>*> done_queues_;

  uint16_t nsegments(std::size_t len) {
    return static_cast<uint16_t>(len / StaticConfig::kPageSize);
  }

  void create_table(DB<StaticConfig>* db,
                    CreateTableLogEntry<StaticConfig>* le);

  void create_hash_index(DB<StaticConfig>* db,
                         CreateHashIndexLogEntry<StaticConfig>* le);
};
};  // namespace transaction
};  // namespace mica

#include "replication_impl/copycat.h"
#include "replication_impl/scheduler_pool.h"
#include "replication_impl/scheduler_thread.h"
#include "replication_impl/worker_thread.h"

#endif
