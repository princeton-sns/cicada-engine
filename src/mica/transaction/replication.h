#pragma once
#ifndef MICA_TRANSACTION_REPLICATION_H_
#define MICA_TRANSACTION_REPLICATION_H_

#include <pthread.h>
#include <stdio.h>

#include <chrono>
#include <functional>
#include <list>
#include <memory>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mica/transaction/db.h"
#include "mica/transaction/logging.h"
#include "mica/transaction/row_version_pool.h"
#include "mica/util/posix_io.h"
#include "mica/util/robin_hood.h"
#include "readerwriterqueue.h"
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
  LogEntryList<StaticConfig>* next;

  uint64_t table_index;
  uint64_t row_id;
  uint64_t tail_ts;

  uint64_t nentries;

  RowVersion<StaticConfig>* head_rv;
  RowVersion<StaticConfig>* tail_rv;

  void push(RowVersion<StaticConfig>* rv) {
    if (tail_rv == nullptr) {
      tail_rv = rv;
    }

    rv->older_rv = head_rv;
    head_rv = rv;

    nentries += 1;
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

struct TableRowID {
  uint64_t table_index;
  uint64_t row_id;

  bool const operator==(const TableRowID& t) const {
    return table_index == t.table_index && row_id == t.row_id;
  }

  bool const operator<(const TableRowID& t) const {
    return table_index < t.table_index ||
           (table_index == t.table_index && row_id < t.row_id);
  }
};

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

struct IOLock {
  volatile IOLock* next;
  volatile bool locked;
  volatile bool done;
} __attribute__((__aligned__(64)));

template <class StaticConfig>
class IOThread {
 public:
  IOThread(DB<StaticConfig>* db,
           std::shared_ptr<MmappedLogFile<StaticConfig>> log,
           SchedulerPool<StaticConfig>* pool, pthread_barrier_t* start_barrier,
           moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue,
           IOLock* my_lock, uint16_t id, uint16_t nios, uint16_t db_id,
           uint16_t lcore);
  ~IOThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;
  std::thread thread_;
  pthread_barrier_t* start_barrier_;
  SchedulerPool<StaticConfig>* pool_;
  LogEntryList<StaticConfig>* allocated_lists_;
  moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue_;
  IOLock* my_lock_;
  uint16_t id_;
  uint16_t db_id_;
  uint16_t lcore_;
  uint16_t nios_;
  volatile bool stop_;

  void run();

  void acquire_io_lock();
  void release_io_lock();

  LogEntryList<StaticConfig>* allocate_list();

  uint64_t build_local_lists(std::size_t segment,
                             std::vector<LogEntryList<StaticConfig>*>& lists);
};

template <class StaticConfig>
class SchedulerThread {
 public:
  SchedulerThread(
      SchedulerPool<StaticConfig>* pool,
      moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue,
      std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
          scheduler_queues,
      std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
          ack_queues,
      pthread_barrier_t* start_barrier, uint16_t id, uint16_t lcore);

  ~SchedulerThread();

  void start();
  void stop();

 private:
  SchedulerPool<StaticConfig>* pool_;
  moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue_;
  std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*> scheduler_queues_;
  std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
      ack_queues_;
  pthread_barrier_t* start_barrier_;
  uint16_t id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;

  void run();

  void ack_executed_rows();

  void free_list(LogEntryList<StaticConfig>* list);
};

struct WorkerMinWTS {
  volatile uint64_t min_wts;
} __attribute__((__aligned__(64)));

template <class StaticConfig>
class SnapshotThread {
 public:
  SnapshotThread(DB<StaticConfig>* db, pthread_barrier_t* start_barrier,
                 std::vector<WorkerMinWTS>& min_wtss, uint16_t id,
                 uint16_t lcore);

  ~SnapshotThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  std::vector<WorkerMinWTS>& min_wtss_;
  robin_hood::unordered_map<uint64_t, uint64_t> counts_index_;
  std::list<std::pair<uint64_t, uint64_t>> counts_;
  uint16_t id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;
  pthread_barrier_t* start_barrier_;

  void run();
};

template <class StaticConfig>
class WorkerThread {
 public:
  WorkerThread(
      DB<StaticConfig>* db,
      moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*
          scheduler_queue,
      moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* ack_queue,
      WorkerMinWTS* min_wts, pthread_barrier_t* start_barrier, uint16_t id,
      uint16_t db_id, uint16_t lcore);

  ~WorkerThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* scheduler_queue_;
  moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* ack_queue_;
  WorkerMinWTS* min_wts_;
  pthread_barrier_t* start_barrier_;
  uint16_t id_;
  uint16_t db_id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;

  nanoseconds time_working_;
  high_resolution_clock::time_point working_start_;
  high_resolution_clock::time_point working_end_;

  void run();

  void insert_row(Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
                  RowAccessHandle<StaticConfig>* rah,
                  InsertRowLogEntry<StaticConfig>* le);

  void write_row(Table<StaticConfig>* tbl, Transaction<StaticConfig>* tx,
                 RowAccessHandle<StaticConfig>* rah,
                 WriteRowLogEntry<StaticConfig>* le);
};

template <class StaticConfig>
class CCCInterface {
 public:
  void read_logs();

  void set_logdir(std::string logdir);
  void preprocess_logs();

  void start_ios();
  void stop_ios();

  void start_schedulers();
  void stop_schedulers();

  void start_snapshot_manager();
  void stop_snapshot_manager();

  void start_workers();
  void stop_workers();

  void reset();
};

template <class StaticConfig>
class CopyCat : public CCCInterface<StaticConfig> {
 public:
  CopyCat(DB<StaticConfig>* db, SchedulerPool<StaticConfig>* pool,
          uint16_t nloggers, uint16_t nios, uint16_t nschedulers,
          uint16_t nworkers, std::string logdir);

  ~CopyCat();

  void read_logs();

  void set_logdir(std::string logdir) { logdir_ = logdir; }

  void preprocess_logs();

  void start_ios();
  void stop_ios();

  void start_schedulers();
  void stop_schedulers();

  void start_snapshot_manager();
  void stop_snapshot_manager();

  void start_workers();
  void stop_workers();

  void reset();

 private:
  moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*> io_queue_;
  std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*> scheduler_queues_;
  DB<StaticConfig>* db_;
  SchedulerPool<StaticConfig>* pool_;

  std::size_t len_;

  uint16_t nloggers_;
  uint16_t nios_;
  uint16_t nschedulers_;
  uint16_t nworkers_;
  uint16_t db_id_;
  uint16_t lcore_;

  std::string logdir_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;

  pthread_barrier_t io_barrier_;
  std::vector<IOThread<StaticConfig>*> ios_;

  std::vector<IOLock> io_locks_;
  pthread_barrier_t scheduler_barrier_;
  std::vector<SchedulerThread<StaticConfig>*> schedulers_;

  std::vector<WorkerMinWTS> min_wtss_;
  pthread_barrier_t worker_barrier_;
  std::vector<WorkerThread<StaticConfig>*> workers_;
  std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*>
      ack_queues_;

  pthread_barrier_t snapshot_barrier_;
  SnapshotThread<StaticConfig>* snapshot_manager_;

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


namespace std {
template <>
struct hash<mica::transaction::TableRowID> {
  std::size_t operator()(const mica::transaction::TableRowID& t) const noexcept {
    std::size_t h1 = std::hash<uint64_t>{}(t.table_index);
    std::size_t h2 = std::hash<uint64_t>{}(t.row_id);
    return h1 ^ h2;
  }
};
};
#include "replication_impl/copycat.h"
#include "replication_impl/io_thread.h"
#include "replication_impl/scheduler_pool.h"
#include "replication_impl/scheduler_thread.h"
#include "replication_impl/snapshot_thread.h"
#include "replication_impl/worker_thread.h"

#endif
