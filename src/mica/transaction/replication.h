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
#include <type_traits>
#include <unordered_map>
#include <utility>
#include <vector>

#include "mica/transaction/db.h"
#include "mica/transaction/logging.h"
#include "mica/transaction/mmap_lf.h"
#include "mica/transaction/replication_utils.h"
#include "mica/transaction/row_version_pool.h"
#include "mica/util/concurrentqueue.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {

using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

using mica::util::PosixIO;

template <class StaticConfig>
class CCCInterface {
 public:
  void set_logdir(std::string logdir);
  void preprocess_logs(std::string srcdir, std::string dstdir);

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

class ListNode {
 public:
  ListNode* next;
  ListNode* tail;
};

template <class StaticConfig>
class LogEntryListNode {
 public:
  LogEntryListNode() : next{nullptr}, nentries{0} {}

  LogEntryListNode<StaticConfig>* next;

  uint64_t nentries;
  WriteRowLogEntry<StaticConfig>* entries[6];

  bool push(WriteRowLogEntry<StaticConfig>* wrle) {
    if (nentries < 6) {
      entries[nentries] = wrle;
      nentries++;

      return true;
    }

    return false;
  }
} __attribute__((aligned(64)));

template <class StaticConfig>
class LogEntryList {
 public:
  LogEntryList()
      : next{nullptr},
        tail{nullptr},
        table_index{0},
        row_id{0},
        tail_ts{0},
        nentries{0} {}

  LogEntryListNode<StaticConfig>* next;
  LogEntryListNode<StaticConfig>* tail;
  uint64_t table_index;
  uint64_t row_id;
  uint64_t tail_ts;

  uint64_t nentries;
  WriteRowLogEntry<StaticConfig>* entries[2];

  void append(LogEntryListNode<StaticConfig>* n) {
    n->next = nullptr;
    tail->next = n;
    tail = n;
  }

  bool push(WriteRowLogEntry<StaticConfig>* wrle) {
    if (nentries < 2) {
      entries[nentries] = wrle;
      nentries++;

      return true;
    }

    if (tail == nullptr || !tail->push(wrle)) {
      auto n = new LogEntryListNode<StaticConfig>();
      tail->next = n;
      tail = n;

      n->push(wrle);
    }

    return true;
  }

} __attribute__((aligned(64)));

template <class StaticConfig, class Class>
class SchedulerPool {
 public:
  typedef typename StaticConfig::Alloc Alloc;

  static_assert(std::is_base_of<ListNode, Class>::value,
                "Class must implement ListNode.");

  static constexpr uint64_t kAllocSize = 1024;
  static constexpr uint64_t class_size = sizeof(Class);

  SchedulerPool(Alloc* alloc, uint64_t size, size_t lcore);
  ~SchedulerPool();

  std::pair<Class*, Class*> allocate_n();

  void free_n(Class* head, Class* tail, uint64_t n);

  void print_status() const {
    printf("SchedulerPool on numa node %" PRIu8 "\n", numa_id_);
    printf(" lists:");
    printf("  in use: %7.3lf GB\n",
           static_cast<double>((total_classes_ - free_classes_) * class_size) /
               1000000000.);
    printf("  free:   %7.3lf GB\n",
           static_cast<double>(free_classes_ * class_size) / 1000000000.);
    printf("  total:  %7.3lf GB\n",
           static_cast<double>(total_classes_ * class_size) / 1000000000.);
  }

 private:
  Alloc* alloc_;
  uint64_t size_;
  uint8_t numa_id_;

  uint64_t total_classes_;
  uint64_t free_classes_;
  char* pages_;
  ListNode* next_class_;

  volatile uint32_t lock_;
} __attribute__((aligned(64)));

struct IOLock {
  volatile IOLock* next;
  volatile bool locked;
  volatile bool done;
} __attribute__((__aligned__(64)));

struct MyTraits : public moodycamel::ConcurrentQueueDefaultTraits {
  static const size_t BLOCK_SIZE = 2048;  // Use bigger blocks
};

template <class StaticConfig>
class IOThread {
 public:
  IOThread(DB<StaticConfig>* db,
           std::shared_ptr<MmappedLogFile<StaticConfig>> log,
           //  SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>* pool,
           pthread_barrier_t* start_barrier,
           moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>*
               io_queue,
           moodycamel::ProducerToken* io_queue_ptok, IOLock* my_lock,
           uint16_t id, uint16_t nios, uint16_t db_id, uint16_t lcore);
  ~IOThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;
  std::thread thread_;
  pthread_barrier_t* start_barrier_;
  // SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>* pool_;
  LogEntryList<StaticConfig>* allocated_lists_;
  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>* io_queue_;
  moodycamel::ProducerToken* io_queue_ptok_;
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

  uint64_t build_local_lists(RowVersionPool<StaticConfig>* pool,
                             std::size_t segment,
                             std::vector<LogEntryList<StaticConfig>*>& lists);
};

template <class StaticConfig>
class SchedulerThread {
 public:
  SchedulerThread(moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*,
                                              MyTraits>* io_queue,
                  moodycamel::ProducerToken* io_queue_ptok,
                  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*,
                                              MyTraits>* scheduler_queue,
                  moodycamel::ProducerToken* scheduler_queue_ptok,
                  pthread_barrier_t* start_barrier, uint16_t id,
                  uint16_t lcore);

  ~SchedulerThread();

  void start();
  void stop();

 private:
  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>* io_queue_;
  moodycamel::ProducerToken* io_queue_ptok_;
  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>*
      scheduler_queue_;
  moodycamel::ProducerToken* scheduler_queue_ptok_;
  pthread_barrier_t* start_barrier_;
  uint16_t id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;

  void run();
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
      //  SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>* pool,
      moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>*
          scheduler_queue,
      moodycamel::ProducerToken* scheduler_queue_ptok, WorkerMinWTS* min_wts,
      pthread_barrier_t* start_barrier, uint16_t id, uint16_t db_id,
      uint16_t lcore);

  ~WorkerThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  // SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>* pool_;
  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>*
      scheduler_queue_;
  moodycamel::ProducerToken* scheduler_queue_ptok_;
  WorkerMinWTS* min_wts_;
  pthread_barrier_t* start_barrier_;
  LogEntryList<StaticConfig>* freed_lists_head_;
  LogEntryList<StaticConfig>* freed_lists_tail_;
  uint64_t num_freed_lists_;
  uint16_t id_;
  uint16_t db_id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;

  void run();

  RowVersion<StaticConfig>* wrle_to_rv(RowVersionPool<StaticConfig>* pool,
                                       WriteRowLogEntry<StaticConfig>* wrle);

  void free_list(LogEntryList<StaticConfig>* list);
};

template <class StaticConfig>
class CopyCat : public CCCInterface<StaticConfig> {
 public:
  typedef typename StaticConfig::Alloc Alloc;

  CopyCat(DB<StaticConfig>* db, Alloc* alloc, uint64_t max_sched_pool_size,
          uint64_t sched_pool_lcore, uint16_t nloggers, uint16_t nios,
          uint16_t nschedulers, uint16_t nworkers, std::string logdir);

  ~CopyCat();

  void set_logdir(std::string logdir) { logdir_ = logdir; }

  void preprocess_logs(std::string srcdir, std::string dstdir);

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
  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits> io_queue_;
  moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>
      scheduler_queue_;
  moodycamel::ProducerToken io_queue_ptok_;
  moodycamel::ProducerToken scheduler_queue_ptok_;
  DB<StaticConfig>* db_;
  // SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>* pool_;

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

  pthread_barrier_t snapshot_barrier_;
  SnapshotThread<StaticConfig>* snapshot_manager_;
};

struct TableRowID {
  uint64_t table_index;
  uint64_t row_id;

  bool operator==(const TableRowID& t) const {
    return table_index == t.table_index && row_id == t.row_id;
  }

  bool operator<(const TableRowID& t) const {
    return table_index < t.table_index ||
           (table_index == t.table_index && row_id < t.row_id);
  }
};

template <class StaticConfig>
class PerTransactionQueue : public ListNode {
 public:
  RowVersion<StaticConfig>* head_rv;
  RowVersion<StaticConfig>* tail_rv;

  uint64_t nwrites;
  TableRowID* write_set;

  uint64_t nconflicts;
  uint64_t ndependents;
  PerTransactionQueue<StaticConfig>** dependents;

  volatile uint32_t lock;

  volatile bool scheduled;
  volatile bool executed;

  bool add_dependent(PerTransactionQueue<StaticConfig>* dep) {
    while (__sync_lock_test_and_set(&lock, 1) == 1) ::mica::util::pause();

    if (executed) {
      __sync_lock_release(&lock);
      return false;
    }

    dependents[ndependents++] = dep;

    __sync_lock_release(&lock);
    return true;
  }

  bool ack() {
    while (!scheduled) ::mica::util::pause();

    while (__sync_lock_test_and_set(&lock, 1) == 1) ::mica::util::pause();
    nconflicts -= 1;
    bool done = (nconflicts == 0);
    __sync_lock_release(&lock);

    return done;
  }

  void ack_dependents(std::set<PerTransactionQueue<StaticConfig>*>& done) {
    while (__sync_lock_test_and_set(&lock, 1) == 1) ::mica::util::pause();
    executed = true;
    __sync_lock_release(&lock);

    for (uint64_t i = 0; i < ndependents; i++) {
      PerTransactionQueue<StaticConfig>* dep = dependents[i];
      if (dep->ack()) {
        done.insert(dep);
      }
    }
  }

  void push_rv(RowVersion<StaticConfig>* rv) {
    if (head_rv == nullptr) {
      head_rv = rv;
    }

    if (tail_rv != nullptr) {
      tail_rv->older_rv = rv;
    }

    rv->older_rv = nullptr;
    tail_rv = rv;
  }
} __attribute__((aligned(64)));

template <class StaticConfig>
class KuaFuIOThread {
 public:
  KuaFuIOThread(
      DB<StaticConfig>* db, std::shared_ptr<MmappedLogFile<StaticConfig>> log,
      SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool,
      pthread_barrier_t* start_barrier,
      moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
          io_queue,
      moodycamel::ProducerToken* io_queue_ptok, IOLock* my_lock, uint16_t id,
      uint16_t nios, uint16_t db_id, uint16_t lcore);
  ~KuaFuIOThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;
  std::thread thread_;
  pthread_barrier_t* start_barrier_;
  SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool_;
  PerTransactionQueue<StaticConfig>* allocated_queues_;
  moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
      io_queue_;
  moodycamel::ProducerToken* io_queue_ptok_;
  IOLock* my_lock_;
  uint16_t id_;
  uint16_t db_id_;
  uint16_t lcore_;
  uint16_t nios_;
  volatile bool stop_;

  void run();

  void acquire_io_lock();
  void release_io_lock();

  PerTransactionQueue<StaticConfig>* allocate_queue();

  void build_local_queues(
      RowVersionPool<StaticConfig>* pool, std::size_t segment,
      std::vector<PerTransactionQueue<StaticConfig>*>& queues);
};

template <class StaticConfig>
class KuaFuSchedulerThread {
 public:
  KuaFuSchedulerThread(
      moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
          io_queue,
      moodycamel::ProducerToken* io_queue_ptok,
      moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
          scheduler_queue,
      moodycamel::ProducerToken* scheduler_queue_ptok,
      pthread_barrier_t* start_barrier, uint16_t id, uint16_t lcore);

  ~KuaFuSchedulerThread();

  void start();
  void stop();

 private:
  std::unordered_map<TableRowID, PerTransactionQueue<StaticConfig>*> conflicts_;
  moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
      io_queue_;
  moodycamel::ProducerToken* io_queue_ptok_;
  moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
      scheduler_queue_;
  moodycamel::ProducerToken* scheduler_queue_ptok_;
  pthread_barrier_t* start_barrier_;
  uint16_t id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;

  void run();
};

template <class StaticConfig>
class KuaFuWorkerThread {
 public:
  KuaFuWorkerThread(
      DB<StaticConfig>* db,
      SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool,
      moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
          scheduler_queue,
      WorkerMinWTS* min_wts, pthread_barrier_t* start_barrier, uint16_t id,
      uint16_t db_id, uint16_t lcore);

  ~KuaFuWorkerThread();

  void start();
  void stop();

 private:
  DB<StaticConfig>* db_;
  SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool_;
  moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
      scheduler_queue_;
  moodycamel::ProducerToken scheduler_queue_ptok_;
  WorkerMinWTS* min_wts_;
  pthread_barrier_t* start_barrier_;
  PerTransactionQueue<StaticConfig>* freed_queues_head_;
  PerTransactionQueue<StaticConfig>* freed_queues_tail_;
  uint64_t num_freed_queues_;
  uint16_t id_;
  uint16_t db_id_;
  uint16_t lcore_;
  volatile bool stop_;
  std::thread thread_;

  void run();

  void execute_queue(Context<StaticConfig>* ctx, Transaction<StaticConfig>* tx,
                     PerTransactionQueue<StaticConfig>* queue);

  void free_queue(PerTransactionQueue<StaticConfig>* queue);
};

template <class StaticConfig>
class KuaFu : public CCCInterface<StaticConfig> {
  typedef typename StaticConfig::Alloc Alloc;

 public:
  KuaFu(DB<StaticConfig>* db, Alloc* alloc, uint64_t max_sched_pool_size,
        uint64_t sched_pool_lcore, uint16_t nloggers, uint16_t nios,
        uint16_t nschedulers, uint16_t nworkers, std::string logdir);
  ~KuaFu();

  void set_logdir(std::string logdir) { logdir_ = logdir; }

  void preprocess_logs(std::string srcdir, std::string dstdir);

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
  moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>
      io_queue_;
  moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>
      scheduler_queue_;
  moodycamel::ProducerToken io_queue_ptok_;
  moodycamel::ProducerToken scheduler_queue_ptok_;
  DB<StaticConfig>* db_;
  SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool_;

  uint16_t nloggers_;
  uint16_t nios_;
  uint16_t nschedulers_;
  uint16_t nworkers_;
  uint16_t db_id_;
  uint16_t lcore_;

  std::string logdir_;
  std::shared_ptr<MmappedLogFile<StaticConfig>> log_;

  pthread_barrier_t io_barrier_;
  std::vector<KuaFuIOThread<StaticConfig>*> ios_;

  std::vector<IOLock> io_locks_;

  pthread_barrier_t scheduler_barrier_;
  KuaFuSchedulerThread<StaticConfig>* scheduler_;

  std::vector<WorkerMinWTS> min_wtss_;
  pthread_barrier_t worker_barrier_;
  std::vector<KuaFuWorkerThread<StaticConfig>*> workers_;
};

};  // namespace transaction
};  // namespace mica

namespace std {
template <>
struct hash<mica::transaction::TableRowID> {
  std::size_t operator()(
      const mica::transaction::TableRowID& t) const noexcept {
    std::size_t h1 = std::hash<uint64_t>{}(t.table_index);
    std::size_t h2 = std::hash<uint64_t>{}(t.row_id);
    return h1 ^ h2;
  }
};
};  // namespace std

#include "replication_impl/copycat.h"
#include "replication_impl/io_thread.h"
#include "replication_impl/scheduler_pool.h"
#include "replication_impl/scheduler_thread.h"
#include "replication_impl/snapshot_thread.h"
#include "replication_impl/worker_thread.h"

#include "replication_impl/kuafu.h"
#include "replication_impl/kuafu_io_thread.h"
#include "replication_impl/kuafu_scheduler_thread.h"
#include "replication_impl/kuafu_worker_thread.h"

#endif
