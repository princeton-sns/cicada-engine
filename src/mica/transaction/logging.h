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
#include <mutex>
#include <set>
#include <sstream>
#include <string>
#include <thread>
#include <unordered_map>
#include <utility>

#include "mica/common.h"
#include "mica/transaction/context.h"
#include "mica/transaction/db.h"
#include "mica/transaction/page_pool.h"
#include "mica/transaction/row.h"
#include "mica/transaction/row_version_pool.h"
#include "mica/transaction/transaction.h"
#include "mica/util/posix_io.h"

#define MICA_LOG_INIT_DIR "/mnt/huge/cicada/log/init"
#define MICA_LOG_WARMUP_DIR "/mnt/huge/cicada/log/warmup"
#define MICA_LOG_WORKLOAD_DIR "/mnt/huge/cicada/log/workload"
#define MICA_RELAY_INIT_DIR "/mnt/huge/cicada/relay/init"
#define MICA_RELAY_WARMUP_DIR "/mnt/huge/cicada/relay/warmup"
#define MICA_RELAY_WORKLOAD_DIR "/mnt/huge/cicada/relay/workload"
#define MICA_LOG_DIR "/mnt/huge/cicada/db"
#define MICA_RELAY_DIR "/mnt/huge/cicada/relay"

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

  void enable();
  void disable();
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

  void enable() {}
  void disable() {}
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

  void enable() { enabled_ = true; }
  void disable() { enabled_ = false; }

 private:
  class Mmapping {
  public:
    void* addr;
    std::size_t len;
    int fd;

    Mmapping(void* addr, std::size_t len, int fd) : addr{addr}, len{len}, fd{fd} {}
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

  bool enabled_;

  LogBuffer mmap_log_buf(uint16_t thread_id, uint64_t file_index);

  char* alloc_log_buf(uint16_t thread_id, std::size_t nbytes);
  void release_log_buf(uint16_t thread_id);
};

template <class StaticConfig>
class CCCInterface {
 public:
  void read_logs();

  void set_logdir(std::string logdir);
  void preprocess_logs();

  void start_workers();
  void stop_workers();
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
  uint64_t nentries;
  LogEntry<StaticConfig> entries[0];

  void print() {
    std::stringstream stream;

    stream << "Log file:" << std::endl;
    stream << "N entries: " << nentries << std::endl;
    stream << "First entry ptr: " << &entries[0] << std::endl;

    std::cout << stream.str();
  }
};

class LogEntryRef {
 public:
  int fd;
  long offset;
  std::size_t size;
  LogEntryType type;
  uint64_t txn_ts;

  static bool compare(const LogEntryRef& r1, const LogEntryRef& r2) {
    switch (r1.type) {
      case LogEntryType::CREATE_TABLE:
        return true;
      case LogEntryType::CREATE_HASH_IDX:
        return r2.type != LogEntryType::CREATE_TABLE;
      case LogEntryType::INSERT_ROW:
      case LogEntryType::WRITE_ROW:
        if (r2.type == LogEntryType::CREATE_TABLE ||
            r2.type == LogEntryType::CREATE_HASH_IDX)
          return false;
        else
          return r1.txn_ts < r2.txn_ts;
    }

    throw std::runtime_error("Unhandled comparison!");
  }

  void print() {
    std::stringstream stream;

    stream << std::endl;
    stream << "LogEntryRef:" << std::endl;
    stream << "FD: " << fd << std::endl;
    stream << "Offset: " << offset << std::endl;
    stream << "Size: " << size << std::endl;
    stream << "Type: " << std::to_string(static_cast<uint8_t>(type))
           << std::endl;
    stream << "Transaction TS: " << txn_ts << std::endl;

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

template <class StaticConfig>
class CopyCat : public CCCInterface<StaticConfig> {
 public:
  CopyCat(DB<StaticConfig>* db, uint16_t nloggers, uint16_t nworkers,
          std::string logdir);

  ~CopyCat();

  void read_logs();

  void set_logdir(std::string logdir) { logdir_ = logdir; }

  void preprocess_logs();

  void start_workers();
  void stop_workers();

 private:
  DB<StaticConfig>* db_;
  std::size_t len_;

  uint16_t nloggers_;
  uint16_t nworkers_;

  std::string logdir_;

  pthread_barrier_t worker_barrier_;
  std::vector<std::thread> workers_;
  std::atomic<bool> workers_stop_;

  void worker_thread(DB<StaticConfig>* db, uint16_t id);

  void create_table(DB<StaticConfig>* db,
                    CreateTableLogEntry<StaticConfig>* le);

  void create_hash_index(DB<StaticConfig>* db,
                         CreateHashIndexLogEntry<StaticConfig>* le);

  void insert_row(Context<StaticConfig>* ctx,
                  Transaction<StaticConfig>* tx,
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

  void write_row(Context<StaticConfig>* ctx,
                 Transaction<StaticConfig>* tx,
                 RowAccessHandle<StaticConfig>* rah,
                 WriteRowLogEntry<StaticConfig>* le);
  void write_data_row(Context<StaticConfig>* ctx,
                      Transaction<StaticConfig>* tx,
                      RowAccessHandle<StaticConfig>* rah,
                      WriteRowLogEntry<StaticConfig>* le);
  void write_hash_idx_row(Context<StaticConfig>* ctx,
                          Transaction<StaticConfig>* tx,
                          RowAccessHandle<StaticConfig>* rah,
                          WriteRowLogEntry<StaticConfig>* le);
};

template <class StaticConfig>
class FileLogger : public LoggerInterface<StaticConfig> {
 private:
  struct ReplicationWork {
    char* ptr;
    std::size_t n;
  };

  class LoggerThread {
   private:
    FileLogger* logger_;

    std::string fname_;
    int fd_;
    int i_;
    uint8_t numa_id_;

    std::unordered_map<char*, uint16_t> npending_;
    std::unordered_map<char*, std::size_t> write_reqs_;
    std::vector<char*> bufs_;
    std::size_t next_bufi_;
    char* p_;
    std::size_t bytes_remaining_;

    std::thread thd_;
    std::atomic<bool> stop_;

    std::mutex m_;
    std::condition_variable work_cv_;
    bool need_alloc_;
    bool new_release_;
    bool need_fsync_;

    std::condition_variable alloc_cv_;

    char* containing_buf(char* p) {
      uint64_t ps =
          static_cast<uint64_t>(PagePool<StaticConfig>::kPageSize) - 1;
      uint64_t i = reinterpret_cast<uint64_t>(p) & ~ps;
      return reinterpret_cast<char*>(i);
    }

    void run() {
      printf("Starting logger thread %d\n", i_);

      std::vector<std::pair<char*, std::size_t>> ready{};

      // ::mica::util::lcore.pin_thread(i_);

      char* curr_buf;
      char* next_buf;
      std::unique_lock<std::mutex> lock(m_, std::defer_lock);

      bool na = false;
      bool nr = false;
      bool nf = false;

      while (true) {
        lock.lock();
        while (!need_alloc_ && !new_release_ && !need_fsync_ && !stop_)
          work_cv_.wait(lock);
        na = need_alloc_;
        nr = new_release_;
        nf = need_fsync_;
        lock.unlock();

        if (na) {
          lock.lock();

          curr_buf = containing_buf(p_);
          write_reqs_[curr_buf] =
              PagePool<StaticConfig>::kPageSize - bytes_remaining_;
          nr = true;

          next_buf = bufs_[next_bufi_];
          auto search = write_reqs_.find(next_buf);
          if (search == write_reqs_.end()) {
            p_ = next_buf;
            next_bufi_ = (next_bufi_ + 1) % bufs_.size();
            bytes_remaining_ = PagePool<StaticConfig>::kPageSize;
          } else {
            throw std::runtime_error("WARNING: Ran out of buffers!");
          }
          need_alloc_ = false;
          alloc_cv_.notify_all();
          lock.unlock();
        }

        if (nr) {
          lock.lock();

          auto it = write_reqs_.begin();
          while (it != write_reqs_.end()) {
            auto npending = npending_[it->first];
            if (npending == 0) {
              std::pair<char*, std::size_t> p(it->first, it->second);
              ready.emplace_back(p);
              it = write_reqs_.erase(it);
            } else {
              it++;
            }
          }

          new_release_ = false;
          lock.unlock();

          for (std::pair<char*, std::size_t> r : ready) {
            std::cout << "Writing buffer: " << (void*)r.first << " of size "
                      << r.second << std::endl;
            ::mica::util::PosixIO::Write(fd_, r.first, r.second);
          }

          ready.clear();
        }

        if (nf) {
          lock.lock();
          need_fsync_ = false;
          lock.unlock();

          ::mica::util::PosixIO::FSync(fd_);
        }

        if (stop_) break;
      }

      printf("Exiting logger thread %d\n", i_);
    }

   public:
    LoggerThread(FileLogger* logger, int i)
        : logger_{logger},
          i_{i},
          numa_id_{0},  // TODO: set numa ID correctly
          npending_{},
          write_reqs_{},
          bufs_{},
          bytes_remaining_{PagePool<StaticConfig>::kPageSize},
          stop_{false},
          m_{},
          work_cv_{},
          need_alloc_{false},
          new_release_{false},
          need_fsync_{false},
          alloc_cv_{} {
      std::stringstream s;
      s << StaticConfig::kDBLogDir << "/out." << i << ".log";
      fname_ = s.str();
      fd_ = ::mica::util::PosixIO::Open(fname_.c_str(),
                                        O_WRONLY | O_APPEND | O_CREAT,
                                        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

      // TODO: make 2 a parameter
      char* buf = nullptr;
      for (auto i = 0; i < 2; i++) {
        buf = logger->page_pool(numa_id_)->allocate();
        bufs_.emplace_back(buf);
        npending_[buf] = 0;
      }

      p_ = bufs_[0];
      next_bufi_ = 1;
    }

    ~LoggerThread() {
      flush();

      ::mica::util::PosixIO::Close(fd_);

      for (char* p : bufs_) {
        logger_->page_pool(numa_id_)->free(p);
      }
    }

    void flush() {
      m_.lock();
      need_alloc_ = true;
      need_fsync_ = true;
      work_cv_.notify_one();
      m_.unlock();
    }

    void start() { thd_ = std::thread{&LoggerThread::run, this}; }

    void stop() {
      stop_ = true;
      work_cv_.notify_one();
      thd_.join();
    }

    char* alloc(std::size_t size) {
      char* p = nullptr;
      char* buf = nullptr;

      std::unique_lock<std::mutex> lock(m_);
      while (bytes_remaining_ < size) {
        need_alloc_ = true;
        work_cv_.notify_one();
        alloc_cv_.wait(lock);
      }

      p = p_;

      buf = containing_buf(p);
      npending_[buf] += 1;

      p_ += size;
      bytes_remaining_ -= size;

      lock.unlock();

      return p;
    }

    void release(char* p) {
      m_.lock();

      char* buf = containing_buf(p);
      npending_[buf] -= 1;
      new_release_ = true;
      work_cv_.notify_one();
      m_.unlock();
    }
  };

  PagePool<StaticConfig>** page_pools_;
  std::vector<LoggerThread*> loggers_;

  std::thread log_consumer_;
  std::atomic<bool> log_consumer_stop_;

  uint16_t nloggers_;

  // Replica workers
  std::vector<std::thread> replica_workers_;
  std::atomic<bool> replica_worker_stop_;
  std::deque<ReplicationWork> replicationq_;
  std::mutex replicationq_lock_;

 public:
  FileLogger(PagePool<StaticConfig>** page_pools, uint16_t nloggers)
      : page_pools_{page_pools},
        loggers_{},
        log_consumer_stop_{false},
        nloggers_{nloggers},
        replica_workers_{},
        replica_worker_stop_{false},
        replicationq_{},
        replicationq_lock_{} {
    for (int i = 0; i < nloggers_; i++) {
      LoggerThread* lt = new LoggerThread{this, i};
      loggers_.push_back(lt);
      lt->start();
    }
  }

  ~FileLogger() {
    for (LoggerThread* lt : loggers_) {
      lt->stop();
      delete lt;
    }

    loggers_.clear();
  }

  PagePool<StaticConfig>* page_pool(uint8_t numa_id) {
    return page_pools_[numa_id];
  }
  const PagePool<StaticConfig>* page_pool(uint8_t numa_id) const {
    return page_pools_[numa_id];
  }

  void start_workers(::mica::transaction::DB<StaticConfig>* db,
                     std::vector<uint16_t> thread_ids) {
    for (uint16_t thread_id : thread_ids) {
      replica_workers_.emplace_back(
          std::thread{&FileLogger::replica_worker_thread, this, db, thread_id});
    }
  }

  void stop_workers() {
    replica_worker_stop_ = true;

    for (auto& w : replica_workers_) {
      w.join();
    }
  }

  void start_log_consumer(::mica::transaction::DB<StaticConfig>* db,
                          uint16_t thread_id) {
    log_consumer_ =
        std::thread{&FileLogger::log_consumer_thread, this, db, thread_id};
  }

  void stop_log_consumer() {
    log_consumer_stop_ = true;
    log_consumer_.join();
  }

  bool flush_log() {
    for (LoggerThread* lt : loggers_) {
      lt->flush();
    }

    return true;
  }

  LoggerThread* get_logger_thread(uint16_t thread_id) {
    uint16_t i = thread_id % nloggers_;
    return loggers_[i];
  }

  bool log(Context<StaticConfig>* ctx, const Table<StaticConfig>* tbl) {
    uint16_t thread_id = ctx->thread_id();

    LoggerThread* lt = get_logger_thread(thread_id);

    char* buf = lt->alloc(sizeof(CreateTableLogEntry<StaticConfig>));

    CreateTableLogEntry<StaticConfig>* le =
        reinterpret_cast<CreateTableLogEntry<StaticConfig>*>(buf);

    le->size = sizeof *le;
    le->type = LogEntryType::CREATE_TABLE;
    le->cf_count = tbl->cf_count();

    std::memcpy(&le->name[0], tbl->name().c_str(), 1 + tbl->name().size());
    for (uint16_t cf_id = 0; cf_id < le->cf_count; cf_id++) {
      le->data_size_hints[cf_id] = tbl->data_size_hint(cf_id);
    }

    lt->release(buf);

    return true;
  }

  template <bool UniqueKey>
  bool log(Context<StaticConfig>* ctx,
           const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx) {
    uint16_t thread_id = ctx->thread_id();

    LoggerThread* lt = get_logger_thread(thread_id);

    char* buf = lt->alloc(sizeof(CreateHashIndexLogEntry<StaticConfig>));

    CreateHashIndexLogEntry<StaticConfig>* le =
        reinterpret_cast<CreateHashIndexLogEntry<StaticConfig>*>(buf);

    le->size = sizeof *le;
    le->type = LogEntryType::CREATE_HASH_IDX;
    le->expected_num_rows = idx->expected_num_rows();
    le->unique_key = UniqueKey;

    std::memcpy(&le->name[0], idx->index_table()->name().c_str(),
                1 + idx->index_table()->name().size());

    std::memcpy(&le->main_tbl_name[0], idx->main_table()->name().c_str(),
                1 + idx->main_table()->name().size());

    lt->release(buf);

    return true;
  }

  bool log(Context<StaticConfig>* ctx, const Transaction<StaticConfig>* tx) {
    uint16_t thread_id = ctx->thread_id();

    LoggerThread* lt = get_logger_thread(thread_id);

    auto accesses = tx->accesses();
    auto iset_idx = tx->iset_idx();
    auto wset_idx = tx->wset_idx();

    std::size_t data_size = 0;
    for (auto j = 0; j < tx->iset_size(); j++) {
      int i = iset_idx[j];
      RowAccessItem<StaticConfig> item = accesses[i];
      RowVersion<StaticConfig>* write_rv = item.write_rv;
      data_size += write_rv->data_size;
    }

    for (auto j = 0; j < tx->wset_size(); j++) {
      int i = wset_idx[j];
      RowAccessItem<StaticConfig> item = accesses[i];
      RowVersion<StaticConfig>* write_rv = item.write_rv;
      data_size += write_rv->data_size;
    }

    std::size_t log_size =
        tx->iset_size() * sizeof(InsertRowLogEntry<StaticConfig>) +
        tx->wset_size() * sizeof(WriteRowLogEntry<StaticConfig>) + data_size;

    // char* buf = lt->alloc(log_size);
    // char* ptr = buf;

    // for (auto j = 0; j < tx->iset_size(); j++) {
    //   int i = iset_idx[j];
    //   RowAccessItem<StaticConfig> item = accesses[i];
    //   RowVersion<StaticConfig>* write_rv = item.write_rv;
    //   uint32_t data_size = write_rv->data_size;
    //   char* data = write_rv->data;
    //   Table<StaticConfig>* tbl = item.tbl;
    //   InsertRowLogEntry<StaticConfig>* le =
    //       reinterpret_cast<InsertRowLogEntry<StaticConfig>*>(ptr);

    //   std::size_t size = sizeof *le + data_size;

    //   le->size = size;
    //   le->type = LogEntryType::INSERT_ROW;

    //   le->txn_ts = tx->ts().t2;
    //   le->cf_id = item.cf_id;
    //   le->row_id = item.row_id;

    //   le->wts = write_rv->wts.t2;
    //   le->rts = write_rv->rts.get().t2;

    //   le->data_size = data_size;
    //   le->tbl_type = static_cast<uint8_t>(tbl->type());

    //   std::memcpy(&le->tbl_name[0], tbl->name().c_str(),
    //               1 + tbl->name().size());
    //   std::memcpy(le->data, data, data_size);

    //   // le->print();

    //   ptr += size;
    // }

    // for (auto j = 0; j < tx->wset_size(); j++) {
    //   int i = wset_idx[j];
    //   RowAccessItem<StaticConfig> item = accesses[i];
    //   RowVersion<StaticConfig>* write_rv = item.write_rv;
    //   uint32_t data_size = write_rv->data_size;
    //   char* data = write_rv->data;
    //   Table<StaticConfig>* tbl = item.tbl;
    //   WriteRowLogEntry<StaticConfig>* le =
    //       reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(ptr);

    //   std::size_t size = sizeof *le + data_size;

    //   le->size = size;
    //   le->type = LogEntryType::WRITE_ROW;

    //   le->txn_ts = tx->ts().t2;
    //   le->cf_id = item.cf_id;
    //   le->row_id = item.row_id;

    //   le->wts = write_rv->wts.t2;
    //   le->rts = write_rv->rts.get().t2;

    //   le->data_size = data_size;
    //   le->tbl_type = static_cast<uint8_t>(tbl->type());

    //   std::memcpy(&le->tbl_name[0], tbl->name().c_str(),
    //               1 + tbl->name().size());
    //   std::memcpy(le->data, data, data_size);

    //   // le->print();

    //   ptr += size;
    // }

    // lt->release(buf);

    return true;
  }

  void process_relay_logs(Context<StaticConfig>* ctx, uint64_t nloggers) {
    auto db = ctx->db();

    std::vector<int> fds{};
    std::vector<LogEntryRef> refs{};

    std::stringstream fname;
    fname << StaticConfig::kRelayLogDir << "/relay.log";
    int outfd = ::mica::util::PosixIO::Open(
        fname.str().c_str(), O_WRONLY | O_APPEND | O_CREAT,
        S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    PagePool<StaticConfig>* page_pool = db->page_pool(ctx->numa_id());
    char* page = page_pool->allocate();

    LogEntryRef ref;
    LogEntry<StaticConfig>* le = nullptr;
    InsertRowLogEntry<StaticConfig>* irle = nullptr;
    WriteRowLogEntry<StaticConfig>* wrle = nullptr;

    for (uint16_t tid = 0; tid < nloggers; tid++) {
      fname.str("");
      fname << StaticConfig::kRelayLogDir << "/out." << tid << ".log";
      int fd = ::mica::util::PosixIO::Open(fname.str().c_str(), O_RDONLY);
      fds.push_back(fd);
    }

    for (int fd : fds) {
      int i = 0;
      //std::cout << "FD: " << fd << std::endl;
      while (true) {
        long offset = ::mica::util::PosixIO::Seek(fd, 0, SEEK_CUR);
        bool ret = read_log_entry(fd, page, PagePool<StaticConfig>::kPageSize);
        if (!ret) break;

        le = reinterpret_cast<LogEntry<StaticConfig>*>(page);

        ref.fd = fd;
        ref.offset = offset;
        ref.size = le->size;
        ref.type = le->type;
        ref.txn_ts = 0;

        switch (le->type) {
          case LogEntryType::INSERT_ROW:
            irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
            ref.txn_ts = irle->txn_ts;
            break;
          case LogEntryType::WRITE_ROW:
            wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
            ref.txn_ts = wrle->txn_ts;
            break;
          case LogEntryType::CREATE_TABLE:
            std::cout << "Log index: " << i << std::endl;
            break;
          default:
            break;
        }

        refs.push_back(ref);
        i += 1;
      }
    }

    std::sort(refs.begin(), refs.end(), LogEntryRef::compare);

    for (LogEntryRef r : refs) {
      // r.print();

      if (::mica::util::PosixIO::PRead(r.fd, page, r.size, r.offset) !=
          static_cast<ssize_t>(r.size)) {
        throw std::runtime_error("Error while reading from offset");
      }

      ::mica::util::PosixIO::Write(outfd, page, r.size);
    }

    ::mica::util::PosixIO::Close(outfd);

    for (int fd : fds) {
      ::mica::util::PosixIO::Close(fd);
    }

    page_pool->free(page);
  }

 private:
  bool read_log_entry(int fd, char* buf, std::size_t max_size) {
    LogEntry<StaticConfig>* rle = nullptr;

    LogEntry<StaticConfig> le;

    if (::mica::util::PosixIO::Read(fd, &le, sizeof le) == sizeof le) {
      if (le.size > max_size) {
        throw std::runtime_error(
            "WARNING: Log entry is larger than buffer size!");
      }

      switch (le.type) {
        case LogEntryType::CREATE_TABLE:
          rle = reinterpret_cast<CreateTableLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          // static_cast<CreateTableLogEntry<StaticConfig>*>(rle)->print();
          break;

        case LogEntryType::CREATE_HASH_IDX:
          rle = reinterpret_cast<CreateHashIndexLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          //static_cast<CreateHashIndexLogEntry<StaticConfig>*>(rle)->print();
          break;

        case LogEntryType::INSERT_ROW:
          rle = reinterpret_cast<InsertRowLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          //static_cast<InsertRowLogEntry<StaticConfig>*>(rle)->print();
          break;

        case LogEntryType::WRITE_ROW:
          rle = reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(buf);

          rle->size = le.size;
          rle->type = le.type;

          ::mica::util::PosixIO::Read(
              fd, reinterpret_cast<char*>(rle) + sizeof(LogEntry<StaticConfig>),
              le.size - sizeof(LogEntry<StaticConfig>));

          //static_cast<WriteRowLogEntry<StaticConfig>*>(rle)->print();
          break;
      }

      return true;
    }

    return false;
  }

  void insert_data_row(Context<StaticConfig>* ctx,
                       InsertRowLogEntry<StaticConfig>* le) {
    auto db = ctx->db();
    Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
    if (tbl == nullptr) {
      throw std::runtime_error("Failed to find table " +
                               std::string{le->tbl_name});
    }

    typename StaticConfig::Timestamp txn_ts;
    txn_ts.t2 = le->txn_ts;

    Transaction<StaticConfig> tx(ctx);
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.new_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
      throw std::runtime_error("Failed to create new row " + le->row_id);
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void insert_hash_idx_row(Context<StaticConfig>* ctx,
                           InsertRowLogEntry<StaticConfig>* le) {
    auto db = ctx->db();

    auto unique_hash_idx =
        db->get_hash_index_unique_u64(std::string{le->tbl_name});
    auto nonunique_hash_idx =
        db->get_hash_index_nonunique_u64(std::string{le->tbl_name});
    Table<StaticConfig>* tbl = nullptr;
    if (unique_hash_idx != nullptr) {
      tbl = unique_hash_idx->index_table();
    } else if (nonunique_hash_idx != nullptr) {
      tbl = nonunique_hash_idx->index_table();
    }

    if (tbl == nullptr) {
      throw std::runtime_error("Failed to find index table.");
    }

    typename StaticConfig::Timestamp txn_ts;
    txn_ts.t2 = le->txn_ts;

    Transaction<StaticConfig> tx(ctx);
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.new_row(tbl, le->cf_id, le->row_id, false, le->data_size)) {
      throw std::runtime_error("Failed to create new row " + le->row_id);
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void insert_row(Context<StaticConfig>* ctx,
                  InsertRowLogEntry<StaticConfig>* le) {
    TableType tbl_type = static_cast<TableType>(le->tbl_type);

    switch (tbl_type) {
      case TableType::DATA:
        insert_data_row(ctx, le);
        break;
      case TableType::HASH_IDX:
        insert_hash_idx_row(ctx, le);
        break;
      default:
        throw std::runtime_error("Insert: Unsupported table type.");
    }
  }

  void write_data_row(Context<StaticConfig>* ctx,
                      WriteRowLogEntry<StaticConfig>* le) {
    auto db = ctx->db();

    Table<StaticConfig>* tbl = db->get_table(std::string{le->tbl_name});
    if (tbl == nullptr) {
      throw std::runtime_error("Failed to find table " +
                               std::string{le->tbl_name});
    }

    typename StaticConfig::Timestamp txn_ts;
    txn_ts.t2 = le->txn_ts;

    Transaction<StaticConfig> tx(ctx);
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.peek_row(tbl, le->cf_id, le->row_id, false, false, true) ||
        !rah.write_row(le->data_size)) {
      throw std::runtime_error("Failed to write row.");
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void write_hash_idx_row(Context<StaticConfig>* ctx,
                          WriteRowLogEntry<StaticConfig>* le) {
    auto db = ctx->db();
    auto unique_hash_idx =
        db->get_hash_index_unique_u64(std::string{le->tbl_name});
    auto nonunique_hash_idx =
        db->get_hash_index_nonunique_u64(std::string{le->tbl_name});
    Table<StaticConfig>* tbl = nullptr;
    if (unique_hash_idx != nullptr) {
      tbl = unique_hash_idx->index_table();
    } else if (nonunique_hash_idx != nullptr) {
      tbl = nonunique_hash_idx->index_table();
    }

    if (tbl == nullptr) {
      throw std::runtime_error("Failed to find index table.");
    }

    typename StaticConfig::Timestamp txn_ts;
    txn_ts.t2 = le->txn_ts;

    Transaction<StaticConfig> tx(ctx);
    RowAccessHandle<StaticConfig> rah(&tx);
    if (!tx.begin(false, &txn_ts)) {
      throw std::runtime_error("Failed to begin transaction.");
    }

    if (!rah.peek_row(tbl, le->cf_id, le->row_id, false, false, true)) {
      throw std::runtime_error("Failed to write row: peek.");
    }

    if (!rah.write_row(le->data_size)) {
      throw std::runtime_error("Failed to write row: write.");
    }

    char* data = rah.data();
    std::memcpy(data, le->data, le->data_size);

    Result result;
    tx.commit(&result);
    if (result != Result::kCommitted) {
      throw std::runtime_error("Failed to commit transaction.");
    }
  }

  void write_row(Context<StaticConfig>* ctx,
                 WriteRowLogEntry<StaticConfig>* le) {
    TableType tbl_type = static_cast<TableType>(le->tbl_type);

    switch (tbl_type) {
      case TableType::DATA:
        write_data_row(ctx, le);
        break;
      case TableType::HASH_IDX:
        write_hash_idx_row(ctx, le);
        break;
      default:
        throw std::runtime_error("Insert: Unsupported table type.");
    }
  }

  void log_consumer_thread(::mica::transaction::DB<StaticConfig>* db,
                           uint16_t thread_id) {
    printf("Starting log consumer\n");
    db->activate(thread_id);
    auto ctx = db->context(thread_id);

    printf("Replicating combined relay log\n");

    PagePool<StaticConfig>* page_pool = db->page_pool(ctx->numa_id());
    char* page = page_pool->allocate();

    std::stringstream fname;
    fname << StaticConfig::kRelayLogDir << "/relay.log";
    int fd = ::mica::util::PosixIO::Open(fname.str().c_str(), O_RDONLY);

    // uint64_t total_committed = 0;
    // struct timeval tv_start;
    // struct timeval tv_end;

    while (true) {
      CreateTableLogEntry<StaticConfig>* ctle = nullptr;
      CreateHashIndexLogEntry<StaticConfig>* hile = nullptr;

      while (read_log_entry(fd, page, PagePool<StaticConfig>::kPageSize)) {
        LogEntry<StaticConfig>* le =
            reinterpret_cast<LogEntry<StaticConfig>*>(page);

        // le->print();

        switch (le->type) {
          case LogEntryType::CREATE_TABLE:
            ctle = static_cast<CreateTableLogEntry<StaticConfig>*>(le);

            if (!db->create_table(std::string{ctle->name}, ctle->cf_count,
                                  ctle->data_size_hints)) {
              throw std::runtime_error("Failed to create table: " +
                                       std::string{ctle->name});
            }

            // Reset start time
            // gettimeofday(&tv_start, nullptr);
            break;

          case LogEntryType::CREATE_HASH_IDX:
            hile = static_cast<CreateHashIndexLogEntry<StaticConfig>*>(le);

            if (hile->unique_key) {
              if (!db->create_hash_index_unique_u64(
                      std::string{hile->name},
                      db->get_table(std::string{hile->main_tbl_name}),
                      hile->expected_num_rows)) {
                throw std::runtime_error("Failed to create unique index: " +
                                         std::string{hile->name});
              }

            } else if (!db->create_hash_index_nonunique_u64(
                           std::string{hile->name},
                           db->get_table(std::string{hile->main_tbl_name}),
                           hile->expected_num_rows)) {
              throw std::runtime_error("Failed to create unique index: " +
                                       std::string{hile->name});
            }

            // Reset start time
            // gettimeofday(&tv_start, nullptr);
            break;

          case LogEntryType::INSERT_ROW:
          case LogEntryType::WRITE_ROW:
            replicationq_lock_.lock();
            ReplicationWork w = {reinterpret_cast<char*>(le), 1};
            replicationq_.push_back(w);
            replicationq_lock_.unlock();
            page = page_pool->allocate();
            if (page == nullptr) {
              throw std::runtime_error("Ran out of pages for log consumer!");
            }
            break;
        };
      }

      // gettimeofday(&tv_end, nullptr);

      // double start =
      //     (double)tv_start.tv_sec * 1. + (double)tv_start.tv_usec * 0.000001;
      // double end =
      //     (double)tv_end.tv_sec * 1. + (double)tv_end.tv_usec * 0.000001;

      // printf("Replication stats:\n");
      // printf("row writes:                   %7.3lf M\n",
      //        static_cast<double>(total_committed) * 0.000001);
      // printf("row throughput:                   %7.3lf M/sec\n",
      //        static_cast<double>(total_committed) / (end - start) * 0.000001);

      if (log_consumer_stop_) break;
    }

    ::mica::util::PosixIO::Close(fd);
    page_pool->free(page);

    db->deactivate(thread_id);
    printf("Exiting log consumer\n");
  }

  void replica_worker_thread(::mica::transaction::DB<StaticConfig>* db,
                             uint16_t worker_id) {
    printf("Starting replica worker\n");
    db->activate(worker_id);
    auto ctx = db->context(worker_id);

    PagePool<StaticConfig>* page_pool = db->page_pool(ctx->numa_id());

    LogEntry<StaticConfig>* le = nullptr;
    InsertRowLogEntry<StaticConfig>* irle = nullptr;
    WriteRowLogEntry<StaticConfig>* wrle = nullptr;

    char* ptr = nullptr;
    char* p = nullptr;
    std::size_t size = 0;
    ReplicationWork w;

    while (true) {
      replicationq_lock_.lock();
      size = replicationq_.size();
      if (size != 0) {
        w = replicationq_.front();
        replicationq_.pop_front();
      }
      replicationq_lock_.unlock();

      if (size == 0 && replica_worker_stop_) break;

      if (size != 0) {
        ptr = w.ptr;
        p = ptr;

        for (std::size_t i = 0; i < w.n; i++) {
          le = reinterpret_cast<LogEntry<StaticConfig>*>(p);

          // le->print();

          switch (le->type) {
            case LogEntryType::INSERT_ROW:
              irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
              insert_row(ctx, irle);
              break;

            case LogEntryType::WRITE_ROW:
              wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
              write_row(ctx, wrle);
              break;

            default:
              throw std::runtime_error(
                  "Unexpected log entry in replication queue: " +
                  std::to_string(static_cast<uint8_t>(le->type)));
          }

          p += le->size;
        }

        page_pool->free(ptr);
      }

      // std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }

    db->deactivate(worker_id);
    printf("Exiting replica worker\n");
  }
};
}  // namespace transaction
}  // namespace mica

#endif
