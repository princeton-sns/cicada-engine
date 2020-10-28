#ifndef MICA_TRANSACTION_REPLICATION_IMPL_IO_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_IO_THREAD_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
IOThread<StaticConfig>::IOThread(
    std::shared_ptr<MmappedLogFile<StaticConfig>> log,
    SchedulerPool<StaticConfig>* pool, pthread_barrier_t* start_barrier,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue,
    IOLock* my_lock,
    uint16_t id, uint16_t nios)
    : log_{log},
      thread_{},
      start_barrier_{start_barrier},
      pool_{pool},
      allocated_lists_{nullptr},
      io_queue_{io_queue},
      my_lock_{my_lock},
      id_{id},
      nios_{nios},
      stop_{false} {};

template <class StaticConfig>
IOThread<StaticConfig>::~IOThread(){
  LogEntryList<StaticConfig>* queue = allocated_lists_;
  while (queue != nullptr) {
    auto next = queue->next;
    pool_->free_list(queue);
    queue = next;
  }
};

template <class StaticConfig>
void IOThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&IOThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void IOThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void IOThread<StaticConfig>::acquire_io_lock() {
  while (my_lock_->locked) {
    mica::util::pause();
  }
  my_lock_->locked = true;
};

template <class StaticConfig>
void IOThread<StaticConfig>::release_io_lock() {
  my_lock_->next->locked = false;
};

template <class StaticConfig>
void IOThread<StaticConfig>::run() {
  printf("Starting replica IO thread: %u\n", id_);

  printf("pinning to thread %d\n", id_);
  mica::util::lcore.pin_thread(id_);

  std::size_t nsegments = log_->get_nsegments();

  robin_hood::unordered_map<uint64_t, LogEntryList<StaticConfig>*>
      local_lists{};

  pthread_barrier_wait(start_barrier_);

  for (std::size_t cur_segment = id_; cur_segment < nsegments;
       cur_segment += nios_) {
    build_local_lists(cur_segment, local_lists);

    acquire_io_lock();
    // Memory barrier here so next IO thread sees all updates
    // to all SPSC queues' internal variables
    // ::mica::util::memory_barrier();

    for (const auto& item : local_lists) {
      io_queue_->enqueue(item.second);
      // printf("pushed queue: %p\n", item.second);
    }

    release_io_lock();

    local_lists.clear();
  }

  printf("Exiting replica IO thread: %u\n", id_);
};

template <class StaticConfig>
LogEntryList<StaticConfig>* IOThread<StaticConfig>::allocate_list() {
  if (allocated_lists_ == nullptr) {
    allocated_lists_ = pool_->allocate_list(1024);
    if (allocated_lists_ == nullptr) {
      printf("pool->allocate_list() returned nullptr\n");
    }
  }

  LogEntryList<StaticConfig>* list = allocated_lists_;
  allocated_lists_ =
      reinterpret_cast<LogEntryList<StaticConfig>*>(allocated_lists_->next);

  list->next = nullptr;
  list->tail = list;
  list->cur = list->buf;
  list->nentries = 0;

  // printf("allocated new queue at %p\n", list);

  return list;
};

template <class StaticConfig>
void IOThread<StaticConfig>::build_local_lists(
    std::size_t segment,
    robin_hood::unordered_map<uint64_t, LogEntryList<StaticConfig>*>& lists) {
  LogFile<StaticConfig>* lf = log_->get_lf(segment);
  // lf->print();

  char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

  InsertRowLogEntry<StaticConfig>* irle = nullptr;
  WriteRowLogEntry<StaticConfig>* wrle = nullptr;

  uint64_t last_txn_ts = 0;
  uint64_t txn_ts = 0;
  uint64_t op_count = 0;
  for (uint64_t i = 0; i < lf->nentries; i++) {
    LogEntry<StaticConfig>* le = reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
    LogEntryType type = le->type;
    std::size_t size = le->size;
    // le->print();

    uint64_t row_id = 0;
    char* tbl_name = nullptr;
    switch (type) {
      case LogEntryType::INSERT_ROW:
        irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
        row_id = irle->row_id;
        tbl_name = irle->tbl_name;
        txn_ts = irle->txn_ts;
        // irle->print();
        break;

      case LogEntryType::WRITE_ROW:
        wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
        row_id = wrle->row_id;
        tbl_name = wrle->tbl_name;
        txn_ts = wrle->txn_ts;
        // wrle->print();
        break;

      default:
        throw std::runtime_error(
            "build_local_lists: Unexpected log entry type.");
    }

    LogEntryList<StaticConfig>* list = nullptr;
    auto search = lists.find(row_id);
    if (search == lists.end()) {  // Not found
      list = allocate_list();
      list->row_id = row_id;
      std::memcpy(list->tbl_name, tbl_name, StaticConfig::kMaxTableNameSize);
      lists[row_id] = list;
    } else {
      list = search->second;
    }

    if (!list->tail->push(le, size)) {
      LogEntryList<StaticConfig>* list2 = allocate_list();
      list2->push(le, size);
      list->append(list2);
    }

    ptr += size;
  }
};
};  // namespace transaction
};  // namespace mica

#endif
