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
    DB<StaticConfig>* db, std::shared_ptr<MmappedLogFile<StaticConfig>> log,
    SchedulerPool<StaticConfig>* pool, pthread_barrier_t* start_barrier,
    moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>* io_queue,
    IOLock* my_lock, uint16_t id, uint16_t nios, uint16_t db_id, uint16_t lcore)
    : db_{db},
      log_{log},
      thread_{},
      start_barrier_{start_barrier},
      pool_{pool},
      allocated_lists_{nullptr},
      io_queue_{io_queue},
      my_lock_{my_lock},
      id_{id},
      db_id_{db_id},
      lcore_{lcore},
      nios_{nios},
      stop_{false} {};

template <class StaticConfig>
IOThread<StaticConfig>::~IOThread() {
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
  printf("Starting replica IO thread: %u %u %u\n", id_, db_id_, lcore_);

  printf("pinning to thread %d\n", lcore_);
  mica::util::lcore.pin_thread(lcore_);

  std::size_t nsegments = log_->get_nsegments();

  std::vector<LogEntryList<StaticConfig>*> local_lists{};

  nanoseconds time_total{0};
  nanoseconds diff;

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  for (std::size_t cur_segment = id_; cur_segment < nsegments;
       cur_segment += nios_) {
    build_local_lists(cur_segment, local_lists);

    acquire_io_lock();
    // Memory barrier here so next IO thread sees all updates
    // to all SPSC queues' internal variables
    ::mica::util::memory_barrier();

    for (const auto& item : local_lists) {
      io_queue_->enqueue(item);
    }

    release_io_lock();

    local_lists.clear();
  }

  run_end = high_resolution_clock::now();
  diff = duration_cast<nanoseconds>(run_end - run_start);
  time_total += diff;

  printf("Exiting replica IO thread: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
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
  // list->cur = list->buf;
  list->nentries = 0;
  list->head_rv = nullptr;
  list->tail_rv = nullptr;

  // printf("allocated new queue at %p\n", list);

  return list;
};

template <class StaticConfig>
uint64_t IOThread<StaticConfig>::build_local_lists(
    std::size_t segment, std::vector<LogEntryList<StaticConfig>*>& lists) {
  auto pool = db_->row_version_pool(db_id_);

  robin_hood::unordered_map<uint64_t, LogEntryList<StaticConfig>*> index{};

  LogFile<StaticConfig>* lf = log_->get_lf(segment);
  // lf->print();

  char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

  InsertRowLogEntry<StaticConfig>* irle = nullptr;
  WriteRowLogEntry<StaticConfig>* wrle = nullptr;

  for (uint64_t i = 0; i < lf->nentries; i++) {
    LogEntry<StaticConfig>* le = reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
    LogEntryType type = le->type;
    std::size_t size = le->size;
    // le->print();

    uint64_t row_id = 0;
    std::size_t table_index = static_cast<std::size_t>(-1);
    uint64_t ts = 0;
    RowVersion<StaticConfig>* rv = nullptr;
    uint32_t data_size = 0;
    uint16_t size_cls = 0;
    switch (type) {
      case LogEntryType::INSERT_ROW:
        irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
        row_id = irle->row_id;
        table_index = irle->table_index;

        data_size = irle->data_size;
        size_cls =
            SharedRowVersionPool<StaticConfig>::data_size_to_class(data_size);

        rv = pool->allocate(size_cls);

        ts = irle->txn_ts;
        rv->wts.t2 = ts;
        rv->rts.init({ts});

        rv->status = RowVersionStatus::kCommitted;

        rv->data_size = static_cast<uint32_t>(data_size);
        std::memcpy(rv->data, irle->data, data_size);

        // irle->print();
        break;

      case LogEntryType::WRITE_ROW:
        wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
        row_id = wrle->row_id;
        table_index = wrle->table_index;

        data_size = wrle->data_size;
        size_cls =
            SharedRowVersionPool<StaticConfig>::data_size_to_class(data_size);

        rv = pool->allocate(size_cls);

        ts = wrle->txn_ts;
        rv->wts.t2 = ts;
        rv->rts.init({ts});

        rv->status = RowVersionStatus::kCommitted;

        rv->data_size = static_cast<uint32_t>(data_size);
        std::memcpy(rv->data, wrle->data, data_size);

        // wrle->print();
        break;

      default:
        throw std::runtime_error(
            "build_local_lists: Unexpected log entry type.");
    }

    LogEntryList<StaticConfig>* list = nullptr;
    auto search = index.find(row_id);
    if (search == index.end()) {  // Not found
      list = allocate_list();
      list->table_index = table_index;
      list->row_id = row_id;
      list->head_ts = ts;

      list->push(rv);

      index[row_id] = list;
      lists.push_back(list);
    } else {
      search->second->push(rv);
    }

    ptr += size;
  }

  return lf->nentries;
};

};  // namespace transaction
};  // namespace mica

#endif
