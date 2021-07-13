#ifndef MICA_TRANSACTION_REPLICATION_IMPL_IO_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_IO_THREAD_H_

#include "mica/transaction/replication.h"

#include "absl/container/flat_hash_map.h"

#include <unordered_map>

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
IOThread<StaticConfig>::IOThread(
    DB<StaticConfig>* db, std::shared_ptr<MmappedLogFile<StaticConfig>> log,
    SchedulerPool<StaticConfig, LogEntryList<StaticConfig>>* pool,
    pthread_barrier_t* start_barrier,
    moodycamel::ConcurrentQueue<LogEntryList<StaticConfig>*, MyTraits>*
        io_queue,
    moodycamel::ProducerToken* io_queue_ptok, IOLock* my_lock, uint16_t id,
    uint16_t nios, uint16_t db_id, uint16_t lcore)
    : db_{db},
      log_{log},
      thread_{},
      start_barrier_{start_barrier},
      pool_{pool},
      allocated_lists_{nullptr},
      io_queue_{io_queue},
      io_queue_ptok_{io_queue_ptok},
      my_lock_{my_lock},
      id_{id},
      db_id_{db_id},
      lcore_{lcore},
      nios_{nios},
      stop_{false} {};

template <class StaticConfig>
IOThread<StaticConfig>::~IOThread(){};

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
  RowVersionPool<StaticConfig>* pool = db_->row_version_pool(db_id_);
  std::vector<LogEntryList<StaticConfig>*> local_lists{};

  nanoseconds time_total{0};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  // uint64_t n = 0;
  uint64_t i = 0;
  uint64_t max = 2048;
  // LogEntryList<StaticConfig>* queues[max];

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  for (std::size_t cur_segment = id_; cur_segment < nsegments;
       cur_segment += nios_) {
    build_local_lists(pool, cur_segment, local_lists);

    acquire_io_lock();

    for (i = 0; local_lists.size() - i >= max; i += max) {
      io_queue_->enqueue_bulk(*io_queue_ptok_, &local_lists[i], max);
    }

    if (local_lists.size() - i > 0) {
      io_queue_->enqueue_bulk(*io_queue_ptok_, &local_lists[i],
                              local_lists.size() - i);
    }

    release_io_lock();

    local_lists.clear();
  }

  run_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(run_end - run_start);

  printf("Exiting replica IO thread: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
};

template <class StaticConfig>
LogEntryList<StaticConfig>* IOThread<StaticConfig>::allocate_list() {
  if (allocated_lists_ == nullptr) {
    auto pair = pool_->allocate_n();
    auto head = pair.first;
    auto tail = pair.second;
    tail->next = allocated_lists_;
    allocated_lists_ = head;
    if (allocated_lists_ == nullptr) {
      printf("pool->allocate_list() returned nullptr\n");
    }
  }

  LogEntryList<StaticConfig>* list = allocated_lists_;
  allocated_lists_ =
      static_cast<LogEntryList<StaticConfig>*>(allocated_lists_->next);

  list->next = nullptr;
  list->nentries = 0;
  list->head_rv = nullptr;
  list->tail_rv = nullptr;

  // printf("allocated new queue at %p\n", list);

  return list;
};

template <class StaticConfig>
uint64_t IOThread<StaticConfig>::build_local_lists(
    RowVersionPool<StaticConfig>* pool, std::size_t segment,
    std::vector<LogEntryList<StaticConfig>*>& lists) {
  absl::flat_hash_map<TableRowID, LogEntryList<StaticConfig>*> index{32768};
  LogFile<StaticConfig>* lf = log_->get_lf(segment);
  // lf->print();

  char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

  BeginTxnLogEntry<StaticConfig>* btle = nullptr;
  WriteRowLogEntry<StaticConfig>* wrle = nullptr;

  for (uint64_t i = 0; i < lf->nentries; i++) {
    LogEntry<StaticConfig>* le = reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
    LogEntryType type = le->type;
    std::size_t size = le->size;
    // le->print();

    // uint64_t row_id;
    // uint64_t table_index;
    // uint64_t ts;
    RowVersion<StaticConfig>* rv;
    // uint32_t data_size;
    // uint16_t size_cls;
    // uint8_t numa_id;
    // switch (type) {
    //   case LogEntryType::BEGIN_TXN:
    //     btle = static_cast<BeginTxnLogEntry<StaticConfig>*>(le);
    //     // btle->print();
    //     break;

    //   case LogEntryType::WRITE_ROW:
    //     wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
    //     row_id = wrle->row_id;
    //     table_index = wrle->table_index;
    //     ts = wrle->rv.wts.t2;

    //     data_size = wrle->rv.data_size;
    //     size_cls =
    //         SharedRowVersionPool<StaticConfig>::data_size_to_class(data_size);

    //     //rv = pool->allocate(size_cls);

    //     //numa_id = rv->numa_id;
    //     //std::memcpy(rv, &wrle->rv, sizeof(wrle->rv) + data_size);
    //     //rv->numa_id = numa_id;
    //     //rv->size_cls = size_cls;

    //     // wrle->print();
    //     break;

    //   default:
    //     throw std::runtime_error(
    //         "build_local_lists: Unexpected log entry type.");
    // }

    if (type == LogEntryType::WRITE_ROW) {
      WriteRowLogEntry<StaticConfig>* wrle =
          static_cast<WriteRowLogEntry<StaticConfig>*>(le);
      uint64_t row_id = wrle->row_id;
      uint64_t table_index = wrle->table_index;
      uint64_t ts = wrle->rv.wts.t2;

      TableRowID key{table_index, row_id};
      LogEntryList<StaticConfig>* list = nullptr;

      auto search = index.find(key);
      if (search == index.end()) {  // Not found
        list = allocate_list();
        list->table_index = table_index;
        list->row_id = row_id;
        list->tail_ts = ts;
        //list->push(rv);

        //list->push(rv);

        index.emplace(key, list);
        lists.push_back(list);
      } else {
        search->second->push(rv);
      }
    }

    ptr += size;
  }

  //  std::cout << "Index size: " << std::to_string(index.size()) << std::endl;

  return lf->nentries;
};

};  // namespace transaction
};  // namespace mica

#endif
