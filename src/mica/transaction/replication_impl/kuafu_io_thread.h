#ifndef MICA_TRANSACTION_REPLICATION_IMPL_KUAFU_IO_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_KUAFU_IO_THREAD_H_

#include <cstdlib>
#include <cstring>

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
KuaFuIOThread<StaticConfig>::KuaFuIOThread(
    DB<StaticConfig>* db, std::shared_ptr<MmappedLogFile<StaticConfig>> log,
    SchedulerPool<StaticConfig, PerTransactionQueue<StaticConfig>>* pool,
    pthread_barrier_t* start_barrier,
    moodycamel::ConcurrentQueue<PerTransactionQueue<StaticConfig>*, MyTraits>*
        io_queue,
    moodycamel::ProducerToken* io_queue_ptok, IOLock* my_lock, uint16_t id,
    uint16_t nios, uint16_t db_id, uint16_t lcore)
    : db_{db},
      log_{log},
      thread_{},
      start_barrier_{start_barrier},
      pool_{pool},
      allocated_queues_{nullptr},
      io_queue_{io_queue},
      io_queue_ptok_{io_queue_ptok},
      my_lock_{my_lock},
      id_{id},
      db_id_{db_id},
      lcore_{lcore},
      nios_{nios},
      stop_{false} {};

template <class StaticConfig>
KuaFuIOThread<StaticConfig>::~KuaFuIOThread(){};

template <class StaticConfig>
void KuaFuIOThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&KuaFuIOThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void KuaFuIOThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void KuaFuIOThread<StaticConfig>::acquire_io_lock() {
  while (my_lock_->locked) {
    mica::util::pause();
  }
  my_lock_->locked = true;
};

template <class StaticConfig>
void KuaFuIOThread<StaticConfig>::release_io_lock() {
  my_lock_->next->locked = false;
};

template <class StaticConfig>
void KuaFuIOThread<StaticConfig>::run() {
  printf("Starting KuaFu IO thread: %u %u %u\n", id_, db_id_, lcore_);

  printf("pinning to thread %d\n", lcore_);
  mica::util::lcore.pin_thread(lcore_);

  std::size_t nsegments = log_->get_nsegments();
  RowVersionPool<StaticConfig>* pool = db_->row_version_pool(db_id_);
  std::vector<PerTransactionQueue<StaticConfig>*> local_queues{};

  nanoseconds time_total{0};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  // uint64_t n = 0;
  uint64_t i = 0;
  uint64_t max = 2048;
  // PerTransactionQueue<StaticConfig>* queues[max];

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  for (std::size_t cur_segment = id_; cur_segment < nsegments;
       cur_segment += nios_) {
    build_local_queues(pool, cur_segment, local_queues);

    acquire_io_lock();

    for (i = 0; local_queues.size() - i >= max; i += max) {
      io_queue_->enqueue_bulk(*io_queue_ptok_, &local_queues[i], max);
    }

    if (local_queues.size() - i > 0) {
      io_queue_->enqueue_bulk(*io_queue_ptok_, &local_queues[i],
                              local_queues.size() - i);
    }

    release_io_lock();

    local_queues.clear();
  }

  run_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(run_end - run_start);

  printf("Exiting KuaFu IO thread: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
};

template <class StaticConfig>
PerTransactionQueue<StaticConfig>*
KuaFuIOThread<StaticConfig>::allocate_queue() {
  if (allocated_queues_ == nullptr) {
    auto pair = pool_->allocate_n();
    auto head = pair.first;
    auto tail = pair.second;
    tail->next = allocated_queues_;
    allocated_queues_ = head;
    if (allocated_queues_ == nullptr) {
      printf("pool->allocate_n() returned nullptr\n");
    }
  }

  PerTransactionQueue<StaticConfig>* queue = allocated_queues_;
  allocated_queues_ =
      static_cast<PerTransactionQueue<StaticConfig>*>(allocated_queues_->next);

  std::memset(queue, 0, sizeof *queue);

  // printf("allocated new queue at %p\n", queue);

  return queue;
};

template <class StaticConfig>
void KuaFuIOThread<StaticConfig>::build_local_queues(
    RowVersionPool<StaticConfig>* pool, std::size_t segment,
    std::vector<PerTransactionQueue<StaticConfig>*>& queues) {
  LogFile<StaticConfig>* lf = log_->get_lf(segment);
  // lf->print();

  char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

  PerTransactionQueue<StaticConfig>* queue = nullptr;

  BeginTxnLogEntry<StaticConfig>* btle;
  WriteRowLogEntry<StaticConfig>* wrle;
  uint64_t j = 0;
  uint64_t nwrites = 0;

  LogEntry<StaticConfig>* le;
  LogEntryType type;
  std::size_t size;

  uint64_t table_index;
  uint64_t row_id;
  RowVersion<StaticConfig>* rv;
  uint32_t data_size;
  uint16_t size_cls;
  uint8_t numa_id;

  for (uint64_t i = 0; i < lf->nentries; i++) {
    le = reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
    type = le->type;
    size = le->size;
    // le->print();

    switch (type) {
      case LogEntryType::BEGIN_TXN:
        btle = static_cast<BeginTxnLogEntry<StaticConfig>*>(le);

        queue = allocate_queue();

        nwrites = btle->nwrites;
        queue->nwrites = nwrites;

        // TODO: error checking
        // TODO: free write set
        queue->write_set =
            (TableRowID*)std::malloc(nwrites * sizeof(TableRowID));
        // TODO: free dependents
        queue->dependents = (PerTransactionQueue<StaticConfig>**)std::malloc(
            nwrites * sizeof(PerTransactionQueue<StaticConfig>*));
        j = 0;

        queues.push_back(queue);

        // btle->print();
        break;

      case LogEntryType::WRITE_ROW:
        if (queue != nullptr) {
          wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);

          // Set write set
          table_index = wrle->table_index;
          row_id = wrle->row_id;

          data_size = wrle->rv.data_size;
          size_cls =
              SharedRowVersionPool<StaticConfig>::data_size_to_class(data_size);

          rv = pool->allocate(size_cls);

          numa_id = rv->numa_id;
          std::memcpy(rv, &wrle->rv, sizeof(wrle->rv) + data_size);
          rv->numa_id = numa_id;
          rv->size_cls = size_cls;

          queue->write_set[j++] = {table_index, row_id};
          queue->push_rv(rv);
          // wrle->print();
        }
        break;

      default:
        throw std::runtime_error(
            "build_local_queues: Unexpected log entry type.");
    }

    ptr += size;
  }

  // Transaction spans two segments
  if (j != nwrites) {
    lf = log_->get_lf(segment + 1);
    // lf->print();

    ptr = reinterpret_cast<char*>(&lf->entries[0]);

    while (j < nwrites) {
      wrle = reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(ptr);
      size = wrle->size;
      if (wrle->type != LogEntryType::WRITE_ROW) {
        throw std::runtime_error(
            "build_local_queues: Expected write row log entry.");
      }

      // wrle->print();

      // Set write set
      table_index = wrle->table_index;
      row_id = wrle->row_id;

      data_size = wrle->rv.data_size;
      size_cls =
          SharedRowVersionPool<StaticConfig>::data_size_to_class(data_size);

      rv = pool->allocate(size_cls);

      numa_id = rv->numa_id;
      std::memcpy(rv, &wrle->rv, sizeof(wrle->rv) + data_size);
      rv->numa_id = numa_id;
      rv->size_cls = size_cls;

      queue->write_set[j++] = {table_index, row_id};
      queue->push_rv(rv);

      ptr += size;
    }
  }
};

};  // namespace transaction
};  // namespace mica

#endif
