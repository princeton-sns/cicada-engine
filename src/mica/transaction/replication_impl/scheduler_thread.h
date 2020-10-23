#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_

#include "mica/transaction/replication.h"

#include <memory>
#include <thread>

namespace mica {
namespace transaction {

using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::nanoseconds;

template <class StaticConfig>
std::unordered_map<uint64_t, LogEntryList<StaticConfig>*>
    SchedulerThread<StaticConfig>::waiting_queues_{};

template <class StaticConfig>
SchedulerThread<StaticConfig>::SchedulerThread(
    std::shared_ptr<MmappedLogFile<StaticConfig>> log,
    SchedulerPool<StaticConfig>* pool,
    tbb::concurrent_queue<LogEntryList<StaticConfig>*>* scheduler_queue,
    moodycamel::ReaderWriterQueue<std::pair<uint64_t, uint64_t>>* op_count_queue,
    std::vector<moodycamel::ReaderWriterQueue<LogEntryList<StaticConfig>*>*> ack_queues,
    pthread_barrier_t* start_barrier, uint16_t id, uint16_t nschedulers,
    SchedulerLock* my_lock)
    : log_{log},
      pool_{pool},
      allocated_nodes_{nullptr},
      allocated_lists_{nullptr},
      scheduler_queue_{scheduler_queue},
      op_count_queue_{op_count_queue},
      ack_queues_{ack_queues},
      start_barrier_{start_barrier},
      id_{id},
      nschedulers_{nschedulers},
      my_lock_{my_lock},
      stop_{false},
      thread_{} {};

template <class StaticConfig>
SchedulerThread<StaticConfig>::~SchedulerThread() {
  for (const auto& item : waiting_queues_) {
    auto queue = item.second;
    while (queue != nullptr) {
      free_nodes_and_list(item.second);
      queue = queue->next;
    }
  }

  waiting_queues_.clear();
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&SchedulerThread<StaticConfig>::run, this};
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::acquire_scheduler_lock() {
  while (my_lock_->locked) {
    mica::util::pause();
  }
  my_lock_->locked = true;
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::release_scheduler_lock(bool done) {
  volatile SchedulerLock* next = my_lock_->next;
  while (next->done) {
    next = next->next;
  }
  my_lock_->next = next;
  if (my_lock_->next != my_lock_) {
    my_lock_->done = done;
  }
  next->locked = false;
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::run() {
  printf("Starting replica scheduler: %u\n", id_);

  printf("pinning to thread %d\n", (id_ + 1) + nschedulers_);
  mica::util::lcore.pin_thread((id_ + 1) + nschedulers_);

  nanoseconds time_noncritical{0};
  nanoseconds time_waiting{0};
  nanoseconds time_critical{0};
  nanoseconds time_total{0};
  uint64_t nentries = 0;

  std::size_t nsegments = log_->get_nsegments();

  std::unordered_map<uint64_t, LogEntryList<StaticConfig>*> local_lists{};
  std::vector<std::pair<uint64_t, uint64_t>> op_counts{};

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;

  high_resolution_clock::time_point start;
  high_resolution_clock::time_point end;
  nanoseconds diff;

  std::size_t waiting_size = 0;

  pthread_barrier_wait(start_barrier_);
  run_start = high_resolution_clock::now();

  for (std::size_t cur_segment = id_; cur_segment < nsegments;
       cur_segment += nschedulers_) {
    // printf("starting segment %lu at %lu\n", cur_segment,
    //        duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count());

    start = high_resolution_clock::now();
    nentries += build_local_lists(cur_segment, local_lists, op_counts);
    end = high_resolution_clock::now();
    diff = duration_cast<nanoseconds>(end - start);
    time_noncritical += diff;

    start = high_resolution_clock::now();
    acquire_scheduler_lock();
    // Memory barrier here so next scheduler thread sees all updates
    // to all SPSC queues' internal variables
    ::mica::util::memory_barrier();
    end = high_resolution_clock::now();
    diff = duration_cast<nanoseconds>(end - start);
    time_waiting += diff;

    start = high_resolution_clock::now();
    // Ack executed rows
    ack_executed_rows();

    // Notify snapshot manager of transaction op counts
    for (const auto& o : op_counts) {
      op_count_queue_->enqueue(o);
    }

    // Enqueue new queues
    for (const auto& item : local_lists) {
      auto row_id = item.first;
      auto queue = item.second;

      auto search = waiting_queues_.find(row_id);
      if (search == waiting_queues_.end()) {  // Not found
        // printf("pushing queue at %p with %lu entries\n", queue, queue->nentries);
        scheduler_queue_->push(queue);
        // printf("setting waiting queue at %p\n", next);
        waiting_queues_[row_id] = nullptr;
        // printf("pushed row id %lu at %lu, %lu\n", row_id,
        //        duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count(),
        //        scheduler_queue_->unsafe_size());
      } else {  // Found
        auto queue2 = search->second;
        if (queue2 == nullptr) {
          waiting_queues_[row_id] = queue;
          // printf("setting waiting queue at %p\n", queue);
        } else {
          queue2->append(queue);
        }
      }
    }

    waiting_size = waiting_queues_.size();

    release_scheduler_lock();
    end = high_resolution_clock::now();
    diff = duration_cast<nanoseconds>(end - start);
    time_critical += diff;

    start = high_resolution_clock::now();
    local_lists.clear();
    op_counts.clear();
    end = high_resolution_clock::now();
    diff = duration_cast<nanoseconds>(end - start);
    time_noncritical += diff;

    // printf("finished segment %lu at %lu\n", cur_segment,
    //        duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count());
  }

  while (waiting_size != 0) {
    start = high_resolution_clock::now();
    acquire_scheduler_lock();
    end = high_resolution_clock::now();
    diff = duration_cast<nanoseconds>(end - start);
    time_waiting += diff;

    start = high_resolution_clock::now();
    ack_executed_rows();
    waiting_size = waiting_queues_.size();

    release_scheduler_lock();
    end = high_resolution_clock::now();
    diff = duration_cast<nanoseconds>(end - start);
    time_critical += diff;

    // std::this_thread::sleep_for(std::chrono::seconds(1));
  }

  // Mark my lock as done
  start = high_resolution_clock::now();
  acquire_scheduler_lock();
  end = high_resolution_clock::now();
  diff = duration_cast<nanoseconds>(end - start);
  time_waiting += diff;

  start = high_resolution_clock::now();
  release_scheduler_lock(true);
  end = high_resolution_clock::now();
  diff += duration_cast<nanoseconds>(end - start);
  time_critical += diff;

  run_end = high_resolution_clock::now();
  diff = duration_cast<nanoseconds>(run_end - run_start);
  time_total += diff;

  printf("Exiting replica scheduler: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
  printf("Time noncritical: %ld nanoseconds\n", time_noncritical.count());
  printf("Time critical: %ld nanoseconds\n", time_critical.count());
  printf("Time waiting: %ld nanoseconds\n", time_waiting.count());
};

template <class StaticConfig>
void SchedulerThread<StaticConfig>::ack_executed_rows() {
  for (auto ack_queue : ack_queues_) {
    LogEntryList<StaticConfig>* queue;
    while (ack_queue->try_dequeue(queue)) {
      uint64_t row_id = queue->row_id;
      // printf("acking row id %lu at %lu\n", row_id,
      //        duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count());
      auto search = waiting_queues_.find(row_id);
      if (search != waiting_queues_.end()) {  // Found
        auto queue_next = search->second;
        if (queue_next != nullptr) {

          // printf("pushing queue at %p with %lu entries\n", queue, queue->nentries);
          scheduler_queue_->push(queue_next);

          waiting_queues_[row_id] = nullptr;
          // printf("setting waiting queue at %p\n", next);
          // printf("pushed row id %lu at %lu, %lu\n", row_id,
          //        duration_cast<nanoseconds>(high_resolution_clock::now().time_since_epoch()).count(),
          //        scheduler_queue_->unsafe_size());
        } else {
          waiting_queues_.erase(row_id);
          // printf("erasing nullptr waiting queue\n");
        }
      } else {
        throw std::runtime_error("unexpected row id: " + row_id);
      }

      while (queue != nullptr) {
        auto next = queue->next;
        free_list(queue);
        queue = next;
      }
    }
  }
};

template <class StaticConfig>
LogEntryList<StaticConfig>* SchedulerThread<StaticConfig>::allocate_list() {
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
}

template <class StaticConfig>
void SchedulerThread<StaticConfig>::free_nodes_and_list(
    LogEntryList<StaticConfig>* list) {
  // LogEntryNode* next = list->list;
  // while (next != nullptr) {
  //   LogEntryNode* temp = next->next;
  //   free_node(next);
  //   next = temp;
  // }

  // list->list = nullptr;
  free_list(list);
}

template <class StaticConfig>
void SchedulerThread<StaticConfig>::free_list(
    LogEntryList<StaticConfig>* list) {
  list->next = allocated_lists_;
  allocated_lists_ = list;
}

template <class StaticConfig>
LogEntryNode* SchedulerThread<StaticConfig>::allocate_node() {
  if (allocated_nodes_ == nullptr) {
    allocated_nodes_ = pool_->allocate_node(1024);
    if (allocated_nodes_ == nullptr) {
      printf("pool->allocate_node() returned nullptr\n");
    }
  }

  LogEntryNode* next = allocated_nodes_;
  allocated_nodes_ = allocated_nodes_->next;

  return next;
}

template <class StaticConfig>
void SchedulerThread<StaticConfig>::free_node(LogEntryNode* node) {
  node->next = allocated_nodes_;
  allocated_nodes_ = node;
}

template <class StaticConfig>
uint64_t SchedulerThread<StaticConfig>::build_local_lists(
    std::size_t segment,
    std::unordered_map<uint64_t, LogEntryList<StaticConfig>*>& lists,
    std::vector<std::pair<uint64_t, uint64_t>>& op_counts) {
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

    if (txn_ts != last_txn_ts) {
      if (last_txn_ts != 0) {
        // printf("op_counts.emplace_back(%lu, %lu)\n", last_txn_ts, op_count);
        op_counts.emplace_back(last_txn_ts, op_count);
      }

      op_count = 0;
      last_txn_ts = txn_ts;
    }
    op_count += 1;

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

  if (txn_ts != 0) {
    // printf("op_counts.emplace_back(%lu, %lu)\n", txn_ts, op_count);
    op_counts.emplace_back(txn_ts, op_count);
  }

  return lf->nentries;
};
};  // namespace transaction
};  // namespace mica

#endif
