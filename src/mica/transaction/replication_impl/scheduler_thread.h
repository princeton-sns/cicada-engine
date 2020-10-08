#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_

#include "mica/transaction/replication.h"

#include <memory>
#include <thread>

namespace mica {
  namespace transaction {

    using std::chrono::duration_cast;
    using std::chrono::microseconds;
    using std::chrono::high_resolution_clock;

    template <class StaticConfig>
    SchedulerThread<StaticConfig>::SchedulerThread(std::shared_ptr<MmappedLogFile<StaticConfig>> log,
                                                   SchedulerPool<StaticConfig>* pool,
                                                   SchedulerQueue<StaticConfig>* queue,
                                                   pthread_barrier_t* start_barrier,
                                                   uint16_t id, uint16_t nschedulers,
                                                   SchedulerLock* my_lock, SchedulerLock* next_lock)
      : log_{log},
        pool_{pool},
        allocated_nodes_{nullptr},
        allocated_lists_{nullptr},
        queue_{queue},
        start_barrier_{start_barrier},
        id_{id},
        nschedulers_{nschedulers},
        my_lock_{my_lock},
        next_lock_{next_lock},
        stop_{false},
        thread_{} {};

    template <class StaticConfig>
    SchedulerThread<StaticConfig>::~SchedulerThread(){};

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
    void SchedulerThread<StaticConfig>::run() {
      printf("Starting replica scheduler: %u\n", id_);

      mica::util::lcore.pin_thread(id_);

      if (id_ == 0) {
        my_lock_->locked = false;
      } else {
        my_lock_->locked = true;
      }

      microseconds time_noncritical{0};
      microseconds time_waiting{0};
      microseconds time_critical{0};
      uint64_t nentries = 0;

      std::size_t nsegments = log_->get_nsegments();

      robin_hood::unordered_map<uint64_t, LogEntryList*> local_lists{};

      pthread_barrier_wait(start_barrier_);

      for (std::size_t cur_segment = id_; cur_segment < nsegments;
           cur_segment += nschedulers_) {

        high_resolution_clock::time_point start = high_resolution_clock::now();
        nentries += build_local_lists(cur_segment, local_lists);
        high_resolution_clock::time_point end = high_resolution_clock::now();
        microseconds diff = duration_cast<microseconds>(end - start);
        time_noncritical += diff;

        start = high_resolution_clock::now();
        while (my_lock_->locked) {
          mica::util::pause();
        }
        my_lock_->locked = true;
        end = high_resolution_clock::now();
        diff = duration_cast<microseconds>(end - start);
        // printf("Thread %u waited %ld microseconds\n", id, diff.count());
        time_waiting += diff;

        start = high_resolution_clock::now();
        LogEntryList* to_deallocate = nullptr;
        for (const auto& item : local_lists) {
          auto row_id = item.first;
          auto list = item.second;

          to_deallocate = queue_->append(row_id, list);
          if (to_deallocate != nullptr) {
            free_nodes_and_list(to_deallocate);
          }
        }

        next_lock_->locked = false;
        end = high_resolution_clock::now();
        diff = duration_cast<microseconds>(end - start);
        time_critical += diff;

        start = high_resolution_clock::now();
        local_lists.clear();
        end = high_resolution_clock::now();
        diff = duration_cast<microseconds>(end - start);
        time_noncritical += diff;
      }

      printf("Exiting replica scheduler: %u\n", id_);
      printf("Time noncritical: %ld microseconds\n", time_noncritical.count());
      printf("Time critical: %ld microseconds\n", time_critical.count());
      printf("Time waiting: %ld microseconds\n", time_waiting.count());
      printf("N entries: %ld\n", nentries);
    };

    template <class StaticConfig>
    LogEntryList* SchedulerThread<StaticConfig>::allocate_list() {
      if (allocated_lists_ == nullptr) {
        // printf("callling pool->allocate_list()\n");
        allocated_lists_ = pool_->allocate_list(1024);
        if (allocated_lists_ == nullptr) {
          printf("pool->allocate_list() returned nullptr\n");
        }
      }

      LogEntryList* next = allocated_lists_;
      allocated_lists_ = reinterpret_cast<LogEntryList*>(allocated_lists_->list);

      return next;
    }

    template <class StaticConfig>
    void SchedulerThread<StaticConfig>::free_nodes_and_list(LogEntryList* list) {
      LogEntryNode* next = list->list;
      while (next != nullptr) {
        LogEntryNode* temp = next->next;
        printf("freeing node at %p\n", next);
        free_node(next);
        next = temp;
      }

      list->list = nullptr;
      free_list(list);
    }

    template <class StaticConfig>
    void SchedulerThread<StaticConfig>::free_list(LogEntryList* list) {
      list->list = reinterpret_cast<LogEntryNode*>(allocated_lists_);
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
    uint64_t SchedulerThread<StaticConfig>::build_local_lists(std::size_t segment,
                                                          robin_hood::unordered_map<uint64_t, LogEntryList*>& lists) {

      LogFile<StaticConfig>* lf = log_->get_lf(segment);
      // lf->print();

      char* ptr = reinterpret_cast<char*>(&lf->entries[0]);

      InsertRowLogEntry<StaticConfig>* irle = nullptr;
      WriteRowLogEntry<StaticConfig>* wrle = nullptr;

      for (uint64_t i = 0; i < lf->nentries; i++) {
        LogEntry<StaticConfig>* le =
          reinterpret_cast<LogEntry<StaticConfig>*>(ptr);
        // le->print();

        uint64_t row_id = 0;
        switch (le->type) {
        case LogEntryType::INSERT_ROW:
          irle = static_cast<InsertRowLogEntry<StaticConfig>*>(le);
          row_id = irle->row_id;
          // irle->print();
          break;

        case LogEntryType::WRITE_ROW:
          wrle = static_cast<WriteRowLogEntry<StaticConfig>*>(le);
          row_id = wrle->row_id;
          // wrle->print();
          break;

        default:
          throw std::runtime_error("build_local_lists: Unexpected log entry type.");
        }

        LogEntryNode* node = allocate_node();
        std::memset(node, 0, sizeof *node);
        node->ptr = ptr;

        // row_id = 0;
        LogEntryList* list = nullptr;
        auto search = lists.find(row_id);
        if (search == lists.end()) { // Not found
          // printf("allocting list for row_id: %lu\n", row_id);
          list = allocate_list();
          list->tail = nullptr;
          while (__sync_lock_test_and_set(&list->lock, 1) == 1) {
            ::mica::util::pause();
          }
          lists[row_id] = list;
        } else {
          list = lists[row_id];
        }

        list->append(node);

        ptr += le->size;
      }

      return lf->nentries;
    };
  };  // namespace transaction
};

#endif
