#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_THREAD_H_

#include "mica/transaction/replication.h"

#include <memory>
#include <thread>

namespace mica {
  namespace transaction {

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

      std::chrono::microseconds time_preprocessing{0};
      std::chrono::microseconds time_waiting{0};
      std::chrono::microseconds time_critical{0};

      pthread_barrier_wait(start_barrier_);

      high_resolution_clock::time_point start = high_resolution_clock::now();
      std::unordered_map<uint64_t, LogEntryList*> local_lists = build_local_lists();
      high_resolution_clock::time_point end = high_resolution_clock::now();
      std::chrono::microseconds diff =
        std::chrono::duration_cast<std::chrono::microseconds>(end - start);
      time_preprocessing += diff;

      start = high_resolution_clock::now();
      while (my_lock_->locked) {
        mica::util::pause();
      }
      my_lock_->locked = true;
      end = high_resolution_clock::now();
      diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
      // printf("Thread %u waited %ld microseconds\n", id, diff.count());
      time_waiting += diff;

      start = high_resolution_clock::now();
      for (const auto& item : local_lists) {
        auto row_id = item.first;
        auto list = item.second;

        queue_->append(row_id, list);
      }

      next_lock_->locked = false;
      end = high_resolution_clock::now();
      diff = std::chrono::duration_cast<std::chrono::microseconds>(end - start);
      // printf("Thread %u waited %ld microseconds\n", id, diff.count());
      time_critical += diff;

      printf("Exiting replica scheduler: %u\n", id_);
      printf("Time preprocessing: %ld microseconds\n", time_preprocessing.count());
      printf("Time critical: %ld microseconds\n", time_critical.count());
      printf("Time waiting: %ld microseconds\n", time_waiting.count());
    };

    template <class StaticConfig>
    std::unordered_map<uint64_t, LogEntryList*> SchedulerThread<StaticConfig>::build_local_lists() {

      std::size_t nsegments = log_->get_nsegments();

      std::unordered_map<uint64_t, LogEntryList*> lists{};

      for (std::size_t cur_segment = id_; cur_segment < nsegments;
           cur_segment += nschedulers_) {
        LogFile<StaticConfig>* lf = log_->get_lf(cur_segment);
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

          LogEntryNode* node = pool_->allocate_node();
          std::memset(node, 0, sizeof *node);
          node->ptr = ptr;

          LogEntryList* list = nullptr;
          auto search = lists.find(row_id);
          if (search == lists.end()) {
            list = pool_->allocate_list();
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
      }

      return lists;
    };
  };  // namespace transaction
};

#endif
