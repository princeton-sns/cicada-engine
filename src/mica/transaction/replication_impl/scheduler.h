#ifndef MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_H_
#define MICA_TRANSACTION_REPLICATION_IMPL_SCHEDULER_H_

#include "mica/transaction/replication.h"

#include <memory>
#include <thread>

namespace mica {
  namespace transaction {

  template <class StaticConfig>
  SchedulerThread<StaticConfig>::SchedulerThread(
      std::shared_ptr<MmappedLogFile<StaticConfig>> log,
      pthread_barrier_t* start_barrier, uint16_t id, SchedulerLock* my_lock,
      SchedulerLock* next_lock)
      : log_{log},
        start_barrier_{start_barrier},
        id_{id},
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

      // std::unordered_map<uint64_t, LogEntryRef*> local_fifos{};
      // std::unordered_map<uint64_t, LogEntryRef*> local_fifo_tails{};

      std::size_t nsegments = log_->get_nsegments();

      if (id_ == 0) {
        my_lock_->locked = false;
      } else {
        my_lock_->locked = true;
      }

      std::chrono::microseconds time_waiting{0};

      pthread_barrier_wait(start_barrier_);


      printf("Exiting replica scheduler: %u\n", id_);
      printf("Time waiting: %ld microseconds\n", time_waiting.count());
    };

};  // namespace transaction
};

#endif
