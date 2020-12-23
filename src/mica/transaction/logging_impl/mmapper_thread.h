#pragma once
#ifndef MICA_TRANSACTION_MMAPPER_THREAD_H_
#define MICA_TRANSACTION_MMAPPER_THREAD_H_

#include "mica/transaction/logging.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {

using mica::util::PosixIO;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;
using std::chrono::nanoseconds;

template <class StaticConfig>
MmapperThread<StaticConfig>::MmapperThread(
    PerThreadLog<StaticConfig>* thread_log, std::string logdir, uint16_t id)
    : logdir_{logdir}, thread_log_{thread_log}, id_{id}, stop_{false} {}

template <class StaticConfig>
MmapperThread<StaticConfig>::~MmapperThread() {}

template <class StaticConfig>
void MmapperThread<StaticConfig>::start() {
  stop_ = false;
  thread_ = std::thread{&MmapperThread<StaticConfig>::run, this};
}

template <class StaticConfig>
void MmapperThread<StaticConfig>::stop() {
  stop_ = true;
  thread_.join();
}

template <class StaticConfig>
void MmapperThread<StaticConfig>::run() {
  printf("Starting mmapper thread: %u\n", id_);

  uint16_t thread_id = id_;
  uint64_t file_index = 0;

  moodycamel::BlockingReaderWriterQueue<uint16_t>* queue =
      thread_log_->get_request_queue();

  high_resolution_clock::time_point run_start;
  high_resolution_clock::time_point run_end;
  nanoseconds time_total{0};

  milliseconds timeout{5};

  run_start = high_resolution_clock::now();

  while (true) {
    uint16_t nreqs;
    if (queue->wait_dequeue_timed(nreqs, timeout)) {
      std::string fname = logdir_ + "/out." + std::to_string(thread_id) + "." +
                          std::to_string(file_index++) + ".log";

      std::shared_ptr<MmappedLogFile<StaticConfig>> mlf =
          MmappedLogFile<StaticConfig>::open_new(fname, StaticConfig::kLogFileSize,
                                                 PROT_READ | PROT_WRITE,
                                                 MAP_SHARED | MAP_POPULATE, 1);

      thread_log_->add_mlf(mlf);

    } else if (stop_) {
      break;
    }
  }

  run_end = high_resolution_clock::now();
  time_total += duration_cast<nanoseconds>(run_end - run_start);

  printf("Exiting mmapper thread: %u\n", id_);
  printf("Time total: %ld nanoseconds\n", time_total.count());
}

}  // namespace transaction
};  // namespace mica

#endif
