#ifndef MICA_TRANSACTION_SCHED_POOL_H_
#define MICA_TRANSACTION_SCHED_POOL_H_

#include <cstdio>

#include "mica/transaction/sched_queue.h"
#include "mica/util/lcore.h"

namespace mica {
namespace transaction {
template <class StaticConfig>
class SchedulerPool {
 public:
  typedef typename StaticConfig::Alloc Alloc;

  static constexpr uint64_t kLERSize = sizeof(LogEntryRef);

  SchedulerPool(Alloc* alloc, uint64_t size, size_t lcore)
      : alloc_(alloc) {

    size_t numa_id = ::mica::util::lcore.numa_id(lcore);
    if (numa_id == ::mica::util::lcore.kUnknown) {
      fprintf(stderr, "error: invalid lcore\n");
      return;
    }

    numa_id_ = static_cast<uint8_t>(numa_id);

    uint64_t ler_count = (size + kLERSize - 1) / kLERSize;
    size_ = ler_count * kLERSize;

    lock_ = 0;
    total_count_ = ler_count;
    free_count_ = ler_count;

    pages_ =
        reinterpret_cast<char*>(alloc_->malloc_contiguous(size_, lcore));
    if (!pages_) {
      printf("failed to initialize SchedulerPool\n");
      return;
    }

    LogEntryRef* ler = reinterpret_cast<LogEntryRef*>(pages_);
    next_ = ler;
    for (uint64_t i = 0; i < ler_count - 1; i++) {
      ler->set_next(reinterpret_cast<LogEntryRef*>(pages_ + (i + 1) * kLERSize));
      ler = ler->get_next();
    }
    ler->set_next(nullptr);

    printf("initialized SchedulerPool on numa node %" PRIu8 " with %.3lf GB\n",
           numa_id_, static_cast<double>(size) / 1000000000.);
  }

  ~SchedulerPool() { alloc_->free_striped(pages_); }

  LogEntryRef* allocate() {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

    auto p = next_;
    if (next_) {
      next_ = p->get_next();
      free_count_--;
    }

    __sync_lock_release(&lock_);

    return new (p) LogEntryRef{nullptr, nullptr};
  }

  void free(LogEntryRef* p) {
    while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

    p->set_next(next_);
    next_ = p;
    free_count_++;

    __sync_lock_release(&lock_);
  }

  uint8_t numa_id() const { return numa_id_; }

  uint64_t total_count() const { return total_count_; }
  uint64_t free_count() const { return free_count_; }

  void print_status() const {
    printf("SchedulerPool on numa node %" PRIu8 "\n", numa_id_);
    printf("  in use: %7.3lf GB\n",
           static_cast<double>((total_count_ - free_count_) * kLERSize) /
               1000000000.);
    printf("  free:   %7.3lf GB\n",
           static_cast<double>(free_count_ * kLERSize) / 1000000000.);
    printf("  total:  %7.3lf GB\n",
           static_cast<double>(total_count_ * kLERSize) / 1000000000.);
  }

 private:
  Alloc* alloc_;
  uint64_t size_;
  uint8_t numa_id_;

  uint64_t total_count_;
  char* pages_;

  volatile uint32_t lock_;
  uint64_t free_count_;
  LogEntryRef* next_;
} __attribute__((aligned(64)));
}
}

#endif
