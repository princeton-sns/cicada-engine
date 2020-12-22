#ifndef MICA_TRANSACTION_SCHEDULER_POOL_H_
#define MICA_TRANSACTION_SCHEDULER_POOL_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

template <class StaticConfig, class Class>
SchedulerPool<StaticConfig, Class>::SchedulerPool(Alloc* alloc, uint64_t size,
                                                  size_t lcore)
    : alloc_{alloc} {
  size_t numa_id = ::mica::util::lcore.numa_id(lcore);
  if (numa_id == ::mica::util::lcore.kUnknown) {
    fprintf(stderr, "error: invalid lcore\n");
    return;
  }

  numa_id_ = static_cast<uint8_t>(numa_id);

  uint64_t class_count = (size + class_size - 1) / class_size;
  size_ = class_count * class_size;

  printf("class_size: %lu\n", class_size);
  printf("class_count: %lu\n", class_count);
  printf("size: %lu\n", size_);

  lock_ = 0;
  total_classes_ = class_count;
  free_classes_ = class_count;

  pages_ = reinterpret_cast<char*>(
      alloc_->malloc_contiguous(class_count * class_size, lcore));
  if (!pages_) {
    printf("failed to initialize SchedulerPool\n");
    return;
  }

  next_class_ = reinterpret_cast<Class*>(pages_);
  Class* list = nullptr;
  for (uint64_t i = 0; i < class_count; i++) {
    list = reinterpret_cast<Class*>(pages_ + (i * class_size));

    if ((i + 1) < class_count) {
      list->next = list + 1;
    } else {
      list->next = nullptr;
    }

    if ((i + kAllocSize - 1) < class_count) {
      list->tail = list + (kAllocSize - 1);
    } else {
      list->tail = nullptr;
    }
  }

  printf("initialized SchedulerPool on numa node %" PRIu8 " with %.3lf GB\n",
         numa_id_, static_cast<double>(size) / 1000000000.);
};

template <class StaticConfig, class Class>
SchedulerPool<StaticConfig, Class>::~SchedulerPool() {
  alloc_->free_contiguous(pages_);
};

template <class StaticConfig, class Class>
std::pair<Class*, Class*> SchedulerPool<StaticConfig,Class>::allocate_n() {
  while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

  if (free_classes_ < kAllocSize) {
    printf("Cannot allocate %lu classes\n", kAllocSize);
    __sync_lock_release(&lock_);
    return {nullptr, nullptr};
  }

  auto head = static_cast<Class*>(next_class_);
  auto tail = static_cast<Class*>(head->tail);
  next_class_ = tail->next;

  free_classes_ -= kAllocSize;

  __sync_lock_release(&lock_);

  return std::make_pair(head, tail);
};

template <class StaticConfig, class Class>
void SchedulerPool<StaticConfig,Class>::free_n(Class* head, Class* tail, uint64_t n) {
  while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

  if (n != kAllocSize) {
    throw std::runtime_error("free_n: unexpect n!");
  }

  tail->set_next(next_class_);
  head->set_tail(tail);
  next_class_ = head;

  free_classes_ += kAllocSize;

  __sync_lock_release(&lock_);
};

};  // namespace transaction
};  // namespace mica

#endif
