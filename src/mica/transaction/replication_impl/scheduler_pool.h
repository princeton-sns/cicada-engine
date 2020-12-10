#ifndef MICA_TRANSACTION_SCHEDULER_POOL_H_
#define MICA_TRANSACTION_SCHEDULER_POOL_H_

#include "mica/transaction/replication.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
SchedulerPool<StaticConfig>::SchedulerPool(Alloc* alloc, uint64_t size,
                                           size_t lcore)
    : alloc_{alloc} {
  size_t numa_id = ::mica::util::lcore.numa_id(lcore);
  if (numa_id == ::mica::util::lcore.kUnknown) {
    fprintf(stderr, "error: invalid lcore\n");
    return;
  }

  numa_id_ = static_cast<uint8_t>(numa_id);

  uint64_t list_count = (size + list_size - 1) / list_size;
  size_ = list_count * list_size;

  printf("list_size: %lu\n", list_size);
  printf("list_count: %lu\n", list_count);
  printf("size: %lu\n", size_);

  lock_ = 0;
  total_lists_ = list_count;
  free_lists_ = list_count;

  list_pages_ = reinterpret_cast<char*>(
      alloc_->malloc_contiguous(list_count * list_size, lcore));
  if (!list_pages_) {
    printf("failed to initialize SchedulerPool\n");
    return;
  }

  next_list_ = reinterpret_cast<LogEntryList<StaticConfig>*>(list_pages_);
  LogEntryList<StaticConfig>* list = nullptr;
  for (uint64_t i = 0; i < list_count; i++) {
    list = reinterpret_cast<LogEntryList<StaticConfig>*>(list_pages_ + (i * list_size));

    if ((i + 1) < list_count) {
      list->next = list + 1;
    } else {
      list->next = nullptr;
    }

    if ((i + kAllocSize - 1) < list_count) {
      list->tail = list + (kAllocSize - 1);
    } else {
      list->tail = nullptr;
    }
  }

  printf("initialized SchedulerPool on numa node %" PRIu8 " with %.3lf GB\n",
         numa_id_, static_cast<double>(size) / 1000000000.);
};

template <class StaticConfig>
SchedulerPool<StaticConfig>::~SchedulerPool() {
  alloc_->free_contiguous(list_pages_);
};

template <class StaticConfig>
std::pair<LogEntryList<StaticConfig>*, LogEntryList<StaticConfig>*>
SchedulerPool<StaticConfig>::allocate_lists() {
  while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

  if (free_lists_ < kAllocSize) {
    printf("Cannot allocate %lu lists\n", kAllocSize);
    __sync_lock_release(&lock_);
    return {nullptr, nullptr};
  }

  auto head = next_list_;
  auto tail = head->tail;
  next_list_ = tail->next;

  free_lists_ -= kAllocSize;

  __sync_lock_release(&lock_);

  return {head, tail};
};

template <class StaticConfig>
void SchedulerPool<StaticConfig>::free_lists(LogEntryList<StaticConfig>* head,
                                             LogEntryList<StaticConfig>* tail, uint64_t n) {
  while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

  if (n != kAllocSize) {
    throw std::runtime_error("free_lists: unexpect n!");
  }

  tail->next = next_list_;
  head->tail = tail;
  next_list_ = head;

  free_lists_ += kAllocSize;

  __sync_lock_release(&lock_);
};

};  // namespace transaction
};  // namespace mica

#endif
