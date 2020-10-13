#ifndef MICA_TRANSACTION_SCHEDULER_POOL_H_
#define MICA_TRANSACTION_SCHEDULER_POOL_H_

#include "mica/transaction/replication.h"

namespace mica {
  namespace transaction {

    template <class StaticConfig>
    SchedulerPool<StaticConfig>::SchedulerPool(Alloc* alloc, uint64_t size, size_t lcore)
      : alloc_{alloc} {

      size_t numa_id = ::mica::util::lcore.numa_id(lcore);
      if (numa_id == ::mica::util::lcore.kUnknown) {
        fprintf(stderr, "error: invalid lcore\n");
        return;
      }

      numa_id_ = static_cast<uint8_t>(numa_id);

      // uint64_t node_count = (size/2 + node_size - 1) / node_size;
      uint64_t node_count = 0;
      uint64_t list_count = (size + list_size - 1) / list_size;
      size_ = (node_count * node_size) + (list_count * list_size);

      printf("node_size: %lu\n", node_size);
      printf("node_count: %lu\n", node_count);
      printf("list_size: %lu\n", list_size);
      printf("list_count: %lu\n", list_count);
      printf("size: %lu\n", size_);

      lock_ = 0;
      total_nodes_ = node_count;
      free_nodes_ = node_count;
      total_lists_ = list_count;
      free_lists_ = list_count;

      // node_pages_ = reinterpret_cast<char*>(alloc_->malloc_contiguous(node_count * node_size, lcore));
      // if (!node_pages_) {
      //   printf("failed to initialize SchedulerPool\n");
      //   return;
      // }
      node_pages_ = nullptr;

      list_pages_ = reinterpret_cast<char*>(alloc_->malloc_contiguous(list_count * list_size, lcore));
      if (!list_pages_) {
        printf("failed to initialize SchedulerPool\n");
        return;
      }

      LogEntryNode* node = reinterpret_cast<LogEntryNode*>(node_pages_);
      next_node_ = node;
      // for (uint64_t i = 0; i < node_count - 1; i++) {
      //   node->next = reinterpret_cast<LogEntryNode*>(node_pages_ + (i + 1) * node_size);
      //   node = node->next;
      // }
      // node->next = nullptr;

      // Use list->next instead of list->next ptr to avoid concurrency bugs
      LogEntryList<StaticConfig>* list = reinterpret_cast<LogEntryList<StaticConfig>*>(list_pages_);
      next_list_ = list;
      for (uint64_t i = 0; i < list_count - 1; i++) {
        list->next = reinterpret_cast<LogEntryList<StaticConfig>*>(list_pages_ + (i + 1) * list_size);
        list = list->next;
      }
      list->next = nullptr;

      printf("initialized SchedulerPool on numa node %" PRIu8 " with %.3lf GB\n",
             numa_id_, static_cast<double>(size) / 1000000000.);
    };

    template <class StaticConfig>
    SchedulerPool<StaticConfig>::~SchedulerPool() {
      alloc_->free_contiguous(node_pages_);
      alloc_->free_contiguous(list_pages_);
    };

    template <class StaticConfig>
    LogEntryList<StaticConfig>* SchedulerPool<StaticConfig>::allocate_list(uint64_t n) {
      while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

      LogEntryList<StaticConfig>* start = next_list_;
      LogEntryList<StaticConfig>* end = next_list_;

      for (uint64_t i = 1; i < n && end != nullptr; i++) {
        end = reinterpret_cast<LogEntryList<StaticConfig>*>(end->next);
        free_lists_--;
      }

      if (end != nullptr) {
        next_list_ = reinterpret_cast<LogEntryList<StaticConfig>*>(end->next);
        end->next = nullptr;
        free_lists_--;
      }

      __sync_lock_release(&lock_);

      return start;
    };

    template <class StaticConfig>
    void SchedulerPool<StaticConfig>::free_list(LogEntryList<StaticConfig>* p) {
      while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

      p->next = next_list_;
      next_list_ = p;
      free_lists_++;

      __sync_lock_release(&lock_);
    };

    template <class StaticConfig>
    LogEntryNode* SchedulerPool<StaticConfig>::allocate_node(uint64_t n) {
      while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

      LogEntryNode* start = next_node_;
      LogEntryNode* end = next_node_;
      for (uint64_t i = 1; i < n && end != nullptr; i++) {
        end = end->next;
        free_nodes_--;
      }

      if (end != nullptr) {
        next_node_ = end->next;
        end->next = nullptr;
        free_nodes_--;
      }

      __sync_lock_release(&lock_);

      return start;
    };

    template <class StaticConfig>
    void SchedulerPool<StaticConfig>::free_node(LogEntryNode* p) {
      while (__sync_lock_test_and_set(&lock_, 1) == 1) ::mica::util::pause();

      p->next = next_node_;
      next_node_ = p;
      free_nodes_++;

      __sync_lock_release(&lock_);
    };
  };  // namespace transaction
};

#endif
