#ifndef MICA_TRANSACTION_SCHED_QUEUE_CC_
#define MICA_TRANSACTION_SCHED_QUEUE_CC_

#include "mica/transaction/replication.h"

namespace mica {
  namespace transaction {

    const uint32_t STATUS_HEAD = (1 << 0);
    const uint32_t STATUS_EXECUTED = (1 << 1);
    const uint32_t STATUS_REMOVED = (1 << 2);

    template <class StaticConfig>
    SchedulerQueue<StaticConfig>::SchedulerQueue(SchedulerPool<StaticConfig>* pool)
      : heads_{}, head_{}, tail_{&head_}, pool_{pool}
    {
      head_.next = nullptr;
      head_.list = nullptr;
      head_.tail = nullptr;
      head_.status = STATUS_HEAD;
    }

    template <class StaticConfig>
    SchedulerQueue<StaticConfig>::~SchedulerQueue() {
      // printf("destroying scheduler queue!\n");
      LogEntryList* next = head_.next;
      while (next != nullptr) {
        // printf("deallocating list at %p\n", next);
        LogEntryList* temp = next->next;
        deallocate_list(next);
        next = temp;
      }
      head_.next = nullptr;
    }

    template <class StaticConfig>
    void SchedulerQueue<StaticConfig>::print() {
      LogEntryList* next = &head_;
      while (next != nullptr) {
        next->print();
        next = next->next;
      }
    }

    template <class StaticConfig>
    void SchedulerQueue<StaticConfig>::deallocate_list(LogEntryList* list) {
      LogEntryNode* next = list->list;
      while (next != nullptr) {
        LogEntryNode* temp = next->next;
        // printf("freeing node at %p\n", next);
        pool_->free_node(next);
        next = temp;
      }

      list->list = nullptr;
      pool_->free_list(list);
    }

    template <class StaticConfig>
    LogEntryList* SchedulerQueue<StaticConfig>::append(uint64_t row_id, LogEntryList* list) {
      // row_id = 0;
      LogEntryList* to_deallocate = nullptr;
      bool new_list = true;

      auto search = heads_.find(row_id);
      if (search != heads_.end()) {  // Found
        LogEntryList* head = heads_[row_id];
        // printf("Found list for row_id: %lu\n", row_id);
        // printf("head: %p\n", head);
        // printf("head->status: %lu\n", head->status);

        while (__sync_lock_test_and_set(&head->lock, 1) == 1) {
          ::mica::util::pause();
        }

        if (!(head->status & STATUS_EXECUTED)) {
          // printf("appending list->list to head\n");
          // printf("list->list: %p\n", list->list);
          // printf("list->tail: %p\n", list->tail);
          head->append(list->list, list->tail);
          list->list = nullptr;
          new_list = false;
        }

        head->lock = 0;

        if (head->status & STATUS_REMOVED) {
          to_deallocate = head;
        }
      // } else {
        // printf("Not found list for row_id: %lu\n", row_id);
      }

      if (new_list) { // Append list to list of lists
        list->next = nullptr;
        list->status = 0;
        list->lock = 0;

        tail_->next = list;
        tail_ = list;
        heads_[row_id] = list;
      } else {
        list->lock = 0;
        to_deallocate = list;
      }

      return to_deallocate;
    }
  };  // namespace transaction
};  // namespace mica

#endif
