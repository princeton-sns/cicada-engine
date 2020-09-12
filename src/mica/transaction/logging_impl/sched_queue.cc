#ifndef MICA_TRANSACTION_SCHED_QUEUE_CC_
#define MICA_TRANSACTION_SCHED_QUEUE_CC_

#include "mica/transaction/sched_queue.h"

namespace mica {
namespace transaction {

const uint8_t FLAG_LOCKED = (1 < 0);
const uint8_t FLAG_EXECUTED = (1 < 1);
const uint8_t FLAG_REMOVED = (1 < 2);
const uint8_t FLAG_DELETED = (1 < 3);

SchedulerQueue::SchedulerQueue()
    : heads_{}, tails_{}, head_{nullptr, nullptr}, tail_{&head_} {}

void SchedulerQueue::Print() {
  FIFOHead* next = &head_;
  while (next != nullptr) {
    next->print();
    next = next->get_next();
  }
}

void SchedulerQueue::Append(uint64_t row_id, LogEntryRef* head,
                            LogEntryRef* tail) {
  auto search = heads_.find(row_id);
  if (search != heads_.end()) {  // Not found
    FIFOHead* h = heads_[row_id];
    LogEntryRef* t = tails_[row_id];

    while (true) {
      StatusOrUint64 status = h->status;
      if (status.status.flags & FLAG_EXECUTED) {
        break; // Create new h
      } else if (status.status.flags & FLAG_REMOVED) {

        auto cur = h->get_fifo();
        while (cur != nullptr) {
          auto next = cur->get_next();
          delete cur;
          cur = next;
        }

        delete h;
        break; // Create new h
      } else if (status.status.flags & FLAG_DELETED) {
        throw std::runtime_error("Append encountered DELETED head!");
      } else {
        // Append to existing queue
        StatusOrUint64 new_status = status;
        new_status.status.nappends += 1;
        if (__sync_bool_compare_and_swap(&h->status.uint, status.uint, new_status.uint)) {
          t->set_next(head);
          tails_[row_id] = tail;
          return;
        }
      }
    }
  }

  // Create new h
  FIFOHead* h = new FIFOHead{nullptr, head};

  tail_->set_next(h);
  tail_ = h;

  heads_[row_id] = h;
  tails_[row_id] = tail;
}

LogEntryRef* Pop() { return nullptr; }

void PopFinish(LogEntryRef* ler) { (void)ler; }
};  // namespace transaction
};  // namespace mica

#endif
