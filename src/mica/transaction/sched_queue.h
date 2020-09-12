#pragma once
#ifndef MICA_TRANSACTION_SCHED_QUEUE_H_
#define MICA_TRANSACTION_SCHED_QUEUE_H_

#include <bitset>
#include <iostream>
#include <sstream>
#include <string>
#include <unordered_map>

namespace mica {
namespace transaction {

class LogEntryRef {
 public:
  LogEntryRef(LogEntryRef* next, char* ptr) : next_{next}, ptr_{ptr} {}

  LogEntryRef* get_next() { return next_; }
  void set_next(LogEntryRef* next) { next_ = next; }

  char* get_ptr() { return ptr_; }
  void set_ptr(char* ptr) { ptr_ = ptr; }

  void print() {
    std::stringstream stream;

    stream << std::endl;
    stream << "LogEntryRef:" << std::endl;
    stream << "next: " << next_ << std::endl;
    stream << "ptr: " << ptr_ << std::endl;

    std::cout << stream.str();
  }

 private:
  LogEntryRef* next_;
  char* ptr_;
} __attribute__((aligned(64)));

struct Status {
  uint32_t nappends;
  uint32_t flags;
};

union StatusOrUint64 {
  Status status;
  uint64_t uint;
};

class FIFOHead {
 public:
  StatusOrUint64 status;

  FIFOHead(FIFOHead* next, LogEntryRef* fifo)
    : status{{0,0}}, next_{next}, fifo_{fifo} {}

  FIFOHead* get_next() { return next_; }
  void set_next(FIFOHead* next) { next_ = next; }

  LogEntryRef* get_fifo() { return fifo_; }

  void print() {
    std::stringstream stream;

    stream << std::endl;
    stream << "FIFOHead:" << std::endl;
    stream << "nappends: " << status.status.nappends << std::endl;
    stream << "flags: " << std::bitset<32>(status.status.flags) << std::endl;
    stream << "next: " << next_ << std::endl;
    stream << "FIFO:" << std::endl;

    auto next = fifo_;
    int i = 0;
    while (next != nullptr) {
      stream << "[" << i << "]: " << static_cast<void*>(next->get_ptr()) << std::endl;
      next = next->get_next();
      i++;
    }

    std::cout << stream.str();
  }

 private:
  FIFOHead* next_;
  LogEntryRef* fifo_;
};

class SchedulerQueue {
 public:
  SchedulerQueue();

  void Append(uint64_t row_id, LogEntryRef* head, LogEntryRef* tail);

  LogEntryRef* Pop();
  void PopFinish(LogEntryRef* ler);

  void Print();

 private:
  std::unordered_map<uint64_t, FIFOHead*> heads_;
  std::unordered_map<uint64_t, LogEntryRef*> tails_;

  FIFOHead head_;
  FIFOHead* tail_;
};

}  // namespace transaction
};  // namespace mica

#endif
