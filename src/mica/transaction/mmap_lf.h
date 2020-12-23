#pragma once
#ifndef MICA_TRANSACTION_MMAP_LF_H_
#define MICA_TRANSACTION_MMAP_LF_H_

#include <stdint.h>

#include <memory>
#include <string>

#include "mica/transaction/log_format.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {

using mica::util::PosixIO;

template <class StaticConfig>
class MmappedLogFile {
 public:
  static std::shared_ptr<MmappedLogFile<StaticConfig>> open_new(
      std::string fname, std::size_t len, int prot, int flags,
      uint16_t nsegments = 1) {
    int fd = PosixIO::Open(fname.c_str(), O_RDWR | O_CREAT,
                           S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

    PosixIO::Ftruncate(fd, static_cast<off_t>(len));
    char* start =
        static_cast<char*>(PosixIO::Mmap(nullptr, len, prot, flags, fd, 0));

    std::vector<LogFile<StaticConfig>*> lfs{};
    std::size_t segment_len = len / nsegments;
    for (int s = 0; s < nsegments; s++) {
      if (s == nsegments - 1) {
        segment_len = len - (nsegments - 1) * segment_len;
      }

      LogFile<StaticConfig>* lf =
          reinterpret_cast<LogFile<StaticConfig>*>(start);

      lf->nentries = 0;
      lf->size = sizeof(LogFile<StaticConfig>);

      lfs.push_back(lf);
      start += segment_len;
    }

    auto mlf = new MmappedLogFile<StaticConfig>{fname, len, fd, lfs};

    return std::shared_ptr<MmappedLogFile<StaticConfig>>{mlf};
  }

  static std::shared_ptr<MmappedLogFile<StaticConfig>> open_existing(
      std::string fname, int prot, int flags, uint16_t nsegments = 1) {
    if (PosixIO::Exists(fname.c_str())) {
      int fd = PosixIO::Open(fname.c_str(), O_RDONLY);
      std::size_t len = PosixIO::Size(fname.c_str());
      char* start =
          static_cast<char*>(PosixIO::Mmap(nullptr, len, prot, flags, fd, 0));

      std::vector<LogFile<StaticConfig>*> lfs{};
      std::size_t segment_len = len / nsegments;
      for (int s = 0; s < nsegments; s++) {
        if (s == nsegments - 1) {
          segment_len = len - (nsegments - 1) * segment_len;
        }

        LogFile<StaticConfig>* lf =
            reinterpret_cast<LogFile<StaticConfig>*>(start);

        lfs.push_back(lf);
        start += segment_len;
      }

      auto mlf = new MmappedLogFile<StaticConfig>{fname, len, fd, lfs};

      return std::shared_ptr<MmappedLogFile<StaticConfig>>{mlf};
    } else {
      return std::shared_ptr<MmappedLogFile<StaticConfig>>{nullptr};
    }
  }

  ~MmappedLogFile() {
    PosixIO::Munmap(get_lf(0), len_);
    PosixIO::Close(fd_);
  }

  void flush() { PosixIO::Msync(get_lf(0), len_, MS_SYNC); }

  char* get_cur_write_ptr() {
    return cur_write_ptr_;
  }

  void advance_cur_write_ptr(std::size_t nbytes) {
    cur_write_ptr_ += nbytes;
  }

  std::size_t get_nsegments() { return lfs_.size(); }

  LogFile<StaticConfig>* get_lf(std::size_t segment = 0) {
    return reinterpret_cast<LogFile<StaticConfig>*>(lfs_[segment]);
  }

  LogEntry<StaticConfig>* get_cur_le() {
    return reinterpret_cast<LogEntry<StaticConfig>*>(cur_read_ptr_);
  }

  bool has_next_le(std::size_t segment) {
    return cur_read_ptr_ <
           (reinterpret_cast<char*>(get_lf(segment)) + get_size(segment));
  }

  bool has_next_le() {
    return has_next_le(cur_segment_) || (cur_segment_ < get_nsegments() - 1 &&
                                         get_size(cur_segment_ + 1) != 0);
  }

  void read_next_le() {
    auto le = reinterpret_cast<LogEntry<StaticConfig>*>(cur_read_ptr_);
    if (has_next_le(cur_segment_)) {
      cur_read_ptr_ += le->size;
    } else if (has_next_le()) {
      cur_segment_ += 1;
      cur_read_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
    } else {
      throw std::runtime_error("read_next_le: nothing more to read!");
    }
  }

  bool has_space_next_le(std::size_t n) {
    char* end = reinterpret_cast<char*>(get_lf(0)) + len_;
    if (cur_segment_ < get_nsegments() - 1) {
      end = reinterpret_cast<char*>(get_lf(cur_segment_ + 1));
    }

    return cur_write_ptr_ + n < end;
  }

  void write_next_le(void* src, std::size_t n) {
    if (!has_space_next_le(n)) {
      cur_segment_ += 1;
      cur_write_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
    }

    std::memcpy(cur_write_ptr_, src, n);
    cur_write_ptr_ += n;
    auto lf = get_lf(cur_segment_);
    lf->nentries += 1;
    lf->size += n;
  }

  std::size_t get_size(std::size_t segment = 0) {
    return get_lf(segment)->size;
  }

  uint64_t get_nentries(std::size_t segment = 0) {
    return get_lf(segment)->nentries;
  }

 private:
  std::string fname_;
  std::size_t len_;
  int fd_;
  char* cur_read_ptr_;
  char* cur_write_ptr_;
  std::vector<LogFile<StaticConfig>*> lfs_;
  std::size_t cur_segment_;

  MmappedLogFile(std::string fname, std::size_t len, int fd,
                 std::vector<LogFile<StaticConfig>*> lfs)
      : fname_{fname}, len_{len}, fd_{fd}, lfs_{lfs}, cur_segment_{0} {
    cur_read_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
    cur_write_ptr_ = reinterpret_cast<char*>(&lfs_[cur_segment_]->entries[0]);
  }
};

};  // namespace transaction
};  // namespace mica

#endif
