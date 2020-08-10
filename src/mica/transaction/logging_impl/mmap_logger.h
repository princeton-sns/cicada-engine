#pragma once
#ifndef MICA_TRANSACTION_MMAPLOGGER_H_
#define MICA_TRANSACTION_MMAPLOGGER_H_

#include "mica/transaction/logging.h"
#include "mica/util/posix_io.h"

namespace mica {
namespace transaction {

using mica::util::PosixIO;

template <class StaticConfig>
MmapLogger<StaticConfig>::MmapLogger(uint16_t nthreads)
    : nthreads_{nthreads}, len_{StaticConfig::kPageSize}, mappings_{}, bufs_{} {
  for (uint16_t i = 0; i < nthreads; i++) {
    LogBuffer lb = mmap_log_buf(i, 0);
    bufs_.push_back(lb);
  }
}

template <class StaticConfig>
MmapLogger<StaticConfig>::~MmapLogger() {
  flush();

  for (Mmapping m : mappings_) {
    PosixIO::Munmap(m.addr, m.len);
    PosixIO::Close(m.fd);
  }

  mappings_.clear();
}

template <class StaticConfig>
void MmapLogger<StaticConfig>::flush() {
  for (Mmapping m : mappings_) {
    PosixIO::Msync(m.addr, m.len, MS_SYNC);
  }
}

template <class StaticConfig>
typename MmapLogger<StaticConfig>::LogBuffer
MmapLogger<StaticConfig>::mmap_log_buf(uint16_t thread_id,
                                       uint64_t file_index) {
  std::string fname = StaticConfig::kDBLogDir + "/out." +
                      std::to_string(thread_id) + "." +
                      std::to_string(file_index) + ".log";

  int fd = PosixIO::Open(fname.c_str(), O_RDWR | O_CREAT,
                         S_IRUSR | S_IWUSR | S_IRGRP | S_IROTH);

  PosixIO::Ftruncate(fd, static_cast<off_t>(len_));

  char* start = static_cast<char*>(
      PosixIO::Mmap(nullptr, len_, PROT_READ | PROT_WRITE,
                    MAP_SHARED | MAP_POPULATE, fd, 0));

  Mmapping m{start, len_, fd};
  mappings_.push_back(m);

  LogFile<StaticConfig>* lf = reinterpret_cast<LogFile<StaticConfig>*>(start);
  lf->nentries = 0;

  LogBuffer lb{start, start + len_, reinterpret_cast<char*>(&lf->entries[0]),
               file_index};
  return lb;
}

template <class StaticConfig>
char* MmapLogger<StaticConfig>::alloc_log_buf(uint16_t thread_id,
                                              std::size_t nbytes) {
  LogBuffer lb = bufs_[thread_id];

  // lb.print();

  // Check we have sufficient space
  if (lb.cur + nbytes > lb.end) {
    PosixIO::Msync(lb.start, static_cast<std::size_t>(lb.end - lb.start),
                   MS_ASYNC);
    LogBuffer lbnew = mmap_log_buf(thread_id, lb.cur_file_index + 1);
    lb = lbnew;
  }

  char* p = lb.cur;
  lb.cur += nbytes;

  // Write back changes
  bufs_[thread_id] = lb;

  return p;
}

template <class StaticConfig>
void MmapLogger<StaticConfig>::release_log_buf(uint16_t thread_id) {
  LogBuffer lb = bufs_[thread_id];
  LogFile<StaticConfig>* lf =
      reinterpret_cast<LogFile<StaticConfig>*>(lb.start);
  lf->nentries += 1;
}

template <class StaticConfig>
bool MmapLogger<StaticConfig>::log(const Context<StaticConfig>* ctx,
                                   const Table<StaticConfig>* tbl) {
  uint16_t thread_id = ctx->thread_id();

  char* buf =
      alloc_log_buf(thread_id, sizeof(CreateTableLogEntry<StaticConfig>));

  CreateTableLogEntry<StaticConfig>* le =
      reinterpret_cast<CreateTableLogEntry<StaticConfig>*>(buf);

  le->size = sizeof *le;
  le->type = LogEntryType::CREATE_TABLE;
  le->cf_count = tbl->cf_count();

  std::memcpy(&le->name[0], tbl->name().c_str(), 1 + tbl->name().size());
  for (uint16_t cf_id = 0; cf_id < le->cf_count; cf_id++) {
    le->data_size_hints[cf_id] = tbl->data_size_hint(cf_id);
  }

  // le->print();

  release_log_buf(thread_id);

  return true;
}

template <class StaticConfig>
template <bool UniqueKey>
bool MmapLogger<StaticConfig>::log(
    const Context<StaticConfig>* ctx,
    const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx) {
  uint16_t thread_id = ctx->thread_id();

  char* buf =
      alloc_log_buf(thread_id, sizeof(CreateHashIndexLogEntry<StaticConfig>));

  CreateHashIndexLogEntry<StaticConfig>* le =
      reinterpret_cast<CreateHashIndexLogEntry<StaticConfig>*>(buf);

  le->size = sizeof *le;
  le->type = LogEntryType::CREATE_HASH_IDX;
  le->expected_num_rows = idx->expected_num_rows();
  le->unique_key = UniqueKey;

  std::memcpy(&le->name[0], idx->index_table()->name().c_str(),
              1 + idx->index_table()->name().size());

  std::memcpy(&le->main_tbl_name[0], idx->main_table()->name().c_str(),
              1 + idx->main_table()->name().size());

  // le->print();

  release_log_buf(thread_id);

  return true;
}

template <class StaticConfig>
bool MmapLogger<StaticConfig>::log(const Context<StaticConfig>* ctx,
                                   const Transaction<StaticConfig>* tx) {
  uint16_t thread_id = ctx->thread_id();

  auto accesses = tx->accesses();
  auto iset_idx = tx->iset_idx();
  auto wset_idx = tx->wset_idx();

  for (auto j = 0; j < tx->iset_size(); j++) {
    int i = iset_idx[j];
    RowAccessItem<StaticConfig> item = accesses[i];
    RowVersion<StaticConfig>* write_rv = item.write_rv;
    uint32_t data_size = write_rv->data_size;
    char* data = write_rv->data;
    Table<StaticConfig>* tbl = item.tbl;

    char* buf = alloc_log_buf(
        thread_id, sizeof(InsertRowLogEntry<StaticConfig>) + data_size);
    InsertRowLogEntry<StaticConfig>* le =
        reinterpret_cast<InsertRowLogEntry<StaticConfig>*>(buf);

    std::size_t size = sizeof *le + data_size;

    le->size = size;
    le->type = LogEntryType::INSERT_ROW;

    le->txn_ts = tx->ts().t2;
    le->cf_id = item.cf_id;
    le->row_id = item.row_id;

    le->wts = write_rv->wts.t2;
    le->rts = write_rv->rts.get().t2;

    le->data_size = data_size;
    le->tbl_type = static_cast<uint8_t>(tbl->type());

    std::memcpy(&le->tbl_name[0], tbl->name().c_str(), 1 + tbl->name().size());
    std::memcpy(le->data, data, data_size);

    // le->print();

    release_log_buf(thread_id);
  }

  for (auto j = 0; j < tx->wset_size(); j++) {
    int i = wset_idx[j];
    RowAccessItem<StaticConfig> item = accesses[i];
    RowVersion<StaticConfig>* write_rv = item.write_rv;
    uint32_t data_size = write_rv->data_size;
    char* data = write_rv->data;
    Table<StaticConfig>* tbl = item.tbl;

    char* buf = alloc_log_buf(
        thread_id, sizeof(WriteRowLogEntry<StaticConfig>) + data_size);
    WriteRowLogEntry<StaticConfig>* le =
        reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(buf);

    std::size_t size = sizeof *le + data_size;
    le->size = size;
    le->type = LogEntryType::WRITE_ROW;

    le->txn_ts = tx->ts().t2;
    le->cf_id = item.cf_id;
    le->row_id = item.row_id;

    le->wts = write_rv->wts.t2;
    le->rts = write_rv->rts.get().t2;

    le->data_size = data_size;
    le->tbl_type = static_cast<uint8_t>(tbl->type());

    std::memcpy(&le->tbl_name[0], tbl->name().c_str(), 1 + tbl->name().size());
    std::memcpy(le->data, data, data_size);

    // le->print();

    release_log_buf(thread_id);
  }

  return true;
}

}  // namespace transaction
};  // namespace mica

#endif