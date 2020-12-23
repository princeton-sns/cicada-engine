#pragma once
#ifndef MICA_TRANSACTION_MMAPLOGGER_H_
#define MICA_TRANSACTION_MMAPLOGGER_H_

#include "mica/transaction/logging.h"

namespace mica {
namespace transaction {

template <class StaticConfig>
MmapLogger<StaticConfig>::MmapLogger(uint16_t nthreads, std::string logdir)
    : thread_logs_{},
      mmappers_{},
      logdir_{logdir},
      nthreads_{nthreads} {
  for (uint16_t i = 0; i < nthreads_; i++) {
    auto l = new PerThreadLog<StaticConfig>{};
    thread_logs_.push_back(l);
    auto m = new MmapperThread<StaticConfig>{l, logdir_, i};
    m->start();
    mmappers_.push_back(m);

    // Pre-request n log bufs
    std::size_t n = 4;
    for (std::size_t j = 0; j < n; j++) {
      l->request_mlf();
    }

    // Wait for mmapper thread to allocate log bufs
    while (l->num_mlfs() != n) ::mica::util::pause();
  }
}

template <class StaticConfig>
MmapLogger<StaticConfig>::~MmapLogger() {
  flush();

  for (auto m : mmappers_) {
    m->stop();
    delete m;
  }
  mmappers_.clear();

  for (auto l : thread_logs_) {
    delete l;
  }
  thread_logs_.clear();
}

template <class StaticConfig>
void MmapLogger<StaticConfig>::change_logdir(std::string logdir) {
  flush();

  for (auto m : mmappers_) {
    m->stop();
    delete m;
  }
  mmappers_.clear();

  for (auto l : thread_logs_) {
    delete l;
  }
  thread_logs_.clear();

  // Change log dir
  logdir_ = logdir;

  for (uint16_t i = 0; i < nthreads_; i++) {
    auto l = new PerThreadLog<StaticConfig>{};
    thread_logs_.push_back(l);
    auto m = new MmapperThread<StaticConfig>{l, logdir_, i};
    m->start();
    mmappers_.push_back(m);

    // Pre-request n log bufs
    std::size_t n = 4;
    for (std::size_t j = 0; j < n; j++) {
      l->request_mlf();
    }

    // Wait for mmapper thread to allocate log bufs
    while (l->num_mlfs() != n) ::mica::util::pause();
  }
}

template <class StaticConfig>
void MmapLogger<StaticConfig>::flush() {
  for (auto l : thread_logs_) {
    l->flush();
  }
}

template <class StaticConfig>
char* MmapLogger<StaticConfig>::alloc_log_buf(uint16_t thread_id,
                                              std::size_t nbytes) {
  PerThreadLog<StaticConfig>* l = thread_logs_[thread_id];

  // Check we have sufficient space
  if (!l->has_space(nbytes)) {
    if (!l->request_mlf()) {
      // TODO: error handling
    }

    l->advance_file_index();
  }

  return l->get_cur_write_ptr();
}

template <class StaticConfig>
void MmapLogger<StaticConfig>::release_log_buf(uint16_t thread_id,
                                               std::size_t nbytes) {
  PerThreadLog<StaticConfig>* l = thread_logs_[thread_id];
  LogFile<StaticConfig>* lf = l->get_lf();

  l->advance_cur_write_ptr(nbytes);
  lf->nentries += 1;
  lf->size += nbytes;
}

template <class StaticConfig>
bool MmapLogger<StaticConfig>::log(const Context<StaticConfig>* ctx,
                                   const Table<StaticConfig>* tbl) {
  uint16_t thread_id = ctx->thread_id();

  std::size_t nbytes = sizeof(CreateTableLogEntry<StaticConfig>);
  char* buf = alloc_log_buf(thread_id, nbytes);

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

  release_log_buf(thread_id, nbytes);

  return true;
}

template <class StaticConfig>
template <bool UniqueKey>
bool MmapLogger<StaticConfig>::log(
    const Context<StaticConfig>* ctx,
    const HashIndex<StaticConfig, UniqueKey, uint64_t>* idx) {
  uint16_t thread_id = ctx->thread_id();

  std::size_t nbytes = sizeof(CreateHashIndexLogEntry<StaticConfig>);
  char* buf = alloc_log_buf(thread_id, nbytes);

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

  release_log_buf(thread_id, nbytes);

  return true;
}

template <class StaticConfig>
bool MmapLogger<StaticConfig>::log(const Context<StaticConfig>* ctx,
                                   const Transaction<StaticConfig>* tx) {
  uint16_t thread_id = ctx->thread_id();
  auto accesses = tx->accesses();

  std::size_t size = sizeof(BeginTxnLogEntry<StaticConfig>);
  char* buf = alloc_log_buf(thread_id, size);
  BeginTxnLogEntry<StaticConfig>* btle =
      reinterpret_cast<BeginTxnLogEntry<StaticConfig>*>(buf);
  btle->size = size;
  btle->type = LogEntryType::BEGIN_TXN;
  btle->ts = tx->ts().t2;
  btle->nwrites = tx->iset_size() + tx->wset_size();
  release_log_buf(thread_id, size);

  for (auto i = 0; i < tx->access_size(); i++) {
    RowAccessItem<StaticConfig> item = accesses[i];
    RowAccessState state = item.state;
    if (state == RowAccessState::kInvalid || state == RowAccessState::kPeek ||
        state == RowAccessState::kRead) {
      continue;
    }

    RowVersion<StaticConfig>* write_rv = item.write_rv;
    uint32_t data_size = write_rv->data_size;

    size = sizeof(WriteRowLogEntry<StaticConfig>) + data_size;
    buf = alloc_log_buf(thread_id, size);
    WriteRowLogEntry<StaticConfig>* le =
        reinterpret_cast<WriteRowLogEntry<StaticConfig>*>(buf);

    le->size = size;
    le->type = LogEntryType::WRITE_ROW;

    le->table_index = item.tbl->index();
    le->row_id = item.row_id;
    le->cf_id = item.cf_id;

    std::memcpy(&le->rv, write_rv, sizeof(*write_rv) + data_size);

    if (state == RowAccessState::kDelete ||
        state == RowAccessState::kReadDelete) {
      le->rv.status = RowVersionStatus::kDeleted;
    } else {
      le->rv.status = RowVersionStatus::kCommitted;
    }

    // le->print();

    release_log_buf(thread_id, size);
  }

  return true;
}

}  // namespace transaction
};  // namespace mica

#endif
