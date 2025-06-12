/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "oblsm/ob_lsm_impl.h"

#include "common/log/log.h"
#include "common/sys/rc.h"
#include "oblsm/include/ob_lsm.h"
#include "oblsm/ob_manifest.h"
#include "oblsm/ob_lsm_define.h"
#include "oblsm/wal/ob_lsm_wal.h"
#include "oblsm/table/ob_merger.h"
#include "oblsm/table/ob_sstable.h"
#include "oblsm/table/ob_sstable_builder.h"
#include "oblsm/util/ob_coding.h"
#include "oblsm/compaction/ob_compaction_picker.h"
#include "oblsm/ob_user_iterator.h"
#include "oblsm/compaction/ob_compaction.h"
#include "oblsm/ob_lsm_define.h"
#include "oblsm/table/ob_merger.h"
#include <cstdio>

namespace oceanbase {
ObLsmImpl::ObLsmImpl(const ObLsmOptions &options, const string &path)
    : options_(options), path_(path), mu_(), mem_table_(nullptr), imem_tables_(), manifest_(path)
{
    printf("ObLsmImpl::ObLsmImpl, path=%s\n", path.c_str());
    std::filesystem::create_directory(path_);
    mem_table_ = make_shared<ObMemTable>();
    wal_ = make_shared<WAL>();
    uint64_t initial_memtable_id = memtable_id_.fetch_add(1); // 原子地获取一个唯一的ID

    RC rc = wal_->open(get_wal_path(initial_memtable_id));    // 使用辅助函数获取路径并打开文件
    if (rc != RC::SUCCESS) {
        // 这是一个致命错误，数据库无法在没有WAL的情况下工作。
        LOG_ERROR("FATAL: Failed to open initial WAL file, rc=%d. Database cannot start.", rc);
        // 在生产系统中，这里可能需要抛出异常或采取其他错误处理措施。
    }
  sstables_  = make_shared<vector<vector<shared_ptr<ObSSTable>>>>();
  if (options_.type == CompactionType::LEVELED) {
    sstables_->resize(options_.default_levels);
  }

  executor_.init("ObLsmBackground", 1, 1, 60 * 1000);
  block_cache_ =
      std::unique_ptr<ObLRUCache<uint64_t, shared_ptr<ObBlock>>>{new ObLRUCache<uint64_t, shared_ptr<ObBlock>>(1024)};
}

RC ObLsmImpl::recover()
{
  // recover from snapshot -> recover from compaction records -> recover from WAL -> write current snapshot into a new
  // manifest file.
  RC rc = manifest_.open();
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to open manifest file, rc=%s", strrc(rc));
    return rc;
  }

  std::vector<ObManifestCompaction>      compaction_records;
  std::unique_ptr<ObManifestSnapshot>    snapshot_record;
  std::unique_ptr<ObManifestNewMemtable> new_memtable_record;

  rc = manifest_.recover(snapshot_record, new_memtable_record, compaction_records);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to recover snapshot and manifest records from manifest file, rc=%s", strrc(rc));
    return rc;
  }

  // Recover Oblsm's state from snapshot.
  if (snapshot_record) {
    rc = load_manifest_snapshot(*snapshot_record);
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to load manifest snapshot, rc=%s", strrc(rc));
      return rc;
    }
  }

  // Recover ObLsm's state from compaction records.
  rc = recover_from_manifest_records(compaction_records);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to recover from manifest compaction records, rc=%s", strrc(rc));
    return rc;
  }

  // Recover memtable from WAL file.
  wal_ = std::make_unique<WAL>();

  // After recover from the old manifest file, write the snapshot into a new manifest file.
  if (!compaction_records.empty()) {
    rc = write_manifest_snapshot();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to write snapshot into manifest file, rc=%s", strrc(rc));
      return rc;
    }
  }

  return RC::SUCCESS;
}

RC ObLsm::open(const ObLsmOptions &options, const string &path, ObLsm **dbptr)
{
  RC         rc  = RC::SUCCESS;
  ObLsmImpl *lsm = new ObLsmImpl(options, path);
  *dbptr         = lsm;
  rc             = lsm->recover();
  return rc;
}

RC ObLsmImpl::put(const string_view &key, const string_view &value)
{
  // TODO: if put rate is too high, slow down writes is needed.
  // currently, the writes is stopped when the memtable is full.
  LOG_TRACE("begin to put key=%s, value=%s", key.data(), value.data());
  RC rc = RC::SUCCESS;
  // TODO: currenttly the memtable use skiplist as the underlying data structure,
  // and the skiplist concurently write is not thread safe, so we use mutex here,
  // if the skiplist support `insert_concurrently()` interface, can we remove the mutex?
  unique_lock<mutex> lock(mu_);
  uint64_t           seq = seq_.fetch_add(1);
  // Write WAL
  rc = wal_->put(seq, key, value);
  if (rc != RC::SUCCESS) {
    return rc;
  }

  if (options_.force_sync_new_log) {
    rc = wal_->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to sync wal logs, rc=%s", strrc(rc));
      return rc;
    }
  }
  // write memtable
  mem_table_->put(seq, key, value);
  size_t mem_size = mem_table_->appro_memory_usage();
  if (mem_size > options_.memtable_size) {
    // Thinking point: here vector is used to store imems,
    // but only one imem is stored at most. Is it possible
    // to store more than one imem and what are the implications
    // of storing more than one imem.
    if (imem_tables_.size() >= 1) {
      cv_.wait(lock);
    }
    // check again after get lock(maybe freeze memtable by another thread)
    if (mem_table_->appro_memory_usage() > options_.memtable_size) {
      manifest_.latest_seq = seq;
      try_freeze_memtable();
    } else {
      // if there are multi put threads waiting here, need to notify one thread to
      // continue to write to memtable.
      cv_.notify_one();
    }
  }
  return rc;
}

RC ObLsmImpl::batch_put(const vector<pair<string, string>> &kvs) { return RC::UNIMPLEMENTED; }

RC ObLsmImpl::remove(const string_view &key) { return RC::UNIMPLEMENTED; }

RC ObLsmImpl::try_freeze_memtable()
{
  RC rc = RC::SUCCESS;
  imem_tables_.emplace_back(mem_table_);
  mem_table_ = make_unique<ObMemTable>();
  // frozen previous wal
  if (!options_.force_sync_new_log) {
    rc = wal_->sync();
    if (rc != RC::SUCCESS) {
      LOG_ERROR("Failed to sync wal logs, rc=%s", strrc(rc));
      return rc;
    }
  }

  frozen_wals_.emplace_back(std::move(wal_));
  wal_                     = std::make_unique<WAL>();
  uint64_t new_memtable_id = memtable_id_.fetch_add(1) + 1;
  wal_->open(get_wal_path(new_memtable_id));
  std::shared_ptr<ObLsmBgCompactCtx> background_compaction_ctx = make_shared<ObLsmBgCompactCtx>(new_memtable_id);
  auto bg_task = [this, background_compaction_ctx]() { this->background_compaction(background_compaction_ctx); };
  int  ret     = executor_.execute(bg_task);
  if (ret != 0) {
    rc = RC::INTERNAL;
    LOG_WARN("fail to execute background compaction task");
  }
  return rc;
}

void ObLsmImpl::background_compaction(std::shared_ptr<ObLsmBgCompactCtx> ctx)
{
  unique_lock<mutex> lock(mu_);
  if (imem_tables_.size() >= 1) {
    shared_ptr<ObMemTable> imem       = imem_tables_.back();
    shared_ptr<WAL>        frozen_wal = frozen_wals_.back();

    // TODO: build memtable to sst shouldn't in lock?
    build_sstable(imem);
    imem_tables_.pop_back();
    frozen_wals_.pop_back();
    manifest_.push(ObManifestNewMemtable{ctx->new_memtable_id});

    ::remove(frozen_wal->filename().c_str());

    lock.unlock();
    cv_.notify_one();

    // TODO: trig compaction at more scenarios, for example,
    // seek compaction in
    // leveldb(https://github.com/google/leveldb/blob/578eeb702ec0fbb6b9780f3d4147b1076630d633/db/version_set.cc#L650).
    if (!compacting_) {
      compacting_.store(true);
      try_major_compaction();
      compacting_.store(false);
    }
    return;
  }
}

void ObLsmImpl::try_major_compaction()
{
  unique_lock<mutex>             lock(mu_);
  unique_ptr<ObCompactionPicker> picker(ObCompactionPicker::create(options_.type, &options_));
  unique_ptr<ObCompaction>       picked = picker->pick(sstables_);
  ObManifestCompaction           mf_record;
  lock.unlock();
  if (picked == nullptr || picked->size() == 0) {
    return;
  }
  vector<shared_ptr<ObSSTable>> results = do_compaction(picked.get());

  SSTablesPtr new_sstables = make_shared<vector<vector<shared_ptr<ObSSTable>>>>();
  lock.lock();
  size_t levels_size        = sstables_->size();
  bool   insert_new_sstable = false;
  auto   find_sstable       = [](const vector<shared_ptr<ObSSTable>> &picked, const shared_ptr<ObSSTable> &sstable) {
    for (auto &p : picked) {
      if (p->sst_id() == sstable->sst_id()) {
        return true;
      }
    }
    return false;
  };

  vector<shared_ptr<ObSSTable>> picked_sstables;
  picked_sstables      = picked->inputs(0);
  const auto &level_i1 = picked->inputs(1);
  if (level_i1.size() > 0) {
    picked_sstables.insert(picked_sstables.end(), level_i1.begin(), level_i1.end());
  }
  // TODO: unify the new sstables logic in all compaction type
  if (options_.type == CompactionType::TIRED) {
    for (int i = levels_size - 1; i >= 0; --i) {
      const vector<shared_ptr<ObSSTable>> &level_i = sstables_->at(i);
      for (auto &sstable : level_i) {
        if (find_sstable(picked_sstables, sstable)) {
          if (!insert_new_sstable) {
            new_sstables->insert(new_sstables->begin(), results);
            insert_new_sstable = true;
          }
        } else {
          new_sstables->insert(new_sstables->begin(), level_i);
          break;
        }
      }
    }
  } else if (options_.type == CompactionType::LEVELED) {
    // TODO: apply the compaction results to sstable

    // Apply the compaction results to sstables
    // results contains new SSTables for picked->level() + 1
    // picked->inputs(0) are from picked->level() (to be removed)
    // picked->inputs(1) are from picked->level() + 1 (to be removed)

    SSTablesPtr temp_sstables = make_shared<vector<vector<shared_ptr<ObSSTable>>>>(*sstables_);
    int compaction_level = picked->level();
    int target_level = compaction_level + 1;

    // Ensure enough levels exist in temp_sstables
    if (static_cast<size_t>(target_level) >= temp_sstables->size()) {
        temp_sstables->resize(target_level + 1);
    }

    // Remove inputs from compaction_level (L_i)
    if (static_cast<size_t>(compaction_level) < temp_sstables->size()) {
        auto& level_i_ssts = (*temp_sstables)[compaction_level];
        level_i_ssts.erase(
            std::remove_if(level_i_ssts.begin(), level_i_ssts.end(),
                           [&](const shared_ptr<ObSSTable>& sst) {
                               for (const auto& p_sst : picked->inputs(0)) {
                                   if (sst->sst_id() == p_sst->sst_id()) return true;
                               }
                               return false;
                           }),
            level_i_ssts.end());
    }

    // Remove inputs from target_level (L_i+1)
    auto& level_i_plus_1_ssts = (*temp_sstables)[target_level];
    level_i_plus_1_ssts.erase(
        std::remove_if(level_i_plus_1_ssts.begin(), level_i_plus_1_ssts.end(),
                       [&](const shared_ptr<ObSSTable>& sst) {
                           for (const auto& p_sst : picked->inputs(1)) {
                               if (sst->sst_id() == p_sst->sst_id()) return true;
                           }
                           return false;
                       }),
        level_i_plus_1_ssts.end());
    
    // Add results to target_level (L_i+1)
    level_i_plus_1_ssts.insert(level_i_plus_1_ssts.end(), results.begin(), results.end());

    // Sort target_level (L_i+1) by first key
    if (!level_i_plus_1_ssts.empty()) {
        std::sort(level_i_plus_1_ssts.begin(), level_i_plus_1_ssts.end(),
                  [](const shared_ptr<ObSSTable>& a, const shared_ptr<ObSSTable>& b) {
                      return a->first_key() < b->first_key();
                  });
    }
    // Assign the modified structure to new_sstables (which is the outer scope variable)
    *new_sstables = *temp_sstables;
  }

  sstables_ = new_sstables;
  lock.unlock();

  // remove from disk
  for (auto &sstable : picked_sstables) {
    sstable->remove();
  }

  mf_record.sstable_sequence_id = sstable_id_.load();
  mf_record.seq_id              = manifest_.latest_seq;
  manifest_.push(std::move(mf_record));
  try_major_compaction();
}

vector<shared_ptr<ObSSTable>> ObLsmImpl::do_compaction(ObCompaction *picked) { 
  if (picked == nullptr || picked->size() == 0) {
    return {};
  }

  vector<shared_ptr<ObSSTable>> result_sstables;
  vector<unique_ptr<ObLsmIterator>> iterators;

  // Collect iterators for level_ inputs (inputs(0))
  for (const auto &sstable : picked->inputs(0)) {
    if (sstable) {
      iterators.emplace_back(sstable->new_iterator());
    }
  }
  // Collect iterators for level_+1 inputs (inputs(1))
  for (const auto &sstable : picked->inputs(1)) {
    if (sstable) {
      iterators.emplace_back(sstable->new_iterator());
    }
  }

  if (iterators.empty()) {
    return {};
  }

  // ObMerger merger(iterators, &internal_key_comparator_);
  // merger.seek_to_first();
  unique_ptr<ObLsmIterator> merger_iter(new_merging_iterator(&internal_key_comparator_, std::move(iterators)));
  if (!merger_iter) { // Handle case where new_merging_iterator returns nullptr (e.g., no valid children)
    return {};
  }
  merger_iter->seek_to_first();

  shared_ptr<ObMemTable> current_mem_table = make_shared<ObMemTable>();

  while (merger_iter->valid()) {
    string_view internal_key_from_merger = merger_iter->key();
    string_view user_value = merger_iter->value();

    // 校验 internal_key_from_merger 的大小是否至少为 SEQ_SIZE
    // SEQ_SIZE 应该在 oblsm/ob_lsm_define.h 中定义，通常是 sizeof(uint64_t)
    if (internal_key_from_merger.size() < SEQ_SIZE) {
      LOG_ERROR("Corrupted internal key from merger: size %zu is less than SEQ_SIZE %zu. Skipping record.",
                internal_key_from_merger.size(), static_cast<size_t>(SEQ_SIZE));
      merger_iter->next();
      continue; // 跳过这条损坏的记录
    }

    // Parse internal key to get user key and sequence number
    string_view user_key_part = extract_user_key(internal_key_from_merger);
    // Assuming sequence number is the last SEQ_SIZE bytes of the internal key
    uint64_t sequence_num = get_numeric<uint64_t>(internal_key_from_merger.data() + internal_key_from_merger.size() - SEQ_SIZE);

    // 可选：进一步校验 user_key_part 和 user_value 的大小是否在合理范围内
    // const size_t MAX_REASONABLE_KEY_SIZE = 1 * 1024 * 1024; // 例如 1MB
    // const size_t MAX_REASONABLE_VALUE_SIZE = 16 * 1024 * 1024; // 例如 16MB
    // if (user_key_part.size() > MAX_REASONABLE_KEY_SIZE || user_value.size() > MAX_REASONABLE_VALUE_SIZE) {
    //   LOG_ERROR("Corrupted key/value size from merger. User key size: %zu, Value size: %zu. Skipping record.",
    //             user_key_part.size(), user_value.size());
    //   merger_iter->next();
    //   continue;
    // }

    current_mem_table->put(sequence_num, user_key_part, user_value);
    merger_iter->next();

    // Check if current memtable exceeds size limit, or if it's the last key from merger
    if (current_mem_table->appro_memory_usage() >= options_.table_size || !merger_iter->valid()) {
      if (current_mem_table->appro_memory_usage() > 0) { // Or check num_entries()
        unique_ptr<ObSSTableBuilder> tb =
            make_unique<ObSSTableBuilder>(&default_comparator_, block_cache_.get());
        uint64_t new_sstable_id = sstable_id_.fetch_add(1);
        // ObSSTableBuilder::build takes a shared_ptr<ObMemTable>
        tb->build(current_mem_table, get_sstable_path(new_sstable_id), new_sstable_id);
        shared_ptr<ObSSTable> new_table = tb->get_built_table();
        if (new_table && new_table->size() > 0) {
            result_sstables.push_back(new_table);
        }

        if (merger_iter->valid()) { // If there's more data to process for a new SSTable
          current_mem_table = make_shared<ObMemTable>();
        }
        // If !merger_iter->valid(), current_mem_table has been processed, and loop will terminate.
      }
    }
  }
  return result_sstables;
}

void ObLsmImpl::build_sstable(shared_ptr<ObMemTable> imem)
{
  unique_ptr<ObSSTableBuilder> tb = make_unique<ObSSTableBuilder>(&default_comparator_, block_cache_.get());

  uint64_t sstable_id_for_this_table = sstable_id_.fetch_add(1); // Atomically get and increment for the new SSTable
  tb->build(imem, get_sstable_path(sstable_id_for_this_table), sstable_id_for_this_table);
  shared_ptr<ObSSTable> built_table = tb->get_built_table();

  if (!built_table || built_table->size() == 0) {
    LOG_WARN("SSTable (id: %lu) built from memtable is empty or null. It will not be added to the LSM tree or manifest. Associated WAL may need cleanup if this path is taken frequently.", sstable_id_for_this_table);
    // The sstable_id_ counter has already been incremented. This will result in a gap in SSTable IDs, which is generally acceptable.
    // Optionally, you could attempt to remove the physical file if it was created:
    // ::remove(get_sstable_path(sstable_id_for_this_table).c_str());
    return; // Do not proceed to add this empty table or record it in the manifest
  }

  // If the table is valid and has data, proceed to add it to sstables_ and update the manifest.
  ObManifestCompaction record; // Used for LEVELED type manifest update
  record.compaction_type     = options_.type; // Note: Using ObManifestCompaction for a flush event.
  // sstable_sequence_id in manifest usually tracks the highest sstable_id after an operation.
  // sstable_id_.load() will give the next available ID.
  record.sstable_sequence_id = sstable_id_.load(); 
  record.seq_id              = manifest_.latest_seq;

  if (options_.type == CompactionType::TIRED) {
    sstables_->insert(sstables_->begin(), {built_table});
    // For TIRED compaction, manifest updates for new L0 tables might be handled differently or not here.
    // The current code does not push a manifest record for TIRED in this specific path.
  } else if (options_.type == CompactionType::LEVELED) {
    sstables_->at(0).emplace_back(built_table);
    record.added_tables.emplace_back(sstable_id_for_this_table, 0); // Record the actual ID of the table added to level 0
    manifest_.push(std::move(record));
  }
}

string ObLsmImpl::get_sstable_path(uint64_t sstable_id)
{
  return filesystem::path(path_) / (to_string(sstable_id) + SSTABLE_SUFFIX);
}

string ObLsmImpl::get_wal_path(uint64_t memtable_id)
{
  return filesystem::path(path_) / (to_string(memtable_id) + WAL_SUFFIX);
}

RC ObLsmImpl::get(const string_view &key, string *value)
{
  RC                 rc = RC::SUCCESS;
  unique_lock<mutex> lock(mu_);
  auto               iter = unique_ptr<ObLsmIterator>(new_iterator(ObLsmReadOptions{}));
  iter->seek(key);
  if (iter->valid() && iter->key() == key) {
    if (iter->value().empty()) {
      rc = RC::NOT_EXIST;
    } else {
      value->assign(iter->value());
    }
  } else {
    rc = RC::NOT_EXIST;
  }
  return rc;
}

ObLsmIterator *ObLsmImpl::new_iterator(ObLsmReadOptions options)
{
  unique_lock<mutex>     lock(mu_);
  shared_ptr<ObMemTable> mem = mem_table_;

  shared_ptr<ObMemTable> imm = nullptr;
  if (!imem_tables_.empty()) {
    imm = imem_tables_.back();
  }
  vector<shared_ptr<ObSSTable>> sstables;
  for (auto &level : *sstables_) {
    sstables.insert(sstables.end(), level.begin(), level.end());
  }
  lock.unlock();
  vector<unique_ptr<ObLsmIterator>> iters;
  iters.emplace_back(mem->new_iterator());
  if (imm != nullptr) {
    iters.emplace_back(imm->new_iterator());
  }
  for (const auto &sst : sstables) {
    iters.emplace_back(sst->new_iterator());
  }

  return new_user_iterator(
      new_merging_iterator(&internal_key_comparator_, std::move(iters)), options.seq == -1 ? seq_.load() : options.seq);
}

ObLsmTransaction *ObLsmImpl::begin_transaction() { return new ObLsmTransaction(this, seq_.load()); }

void ObLsmImpl::dump_sstables()
{
  unique_lock<mutex> lock(mu_);
  int                level = sstables_->size();
  for (int i = 0; i < level; i++) {
    cout << "level " << i << endl;
    int level_size = 0;
    for (auto &sst : sstables_->at(i)) {
      cout << sst->sst_id() << ": " << sst->size() << ";";
      level_size += sst->size();
    }
    cout << "level size " << level_size << endl;
  }
}

RC ObLsmImpl::recover_from_manifest_records(const std::vector<ObManifestCompaction> &records)
{
  std::vector<std::vector<uint64_t>> tmp_sstables;
  for (size_t i = 0; i < options_.default_levels; i++) {
    tmp_sstables.emplace_back();
  }
  for (auto &record : records) {
    // assert(sstable_id_ < record.sstable_sequence_id);
    sstable_id_ = record.sstable_sequence_id;
    seq_        = record.seq_id;
    // Added tables
    for (auto &info : record.added_tables) {
      uint32_t level      = info.level;
      uint64_t sstable_id = info.sstable_id;
      ASSERT(level < options_.default_levels, "level shouldn't greater than or equal to default level size");
      tmp_sstables[level].push_back(sstable_id);
    }
    // Deleted tables
    for (auto &info : record.deleted_tables) {
      uint32_t level = info.level;
      uint32_t sid   = info.sstable_id;
      ASSERT(level < options_.default_levels, "level shouldn't greater than or equal to default level size");
      auto del_iter = std::find(tmp_sstables[level].begin(), tmp_sstables[level].end(), sid);
      tmp_sstables[level].erase(del_iter);
    }
  }

  RC rc = load_manifest_sstable(tmp_sstables);
  if (rc != RC::SUCCESS) {
    LOG_ERROR("Failed to load sstables, rc=%s", strrc(rc));
    return rc;
  }
  return RC::SUCCESS;
}

RC ObLsmImpl::load_manifest_snapshot(const ObManifestSnapshot &snapshot)
{
  seq_        = snapshot.seq;
  sstable_id_ = snapshot.sstable_id;
  RC rc       = load_manifest_sstable(snapshot.sstables);
  return rc;
}

RC ObLsmImpl::load_manifest_sstable(const std::vector<std::vector<uint64_t>> &sstables)
{
  // After Getting the final state of lsm tree, recovering the system's state from tmp_sstables
  size_t cur_level_idx = 0;
  for (auto &sst_ids : sstables) {
    auto &cur_level = sstables_->at(cur_level_idx++);
    for (auto &sst_id : sst_ids) {
      auto filename = get_sstable_path(sst_id);
      auto sstable  = std::make_shared<ObSSTable>(sst_id, filename, &default_comparator_, block_cache_.get());
      sstable->init();
      cur_level.emplace_back(sstable);
    }
  }
  return RC::SUCCESS;
}

RC ObLsmImpl::write_manifest_snapshot()
{
  ObManifestSnapshot    snapshot;
  ObManifestNewMemtable new_memtable;
  snapshot.seq             = seq_.load();
  snapshot.sstable_id      = sstable_id_.load();
  snapshot.compaction_type = options_.type;
  snapshot.sstables.resize(sstables_->size());
  new_memtable.memtable_id = memtable_id_.load();
  for (size_t i = 0; i < sstables_->size(); ++i) {
    auto &level = sstables_->at(i);
    for (size_t j = 0; j < level.size(); ++j) {
      snapshot.sstables[i].push_back(level[j]->sst_id());
    }
  }
  RC rc = manifest_.redirect(snapshot, new_memtable);
  if (OB_FAIL(rc)) {
    LOG_ERROR("Failed to redirect snapshot");
    return rc;
  }
  return RC::SUCCESS;
}

}  // namespace oceanbase