//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// buffer_pool_manager_instance.cpp
//
// Identification: src/buffer/buffer_pool_manager.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/buffer_pool_manager_instance.h"

#include "common/exception.h"
#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager, size_t replacer_k,
                                                     LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager), log_manager_(log_manager) {
  // we allocate a consecutive memory space for the buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHashTable<page_id_t, frame_id_t>(bucket_size_);
  replacer_ = new LRUKReplacer(pool_size, replacer_k);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
}

auto BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) -> Page * {
  std::scoped_lock lock(latch_);

  frame_id_t f_id;
  // 1. pick a free frame
  if (!this->free_list_.empty()) {
    f_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else if (this->replacer_->Evict(&f_id)) {
    Page *p_writeback = &pages_[f_id];
    // evicted page in the picked frame is dirty , write it back to disk
    if (p_writeback->is_dirty_) {
      disk_manager_->WritePage(p_writeback->page_id_, p_writeback->data_);
    }
    // reset the memory and metadata for the evicted page
    page_table_->Remove(p_writeback->page_id_);
    p_writeback->pin_count_ = 0;
    p_writeback->page_id_ = INVALID_PAGE_ID;
    p_writeback->ResetMemory();

  } else {
    return nullptr;  // all frames are currently in use and not evictable
  }
  // 2. allocate a new page and init it
  *page_id = this->AllocatePage();
  page_table_->Insert(*page_id, f_id);
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);

  Page *new_page = &pages_[f_id];
  new_page->page_id_ = *page_id;
  new_page->pin_count_ = 1;

  return new_page;
}

auto BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) -> Page * {
  std::scoped_lock lock(latch_);
  frame_id_t f_id;
  // 1. target page is already in buffer pool
  if (page_table_->Find(page_id, f_id)) {
    pages_[f_id].pin_count_++;
    replacer_->RecordAccess(f_id);
    replacer_->SetEvictable(f_id, false);
    return &pages_[f_id];
  }

  // 2. target page is not in buffer pool, and we need pick a free frame
  if (!this->free_list_.empty()) {
    f_id = this->free_list_.front();
    this->free_list_.pop_front();
  } else if (this->replacer_->Evict(&f_id)) {
    Page *p_writeback = &pages_[f_id];
    // evicted page in the picked frame is dirty , write it back to disk
    if (p_writeback->is_dirty_) {
      disk_manager_->WritePage(p_writeback->page_id_, p_writeback->data_);
    }
    // reset the memory and metadata for the evicted page
    page_table_->Remove(p_writeback->page_id_);
    p_writeback->pin_count_ = 0;
    p_writeback->page_id_ = INVALID_PAGE_ID;
    p_writeback->ResetMemory();

  } else {
    return nullptr;  // all frames are currently in use and not evictable
  }

  // 3. reload page data from disk , and init this new loaded page
  Page *target_page = &pages_[f_id];
  disk_manager_->ReadPage(page_id, target_page->data_);

  page_table_->Insert(page_id, f_id);
  replacer_->RecordAccess(f_id);
  replacer_->SetEvictable(f_id, false);
  target_page->page_id_ = page_id;
  target_page->pin_count_ = 1;

  return target_page;
}

auto BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) -> bool {
  std::scoped_lock lock(latch_);

  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {
    return false;  // page not in buffer pool
  }

  Page *page = &pages_[f_id];
  page->is_dirty_ = is_dirty;

  if (page->pin_count_ <= 0) {
    return false;
  }

  page->pin_count_--;
  if (page->pin_count_ == 0) {
    replacer_->SetEvictable(f_id, true);
  }

  return true;
}

auto BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);
  return FlushPgNonLockImp(page_id);
}

auto BufferPoolManagerInstance::FlushPgNonLockImp(page_id_t page_id) -> bool {
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }
  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {
    return false;
  }

  disk_manager_->WritePage(page_id, pages_[f_id].data_);
  pages_[f_id].is_dirty_ = false;

  return true;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  std::scoped_lock lock(latch_);
  for (size_t i = 0; i < pool_size_; i++) {
    FlushPgNonLockImp(pages_[i].page_id_);
  }
}

auto BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) -> bool {
  std::scoped_lock lock(latch_);

  frame_id_t f_id;
  if (!page_table_->Find(page_id, f_id)) {
    return true;
  }

  if (pages_[f_id].pin_count_ > 0) {
    return false;
  }

  replacer_->Remove(f_id);

  pages_[f_id].ResetMemory();
  pages_[f_id].page_id_ = INVALID_PAGE_ID;
  pages_[f_id].pin_count_ = 0;
  pages_[f_id].is_dirty_ = false;

  page_table_->Remove(page_id);
  free_list_.push_back(f_id);

  DeallocatePage(page_id);

  return true;
}

auto BufferPoolManagerInstance::AllocatePage() -> page_id_t { return next_page_id_++; }

}  // namespace bustub
