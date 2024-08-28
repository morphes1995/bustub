//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_k_replacer.cpp
//
// Identification: src/buffer/lru_k_replacer.cpp
//
// Copyright (c) 2015-2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "buffer/lru_k_replacer.h"

namespace bustub {

LRUKReplacer::LRUKReplacer(size_t num_frames, size_t k) : replacer_size_(num_frames), k_(k) {}

auto LRUKReplacer::Evict(frame_id_t *frame_id) -> bool {
  std::scoped_lock lock(latch_);

  if (curr_size_ <= 0) {
    return false;  // no evictable_ frame
  }

  for (auto it = this->history_list_.begin(); it != this->history_list_.end(); it++) {
    frame_id_t id = *it;
    if (this->frames_[id].evictable_) {
      *frame_id = id;
      history_list_.erase(it);
      this->curr_size_--;
      this->frames_.erase(id);
      return true;
    }
  }

  for (auto it = this->cache_list_.begin(); it != this->cache_list_.end(); it++) {
    frame_id_t id = *it;
    if (this->frames_[id].evictable_) {
      *frame_id = id;
      cache_list_.erase(it);
      this->curr_size_--;
      this->frames_.erase(id);
      return true;
    }
  }

  return false;
}

void LRUKReplacer::RecordAccess(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  this->current_timestamp_ += 1;  // update logical current timestamp

  if (static_cast<size_t>(frame_id) > this->replacer_size_) {
    char msg[32];
    sprintf(msg, "invalid frame: %d", frame_id);
    BUSTUB_ASSERT(false, msg);
  }

  auto it = this->frames_.find(frame_id);
  // first visit of this frame
  if (it == this->frames_.end()) {
    this->curr_size_++;
    this->history_list_.emplace_back(frame_id);
    // todo ?
    //    this->frames.emplace(frame_id, Frame(true, 1, {current_timestamp_}, std::prev(this->history_list_.end())));
    this->frames_.emplace(frame_id, Frame(true, 1, {current_timestamp_}, --(this->history_list_.end())));

    return;
  }

  Frame *frame = &it->second;
  frame->access_count_++;
  frame->timestamps_.push_back(this->current_timestamp_);
  if (frame->access_count_ < this->k_) {
    // 1. this frame still in history_list_ and only add access timestamp
  } else if (frame->access_count_ == this->k_) {
    // 2. this frame need to move from history_list_ to cache_list_
    size_t pivot = frame->timestamps_.front();

    this->history_list_.erase(frame->pos_iter_);

    auto insert_it = this->cache_list_.begin();
    while (insert_it != this->cache_list_.end() && this->frames_[*insert_it].timestamps_.front() < pivot) {
      insert_it++;
    }

    frame->pos_iter_ = cache_list_.insert(insert_it, frame_id);  // insert and record position in frame
  } else {
    // 3. this frame already in cache_list_, update its access timestamps_ and the position in cache list
    frame->timestamps_.pop_front();
    size_t pivot = frame->timestamps_.front();

    auto insert_it = std::next(frame->pos_iter_);  // start from the next of old position , get insert iterator before
                                                   // frame.pos_iter_ become invalid
    this->cache_list_.erase(frame->pos_iter_);     // remove from old position, frame->pos_iter_ become invalid

    while (insert_it != this->cache_list_.end() && this->frames_[*insert_it].timestamps_.front() < pivot) {
      insert_it++;
    }
    // Inserts given value into %list before specified iterator.
    frame->pos_iter_ = cache_list_.insert(insert_it, frame_id);  // insert and record position in frame
  }
}

void LRUKReplacer::SetEvictable(frame_id_t frame_id, bool set_evictable) {
  std::scoped_lock lock(latch_);

  auto it = this->frames_.find(frame_id);
  if (it == this->frames_.end()) {
    return;
  }

  // update current size
  if (!it->second.evictable_ && set_evictable) {
    this->curr_size_++;  // a frame from unevictable to evictable_
  }
  if (it->second.evictable_ && !set_evictable) {
    this->curr_size_--;  // a frame from evictable_ to unevictable
  }

  it->second.evictable_ = set_evictable;
}

void LRUKReplacer::Remove(frame_id_t frame_id) {
  std::scoped_lock lock(latch_);

  auto it = this->frames_.find(frame_id);
  if (it == this->frames_.end()) {
    return;  // not found
  }
  Frame f = it->second;
  if (!f.evictable_) {
    char msg[64];
    sprintf(msg, "try to remove an unevictable frame: %d", frame_id);
    BUSTUB_ASSERT(false, msg);
  }
  // remove from visit list
  if (f.access_count_ >= this->k_) {
    this->cache_list_.erase(f.pos_iter_);
  } else {
    this->history_list_.erase(f.pos_iter_);
  }
  // remove from frames map
  this->frames_.erase(frame_id);
  // update current count
  this->curr_size_--;
}

auto LRUKReplacer::Size() -> size_t {
  std::scoped_lock lock(latch_);
  return this->curr_size_;
}

}  // namespace bustub
