//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2022, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cassert>
#include <cstdlib>
#include <functional>
#include <list>
#include <utility>

#include "container/hash/extendible_hash_table.h"
#include "storage/page/page.h"

namespace bustub {

template <typename K, typename V>
ExtendibleHashTable<K, V>::ExtendibleHashTable(size_t bucket_size)
    : global_depth_(0), bucket_size_(bucket_size), num_buckets_(1) {
  this->dir_.push_back(std::make_shared<Bucket>(bucket_size));
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::IndexOf(const K &key) -> size_t {
  int mask = (1 << global_depth_) - 1;
  return std::hash<K>()(key) & mask;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepth() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetGlobalDepthInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetGlobalDepthInternal() const -> int {
  return global_depth_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepth(int dir_index) const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetLocalDepthInternal(dir_index);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetLocalDepthInternal(int dir_index) const -> int {
  return dir_[dir_index]->GetDepth();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBuckets() const -> int {
  std::scoped_lock<std::mutex> lock(latch_);
  return GetNumBucketsInternal();
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::GetNumBucketsInternal() const -> int {
  return num_buckets_;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Find(const K &key, V &value) -> bool {
  std::scoped_lock lock(latch_);

  size_t i = IndexOf(key);
  std::shared_ptr<Bucket> bucket = this->dir_[i];
  return bucket->Find(key, value);
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Remove(const K &key) -> bool {
  std::scoped_lock lock(latch_);
  return this->dir_[IndexOf(key)]->Remove(key);
}

template <typename K, typename V>
void ExtendibleHashTable<K, V>::Insert(const K &key, const V &value) {
  std::scoped_lock lock(latch_);

  size_t idx = IndexOf(key);
  std::shared_ptr<Bucket> bucket = this->dir_[idx];

  while (bucket->IsFull()) {
    // double the slot array;
    if (bucket->GetDepth() == this->global_depth_) {
      int num_buckets = 1 << this->global_depth_;
      for (int i = 0; i < num_buckets; i++) {
        this->dir_.push_back(this->dir_[i]);
      }
      this->global_depth_ += 1;
    }

    // create two new buckets
    int mask = 1 << bucket->GetDepth();
    int idx2 =
        (mask & idx) == 0 ? mask + idx : idx - mask;  // corresponding bucket index to offload KVs in idx-th bucket
    this->dir_[idx] = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    this->dir_[idx2] = std::make_shared<Bucket>(bucket_size_, bucket->GetDepth() + 1);
    this->num_buckets_ += 1;

    // rehash kv to new buckets
    std::for_each(bucket->GetItems().begin(), bucket->GetItems().end(), [this](const auto &item) {
      size_t new_idx = this->IndexOf(item.first);
      this->dir_[new_idx]->Insert(item.first, item.second);
    });

    // update target bucket for next loop (target bucket may be still full)
    idx = IndexOf(key);
    bucket = this->dir_[idx];
  }

  // bucket is not full now , inset and return
  bucket->Insert(key, value);
}

//===--------------------------------------------------------------------===//
// Bucket
//===--------------------------------------------------------------------===//
template <typename K, typename V>
ExtendibleHashTable<K, V>::Bucket::Bucket(size_t array_size, int depth) : size_(array_size), depth_(depth) {}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Find(const K &key, V &value) -> bool {
  return std::any_of(this->list_.begin(), this->list_.end(), [&key, &value](const auto &it) {
    if (it.first == key) {
      value = it.second;
      return true;
    }
    return false;
  });

  //  auto  it = this->list_.begin();
  //  auto end = this->list_.end();
  //  while (it != end){
  //    if (it->first == key){
  //      value = it->second;
  //      return true;
  //    }
  //    ++it;
  //  }
  //  return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Remove(const K &key) -> bool {
  return std::any_of(this->list_.begin(), this->list_.end(), [&key, this](const auto &it) {
    if (it.first == key) {
      this->list_.remove(it);
      return true;
    }
    return false;
  });

  //    auto  it = this->list_.begin();
  //    auto end = this->list_.end();
  //    while (it != end){
  //      if (it->first == key){
  //        this->list_.remove(*it);
  //        return true;
  //      }
  //      ++it;
  //    }
  //    return false;
}

template <typename K, typename V>
auto ExtendibleHashTable<K, V>::Bucket::Insert(const K &key, const V &value) -> bool {
  if (this->IsFull()) {
    return false;
  }

  auto it = this->list_.begin();
  auto end = this->list_.end();
  while (it != end) {
    if (it->first == key) {
      it->second = value;
      return true;
    }
    ++it;
  }

  this->list_.emplace_back(key, value);
  return true;
}

template class ExtendibleHashTable<page_id_t, Page *>;
template class ExtendibleHashTable<Page *, std::list<Page *>::iterator>;
template class ExtendibleHashTable<int, int>;
// test purpose
template class ExtendibleHashTable<int, std::string>;
template class ExtendibleHashTable<int, std::list<int>::iterator>;

}  // namespace bustub
