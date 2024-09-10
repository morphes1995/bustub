//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetSize(0);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) { array_[index].first = key; }

/*
 * Helper method to get/set the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const -> ValueType { return array_[index].second; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValuePosition(const ValueType &value) -> int {
  auto it = std::find_if(array_, array_ + GetSize(), [&value](const auto &pair) { return pair.second == value; });

  return std::distance(array_, it);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetValueAt(int index, const ValueType &value) { array_[index].second = value; }
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Search(const KeyType &key, const KeyComparator &comparator) const -> ValueType {
  // target is a pointing to the first element not less than key, or end() if every element is less than key.
  // lower_bound is based on binary search
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key, [&comparator](const auto &pair, const auto &k) {
    return comparator(pair.first, k) < 0;
  });

  if (target == array_ + GetSize()) {
    return ValueAt(GetSize() - 1);
  }
  if (comparator(target->first, key) == 0) {
    return target->second;
  }
  return (--target)->second;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int pos = KeyPosition(key, comparator);

  std::move_backward(array_ + pos, array_ + GetSize(), array_ + GetSize() + 1);
  array_[pos] = std::make_pair(key, value);
  IncreaseSize(1);
  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyPosition(const KeyType &key, const KeyComparator &comparator) -> int {
  auto target = std::lower_bound(array_ + 1, array_ + GetSize(), key,
                                 [&comparator](const auto &pair, auto &k) { return comparator(pair.first, k) < 0; });
  return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_INTERNAL_PAGE_TYPE::SplitTo(Page *to_page, const KeyType &key, const ValueType &value,
                                             const KeyComparator &comparator) -> B_PLUS_TREE_INTERNAL_PAGE_TYPE * {
  auto *tree_to_page = reinterpret_cast<B_PLUS_TREE_INTERNAL_PAGE_TYPE *>(to_page->GetData());
  tree_to_page->Init(to_page->GetPageId(), GetParentPageId(), GetMaxSize());

  int pos = KeyPosition(key, comparator);
  int split_pos = GetMinSize();

  // key and value will insert into this to_page, split one more to tree_to_page
  if (pos < split_pos) {
    // copy kv to tree_to_page
    std::copy(array_ + split_pos - 1, array_ + GetSize(), tree_to_page->array_);
    tree_to_page->SetSize(GetSize() - split_pos + 1);

    // truncate this page
    SetSize(split_pos);
    Insert(key, value, comparator);

  } else {  // key and value will insert into tree_to_page
    // copy kv to tree_to_page
    std::copy(array_ + split_pos, array_ + GetSize(), tree_to_page->array_);
    tree_to_page->SetSize(GetSize() - split_pos);
    // insert kv to tree_to_page
    tree_to_page->Insert(key, value, comparator);

    // truncate this to_page
    SetSize(split_pos);
  }

  return tree_to_page;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int idx) {
  std::move(array_ + idx + 1, array_ + GetSize(), array_ + idx);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveRearToFrontOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *target_page,
                                                       const KeyType &target_page_risen_key) {
  auto pair_to_move = array_[GetSize() - 1];

  target_page->SetKeyAt(0, target_page_risen_key);
  std::move_backward(target_page->array_, target_page->array_ + target_page->GetSize(),
                     target_page->array_ + target_page->GetSize() + 1);
  target_page->array_[0] = pair_to_move;
  target_page->IncreaseSize(1);

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFrontToRearOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *target_page,
                                                       const KeyType &this_page_risen_key) {
  auto pair_to_move = array_[0];
  pair_to_move.first = this_page_risen_key;

  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);

  target_page->array_[target_page->GetSize()] = pair_to_move;
  target_page->IncreaseSize(1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *target_page,
                                               const KeyType &this_page_risen_key) {
  array_[0].first = this_page_risen_key;
  std::move(array_, array_ + GetSize(), target_page->array_ + target_page->GetSize());

  target_page->IncreaseSize(GetSize());
  SetSize(0);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
