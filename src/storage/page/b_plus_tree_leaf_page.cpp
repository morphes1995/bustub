//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_leaf_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/page/b_plus_tree_leaf_page.h"

namespace bustub {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetPageId(page_id);
  SetParentPageId(parent_id);
  SetMaxSize(max_size);
  SetSize(0);
  SetNextPageId(INVALID_PAGE_ID);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const -> page_id_t { return next_page_id_; }

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) { next_page_id_ = next_page_id; }

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const -> KeyType { return array_[index].first; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::ItemAt(int idx) -> const MappingType & { return array_[idx]; }

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::KeyPosition(const KeyType &key, const KeyComparator &comparator) -> int {
  auto target = std::lower_bound(array_, array_ + GetSize(), key,
                                 [&comparator](const auto &pair, auto &k) { return comparator(pair.first, k) < 0; });

  return std::distance(array_, target);
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Search(const KeyType &key, const KeyComparator &comparator, ValueType *val) -> bool {
  int pos = KeyPosition(key, comparator);
  if (pos == GetSize() || comparator(key, array_[pos].first) != 0) {
    return false;
  }

  *val = array_[pos].second;
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator)
    -> int {
  int pos = KeyPosition(key, comparator);
  if (pos == GetSize()) {
    // append to rear position
    array_[pos] = std::make_pair(key, value);
    IncreaseSize(1);
    return GetSize();
  }

  // key already exist
  if (comparator(key, array_[pos].first) == 0) {
    return GetSize();
  }

  // eg insert 1.5 : [1,2,3,4] -> [1,2,2,3,4] ->[1,1.5, 2,3,4]
  std::move_backward(array_ + pos, array_ + GetSize(), array_ + GetSize() + 1);
  array_[pos] = std::make_pair(key, value);
  IncreaseSize(1);

  return GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::SplitTo(Page *new_page) -> B_PLUS_TREE_LEAF_PAGE_TYPE * {
  auto *new_leaf_page = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(new_page->GetData());
  new_leaf_page->Init(new_page->GetPageId(), GetParentPageId(), GetMaxSize());

  int idx = GetMinSize();
  std::copy(array_ + idx, array_ + GetSize(), new_leaf_page->array_);  // copy array_[idx:] to new leaf page
  new_leaf_page->IncreaseSize(GetSize() - idx);

  SetSize(idx);  // truncate array_, reserve array_[:idx) to this leaf page

  return new_leaf_page;
}

INDEX_TEMPLATE_ARGUMENTS
auto B_PLUS_TREE_LEAF_PAGE_TYPE::Remove(const KeyType &key, const KeyComparator &comparator) -> bool {
  int pos = KeyPosition(key, comparator);
  if (pos == GetSize() || comparator(KeyAt(pos), key) != 0) {
    return false;
  }

  std::move(array_ + pos + 1, array_ + GetSize(), array_ + pos);
  IncreaseSize(-1);
  return true;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveRearToFrontOf(B_PLUS_TREE_LEAF_PAGE_TYPE *target_page) {
  auto pair_to_move = array_[GetSize() - 1];

  std::move_backward(target_page->array_, target_page->array_ + GetSize(),
                     target_page->array_ + target_page->GetSize() + 1);
  target_page->array_[0] = pair_to_move;
  target_page->IncreaseSize(1);

  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFrontToRearOf(B_PLUS_TREE_LEAF_PAGE_TYPE *target_page) {
  auto pair_to_move = array_[0];

  target_page->array_[target_page->GetSize()] = pair_to_move;
  target_page->IncreaseSize(1);

  std::move(array_ + 1, array_ + GetSize(), array_);
  IncreaseSize(-1);
}
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(B_PLUS_TREE_LEAF_PAGE_TYPE *target_page) {
  std::move(array_, array_ + GetSize(), target_page->array_ + target_page->GetSize());
  SetSize(0);
  target_page->SetNextPageId(GetNextPageId());
}

template class BPlusTreeLeafPage<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID, GenericComparator<64>>;
}  // namespace bustub
