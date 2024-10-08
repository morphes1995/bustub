//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/include/page/b_plus_tree_internal_page.h
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//
#pragma once

#include <queue>

#include "storage/page/b_plus_tree_page.h"

namespace bustub {

#define B_PLUS_TREE_INTERNAL_PAGE_TYPE BPlusTreeInternalPage<KeyType, ValueType, KeyComparator>
#define INTERNAL_PAGE_HEADER_SIZE 24
#define INTERNAL_PAGE_SIZE ((BUSTUB_PAGE_SIZE - INTERNAL_PAGE_HEADER_SIZE) / (sizeof(MappingType)))
/**
 * Store n indexed keys and n+1 child pointers (page_id) within internal page.
 * Pointer PAGE_ID(i) points to a subtree in which all keys K satisfy:
 * K(i) <= K < K(i+1).
 * NOTE: since the number of keys does not equal to number of child pointers,
 * the first key always remains invalid. That is to say, any search/lookup
 * should ignore the first key.
 *
 * Internal page format (keys are stored in increasing order):
 *  --------------------------------------------------------------------------
 * | HEADER | KEY(1)+PAGE_ID(1) | KEY(2)+PAGE_ID(2) | ... | KEY(n)+PAGE_ID(n) |
 *  --------------------------------------------------------------------------
 *  array example :
 *
 *  idx   0       1       2         3
 *  key   x       5       10        20
 *  val  (,5)   [5,10)  [10,20)   [20,)
 *
 *  array_[0] may physically stores a valid key (inflated by internal node splitting ), but logically we do not use it
 *
 */
INDEX_TEMPLATE_ARGUMENTS
class BPlusTreeInternalPage : public BPlusTreePage {
 public:
  // must call initialize method after "create" a new node
  void Init(page_id_t page_id, page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);

  auto KeyAt(int index) const -> KeyType;
  void SetKeyAt(int index, const KeyType &key);
  auto ValueAt(int index) const -> ValueType;
  auto ValuePosition(const ValueType &value) -> int;
  void SetValueAt(int index, const ValueType &value);

  auto Search(const KeyType &key, const KeyComparator &comparator) const -> ValueType;

  auto Insert(const KeyType &key, const ValueType &value, const KeyComparator &comparator) -> int;
  auto SplitTo(Page *to_page, const KeyType &key, const ValueType &value, const KeyComparator &comparator)
      -> B_PLUS_TREE_INTERNAL_PAGE_TYPE *;

  void Remove(int idx);
  void MoveRearToFrontOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *target_page, const KeyType &target_page_risen_key);
  void MoveFrontToRearOf(B_PLUS_TREE_INTERNAL_PAGE_TYPE *target_page, const KeyType &this_page_risen_key);

  void MoveAllTo(BPlusTreeInternalPage<KeyType, ValueType, KeyComparator> *target_page, const KeyType &key);

 private:
  // Flexible array member for page data.
  MappingType array_[1];

  auto KeyPosition(const KeyType &key, const KeyComparator &comparator) -> int;
};
}  // namespace bustub
