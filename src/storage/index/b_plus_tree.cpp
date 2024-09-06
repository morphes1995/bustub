#include <string>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::IsEmpty() const -> bool { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) -> bool {
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "invalid root page id");

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!tree_page->IsLeafPage()) {
    auto *tree_internal_page = reinterpret_cast<InternalPage *>(page->GetData());

    page_id_t tree_child_page_id = tree_internal_page->Search(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(tree_child_page_id);
    auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    page = child_page;
    tree_page = child_tree_page;
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  ValueType val;
  bool find = leaf_page->Search(key, comparator_, &val);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

  if (find) {
    result->push_back(val);
    return true;
  }

  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  Page *page = buffer_pool_manager_->NewPage(&root_page_id_);
  BUSTUB_ASSERT(page != nullptr, "allocate b+tree root node failed");

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  leaf_page->Init(root_page_id_, INVALID_PAGE_ID, leaf_max_size_);

  leaf_page->Insert(key, value, comparator_);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  // Insert this index name and root page id to header page(where page_id = 0 )
  UpdateRootPageId(1);
}

INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *pTransaction) -> bool {
  BUSTUB_ASSERT(root_page_id_ != INVALID_PAGE_ID, "invalid root page id");

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!tree_page->IsLeafPage()) {
    auto *tree_internal_page = reinterpret_cast<InternalPage *>(page->GetData());

    page_id_t tree_child_page_id = tree_internal_page->Search(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(tree_child_page_id);
    auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    page = child_page;
    tree_page = child_tree_page;
  }

  auto *leaf_page = reinterpret_cast<LeafPage *>(page->GetData());
  int old_size = leaf_page->GetSize();
  int new_size = leaf_page->Insert(key, value, comparator_);

  // duplicate key
  if (new_size == old_size) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return false;
  }

  // after insertion , check split condition
  // not full
  if (new_size < leaf_max_size_) {
    buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
    return true;
  }

  // leaf node become full
  page_id_t new_page_id;
  Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
  BUSTUB_ASSERT(new_page != nullptr, "allocate new leaf page failed when splitting");
  LeafPage *new_leaf_page = leaf_page->SplitTo(new_page);
  new_leaf_page->SetNextPageId(leaf_page->GetNextPageId());
  leaf_page->SetNextPageId(new_leaf_page->GetPageId());

  auto risen_key = new_leaf_page->KeyAt(0);
  InsertRisenKeyToParent(risen_key, leaf_page, new_leaf_page);

  buffer_pool_manager_->UnpinPage(new_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);
  return true;
}
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertRisenKeyToParent(KeyType &risen_key, BPlusTreePage *page_origin, BPlusTreePage *page_split) {
  // 1. root node split
  if (page_origin->IsRootPage()) {
    page_id_t new_root_page_id;
    Page *new_root_page = buffer_pool_manager_->NewPage(&new_root_page_id);
    BUSTUB_ASSERT(new_root_page != nullptr, "allocate new root page_origin failed when splitting");

    auto *tree_new_root_page = reinterpret_cast<InternalPage *>(new_root_page->GetData());
    tree_new_root_page->Init(new_root_page->GetPageId(), INVALID_PAGE_ID, internal_max_size_);

    // update new root page
    tree_new_root_page->SetKeyAt(1, risen_key);
    tree_new_root_page->SetValueAt(0, page_origin->GetPageId());
    tree_new_root_page->SetValueAt(1, page_split->GetPageId());
    tree_new_root_page->SetSize(2);

    // update parent pointer in child nodes
    page_origin->SetParentPageId(tree_new_root_page->GetPageId());
    page_split->SetParentPageId(tree_new_root_page->GetPageId());

    // update root page id in disk
    UpdateRootPageId(0);

    buffer_pool_manager_->UnpinPage(new_root_page->GetPageId(), true);
    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(page_origin->GetParentPageId());
  BUSTUB_ASSERT(parent_page != nullptr, "fetch parent node failed when rise key");
  auto *tree_parent_page = reinterpret_cast<InternalPage *>(parent_page->GetData());

  // 2. parent internal node is not full
  if (tree_parent_page->GetSize() < internal_max_size_) {
    tree_parent_page->Insert(risen_key, page_split->GetPageId(), comparator_);
    //    page_split->SetParentPageId(tree_parent_page->GetPageId()); // page_split initialized with parent id same as
    //    page_origin
    buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
    return;
  }

  // 3. parent internal node is full, tree_parent_page->GetSize() == internal_max_size_
  page_id_t new_page_id;
  Page *split_parent_page = buffer_pool_manager_->NewPage(&new_page_id);
  BUSTUB_ASSERT(split_parent_page != nullptr, "allocate new parent page failed when splitting");

  // re-organize pairs in origin parent node and risen key, and split
  InternalPage *tree_split_parent_page =
      tree_parent_page->SplitTo(split_parent_page, risen_key, page_split->GetPageId(), comparator_);

  // for child pages move from tree_parent_page to tree_split_parent_page, update those child page's parent page id
  for (int i = 0; i < tree_split_parent_page->GetSize(); i++) {
    Page *child_page = buffer_pool_manager_->FetchPage(tree_split_parent_page->ValueAt(i));
    auto *tree_child_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
    tree_child_page->SetParentPageId(tree_split_parent_page->GetPageId());

    buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
  }

  KeyType parent_risen_key = tree_split_parent_page->KeyAt(0);
  InsertRisenKeyToParent(parent_risen_key, tree_parent_page, tree_split_parent_page);

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(split_parent_page->GetPageId(), true);
}

/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) -> bool {
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }

  return InsertIntoLeaf(key, value, transaction);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/

/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  auto *tree_page = reinterpret_cast<BPlusTreePage *>(page->GetData());

  while (!tree_page->IsLeafPage()) {
    auto *tree_internal_page = reinterpret_cast<InternalPage *>(page->GetData());

    page_id_t tree_child_page_id = tree_internal_page->Search(key, comparator_);
    Page *child_page = buffer_pool_manager_->FetchPage(tree_child_page_id);
    auto *child_tree_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);

    page = child_page;
    tree_page = child_tree_page;
  }

  auto *tree_leaf_page = reinterpret_cast<LeafPage *>(page->GetData());

  if (!tree_leaf_page->Remove(key, comparator_)) {
    // key not found in this leaf page
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    return;
  }

  // key was deleted from leaf page, check condition for redistribution and coalesce from leaf page
  ReBalanceAfterDeletion(tree_leaf_page, transaction);

  buffer_pool_manager_->UnpinPage(page->GetPageId(), true);

  // delete empty Page from buffer pool
  std::for_each(transaction->GetDeletedPageSet()->begin(), transaction->GetDeletedPageSet()->end(),
                [this](const auto &id) { buffer_pool_manager_->DeletePage(id); });
  transaction->GetDeletedPageSet()->clear();
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReBalanceAfterDeletion(LeafPage *tree_leaf_page, Transaction *transaction) {
  if (tree_leaf_page->IsRootPage()) {
    if (tree_leaf_page->GetSize() == 0) {
      // the key deletion leaf page is root page
      transaction->AddIntoDeletedPageSet(tree_leaf_page->GetPageId());
      tree_leaf_page->SetPageId(INVALID_PAGE_ID);
      UpdateRootPageId(0);
    }
    return;
  }

  // the key deletion leaf page is not root page
  if (tree_leaf_page->GetSize() >= tree_leaf_page->GetMinSize()) {
    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(tree_leaf_page->GetParentPageId());
  auto *tree_parent_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int pos = tree_parent_page->ValuePosition(tree_leaf_page->GetPageId());

  BUSTUB_ASSERT(tree_parent_page->GetSize() > 1, "any parent page must has more than one child");
  bool prev_sibling = true;
  int sibling_pos = pos - 1;
  if (pos == 0) {
    prev_sibling = false;
    sibling_pos = pos + 1;
  }

  Page *sibling_page = buffer_pool_manager_->FetchPage(tree_parent_page->ValueAt(sibling_pos));
  auto *tree_leaf_sibling_page = reinterpret_cast<LeafPage *>(sibling_page->GetData());

  if (tree_leaf_sibling_page->GetSize() > tree_leaf_sibling_page->GetMinSize()) {
    // redistribute
    if (prev_sibling) {
      tree_leaf_sibling_page->MoveRearToFrontOf(tree_leaf_page);
      tree_parent_page->SetKeyAt(pos, tree_leaf_page->KeyAt(0));
    } else {
      tree_leaf_sibling_page->MoveFrontToRearOf(tree_leaf_page);
      tree_parent_page->SetKeyAt(sibling_pos, tree_leaf_sibling_page->KeyAt(0));
    }

  } else {
    // coalesce
    if (prev_sibling) {
      tree_leaf_page->MoveAllTo(tree_leaf_sibling_page);
      // leaf_page is now empty
      tree_parent_page->Remove(pos);
      transaction->AddIntoDeletedPageSet(tree_leaf_page->GetPageId());

    } else {
      tree_leaf_sibling_page->MoveAllTo(tree_leaf_page);
      // sibling_leaf_page is now empty
      tree_parent_page->Remove(sibling_pos);
      transaction->AddIntoDeletedPageSet(tree_leaf_sibling_page->GetPageId());
    }

    ReBalanceInternal(tree_parent_page, transaction);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
}

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ReBalanceInternal(InternalPage *internal_page, Transaction *transaction) {
  if (internal_page->IsRootPage() && internal_page->GetSize() == 1) {
    Page *only_child = buffer_pool_manager_->FetchPage(internal_page->ValueAt(0));
    auto *tree_only_child = reinterpret_cast<BPlusTreePage *>(only_child->GetData());
    tree_only_child->SetParentPageId(INVALID_PAGE_ID);
    root_page_id_ = only_child->GetPageId();
    UpdateRootPageId(0);
    transaction->AddIntoDeletedPageSet(internal_page->GetPageId());

    buffer_pool_manager_->UnpinPage(only_child->GetPageId(), true);
    return;
  }

  // the key deletion internal page is not root page
  if (internal_page->GetSize() >= internal_page->GetMinSize()) {
    return;
  }

  Page *parent_page = buffer_pool_manager_->FetchPage(internal_page->GetParentPageId());
  auto *tree_parent_page = reinterpret_cast<InternalPage *>(parent_page->GetData());
  int pos = tree_parent_page->ValuePosition(internal_page->GetPageId());

  BUSTUB_ASSERT(tree_parent_page->GetSize() > 1, "any parent page must has more than one child");
  bool prev_sibling = true;
  int sibling_pos = pos - 1;
  if (pos == 0) {
    prev_sibling = false;
    sibling_pos = pos + 1;
  }

  Page *sibling_page = buffer_pool_manager_->FetchPage(tree_parent_page->ValueAt(sibling_pos));
  auto *tree_internal_sibling_page = reinterpret_cast<InternalPage *>(sibling_page->GetData());

  if (tree_internal_sibling_page->GetSize() > tree_internal_sibling_page->GetMinSize()) {
    // redistribute
    if (prev_sibling) {
      tree_internal_sibling_page->MoveRearToFrontOf(internal_page, tree_parent_page->KeyAt(pos));
      tree_parent_page->SetKeyAt(pos, internal_page->KeyAt(0));

      // update moved pair page's parent page id
      Page *page_for_moved_pair = buffer_pool_manager_->FetchPage(internal_page->ValueAt(0));
      auto *tree_page_for_moved_pair = reinterpret_cast<BPlusTreePage *>(page_for_moved_pair->GetData());
      tree_page_for_moved_pair->SetParentPageId(internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(page_for_moved_pair->GetPageId(), true);
    } else {
      tree_internal_sibling_page->MoveFrontToRearOf(internal_page, tree_parent_page->KeyAt(sibling_pos));
      tree_parent_page->SetKeyAt(sibling_pos, tree_internal_sibling_page->KeyAt(0));

      // update moved pair page's parent page id
      Page *page_for_moved_pair = buffer_pool_manager_->FetchPage(internal_page->ValueAt(internal_page->GetSize() - 1));
      auto *tree_page_for_moved_pair = reinterpret_cast<BPlusTreePage *>(page_for_moved_pair->GetData());
      tree_page_for_moved_pair->SetParentPageId(internal_page->GetPageId());
      buffer_pool_manager_->UnpinPage(page_for_moved_pair->GetPageId(), true);
    }

  } else {
    // coalesce
    if (prev_sibling) {
      int old_size = tree_internal_sibling_page->GetSize();
      internal_page->MoveAllTo(tree_internal_sibling_page, tree_parent_page->KeyAt(pos));

      // update moved pair page's parent page id
      int new_size = tree_internal_sibling_page->GetSize();
      for (int i = old_size; i < new_size; i++) {
        Page *child_page = buffer_pool_manager_->FetchPage(tree_internal_sibling_page->ValueAt(i));
        auto tree_child_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        tree_child_page->SetParentPageId(tree_internal_sibling_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      }

      // internal_page is now empty
      tree_parent_page->Remove(pos);
      transaction->AddIntoDeletedPageSet(internal_page->GetPageId());

    } else {
      int old_size = internal_page->GetSize();
      tree_internal_sibling_page->MoveAllTo(internal_page, tree_parent_page->KeyAt(sibling_pos));
      // update moved pair page's parent page id
      int new_size = internal_page->GetSize();
      for (int i = old_size; i < new_size; i++) {
        Page *child_page = buffer_pool_manager_->FetchPage(internal_page->ValueAt(i));
        auto tree_child_page = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
        tree_child_page->SetParentPageId(internal_page->GetPageId());
        buffer_pool_manager_->UnpinPage(child_page->GetPageId(), true);
      }

      // tree_internal_sibling_page is now empty
      tree_parent_page->Remove(sibling_pos);
      transaction->AddIntoDeletedPageSet(tree_internal_sibling_page->GetPageId());
    }

    ReBalanceInternal(tree_parent_page, transaction);
  }

  buffer_pool_manager_->UnpinPage(parent_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(sibling_page->GetPageId(), true);
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::Begin(const KeyType &key) -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::End() -> INDEXITERATOR_TYPE { return INDEXITERATOR_TYPE(); }

/**
 * @return Page id of the root of this tree
 */
INDEX_TEMPLATE_ARGUMENTS
auto BPLUSTREE_TYPE::GetRootPageId() -> page_id_t { return root_page_id_; }

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  auto *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Draw(BufferPoolManager *bpm, const std::string &outf) {
  if (IsEmpty()) {
    LOG_WARN("Draw an empty tree");
    return;
  }
  std::ofstream out(outf);
  out << "digraph G {" << std::endl;
  ToGraph(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm, out);
  out << "}" << std::endl;
  out.flush();
  out.close();
}

/**
 * This method is used for debug only, You don't need to modify
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Print(BufferPoolManager *bpm) {
  if (IsEmpty()) {
    LOG_WARN("Print an empty tree");
    return;
  }
  ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(root_page_id_)->GetData()), bpm);
}

/**
 * This method is used for debug only, You don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << ",size=" << leaf->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    auto *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << ",size=" << inner->GetSize()
        << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    auto *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    auto *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
