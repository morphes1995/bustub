/**
 * index_iterator.cpp
 */
#include <cassert>

#include "storage/index/index_iterator.h"

namespace bustub {

/*
 * NOTE: you can change the destructor/constructor method here
 * set your own input parameters
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator() = default;

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::IndexIterator(BufferPoolManager *bpm, Page * curr_page, LeafPage *curr_leaf_page,  int curr_idx)
    : bpm_(bpm), curr_page_(curr_page), curr_leaf_page_(curr_leaf_page),curr_idx_(curr_idx){}

INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE::~IndexIterator(){
  bpm_->UnpinPage(curr_page_->GetPageId(), false);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::IsEnd() -> bool {
  return curr_leaf_page_->GetNextPageId() == INVALID_PAGE_ID && curr_idx_ == curr_leaf_page_->GetSize();
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator*() -> const MappingType & {
  return curr_leaf_page_->ItemAt(curr_idx_);
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator++() -> INDEXITERATOR_TYPE & {
  BUSTUB_ASSERT(!IsEnd(),"iterator is already on end, can not advance further !");

  if(curr_leaf_page_->GetNextPageId() != INVALID_PAGE_ID && curr_idx_ == curr_leaf_page_->GetSize()-1){

    Page *next_page = bpm_->FetchPage(curr_leaf_page_->GetNextPageId());
    bpm_->UnpinPage(curr_page_->GetPageId(), false);

    curr_page_ = next_page;
    curr_leaf_page_ = reinterpret_cast<LeafPage *>(next_page);
    curr_idx_ =0;
  }else{
    curr_idx_++;
  }

  return *this;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator==(const IndexIterator &itr) const -> bool{
  return curr_leaf_page_->GetPageId() == itr.curr_leaf_page_->GetPageId() && curr_idx_ == itr.curr_idx_;
}

INDEX_TEMPLATE_ARGUMENTS
auto INDEXITERATOR_TYPE::operator!=(const IndexIterator &itr) const -> bool{
  return !(*this==itr);
}

template class IndexIterator<GenericKey<4>, RID, GenericComparator<4>>;

template class IndexIterator<GenericKey<8>, RID, GenericComparator<8>>;

template class IndexIterator<GenericKey<16>, RID, GenericComparator<16>>;

template class IndexIterator<GenericKey<32>, RID, GenericComparator<32>>;

template class IndexIterator<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
