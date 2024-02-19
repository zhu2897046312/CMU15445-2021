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

#include "common/macros.h"

namespace bustub {

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, DiskManager *disk_manager,
                                                     LogManager *log_manager)
    : BufferPoolManagerInstance(pool_size, 1, 0, disk_manager, log_manager) {}

BufferPoolManagerInstance::BufferPoolManagerInstance(size_t pool_size, uint32_t num_instances, uint32_t instance_index,
                                                     DiskManager *disk_manager, LogManager *log_manager)
    : pool_size_(pool_size),
      num_instances_(num_instances),
      instance_index_(instance_index),
      next_page_id_(instance_index),
      disk_manager_(disk_manager),
      log_manager_(log_manager) {
  BUSTUB_ASSERT(num_instances > 0, "If BPI is not part of a pool, then the pool size should just be 1");
  BUSTUB_ASSERT(
      instance_index < num_instances,
      "BPI index cannot be greater than the number of BPIs in the pool. In non-parallel case, index should just be 1.");
  // We allocate a consecutive memory space for the buffer pool.
  pages_ = new Page[pool_size_];
  replacer_ = new LRUReplacer(pool_size);

  // Initially, every page is in the free list.
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_.emplace_back(static_cast<int>(i));
  }
}

BufferPoolManagerInstance::~BufferPoolManagerInstance() {
  delete[] pages_;
  delete replacer_;
}

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) {
  // Make sure you call DiskManager::WritePage!
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::scoped_lock lock(latch_);
//1.找
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }
//2.写入磁盘
  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  disk_manager_->WritePage(page_id, page->GetData());
  page->is_dirty_ = false;
  return true;
}
 // 功能：将所有在缓冲池中的页写回磁盘
void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  std::scoped_lock lock(latch_);
  for (auto [page_id, frame_id] : page_table_) {
    Page *page = pages_ + frame_id;
    disk_manager_->WritePage(page_id, page->GetData());
    page->is_dirty_ = false;
  }
}
// 功能：分配一个新的物理页，此时bufferpool为空
Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  std::scoped_lock lock(latch_);
  frame_id_t frame_id = 0;
// 2. 从空闲列表或替换器中选择一个受害者页面P。始终首先从空闲列表中选择。
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Size() != 0) {
    replacer_->Victim(&frame_id);
  } else {
//1.无可用页面
    return nullptr;
  }

  Page *page = pages_ + frame_id;
  *page_id = AllocatePage();
// 3. 更新P的元数据，清零内存并将P添加到页面表中。
  if (auto iter = page_table_.find(page->page_id_); iter != page_table_.end()) {
    page_table_.erase(iter);
  }
  page_table_[*page_id] = frame_id;

  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
  }
// 4. 设置页面ID输出参数。返回指向P的指针。
  page->ResetMemory();
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->page_id_ = *page_id;
  replacer_->Pin(frame_id);

  return page;
}
// 功能：将页面从磁盘加载到内存
Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  if (page_id == INVALID_PAGE_ID) {
	//无效页面id
	 return nullptr;
  }

  std::scoped_lock lock(latch_);
  frame_id_t frame_id = 0;
//1. 查找请求页面(P),在页表中的位置
  if (auto iter = page_table_.find(page_id); iter != page_table_.end()) {
    frame_id = iter->second;
//1.1 如果P存在,则立即将其固定并返回
    Page *page = pages_ + frame_id;
    page->pin_count_++;
    if (page->pin_count_ == 1) {
      replacer_->Pin(frame_id);
    }
    return page;
  }
// 1.2 如果P不存在，则从空闲列表或替换器中找到替换页面（R）。
	// 注意页面总是首先从空闲列表中找到。
  if (!free_list_.empty()) {
    frame_id = free_list_.back();
    free_list_.pop_back();
  } else if (replacer_->Size() != 0) {
	replacer_->Victim(&frame_id);
  } else {
    return nullptr;
  }

  Page *page = pages_ + frame_id;//(R)
  if (auto iter = page_table_.find(page->page_id_); iter != page_table_.end()) {
// 3. 删除页表中的R并插入P
    page_table_.erase(iter);
  }
	// 更新页表,将P插入其中
  page_table_[page_id] = frame_id;
// 2. 如果R是脏页,则将其写回磁盘
  if (page->IsDirty()) {
    disk_manager_->WritePage(page->page_id_, page->GetData());
  }
// 4. 更新P的元数据,从磁盘读入页面内容,然后返回指向P的指针
  disk_manager_->ReadPage(page_id, page->GetData());
  page->is_dirty_ = false;
  page->pin_count_ = 1;
  page->page_id_ = page_id;
  replacer_->Pin(frame_id);

  return page;
}
 // 功能：将page_id从缓冲池中移除，并写回磁盘
bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  if (page_id == INVALID_PAGE_ID) {
    return true;
  }

  std::scoped_lock lock(latch_);
//1.从页表找到(P)
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return true;
  }
//2.页表中存在(P)
  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  if (page->pin_count_ != 0) {
    return false;
  }
//3.可以delet
  page_table_.erase(iter);
  free_list_.emplace_back(frame_id);
	//脏页,重新写回磁盘
  if (page->IsDirty()) {
    disk_manager_->WritePage(page_id, page->GetData());
  }
//3.重置
  page->is_dirty_ = false;
  page->pin_count_ = 0;
  page->page_id_ = INVALID_PAGE_ID;
  DeallocatePage(page->page_id_);

  return true;
}
// 功能：如果进程已经完成了对这个页的操作。我们需要unpin操作
bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  if (page_id == INVALID_PAGE_ID) {
    return false;
  }

  std::scoped_lock lock(latch_);
//1.找
  auto iter = page_table_.find(page_id);
  if (iter == page_table_.end()) {
    return false;
  }

  frame_id_t frame_id = iter->second;
  Page *page = pages_ + frame_id;
  if (page->pin_count_ == 0) {
    return false;
  }
  page->pin_count_--;
  page->is_dirty_ = page->is_dirty_ || is_dirty;
//2.unpin
  if (page->pin_count_ == 0) {
    replacer_->Unpin(frame_id);
  }
  return true;
 }

page_id_t BufferPoolManagerInstance::AllocatePage() {
  const page_id_t next_page_id = next_page_id_;
  next_page_id_ += num_instances_;
  ValidatePageId(next_page_id);
  return next_page_id;
}

void BufferPoolManagerInstance::ValidatePageId(const page_id_t page_id) const {
  assert(page_id % num_instances_ == instance_index_);  // allocated pages mod back to this BPI
}

}  // namespace bustub
