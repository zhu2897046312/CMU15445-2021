//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.cpp
//
// Identification: src/buffer/lru_replacer.cpp
//
// Copyright (c) 2015-2019, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <algorithm>

#include "buffer/lru_replacer.h"

namespace bustub {

/*LRUReplacer::LRUReplacer(size_t num_pages) {
    iters_.resize(num_pages);
    iter_valid_.resize(num_pages, false);
}

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) { 
    std::shared_lock rlock(r_mu_);
    //确保在选择受害者页面之前存在可替换的页面。
    if (unpinned_pages_.empty()) {
        return false;
    }
    std::scoped_lock wlock(w_mu_);
    auto iter = unpinned_pages_.begin();
    // 选择最久未使用的页面
    *frame_id = *iter;
    // 更新迭代器
    iter_valid_[*iter] = false;
    // 移除此页面
    unpinned_pages_.erase(iter);
    return true;
 }
//pin正在读取
void LRUReplacer::Pin(frame_id_t frame_id) {
    if (frame_id < 0 || static_cast<size_t>(frame_id) >= iter_valid_.size()) {
    return;
  }
  std::shared_lock rlock(r_mu_);
  // 若页面存在于替换器中，将其标记为固定状态
  if (iter_valid_[frame_id]) {
    std::scoped_lock wlock(w_mu_);
    unpinned_pages_.erase(iters_[frame_id]);
    iter_valid_[frame_id] = false;
  }
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
    if (frame_id < 0 || static_cast<size_t>(frame_id) >= iter_valid_.size()) {
    return;
  }
  std::shared_lock rlock(r_mu_);
  // 若页面不在替换器中，则添加到列表中
  if (!iter_valid_[frame_id]) {
    std::scoped_lock wlock(w_mu_);
    unpinned_pages_.push_back(frame_id);
    iters_[frame_id] = std::prev(unpinned_pages_.end());
    iter_valid_[frame_id] = true;
  }
}

size_t LRUReplacer::Size() { 
    std::shared_lock lock(r_mu_);
    return unpinned_pages_.size();
}*/
// 构造函数
 LRUReplacer::LRUReplacer(size_t num_pages) {
     capacity = num_pages;
     maps.clear();
     lists.clear();
     size = lists.size();
 }

LRUReplacer::~LRUReplacer() = default;

 // Victim
 //记得加线程的互斥锁，防止对数据造成污染
 bool LRUReplacer::Victim(frame_id_t *frame_id) {
     if (lists.empty()) return false;
     mtx.lock();
     frame_id_t last_frame = lists.back();
     maps.erase(last_frame);
     lists.pop_back();
     *frame_id = last_frame;
     mtx.unlock();
     return true;
 }
 // Pin
 void LRUReplacer::Pin(frame_id_t frame_id) {
     //删除操作：传入的是pinned的帧，从LRU replacer中删除
     if (maps.count(frame_id) == 0) return;
     mtx.lock();
     lists.remove(frame_id);
     maps.erase(frame_id);
     mtx.unlock();
 }
 // Unpin
 void LRUReplacer::Unpin(frame_id_t frame_id) {
     //插入操作: 查看是否已存在，查看是否已满，都满足则插入
     if (maps.count(frame_id) != 0) return;
     mtx.lock();
     if (lists.size() == capacity) {
         frame_id_t last_frame = lists.back();
         maps.erase(last_frame);
         lists.pop_back();
     }
     lists.push_front(frame_id);
     maps[frame_id] = 1;
     mtx.unlock();
 }
 // Size
 size_t LRUReplacer::Size() { return size = lists.size(); }

}  // namespace bustub
