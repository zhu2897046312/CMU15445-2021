//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// lru_replacer.h
//
// Identification: src/include/buffer/lru_replacer.h
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#pragma once

#include <list>
#include <mutex>  // NOLINT
#include <vector>
#include <unordered_map>


#include <shared_mutex>  // NOLINT
#include <utility>

#include "buffer/replacer.h"
#include "common/config.h"

namespace bustub {

/**
 * LRUReplacer implements the Least Recently Used replacement policy.
 */
class LRUReplacer : public Replacer {
 public:
  /**
   * Create a new LRUReplacer.
   * @param num_pages the maximum number of pages the LRUReplacer will be required to store
   */
  explicit LRUReplacer(size_t num_pages);

  /**
   * Destroys the LRUReplacer.
   */
  ~LRUReplacer() override;

  bool Victim(frame_id_t *frame_id) override;

  void Pin(frame_id_t frame_id) override;

  void Unpin(frame_id_t frame_id) override;

  size_t Size() override;

 /*private:
  // TODO(student): implement me!
  std::shared_mutex r_mu_;
  std::mutex w_mu_;
  std::vector<bool> iter_valid_;
  std::vector<std::list<frame_id_t>::const_iterator> iters_;
  std::list<frame_id_t> unpinned_pages_;
*/
private:
  size_t capacity;      //容量
  size_t size;          //当前大小
  std::list<frame_id_t> lists;           //存储帧的双向链表
  std::unordered_map<frame_id_t , int> maps;      //哈希映射
  std::mutex mtx;
};

}  // namespace bustub
