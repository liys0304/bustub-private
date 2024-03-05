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

#include "buffer/lru_replacer.h"
#include "common/config.h"

namespace bustub {

LRUReplacer::LRUReplacer(size_t num_pages) { max_page_num_ = static_cast<int>(num_pages); }  //父类和子类之间转化

LRUReplacer::~LRUReplacer() = default;

bool LRUReplacer::Victim(frame_id_t *frame_id) {
  mtx_.lock();               //访问之前上锁
  auto vic = list_.front();  //队头元素准备出队
  list_.pop_front();
  map_.erase(map_.find(vic));  //出队后将哈希表中的页删除
  if (frame_id != nullptr) {
    *frame_id = vic;
  }

  mtx_.unlock();
  return true;
}

void LRUReplacer::Pin(frame_id_t frame_id) {
  mtx_.lock();
  if (map_.find(frame_id) != map_.end()) {
    list_.erase(map_[frame_id]);
    map_.erase(frame_id);
  }

  mtx_.unlock();
}

void LRUReplacer::Unpin(frame_id_t frame_id) {
  mtx_.lock();
  if (map_.find(frame_id) != map_.end()) {
    mtx_.unlock();
    return;
  }
  if (static_cast<int>(list_.size()) == max_page_num_) {
    frame_id_t *vic = nullptr;
    if (!Victim(vic)) {
      mtx_.unlock();
      return;
    }
  }
  list_.emplace_back(frame_id);
  map_[frame_id] = --list_.end();
  mtx_.unlock();
}

size_t LRUReplacer::Size() { return list_.size(); }

}  // namespace bustub
