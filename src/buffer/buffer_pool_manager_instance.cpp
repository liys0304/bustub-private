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

#include "common/config.h"
#include "common/macros.h"
#include "storage/page/page.h"

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

bool BufferPoolManagerInstance::FlushPgImp(page_id_t page_id) { //flush函数用于写回某个页面
  // Make sure you call DiskManager::WritePage!
  latch_.lock();
  if(page_table_.find(page_id) != page_table_.end()) { //如果页表中有该页（目前在页框中）
    //在BPM中缓存池页面是用数列存储的，即*pages_
    //调用diskManager写回页面
    disk_manager_->WritePage(page_id, pages_[page_id].GetData());
    latch_.unlock();
    return true;
  }
  latch_.unlock();
  return false;
}

void BufferPoolManagerInstance::FlushAllPgsImp() {
  // You can do it!
  //页表的映射为<page_id, frame_id>
  for(auto page : page_table_) {
    FlushPgImp(page.first);
  }
}

Page *BufferPoolManagerInstance::NewPgImp(page_id_t *page_id) {
  // 0.   Make sure you call AllocatePage!
  // 1.   If all the pages in the buffer pool are pinned, return nullptr.
  // 2.   Pick a victim page P from either the free list or the replacer. Always pick from the free list first.
  // 3.   Update P's metadata, zero out memory and add P to the page table.
  // 4.   Set the page ID output parameter. Return a pointer to P.
  latch_.lock();
  Page *ret = nullptr;
  if(!(free_list_.empty() && replacer_->Size() == 0)) { //判断是否空闲页或可被逐出的页面
    page_id_t new_page_id = AllocatePage();
    frame_id_t vic_p;
    //如果空闲页非空
    if(!free_list_.empty()) {
      vic_p = free_list_.front(); //拿到空页的页框号（该函数返回的是页框号）
      free_list_.pop_front(); //链表头的页框已经被占用，删除节点
    } else { //如果没有空闲页面，但是有可以被逐出的页面
      if(!replacer_->Victim(&vic_p)) { //如果逐出失败解锁返回空指针
        latch_.unlock();
        return nullptr;
      }
      //vic_p能通过victim方法来带回被逐出的页框号
      if(pages_[vic_p].IsDirty()) {//如果该页面脏则需要写回
        disk_manager_->WritePage(pages_[vic_p].GetPageId(), pages_[vic_p].GetData());
      }
      page_table_.erase(pages_[vic_p].page_id_); //逐出
    }

    //清理页框，给新页面建立映射
    //这里pages_维护的是实际上的BP空间，page_table_是页框号到页面号的映射
    pages_[vic_p].ResetMemory();
    pages_[vic_p].is_dirty_ = false;
    pages_[vic_p].page_id_ = new_page_id;
    page_table_[new_page_id] = vic_p;
    *page_id = new_page_id;
    ret = &pages_[vic_p];
    //如果所有缓冲池中的页面都被pin了，返回空指针意味新页也需要被pin
    pages_[vic_p].pin_count_ = 1;
  }
  latch_.unlock();
  return ret;
}

Page *BufferPoolManagerInstance::FetchPgImp(page_id_t page_id) {
  // 1.     Search the page table for the requested page (P).
  // 1.1    If P exists, pin it and return it immediately.
  // 1.2    If P does not exist, find a replacement page (R) from either the free list or the replacer.
  //        Note that pages are always found from the free list first.
  // 2.     If R is dirty, write it back to the disk.
  // 3.     Delete R from the page table and insert P.
  // 4.     Update P's metadata, read in the page content from disk, and then return a pointer to P.
  latch_.lock();
  Page *ret = nullptr;
  if(page_table_.find(page_id) != page_table_.end()) { //页面已经存在就PIN一下
    replacer_->Pin(page_table_.find(page_id)->second);
    ret = &pages_[page_table_.find(page_id)->second];
    ++ret->pin_count_;
  } else if (!(free_list_.empty() && replacer_->Size() == 0)) { //如果需要读入，检查是否有位置
    frame_id_t vic_p;
    if(!free_list_.empty()) {
      vic_p = free_list_.front();
      free_list_.pop_front();
    } else {
      replacer_->Victim(&vic_p);
      if(pages_[vic_p].IsDirty()) {
        disk_manager_->WritePage(pages_[vic_p].GetPageId(), pages_[vic_p].GetData());
      }
      page_table_.erase(pages_[vic_p].GetPageId());
    }
    pages_[vic_p].ResetMemory();
    pages_[vic_p].is_dirty_ = false;
    pages_[vic_p].page_id_ = page_id;
    disk_manager_->ReadPage(page_id, pages_[vic_p].data_);
    page_table_[page_id] = vic_p;
    pages_[vic_p].page_id_ = 1;
  } else {
    for (frame_id_t i = 0; i < static_cast<int>(pool_size_); i++) {
      //LOG_INFO()
    }
  }
  latch_.unlock();
  return ret;
}

bool BufferPoolManagerInstance::DeletePgImp(page_id_t page_id) {
  // 0.   Make sure you call DeallocatePage!
  // 1.   Search the page table for the requested page (P).
  // 1.   If P does not exist, return true.
  // 2.   If P exists, but has a non-zero pin-count, return false. Someone is using the page.
  // 3.   Otherwise, P can be deleted. Remove P from the page table, reset its metadata and return it to the free list.
  
  latch_.lock();
  bool ret = true;
  if(page_table_.find(page_id) != page_table_.end()) {
    if(pages_[page_table_[page_id]].GetPinCount() == 0) {
      replacer_->Pin(page_table_[page_id]); //PIN和Victm都是将可逐出的页面删除，pin执行效率更高
      pages_[page_table_[page_id]].ResetMemory();
      pages_[page_table_[page_id]].is_dirty_ = false;
      free_list_.emplace_back(page_table_[page_id]);
      page_table_.erase(page_id);
      DeallocatePage(page_id);//这个函数的作用是将页面写回磁盘
    } else {
      ret = false;
    }
  }
  latch_.unlock();
  return ret;
}

bool BufferPoolManagerInstance::UnpinPgImp(page_id_t page_id, bool is_dirty) { 
  latch_.lock();
  bool ret = false;
  if(page_table_.find(page_id) != page_table_.end()) { //如果解锁页面在页表中
    if(pages_[page_table_[page_id]].pin_count_ <= 0) {
      //
    }
    ret = true;
    pages_[page_table_[page_id]].is_dirty_ |= is_dirty;
    --pages_[page_table_[page_id]].pin_count_;
    if(pages_[page_table_[page_id]].pin_count_ == 0) {
      replacer_->Unpin(page_table_[page_id]);
    }
  }
  latch_.unlock();
  return ret;
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
