//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// hash_table_header_page.cpp
//
// Identification: src/storage/page/hash_table_header_page.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include "storage/page/hash_table_directory_page.h"
#include <algorithm>
#include <cstdint>
#include <unordered_map>
#include "common/logger.h"

namespace bustub {
page_id_t HashTableDirectoryPage::GetPageId() const { return page_id_; } //获取当前页目录号

void HashTableDirectoryPage::SetPageId(bustub::page_id_t page_id) { page_id_ = page_id; } //设置当前页目录号

lsn_t HashTableDirectoryPage::GetLSN() const { return lsn_; } //返回日志序列号

void HashTableDirectoryPage::SetLSN(lsn_t lsn) { lsn_ = lsn; } //设置日志号

uint32_t HashTableDirectoryPage::GetGlobalDepth() { return global_depth_; } //获得全局页目录表深度
/*global_depth用于确定这个key对应的directory_index在哪
就是取这个key经过哈希之后的低global_depth位来判断directory_index
比如当前要插入的key哈希之后是00110，global_depth=3
那么低三位就是110,directory_index = 6*/

uint32_t HashTableDirectoryPage::GetGlobalDepthMask() { return (1 << global_depth_) - 1; } 
//由上述，掩码就是1左移global_depth位后-1, 根据上例此时掩码为111

void HashTableDirectoryPage::IncrGlobalDepth() {
  //增加页目录表容量，把扩容前的桶分布情况拷贝一份到扩容后的位置
  for(int i = static_cast<int>(Size() - 1); i >= 0; i--) {
    bucket_page_ids_[i + Size()] = bucket_page_ids_[i];
    local_depths_[i + Size()] = local_depths_[i];
  }
  global_depth_++;
}

void HashTableDirectoryPage::DecrGlobalDepth() { global_depth_--; }

page_id_t HashTableDirectoryPage::GetBucketPageId(uint32_t bucket_idx) { return bucket_page_ids_[bucket_idx]; }

void HashTableDirectoryPage::SetBucketPageId(uint32_t bucket_idx, page_id_t bucket_page_id) {
  bucket_page_ids_[bucket_idx] = bucket_page_id;
}

//global_depth有多少位目录长度就是2的多少次方
uint32_t HashTableDirectoryPage::Size() { return 1 << global_depth_; }

//检查页目录表是否能收缩，即有没有一个局部深度比全局深度大
bool HashTableDirectoryPage::CanShrink() { 
  for(int i = static_cast<int>(Size() - 1); i >= 0; i--) {
    if(local_depths_[i] >= global_depth_) {
      return false;
    }
  }
  return true;
}

/*local_depth不和directory_index相关，而是和桶相关，根据local_depth和global_depth的关系来决定发生桶溢出*/
uint32_t HashTableDirectoryPage::GetLocalDepth(uint32_t bucket_idx) { return local_depths_[bucket_idx]; }

void HashTableDirectoryPage::SetLocalDepth(uint32_t bucket_idx, uint8_t local_depth) {
  local_depths_[bucket_idx] = local_depth;
}

void HashTableDirectoryPage::IncrLocalDepth(uint32_t bucket_idx) { ++local_depths_[bucket_idx]; }

void HashTableDirectoryPage::DecrLocalDepth(uint32_t bucket_idx) { --local_depths_[bucket_idx]; }

uint32_t HashTableDirectoryPage::GetLocalHighBit(uint32_t bucket_idx) {
  return bucket_idx & ~((1 << GetLocalDepth(bucket_idx)) - 1);
}

/*桶分裂之后分裂出来的bucket_page的directory_index，只要将当前桶的directory_index中
local_depth对应的位取反，例如directory_index = 001, local_depth = 2
那么split_img_index就是001*/
uint32_t HashTableDirectoryPage::GetSplitImageIndex(uint32_t bucket_idx) {
  return (bucket_idx * 2 < Size()) ? (bucket_idx + Size() / 2) : (bucket_idx - Size() / 2);
}

/**
 * VerifyIntegrity - Use this for debugging but **DO NOT CHANGE**
 *
 * If you want to make changes to this, make a new function and extend it.
 *
 * Verify the following invariants:
 * (1) All LD <= GD.
 * (2) Each bucket has precisely 2^(GD - LD) pointers pointing to it.
 * (3) The LD is the same at each index with the same bucket_page_id
 */
void HashTableDirectoryPage::VerifyIntegrity() {
  //  build maps of {bucket_page_id : pointer_count} and {bucket_page_id : local_depth}
  std::unordered_map<page_id_t, uint32_t> page_id_to_count = std::unordered_map<page_id_t, uint32_t>();
  std::unordered_map<page_id_t, uint32_t> page_id_to_ld = std::unordered_map<page_id_t, uint32_t>();

  //  verify for each bucket_page_id, pointer
  for (uint32_t curr_idx = 0; curr_idx < Size(); curr_idx++) {
    page_id_t curr_page_id = bucket_page_ids_[curr_idx];
    uint32_t curr_ld = local_depths_[curr_idx];
    assert(curr_ld <= global_depth_);

    ++page_id_to_count[curr_page_id];

    if (page_id_to_ld.count(curr_page_id) > 0 && curr_ld != page_id_to_ld[curr_page_id]) {
      uint32_t old_ld = page_id_to_ld[curr_page_id];
      LOG_WARN("Verify Integrity: curr_local_depth: %u, old_local_depth %u, for page_id: %u", curr_ld, old_ld,
               curr_page_id);
      PrintDirectory();
      assert(curr_ld == page_id_to_ld[curr_page_id]);
    } else {
      page_id_to_ld[curr_page_id] = curr_ld;
    }
  }

  auto it = page_id_to_count.begin();

  while (it != page_id_to_count.end()) {
    page_id_t curr_page_id = it->first;
    uint32_t curr_count = it->second;
    uint32_t curr_ld = page_id_to_ld[curr_page_id];
    uint32_t required_count = 0x1 << (global_depth_ - curr_ld);

    if (curr_count != required_count) {
      LOG_WARN("Verify Integrity: curr_count: %u, required_count %u, for page_id: %u", curr_ld, required_count,
               curr_page_id);
      PrintDirectory();
      assert(curr_count == required_count);
    }
    it++;
  }
}

void HashTableDirectoryPage::PrintDirectory() {
  LOG_DEBUG("======== DIRECTORY (global_depth_: %u) ========", global_depth_);
  LOG_DEBUG("| bucket_idx | page_id | local_depth |");
  for (uint32_t idx = 0; idx < static_cast<uint32_t>(0x1 << global_depth_); idx++) {
    LOG_DEBUG("|      %u     |     %u     |     %u     |", idx, bucket_page_ids_[idx], local_depths_[idx]);
  }
  LOG_DEBUG("================ END DIRECTORY ================");
}

}  // namespace bustub
