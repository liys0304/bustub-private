//===----------------------------------------------------------------------===//
//
//                         BusTub
//
// extendible_hash_table.cpp
//
// Identification: src/container/hash/extendible_hash_table.cpp
//
// Copyright (c) 2015-2021, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <cstddef>
#include <cstdint>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#include "common/config.h"
#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "concurrency/transaction.h"
#include "container/hash/extendible_hash_table.h"
#include "storage/page/hash_table_directory_page.h"
#include "storage/page/hash_table_page_defs.h"
#include "type/numeric_type.h"
#include "type/value.h"

namespace bustub {

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_TYPE::ExtendibleHashTable(const std::string &name, BufferPoolManager *buffer_pool_manager,
                                     const KeyComparator &comparator, HashFunction<KeyType> hash_fn)
    : buffer_pool_manager_(buffer_pool_manager), comparator_(comparator), hash_fn_(std::move(hash_fn)) {
  //  implement me!
  //创建第一个目录页和第一个桶
  //将新创建的Page对象转换成directory_page或者bucket_page
  auto directory_page = 
      reinterpret_cast<HashTableDirectoryPage *>(buffer_pool_manager->NewPage(&directory_page_id_, nullptr)->GetData());
      directory_page->SetPageId(directory_page_id_);
      page_id_t buckect_page_id = INVALID_PAGE_ID;
      buffer_pool_manager->NewPage(&buckect_page_id, nullptr);
      directory_page->SetBucketPageId(0, buckect_page_id);
      //注意创建之后的对象pin_count默认为1,此时需要Unpin
      buffer_pool_manager->UnpinPage(directory_page_id_, true, nullptr);
      buffer_pool_manager->UnpinPage(buckect_page_id, true, nullptr);
}

/*****************************************************************************
 * HELPERS
 *****************************************************************************/
/**
 * Hash - simple helper to downcast MurmurHash's 64-bit hash to 32-bit
 * for extendible hashing.
 *
 * @param key the key to hash
 * @return the downcasted 32-bit hash
 */
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::Hash(KeyType key) {
  return static_cast<uint32_t>(hash_fn_.GetHash(key));
}

//将哈希值映射到页目录索引上
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToDirectoryIndex(KeyType key, HashTableDirectoryPage *dir_page) {
  return Hash(key) & dir_page->GetGlobalDepthMask();
}

//通过页目录索引找到关键字对应的页号
template <typename KeyType, typename ValueType, typename KeyComparator>
inline uint32_t HASH_TABLE_TYPE::KeyToPageId(KeyType key, HashTableDirectoryPage *dir_page) {
  return dir_page->GetBucketPageId(KeyToDirectoryIndex(key, dir_page));
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HashTableDirectoryPage *HASH_TABLE_TYPE::FetchDirectoryPage() {
  return reinterpret_cast<HashTableDirectoryPage *>(
        buffer_pool_manager_->FetchPage(directory_page_id_, nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchBucketPage(page_id_t bucket_page_id) {
  return reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(
      buffer_pool_manager_->FetchPage(bucket_page_id, nullptr)->GetData());
}

template <typename KeyType, typename ValueType, typename KeyComparator>
HASH_TABLE_BUCKET_TYPE *HASH_TABLE_TYPE::FetchPageByKey(KeyType key) {
  auto ans = FetchBucketPage(KeyToPageId(key, FetchDirectoryPage()));
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  return ans;
}

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::GetValue(Transaction *transaction, const KeyType &key, std::vector<ValueType> *result) {
  table_latch_.RLock();
  auto dpg = FetchDirectoryPage(); //目录页
  auto bpg = FetchPageByKey(key); //通过关键字找到桶
  bool ans = bpg->GetValue(key, comparator_, result);
  buffer_pool_manager_->UnpinPage(KeyToPageId(key, dpg), false, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.RUnlock();
  return ans;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Insert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto bpg = FetchPageByKey(key);
  bool ans = bpg->Insert(key, value, comparator_);
  page_id_t bpg_page_id = KeyToPageId(key, FetchDirectoryPage());//插入的桶号
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.WUnlock();
  if (!ans) { //先释放锁，判断插入不成功时是因为需要分裂还是键值对已经存在
    std::vector<ValueType> res;
    bpg->GetValue(key, comparator_, &res); //保存key对应的所有结果
    auto it = find(res.begin(), res.end(), value); //遍历查找键值对是否已经存在
    if(it == res.end()) { //不存在，此时需要分裂
      ans = SplitInsert(transaction, key, value);
    }
  }
  buffer_pool_manager_->UnpinPage(bpg_page_id, true, nullptr);
  return ans;
}

template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::SplitInsert(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dpg = FetchDirectoryPage(); //先将页目录和key所在的桶拉取出来
  table_latch_.WLock();
  auto bpg = FetchPageByKey(key);
  auto kti = KeyToDirectoryIndex(key, dpg); //key所在桶的页目录号
  auto bpg_page_id = dpg->GetBucketPageId(kti); //获得该桶的页号
  if (dpg->GetGlobalDepth() == dpg->GetLocalDepth(kti)) { //如果全局深度=局部深度则说明需要扩容目录，检查此时目录是否已经无法扩容
    if (1 << dpg->GetGlobalDepth() != DIRECTORY_ARRAY_SIZE) {
      dpg->IncrGlobalDepth();
    } else {
      table_latch_.WUnlock();
      return false; //目录已经满
    }
  }
  //若不是则说明只需要分裂桶
  page_id_t bucket_page_id = INVALID_PAGE_ID;
  Page *tmp = buffer_pool_manager_->NewPage(&bucket_page_id, nullptr); //为需要分裂的桶创建一个新的页面
  auto bucket_page = reinterpret_cast<HashTableBucketPage<KeyType, ValueType, KeyComparator> *>(tmp->GetData());

  //添加指针到镜像页
  //重新散列原先桶中的数据，并把局部深度+1
  size_t common_bits = kti % (1 << dpg->GetLocalDepth(kti));
  size_t ld = dpg->GetLocalDepth(kti);
  for (size_t i = common_bits; i < dpg->Size(); i += (1 << ld)) {
    if(((i >> ld) & 1) != ((kti >> ld) & 1)) {
      dpg->SetBucketPageId(i, bucket_page_id);
    }
    dpg->IncrLocalDepth(i);
  }
  
  for(size_t i = 0; i < BUCKET_ARRAY_SIZE; i++) {
    if(bpg->IsOccupied(i)) {
      if(static_cast<page_id_t>(KeyToPageId(bpg->KeyAt(i), dpg)) == bucket_page_id) {
        bucket_page->SetPair(bpg->KeyAt(i), bpg->ValueAt(i), i);
        bpg->DeleteAt(i);
      }
    }
  }
  buffer_pool_manager_->UnpinPage(bpg_page_id, true, nullptr);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
  buffer_pool_manager_->UnpinPage(bucket_page_id, true, nullptr);
  table_latch_.WUnlock();
  bool ans = Insert(transaction, key, value);
  return ans;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::Remove(Transaction *transaction, const KeyType &key, const ValueType &value) {
  table_latch_.WLock();
  auto bpg = FetchPageByKey(key);
  auto dpg = FetchDirectoryPage();
  bool ans = bpg->Remove(key, value, comparator_);
  page_id_t bpg_page_id = KeyToPageId(key, dpg);
  if (ans && bpg->IsEmpty()) {
    //注意先unpin再删除
    buffer_pool_manager_->UnpinPage(bpg_page_id, true, nullptr);
    Merge(transaction, key, value);
  } else {
    buffer_pool_manager_->UnpinPage(bpg_page_id, true, nullptr);
  }
  buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr);
  table_latch_.WUnlock();
  return ans;
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::Merge(Transaction *transaction, const KeyType &key, const ValueType &value) {
  auto dpg = FetchDirectoryPage();
  auto kti = KeyToDirectoryIndex(key, dpg);
  MergeMain(dpg, kti);
  buffer_pool_manager_->UnpinPage(directory_page_id_, true, nullptr);
}

template<typename KeyType, typename ValueType, typename KeyComparator>
bool HASH_TABLE_TYPE::MergeMain(HashTableDirectoryPage *dpg, uint32_t kti) {
  auto bpg_page_id = dpg->GetBucketPageId(kti);
  size_t image_index = dpg->GetSplitImageIndex(kti);
  if (dpg->GetLocalDepth(kti) != 0 && dpg->GetLocalDepth(kti) == dpg->GetLocalDepth(image_index) &&
      dpg->GetBucketPageId(kti) != dpg->GetBucketPageId(image_index)) {
    size_t ld = dpg->GetLocalDepth(kti) - 1;
    size_t common_bits = image_index % (1 << ld);
    for(size_t i = common_bits; i < dpg->Size(); i += (1 << ld)) {
      if(((i >> ld) & 1) == ((kti >> ld) & 1)) {
        dpg->SetBucketPageId(i, dpg->GetBucketPageId(image_index));
      }
      dpg->DecrLocalDepth(i);
    }
    buffer_pool_manager_->DeletePage(bpg_page_id, nullptr);
    if(dpg->CanShrink()) {
      dpg->DecrGlobalDepth();
      for (size_t i = dpg->Size() - 1; i >= 0 && i < dpg->Size(); --i) {
        auto bucket_page = reinterpret_cast<HASH_TABLE_BUCKET_TYPE *>(
            buffer_pool_manager_->FetchPage(dpg->GetBucketPageId(i), nullptr)->GetData());
        if (bucket_page->IsEmpty()) {
          buffer_pool_manager_->UnpinPage(dpg->GetBucketPageId(i), false);
          MergeMain(dpg, i);
        } else {
          buffer_pool_manager_->UnpinPage(dpg->GetBucketPageId(i), false);
        }
      }
    }
    return true;
  }
  return false;
} 

/*****************************************************************************
 * GETGLOBALDEPTH - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
uint32_t HASH_TABLE_TYPE::GetGlobalDepth() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  uint32_t global_depth = dir_page->GetGlobalDepth();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
  return global_depth;
}

/*****************************************************************************
 * VERIFY INTEGRITY - DO NOT TOUCH
 *****************************************************************************/
template <typename KeyType, typename ValueType, typename KeyComparator>
void HASH_TABLE_TYPE::VerifyIntegrity() {
  table_latch_.RLock();
  HashTableDirectoryPage *dir_page = FetchDirectoryPage();
  dir_page->VerifyIntegrity();
  assert(buffer_pool_manager_->UnpinPage(directory_page_id_, false, nullptr));
  table_latch_.RUnlock();
}

/*****************************************************************************
 * TEMPLATE DEFINITIONS - DO NOT TOUCH
 *****************************************************************************/
template class ExtendibleHashTable<int, int, IntComparator>;

template class ExtendibleHashTable<GenericKey<4>, RID, GenericComparator<4>>;
template class ExtendibleHashTable<GenericKey<8>, RID, GenericComparator<8>>;
template class ExtendibleHashTable<GenericKey<16>, RID, GenericComparator<16>>;
template class ExtendibleHashTable<GenericKey<32>, RID, GenericComparator<32>>;
template class ExtendibleHashTable<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
