/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#pragma once

#include <stdint.h>
#include <cstddef>
#include <unordered_map>
#include <mutex>

namespace oceanbase {

/**
 * @class ObLRUCache
 * @brief A thread-safe implementation of an LRU (Least Recently Used) cache.
 *
 * The `ObLRUCache` class provides a fixed-size cache that evicts the least recently used
 * entries when the cache exceeds its capacity. It supports thread-safe operations for
 * inserting, retrieving, and checking the existence of cache entries.
 *
 * @tparam KeyType The type of keys used to identify cache entries.
 * @tparam ValueType The type of values stored in the cache.
 */
template <typename KeyType, typename ValueType>
class ObLRUCache
{
public:
  /**
   * @brief Constructs an `ObLRUCache` with a specified capacity.
   *
   * @param capacity The maximum number of elements the cache can hold.
   */
  ObLRUCache(size_t capacity) : capacity_(capacity), size_(0) {
    head_ = new ObLinkNode();
    tail_ = new ObLinkNode();
    head_->next = tail_;
    tail_->prev = head_;
  }

  /**
   * @brief 析构函数，清理所有节点
   */
  ~ObLRUCache() {
    ObLinkNode *node = head_;
    while (node) {
      ObLinkNode *next = node->next;
      delete node;
      node = next;
    }
  }

  /**
   * @brief Retrieves a value from the cache using the specified key.
   *
   * This method searches for the specified key in the cache. If the key is found, the
   * corresponding value is returned and the key-value pair is moved to the front of the
   * LRU list (indicating recent use).
   *
   * @param key The key to search for in the cache.
   * @param value A reference to store the value associated with the key.
   * @return `true` if the key is found and the value is retrieved; `false` otherwise.
   */
  bool get(const KeyType &key, ValueType &value) { 
    std::lock_guard<std::mutex> lock(mutex_);
    if (!map_.contains(key)) {
      return false;
    }
    ObLinkNode *node = map_[key];
    value = node->value_;
    move_to_head(node);
    return true; 
  }

  /**
   * @brief Inserts a key-value pair into the cache.
   *
   * If the key already exists in the cache, its value is updated, and the key-value pair
   * is moved to the front of the LRU list. If the cache exceeds its capacity after insertion,
   * the least recently used entry is evicted.
   *
   * @param key The key to insert into the cache.
   * @param value The value to associate with the specified key.
   */
  void put(const KeyType &key, const ValueType &value) {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!map_.contains(key)) {
      ObLinkNode *node = new ObLinkNode(key, value);
      map_[key] = node;
      add_to_head(node);
      size_++;
      if (size_ > capacity_) {
        ObLinkNode *last_node = remove_tail();
        if (last_node != nullptr) {
          map_.erase(last_node->key_);
          delete last_node;
          size_--;
        }
      }
    }else {
      ObLinkNode *node = map_[key];
      node->value_ = value;
      move_to_head(node);
    }
  }

  /**
   * @brief Checks whether the specified key exists in the cache.
   *
   * @param key The key to check in the cache.
   * @return `true` if the key exists; `false` otherwise.
   */
  bool contains(const KeyType &key) const {
    return map_.find(key) != map_.end();
  }

private:
  /**
   * @brief 链表节点，按照操作时间连接节点
   */  
  struct ObLinkNode{
    KeyType key_;
    ValueType value_;
    ObLinkNode *prev;
    ObLinkNode *next;
    ObLinkNode() : key_(), value_(), prev(nullptr), next(nullptr) {}
    ObLinkNode(const KeyType &key, const ValueType &value)
    : key_(key), value_(value), prev(nullptr), next(nullptr) {}
  };

  /**
   * @brief The maximum number of elements the cache can hold. 可负载大小
   */
  size_t capacity_;
  // 当前存储元素个数
  size_t size_;
  // 存储数据的哈希表
  std::unordered_map<KeyType, ObLinkNode*> map_;
  // 链表头结点
  ObLinkNode *head_;
  // 链表尾节点
  ObLinkNode *tail_;
  // 保证线程安全的锁
  mutable std::mutex mutex_;

  /**
   * @brief 将指定节点移至链表开头。
   *
   * @param node 待移至链表开头的节点。
   */
  void move_to_head(ObLinkNode *node) {
    node->prev->next = node->next;
    node->next->prev = node->prev;

    node->next = head_->next;
    node->prev = head_;
    head_->next->prev = node;
    head_->next = node;
  }

  /**
   * @brief 将指定节点添加链表开头。
   *
   * @param node 待添加链表开头的节点。
   */
  void add_to_head(ObLinkNode *node) {
    node->next = head_->next;
    node->prev = head_;
    head_->next->prev = node;
    head_->next = node;
  }

  /**
   * @brief 移除最末尾的节点并返回
   *
   * @return 移除的最末尾的节点。
   */
  ObLinkNode *remove_tail() {
    ObLinkNode *node = tail_->prev;
    if (node == head_) {
      return nullptr;
    }
    node->prev->next = node->next;
    tail_->prev = node->prev;

    return node;
  }
};

/**
 * @brief Creates a new instance of `ObLRUCache` with the specified capacity.
 *
 * This factory function constructs an `ObLRUCache` instance for the specified key and
 * value types, and initializes it with the given capacity.
 *
 * @tparam Key The type of keys used to identify cache entries.
 * @tparam Value The type of values stored in the cache.
 * @param capacity The maximum number of elements the cache can hold.
 * @return A pointer to the newly created `ObLRUCache` instance.
 */
template <typename Key, typename Value>
ObLRUCache<Key, Value> *new_lru_cache(uint32_t capacity)
{
  return new ObLRUCache<Key, Value>(capacity);
}

}  // namespace oceanbase
