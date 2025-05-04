#include "lru_k_replacer.h"

namespace insomnia::policy {

void LruKReplacer::access(index_t index) {
  {
    std::unique_lock lock(obscure_latch_);
    if(auto it = obscure_list_.find(index); it != obscure_list_.end()) {
      it->second.access(timestamp_++);
      if(it->second.heated()) {
        {
          std::unique_lock lock2(hotspot_latch_);
          hotspot_list_.emplace(index, std::move(it->second));
        }
        obscure_list_.erase(it);
      }
      return;
    }
  }
  {
    std::unique_lock lock(hotspot_latch_);
    if(auto it = hotspot_list_.find(index); it != hotspot_list_.end()) {
      it->second.access(timestamp_++);
      return;
    }
  }
  std::unique_lock lock(obscure_latch_);
  Slot slot(k_);
  slot.access(timestamp_++);
  obscure_list_.emplace(index, std::move(slot));
}

LruKReplacer::index_t LruKReplacer::evict() {
  {
    index_t result = npos;
    std::unique_lock lock(obscure_latch_);
    size_t earliest_timestamp = TIME_T_MAX;
    for(auto &[index, node] : obscure_list_) {
      if(!node.evictable)
        continue;
      if(earliest_timestamp > node.k_dist()) {
        result = index;
        earliest_timestamp = node.k_dist();
      }
    }
    if(result != npos) {
      obscure_list_.erase(result);
      size_--;
      return result;
    }
  }
  {
    index_t result = npos;
    std::unique_lock lock(hotspot_latch_);
    size_t earliest_timestamp = TIME_T_MAX;
    for(auto &[index, node] : hotspot_list_) {
      if(!node.evictable)
        continue;
      if(earliest_timestamp > node.k_dist()) {
        result = index;
        earliest_timestamp = node.k_dist();
      }
    }
    if(result != npos) {
      hotspot_list_.erase(result);
      size_--;
      return result;
    }
  }
  return npos;
}

bool LruKReplacer::remove(index_t index) {
  {
    std::unique_lock lock(obscure_latch_);
    if (auto it = obscure_list_.find(index); it != obscure_list_.end()) {
      if (!it->second.evictable)
        return false;
      obscure_list_.erase(it);
      size_--;
      return true;
    }
  }
  {
    std::unique_lock lock(hotspot_latch_);
    if (auto it = hotspot_list_.find(index); it != hotspot_list_.end()) {
      if (!it->second.evictable)
        return false;
      hotspot_list_.erase(it);
      size_--;
      return true;
    }
  }
  return false;
}

void LruKReplacer::pin(index_t index) {
  {
    std::unique_lock lock(obscure_latch_);
    if (auto it = obscure_list_.find(index); it != obscure_list_.end()) {
      if (it->second.evictable) {
        it->second.evictable = false;
        size_--;
      }
      return;
    }
  }
  {
    std::unique_lock lock(hotspot_latch_);
    if (auto it = hotspot_list_.find(index); it != hotspot_list_.end()) {
      if (it->second.evictable) {
        it->second.evictable = false;
        size_--;
      }
      return;
    }
  }
}

void LruKReplacer::unpin(index_t index) {
  {
    std::unique_lock lock(obscure_latch_);
    if (auto it = obscure_list_.find(index); it != obscure_list_.end()) {
      if (!it->second.evictable) {
        it->second.evictable = true;
        size_++;
      }
      return;
    }
  }
  {
    std::unique_lock lock(hotspot_latch_);
    if (auto it = hotspot_list_.find(index); it != hotspot_list_.end()) {
      if (!it->second.evictable) {
        it->second.evictable = true;
        size_++;
      }
      return;
    }
  }
}



}