#ifndef INSOMNIA_LRU_K_REPLACER_H
#define INSOMNIA_LRU_K_REPLACER_H

#include <mutex>

#include "index_pool.h"
#include "unordered_map.h"

namespace insomnia {

class LruKReplacer {
public:
  using index_t = IndexPool::index_t;
  using timestamp_t = size_t;
  static constexpr timestamp_t TIME_T_MAX = SIZE_MAX;

public:
  LruKReplacer(size_t k, size_t capacity)
    : k_(k), capacity_(capacity), npos(capacity) {}


  /**
   * @brief evict a frame/... and erase its attendence in the replacer.
   * @attention better call has_evictable_frame() before eviction to ensure the validity of eviction.
   * @return the index of the evicted one. If eviction failed (all pages are pinned), return npos.
   */
  index_t evict();
  /**
   * @brief forcefully remove a frame. Fails if index not exist or frame is unevictable.
   * @param index
   * @return whether the remove succeeds.
   */
  bool remove(index_t index);
  void access(index_t index); // initially non-evictable
  void unpin(index_t index);
  void pin(index_t index);
  size_t evictable_cnt() const { return size_; }
  bool has_evictable_frame() const { return size_ != 0; }

  index_t npos;

private:
  enum class SlotStat { Invalid, Obscure, Hotspot };
  class Slot {
  private:
    size_t k_;
    timestamp_t *history;
    size_t access_num{0};

  public:
    bool evictable{false};

    explicit Slot(size_t k) : k_(k) { history = new timestamp_t[k]; }
    Slot(Slot &&other) : k_(other.k_) {
      history = other.history;
      access_num = other.access_num;
      evictable = other.evictable;
      other.history = nullptr;
    }
    Slot& operator=(Slot &&other) {
      if(this == &other) return *this;
      delete[] history;
      k_ = other.k_;
      history = other.history;
      access_num = other.access_num;
      evictable = other.evictable;
      other.history = nullptr;
      return *this;
    }
    ~Slot() { delete[] history; }
    void clear() {
      access_num = 0;
      evictable = false;
    }
    size_t k_dist() const {
      if(heated())
        return history[access_num % k_];
      return history[0];
    }
    void access(timestamp_t timestamp) {
      history[(access_num++) % k_] = timestamp;
    }
    bool heated() const {
      return access_num >= k_;
    }
  };

  const size_t k_, capacity_;
  unordered_map<index_t, Slot> hotspot_list_, obscure_list_;
  size_t size_{0};
  timestamp_t timestamp_{0};
  std::mutex hotspot_latch_, obscure_latch_;
};

}

#endif