#ifndef INSOMNIA_LRUK_CACHE_H
#define INSOMNIA_LRUK_CACHE_H

#include "buffer_types.h"

namespace insomnia::buffer {

namespace cntr = container;

// Ya know, a linked hashmap might be a better implementation.
// But I'm too lazy to implement it.
// So take this plain version.
template <size_t capacity, size_t k>
class LruKReplacer {
public:

private:
  struct FrameInfo {
    cntr::vector<TimestampId> history;
    bool is_evictable = true;
  };
  TimestampId _curtime;
  cntr::vector<FrameInfo> _frames;

};

}

#endif