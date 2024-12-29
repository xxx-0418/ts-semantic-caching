#ifndef STSCACHE_ITEM_H
#define STSCACHE_ITEM_H
#include <bits/types.h>
#include <cstdint>

#include "common/macros.h"

namespace SemanticCache {

struct Slabitem {
  uint64_t timestamp_;
  uint8_t end_[1];
};

#define ITEM_MAGIC 0xfeedface
#define ITEM_HDR_SIZE offsetof(struct Slabitem, end_)

#define ITEM_MIN_PAYLOAD_SIZE (sizeof("k") + sizeof(uint64_t))
#define ITEM_MIN_CHUNK_SIZE FC_ALIGN(ITEM_HDR_SIZE + ITEM_MIN_PAYLOAD_SIZE, FC_ALIGNMENT)

#define ITEM_PAYLOAD_SIZE 32
#define ITEM_CHUNK_SIZE FC_ALIGN(ITEM_HDR_SIZE + ITEM_PAYLOAD_SIZE, FC_ALIGNMENT)

}
#endif
