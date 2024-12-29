#ifndef STSCACHE_SLAB_H
#define STSCACHE_SLAB_H
#include <bits/types.h>
#include <fcntl.h>
#include <linux/fs.h>
#include <sys/cdefs.h>
#include <sys/ioctl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include "common/macros.h"

namespace SemanticCache {
struct Slab {
  uint32_t sid_;
  uint8_t cid_;
  uint8_t data_[1];
};

#define SLAB_MAGIC 0xdeadbeef
#define SLAB_HDR_SIZE offsetof(struct SemanticCache::Slab, data_)
#define SLAB_MIN_SIZE ((size_t)MB)
#define SLAB_SIZE ((size_t)(0.125 * 0.25*0.5 * MB))
#define SLAB_MAX_SIZE ((size_t)(512 * MB))

#define SLAB_INVALID_SID ((1UL << 32) - 1)
#define SLAB_MAX_SID (SLAB_INVALID_SID - 1)

struct Itembound {
  uint64_t timestamp_;
  uint32_t addr_;
};

struct Slabinfo {
  uint64_t time_start_;
  uint64_t time_end_;
  uint32_t sid_;
  uint32_t addr_;
  TAILQ_ENTRY(Slabinfo) tqe_;
  uint32_t nalloc_;
  uint8_t cid_;
  unsigned mem_ : 1;
  unsigned free_ : 1;
  uint32_t nbound_ = 0;
  Itembound* bound_ = nullptr;
};

TAILQ_HEAD(Slabhinfo, Slabinfo);

struct Slabclass {
  uint32_t nitem_;
  size_t size_;
};

#define SLABCLASS_MIN_ID 0
#define SLABCLASS_MAX_ID (UCHAR_MAX - 1)
#define SLABCLASS_INVALID_ID UCHAR_MAX
#define SLABCLASS_MAX_IDS UCHAR_MAX


}
#endif
