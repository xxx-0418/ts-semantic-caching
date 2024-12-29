#ifndef STSCACHE_SETTINGS_H
#define STSCACHE_SETTINGS_H
#include <bits/types.h>
#include <cstddef>
#include <cstdint>
#include <cstdlib>

#include "common/macros.h"
#include "storage/slab_management/slab.h"

namespace SemanticCache {

#define FC_FACTOR 1.25
#define FC_SLAB_MEMORY (1 * MB)
#define FC_CHUNK_SIZE FC_ALIGNMENT*2
#define FC_SLAB_SIZE SLAB_SIZE
#define FC_SERVER_ID 0
#define FC_SERVER_N 1

class Settings {
 public:
  explicit Settings();

  double factor_;
  size_t max_slab_memory_;
  char *ssd_device_;
  size_t chunk_size_;
  size_t max_chunk_size_;
  size_t slab_size_;

  size_t profile_[SLABCLASS_MAX_IDS];
  uint8_t profile_last_id_;

  uint32_t server_id_;
  uint32_t server_n_;

  int port;
  char const *addr;

 private:
  void SetDefault();

  auto FcSetProfile() -> rstatus_t;

  auto FcGenerateProfile() -> rstatus_t;
};
}

#endif
