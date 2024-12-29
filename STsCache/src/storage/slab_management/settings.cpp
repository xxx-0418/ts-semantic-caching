#include "storage/slab_management/settings.h"
#include "common/macros.h"

namespace SemanticCache {

Settings::Settings() {
  SetDefault();
  auto status = FcSetProfile();
  ST_ASSERT(status == FC_OK, "setting set profile failed");
}

void Settings::SetDefault() {
  factor_ = FC_FACTOR;
  max_slab_memory_ = FC_SLAB_MEMORY;
  ssd_device_ = nullptr;
  chunk_size_ = FC_CHUNK_SIZE;
  slab_size_ = FC_SLAB_SIZE;
  max_chunk_size_ = slab_size_ - SLAB_HDR_SIZE;
  memset(profile_, 0, sizeof(profile_));
  profile_last_id_ = SLABCLASS_INVALID_ID;
  server_id_ = FC_SERVER_ID;
  server_n_ = FC_SERVER_N;
  port = 11211;
  addr = "";
}

auto Settings::FcSetProfile() -> rstatus_t {
  return FcGenerateProfile();
}

auto Settings::FcGenerateProfile() -> rstatus_t {
  size_t *profile = profile_;
  uint8_t id;
  size_t item_sz;
  size_t min_item_sz;
  size_t max_item_sz;

  auto slab_data_size = slab_size_ - SLAB_HDR_SIZE;

  ST_ASSERT(chunk_size_ % FC_ALIGNMENT == 0,
                "FcGenerateProfile failed: chunk_size_ \% FC_ALIGNMENT should be zero\n");
  ST_ASSERT(chunk_size_ <= slab_data_size,
                "FcGenerateProfile failed: chunk_size_ should be <= slab_data_size\n");

  min_item_sz = chunk_size_;
  max_item_sz = slab_data_size;
  id = SLABCLASS_MIN_ID;
  item_sz = min_item_sz;

  while (id < SLABCLASS_MAX_ID && item_sz < max_item_sz) {
    profile[id] = item_sz;
    id++;

    item_sz += 8;
  }

  profile[id] = max_item_sz;
  profile_last_id_ = id;
  max_chunk_size_ = max_item_sz;

  return FC_OK;
}
}