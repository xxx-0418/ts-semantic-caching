#include "common/util.h"
#include <cstdio>
#include <cstring>
#include <string>
#include "common/macros.h"

void *_fc_mmap(std::size_t size, const char *name, int line) {
  void *p;

  ST_ENSURE(size != 0, "fc_mmap failed: size should be non-zero\n");

  p = mmap(nullptr, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
  ST_ASSERT(p != MAP_FAILED, "mmap failed\n");

  return p;
}
int _fc_munmap(void *p, std::size_t size, const char *name, int line) {
  int status;

  ST_ASSERT(p != nullptr, "fc_munmap failed: p should not be nullptr\n");
  ST_ASSERT(size != 0, "fc_munmap failed: size should be non-zero\n");

  status = munmap(p, size);
  if (status < 0) {
    UNREACHABLE("unmap failed\n");
  }

  return status;
}

void *_fc_alloc(size_t size, const char *name, int line) {
  void *p;

  ST_ENSURE(size != 0, "_fc_alloc failed: size should be non-zero\n");

  p = malloc(size);
  if(p == nullptr) {
    
  }
  ST_ENSURE(p != nullptr, "_fc_alloc failed: malloc return nullptr\n");

  return p;
}

void
_fc_free(void *ptr, const char *name, int line)
{
    ST_ASSERT(ptr != nullptr, "_fc_free failed: ptr must be non-nullptr\n");
    free(ptr);
}
