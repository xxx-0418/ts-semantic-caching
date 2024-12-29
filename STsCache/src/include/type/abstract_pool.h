#pragma once
#ifndef ABSTRACT_POOL_H
#define ABSTRACT_POOL_H
#include <cstdlib>

namespace SemanticCache {

class AbstractPool {
 public:
  virtual ~AbstractPool() = default;

  virtual auto Allocate(size_t size) -> void * = 0;

  virtual void Free(void *ptr) = 0;
};

}  
#endif