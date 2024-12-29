#ifndef UTIL_H_
#define UTIL_H_

#include <cstddef>
#include <sys/mman.h>

void *_fc_mmap(std::size_t size, const char *name, int line);
int _fc_munmap(void *p, size_t size, const char *name, int line);

void *_fc_alloc(size_t size, const char *name, int line);
void _fc_free(void *ptr, const char *name, int line);
#endif