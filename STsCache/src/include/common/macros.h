#pragma once
#ifndef MACROS_H
#define MACROS_H

#include <cassert>
#include <stdexcept>

namespace SemanticCache {

#define ll long long

#define ST_ASSERT(expr, message) assert((expr) && (message))

#define UNIMPLEMENTED(message) throw std::logic_error(message)

#define ST_ENSURE(expr, message)     \
  if (!(expr)) {                     \
    throw std::logic_error(message); \
  }
#define UNREACHABLE(message) throw std::logic_error(message)

#define DISALLOW_COPY(cname)                                    \
  cname(const cname &) = delete;                   /* NOLINT */ \
  auto operator=(const cname &)->cname & = delete; /* NOLINT */

#define DISALLOW_MOVE(cname)                               \
  cname(cname &&) = delete;                   /* NOLINT */ \
  auto operator=(cname &&)->cname & = delete; /* NOLINT */

enum EDGE_WEIGHT_TYPE : uint8_t { Atomic_To_Atomic, Atomic_To_Non_Atomic };
enum EDGE_COMPUTE_TYPE : uint8_t { Predicate_Compute, Aggregation_Compute, Not_Compute };
enum EDGE_INTERSECTION_TYPE : uint8_t { Same, My_Subset, Intersection, Totally_Contained };

enum Aggregation_Type : uint8_t { MAX, MIN, COUNT, SUM, MEAN, LAST, FIRST, NONE };
enum Aggregation_Interval_Type : uint8_t { NATURAL, SECOND, MINUTE, HOUR, DAY, WEEK, MONTH, YEAR };

}

namespace SemanticCache {
#define KB (1024)
#define MB (1024 * KB)
#define GB (1024 * MB)

#define ITEM_KEY_LEN 8
#define TIMESTAMP_LEN 8
#define TIME_INTERVAL 120

#define ITEM_BOUND_SIZE (4 * KB)

#define DISK_READ_GRAN 512

#ifndef UCHAR_MAX
#define UCHAR_MAX (__SCHAR_MAX__ * 2 + 1)
#endif

#define SLABCLASS_MIN_ID 0
#define SLABCLASS_MAX_ID (UCHAR_MAX - 1)
#define SLABCLASS_INVALID_ID UCHAR_MAX
#define SLABCLASS_MAX_IDS UCHAR_MAX
#define SLAB_INVALID_ID UINT32_MAX

#define FC_OK 0
#define FC_ERROR (-1)
#define FC_EAGAIN (-2)
#define FC_ENOMEM (-3)

#define FC_ALIGNMENT sizeof(unsigned long) /* platform word */
#define FC_ALIGN(d, n) ((size_t)(((d) + ((n)-1)) & ~((n)-1)))
#define FC_ALIGN_PTR(p, n) (void *)(((uintptr_t)(p) + ((uintptr_t)(n)-1)) & ~((uintptr_t)(n)-1))

#define ROUND_UP(x, step) (((x) + (step)-1) / (step) * (step))

#define ROUND_DOWN(x, step) ((x) / (step) * (step))

using rstatus_t = int;
using err_t = int;

#define QMD_TRACE_ELEM(elem)
#define QMD_TRACE_HEAD(head)
#define TRACEBUF

#define QMD_TAILQ_CHECK_HEAD(head, field)
#define QMD_TAILQ_CHECK_TAIL(head, headname)
#define QMD_TAILQ_CHECK_NEXT(elm, field)
#define QMD_TAILQ_CHECK_PREV(elm, field)

#define QMD_SAVELINK(name, link) void **name = (void **)&(link)

#define TRASHIT(x)      \
  do {                  \
    (x) = (void *)NULL; \
  } while (0)

#define TAILQ_EMPTY(head) ((head)->tqh_first == NULL)

#define TAILQ_FIRST(head) ((head)->tqh_first)

#define TAILQ_FOREACH(var, head, field) \
  for ((var) = TAILQ_FIRST((head)); (var); (var) = TAILQ_NEXT((var), field))

#define STAILQ_FOREACH(var, head, field) \
  for ((var) = STAILQ_FIRST((head)); (var); (var) = STAILQ_NEXT((var), field))

#define TAILQ_HEAD(name, type)                              \
  struct name {                                             \
    struct type *tqh_first;              \
    struct type **tqh_last;  \
    TRACEBUF                                                \
  }

#define TAILQ_ENTRY(type)                                          \
  struct {                                                         \
    struct type *tqe_next;                      \
    struct type **tqe_prev; \
    TRACEBUF                                                       \
  }

#define TAILQ_INIT(head)                     \
  do {                                       \
    TAILQ_FIRST((head)) = NULL;              \
    (head)->tqh_last = &TAILQ_FIRST((head)); \
    QMD_TRACE_HEAD(head);                    \
  } while (0)

#define TAILQ_NEXT(elm, field) ((elm)->field.tqe_next)

#define TAILQ_INSERT_HEAD(head, elm, field)                            \
  do {                                                                 \
    QMD_TAILQ_CHECK_HEAD(head, field);                                 \
    if ((TAILQ_NEXT((elm), field) = TAILQ_FIRST((head))) != NULL)      \
      TAILQ_FIRST((head))->field.tqe_prev = &TAILQ_NEXT((elm), field); \
    else                                                               \
      (head)->tqh_last = &TAILQ_NEXT((elm), field);                    \
    TAILQ_FIRST((head)) = (elm);                                       \
    (elm)->field.tqe_prev = &TAILQ_FIRST((head));                      \
    QMD_TRACE_HEAD(head);                                              \
    QMD_TRACE_ELEM(&(elm)->field);                                     \
  } while (0)

#define TAILQ_INSERT_TAIL(head, elm, field)       \
  do {                                            \
    QMD_TAILQ_CHECK_TAIL(head, field);            \
    TAILQ_NEXT((elm), field) = NULL;              \
    (elm)->field.tqe_prev = (head)->tqh_last;     \
    *(head)->tqh_last = (elm);                    \
    (head)->tqh_last = &TAILQ_NEXT((elm), field); \
    QMD_TRACE_HEAD(head);                         \
    QMD_TRACE_ELEM(&(elm)->field);                \
  } while (0)

#define TAILQ_REMOVE(head, elm, field)                                  \
  do {                                                                  \
    QMD_SAVELINK(oldnext, (elm)->field.tqe_next);                       \
    QMD_SAVELINK(oldprev, (elm)->field.tqe_prev);                       \
    QMD_TAILQ_CHECK_NEXT(elm, field);                                   \
    QMD_TAILQ_CHECK_PREV(elm, field);                                   \
    if ((TAILQ_NEXT((elm), field)) != NULL) {                           \
      TAILQ_NEXT((elm), field)->field.tqe_prev = (elm)->field.tqe_prev; \
    } else {                                                            \
      (head)->tqh_last = (elm)->field.tqe_prev;                         \
      QMD_TRACE_HEAD(head);                                             \
    }                                                                   \
    *(elm)->field.tqe_prev = TAILQ_NEXT((elm), field);                  \
    TRASHIT(*oldnext);                                                  \
    TRASHIT(*oldprev);                                                  \
    QMD_TRACE_ELEM(&(elm)->field);                                      \
  } while (0)

#define fc_mmap(_s) _fc_mmap((size_t)(_s), __FILE__, __LINE__)

#define fc_munmap(_p, _s) _fc_munmap(_p, (size_t)(_s), __FILE__, __LINE__)

#define fc_alloc(_s) _fc_alloc((size_t)(_s), __FILE__, __LINE__)

#define fc_free(_p)                   \
  do {                                \
    _fc_free(_p, __FILE__, __LINE__); \
    (_p) = NULL;                      \
  } while (0)

}

#endif