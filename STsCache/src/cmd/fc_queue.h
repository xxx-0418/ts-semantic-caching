#ifndef _FC_QUEUE_H_
#define _FC_QUEUE_H_

#include "common/macros.h"

#define __offsetof(type, field) ((size_t)(&((type *)NULL)->field))

#define STAILQ_EMPTY(head)    ((head)->stqh_first == NULL)

#define STAILQ_FIRST(head)    ((head)->stqh_first)

#define STAILQ_HEAD_INITIALIZER(head)                                   \
    { NULL, &(head).stqh_first }

#define STAILQ_ENTRY(type)                                              \
struct {                                                                \
    struct type *stqe_next;                       \
}
#define STAILQ_HEAD(name, type)                                         \
struct name {                                                           \
    struct type *stqh_first;                   \
    struct type **stqh_last;           \
}

#define STAILQ_NEXT(elm, field)    ((elm)->field.stqe_next)

#define STAILQ_REMOVE_HEAD(head, field) do {                            \
    QMD_SAVELINK(oldnext, STAILQ_NEXT(STAILQ_FIRST((head)), field));    \
    if ((STAILQ_FIRST((head)) =                                         \
         STAILQ_NEXT(STAILQ_FIRST((head)), field)) == NULL) {           \
        (head)->stqh_last = &STAILQ_FIRST((head));                      \
    }                                                                   \
    TRASHIT(*oldnext);                                                  \
} while (0)

#define STAILQ_INSERT_TAIL(head, elm, field) do {                       \
    STAILQ_NEXT((elm), field) = NULL;                                   \
    *(head)->stqh_last = (elm);                                         \
    (head)->stqh_last = &STAILQ_NEXT((elm), field);                     \
} while (0)

#define STAILQ_REMOVE_AFTER(head, elm, field) do {                      \
    QMD_SAVELINK(oldnext, STAILQ_NEXT(STAILQ_NEXT(elm, field), field)); \
    if ((STAILQ_NEXT(elm, field) =                                      \
         STAILQ_NEXT(STAILQ_NEXT(elm, field), field)) == NULL) {        \
        (head)->stqh_last = &STAILQ_NEXT((elm), field);                 \
    }                                                                   \
    TRASHIT(*oldnext);                                                  \
} while (0)

#define STAILQ_INSERT_HEAD(head, elm, field) do {                       \
    if ((STAILQ_NEXT((elm), field) = STAILQ_FIRST((head))) == NULL)     \
        (head)->stqh_last = &STAILQ_NEXT((elm), field);                 \
    STAILQ_FIRST((head)) = (elm);                                       \
} while (0)

#define STAILQ_REMOVE(head, elm, type, field) do {                      \
    if (STAILQ_FIRST((head)) == (elm)) {                                \
        STAILQ_REMOVE_HEAD((head), field);                              \
    }                                                                   \
    else {                                                              \
        struct type *curelm = STAILQ_FIRST((head));                     \
        while (STAILQ_NEXT(curelm, field) != (elm))                     \
            curelm = STAILQ_NEXT(curelm, field);                        \
        STAILQ_REMOVE_AFTER(head, curelm, field);                       \
    }                                                                   \
} while (0)

#define STAILQ_LAST(head, type, field)                                  \
    (STAILQ_EMPTY((head)) ?                                             \
        NULL :                                                          \
            ((struct type *)(void *)                                    \
        ((char *)((head)->stqh_last) - __offsetof(struct type, field))))

#define STAILQ_HEAD_INITIALIZER(head)                                   \
    { NULL, &(head).stqh_first }

#define STAILQ_ENTRY(type)                                              \
struct {                                                                \
    struct type *stqe_next;                        \
}

#define STAILQ_INIT(head) do {                                          \
    STAILQ_FIRST((head)) = NULL;                                        \
    (head)->stqh_last = &STAILQ_FIRST((head));                          \
} while (0)
#endif