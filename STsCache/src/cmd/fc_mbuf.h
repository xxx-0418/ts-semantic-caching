/*
 * fatcache - memcache on ssd.
 * Copyright (C) 2013 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef _FC_MBUF_H_
#define _FC_MBUF_H_

#include <fc_core.h>

typedef void (*mbuf_copy_t)(struct mbuf *, void *);

struct mbuf {
    uint32_t           magic;
    STAILQ_ENTRY(mbuf) next;
    uint8_t            *pos;
    uint8_t            *last;
    uint8_t            *start;
    uint8_t            *end;
};

STAILQ_HEAD(mhdr, mbuf);

#define MBUF_MAGIC      0xdeadbeef
#define MBUF_MIN_SIZE   512
#define MBUF_MAX_SIZE   65536
#define MBUF_SIZE       81920
#define MBUF_HSIZE      sizeof(struct mbuf)

static inline bool
mbuf_empty(struct mbuf *mbuf)
{
    return mbuf->pos == mbuf->last ? true : false;
}

static inline bool
mbuf_full(struct mbuf *mbuf)
{
    return mbuf->last == mbuf->end ? true : false;
}

void mbuf_init(void);
void mbuf_deinit(void);
struct mbuf *mbuf_get(void);
void mbuf_put(struct mbuf *mbuf);
void mbuf_rewind(struct mbuf *mbuf);
uint32_t mbuf_length(struct mbuf *mbuf);
uint32_t mbuf_size(struct mbuf *mbuf);
size_t mbuf_data_size(void);
bool mbuf_contains(struct mbuf *mbuf, uint8_t *p);
void mbuf_insert(struct mhdr *mhdr, struct mbuf *mbuf);
void mbuf_remove(struct mhdr *mhdr, struct mbuf *mbuf);
void mbuf_copy(struct mbuf *mbuf, uint8_t *pos, size_t size);
rstatus_t mbuf_copy_from(struct mhdr *mhdr, uint8_t *pos, size_t size);
void mbuf_copy_to(struct mhdr *mhdr, uint8_t *marker, uint8_t *pos, size_t size);
struct mbuf *mbuf_split(struct mhdr *h, uint8_t *pos, mbuf_copy_t cb, void *cbarg);

#endif
