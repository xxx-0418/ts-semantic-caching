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

#include <stdlib.h>
#include <string.h>

#include <fc_core.h>

static uint32_t nfree_mbufq;
static struct mhdr free_mbufq;

static size_t mbuf_chunk_size;
static size_t mbuf_offset;

static struct mbuf *
_mbuf_get(void)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    if (!STAILQ_EMPTY(&free_mbufq)) {

        mbuf = STAILQ_FIRST(&free_mbufq);
        nfree_mbufq--;
        STAILQ_REMOVE_HEAD(&free_mbufq, next);

        goto done;
    }

    buf = (uint8_t *)fc_alloc(mbuf_chunk_size);
    if (buf == NULL) {
        return NULL;
    }

    mbuf = (struct mbuf *)(buf + mbuf_offset);
    mbuf->magic = MBUF_MAGIC;

done:
    STAILQ_NEXT(mbuf, next) = NULL;
    return mbuf;
}

struct mbuf *
mbuf_get(void)
{
    struct mbuf *mbuf;
    uint8_t *buf;

    mbuf = _mbuf_get();
    if (mbuf == NULL) {
        return NULL;
    }

    buf = (uint8_t *)mbuf - mbuf_offset;
    mbuf->start = buf;
    mbuf->end = buf + mbuf_offset;

    mbuf->pos = mbuf->start;
    mbuf->last = mbuf->start;

    return mbuf;
}

static void
mbuf_free(struct mbuf *mbuf)
{
    uint8_t *buf;

    buf = (uint8_t *)mbuf - mbuf_offset;
    fc_free(buf);
}

void
mbuf_put(struct mbuf *mbuf)
{
    nfree_mbufq++;
    STAILQ_INSERT_HEAD(&free_mbufq, mbuf, next);
}

void
mbuf_rewind(struct mbuf *mbuf)
{
    mbuf->pos = mbuf->start;
    mbuf->last = mbuf->start;
}

uint32_t
mbuf_length(struct mbuf *mbuf)
{
    return (uint32_t)(mbuf->last - mbuf->pos);
}

uint32_t
mbuf_size(struct mbuf *mbuf)
{
    return (uint32_t)(mbuf->end - mbuf->last);
}

size_t
mbuf_data_size(void)
{
    return mbuf_offset;
}

bool
mbuf_contains(struct mbuf *mbuf, uint8_t *p)
{
    if (p >= mbuf->start && p < mbuf->last) {
        return true;
    }

    return false;
}

void
mbuf_insert(struct mhdr *mhdr, struct mbuf *mbuf)
{
    STAILQ_INSERT_TAIL(mhdr, mbuf, next);
}

void
mbuf_remove(struct mhdr *mhdr, struct mbuf *mbuf)
{
    STAILQ_REMOVE(mhdr, mbuf, mbuf, next);
    STAILQ_NEXT(mbuf, next) = NULL;
}

void
mbuf_copy(struct mbuf *mbuf, uint8_t *pos, size_t size)
{
    if (size == 0) {
        return;
    }

    fc_memcpy(mbuf->last, pos, size);
    mbuf->last += size;
}

rstatus_t
mbuf_copy_from(struct mhdr *mhdr, uint8_t *pos, size_t size)
{
    struct mbuf *mbuf;
    size_t n;

    if (size == 0) {
        return FC_OK;
    }

    STAILQ_FOREACH(mbuf, mhdr, next) {
    }

    do {
        mbuf = STAILQ_LAST(mhdr, mbuf, next);
        if (mbuf == NULL || mbuf_full(mbuf)) {
            mbuf = mbuf_get();
            if (mbuf == NULL) {
                return FC_ENOMEM;
            }
            STAILQ_INSERT_TAIL(mhdr, mbuf, next);
        }

        n = MIN(mbuf_size(mbuf), size);

        mbuf_copy(mbuf, pos, n);
        pos += n;
        size -= n;

    } while (size > 0);

    return FC_OK;
}

void
mbuf_copy_to(struct mhdr *mhdr, uint8_t *marker, uint8_t *pos, size_t size)
{
    struct mbuf *mbuf;
    size_t n;

    if (size == 0) {
        return;
    }

    for (mbuf = STAILQ_FIRST(mhdr); mbuf != NULL;
         mbuf = STAILQ_NEXT(mbuf, next)) {

        if (mbuf_contains(mbuf, marker)) {
            n = MIN(size, (size_t)(mbuf->last - marker));

            fc_memcpy(pos, marker, n);
            pos += n;
            size -= n;
            break;
        }
    }

    for (mbuf = STAILQ_NEXT(mbuf, next); mbuf != NULL && size > 0;
         mbuf = STAILQ_NEXT(mbuf, next)) {
        n = MIN(size, mbuf_length(mbuf));

        fc_memcpy(pos, mbuf->pos, n);
        pos += n;
        size -= n;
    }
}

struct mbuf *
mbuf_split(struct mhdr *h, uint8_t *pos, mbuf_copy_t cb, void *cbarg)
{
    struct mbuf *mbuf, *nbuf;
    size_t size;

    mbuf = STAILQ_LAST(h, mbuf, next);

    nbuf = mbuf_get();
    if (nbuf == NULL) {
        return NULL;
    }

    if (cb != NULL) {
        cb(nbuf, cbarg);
    }

    size = (size_t)(mbuf->last - pos);
    mbuf_copy(nbuf, pos, size);

    mbuf->last = pos;

    return nbuf;
}

void
mbuf_init(void)
{
    nfree_mbufq = 0;
    STAILQ_INIT(&free_mbufq);

    mbuf_chunk_size = MBUF_SIZE;
    mbuf_offset = mbuf_chunk_size - MBUF_HSIZE;
}

void
mbuf_deinit(void)
{
    while (!STAILQ_EMPTY(&free_mbufq)) {
        struct mbuf *mbuf = STAILQ_FIRST(&free_mbufq);
        mbuf_remove(&free_mbufq, mbuf);
        mbuf_free(mbuf);
        nfree_mbufq--;
    }
}
