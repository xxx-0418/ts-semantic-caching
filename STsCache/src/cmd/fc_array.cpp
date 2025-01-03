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

#include <fc_core.h>

struct array *
array_create(uint32_t n, size_t size)
{
    struct array *a;

    ASSERT(n != 0 && size != 0);

    a = (struct array *)fc_alloc(sizeof(*a));
    if (a == NULL) {
        return NULL;
    }

    a->elem = fc_alloc(n * size);
    if (a->elem == NULL) {
        fc_free(a);
        return NULL;
    }

    a->nelem = 0;
    a->size = size;
    a->nalloc = n;

    return a;
}

void
array_destroy(struct array *a)
{
    array_deinit(a);
    fc_free(a);
}

rstatus_t
array_init(struct array *a, uint32_t n, size_t size)
{

    a->elem = fc_alloc(n * size);
    if (a->elem == NULL) {
        return FC_ENOMEM;
    }

    a->nelem = 0;
    a->size = size;
    a->nalloc = n;

    return FC_OK;
}

void
array_deinit(struct array *a)
{

    if (a->elem != NULL) {
        fc_free(a->elem);
    }
}

uint32_t
array_idx(struct array *a, void *elem)
{
    uint8_t *p, *q;
    uint32_t off, idx;

    p = (uint8_t *)a->elem;
    q = (uint8_t *)elem;
    off = (uint32_t)(q - p);

    idx = off / (uint32_t)a->size;

    return idx;
}

void *
array_push(struct array *a)
{
    void *elem, *new_elem;
    size_t size;

    if (a->nelem == a->nalloc) {

        size = a->size * a->nalloc;
        new_elem = fc_realloc(a->elem, 2 * size);
        if (new_elem == NULL) {
            return NULL;
        }

        a->elem = new_elem;
        a->nalloc *= 2;
    }

    elem = (uint8_t *)a->elem + a->size * a->nelem;
    a->nelem++;

    return elem;
}

void *
array_pop(struct array *a)
{
    void *elem;

    a->nelem--;
    elem = (uint8_t *)a->elem + a->size * a->nelem;

    return elem;
}

void *
array_get(struct array *a, uint32_t idx)
{
    void *elem;

    elem = (uint8_t *)a->elem + (a->size * idx);

    return elem;
}

void *
array_top(struct array *a)
{
    return array_get(a, a->nelem - 1);
}

void
array_swap(struct array *a, struct array *b)
{
    struct array tmp;

    tmp = *a;
    *a = *b;
    *b = tmp;
}

void
array_sort(struct array *a, array_compare_t compare)
{
    qsort(a->elem, a->nelem, a->size, compare);
}

rstatus_t
array_each(struct array *a, array_each_t func, void *data)
{
    uint32_t i, nelem;

    for (i = 0, nelem = array_n(a); i < nelem; i++) {
        void *elem = array_get(a, i);
        rstatus_t status;

        status = func(elem, data);
        if (status != FC_OK) {
            return status;
        }
    }

    return FC_OK;
}
