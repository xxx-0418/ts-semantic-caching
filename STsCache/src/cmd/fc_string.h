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

#ifndef _FC_STRING_H_
#define _FC_STRING_H_

#include <string.h>
#include <fc_core.h>

struct string {
    uint32_t len;
    uint8_t  *data;
};

#define fc_string(_str)   { sizeof(_str) - 1, (uint8_t *)(_str) }
#define null_string    { 0, NULL }

#define string_set_text(_str, _text) do {       \
    (_str)->len = (uint32_t)(sizeof(_text) - 1);\
    (_str)->data = (uint8_t *)(_text);          \
} while (0);

#define string_set_raw(_str, _raw) do {         \
    (_str)->len = (uint32_t)(fc_strlen(_raw));  \
    (_str)->data = (uint8_t *)(_raw);           \
} while (0);

void string_init(struct string *str);
void string_deinit(struct string *str);
bool string_empty(const struct string *str);
rstatus_t string_duplicate(struct string *dst, const struct string *src);
rstatus_t string_copy(struct string *dst, const uint8_t *src, uint32_t srclen);
int string_compare(const struct string *s1, const struct string *s2);

#define fc_memcpy(_d, _c, _n)           \
    memcpy(_d, _c, (size_t)(_n))

#define fc_memmove(_d, _c, _n)          \
    memmove(_d, _c, (size_t)(_n))

#define fc_memchr(_d, _c, _n)           \
    memchr(_d, _c, (size_t)(_n))

#define fc_strlen(_s)                   \
    strlen((char *)(_s))

#define fc_strncmp(_s1, _s2, _n)        \
    strncmp((char *)(_s1), (char *)(_s2), (size_t)(_n))

#define fc_strchr(_p, _l, _c)           \
    _fc_strchr((uint8_t *)(_p), (uint8_t *)(_l), (uint8_t)(_c))

#define fc_strrchr(_p, _s, _c)          \
    _fc_strrchr((uint8_t *)(_p),(uint8_t *)(_s), (uint8_t)(_c))

#define fc_strndup(_s, _n)              \
    (uint8_t *)strndup((char *)(_s), (size_t)(_n));

#define fc_snprintf(_s, _n, ...)        \
    snprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define fc_scnprintf(_s, _n, ...)       \
    _scnprintf((char *)(_s), (size_t)(_n), __VA_ARGS__)

#define fc_vscnprintf(_s, _n, _f, _a)   \
    _vscnprintf((char *)(_s), (size_t)(_n), _f, _a)

static inline uint8_t *
_fc_strchr(uint8_t *p, uint8_t *last, uint8_t c)
{
    while (p < last) {
        if (*p == c) {
            return p;
        }
        p++;
    }

    return NULL;
}

static inline uint8_t *
_fc_strrchr(uint8_t *p, uint8_t *start, uint8_t c)
{
    while (p >= start) {
        if (*p == c) {
            return p;
        }
        p--;
    }

    return NULL;
}

#endif
