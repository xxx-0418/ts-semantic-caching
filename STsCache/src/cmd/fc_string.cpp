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

void
string_init(struct string *str)
{
    str->len = 0;
    str->data = NULL;
}

void
string_deinit(struct string *str)
{
    if (str->data != NULL) {
        fc_free(str->data);
        string_init(str);
    }
}

bool
string_empty(const struct string *str)
{
    return str->len == 0 ? true : false;
}

rstatus_t
string_duplicate(struct string *dst, const struct string *src)
{
    dst->data = fc_strndup(src->data, src->len + 1);
    if (dst->data == NULL) {
        return FC_ENOMEM;
    }

    dst->len = src->len;
    dst->data[dst->len] = '\0';

    return FC_OK;
}

rstatus_t
string_copy(struct string *dst, const uint8_t *src, uint32_t srclen)
{
    dst->data = fc_strndup(src, srclen + 1);
    if (dst->data == NULL) {
        return FC_ENOMEM;
    }

    dst->len = srclen;
    dst->data[dst->len] = '\0';

    return FC_OK;
}

int
string_compare(const struct string *s1, const struct string *s2)
{
    if (s1->len != s2->len) {
        return s1->len - s2->len > 0 ? 1 : -1;
    }

    return fc_strncmp(s1->data, s2->data, s1->len);
}
