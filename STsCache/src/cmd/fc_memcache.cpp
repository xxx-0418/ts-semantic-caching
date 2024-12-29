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

#include <ctype.h>

#include <fc_core.h>
#include <fc_memcache.h>

#define MEMCACHE_MAX_KEY_LENGTH 4096

static bool
memcache_storage(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_SET:
    case MSG_REQ_CAS:
    case MSG_REQ_ADD:
    case MSG_REQ_REPLACE:
    case MSG_REQ_APPEND:
    case MSG_REQ_PREPEND:
        return true;

    default:
        break;
    }

    return false;
}

static bool
memcache_cas(struct msg *r)
{
    if (r->type == MSG_REQ_CAS) {
        return true;
    }

    return false;
}

static bool
memcache_retrieval(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_GET:
    case MSG_REQ_GETS:
        return true;

    default:
        break;
    }

    return false;
}

static bool
memcache_arithmetic(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_INCR:
    case MSG_REQ_DECR:
        return true;

    default:
        break;
    }

    return false;
}

static bool
memcache_delete(struct msg *r)
{
    if (r->type == MSG_REQ_DELETE) {
        return true;
    }

    return false;
}

static bool
memcache_key(struct msg *r)
{
    switch (r->type) {
    case MSG_REQ_GET:
    case MSG_REQ_GETS:
    case MSG_REQ_DELETE:
    case MSG_REQ_CAS:
    case MSG_REQ_SET:
    case MSG_REQ_ADD:
    case MSG_REQ_REPLACE:
    case MSG_REQ_APPEND:
    case MSG_REQ_PREPEND:
    case MSG_REQ_INCR:
    case MSG_REQ_DECR:
        return true;

    default:
        break;
    }

    return false;
}

static bool
memcache_quit(struct msg *r)
{
    if (r->type == MSG_REQ_QUIT) {
        return true;
    }

    return false;
}

static bool
memcache_version(struct msg *r)
{
    if (r->type == MSG_REQ_VERSION) {
        return true;
    }

    return false;
}

static bool
memcache_stats(struct msg *r)
{
    return r->type == MSG_REQ_STATS;
}

void
memcache_parse_req(struct msg *r)
{
    struct mbuf *b;
    uint8_t *p, *m;
    uint8_t ch;
    uint64_t table_index = 0;
   typedef enum  {
        SW_START,
        SW_REQ_TYPE,
        SW_SPACES_BEFORE_KEY,
        SW_KEY,
        SW_SPACES_BEFORE_KEYS,
        SW_SPACES_BEFORE_FLAGS,
        SW_FLAGS,
        SW_SPACES_BEFORE_EXPIRY,
        SW_EXPIRY,
        SW_SPACES_BEFORE_VLEN,
        SW_VLEN,
        SW_SPACES_BEFORE_START_TIME,
        SW_START_TIME,
        SW_SPACES_BEFORE_END_TIME,
        SW_END_TIME,
        SW_SPACES_BEFORE_NUMOFTAB,
        SW_NUMOFTAB,
        SW_SINGLE_SEMANTIC_SEGMENT,
        SW_TABLE_BYTES_LENGTH,
        SW_VALUES,

        SW_SPACES_BEFORE_CAS,
        SW_CAS,
        SW_RUNTO_VAL,
        SW_VAL,
        SW_SPACES_BEFORE_NUM,
        SW_NUM,
        SW_RUNTO_CRLF,
        SW_CRLF,
        SW_NOREPLY,
        SW_AFTER_NOREPLY,
        SW_ALMOST_DONE,
        SW_SENTINEL,
    } state_type;

    state_type state  =(state_type)r->state;
    b = STAILQ_LAST(&r->mhdr, mbuf, next);

    for (p = r->pos; p < b->last; p++) {
        ch = *p;

        switch (state) {

        case SW_START:
            if (ch == ' ') {
                break;
            }

            if (!islower(ch)) {
                goto error;
            }

            r->token = p;
            state = SW_REQ_TYPE;

            break;

        case SW_REQ_TYPE:
            if (ch == ' ' || ch == CR) {
                m = r->token;
                r->token = NULL;
                r->type = MSG_UNKNOWN;

                switch (p - m) {

                case 3:
                    if (str4cmp(m, 'g', 'e', 't', ' ')) {
                        r->type = MSG_REQ_GET;
                        break;
                    }

                    if (str4cmp(m, 's', 'e', 't', ' ')) {
                        r->type = MSG_REQ_SET;
                        break;
                    }

                    if (str4cmp(m, 'a', 'd', 'd', ' ')) {
                        r->type = MSG_REQ_ADD;
                        break;
                    }

                    if (str4cmp(m, 'c', 'a', 's', ' ')) {
                        r->type = MSG_REQ_CAS;
                        break;
                    }

                    break;

                case 4:
                    if (str4cmp(m, 'g', 'e', 't', 's')) {
                        r->type = MSG_REQ_GETS;
                        break;
                    }

                    if (str4cmp(m, 'i', 'n', 'c', 'r')) {
                        r->type = MSG_REQ_INCR;
                        break;
                    }

                    if (str4cmp(m, 'd', 'e', 'c', 'r')) {
                        r->type = MSG_REQ_DECR;
                        break;
                    }

                    if (str4cmp(m, 'q', 'u', 'i', 't')) {
                        r->type = MSG_REQ_QUIT;
                        r->quit = 1;
                        break;
                    }

                    break;

                case 5:
                    if (str5cmp(m, 's', 't', 'a', 't', 's')) {
                        r->type = MSG_REQ_STATS;
                        break;
                    }
                    break;
                case 6:
                    if (str6cmp(m, 'a', 'p', 'p', 'e', 'n', 'd')) {
                        r->type = MSG_REQ_APPEND;
                        break;
                    }

                    if (str6cmp(m, 'd', 'e', 'l', 'e', 't', 'e')) {
                        r->type = MSG_REQ_DELETE;
                        break;
                    }

                    break;

                case 7:
                    if (str7cmp(m, 'p', 'r', 'e', 'p', 'e', 'n', 'd')) {
                        r->type = MSG_REQ_PREPEND;
                        break;
                    }

                    if (str7cmp(m, 'r', 'e', 'p', 'l', 'a', 'c', 'e')) {
                        r->type = MSG_REQ_REPLACE;
                        break;
                    }

                    if (str7cmp(m, 'v', 'e', 'r', 's', 'i', 'o', 'n')) {
                        r->type = MSG_REQ_VERSION;
                        break;
                    }

                    break;
                }

                if (memcache_key(r)) {
                    if (ch == CR) {
                        goto error;
                    }
                    state = SW_SPACES_BEFORE_KEY;
                } else if (memcache_quit(r) || memcache_version(r)) {
                    p = p - 1;
                    state = SW_CRLF;
                } else if(memcache_stats(r)) {
                    while(*p == ' ') p++;
                    if (*p == '\r') {
                        state = SW_CRLF;
                    } else {
                        state = SW_SPACES_BEFORE_KEY; 
                    }
                    p = p - 1;
                } else {
                    goto error;
                }
            } else if (!islower(ch)) {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_KEY:
            if (ch != ' ') {
                p = p - 1;
                state = SW_KEY;
            }

            break;

        case SW_KEY:
            if (r->token == NULL) {
                r->token = p;
                r->key_start = p;
                r->semantic_segment_start = p;
            }

            if (ch == ' ' || ch == CR) {
                if ((p - r->key_start) > MEMCACHE_MAX_KEY_LENGTH) {
                    goto error;
                }
                r->key_end = p;
                r->semantic_segment_end = p;
                r->token = NULL;

                if (memcache_storage(r)) {
                    state = SW_SPACES_BEFORE_START_TIME;
                } else if (memcache_arithmetic(r)) {
                    state = SW_SPACES_BEFORE_NUM;
                } else if (memcache_delete(r)) {
                    state = SW_RUNTO_CRLF;
                } else if (memcache_retrieval(r)) {
                    state = SW_SPACES_BEFORE_START_TIME;
                } else {
                    state = SW_RUNTO_CRLF;
                }

                if (ch == CR) {
                    if (memcache_storage(r) || memcache_arithmetic(r)) {
                        goto error;
                    }
                    p = p - 1;
                }
            }

            break;

        case SW_SPACES_BEFORE_KEYS:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                r->token = p;
                goto fragment;
            }

            break;

        case SW_SPACES_BEFORE_FLAGS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_FLAGS;
            }

            break;

        case SW_FLAGS:
            if (r->token == NULL) {
                r->token = p;
                r->flags = 0;
            }

            if (isdigit(ch)) {
                r->flags = r->flags * 10 + (uint32_t)(ch - '0');
            } else if (ch == ' ') {
                r->token = NULL;
                state = SW_SPACES_BEFORE_EXPIRY;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_EXPIRY:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_EXPIRY;
            }

            break;

        case SW_EXPIRY:
            if (r->token == NULL) {
                r->token = p;
                r->expiry = 0;
            }

            if (isdigit(ch)) {
                r->expiry = r->expiry * 10 + (uint32_t)(ch - '0');
            } else if (ch == ' ') {
                r->token = NULL;
                state = SW_SPACES_BEFORE_VLEN;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_VLEN:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_VLEN;
            }

            break;

        case SW_VLEN:
            if (r->token == NULL) {
                r->token = p;
                r->vlen = 0;
            }

            if (isdigit(ch)) {
                r->vlen = r->vlen * 10 + (uint32_t)(ch - '0');
            } else if (memcache_cas(r)) {
                if (ch != ' ') {
                    goto error;
                }
                r->rvlen = r->vlen;
                p = p - 1;
                r->token = NULL;
                state = SW_SPACES_BEFORE_CAS;
            } else if (ch == ' ' || ch == CR) {
                r->rvlen = r->vlen;
                r->token = NULL;
                state = SW_SPACES_BEFORE_START_TIME;
            } else {
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_CAS:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_CAS;
            }

            break;

        case SW_CAS:
            if (r->token == NULL) {
                r->token = p;
                r->cas = 0;
            }

            if (isdigit(ch)) {
                r->cas = r->cas * 10ULL + (uint64_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                p = p - 1;
                r->token = NULL;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_VAL:
            switch (ch) {
            case LF:
                state = SW_VAL;
                break;

            default:
                goto error;
            }

            break;

        case SW_VAL:
            if (r->value == NULL) {
                r->value = p;
            }
            m = p + r->rvlen;
            if (m >= b->last) {
                r->rvlen -= (uint32_t)(b->last - p);
                m = b->last - 1;
                p = m;
                break;
            }
            switch (*m) {
            case CR:
                p = m;
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_SPACES_BEFORE_NUM:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_NUM;
            }

            break;

        case SW_NUM:
            if (r->token == NULL) {
                r->token = p;
                r->num = 0;
            }

            if (isdigit(ch)) {
                r->num = r->num * 10ULL + (uint64_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                r->token = NULL;
                p = p - 1;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_RUNTO_CRLF:
            switch (ch) {
            case ' ':
                break;

            case 'n':
                if (memcache_storage(r) || memcache_arithmetic(r) || memcache_delete(r)) {
                    p = p - 1;
                    state = SW_NOREPLY;
                } else {
                    goto error;
                }

                break;

            case CR:
                if (memcache_storage(r)) {
                    p = p + 1;
                    state = SW_SINGLE_SEMANTIC_SEGMENT;
                } else {
                    state = SW_ALMOST_DONE;
                }

                break;

            default:
                goto error;
            }

            break;

        case SW_NOREPLY:
            if (r->token == NULL) {
                r->token = p;
            }

            switch (ch) {
            case ' ':
            case CR:
                m = r->token;
                if (((p - m) == 7) && str7cmp(m, 'n', 'o', 'r', 'e', 'p', 'l', 'y')) {
                    r->token = NULL;
                    r->noreply = 1;
                    state = SW_AFTER_NOREPLY;
                    p = p - 1;
                } else {
                    goto error;
                }
            }

            break;

        case SW_AFTER_NOREPLY:
            switch (ch) {
            case ' ':
                break;

            case CR:
                if (memcache_storage(r)) {
                    state = SW_RUNTO_VAL;
                } else {
                    state = SW_ALMOST_DONE;
                }
                break;

            default:
                goto error;
            }

            break;

        case SW_CRLF:
            switch (ch) {
            case ' ':
                break;

            case CR:
                state = SW_ALMOST_DONE;
                break;

            default:
                goto error;
            }

            break;

        case SW_ALMOST_DONE:
            switch (ch) {
            case LF:
                goto done;

            default:
                goto error;
            }

            break;
        case SW_SPACES_BEFORE_START_TIME:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_START_TIME;
            }

            break;
        case SW_START_TIME: {
            if (r->token == NULL) {
                r->token = p;
                r->start_time = 0;
            }
            if (isdigit(ch)) {
                r->start_time = r->start_time * 10ULL + (uint64_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                r->token = NULL;
                p = p - 1;
                state = SW_SPACES_BEFORE_END_TIME;
            } else {
                goto error;
            }

            break;
        }
        case SW_SPACES_BEFORE_END_TIME:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_END_TIME;
            }

            break;
        case SW_END_TIME: {
            if (r->token == NULL) {
                r->token = p;
                r->end_time = 0;
            }
            if (isdigit(ch)) {
                r->end_time = r->end_time * 10ULL + (uint64_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                r->token = NULL;
                p = p - 1;

                if(memcache_storage(r)){
                    state = SW_SPACES_BEFORE_NUMOFTAB;
                } else if(memcache_retrieval(r)){
                    state = SW_RUNTO_CRLF;
                }

            } else {
                goto error;
            }

            break;
        }

        case SW_SPACES_BEFORE_NUMOFTAB:
            if (ch != ' ') {
                if (!isdigit(ch)) {
                    goto error;
                }
                p = p - 1;
                state = SW_NUMOFTAB;
            }

            break;

        case SW_NUMOFTAB:
            if (r->token == NULL) {
                r->token = p;
                r->number_of_table= 0;
            }

            if (isdigit(ch)) {
                r->number_of_table = r->number_of_table * 10ULL + (uint64_t)(ch - '0');
            } else if (ch == ' ' || ch == CR) {
                r->token = NULL;
                p = p - 1;
                state = SW_RUNTO_CRLF;
            } else {
                goto error;
            }

            break;

        case SW_SINGLE_SEMANTIC_SEGMENT:
            table_index = r->cur_table_number;
            if (r->token == NULL) {
                r->token = p;
                r->single_seg_start[table_index] = p;
            }

            if (ch == ' ') {
                r->single_seg_end[table_index] = p;
                r->token = NULL;
                r->single_seg_len[table_index] = r->single_seg_end[table_index] - r->single_seg_start[table_index];

                state = SW_TABLE_BYTES_LENGTH;

            }

            break;

        case SW_TABLE_BYTES_LENGTH:
            if (r->token == NULL) {
                r->token = p;
                r->table_byte_length_start = p;
            }

            m = p + 8;
            if (m >= b->last) {
                m = b->last - 1;
                p = m;
                break;
            }

            table_index = r->cur_table_number;
            r->table_byte_length[table_index] = 0;
            memcpy(&r->table_byte_length[table_index], p, 8);

            r->remain_tab_byte_len[table_index] = r->table_byte_length[table_index];

            p = p + 8;
            p = p - 1;
            r->token = NULL;
            state = SW_VALUES;

            break;

        case SW_VALUES:
            table_index = r->cur_table_number;
            if (r->values[table_index] == NULL) {
                r->values[table_index] = p;
            }

            m = p + r->remain_tab_byte_len[table_index];
            if (m >= b->last) {
                r->remain_tab_byte_len[table_index] -= (uint64_t)(b->last - p);
                m = b->last - 1;
                p = m;

                break;
            }

            r->cur_table_number += 1;
            if (r->cur_table_number == r->number_of_table) {
                p = m;
                p = p - 1;
                state = SW_CRLF;
                break;
            } else {
                p = m;
                p = p - 1;
                state = SW_SINGLE_SEMANTIC_SEGMENT;
            }

            break;

        case SW_SENTINEL:
        default:
            NOT_REACHED();
            break;

        }
    }

    r->pos = p;
    r->state = state;

    if (b->last == b->end && r->token != NULL) {
        r->pos = r->token;
        r->token = NULL;
        r->result = MSG_PARSE_REPAIR;
    } else {
        r->result = MSG_PARSE_AGAIN;
    }

    return;

fragment:
    ASSERT(p != b->last);
    r->pos = r->token;
    r->token = NULL;
    r->state = state;
    r->result = MSG_PARSE_FRAGMENT;

    return;

done:
    ASSERT(r->type > MSG_UNKNOWN && r->type < MSG_SENTINEL);
    r->pos = p + 1;
    r->state = SW_START;
    r->result = MSG_PARSE_OK;

    return;

error:
    r->result = MSG_PARSE_ERROR;
    r->state = state;
    errno = EINVAL;
}

void
memcache_pre_splitcopy(struct mbuf *mbuf, void *arg)
{
    struct msg *r = (struct msg *)arg;
    struct string get = fc_string("get ");
    struct string gets = fc_string("gets ");

    switch (r->type) {
    case MSG_REQ_GET:
        mbuf_copy(mbuf, get.data, get.len);
        break;

    case MSG_REQ_GETS:
        mbuf_copy(mbuf, gets.data, gets.len);
        break;

    default:
        NOT_REACHED();
    }
}

rstatus_t
memcache_post_splitcopy(struct msg *r)
{
    struct mbuf *mbuf;
    struct string crlf = fc_string(CRLF);

    mbuf = STAILQ_LAST(&r->mhdr, mbuf, next);
    mbuf_copy(mbuf, crlf.data, crlf.len);

    return FC_OK;
}
