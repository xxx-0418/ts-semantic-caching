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

#include <stdio.h>
#include <stdarg.h>
#include <string.h>
#include <fc_stats.h>
#include <cstdint>
#include "defs.h"

static  stats_info st_info;
static stats_info sc_st_info[SLABCLASS_MAX_ID+1];
extern Settings settings;

buffer *
stats_alloc_buffer(int n)
{
    if (n <= 0) n = 128;

    buffer *buf = (buffer *)fc_alloc(sizeof(*buf));
    buf->data = (uint8_t  *)fc_alloc(n);
    buf->nused = 0;
    buf->nalloc = n;
    return buf;
}

void
stats_dealloc_buffer(buffer *buf)
{
    if (!buf) return;

    if (buf->data) fc_free(buf->data);
    fc_free(buf);
}

static void
_stats_append(buffer *buf, uint8_t cid, const char *key, int nkey, const char *val, int nval)
{
    int n, new_size, avail, bytes = 0;
    uint8_t *new_data;
    
    if (!buf) {
        return;
    }

    n = nkey + nval + 14;
    if (!buf->data) {
        buf->data = (uint8_t *)fc_alloc(n);
        buf->nused = 0;
        buf->nalloc = n;
    } else if (buf->nalloc - buf->nused < (uint32_t)n) {
        new_size = buf->nalloc > (uint32_t)n ? buf->nalloc * 2 :  buf->nalloc + 2 * n;
        new_data = (uint8_t *)fc_realloc(buf->data, new_size);
        if (new_data == NULL) {
            return;
        }
        buf->data = new_data;
        buf->nalloc = new_size;
    }

    avail = buf->nalloc - buf->nused;
    if (nkey == 0 && nval == 0) {
        bytes = fc_snprintf(buf->data + buf->nused, avail - 1, "END\r\n"); 
    } else if (nval == 0) {
        if (cid == SLABCLASS_INVALID_ID) {
            bytes = fc_snprintf(buf->data + buf->nused, avail - 1, "STAT %s\r\n", key);
        } else {
            bytes = fc_snprintf(buf->data + buf->nused, avail - 1, "STAT %u:%s\r\n", cid, key);
        }
    } else if (nkey > 0 && nval > 0) {
        if (cid == SLABCLASS_INVALID_ID) {
            bytes = fc_snprintf(buf->data + buf->nused, avail - 1, "STAT %s %s\r\n", key, val);
        } else {
            bytes = fc_snprintf(buf->data + buf->nused, avail - 1, "STAT %u:%s %s\r\n", cid, key, val);
        }
    }
    buf->nused += bytes;
    buf->data[buf->nused] = '\0';
}

void
stats_append(buffer *buf, uint8_t cid, const char*name, const char *fmt, ...)
{
    int n;
    va_list ap;
    char val[128];

    if (name && fmt) {
        va_start(ap, fmt);
        n = vsnprintf(val, sizeof(val) - 1, fmt, ap);
        va_end(ap);
        _stats_append(buf, cid, name, strlen(name), val, n);
    } else {
        _stats_append(buf, cid, NULL, 0, NULL, 0);
    }
}

uint64_t
stats_get(uint8_t cid, msg_type_t type, int is_miss)
{
    stats_info *info;

    info = cid == SLABCLASS_INVALID_ID ? &st_info : &sc_st_info[cid];

    switch(type) {
        case MSG_REQ_GET:
        case MSG_REQ_GETS:
            return is_miss? info->get - info->get_hits : info->get;
        case MSG_REQ_SET:
        case MSG_REQ_ADD:
        case MSG_REQ_APPEND:
        case MSG_REQ_PREPEND:
        case MSG_REQ_REPLACE:
            return info->set;
        case MSG_REQ_DELETE:
            return is_miss ? info->del - info->del_hits : info->del;
        case MSG_REQ_INCR:
            return is_miss ? info->incr - info->incr_hits : info->incr;
        case MSG_REQ_DECR:
            return is_miss ? info->decr - info->decr_hits : info->decr;
        case MSG_REQ_CAS:
            return is_miss ? info->cas - info->cas_hits : info->cas;
        default:
            return 0;
    }
}

void
stats_incr(uint8_t cid, msg_type_t type, int is_hit)
{
    stats_info *info;

    info = cid == SLABCLASS_INVALID_ID ? &st_info : &sc_st_info[cid];

    switch(type) {
    case MSG_REQ_GET:
    case MSG_REQ_GETS:
        is_hit ? info->get_hits++ : info->get++;
        break; 
    case MSG_REQ_SET:
    case MSG_REQ_ADD:
    case MSG_REQ_APPEND:
    case MSG_REQ_PREPEND:
    case MSG_REQ_REPLACE:
        info->set++;
        break;
    case MSG_REQ_DELETE:
        is_hit ? info->del_hits++ : info->del++;
        break;
    case MSG_REQ_INCR:
        is_hit ? info->incr_hits++ : info->incr++;
        break;
    case MSG_REQ_DECR:
        is_hit ? info->decr_hits++ : info->decr++;
        break;
    case MSG_REQ_CAS:
        is_hit ? info->cas_hits++ : info->cas++;
        break;
    default:
        break;
    }
}

buffer*
stats_server(void)
{
    buffer *stats_buf;

    stats_buf = stats_alloc_buffer(1024);
    if (stats_buf == NULL) {
        return NULL;
    }

    APPEND_STAT(stats_buf, "pid", "%u", getpid());
    APPEND_STAT(stats_buf, "uptime", "%u", time_started());
    APPEND_STAT(stats_buf, "version", "%s", "FC_MAJOR.FC_MINOR.FC_PATCH");
    APPEND_STAT(stats_buf, "pointer_size", "%u", sizeof(void*));
    APPEND_STAT(stats_buf, "curr_connection", "%u", conn_nused());
    APPEND_STAT(stats_buf, "free_connection", "%u", conn_nfree());
    APPEND_STAT(stats_buf, "total_connection", "%u", conn_total());
    APPEND_STAT(stats_buf, "cmd_get", "%llu", STATS_GET(MSG_REQ_GET));
    APPEND_STAT(stats_buf, "cmd_get_miss", "%llu", STATS_GET_MISS(MSG_REQ_GET));
    APPEND_STAT(stats_buf, "cmd_set", "%llu", STATS_GET(MSG_REQ_SET));
    APPEND_STAT(stats_buf, "cmd_del", "%llu", STATS_GET(MSG_REQ_DELETE));
    APPEND_STAT(stats_buf, "cmd_del_miss", "%llu", STATS_GET_MISS(MSG_REQ_DELETE));
    APPEND_STAT(stats_buf, "cmd_decr", "%llu", STATS_GET(MSG_REQ_DECR));
    APPEND_STAT(stats_buf, "cmd_decr_miss", "%llu", STATS_GET_MISS(MSG_REQ_DECR));
    APPEND_STAT(stats_buf, "cmd_incr", "%llu", STATS_GET(MSG_REQ_INCR));
    APPEND_STAT(stats_buf, "cmd_incr_miss", "%llu", STATS_GET_MISS(MSG_REQ_INCR));
    APPEND_STAT(stats_buf, "cmd_cas", "%llu", STATS_GET(MSG_REQ_CAS));
    APPEND_STAT(stats_buf, "cmd_cas_miss", "%llu", STATS_GET_MISS(MSG_REQ_CAS));
    APPEND_STAT_END(stats_buf);

    return stats_buf;
}

buffer*
stats_slabs(void)
{
    buffer *stats_buf;

    stats_buf = stats_alloc_buffer(512);
    if (stats_buf == NULL) {
        return NULL;
    }

    APPEND_STAT_END(stats_buf);

    return stats_buf;
}

buffer*
stats_settings(void)
{
    buffer *stats_buf;

    stats_buf = stats_alloc_buffer(256);
    if (stats_buf == NULL) {
        return NULL;
    }

    APPEND_STAT(stats_buf, "addr", "%s", settings.addr);
    APPEND_STAT(stats_buf, "port", "%d", settings.port);
    APPEND_STAT_END(stats_buf);

    return stats_buf;
}
