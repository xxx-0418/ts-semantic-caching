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

#ifndef _FC_MESSAGE_H_
#define _FC_MESSAGE_H_

#include <fc_core.h>
#include <vector>
#include "storage/slab_management/item.h"

typedef void (*msg_parse_t)(struct msg *);

#define MSG_CODEC(ACTION)                                \
    ACTION( UNKNOWN,            ""       ) \
    ACTION( REQ_GET,            "get  "                ) \
    ACTION( REQ_GETS,           "gets "                ) \
    ACTION( REQ_DELETE,         "delete "              ) \
    ACTION( REQ_CAS,            "cas "                 ) \
    ACTION( REQ_SET,            "set "                 ) \
    ACTION( REQ_ADD,            "add "                 ) \
    ACTION( REQ_REPLACE,        "replace "             ) \
    ACTION( REQ_APPEND,         "append  "             ) \
    ACTION( REQ_PREPEND,        "prepend "             ) \
    ACTION( REQ_INCR,           "incr "                ) \
    ACTION( REQ_DECR,           "decr "                ) \
    ACTION( REQ_STATS,           "stats "              ) \
    ACTION( REQ_VERSION,        "version "             ) \
    ACTION( REQ_QUIT,           "quit "                ) \
    ACTION( RSP_NUM,            "" /* na */            ) \
    ACTION( RSP_VALUE,          "VALUE "               ) \
    ACTION( RSP_END,            "END\r\n"              ) \
    ACTION( RSP_STORED,         "STORED\r\n"           ) \
    ACTION( RSP_NOT_STORED,     "NOT_STORED\r\n"       ) \
    ACTION( RSP_EXISTS,         "EXISTS\r\n"           ) \
    ACTION( RSP_NOT_FOUND,      "NOT_FOUND\r\n"        ) \
    ACTION( RSP_DELETED,        "DELETED\r\n"          ) \
    ACTION( RSP_CLIENT_ERROR,   "CLIENT_ERROR "        ) \
    ACTION( RSP_SERVER_ERROR,   "SERVER_ERROR "        ) \
    ACTION( RSP_VERSION,        "VERSION fatcache\r\n" ) \
    ACTION( CRLF,               "\r\n"      ) \
    ACTION( EMPTY,              ""         ) \
    ACTION( WHITESPACE,         " "          ) \

#define DEFINE_ACTION(_hash, _name) MSG_##_hash,
typedef enum msg_type {
    MSG_CODEC( DEFINE_ACTION )
    MSG_SENTINEL
} msg_type_t;
#undef DEFINE_ACTION

typedef enum msg_parse_result {
    MSG_PARSE_OK,
    MSG_PARSE_ERROR,
    MSG_PARSE_REPAIR,
    MSG_PARSE_FRAGMENT,
    MSG_PARSE_AGAIN,
} msg_parse_result_t;

struct msg {
    TAILQ_ENTRY(msg)     c_tqe;
    TAILQ_ENTRY(msg)     m_tqe;

    uint64_t             id;
    struct msg           *peer;
    struct conn          *owner;

    struct mhdr          mhdr;
    uint32_t             mlen;

    int                  state;
    uint8_t              *pos;
    uint8_t              *token;

    msg_parse_t          parser;
    msg_parse_result_t   result;

    msg_type_t           type;

    uint8_t              *key_start;
    uint8_t              *key_end;

    uint32_t             hash;
    uint8_t              md[20];

    uint32_t             flags;
    uint32_t             expiry;
    uint32_t             vlen;
    uint32_t             rvlen;
    uint8_t              *value;
    uint64_t             cas;
    uint64_t             num;
    uint64_t             start_time;
    uint64_t             end_time;
    uint64_t             number_of_table;
    uint8_t              *semantic_segment_start;
    uint8_t              *semantic_segment_end;
    uint32_t             semantic_segment_len;

    uint64_t             cur_table_number;
    uint8_t              *single_seg_start[200];
    uint8_t              *single_seg_end[200];
    uint32_t             single_seg_len[200];

    uint8_t              *table_byte_length_start;
    uint64_t             table_byte_length[200];
    uint64_t             remain_tab_byte_len[200];
    uint8_t              *values[200];

    struct msg           *frag_owner;
    uint32_t             nfrag;
    uint64_t             frag_id;

    err_t                err;
    unsigned             error:1;
    unsigned             request:1;
    unsigned             quit:1;
    unsigned             noreply:1;
    unsigned             done:1;
    unsigned             first_fragment:1;
    unsigned             last_fragment:1;
    unsigned             swallow:1;
};

TAILQ_HEAD(msg_tqh, msg);

bool msg_empty(struct msg *msg);
rstatus_t msg_recv(struct context *ctx, struct conn *conn);
rstatus_t msg_send(struct context *ctx, struct conn *conn);

struct msg *req_get(struct conn *conn);
void req_put(struct msg *msg);
struct msg *req_recv_next(struct context *ctx, struct conn *conn, bool alloc);

struct msg *msg_get(struct conn *conn, bool request);
void msg_put(struct msg *msg);

void msg_init(void);
void msg_deinit(void);

struct msg *rsp_get(struct conn *conn);
void rsp_put(struct msg *msg);

bool req_done(struct conn *conn, struct msg *msg);
struct msg *rsp_send_next(struct context *ctx, struct conn *conn);

void req_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void req_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg);
void rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg);
void req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg, struct msg *nmsg);

void req_process_error(struct context *ctx, struct conn *conn, struct msg *msg, int err);


void rsp_send_status(struct context *ctx, struct conn *conn, struct msg *msg, msg_type_t rsp_type);
void rsp_send_error(struct context *ctx, struct conn *conn, struct msg *msg, msg_type_t rsp_type, int err);
void rsp_send_value(struct context *ctx, struct conn *conn, struct msg *msg, std::vector<uint8_t *>& items,std::vector<std::string> &segments,std::vector<uint32_t > &items_len,std::vector<uint32_t>&per_segment_items_num,std::vector<TimeRange>&per_segment_time_range);
void rsp_send_num(struct context *ctx, struct conn *conn, struct msg *msg, struct item *it);
void rsp_send_string(struct context *ctx, struct conn *conn, struct msg *msg, struct string *str);

#endif
