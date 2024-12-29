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
#include <sys/types.h>
#include <unistd.h>

#include <fc_core.h>
#include <fc_event.h>
#include <fc_stats.h>
#include <cstddef>
#include <cstdint>
#include <iostream>
#include <vector>
#include "common/util/hash_util.h"
#include "defs.h"
#include"storage/slab_management/sha1.h"

#include "storage/slab_management/item.h"
#include "operation/operation.h"
#include "storage/semantic_graph.h"

extern Settings msg_strings[];
extern SemanticCache::SemanticGraph semantic_graph;
extern SemanticCache::SlabManagement slab_m;

struct msg *
req_get(struct conn *conn)
{
    struct msg *msg;

    msg = msg_get(conn, true);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
req_put(struct msg *msg)
{
    struct msg *pmsg;

    pmsg = msg->peer;
    if (pmsg != NULL) {
        msg->peer = NULL;
        pmsg->peer = NULL;
        rsp_put(pmsg);
    }

    msg_put(msg);
}

bool
req_done(struct conn *conn, struct msg *msg)
{
    if (!msg->done) {
        return false;
    }

    return true;
}

void
req_enqueue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    TAILQ_INSERT_TAIL(&conn->omsg_q, msg, c_tqe);
}

void
req_dequeue_omsgq(struct context *ctx, struct conn *conn, struct msg *msg)
{
    TAILQ_REMOVE(&conn->omsg_q, msg, c_tqe);
}

struct msg *
req_recv_next(struct context *ctx, struct conn *conn, bool alloc)
{
    struct msg *msg;

    if (conn->eof) {
        msg = conn->rmsg;

        if (msg != NULL) {
            conn->rmsg = NULL;
            req_put(msg);
        }

        if (!conn->active(conn)) {
            conn->done = 1;
        }

        return NULL;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        return msg;
    }

    if (!alloc) {
        return NULL;
    }

    msg = req_get(conn);
    if (msg != NULL) {
        conn->rmsg = msg;
    }

    return msg;
}

static bool
req_filter(struct context *ctx, struct conn *conn, struct msg *msg)
{
    if (msg_empty(msg)) {
        req_put(msg);
        return true;
    }
    if (msg->quit) {
        conn->eof = 1;
        conn->recv_ready = 0;
        req_put(msg);
        return true;
    }

    return false;
}

static void
req_process_get(struct context *ctx, struct conn *conn, struct msg *msg)
{
    std::vector<uint8_t*>items;
    std::vector<uint32_t>items_len;
    std::vector<std::string>segments;
    std::vector<uint32_t>per_segment_items_num;
    std::vector<TimeRange>per_segment_time_range;

    std::string semantic_segment = std::string(reinterpret_cast<const char*>(msg->semantic_segment_start), msg->semantic_segment_len);
    OpValue op_value;
    bool status = SemanticCache::GetProcess(slab_m,semantic_graph,semantic_segment,msg->start_time,msg->end_time,items,segments,items_len,per_segment_items_num,per_segment_time_range,op_value);
    if (!status) {
         msg_type_t type;

         if (msg->frag_id == 0 || msg->last_fragment) {
             type = MSG_RSP_END;
         } else {
             type = MSG_EMPTY;
         }

         rsp_send_status(ctx, conn, msg, type);
         return;
    }
     if (items.empty()) {
         rsp_send_error(ctx, conn, msg, MSG_RSP_SERVER_ERROR, errno);
         return;
     }

    STATS_HIT_INCR(msg->type);
    rsp_send_value(ctx, conn, msg, items,segments,items_len,per_segment_items_num,per_segment_time_range);
    if(op_value.flag){
      for(uint32_t i=0;i<op_value.segs_.size();i++){
        SemanticCache::SetProcess(slab_m,semantic_graph,op_value.key_,op_value.segs_[i],op_value.time_ranges_[i].time_start_,op_value.time_ranges_[i].time_end_,op_value.items_pair_[i].first,op_value.items_pair_[i].second);
      }
    }
}

static void
req_process_set(struct context *ctx, struct conn *conn, struct msg *msg)
{

    std::string semantic_segment = std::string(reinterpret_cast<const char*>(msg->semantic_segment_start), msg->semantic_segment_len);
    auto key = semantic_graph.SplitSegment(semantic_segment)->second;
    if(msg->number_of_table == 1){
        uint8_t *val;
        val = (uint8_t *)malloc(msg->table_byte_length[0]+1);
        if (val == NULL) {
            rsp_send_error(ctx, conn, msg, MSG_RSP_SERVER_ERROR, ENOMEM);
            return;
        }

        if(msg->table_byte_length[0]!=0&&msg->values[0] != nullptr){
          mbuf_copy_to(&msg->mhdr, msg->values[0], val, msg->table_byte_length[0]);
        }


        std::string single_semantic_segment = std::string(reinterpret_cast<const char*>(msg->single_seg_start[0]), msg->single_seg_len[0]);

        bool set = SemanticCache::SetProcess(slab_m,semantic_graph,key,single_semantic_segment,msg->start_time,msg->end_time,val,msg->table_byte_length[0]);

        free(val);

        if (set) {
            msg->single_seg_start[0] = NULL;
            msg->single_seg_end[0] = NULL;
            msg->single_seg_len[0] = 0;
            msg->table_byte_length[0] = 0;
            msg->values[0] = NULL;
            msg->remain_tab_byte_len[0] = 0;
        } else {
            rsp_send_error(ctx, conn, msg, MSG_RSP_NOT_STORED, EAGAIN);
            return;
        }
    }else{
        uint32_t  v_len = 0;
        bool flag = false;
        std::string semantic_segment = std::string(reinterpret_cast<const char*>(msg->semantic_segment_start), msg->semantic_segment_len);
        if(!semantic_graph.IsExistNode(semantic_segment)){
            flag = true;
        }
        for (uint64_t i = 0; i < msg->number_of_table; i++) {
            uint8_t *val;
            val = (uint8_t *)malloc(msg->table_byte_length[i]+1);
            if (val == NULL) {
                rsp_send_error(ctx, conn, msg, MSG_RSP_SERVER_ERROR, ENOMEM);
                return;
            }
            if(msg->values[i] != nullptr&&msg->table_byte_length[i] != 0){
                mbuf_copy_to(&msg->mhdr, msg->values[i], val, msg->table_byte_length[i]);
            }

            std::string single_semantic_segment = std::string(reinterpret_cast<const char*>(msg->single_seg_start[i]), msg->single_seg_len[i]);

            bool set = SemanticCache::SetProcess(slab_m,semantic_graph,key,single_semantic_segment,msg->start_time,msg->end_time,val,msg->table_byte_length[i]);

            free(val);

            if (set) {
                msg->single_seg_start[i] = NULL;
                msg->single_seg_end[i] = NULL;
                msg->single_seg_len[i] = 0;
                msg->table_byte_length[i] = 0;
                msg->values[i] = NULL;
                msg->remain_tab_byte_len[i] = 0;

            } else {
                rsp_send_error(ctx, conn, msg, MSG_RSP_NOT_STORED, EAGAIN);
                return;
            }
        }
    }
    msg->semantic_segment_start = NULL;
    msg->semantic_segment_end = NULL;
    msg->semantic_segment_len = 0;
    rsp_send_status(ctx, conn, msg, MSG_RSP_STORED);

}

static void
req_process_add(struct context *ctx, struct conn *conn, struct msg *msg)
{
}

void
req_process_error(struct context *ctx, struct conn *conn, struct msg *msg,
                  int err)
{
    rstatus_t status;
    msg->done = 1;
    msg->error = 1;
    msg->err = err != 0 ? err : errno;

    if (msg->noreply) {
        req_put(msg);
        return;
    }

    if (req_done(conn, TAILQ_FIRST(&conn->omsg_q))) {
        status = event_add_out(ctx->ep, conn);
        if (status != FC_OK) {
            conn->err = errno;
        }
    }
}

static void
req_process(struct context *ctx, struct conn *conn, struct msg *msg)
{
    uint8_t *key;
    size_t keylen;

    if (!msg->noreply) {
        req_enqueue_omsgq(ctx, conn, msg);
    }

    key = msg->key_start;
    keylen = msg->key_end - msg->key_start;

    msg->semantic_segment_len = msg->semantic_segment_end - msg->semantic_segment_start;
    STATS_INCR(msg->type);
    switch (msg->type) {
    case MSG_REQ_GET:
    case MSG_REQ_GETS:
        req_process_get(ctx, conn, msg);
        break;

    case MSG_REQ_SET:
        req_process_set(ctx, conn, msg);
        break;

    case MSG_REQ_ADD:
        req_process_add(ctx, conn, msg);
        break;

    default:
        NOT_REACHED();
    }
}

void
req_recv_done(struct context *ctx, struct conn *conn, struct msg *msg,
              struct msg *nmsg)
{
    conn->rmsg = nmsg;

    if (req_filter(ctx, conn, msg)) {
        return;
    }

    req_process(ctx, conn, msg);
}
