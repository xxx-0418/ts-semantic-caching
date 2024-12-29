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

#include <fc_core.h>
#include <string>
#include <vector>

extern struct string msg_strings[];

struct msg *
rsp_get(struct conn *conn)
{
    struct msg *msg;

    msg = msg_get(conn, false);
    if (msg == NULL) {
        conn->err = errno;
    }

    return msg;
}

void
rsp_put(struct msg *msg)
{
    ASSERT(!msg->request);
    ASSERT(msg->peer == NULL);
    msg_put(msg);
}

struct msg *
rsp_send_next(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *pmsg;

    pmsg = TAILQ_FIRST(&conn->omsg_q);
    if (pmsg == NULL || !pmsg->done) {
        if (pmsg == NULL && conn->eof) {
            conn->done = 1;
        }

        status = event_del_out(ctx->ep, conn);
        if (status != FC_OK) {
            conn->err = errno;
        }

        return NULL;
    }

    msg = conn->smsg;
    if (msg != NULL) {
        pmsg = TAILQ_NEXT(msg->peer, c_tqe);
    }

    if (pmsg == NULL || !pmsg->done) {
        conn->smsg = NULL;
        return NULL;
    }

    msg = pmsg->peer;

    conn->smsg = msg;

    return msg;
}

void
rsp_send_done(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *pmsg;

    pmsg = msg->peer;

    req_dequeue_omsgq(ctx, conn, pmsg);

    req_put(pmsg);
}

void rsp_send_string(struct context *ctx, struct conn *conn, struct msg *msg,
                     struct string *str)
{
    rstatus_t status;
    struct msg *pmsg;

    if(!str) return;

    pmsg = rsp_get(conn);
    if (pmsg == NULL) {
        req_process_error(ctx, conn, msg, ENOMEM);
        return;
    }

    status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
    pmsg->mlen += str->len;

    msg->done = 1;
    msg->peer = pmsg;
    pmsg->peer = msg;

    status = event_add_out(ctx->ep, conn);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
}

void
rsp_send_status(struct context *ctx, struct conn *conn, struct msg *msg,
                msg_type_t rsp_type)
{
    rstatus_t status;
    struct msg *pmsg;
    struct string *str;

    if (msg->noreply) {
        req_put(msg);
        return;
    }

    pmsg = rsp_get(conn);
    if (pmsg == NULL) {
        req_process_error(ctx, conn, msg, ENOMEM);
        return;
    }

    str = &msg_strings[rsp_type];
    status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
    pmsg->mlen += str->len;

    msg->done = 1;
    msg->peer = pmsg;
    pmsg->peer = msg;

    status = event_add_out(ctx->ep, conn);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
}

void
rsp_send_error(struct context *ctx, struct conn *conn, struct msg *msg,
               msg_type_t rsp_type, int err)
{
    rstatus_t status;
    struct msg *pmsg;
    struct string *str, pstr;
    char const*errstr;

    pmsg = rsp_get(conn);
    if (pmsg == NULL) {
        req_process_error(ctx, conn, msg, ENOMEM);
        return;
    }

    str = &msg_strings[rsp_type];
    status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
    pmsg->mlen += str->len;

    errstr = err != 0 ? strerror(err) : "unknown";
    string_set_raw(&pstr, errstr);
    str = &pstr;
    status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
    pmsg->mlen += str->len;

    str = &msg_strings[MSG_CRLF];
    status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
    pmsg->mlen += str->len;

    msg->done = 1;
    msg->peer = pmsg;
    pmsg->peer = msg;

    status = event_add_out(ctx->ep, conn);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
}


void rsp_send_value(struct context *ctx, struct conn *conn, struct msg *msg, std::vector<uint8_t *>& items,std::vector<std::string> &segments,std::vector<uint32_t > &items_len,std::vector<uint32_t>&per_segment_items_num,std::vector<TimeRange>&per_segment_time_range){
  rstatus_t status;
    struct msg *pmsg;
    struct string *str;

    pmsg = rsp_get(conn);
    if (pmsg == NULL) {
        req_process_error(ctx, conn, msg, ENOMEM);
        return;
    }

    uint64_t idx=0;
    for(uint32_t i=0;i<segments.size();i++){
        status = mbuf_copy_from(&pmsg->mhdr, (uint8_t*)(segments[i]).c_str(), (uint32_t)(segments[i]).length());
        if (status != FC_OK) {
            req_process_error(ctx, conn, msg, errno);
            return;
        }
        pmsg->mlen += (uint32_t)(segments[i]).length();

        str = &msg_strings[MSG_WHITESPACE];
        status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
        if (status != FC_OK) {
            req_process_error(ctx, conn, msg, errno);
            return;
        }
        pmsg->mlen += str->len;


        uint8_t flag = 0;
        if(per_segment_time_range[i].time_start_!=0||per_segment_time_range[i].time_end_!=0){
            flag = 1;
        }

        status = mbuf_copy_from(&pmsg->mhdr, (uint8_t *)&flag, 1);
        if (status != FC_OK) {
            req_process_error(ctx, conn, msg, errno);
            return;
        }
        pmsg->mlen += 1;

        if(flag == 1){
            status = mbuf_copy_from(&pmsg->mhdr, (uint8_t *)&(per_segment_time_range[i].time_start_), 8);
            if (status != FC_OK) {
                req_process_error(ctx, conn, msg, errno);
                return;
            }
            pmsg->mlen += 8;
            status = mbuf_copy_from(&pmsg->mhdr, (uint8_t *)&(per_segment_time_range[i].time_end_), 8);
            if (status != FC_OK) {
                req_process_error(ctx, conn, msg, errno);
                return;
            }
            pmsg->mlen += 8;
        }
        uint64_t nbytes = 0;
        for(uint32_t j=0;j<per_segment_items_num[i];j++){
            nbytes += items_len[j+idx];
        }
        status = mbuf_copy_from(&pmsg->mhdr, (uint8_t *)&nbytes, 8);
        if (status != FC_OK) {
            req_process_error(ctx, conn, msg, errno);
            return;
        }
        pmsg->mlen += 8;

        for(uint32_t j=0;j<per_segment_items_num[i];j++){
            if(items[idx+j]== nullptr||items_len[j+idx]==0){
              continue;
            }
            status = mbuf_copy_from(&pmsg->mhdr, items[idx+j], items_len[j+idx]);
            if (status != FC_OK) {
                req_process_error(ctx, conn, msg, errno);
                return;
            }
            pmsg->mlen += items_len[j+idx];
        }
        idx += per_segment_items_num[i];

    }
    str = &msg_strings[MSG_CRLF];
    status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
    pmsg->mlen += str->len;

    if (msg->frag_id == 0 || msg->last_fragment) {
        str = &msg_strings[MSG_RSP_END];
        status = mbuf_copy_from(&pmsg->mhdr, str->data, str->len);
        if (status != FC_OK) {
            req_process_error(ctx, conn, msg, errno);
            return;
        }
        pmsg->mlen += str->len;
    }

    msg->done = 1;
    msg->peer = pmsg;
    pmsg->peer = msg;

    status = event_add_out(ctx->ep, conn);
    if (status != FC_OK) {
        req_process_error(ctx, conn, msg, errno);
        return;
    }
}