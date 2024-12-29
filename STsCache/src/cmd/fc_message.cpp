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
#include <stdlib.h>

#include <sys/uio.h>

#include <fc_core.h>
#include <bits/xopen_lim.h>

#if (IOV_MAX > 128)
#define FC_IOV_MAX 128
#else
#define FC_IOV_MAX IOV_MAX
#endif

#define DEFINE_ACTION(_hash, _name) fc_string(_name),
struct string msg_strings[] = {
    MSG_CODEC( DEFINE_ACTION )
    null_string
};
#undef DEFINE_ACTION

static uint64_t msg_id;
static uint64_t frag_id;
static uint32_t nfree_msgq;
static struct msg_tqh free_msgq;

static struct msg *
_msg_get(void)
{
    struct msg *msg;

    if (!TAILQ_EMPTY(&free_msgq)) {
        msg = TAILQ_FIRST(&free_msgq);
        nfree_msgq--;
        TAILQ_REMOVE(&free_msgq, msg, m_tqe);
        goto done;
    }

    msg = (struct msg *)fc_alloc(sizeof(*msg));
    if (msg == NULL) {
        return NULL;
    }

done:
    msg->id = ++msg_id;
    msg->peer = NULL;
    msg->owner = NULL;

    STAILQ_INIT(&msg->mhdr);
    msg->mlen = 0;

    msg->state = 0;
    msg->pos = NULL;
    msg->token = NULL;

    msg->parser = memcache_parse_req;
    msg->result = MSG_PARSE_OK;

    msg->type = MSG_UNKNOWN;

    msg->key_start = NULL;
    msg->key_end = NULL;
    msg->hash = 0;

    msg->semantic_segment_start = NULL;
    msg->semantic_segment_end = NULL;
    msg->semantic_segment_len = 0;

    msg->number_of_table = 0;
    msg->cur_table_number = 0;
    msg->table_byte_length_start = NULL;
    msg->end_time = 0;
    msg->start_time = 0;
    for (int i = 0; i < 200; i++) {
        msg->single_seg_start[i] = NULL;
        msg->single_seg_end[i] = NULL;
        msg->single_seg_len[i] = 0;
        msg->table_byte_length[i] = 0;
        msg->remain_tab_byte_len[i] = 0;
        msg->values[i] = NULL;
    }

    msg->flags = 0;
    msg->expiry = 0;
    msg->vlen = 0;
    msg->rvlen = 0;
    msg->value = NULL;
    msg->cas = 0;
    msg->num = 0;

    msg->frag_owner = NULL;
    msg->nfrag = 0;
    msg->frag_id = 0;

    msg->err = 0;
    msg->error = 0;
    msg->request = 0;
    msg->quit = 0;
    msg->noreply = 0;
    msg->done = 0;
    msg->first_fragment = 0;
    msg->last_fragment = 0;
    msg->swallow = 0;

    return msg;
}

struct msg *
msg_get(struct conn *conn, bool request)
{
    struct msg *msg;

    msg = _msg_get();
    if (msg == NULL) {
        return NULL;
    }

    msg->owner = conn;
    msg->request = request ? 1 : 0;

    return msg;
}

static void
msg_free(struct msg *msg)
{
    fc_free(msg);
}

void
msg_put(struct msg *msg)
{
    while (!STAILQ_EMPTY(&msg->mhdr)) {
        struct mbuf *mbuf = STAILQ_FIRST(&msg->mhdr);
        mbuf_remove(&msg->mhdr, mbuf);
        mbuf_put(mbuf);
    }

    nfree_msgq++;
    TAILQ_INSERT_HEAD(&free_msgq, msg, m_tqe);
}

bool
msg_empty(struct msg *msg)
{
    return msg->mlen == 0 ? true : false;
}

static rstatus_t
msg_parsed(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (msg->pos == mbuf->last) {
        req_recv_done(ctx, conn, msg, NULL);
        return FC_OK;
    }

    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return FC_ENOMEM;
    }

    nmsg = msg_get(msg->owner, msg->request);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return FC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    req_recv_done(ctx, conn, msg, nmsg);

    return FC_OK;
}

static rstatus_t
msg_fragment(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *nbuf;

    nbuf = mbuf_split(&msg->mhdr, msg->pos, memcache_pre_splitcopy, msg);
    if (nbuf == NULL) {
        return FC_ENOMEM;
    }

    status = memcache_post_splitcopy(msg);
    if (status != FC_OK) {
        mbuf_put(nbuf);
        return status;
    }

    nmsg = msg_get(msg->owner, msg->request);
    if (nmsg == NULL) {
        mbuf_put(nbuf);
        return FC_ENOMEM;
    }
    mbuf_insert(&nmsg->mhdr, nbuf);
    nmsg->pos = nbuf->pos;

    nmsg->mlen = mbuf_length(nbuf);
    msg->mlen -= nmsg->mlen;

    if (msg->frag_id == 0) {
        msg->frag_id = ++frag_id;
        msg->first_fragment = 1;
        msg->nfrag = 1;
        msg->frag_owner = msg;
    }
    nmsg->frag_id = msg->frag_id;
    msg->last_fragment = 0;
    nmsg->last_fragment = 1;
    nmsg->frag_owner = msg->frag_owner;
    msg->frag_owner->nfrag++;

    req_recv_done(ctx, conn, msg, nmsg);

    return FC_OK;
}

static rstatus_t
msg_repair(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct mbuf *nbuf;

    nbuf = mbuf_split(&msg->mhdr, msg->pos, NULL, NULL);
    if (nbuf == NULL) {
        return FC_ENOMEM;
    }
    mbuf_insert(&msg->mhdr, nbuf);
    msg->pos = nbuf->pos;

    return FC_OK;
}

static rstatus_t
msg_parse(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;

    if (msg_empty(msg)) {
        req_recv_done(ctx, conn, msg, NULL);
        return FC_OK;
    }

    msg->parser(msg);

    switch (msg->result) {
    case MSG_PARSE_OK:
        status = msg_parsed(ctx, conn, msg);
        break;

    case MSG_PARSE_FRAGMENT:
        status = msg_fragment(ctx, conn, msg);
        break;

    case MSG_PARSE_REPAIR:
        status = msg_repair(ctx, conn, msg);
        break;

    case MSG_PARSE_AGAIN:
        status = FC_OK;
        break;

    default:
        status = FC_ERROR;
        conn->err = errno;
        break;
    }

    return conn->err != 0 ? FC_ERROR : status;
}

static rstatus_t
msg_recv_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    rstatus_t status;
    struct msg *nmsg;
    struct mbuf *mbuf;
    size_t msize;
    ssize_t n;

    mbuf = STAILQ_LAST(&msg->mhdr, mbuf, next);
    if (mbuf == NULL || mbuf_full(mbuf)) {
        mbuf = mbuf_get();
        if (mbuf == NULL) {
            return FC_ENOMEM;
        }
        mbuf_insert(&msg->mhdr, mbuf);
        msg->pos = mbuf->pos;
    }

    msize = mbuf_size(mbuf);

    n = conn_recv(conn, mbuf->last, msize);
    if (n < 0) {
        if (n == FC_EAGAIN) {
            return FC_OK;
        }
        return FC_ERROR;
    }

    if(n == 0) {
        conn->done = 1;
        return FC_OK;
    }

    mbuf->last += n;
    msg->mlen += (uint32_t)n;

    for (;;) {
        status = msg_parse(ctx, conn, msg);
        if (status != FC_OK) {
            return status;
        }

        nmsg = req_recv_next(ctx, conn, false);
        if (nmsg == NULL || nmsg == msg) {
            break;
        }

        msg = nmsg;
    }

    return FC_OK;
}

rstatus_t
msg_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    conn->recv_ready = 1;
    do {
        msg = req_recv_next(ctx, conn, true);
        if (msg == NULL) {
            return FC_OK;
        }

        status = msg_recv_chain(ctx, conn, msg);
        if (status != FC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return FC_OK;
}

static rstatus_t
msg_send_chain(struct context *ctx, struct conn *conn, struct msg *msg)
{
    struct msg_tqh send_msgq;
    struct msg *nmsg;
    struct mbuf *mbuf, *nbuf;
    size_t mlen;
    struct iovec *ciov, iov[FC_IOV_MAX];
    struct array sendv;
    size_t nsend, nsent;
    size_t limit;
    ssize_t n;

    TAILQ_INIT(&send_msgq);

    array_set(&sendv, iov, sizeof(iov[0]), FC_IOV_MAX);

    nsend = 0;
    limit = SSIZE_MAX;

    for (;;) {

        TAILQ_INSERT_TAIL(&send_msgq, msg, m_tqe);

        for (mbuf = STAILQ_FIRST(&msg->mhdr);
             mbuf != NULL && array_n(&sendv) < FC_IOV_MAX && nsend < limit;
             mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if ((nsend + mlen) > limit) {
                mlen = limit - nsend;
            }

            ciov = (struct iovec *)array_push(&sendv);
            ciov->iov_base = mbuf->pos;
            ciov->iov_len = mlen;

            nsend += mlen;
        }

        if (array_n(&sendv) >= FC_IOV_MAX || nsend >= limit) {
            break;
        }

        msg = rsp_send_next(ctx, conn);
        if (msg == NULL) {
            break;
        }
    }

    conn->smsg = NULL;

    if (nsend != 0) {
        n = conn_sendv(conn, &sendv, nsend);
    } else {
        NOT_REACHED();
        n = 0;
    }
    nsent = n > 0 ? (size_t)n : 0;

    for (msg = TAILQ_FIRST(&send_msgq); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, m_tqe);

        TAILQ_REMOVE(&send_msgq, msg, m_tqe);

        if (nsent == 0) {
            if (msg->mlen == 0) {
                rsp_send_done(ctx, conn, msg);
            }
            continue;
        }

        for (mbuf = STAILQ_FIRST(&msg->mhdr); mbuf != NULL; mbuf = nbuf) {
            nbuf = STAILQ_NEXT(mbuf, next);

            if (mbuf_empty(mbuf)) {
                continue;
            }

            mlen = mbuf_length(mbuf);
            if (nsent < mlen) {
                mbuf->pos += nsent;
                nsent = 0;
                break;
            }

            mbuf->pos = mbuf->last;
            nsent -= mlen;
        }

        if (mbuf == NULL) {
            rsp_send_done(ctx, conn, msg);
        }
    }

    if (n >= 0) {
        return FC_OK;
    }

    return (n == FC_EAGAIN) ? FC_OK : FC_ERROR;
}

rstatus_t
msg_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg;

    conn->send_ready = 1;
    do {
        msg = rsp_send_next(ctx, conn);
        if (msg == NULL) {
            return FC_OK;
        }

        status = msg_send_chain(ctx, conn, msg);
        if (status != FC_OK) {
            return status;
        }
    } while (conn->send_ready);

    return FC_OK;
}

void
msg_init(void)
{
    msg_id = 0;
    frag_id = 0;
    nfree_msgq = 0;
    TAILQ_INIT(&free_msgq);
}

void
msg_deinit(void)
{
    struct msg *msg, *nmsg;

    for (msg = TAILQ_FIRST(&free_msgq); msg != NULL;
         msg = nmsg, nfree_msgq--) {
        nmsg = TAILQ_NEXT(msg, m_tqe);
        msg_free(msg);
    }
}
