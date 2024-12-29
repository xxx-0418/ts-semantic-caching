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

#include <fc_core.h>
#include <fc_server.h>
#include <fc_client.h>

extern struct settings settings;

static uint32_t nalloc_conn;
static uint32_t nfree_connq;
static struct conn_tqh free_connq;

static void conn_free(struct conn *conn);

void
conn_init(void)
{
    nfree_connq = 0;
    nalloc_conn = 0;
    TAILQ_INIT(&free_connq);
}

void
conn_deinit(void)
{
    struct conn *conn, *nconn;

    for (conn = TAILQ_FIRST(&free_connq); conn != NULL;
         conn = nconn, nfree_connq--) {
        nconn = TAILQ_NEXT(conn, tqe);
        conn_free(conn);
    }
}

ssize_t
conn_recv(struct conn *conn, void *buf, size_t size)
{
    ssize_t n;

    for (;;) {
        n = fc_read(conn->sd, buf, size);

        if (n > 0) {
            if (n < (ssize_t) size) {
                conn->recv_ready = 0;
            }
            conn->recv_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            conn->recv_ready = 0;
            conn->eof = 1;
            return n;
        }

        if (errno == EINTR) {
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->recv_ready = 0;
            return FC_EAGAIN;
        } else {
            conn->recv_ready = 0;
            conn->err = errno;
            return FC_ERROR;
        }
    }
    return FC_ERROR;
}

ssize_t
conn_sendv(struct conn *conn, struct array *sendv, size_t nsend)
{
    ssize_t n;

    for (;;) {
        n = fc_writev(conn->sd, (const struct iovec *)sendv->elem, sendv->nelem);

        if (n > 0) {
            if (n < (ssize_t) nsend) {
                conn->send_ready = 0;
            }
            conn->send_bytes += (size_t)n;
            return n;
        }

        if (n == 0) {
            conn->send_ready = 0;
            return 0;
        }

        if (errno == EINTR) {
            continue;
        } else if (errno == EAGAIN || errno == EWOULDBLOCK) {
            conn->send_ready = 0;
            return FC_EAGAIN;
        } else {
            conn->send_ready = 0;
            conn->err = errno;
            return FC_ERROR;
        }
    }

    return FC_ERROR;
}

static void
conn_free(struct conn *conn)
{
    fc_free(conn);
}

void
conn_put(struct conn *conn)
{
    nfree_connq++;
    TAILQ_INSERT_HEAD(&free_connq, conn, tqe);
}

static struct conn *
_conn_get(void)
{
    struct conn *conn;

    if (!TAILQ_EMPTY(&free_connq)) {

        conn = TAILQ_FIRST(&free_connq);
        nfree_connq--;
        TAILQ_REMOVE(&free_connq, conn, tqe);
    } else {
        conn = (struct conn *)fc_alloc(sizeof(*conn));
        if (conn == NULL) {
            return NULL;
        }
        ++nalloc_conn;
    }

    TAILQ_INIT(&conn->omsg_q);
    conn->rmsg = NULL;
    conn->smsg = NULL;

    conn->recv = NULL;
    conn->send = NULL;
    conn->close = NULL;
    conn->active = NULL;

    conn->send_bytes = 0;
    conn->recv_bytes = 0;

    conn->events = 0;
    conn->err = 0;
    conn->recv_active = 0;
    conn->recv_ready = 0;
    conn->send_active = 0;
    conn->send_ready = 0;

    conn->client = 0;
    conn->eof = 0;
    conn->done = 0;
    conn->noreply = 0;

    return conn;
}

struct conn *
conn_get(int sd, bool client)
{
    struct conn *c;

    c = _conn_get();
    if (c == NULL) {
        return NULL;
    }
    c->sd = sd;
    c->client = client ? 1 : 0;

    if (client) {
        c->recv = msg_recv;
        c->send = msg_send;
        c->close = client_close;
        c->active = client_active;
    } else {
        c->recv = server_recv;
        c->send = NULL;
        c->close = NULL;
        c->active = NULL;
    }

    return c;
}

uint32_t
conn_total(void)
{
    return nalloc_conn - 1;
}

uint32_t
conn_nused(void)
{
    return nalloc_conn - nfree_connq - 1;
}

uint32_t
conn_nfree(void)
{
    return nfree_connq;
}
