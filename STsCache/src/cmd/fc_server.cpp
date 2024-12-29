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
#include "defs.h"

extern Settings settings;

#define SERVER_BACKLOG 1024

static rstatus_t
server_accept(struct context *ctx, struct conn *s)
{
    rstatus_t status;
    struct conn *c;
    int sd;
    for (;;) {
        sd = accept(s->sd, NULL, NULL);
        if (sd < 0) {
            if (errno == EINTR) {
                continue;
            }

            if (errno == EAGAIN || errno == EWOULDBLOCK) {
                s->recv_ready = 0;
                return FC_OK;
            }

            return FC_ERROR;
        }

        break;
    }

    c = conn_get(sd, true);
    if (c == NULL) {
        status = close(sd);
        if (status < 0) {
        }
        return FC_ENOMEM;
    }

    status = fc_set_nonblocking(sd);
    if (status < 0) {
        return FC_ERROR;
    }

    status = fc_set_tcpnodelay(c->sd);
    if (status < 0) {
    }

    status = fc_set_keepalive(c->sd);
    if (status < 0) {
    }

    status = event_add_conn(ctx->ep, c);
    if (status < 0) {
        return FC_ERROR;
    }
    return FC_OK;
}

rstatus_t
server_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    conn->recv_ready = 1;
    do {
        status = server_accept(ctx, conn);
        if (status != FC_OK) {
            return status;
        }
    } while (conn->recv_ready);

    return FC_OK;
}

rstatus_t
server_listen(struct context *ctx)
{
    rstatus_t status;
    struct sockinfo si;
    struct string addrstr;
    int sd, family;
    socklen_t addrlen;
    struct sockaddr *addr;
    struct conn *s;

    string_set_raw(&addrstr, settings.addr);
    status = fc_resolve(&addrstr, settings.port, &si);
    if (status != FC_OK) {
        return FC_ERROR;
    }

    family = si.family;
    addrlen = si.addrlen;
    addr = (struct sockaddr *)&si.addr;

    sd = socket(family, SOCK_STREAM, 0);
    if (sd < 0) {
        return FC_ERROR;
    }

    status = fc_set_reuseaddr(sd);
    if (status != FC_OK) {
        return FC_ERROR;
    }

    status = bind(sd, addr, addrlen);
    if (status < 0) {
        return FC_ERROR;
    }

    status = listen(sd, SERVER_BACKLOG);
    if (status < 0) {
        return FC_ERROR;
    }

    status = fc_set_nonblocking(sd);
    if (status != FC_OK) {
        return FC_ERROR;
    }

    s = conn_get(sd, false);
    if (s == NULL) {
        status = close(sd);
        if (status < 0) {
        }
        return FC_ENOMEM;
    }

    status = event_add_conn(ctx->ep, s);
    if (status < 0) {
        return FC_ERROR;
    }

    status = event_del_out(ctx->ep, s);
    if (status != FC_OK) {
        return status;
    }

    return FC_OK;
}
