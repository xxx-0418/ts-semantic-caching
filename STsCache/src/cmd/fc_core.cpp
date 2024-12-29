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

#include <fc_core.h>
#include <fc_server.h>
#include <iostream>


extern struct settings settings;

rstatus_t
core_init(void)
{
    rstatus_t status;

    status = signal_init();
    if (status != FC_OK) {
        return status;
    }

    conn_init();

    mbuf_init();

    msg_init();

    return FC_OK;
}

void
core_deinit(void)
{
}

static rstatus_t
core_recv(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->recv(ctx, conn);
    if (status != FC_OK) {
      std::cout << "core_recv failed" << std::endl;
    }

    return status;
}

static rstatus_t
core_send(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = conn->send(ctx, conn);
    if (status != FC_OK) {
    }

    return status;
}

static void
core_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    status = event_del_conn(ctx->ep, conn);
    if (status < 0) {
    }

    conn->close(ctx, conn);
}

static void
core_error(struct context *ctx, struct conn *conn)
{
    rstatus_t status;

    status = fc_get_soerror(conn->sd);
    if (status < 0) {
    }
    conn->err = errno;

    core_close(ctx, conn);
}

static void
core_core(struct context *ctx, struct conn *conn, uint32_t events)
{
    rstatus_t status;

    conn->events = events;

    if (events & EPOLLERR) {
        core_error(ctx, conn);
        return;
    }

    if (events & (EPOLLIN | EPOLLHUP)) {
        status = core_recv(ctx, conn);
        if (status != FC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return;
        }
    }

    if (events & EPOLLOUT) {
        status = core_send(ctx, conn);
        if (status != FC_OK || conn->done || conn->err) {
            core_close(ctx, conn);
            return;
        }
    }
}

rstatus_t
core_start(struct context *ctx)
{
    rstatus_t status;

    ctx->ep = -1;
    ctx->nevent = 1024;
    ctx->max_timeout = -1;
    ctx->timeout = ctx->max_timeout;
    ctx->event = NULL;

    status = event_init(ctx, 1024);
    if (status != FC_OK) {
        return status;
    }

    status = server_listen(ctx);
    if (status != FC_OK) {
        return status;
    }

    return FC_OK;
}

void
core_stop(struct context *ctx)
{
}

rstatus_t
core_loop(struct context *ctx)
{
    int i, nsd;

    nsd = event_wait(ctx->ep, ctx->event, ctx->nevent, ctx->timeout);
    if (nsd < 0) {
        return nsd;
    }

    for (i = 0; i < nsd; i++) {
        struct epoll_event *ev = &ctx->event[i];

        core_core(ctx, (struct conn *)ev->data.ptr, ev->events);
    }

    return FC_OK;
}

