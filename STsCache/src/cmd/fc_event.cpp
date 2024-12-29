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

#include <unistd.h>
#include <sys/epoll.h>

#include <fc_core.h>
#include <fc_event.h>

int
event_init(struct context *ctx, int size)
{
    int status, ep;
    struct epoll_event *event;

    ep = epoll_create(size);
    if (ep < 0) {
        return -1;
    }

    event = (struct epoll_event *)fc_calloc(ctx->nevent, sizeof(*ctx->event));
    if (event == NULL) {
        status = close(ep);
        if (status < 0) {
        }
        return -1;
    }

    ctx->ep = ep;
    ctx->event = event;

    return 0;
}

void
event_deinit(struct context *ctx)
{
    int status;

    fc_free(ctx->event);

    status = close(ctx->ep);
    if (status < 0) {
    }
    ctx->ep = -1;
}

int
event_add_out(int ep, struct conn *c)
{
    int status;
    struct epoll_event event;

    if (c->send_active) {
        return 0;
    }

    event.events = (uint32_t)(EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = c;

    status = epoll_ctl(ep, EPOLL_CTL_MOD, c->sd, &event);
    if (status < 0) {
    } else {
        c->send_active = 1;
    }

    return status;
}

int
event_del_out(int ep, struct conn *c)
{
    int status;
    struct epoll_event event;

    if (!c->send_active) {
        return 0;
    }

    event.events = (uint32_t)(EPOLLIN | EPOLLET);
    event.data.ptr = c;

    status = epoll_ctl(ep, EPOLL_CTL_MOD, c->sd, &event);
    if (status < 0) {
    } else {
        c->send_active = 0;
    }

    return status;
}

int
event_add_conn(int ep, struct conn *c)
{
    int status;
    struct epoll_event event;

    event.events = (uint32_t)(EPOLLIN | EPOLLOUT | EPOLLET);
    event.data.ptr = c;

    status = epoll_ctl(ep, EPOLL_CTL_ADD, c->sd, &event);
    if (status < 0) {
    } else {
        c->send_active = 1;
        c->recv_active = 1;
    }

    return status;
}

int
event_del_conn(int ep, struct conn *c)
{
    int status;

    status = epoll_ctl(ep, EPOLL_CTL_DEL, c->sd, NULL);
    if (status < 0) {
    } else {
        c->recv_active = 0;
        c->send_active = 0;
    }

    return status;
}

int
event_wait(int ep, struct epoll_event *event, int nevent, int timeout)
{
    int nsd;

    for (;;) {
        nsd = epoll_wait(ep, event, nevent, timeout);
        if (nsd > 0) {
            return nsd;
        }

        if (nsd == 0) {
            if (timeout == -1) {
                return -1;
            }

            return 0;
        }

        if (errno == EINTR) {
            continue;
        }

        return -1;
    }

    NOT_REACHED();
}
