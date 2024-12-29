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

extern struct settings settings;

#define TIME_MAXDELTA   (time_t)(60 * 60 * 24 * 30)

static time_t process_started;

static volatile rel_time_t now;

void
time_update(void)
{
    int status;
    struct timeval timer;

    status = gettimeofday(&timer, NULL);
    if (status < 0) {
    }
    now = (rel_time_t) (timer.tv_sec - process_started);
}

rel_time_t
time_now(void)
{
    return now;
}

time_t
time_now_abs(void)
{
    return process_started + (time_t)now;
}

time_t
time_started(void)
{
    return process_started;
}

rel_time_t
time_reltime(time_t exptime)
{
    if (exptime == 0) {
        return 0;
    }

    if (exptime > TIME_MAXDELTA) {
        if (exptime <= process_started) {
            return (rel_time_t)1;
        }

        return (rel_time_t)(exptime - process_started);
    } else {
        return (rel_time_t)(exptime + now);
    }
}

static void *
time_loop(void *arg)
{
    struct epoll_event event;
    int ep;
    int n;

    ep = epoll_create(100);
    if (ep < 0) {
        return NULL;
    }

    for (;;) {
        n = epoll_wait(ep, &event, 1, 1000);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }

        if (n == 0) {
            time_update();
            continue;
        }
    }

    return NULL;
}

rstatus_t
time_init(void)
{
    int status;
    pthread_t tid;

    process_started = time(NULL) - 2;

    status = pthread_create(&tid, NULL, time_loop, NULL);
    if (status != 0) {
        return FC_ERROR;
    }

    return FC_OK;
}

void
time_deinit(void)
{
}
