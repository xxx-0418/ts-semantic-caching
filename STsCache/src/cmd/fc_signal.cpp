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

#include <stdlib.h>
#include <signal.h>

#include <fc_core.h>
#include <cstddef>
#include <iostream>
#include <string>

static struct signal signals[] = {
    { SIGUSR1, "SIGUSR1", 0,            signal_handler },
    { SIGUSR2, "SIGUSR2", 0,            signal_handler },
    { SIGTTIN, "SIGTTIN", 0,            signal_handler },
    { SIGTTOU, "SIGTTOU", 0,            signal_handler },
    { SIGHUP,  "SIGHUP",  0,            signal_handler },
    { SIGINT,  "SIGINT",  0,            signal_handler },
    { SIGSEGV, "SIGSEGV", (int)SA_RESETHAND, signal_handler },
    { SIGPIPE, "SIGPIPE", 0,            SIG_IGN },
    { 0,        NULL,     0,            NULL }
};

rstatus_t
signal_init(void)
{
    struct signal *sig;

    for (sig = signals; sig->signo != 0; sig++) {
        rstatus_t status;
        struct sigaction sa;

        memset(&sa, 0, sizeof(sa));
        sa.sa_handler = sig->handler;
        sa.sa_flags = sig->flags;
        sigemptyset(&sa.sa_mask);

        status = sigaction(sig->signo, &sa, NULL);
        if (status < 0) {
            return FC_ERROR;
        }
    }

    return FC_OK;
}

void
signal_deinit(void)
{
}

void
signal_handler(int signo)
{
  std::cout << "fc_signal.cpp: signal_handler failed" << std::endl;
}
