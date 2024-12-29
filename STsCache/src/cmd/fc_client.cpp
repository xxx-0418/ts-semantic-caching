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
#include <fc_client.h>

bool
client_active(struct conn *conn)
{
    if (!TAILQ_EMPTY(&conn->omsg_q)) {
        return true;
    }

    if (conn->rmsg != NULL) {
        return true;
    }

    if (conn->smsg != NULL) {
        return true;
    }

    return false;
}

void
client_close(struct context *ctx, struct conn *conn)
{
    rstatus_t status;
    struct msg *msg, *nmsg;

    if (conn->sd < 0) {
        conn_put(conn);
        return;
    }

    msg = conn->rmsg;
    if (msg != NULL) {
        conn->rmsg = NULL;

        req_put(msg);
    }

    for (msg = TAILQ_FIRST(&conn->omsg_q); msg != NULL; msg = nmsg) {
        nmsg = TAILQ_NEXT(msg, c_tqe);

        req_dequeue_omsgq(ctx, conn, msg);

        if (msg->done) {
            req_put(msg);
        } else {
            msg->swallow = 1;
        }
    }

    status = close(conn->sd);
    if (status < 0) {
    }
    conn->sd = -1;

    conn_put(conn);
}
