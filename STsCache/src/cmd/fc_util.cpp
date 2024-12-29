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
#include <ctype.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <stdarg.h>
#include <execinfo.h>

#include <sys/ioctl.h>
#include <linux/fs.h>

#include <fc_core.h>

int
fc_set_blocking(int sd)
{
    int flags;

    flags = fcntl(sd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }

    return fcntl(sd, F_SETFL, flags & ~O_NONBLOCK);
}

int
fc_set_nonblocking(int sd)
{
    int flags;

    flags = fcntl(sd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }

    return fcntl(sd, F_SETFL, flags | O_NONBLOCK);
}

int
fc_set_directio(int fd)
{
    int flags;

    flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) {
        return flags;
    }

    return fcntl(fd, F_SETFL, flags | O_DIRECT);
}

int
fc_set_reuseaddr(int sd)
{
    int reuse;
    socklen_t len;

    reuse = 1;
    len = sizeof(reuse);

    return setsockopt(sd, SOL_SOCKET, SO_REUSEADDR, &reuse, len);
}

int
fc_set_tcpnodelay(int sd)
{
    int nodelay;
    socklen_t len;

    nodelay = 1;
    len = sizeof(nodelay);

    return setsockopt(sd, IPPROTO_TCP, TCP_NODELAY, &nodelay, len);
}

int
fc_set_keepalive(int sd)
{
    int keepalive;
    socklen_t len;

    keepalive = 1;
    len = sizeof(keepalive);

    return setsockopt(sd, SOL_SOCKET, SO_KEEPALIVE, &keepalive, len);
}

int
fc_set_linger(int sd, int timeout)
{
    struct linger linger;
    socklen_t len;

    linger.l_onoff = 1;
    linger.l_linger = timeout;

    len = sizeof(linger);

    return setsockopt(sd, SOL_SOCKET, SO_LINGER, &linger, len);
}

int
fc_unset_linger(int sd)
{
    struct linger linger;
    socklen_t len;

    linger.l_onoff = 0;
    linger.l_linger = 0;

    len = sizeof(linger);

    return setsockopt(sd, SOL_SOCKET, SO_LINGER, &linger, len);
}

int
fc_set_sndbuf(int sd, int size)
{
    socklen_t len;

    len = sizeof(size);

    return setsockopt(sd, SOL_SOCKET, SO_SNDBUF, &size, len);
}

int
fc_set_rcvbuf(int sd, int size)
{
    socklen_t len;

    len = sizeof(size);

    return setsockopt(sd, SOL_SOCKET, SO_RCVBUF, &size, len);
}

int
fc_get_soerror(int sd)
{
    int status, err;
    socklen_t len;

    err = 0;
    len = sizeof(err);

    status = getsockopt(sd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (status == 0) {
        errno = err;
    }

    return status;
}

int
fc_get_sndbuf(int sd)
{
    int status, size;
    socklen_t len;

    size = 0;
    len = sizeof(size);

    status = getsockopt(sd, SOL_SOCKET, SO_SNDBUF, &size, &len);
    if (status < 0) {
        return status;
    }

    return size;
}

int
fc_get_rcvbuf(int sd)
{
    int status, size;
    socklen_t len;

    size = 0;
    len = sizeof(size);

    status = getsockopt(sd, SOL_SOCKET, SO_RCVBUF, &size, &len);
    if (status < 0) {
        return status;
    }

    return size;
}

void
fc_maximize_sndbuf(int sd)
{
    int status, min, max, avg;

    min = fc_get_sndbuf(sd);
    if (min < 0) {
        return;
    }

    max = 256 * MB;

    while (min <= max) {
        avg = (min + max) / 2;
        status = fc_set_sndbuf(sd, avg);
        if (status != 0) {
            max = avg - 1;
        } else {
            min = avg + 1;
        }
    }
}

int64_t
fc_usec_now(void)
{
    struct timeval now;
    int64_t usec;
    int status;

    status = gettimeofday(&now, NULL);
    if (status < 0) {
        return -1;
    }

    usec = (int64_t)now.tv_sec * 1000000LL + (int64_t)now.tv_usec;

    return usec;
}

rstatus_t
fc_device_size(const char *path, size_t *size)
{
    int status;
    struct stat statinfo;
    int fd;

    status = stat(path, &statinfo);
    if (status < 0) {
        return FC_ERROR;
    }

    if (!S_ISREG(statinfo.st_mode) && !S_ISBLK(statinfo.st_mode)) {
        return FC_ERROR;
    }

    if (S_ISREG(statinfo.st_mode)) {
        *size = (size_t)statinfo.st_size;
        return FC_OK;
    }

    fd = open(path, O_RDONLY, 0644);
    if (fd < 0) {
        return FC_ERROR;
    }

    status = ioctl(fd, BLKGETSIZE64, size);
    if (status < 0) {
        close(fd);
        return FC_ERROR;
    }

    close(fd);

    return FC_OK;
}

int
_fc_atoi(uint8_t *line, size_t n)
{
    int value;

    if (n == 0) {
        return -1;
    }

    for (value = 0; n--; line++) {
        if (*line < '0' || *line > '9') {
            return -1;
        }

        value = value * 10 + (*line - '0');
    }

    if (value < 0) {
        return -1;
    }

    return value;
}

rstatus_t
_fc_atou32(uint8_t *line, size_t n, uint32_t *u32)
{
    uint32_t value;

    *u32 = 0UL;

    if (n == 0UL || n >= FC_UINT32_MAXLEN) {
        return FC_ERROR;
    }

    for (value = 0; n--; line++) {
        if (*line < '0' || *line > '9') {
            return FC_ERROR;
        }

        if(value > (UINT32_MAX / 10) || value * 10ULL > (UINT32_MAX-(*line - '0'))) {
            *u32 = 0UL;
            return FC_ERROR;
        }

        value = value * 10UL + (uint32_t)(*line - '0');
    }

    *u32 = value;

    return FC_OK;
}

rstatus_t
_fc_atou64(uint8_t *line, size_t n, uint64_t *u64)
{
    uint64_t value;

    *u64 = 0ULL;

    if (n == 0 || n >= FC_UINT64_MAXLEN) {
        return FC_ERROR;
    }

    for (value = 0ULL; n--; line++) {
        if (*line < '0' || *line > '9') {
            return FC_ERROR;
        }

        if(value > (UINT64_MAX / 10) || value * 10ULL > (UINT64_MAX-(*line - '0'))) {
            *u64 = 0ULL;
            return FC_ERROR;
        }

        value = value * 10ULL + (uint64_t)(*line - '0');
    }

    *u64 = value;

    return FC_OK;
}

bool
fc_valid_port(int n)
{
    if (n < 1 || n > UINT16_MAX) {
        return false;
    }

    return true;
}

bool
fc_strtoull(const char *str, uint64_t *out)
{
    char *endptr;
    unsigned long long ull;

    errno = 0;
    *out = 0ULL;

    ull = strtoull(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long long) ull < 0) {
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }

        *out = ull;

        return true;
    }

    return false;
}

bool
fc_strtoll(const char *str, int64_t *out)
{
    char *endptr;
    long long l_l;

    errno = 0;
    *out = 0LL;

    l_l = strtoll(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l_l;
        return true;
    }

    return false;
}

bool
fc_strtoul(const char *str, uint32_t *out)
{
    char *endptr;
    unsigned long l;

    errno = 0;
    *out = 0UL;

    l = strtoul(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        if ((long) l < 0) {
            if (strchr(str, '-') != NULL) {
                return false;
            }
        }

        *out = l;

        return true;
    }

    return false;
}

bool
fc_strtol(const char *str, int32_t *out)
{
    char *endptr;
    long l;

    *out = 0L;
    errno = 0;

    l = strtol(str, &endptr, 10);

    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }

    return false;
}

bool
fc_str2oct(const char *str, int32_t *out)
{
    char *endptr;
    long l;

    *out = 0L;
    errno = 0;

    l = strtol(str, &endptr, 8);

    if (errno == ERANGE) {
        return false;
    }

    if (isspace(*endptr) || (*endptr == '\0' && endptr != str)) {
        *out = l;
        return true;
    }

    return false;
}

int
_vscnprintf(char *buf, size_t size, const char *fmt, va_list args)
{
    int i;

    i = vsnprintf(buf, size, fmt, args);

    if (i <= 0) {
        return 0;
    }

    if ((size_t)i < size) {
        return i;
    }

    return size - 1;
}

int
_scnprintf(char *buf, size_t size, const char *fmt, ...)
{
    va_list args;
    int i;

    va_start(args, fmt);
    i = _vscnprintf(buf, size, fmt, args);
    va_end(args);

    return i;
}

void *
_fc_alloc(size_t size, const char *name, int line)
{
    void *p;
    p = malloc(size);
    if (p == NULL) {
    } else {
    }

    return p;
}

void *
_fc_zalloc(size_t size, const char *name, int line)
{
    void *p;

    p = _fc_alloc(size, name, line);
    if (p != NULL) {
        memset(p, 0, size);
    }

    return p;
}

void *
_fc_calloc(size_t nmemb, size_t size, const char *name, int line)
{
    return _fc_zalloc(nmemb * size, name, line);
}

void *
_fc_realloc(void *ptr, size_t size, const char *name, int line)
{
    void *p;

    p = realloc(ptr, size);
    if (p == NULL) {
    } else {
    }

    return p;
}

void
_fc_free(void *ptr, const char *name, int line)
{
    free(ptr);
}

void *
_fc_mmap(size_t size, const char *name, int line)
{
    void *p;

    p = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS,
             -1, 0);
    if (p == ((void *) -1)) {
        return NULL;
    }

    return p;
}

int
_fc_munmap(void *p, size_t size, const char *name, int line)
{
    int status;

    status = munmap(p, size);
    if (status < 0) {
    }

    return status;
}

void
fc_assert(const char *cond, const char *file, int line, int panic)
{
    if (panic) {
        fc_stacktrace(1);
        abort();
    }
}

void
fc_stacktrace(int skip_count)
{
#ifdef FC_BACKTRACE
    void *stack[64];
    char **symbols;
    int size, i, j;

    size = backtrace(stack, 64);
    symbols = backtrace_symbols(stack, size);
    if (symbols == NULL) {
        return;
    }

    skip_count++; /* skip the current frame also */

    for (i = skip_count, j = 0; i < size; i++, j++) {
        loga("[%d] %s", j, symbols[i]);
    }

    free(symbols);
#endif
}

static int
fc_resolve_inet(struct string *name, int port, struct sockinfo *si)
{
    int status;
    struct addrinfo *ai, *cai;
    struct addrinfo hints;
    char *node, service[FC_UINTMAX_MAXLEN];
    bool found;

    memset(&hints, 0, sizeof(hints));
    hints.ai_flags = AI_NUMERICSERV;
    hints.ai_family = AF_UNSPEC;
    hints.ai_socktype = SOCK_STREAM;
    hints.ai_protocol = 0;
    hints.ai_addrlen = 0;
    hints.ai_addr = NULL;
    hints.ai_canonname = NULL;

    if (name != NULL) {
        node = (char *)name->data;
    } else {
        node = NULL;
        hints.ai_flags |= AI_PASSIVE;
    }

    fc_snprintf(service, FC_UINTMAX_MAXLEN, "%d", port);

    status = getaddrinfo(node, service, &hints, &ai);
    if (status < 0) {
        return -1;
    }

    for (cai = ai, found = false; cai != NULL; cai = cai->ai_next) {
        si->family = cai->ai_family;
        si->addrlen = cai->ai_addrlen;
        fc_memcpy(&si->addr, cai->ai_addr, si->addrlen);
        found = true;
        break;
    }

    freeaddrinfo(ai);

    return !found ? -1 : 0;
}

static int
fc_resolve_unix(struct string *name, struct sockinfo *si)
{
    struct sockaddr_un *un;

    if (name->len >= FC_UNIX_ADDRSTRLEN) {
        return -1;
    }

    un = &si->addr.un;

    un->sun_family = AF_UNIX;
    fc_memcpy(un->sun_path, name->data, name->len);
    un->sun_path[name->len] = '\0';

    si->family = AF_UNIX;
    si->addrlen = sizeof(*un);

    return 0;
}

int
fc_resolve(struct string *name, int port, struct sockinfo *si)
{
    if (name != NULL && name->data[0] == '/') {
        return fc_resolve_unix(name, si);
    }

    return fc_resolve_inet(name, port, si);
}
