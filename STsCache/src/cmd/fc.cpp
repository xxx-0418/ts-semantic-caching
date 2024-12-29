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

#include <getopt.h>
#include <unistd.h>
#include <iostream>
#include <string>
#include "storage/semantic_graph.h"
#include "storage/time_range_skiplist.h"
#include "fc_util.h"
#include "fc_core.h"
#include "defs.h"
#include "common/macros.h"

#define FC_SLAB_SIZE        SLAB_SIZE

#define FC_DAEMONIZE        false

#define FC_LOG_FILE         NULL
#define FC_LOG_DEFAULT      LOG_INFO
#define FC_LOG_MIN          LOG_EMERG
#define FC_LOG_MAX          LOG_PVERB

#define FC_PORT             11211
#define FC_ADDR             "0.0.0.0"

#define FC_HASH_POWER       ITEMX_HASH_POWER

#define FC_FACTOR           1.25

#define FC_INDEX_MEMORY     (64 * MB)

#define FC_SERVER_ID        0
#define FC_SERVER_N         1

Settings settings;

SemanticCache::SemanticGraph semantic_graph;
SlabManagement slab_m;

 static struct option long_options[] = {
     { "port",                 required_argument,  NULL,   'p' },
     { "max-slab-memory",      required_argument,  NULL,   'm' },
     { "ssd-device",           required_argument,  NULL,   'D' },
 };

 static char short_options[] =
     "p:"
     "m:"
     "D:"
     ;

static void
fc_set_default_options(void)
{
    settings.port = FC_PORT;
    settings.addr = FC_ADDR;
}

 static rstatus_t
 fc_get_options(int argc, char **argv)
 {
      int c, value;

      opterr = 0;

      for (;;) {
          c = getopt_long(argc, argv, short_options, long_options, NULL);
          if (c == -1) {
              break;
          }

          switch (c) {
          case 'p':
              value = fc_atoi(optarg, strlen(optarg));
              if (value <= 0) {
                  return FC_ERROR;
              }

              if (!fc_valid_port(value)) {
              }

              settings.port = value;
              break;

          case 'm':
              value = fc_atoi(optarg, strlen(optarg));
              if (value <= 0) {
                  return FC_ERROR;
              }

              settings.max_slab_memory_ = (size_t)value * MB;
              break;

          case 'D':
              settings.ssd_device_ = optarg;
              break;

          default:
              return FC_ERROR;
          }
      }

     return FC_OK;
 }

int
main(int argc, char **argv)
{
    std::cout<<"STsCache main"<<std::endl;
    rstatus_t status;
    struct context ctx;

    fc_set_default_options();

     status = fc_get_options(argc, argv);
     if (status != FC_OK) {
         exit(1);
     }
    slab_m.SetSlabManagement(settings);

    status = core_init();
    if (status != FC_OK) {
        exit(1);
    }
    status = core_start(&ctx);
    if (status != FC_OK) {
        exit(1);
    }
    for (;;) {
        status = core_loop(&ctx);
        if (status != FC_OK) {
            break;
        }
    }
    core_stop(&ctx);
    return 0;
}
