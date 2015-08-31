#include <assert.h>
#include <time.h>

#include <czmq.h>
#include <hiredis/hiredis.h>
#include <jemalloc/jemalloc.h>

#include "heartbeat_service.h"

#define UNUSED(x) (void)(x)

static const char *REG_SCRIPT =
"redis.call('SADD', 'ecm-keys', KEYS[1])\n"
"redis.call('ZADD', 'ecm:' .. KEYS[1], ARGV[1], ARGV[2])";

static const uint16_t PROTOCOL_VERSION = 0;

struct _heartbeat_service_t {
    redisContext *ctx;
    zactor_t *actor;
    zsock_t *sock;
};

static bool check_version(zframe_t **version)
{
    bool check = zframe_size(*version) == sizeof (uint16_t) &&
        memcmp(zframe_data(*version), &PROTOCOL_VERSION, sizeof (uint16_t)) == 0;
    zframe_destroy(version);
    return check;
}

static void handle_heartbeat(heartbeat_service_t *self, zmsg_t *msg)
{
    // 1 - Action
    // 2 - Ecumene Key
    // 3 - Endpoint
    zsys_debug("Handling...");

    if (zmsg_size(msg) != 3) {
        return;
    }

    char *action = zmsg_popstr(msg);
    char *ecm_key = zmsg_popstr(msg);
    char *endpoint = zmsg_popstr(msg);
    time_t score = time(NULL);
    
    if (streq(action, "")) {
        zsys_debug("%s => %s", ecm_key, endpoint);

        freeReplyObject(redisCommand(
                    self->ctx,
                    "EVAL %s 1 %s %llu %s",
                    REG_SCRIPT,
                    ecm_key,
                    score,
                    endpoint));
    } else if (streq(action, "U")) {
        zsys_debug("%s unregistered from %s", endpoint, ecm_key);

        freeReplyObject(redisCommand(
                    self->ctx,
                    "ZREM ecm:%s %s",
                    ecm_key,
                    endpoint));

        // Do not remove empty keys from ecmKeys here.
        // We will let the expiration service handle this.
    }

    free(action);
    free(ecm_key);
    free(endpoint);
}

static void s_actor(zsock_t *pipe, void *args)
{
    assert(pipe);
    assert(args);

    heartbeat_service_t *self = args;

    zpoller_t *poller = zpoller_new(pipe, self->sock, NULL);
    assert(poller);

    int rc = zsock_signal(pipe, 0);
    UNUSED(rc);
    assert(rc == 0);

    bool terminated = false;
    while (!terminated && !zsys_interrupted) {
        zsock_t *sock = zpoller_wait(poller, -1);

        if (sock == pipe) {
            char *command = zstr_recv(sock);
            if (streq(command, "$TERM")) {
                terminated = true;
            }
            free(command);
        } else if (sock == self->sock) {
            zmsg_t *msg = zmsg_recv(sock);
            zframe_t *version = zmsg_pop(msg);
            if (check_version(&version)) {
                handle_heartbeat(self, msg);
            }
            zmsg_destroy(&msg);
        }
    }

    zpoller_destroy(&poller);

    zsys_debug("Cleaned up heartbeat service.");
}

heartbeat_service_t *heartbeat_service_new(const char *unix_sock_path)
{
    heartbeat_service_t *self = malloc(sizeof (heartbeat_service_t));
    if (self) {
        self->ctx = redisConnectUnix(unix_sock_path);
        if (self->ctx == NULL) {
            free(self);
            return NULL;
        }
        if (self->ctx->err) {
            redisFree(self->ctx);
            free(self);
            return NULL;
        }

        self->sock = zsock_new_pull("@tcp://*:23331");
        if (self->sock == NULL) {
            redisFree(self->ctx);
            free(self);
            return NULL;
        }
        
        self->actor = zactor_new(s_actor, self);
        if (self->actor == NULL) {
            redisFree(self->ctx);
            free(self);
            return NULL;
        }
    }
    return self;
}

void heartbeat_service_destroy(heartbeat_service_t **self_p)
{
    assert(self_p);
    if (*self_p) {
        heartbeat_service_t *self = *self_p;

        zactor_destroy(&self->actor);
        zsock_destroy(&self->sock);
        redisFree(self->ctx);
        free(self);
        self_p = NULL;
    }
}
