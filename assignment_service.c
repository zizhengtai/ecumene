#include <assert.h>
#include <stdlib.h>

#include <czmq.h>
#include <hiredis/hiredis.h>
#include <jemalloc/jemalloc.h>

#include "assignment_service.h"
#include "redis_script.h"

#define UNUSED(x) (void)(x)

static const uint16_t PROTOCOL_VERSION = 0;
static const time_t TTL = 10;

static const char *SCRIPT =
"local key = 'ecm:' .. KEYS[1]\n"
"local cnt = redis.call('ZCOUNT', key, ARGV[1], '+inf')\n"
"local idx = math.floor(cnt * ARGV[2])\n"
"return redis.call('ZRANGEBYSCORE', key, ARGV[1], '+inf', 'LIMIT', idx, 1)[1]";

struct _assignment_service_t {
    redisContext *ctx;
    redis_script_t *script;
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

static uint64_t rand_state;

static void init_randf(void)
{
    rand_state = time(NULL);
}

static double randf(void)
{
    rand_state ^= rand_state >> 12; // a
	rand_state ^= rand_state << 25; // b
	rand_state ^= rand_state >> 27; // c
    return 0.999999 * (rand_state * 2685821657736338717ULL) / (uint64_t)(-1);
}

static redisReply *eval_lookup_script(assignment_service_t *self, const char *ecm_key)
{
    return redisCommand(
            self->ctx,
            "EVALSHA %s 1 %s %llu %f",
            redis_script_sha(self->script),
            ecm_key,
            (unsigned long long)(time(NULL) - TTL),
            randf());
}

static char *get_random_endpoint(assignment_service_t *self, const char *ecm_key)
{
    redisReply *rep = eval_lookup_script(self, ecm_key);
    assert(rep);

    if (rep->type != REDIS_REPLY_STRING && rep->type != REDIS_REPLY_NIL) {
        zsys_debug("Reloading Lua script...");

        assert(is_noscript_error(rep));
        freeReplyObject(rep);

        redis_script_load(self->script, self->ctx);

        rep = eval_lookup_script(self, ecm_key);
        assert(rep);
    }

    char *endpoint = NULL;
    if (rep->type == REDIS_REPLY_STRING) {
        endpoint = malloc((rep->len + 1) * sizeof (char));
        assert(endpoint);
        strcpy(endpoint, rep->str);
    }
    freeReplyObject(rep);

    zsys_debug("endpoint: %s", endpoint);

    return endpoint;
}

static void s_actor(zsock_t *pipe, void *args)
{
    assert(pipe);
    assert(args);

    assignment_service_t *self = args;

    zpoller_t *poller = zpoller_new(pipe, self->sock, NULL);
    assert(poller);

    int rc = zsock_signal(pipe, 0);
    UNUSED(rc);
    assert(rc == 0);

    init_randf();

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
            zframe_t *identity = zmsg_pop(msg);

            zframe_t *version = zmsg_pop(msg);
            if (check_version(&version)) {
                if (zmsg_size(msg) == 2) {
                    // 1 - ID
                    // 2 - Ecumene Key
                    zframe_t *id = zmsg_pop(msg);
                    char *ecm_key = zmsg_popstr(msg);

                    char *endpoint = get_random_endpoint(self, ecm_key);
                    const char *status = endpoint ? "" : "U";

                    // 1 - Identity
                    // 2 - ID
                    // 3 - Ecumene Key
                    // 4 - Status
                    // 5 - Endpoint
                    zmsg_append(msg, &identity);
                    zmsg_append(msg, &id);
                    zmsg_addstr(msg, ecm_key);
                    zmsg_addstr(msg, status);
                    zmsg_addstr(msg, endpoint ? endpoint : "");

                    rc = zmsg_send(&msg, sock);
                    assert(rc == 0);

                    free(endpoint);
                    free(ecm_key);
                }
            }

            zframe_destroy(&identity);
            zmsg_destroy(&msg);
        }
    }

    zpoller_destroy(&poller);

    zsys_debug("Cleaned up assignment service.");
}

assignment_service_t *assignment_service_new(const char *unix_sock_path)
{
    assignment_service_t *self = malloc(sizeof (assignment_service_t));
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

        self->script = redis_script_new(SCRIPT);
        if (self->script == NULL) {
            redisFree(self->ctx);
            free(self);
            return NULL;
        }
        redis_script_load(self->script, self->ctx);

        self->sock = zsock_new_router("@tcp://*:23332");
        if (self->sock == NULL) {
            redisFree(self->ctx);
            free(self);
            return NULL;
        }

        self->actor = zactor_new(s_actor, self);
        if (self->actor == NULL) {
            zsock_destroy(&self->sock);
            redisFree(self->ctx);
            free(self);
            return NULL;
        }
    }
    return self;
}

void assignment_service_destroy(assignment_service_t **self_p)
{
    assert(self_p);
    if (*self_p) {
        assignment_service_t *self = *self_p;

        zactor_destroy(&self->actor);
        zsock_destroy(&self->sock);
        redis_script_destroy(&self->script);
        redisFree(self->ctx);
        free(self);
    }
    *self_p = NULL;
}
