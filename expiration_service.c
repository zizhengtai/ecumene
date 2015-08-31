#include <assert.h>
#include <time.h>

#include <czmq.h>
#include <hiredis/hiredis.h>
#include <jemalloc/jemalloc.h>

#include "expiration_service.h"
#include "redis_script.h"

#define UNUSED(x) (void)(x)

static const char *SCRIPT =
"local count = 0\n"
"local keys = redis.call('SMEMBERS', 'ecm-keys')\n"
"for i = 1, #keys do\n"
"  local k = 'ecm:' .. keys[i]\n"
"  count = count + redis.call('ZREMRANGEBYSCORE', k, '-inf', ARGV[1])\n"
"  if redis.call('EXISTS', k) == 0 then\n"
"    redis.call('SREM', 'ecm-keys', keys[i])\n"
"  end\n"
"end\n"
"return count";


struct _expiration_service_t {
    time_t ttl;     // seconds
    int64_t delay;  // milliseconds
    redis_script_t *script;
    redisContext *ctx;
    zactor_t *actor;
};

static redisReply *eval_expiration_script(expiration_service_t *self)
{
    return redisCommand(
            self->ctx,
            "EVALSHA %s 0 %llu",
            redis_script_sha(self->script),
            (unsigned long long)(time(NULL) - self->ttl));
}

static void s_actor(zsock_t *pipe, void *args)
{
    assert(pipe);
    assert(args);

    expiration_service_t *self = args;

    zpoller_t *poller = zpoller_new(pipe, NULL);
    assert(poller);

    int rc = zsock_signal(pipe, 0);
    UNUSED(rc);
    assert(rc == 0);

    int64_t clean_at = zclock_mono();

    while (!zsys_interrupted) {
        if (zclock_mono() >= clean_at) {
            zsys_debug("Start running script...");

            redisReply *rep = eval_expiration_script(self);
            assert(rep);

            if (rep->type != REDIS_REPLY_INTEGER) {
                assert(is_noscript_error(rep));
                freeReplyObject(rep);

                redis_script_load(self->script, self->ctx);

                rep = eval_expiration_script(self);
                assert(rep);
            }
            assert(rep->type == REDIS_REPLY_INTEGER);

            unsigned long long count = rep->integer;
            freeReplyObject(rep);

            zsys_debug("Removed %llu dead workers.", count);

            clean_at = zclock_mono() + self->delay;
        }

        zsock_t *sock = zpoller_wait(poller, self->delay);

        if (sock) {
            char *command = zstr_recv(sock);
            if (streq(command, "$TERM")) {
                free(command);
                break;
            }
            free(command);
        }
    }

    zpoller_destroy(&poller);

    zsys_debug("Cleaned up expiration service.");
}

expiration_service_t *expiration_service_new(
        const char *unix_sock_path,
        time_t ttl,
        time_t delay)
{
    expiration_service_t *self = malloc(sizeof (expiration_service_t));
    if (self) {
        // ttl
        self->ttl = ttl;
        
        // delay
        self->delay = (int64_t)(delay) * 1000;

        // ctx
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

        // script
        self->script = redis_script_new(SCRIPT);
        if (self->script == NULL) {
            redisFree(self->ctx);
            free(self);
            return NULL;
        }
        redis_script_load(self->script, self->ctx);
        
        // actor
        self->actor = zactor_new(s_actor, self);
        if (self->actor == NULL) {
            redis_script_destroy(&self->script);
            redisFree(self->ctx);
            free(self);
            return NULL;
        }
    }
    return self;
}

void expiration_service_destroy(expiration_service_t **self_p)
{
    assert(self_p);
    if (*self_p) {
        expiration_service_t *self = *self_p;

        zactor_destroy(&self->actor);
        redis_script_destroy(&self->script);
        redisFree(self->ctx);
        free(self);
        *self_p = NULL;
    }
}
