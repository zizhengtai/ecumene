#include <assert.h>
#include <string.h>

#include <jemalloc/jemalloc.h>

#include "redis_script.h"

struct _redis_script_t {
    char *script;
    char *script_sha;
};

redis_script_t *redis_script_new(const char *script)
{
    redis_script_t *self = malloc(sizeof (redis_script_t));
    if (self) {
        self->script = malloc((strlen(script) + 1) * sizeof (char));
        if (self->script == NULL) {
            free(self);
            return NULL;
        }
        strcpy(self->script, script);

        self->script_sha = NULL;
    }
    return self;
}

void redis_script_destroy(redis_script_t **self_p)
{
    assert(self_p);
    if (*self_p) {
        redis_script_t *self = *self_p;

        free(self->script_sha);
        free(self->script);
        free(self);
        *self_p = NULL;
    }
}

int redis_script_load(redis_script_t *self, redisContext *ctx)
{
    redisReply *rep = redisCommand(
            ctx,
            "SCRIPT LOAD %s",
            self->script);
    if (rep == NULL) {
        return -1;
    }
    if (rep->type != REDIS_REPLY_STRING) {
        freeReplyObject(rep);
        return -1;
    }

    free(self->script_sha);
    self->script_sha = malloc((rep->len + 1) * sizeof (char));
    if (self->script_sha == NULL) {
        return -1;
    }
    strcpy(self->script_sha, rep->str);
    freeReplyObject(rep);

    return 0;
}

const char *redis_script_sha(redis_script_t *self)
{
    return self->script_sha ? self->script_sha : "";
}

bool is_noscript_error(redisReply *rep)
{
    static const char *NOSCRIPT_ERROR = "NOSCRIPT";
    static const int NOSCRIPT_ERROR_LEN = 8;

    return rep->type == REDIS_REPLY_ERROR &&
        strncmp(rep->str, NOSCRIPT_ERROR, NOSCRIPT_ERROR_LEN) == 0;
}
