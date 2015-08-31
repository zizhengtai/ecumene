#ifndef REDIS_SCRIPT_H
#define REDIS_SCRIPT_H

#include <stdbool.h>

#include <hiredis/hiredis.h>

typedef struct _redis_script_t redis_script_t;

redis_script_t *redis_script_new(const char *script);
void redis_script_destroy(redis_script_t **self_p);
int redis_script_load(redis_script_t *self, redisContext *ctx);
const char *redis_script_sha(redis_script_t *self);

bool is_noscript_error(redisReply *rep);

#endif /* REDIS_SCRIPT_H */
