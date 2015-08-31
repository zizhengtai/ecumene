#ifndef EXPIRATION_SERVICE_H
#define EXPIRATION_SERVICE_H

#include <stddef.h>

typedef struct _expiration_service_t expiration_service_t;

expiration_service_t *expiration_service_new(
        const char *unix_sock_path,
        time_t ttl,
        time_t delay);
void expiration_service_destroy(expiration_service_t **self);

#endif /* EXPIRATION_SERVICE_H */
