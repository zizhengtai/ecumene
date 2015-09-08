#ifndef HEARTBEAT_SERVICE_H
#define HEARTBEAT_SERVICE_H

typedef struct _heartbeat_service_t heartbeat_service_t;

heartbeat_service_t *heartbeat_service_new(const char *unix_sock_path);
void heartbeat_service_destroy(heartbeat_service_t **self_p);

#endif /* HEARTBEAT_SERVICE_H */
