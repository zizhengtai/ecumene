#ifndef ASSIGNMENT_SERVICE_H
#define ASSIGNMENT_SERVICE_H

typedef struct _assignment_service_t assignment_service_t;

assignment_service_t *assignment_service_new(const char *unix_sock_path);
void assignment_service_destroy(assignment_service_t **self_p);


#endif /* ASSIGNMENT_SERVICE_H */
