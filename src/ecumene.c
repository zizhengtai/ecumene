#include <assert.h>

#include <czmq.h>
#include <hiredis/hiredis.h>

#include "assignment_service.h"
#include "expiration_service.h"
#include "heartbeat_service.h"

const time_t TTL = 10;
const time_t EXPIRATION_DELAY = 12;
const char *UNIX_SOCK_PATH = "/tmp/redis.sock";

int main(void) {

    // Start expiration service
    expiration_service_t *expr =
        expiration_service_new(UNIX_SOCK_PATH, TTL, EXPIRATION_DELAY);
    assert(expr);

    // Start heartbeat service
    heartbeat_service_t *heartbeat =
        heartbeat_service_new(UNIX_SOCK_PATH);
    assert(heartbeat);

    // Start assignment service
    assignment_service_t *assignment =
        assignment_service_new(UNIX_SOCK_PATH);
    assert(heartbeat);

    while (!zsys_interrupted) {
        zclock_sleep(500);
    }

    assignment_service_destroy(&assignment);
    heartbeat_service_destroy(&heartbeat);
    expiration_service_destroy(&expr);

    zsys_debug("Cleaned up everything.");

    return 0;
}
