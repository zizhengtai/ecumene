#!/bin/sh

rm -f ecumene
gcc -Wall -Wextra -pedantic -O3 -flto -o ecumene ecumene.c assignment_service.c expiration_service.c heartbeat_service.c redis_script.c -lczmq -lhiredis -ljemalloc
