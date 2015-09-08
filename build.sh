#!/bin/sh

gcc -std=c99 -Wall -Wextra -pedantic -O3 -flto -o ecumene src/*.c -lczmq -lhiredis -ljemalloc
