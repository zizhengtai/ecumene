#!/bin/sh

gcc -std=c99 -Wall -Wextra -pedantic -O3 -flto -I/usr/local/include -L/usr/local/lib -o ecumene src/*.c -lczmq -lhiredis -ljemalloc
