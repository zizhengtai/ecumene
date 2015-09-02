# Welcome
This is the C implementation for the Ecumene RPC broker.

# Building and Installation
## OS X

```shell
brew install libsodium czmq hiredis jemalloc
git clone https://github.com/ZizhengTai/ecumene.git && cd ecumene && ./build.sh
```

# Getting Started
First, start the Redis server and listen on Unix socket `/tmp/redis.sock`.

Then run `./ecumene` to start the Ecumene broker.

# License
This project is licensed under the GNU General Public License v3.0. See the [`LICENSE`](./LICENSE) file for details.
