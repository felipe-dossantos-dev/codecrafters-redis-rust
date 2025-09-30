#!/bin/sh
set -e # Exit early if any commands fail

(
  redis-cli ZADD zset_key 100.0 foo | sed 's/^/[CLI 0] /' &
  redis-cli ZADD zset_key 100.0 bar | sed 's/^/[CLI 1] /' &
  redis-cli ZADD zset_key 20.0 baz  | sed 's/^/[CLI 2] /' &
  redis-cli ZADD zset_key 30.1 caz  | sed 's/^/[CLI 3] /' &
  redis-cli ZADD zset_key 40.2 paz  | sed 's/^/[CLI 4] /' &
  wait
  redis-cli ZRANK zset_key caz | sed 's/^/[CLI R1] /' &
  redis-cli ZRANK zset_key baz | sed 's/^/[CLI R0] /' &
  redis-cli ZRANK zset_key foo | sed 's/^/[CLI R4] /' &
  redis-cli ZRANK zset_key bar | sed 's/^/[CLI R3] /' &
  wait
)

echo "Tests finished."