#!/bin/sh
set -e # Exit early if any commands fail

(
  redis-cli BLPOP banana 10 | sed 's/^/[CLI 1] /' &
  redis-cli BLPOP banana 10 | sed 's/^/[CLI 2] /' &
  redis-cli RPUSH banana strawberry | sed 's/^/[CLI 3] /' &
  wait
)

echo "Tests finished."