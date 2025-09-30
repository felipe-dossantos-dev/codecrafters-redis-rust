#!/bin/sh
set -e # Exit early if any commands fail

(
  redis-cli ZADD raspberry 60.83621027881507 grape | sed 's/^/[CLI 0] /' &
  redis-cli ZADD raspberry 68.04744394728739 orange | sed 's/^/[CLI 0] /' &
  redis-cli ZADD raspberry 68.04744394728739 banana | sed 's/^/[CLI 0] /' &
  wait
  redis-cli ZRANK raspberry banana | sed 's/^/[Result] /' &
  wait
)

echo "Tests finished."