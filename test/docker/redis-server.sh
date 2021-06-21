#!/bin/bash

MODULES=" --loadmodule /usr/lib/redis/modules/redisearch.so"

if [[ -f /usr/lib/redis/modules/rejson.so ]]; then
	MODULES+=" --loadmodule /usr/lib/redis/modules/rejson.so"
fi

redis-server $MODULES
